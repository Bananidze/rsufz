// Package postgres реализует usecase.TaskRepository поверх PostgreSQL (pgx/v5).
package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Bananidze/rsufz/internal/domain"
	"github.com/Bananidze/rsufz/internal/usecase"
)

// Repo — реализация usecase.TaskRepository.
type Repo struct {
	pool *pgxpool.Pool
}

// New создаёт репозиторий с готовым пулом соединений.
func New(pool *pgxpool.Pool) *Repo {
	return &Repo{pool: pool}
}

// — helpers ---------------------------------------------------------------

func toUUID(id domain.TaskID) (pgtype.UUID, error) {
	parsed, err := uuid.Parse(string(id))
	if err != nil {
		return pgtype.UUID{}, fmt.Errorf("invalid task id %q: %w", id, err)
	}
	return pgtype.UUID{Bytes: parsed, Valid: true}, nil
}

func fromUUID(u pgtype.UUID) domain.TaskID {
	return domain.TaskID(uuid.UUID(u.Bytes).String())
}

const taskColumns = `
	id, type, payload, priority, status,
	attempt_count, retry_limit,
	scheduled_at, created_at, updated_at,
	worker_id, last_error, result, idempotency_key`

func scanTask(row pgx.Row) (*domain.Task, error) {
	var (
		id             pgtype.UUID
		typ            string
		payload        []byte
		priority       int16
		status         string
		attemptCount   int32
		retryLimit     int32
		scheduledAt    pgtype.Timestamptz
		createdAt      pgtype.Timestamptz
		updatedAt      pgtype.Timestamptz
		workerID       pgtype.Text
		lastError      pgtype.Text
		result         []byte
		idempotencyKey pgtype.Text
	)
	err := row.Scan(
		&id, &typ, &payload, &priority, &status,
		&attemptCount, &retryLimit,
		&scheduledAt, &createdAt, &updatedAt,
		&workerID, &lastError, &result, &idempotencyKey,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, err
	}
	t := &domain.Task{
		ID:           fromUUID(id),
		Type:         typ,
		Payload:      payload,
		Priority:     domain.Priority(priority),
		Status:       domain.Status(status),
		AttemptCount: int(attemptCount),
		RetryLimit:   int(retryLimit),
		ScheduledAt:  scheduledAt.Time.UTC(),
		CreatedAt:    createdAt.Time.UTC(),
		UpdatedAt:    updatedAt.Time.UTC(),
	}
	if workerID.Valid {
		t.WorkerID = workerID.String
	}
	if lastError.Valid {
		t.LastError = lastError.String
	}
	t.Result = result
	if idempotencyKey.Valid {
		t.IdempotencyKey = idempotencyKey.String
	}
	return t, nil
}

func loadDeps(ctx context.Context, db pgx.Tx, taskID pgtype.UUID) ([]domain.TaskID, error) {
	rows, err := db.Query(ctx,
		`SELECT depends_on FROM task_dependencies WHERE task_id = $1`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var deps []domain.TaskID
	for rows.Next() {
		var depID pgtype.UUID
		if err := rows.Scan(&depID); err != nil {
			return nil, err
		}
		deps = append(deps, fromUUID(depID))
	}
	return deps, rows.Err()
}

func insertDeps(ctx context.Context, tx pgx.Tx, taskID pgtype.UUID, deps []domain.TaskID) error {
	if len(deps) == 0 {
		return nil
	}
	for _, dep := range deps {
		depUUID, err := toUUID(dep)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx,
			`INSERT INTO task_dependencies (task_id, depends_on) VALUES ($1, $2)`,
			taskID, depUUID)
		if err != nil {
			return err
		}
	}
	return nil
}

// — TaskRepository --------------------------------------------------------

// Create сохраняет новую задачу со статусом pending.
func (r *Repo) Create(ctx context.Context, t *domain.Task) error {
	uid, err := toUUID(t.ID)
	if err != nil {
		return err
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: create: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	_, err = tx.Exec(ctx, `
		INSERT INTO tasks (
			id, type, payload, priority, status,
			attempt_count, retry_limit,
			scheduled_at, created_at, updated_at,
			worker_id, last_error, result, idempotency_key
		) VALUES (
			$1,$2,$3,$4,$5,
			$6,$7,
			$8,$9,$10,
			$11,$12,$13,$14
		)`,
		uid, t.Type, t.Payload, int16(t.Priority), string(t.Status),
		int32(t.AttemptCount), int32(t.RetryLimit),
		t.ScheduledAt.UTC(), t.CreatedAt.UTC(), t.UpdatedAt.UTC(),
		nullText(t.WorkerID), nullText(t.LastError), t.Result, nullText(t.IdempotencyKey),
	)
	if err != nil {
		return fmt.Errorf("postgres: create: insert task: %w", err)
	}

	if err = insertDeps(ctx, tx, uid, t.Dependencies); err != nil {
		return fmt.Errorf("postgres: create: insert deps: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres: create: commit: %w", err)
	}
	return nil
}

// GetByID возвращает задачу по ID. При отсутствии — domain.ErrNotFound.
func (r *Repo) GetByID(ctx context.Context, id domain.TaskID) (*domain.Task, error) {
	uid, err := toUUID(id)
	if err != nil {
		return nil, err
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("postgres: get: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	row := tx.QueryRow(ctx,
		`SELECT `+taskColumns+` FROM tasks WHERE id = $1`, uid)
	t, err := scanTask(row)
	if err != nil {
		return nil, fmt.Errorf("postgres: get: %w", err)
	}

	t.Dependencies, err = loadDeps(ctx, tx, uid)
	if err != nil {
		return nil, fmt.Errorf("postgres: get: deps: %w", err)
	}

	return t, tx.Commit(ctx)
}

// FindByIdempotencyKey ищет задачу по ключу дедупликации (МТ.6.3).
func (r *Repo) FindByIdempotencyKey(ctx context.Context, key string) (*domain.Task, error) {
	row := r.pool.QueryRow(ctx,
		`SELECT `+taskColumns+` FROM tasks WHERE idempotency_key = $1`, key)
	t, err := scanTask(row)
	if err != nil {
		return nil, fmt.Errorf("postgres: find-by-key: %w", err)
	}
	return t, nil
}

// UpdateTask атомарно загружает задачу, применяет mutate и сохраняет результат.
// Если mutate вернул ошибку — транзакция откатывается.
func (r *Repo) UpdateTask(ctx context.Context, id domain.TaskID, mutate func(*domain.Task) error) error {
	uid, err := toUUID(id)
	if err != nil {
		return err
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: update: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	row := tx.QueryRow(ctx,
		`SELECT `+taskColumns+` FROM tasks WHERE id = $1 FOR UPDATE`, uid)
	t, err := scanTask(row)
	if err != nil {
		return fmt.Errorf("postgres: update: %w", err)
	}

	t.Dependencies, err = loadDeps(ctx, tx, uid)
	if err != nil {
		return fmt.Errorf("postgres: update: deps: %w", err)
	}

	if err = mutate(t); err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		UPDATE tasks SET
			status        = $2,
			attempt_count = $3,
			retry_limit   = $4,
			scheduled_at  = $5,
			updated_at    = $6,
			worker_id     = $7,
			last_error    = $8,
			result        = $9
		WHERE id = $1`,
		uid,
		string(t.Status),
		int32(t.AttemptCount),
		int32(t.RetryLimit),
		t.ScheduledAt.UTC(),
		t.UpdatedAt.UTC(),
		nullText(t.WorkerID),
		nullText(t.LastError),
		t.Result,
	)
	if err != nil {
		return fmt.Errorf("postgres: update: exec: %w", err)
	}

	return tx.Commit(ctx)
}

// LockNextPending выбирает до limit pending-задач, готовых к запуску,
// и блокирует их (FOR UPDATE SKIP LOCKED) для планировщика (МТ.2.1–2.4).
// DAG-фильтр: задача выбирается только если все её зависимости completed.
func (r *Repo) LockNextPending(ctx context.Context, limit int) ([]*domain.Task, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("postgres: lock-next: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	rows, err := tx.Query(ctx, `
		SELECT `+taskColumns+`
		FROM tasks t
		WHERE t.status = 'pending'
		  AND t.scheduled_at <= NOW()
		  AND NOT EXISTS (
		        SELECT 1
		        FROM task_dependencies td
		        JOIN tasks dep ON dep.id = td.depends_on
		        WHERE td.task_id = t.id
		          AND dep.status != 'completed'
		  )
		ORDER BY t.priority DESC, t.scheduled_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED`,
		int32(limit))
	if err != nil {
		return nil, fmt.Errorf("postgres: lock-next: query: %w", err)
	}
	defer rows.Close()

	// Сначала собираем все задачи — rows должен быть закрыт до loadDeps,
	// иначе pgx возвращает "conn busy" (нельзя читать с открытым курсором).
	var tasks []*domain.Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("postgres: lock-next: scan: %w", err)
		}
		tasks = append(tasks, t)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: lock-next: rows: %w", err)
	}

	// Затем загружаем зависимости (соединение свободно).
	for _, t := range tasks {
		uid, _ := toUUID(t.ID)
		t.Dependencies, err = loadDeps(ctx, tx, uid)
		if err != nil {
			return nil, fmt.Errorf("postgres: lock-next: deps: %w", err)
		}
	}

	return tasks, tx.Commit(ctx)
}

// List возвращает задачи по фильтру и общее число совпадений.
func (r *Repo) List(ctx context.Context, f usecase.ListFilter) ([]*domain.Task, int, error) {
	if f.PageSize <= 0 {
		f.PageSize = 50
	}
	if f.Page <= 0 {
		f.Page = 1
	}
	offset := (f.Page - 1) * f.PageSize

	where, args := buildListWhere(f)

	countRow := r.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM tasks`+where, args...)
	var total int
	if err := countRow.Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("postgres: list: count: %w", err)
	}

	args = append(args, int32(f.PageSize), int32(offset))
	rows, err := r.pool.Query(ctx,
		`SELECT `+taskColumns+` FROM tasks`+where+
			` ORDER BY created_at DESC LIMIT $`+fmt.Sprintf("%d", len(args)-1)+
			` OFFSET $`+fmt.Sprintf("%d", len(args)),
		args...)
	if err != nil {
		return nil, 0, fmt.Errorf("postgres: list: query: %w", err)
	}
	defer rows.Close()

	var tasks []*domain.Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, 0, fmt.Errorf("postgres: list: scan: %w", err)
		}
		tasks = append(tasks, t)
	}
	if err = rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("postgres: list: rows: %w", err)
	}

	return tasks, total, nil
}

func buildListWhere(f usecase.ListFilter) (string, []any) {
	var clauses []string
	var args []any
	n := 1
	if f.Status != "" {
		clauses = append(clauses, fmt.Sprintf("status = $%d", n))
		args = append(args, string(f.Status))
		n++
	}
	if f.Type != "" {
		clauses = append(clauses, fmt.Sprintf("type = $%d", n))
		args = append(args, f.Type)
	}
	if len(clauses) == 0 {
		return "", args
	}
	return " WHERE " + strings.Join(clauses, " AND "), args
}

// PickAndMarkRunning атомарно выбирает до limit pending-задач, готовых к запуску,
// переводит их в running и возвращает. Один вызов = одна транзакция (МТ.2.1–2.4).
func (r *Repo) PickAndMarkRunning(ctx context.Context, limit int) ([]*domain.Task, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("postgres: pick-and-mark: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	rows, err := tx.Query(ctx, `
		SELECT `+taskColumns+`
		FROM tasks t
		WHERE t.status = 'pending'
		  AND t.scheduled_at <= NOW()
		  AND NOT EXISTS (
		        SELECT 1
		        FROM task_dependencies td
		        JOIN tasks dep ON dep.id = td.depends_on
		        WHERE td.task_id = t.id
		          AND dep.status != 'completed'
		  )
		ORDER BY t.priority DESC, t.scheduled_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED`,
		int32(limit))
	if err != nil {
		return nil, fmt.Errorf("postgres: pick-and-mark: query: %w", err)
	}

	var tasks []*domain.Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("postgres: pick-and-mark: scan: %w", err)
		}
		tasks = append(tasks, t)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: pick-and-mark: rows: %w", err)
	}

	// Загружаем зависимости после закрытия rows (pgx: нельзя с открытым курсором).
	for _, t := range tasks {
		uid, _ := toUUID(t.ID)
		t.Dependencies, err = loadDeps(ctx, tx, uid)
		if err != nil {
			return nil, fmt.Errorf("postgres: pick-and-mark: deps: %w", err)
		}
	}

	now := time.Now().UTC()
	for _, t := range tasks {
		uid, err := toUUID(t.ID)
		if err != nil {
			return nil, err
		}
		if _, err = tx.Exec(ctx,
			`UPDATE tasks SET status = 'running', updated_at = $2 WHERE id = $1`,
			uid, now,
		); err != nil {
			return nil, fmt.Errorf("postgres: pick-and-mark: update %s: %w", t.ID, err)
		}
		t.Status = domain.StatusRunning
		t.UpdatedAt = now
	}

	return tasks, tx.Commit(ctx)
}

// Heartbeat обновляет updated_at и worker_id для задачи в статусе running (МТ.4.1–4.4).
// Возвращает ErrNotFound, если задача не found или уже не running.
func (r *Repo) Heartbeat(ctx context.Context, id domain.TaskID, workerID string) error {
	uid, err := toUUID(id)
	if err != nil {
		return err
	}
	tag, err := r.pool.Exec(ctx,
		`UPDATE tasks SET updated_at = NOW(), worker_id = $2
		 WHERE id = $1 AND status = 'running'`,
		uid, workerID)
	if err != nil {
		return fmt.Errorf("postgres: heartbeat: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("postgres: heartbeat: %w", domain.ErrNotFound)
	}
	return nil
}

// ResetStuckRunning переводит задачи из running в pending, если их updated_at
// старше timeout — воркер умер без heartbeat (МТ.4.1–4.4).
func (r *Repo) ResetStuckRunning(ctx context.Context, timeout time.Duration) (int64, error) {
	threshold := time.Now().UTC().Add(-timeout)
	tag, err := r.pool.Exec(ctx, `
		UPDATE tasks
		SET status = 'pending', worker_id = NULL, updated_at = NOW()
		WHERE status = 'running' AND updated_at < $1`,
		threshold)
	if err != nil {
		return 0, fmt.Errorf("postgres: reset-stuck: %w", err)
	}
	return tag.RowsAffected(), nil
}

// CleanupExpired удаляет завершённые/отменённые задачи старше ttl (МТ.5.3).
func (r *Repo) CleanupExpired(ctx context.Context, ttl time.Duration) (int64, error) {
	threshold := time.Now().UTC().Add(-ttl)
	tag, err := r.pool.Exec(ctx, `
		DELETE FROM tasks
		WHERE status IN ('completed', 'cancelled')
		  AND updated_at < $1`,
		threshold)
	if err != nil {
		return 0, fmt.Errorf("postgres: cleanup: %w", err)
	}
	return tag.RowsAffected(), nil
}

// — helpers ---------------------------------------------------------------

func nullText(s string) pgtype.Text {
	if s == "" {
		return pgtype.Text{}
	}
	return pgtype.Text{String: s, Valid: true}
}

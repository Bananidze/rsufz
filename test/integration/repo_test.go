//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	repopostgres "github.com/Bananidze/rsufz/internal/adapter/repo/postgres"
	"github.com/Bananidze/rsufz/internal/domain"
	"github.com/Bananidze/rsufz/internal/platform/migrate"
	"github.com/Bananidze/rsufz/internal/usecase"
)

// setupDB поднимает postgres-контейнер, применяет миграции и возвращает готовый пул.
func setupDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	ctr, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("rsufz_test"),
		tcpostgres.WithUsername("rsufz"),
		tcpostgres.WithPassword("rsufz"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ctr.Terminate(ctx) })

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	sqlDB, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	require.NoError(t, migrate.Up(ctx, sqlDB))
	_ = sqlDB.Close()

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	return pool
}

func newTaskID() domain.TaskID {
	return domain.TaskID(uuid.Must(uuid.NewV7()).String())
}

func newTask(typ string, priority domain.Priority) *domain.Task {
	now := time.Now().UTC().Truncate(time.Microsecond)
	return &domain.Task{
		ID:          newTaskID(),
		Type:        typ,
		Payload:     []byte(`{"k":"v"}`),
		Priority:    priority,
		Status:      domain.StatusPending,
		ScheduledAt: now,
		RetryLimit:  3,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

// МТ.5.1 — сохранение и получение задачи по ID.
func TestRepo_CreateAndGet(t *testing.T) {
	t.Parallel()
	repo := repopostgres.New(setupDB(t))
	ctx := context.Background()

	task := newTask("send_email", 5)
	require.NoError(t, repo.Create(ctx, task))

	got, err := repo.GetByID(ctx, task.ID)
	require.NoError(t, err)

	assert.Equal(t, task.ID, got.ID)
	assert.Equal(t, task.Type, got.Type)
	assert.Equal(t, task.Priority, got.Priority)
	assert.Equal(t, domain.StatusPending, got.Status)
	assert.Equal(t, task.RetryLimit, got.RetryLimit)
}

// МТ.5.1 — GetByID возвращает ErrNotFound для несуществующего ID.
func TestRepo_GetByID_NotFound(t *testing.T) {
	t.Parallel()
	repo := repopostgres.New(setupDB(t))
	ctx := context.Background()

	_, err := repo.GetByID(ctx, newTaskID())
	assert.ErrorIs(t, err, domain.ErrNotFound)
}

// МТ.5.2 — атомарное обновление статуса через UpdateTask.
func TestRepo_UpdateTask_StatusTransition(t *testing.T) {
	t.Parallel()
	repo := repopostgres.New(setupDB(t))
	ctx := context.Background()

	task := newTask("generate_report", 3)
	require.NoError(t, repo.Create(ctx, task))

	err := repo.UpdateTask(ctx, task.ID, func(t *domain.Task) error {
		return t.TransitionTo(domain.StatusRunning)
	})
	require.NoError(t, err)

	got, err := repo.GetByID(ctx, task.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusRunning, got.Status)
}

// МТ.7.2 — UpdateTask не применяет недопустимый переход (completed → running).
func TestRepo_UpdateTask_InvalidTransition(t *testing.T) {
	t.Parallel()
	repo := repopostgres.New(setupDB(t))
	ctx := context.Background()

	task := newTask("proc", 1)
	task.Status = domain.StatusCompleted
	require.NoError(t, repo.Create(ctx, task))

	err := repo.UpdateTask(ctx, task.ID, func(t *domain.Task) error {
		return t.TransitionTo(domain.StatusRunning)
	})
	assert.ErrorIs(t, err, domain.ErrInvalidStateTransition)

	got, err := repo.GetByID(ctx, task.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusCompleted, got.Status)
}

// МТ.6.3 — FindByIdempotencyKey возвращает существующую задачу.
func TestRepo_FindByIdempotencyKey(t *testing.T) {
	t.Parallel()
	repo := repopostgres.New(setupDB(t))
	ctx := context.Background()

	task := newTask("send_sms", 2)
	task.IdempotencyKey = "idem-key-abc"
	require.NoError(t, repo.Create(ctx, task))

	got, err := repo.FindByIdempotencyKey(ctx, "idem-key-abc")
	require.NoError(t, err)
	assert.Equal(t, task.ID, got.ID)

	_, err = repo.FindByIdempotencyKey(ctx, "idem-key-xyz")
	assert.ErrorIs(t, err, domain.ErrNotFound)
}

// МТ.2.1 — LockNextPending выбирает задачи в порядке убывания приоритета.
func TestRepo_LockNextPending_Priority(t *testing.T) {
	t.Parallel()
	repo := repopostgres.New(setupDB(t))
	ctx := context.Background()

	lo := newTask("low", 1)
	hi := newTask("high", 9)
	require.NoError(t, repo.Create(ctx, lo))
	require.NoError(t, repo.Create(ctx, hi))

	tasks, err := repo.LockNextPending(ctx, 2)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	assert.Equal(t, hi.ID, tasks[0].ID, "высокий приоритет должен быть первым")
}

// МТ.2.4 — LockNextPending не выбирает задачу, если зависимость не completed.
func TestRepo_LockNextPending_DAG(t *testing.T) {
	t.Parallel()
	repo := repopostgres.New(setupDB(t))
	ctx := context.Background()

	dep := newTask("dep", 5)
	require.NoError(t, repo.Create(ctx, dep))

	child := newTask("child", 5)
	child.Dependencies = []domain.TaskID{dep.ID}
	require.NoError(t, repo.Create(ctx, child))

	// dep ещё pending → child не должен появиться в выборке
	tasks, err := repo.LockNextPending(ctx, 10)
	require.NoError(t, err)
	for _, tsk := range tasks {
		assert.NotEqual(t, child.ID, tsk.ID, "child не должен выбираться пока dep не completed")
	}

	// завершаем dep: pending → running → completed (state machine не пропускает pending → completed)
	require.NoError(t, repo.UpdateTask(ctx, dep.ID, func(tsk *domain.Task) error {
		if err := tsk.TransitionTo(domain.StatusRunning); err != nil {
			return err
		}
		return tsk.TransitionTo(domain.StatusCompleted)
	}))

	// теперь child должен появиться
	tasks, err = repo.LockNextPending(ctx, 10)
	require.NoError(t, err)
	found := false
	for _, tsk := range tasks {
		if tsk.ID == child.ID {
			found = true
		}
	}
	assert.True(t, found, "child должен выбираться после завершения dep")
}

// МТ.5.3 — CleanupExpired удаляет завершённые задачи старше ttl.
func TestRepo_CleanupExpired(t *testing.T) {
	t.Parallel()
	repo := repopostgres.New(setupDB(t))
	ctx := context.Background()

	old := newTask("old", 1)
	old.Status = domain.StatusCompleted
	old.UpdatedAt = time.Now().UTC().Add(-2 * time.Hour)
	require.NoError(t, repo.Create(ctx, old))

	fresh := newTask("fresh", 1)
	fresh.Status = domain.StatusCompleted
	require.NoError(t, repo.Create(ctx, fresh))

	n, err := repo.CleanupExpired(ctx, time.Hour)
	require.NoError(t, err)
	assert.EqualValues(t, 1, n)

	_, err = repo.GetByID(ctx, old.ID)
	assert.ErrorIs(t, err, domain.ErrNotFound)

	_, err = repo.GetByID(ctx, fresh.ID)
	assert.NoError(t, err)
}

// МТ.5.4 — List возвращает задачи по фильтру с пагинацией.
func TestRepo_List(t *testing.T) {
	t.Parallel()
	repo := repopostgres.New(setupDB(t))
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		require.NoError(t, repo.Create(ctx, newTask(fmt.Sprintf("type-%d", i), 5)))
	}
	completed := newTask("done", 5)
	completed.Status = domain.StatusCompleted
	require.NoError(t, repo.Create(ctx, completed))

	tasks, total, err := repo.List(ctx, usecase.ListFilter{
		Status: domain.StatusPending, Page: 1, PageSize: 3,
	})
	require.NoError(t, err)
	assert.Len(t, tasks, 3)
	assert.Equal(t, 5, total)
}

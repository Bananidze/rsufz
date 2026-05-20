package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/Bananidze/rsufz/internal/domain"
)

// EnqueueCmd — входные данные для постановки задачи в очередь.
type EnqueueCmd struct {
	Type           string
	Payload        []byte          // должен быть валидным JSON (если не пустой)
	Priority       domain.Priority // 0..10 (МТ.1.5–1.7)
	Dependencies   []domain.TaskID
	ScheduledAt    time.Time // zero = запустить немедленно
	RetryLimit     int
	IdempotencyKey string // ключ дедупликации (МТ.6.3)
}

// Validate проверяет поля команды до обращения к хранилищу (МТ.1.2–1.4, МТ.1.7).
func (c EnqueueCmd) Validate() error {
	if strings.TrimSpace(c.Type) == "" {
		return fmt.Errorf("%w: field 'type' is required", domain.ErrInvalidArgument)
	}
	if len(c.Payload) > 0 && !json.Valid(c.Payload) {
		return fmt.Errorf("%w: field 'payload' must be valid JSON", domain.ErrInvalidArgument)
	}
	return c.Priority.Validate()
}

// EnqueueUseCase регистрирует новые фоновые задачи.
type EnqueueUseCase struct {
	repo       TaskRepository
	clock      Clock
	ids        IDGenerator
	metrics    Metrics
	knownTypes map[string]struct{} // если не пусто — принимаем только эти типы (МТ.1.3)
	log        *slog.Logger
}

// NewEnqueue создаёт use case. knownTypes — список допустимых типов задач;
// передайте nil/empty чтобы принимать любые типы.
func NewEnqueue(repo TaskRepository, clock Clock, ids IDGenerator, metrics Metrics, log *slog.Logger, knownTypes ...string) *EnqueueUseCase {
	kt := make(map[string]struct{}, len(knownTypes))
	for _, t := range knownTypes {
		kt[t] = struct{}{}
	}
	return &EnqueueUseCase{repo: repo, clock: clock, ids: ids, metrics: metrics, knownTypes: kt, log: log}
}

// Handle обрабатывает запрос постановки задачи.
// Алгоритм из ПЗ §«Алгоритм приёма и регистрации задачи» (рис. 2.1).
func (u *EnqueueUseCase) Handle(ctx context.Context, cmd EnqueueCmd) (domain.TaskID, error) {
	start := u.clock.Now()
	ctx, span := otel.Tracer("rsufz").Start(ctx, "enqueue")
	defer span.End()

	// 1. Валидация полей
	if err := cmd.Validate(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}

	// 2. Проверка допустимости типа задачи (МТ.1.3)
	if len(u.knownTypes) > 0 {
		if _, ok := u.knownTypes[cmd.Type]; !ok {
			return "", fmt.Errorf("%w: unknown task type %q", domain.ErrInvalidArgument, cmd.Type)
		}
	}

	// 3. Идемпотентность: повторный запрос с тем же ключом → возвращаем существующий ID (МТ.6.3)
	if cmd.IdempotencyKey != "" {
		if existing, err := u.repo.FindByIdempotencyKey(ctx, cmd.IdempotencyKey); err == nil {
			u.log.DebugContext(ctx, "enqueue: idempotent hit",
				slog.String("key", cmd.IdempotencyKey),
				slog.String("task_id", string(existing.ID)),
			)
			return existing.ID, nil
		} else if !errors.Is(err, domain.ErrNotFound) {
			return "", fmt.Errorf("enqueue: idempotency check: %w", err)
		}
	}

	// 4. Создание задачи
	now := u.clock.Now()
	scheduledAt := cmd.ScheduledAt
	if scheduledAt.IsZero() {
		scheduledAt = now
	}

	task := &domain.Task{
		ID:             u.ids.New(),
		Type:           cmd.Type,
		Payload:        cmd.Payload,
		Priority:       cmd.Priority,
		Status:         domain.StatusPending,
		Dependencies:   cmd.Dependencies,
		ScheduledAt:    scheduledAt,
		RetryLimit:     cmd.RetryLimit,
		IdempotencyKey: cmd.IdempotencyKey,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	// 5. Сохранение в БД со статусом pending
	if err := u.repo.Create(ctx, task); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", fmt.Errorf("enqueue: create: %w", err)
	}

	latency := u.clock.Now().Sub(start)
	span.SetAttributes(
		attribute.String("task.id", string(task.ID)),
		attribute.String("task.type", task.Type),
		attribute.Int("task.priority", int(task.Priority)),
	)
	span.SetStatus(codes.Ok, "")
	u.metrics.TaskEnqueued(latency)

	u.log.InfoContext(ctx, "task enqueued",
		slog.String("task_id", string(task.ID)),
		slog.String("type", task.Type),
		slog.Int("priority", int(task.Priority)),
		slog.Duration("latency", latency),
	)
	return task.ID, nil
}

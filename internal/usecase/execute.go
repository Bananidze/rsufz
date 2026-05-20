package usecase

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/Bananidze/rsufz/internal/domain"
)

const (
	heartbeatInterval = 5 * time.Second
	taskStreamConst   = "rsufz:tasks"
	workerGroupName   = "rsufz:workers"
	dlqStream         = "rsufz:dlq"
)

// ExecuteUseCase обрабатывает задачи, поступающие от планировщика через Redis Stream.
type ExecuteUseCase struct {
	repo     TaskRepository
	broker   Broker
	registry *Registry
	clock    Clock
	metrics  Metrics
	workerID string
	log      *slog.Logger
}

// NewExecute создаёт use case исполнения задач.
func NewExecute(
	repo TaskRepository,
	broker Broker,
	registry *Registry,
	clock Clock,
	metrics Metrics,
	workerID string,
	log *slog.Logger,
) *ExecuteUseCase {
	return &ExecuteUseCase{
		repo:     repo,
		broker:   broker,
		registry: registry,
		clock:    clock,
		metrics:  metrics,
		workerID: workerID,
		log:      log,
	}
}

// Run подписывается на Redis Stream и обрабатывает задачи до ctx.Done().
func (u *ExecuteUseCase) Run(ctx context.Context) error {
	ch, err := u.broker.Subscribe(ctx, taskStreamConst, workerGroupName, u.workerID)
	if err != nil {
		return fmt.Errorf("execute: subscribe: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case d, ok := <-ch:
			if !ok {
				return nil
			}
			u.process(ctx, d)
		}
	}
}

func (u *ExecuteUseCase) process(ctx context.Context, d Delivery) {
	ctx, span := otel.Tracer("rsufz").Start(ctx, "execute")
	defer span.End()

	log := u.log.With(slog.String("task_id", string(d.TaskID)), slog.String("msg_id", d.ID))
	start := u.clock.Now()

	task, err := u.repo.GetByID(ctx, d.TaskID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		log.ErrorContext(ctx, "execute: get task", slog.Any("err", err))
		return
	}
	span.SetAttributes(
		attribute.String("task.id", string(task.ID)),
		attribute.String("task.type", task.Type),
		attribute.Int("task.priority", int(task.Priority)),
	)

	// Идемпотентность: задача уже завершена (МТ.6.3)
	if task.Status == domain.StatusCompleted || task.Status == domain.StatusCancelled {
		log.DebugContext(ctx, "execute: task already done, ack only")
		_ = u.broker.Ack(ctx, taskStreamConst, workerGroupName, d.ID)
		return
	}

	handler, err := u.registry.Get(task.Type)
	if err != nil {
		log.ErrorContext(ctx, "execute: no handler", slog.String("type", task.Type))
		u.markFailed(ctx, task, fmt.Sprintf("no handler for type %q", task.Type), d.ID)
		return
	}

	// Запускаем выполнение с heartbeat в отдельной горутине.
	taskCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	resultCh := make(chan execResult, 1)
	go func() {
		result, err := handler(taskCtx, task.Payload)
		resultCh <- execResult{result: result, err: err}
	}()

	hbTicker := time.NewTicker(heartbeatInterval)
	defer hbTicker.Stop()

	var res execResult
	for {
		select {
		case <-hbTicker.C:
			if hbErr := u.repo.Heartbeat(ctx, task.ID, u.workerID); hbErr != nil {
				log.WarnContext(ctx, "execute: heartbeat failed", slog.Any("err", hbErr))
			}
		case res = <-resultCh:
			goto done
		case <-ctx.Done():
			cancel()
			res = execResult{err: ctx.Err()}
			goto done
		}
	}
done:

	if res.err != nil {
		span.RecordError(res.err)
		span.SetStatus(codes.Error, res.err.Error())
		task.IncrementAttempt()
		if task.CanRetry() && !errors.Is(res.err, context.Canceled) {
			u.metrics.TaskRetried()
			u.markRetry(ctx, task, res.err, d.ID)
		} else {
			u.metrics.TaskFailed()
			u.markFailed(ctx, task, res.err.Error(), d.ID)
		}
		return
	}

	span.SetStatus(codes.Ok, "")
	u.metrics.TaskCompleted(u.clock.Now().Sub(start))
	u.markCompleted(ctx, task, res.result, d.ID)
}

type execResult struct {
	result []byte
	err    error
}

func (u *ExecuteUseCase) markCompleted(ctx context.Context, task *domain.Task, result []byte, msgID string) {
	err := u.repo.UpdateTask(ctx, task.ID, func(t *domain.Task) error {
		if err := t.TransitionTo(domain.StatusCompleted); err != nil {
			return err
		}
		t.Result = result
		t.WorkerID = u.workerID
		return nil
	})
	if err != nil {
		u.log.ErrorContext(ctx, "execute: mark completed", slog.Any("err", err))
		return
	}
	_ = u.broker.Ack(ctx, taskStreamConst, workerGroupName, msgID)
	u.log.InfoContext(ctx, "execute: completed", slog.String("task_id", string(task.ID)))
}

func (u *ExecuteUseCase) markRetry(ctx context.Context, task *domain.Task, taskErr error, msgID string) {
	backoff := domain.DefaultBackoff()
	nextAt := u.clock.Now().Add(backoff.Delay(task.AttemptCount))

	err := u.repo.UpdateTask(ctx, task.ID, func(t *domain.Task) error {
		if err := t.TransitionTo(domain.StatusPending); err != nil {
			return err
		}
		t.AttemptCount = task.AttemptCount
		t.LastError = taskErr.Error()
		t.ScheduledAt = nextAt
		t.WorkerID = ""
		return nil
	})
	if err != nil {
		u.log.ErrorContext(ctx, "execute: mark retry", slog.Any("err", err))
		return
	}
	_ = u.broker.Ack(ctx, taskStreamConst, workerGroupName, msgID)
	u.log.WarnContext(ctx, "execute: retry scheduled",
		slog.String("task_id", string(task.ID)),
		slog.Int("attempt", task.AttemptCount),
		slog.Time("next_at", nextAt),
	)
}

func (u *ExecuteUseCase) markFailed(ctx context.Context, task *domain.Task, errMsg string, msgID string) {
	err := u.repo.UpdateTask(ctx, task.ID, func(t *domain.Task) error {
		if err := t.TransitionTo(domain.StatusFailed); err != nil {
			return err
		}
		t.LastError = errMsg
		t.WorkerID = ""
		return nil
	})
	if err != nil {
		u.log.ErrorContext(ctx, "execute: mark failed", slog.Any("err", err))
		return
	}
	// Публикуем в DLQ для ручного анализа (МТ.3.5)
	_ = u.broker.Publish(ctx, dlqStream, task.ID)
	_ = u.broker.Ack(ctx, taskStreamConst, workerGroupName, msgID)
	u.log.ErrorContext(ctx, "execute: task failed, sent to DLQ",
		slog.String("task_id", string(task.ID)),
		slog.String("error", errMsg),
	)
}

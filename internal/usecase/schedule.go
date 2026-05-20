package usecase

import (
	"context"
	"log/slog"
	"time"
)

const (
	defaultPollInterval = 100 * time.Millisecond
	defaultBatchSize    = 50
	taskStream          = "rsufz:tasks"
)

// ScheduleUseCase — планировщик: поллит БД, переводит задачи в running, пушит в Redis.
// Мониторинг зависших задач (heartbeat) вынесен в HeartbeatUseCase.
type ScheduleUseCase struct {
	repo         TaskRepository
	broker       Broker
	metrics      Metrics
	pollInterval time.Duration
	batchSize    int
	log          *slog.Logger
}

// NewSchedule создаёт планировщик с опциональными настройками.
func NewSchedule(repo TaskRepository, broker Broker, metrics Metrics, log *slog.Logger, opts ...ScheduleOption) *ScheduleUseCase {
	s := &ScheduleUseCase{
		repo:         repo,
		broker:       broker,
		metrics:      metrics,
		pollInterval: defaultPollInterval,
		batchSize:    defaultBatchSize,
		log:          log,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// ScheduleOption позволяет настраивать планировщик.
type ScheduleOption func(*ScheduleUseCase)

func WithPollInterval(d time.Duration) ScheduleOption {
	return func(s *ScheduleUseCase) { s.pollInterval = d }
}

func WithBatchSize(n int) ScheduleOption {
	return func(s *ScheduleUseCase) { s.batchSize = n }
}

// Loop запускает основной цикл планировщика. Блокируется до ctx.Done().
// Запускать через errgroup или отдельную горутину.
// Мониторинг зависших задач — отдельная горутина с HeartbeatUseCase.Run.
func (s *ScheduleUseCase) Loop(ctx context.Context) error {
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := s.tick(ctx); err != nil {
				s.log.ErrorContext(ctx, "scheduler: tick error", slog.Any("err", err))
			}
		}
	}
}

func (s *ScheduleUseCase) tick(ctx context.Context) error {
	tasks, err := s.repo.PickAndMarkRunning(ctx, s.batchSize)
	if err != nil {
		return err
	}
	for _, t := range tasks {
		if err := s.broker.Publish(ctx, taskStream, t.ID); err != nil {
			// задача уже в running; heartbeat-монитор вернёт её в pending если воркер не заберёт
			s.log.ErrorContext(ctx, "scheduler: publish failed",
				slog.String("task_id", string(t.ID)),
				slog.Any("err", err),
			)
			continue
		}
		s.metrics.TaskDispatched()
		s.log.DebugContext(ctx, "scheduler: dispatched",
			slog.String("task_id", string(t.ID)),
			slog.String("type", t.Type),
			slog.Int("priority", int(t.Priority)),
		)
	}
	return nil
}

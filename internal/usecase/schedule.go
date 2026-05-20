package usecase

import (
	"context"
	"log/slog"
	"time"
)

const (
	defaultPollInterval   = 100 * time.Millisecond
	defaultBatchSize      = 50
	defaultHeartbeatTimeout = 30 * time.Second
	taskStream            = "rsufz:tasks"
)

// ScheduleUseCase — планировщик: поллит БД, переводит задачи в running, пушит в Redis.
type ScheduleUseCase struct {
	repo         TaskRepository
	broker       Broker
	pollInterval time.Duration
	batchSize    int
	hbTimeout    time.Duration
	log          *slog.Logger
}

// NewSchedule создаёт планировщик с опциональными настройками.
func NewSchedule(repo TaskRepository, broker Broker, log *slog.Logger, opts ...ScheduleOption) *ScheduleUseCase {
	s := &ScheduleUseCase{
		repo:         repo,
		broker:       broker,
		pollInterval: defaultPollInterval,
		batchSize:    defaultBatchSize,
		hbTimeout:    defaultHeartbeatTimeout,
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

func WithHeartbeatTimeout(d time.Duration) ScheduleOption {
	return func(s *ScheduleUseCase) { s.hbTimeout = d }
}

// Loop запускает основной цикл планировщика. Блокируется до ctx.Done().
// Запускать через errgroup или отдельную горутину.
func (s *ScheduleUseCase) Loop(ctx context.Context) error {
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	hbTicker := time.NewTicker(s.hbTimeout / 2)
	defer hbTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-hbTicker.C:
			n, err := s.repo.ResetStuckRunning(ctx, s.hbTimeout)
			if err != nil {
				s.log.ErrorContext(ctx, "scheduler: reset stuck running", slog.Any("err", err))
			} else if n > 0 {
				s.log.InfoContext(ctx, "scheduler: reset stuck tasks", slog.Int64("count", n))
			}

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
		s.log.DebugContext(ctx, "scheduler: dispatched",
			slog.String("task_id", string(t.ID)),
			slog.String("type", t.Type),
			slog.Int("priority", int(t.Priority)),
		)
	}
	return nil
}

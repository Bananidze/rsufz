package usecase

import (
	"context"
	"log/slog"
	"time"
)

// HeartbeatUseCase следит за «живостью» воркеров через БД.
//
// Воркер (ExecuteUseCase) периодически вызывает repo.Heartbeat, обновляя updated_at
// задачи в статусе running. HeartbeatUseCase запускается в планировщике и сбрасывает
// задачи, чей updated_at устарел — это значит воркер умер (МТ.4.1–4.4).
type HeartbeatUseCase struct {
	repo     TaskRepository
	timeout  time.Duration // если updated_at старше timeout → воркер мёртв
	interval time.Duration // как часто проверять (обычно timeout/2)
	log      *slog.Logger
}

// NewHeartbeat создаёт монитор сердцебиения.
// timeout — максимальный возраст heartbeat'а; interval — период проверки.
func NewHeartbeat(repo TaskRepository, timeout, interval time.Duration, log *slog.Logger) *HeartbeatUseCase {
	return &HeartbeatUseCase{
		repo:     repo,
		timeout:  timeout,
		interval: interval,
		log:      log,
	}
}

// Run запускает цикл мониторинга. Блокируется до ctx.Done().
// Интегрируется в errgroup планировщика.
func (h *HeartbeatUseCase) Run(ctx context.Context) error {
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			h.check(ctx)
		}
	}
}

func (h *HeartbeatUseCase) check(ctx context.Context) {
	n, err := h.repo.ResetStuckRunning(ctx, h.timeout)
	if err != nil {
		h.log.ErrorContext(ctx, "heartbeat: reset stuck tasks", slog.Any("err", err))
		return
	}
	if n > 0 {
		h.log.WarnContext(ctx, "heartbeat: reset stuck tasks", slog.Int64("count", n))
	}
}

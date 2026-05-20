// Бинарник воркера РСУФЗ.
// Читает задачи из Redis Stream, исполняет через зарегистрированные хендлеры.
//
// Добавление хендлера:
//
//	registry.Register("send_email", func(ctx context.Context, payload []byte) ([]byte, error) {
//	    // бизнес-логика
//	    return nil, nil
//	})
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"

	"github.com/Bananidze/rsufz/internal/app"
	"github.com/Bananidze/rsufz/internal/platform/config"
	"github.com/Bananidze/rsufz/internal/platform/logger"
	"github.com/Bananidze/rsufz/internal/platform/shutdown"
	"github.com/Bananidze/rsufz/internal/usecase"
)

func main() {
	cfg := config.LoadWorker()
	log := logger.New(cfg.LogLevel)

	// Регистрируем обработчики задач. В реальном проекте — из отдельного пакета handler/.
	registry := usecase.NewRegistry()
	registry.Register("noop", func(_ context.Context, _ []byte) ([]byte, error) {
		return nil, nil
	})

	ctx, stop := shutdown.NotifyContext(context.Background())
	defer stop()

	log.Info("rsufz worker starting", slog.String("worker_id", cfg.WorkerID))

	if err := app.RunWorker(ctx, cfg, registry, log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("worker exited with error", slog.Any("err", err))
		os.Exit(1)
	}
}

// Бинарник планировщика РСУФЗ.
// Поллит PostgreSQL, переводит pending-задачи в running, пушит в Redis Stream.
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
)

func main() {
	cfg := config.LoadScheduler()
	log := logger.New(cfg.LogLevel)

	ctx, stop := shutdown.NotifyContext(context.Background())
	defer stop()

	log.Info("rsufz scheduler starting",
		slog.String("redis", cfg.RedisAddr),
		slog.Duration("poll_interval", cfg.PollInterval),
	)

	if err := app.RunScheduler(ctx, cfg, log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("scheduler exited with error", slog.Any("err", err))
		os.Exit(1)
	}
}

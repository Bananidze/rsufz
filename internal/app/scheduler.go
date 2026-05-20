package app

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"

	redisbroker "github.com/Bananidze/rsufz/internal/adapter/broker/redis"
	repopostgres "github.com/Bananidze/rsufz/internal/adapter/repo/postgres"
	"github.com/Bananidze/rsufz/internal/platform/config"
	"github.com/Bananidze/rsufz/internal/usecase"
)

// RunScheduler собирает зависимости планировщика и запускает цикл опроса БД.
// Блокируется до ctx.Done() или первой ошибки.
func RunScheduler(ctx context.Context, cfg config.Scheduler, log *slog.Logger) error {
	pool, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		return fmt.Errorf("app/scheduler: pgxpool: %w", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("app/scheduler: postgres ping: %w", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("app/scheduler: redis ping: %w", err)
	}

	repo := repopostgres.New(pool)
	broker := redisbroker.New(rdb)

	scheduler := usecase.NewSchedule(repo, broker, log,
		usecase.WithPollInterval(cfg.PollInterval),
		usecase.WithBatchSize(cfg.BatchSize),
		usecase.WithHeartbeatTimeout(cfg.HBTimeout),
	)

	grp, ctx := errgroup.WithContext(ctx)
	grp.Go(func() error { return scheduler.Loop(ctx) })
	return grp.Wait()
}

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

// RunWorker собирает зависимости воркера и запускает цикл обработки задач.
// registry заполняется в cmd/worker/main.go конкретными хендлерами бизнес-логики.
// Блокируется до ctx.Done() или первой ошибки.
func RunWorker(ctx context.Context, cfg config.Worker, registry *usecase.Registry, log *slog.Logger) error {
	pool, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		return fmt.Errorf("app/worker: pgxpool: %w", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("app/worker: postgres ping: %w", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("app/worker: redis ping: %w", err)
	}

	repo := repopostgres.New(pool)
	broker := redisbroker.New(rdb)
	clock := usecase.SystemClock{}

	execute := usecase.NewExecute(repo, broker, registry, clock, cfg.WorkerID, log)

	grp, ctx := errgroup.WithContext(ctx)
	grp.Go(func() error { return execute.Run(ctx) })
	return grp.Wait()
}

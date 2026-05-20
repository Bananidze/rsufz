// Package app содержит wiring-функции — сборку зависимостей для каждого бинарника.
package app

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"

	"github.com/Bananidze/rsufz/internal/adapter/grpcserver"
	repopostgres "github.com/Bananidze/rsufz/internal/adapter/repo/postgres"
	"github.com/Bananidze/rsufz/internal/platform/config"
	"github.com/Bananidze/rsufz/internal/platform/ids"
	"github.com/Bananidze/rsufz/internal/usecase"
)

// RunAPIGateway собирает зависимости API-шлюза и запускает gRPC-сервер.
// Блокируется до ctx.Done() или первой ошибки.
func RunAPIGateway(ctx context.Context, cfg config.APIGateway, log *slog.Logger) error {
	pool, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		return fmt.Errorf("app/apigateway: pgxpool: %w", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("app/apigateway: postgres ping: %w", err)
	}

	repo := repopostgres.New(pool)
	clock := usecase.SystemClock{}
	gen := ids.UUIDv7Gen{}

	enqueue := usecase.NewEnqueue(repo, clock, gen, log)
	get := usecase.NewGet(repo)
	cancel := usecase.NewCancel(repo)
	republish := usecase.NewRepublish(repo)
	list := usecase.NewList(repo)

	svc := grpcserver.New(enqueue, get, cancel, republish, list)

	grp, ctx := errgroup.WithContext(ctx)
	grp.Go(func() error { return grpcserver.Serve(ctx, cfg.GRPCAddr, svc, log) })
	return grp.Wait()
}

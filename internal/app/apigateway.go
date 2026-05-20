// Package app содержит wiring-функции — сборку зависимостей для каждого бинарника.
package app

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	"github.com/Bananidze/rsufz/internal/adapter/grpcserver"
	prommetrics "github.com/Bananidze/rsufz/internal/adapter/metrics/prom"
	repopostgres "github.com/Bananidze/rsufz/internal/adapter/repo/postgres"
	tracing "github.com/Bananidze/rsufz/internal/adapter/trace/otel"
	"github.com/Bananidze/rsufz/internal/platform/config"
	"github.com/Bananidze/rsufz/internal/platform/ids"
	"github.com/Bananidze/rsufz/internal/usecase"
)

// RunAPIGateway собирает зависимости API-шлюза и запускает gRPC-сервер.
// Блокируется до ctx.Done() или первой ошибки.
func RunAPIGateway(ctx context.Context, cfg config.APIGateway, log *slog.Logger) error {
	// Tracing
	shutdownTrace, err := tracing.Setup(ctx, cfg.OTLPEndpoint)
	if err != nil {
		return fmt.Errorf("app/apigateway: otel: %w", err)
	}
	defer shutdownTrace(context.Background()) //nolint:errcheck

	// Metrics
	reg := prometheus.NewRegistry()
	metrics := prommetrics.New(reg)

	// Database
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

	enqueue := usecase.NewEnqueue(repo, clock, gen, metrics, log)
	get := usecase.NewGet(repo)
	cancel := usecase.NewCancel(repo)
	republish := usecase.NewRepublish(repo)
	list := usecase.NewList(repo)

	svc := grpcserver.New(enqueue, get, cancel, republish, list)

	grp, ctx := errgroup.WithContext(ctx)
	grp.Go(func() error { return grpcserver.Serve(ctx, cfg.GRPCAddr, svc, log) })
	grp.Go(func() error { return prommetrics.Serve(ctx, cfg.MetricsAddr, reg) })
	return grp.Wait()
}

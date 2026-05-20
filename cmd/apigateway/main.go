// Бинарник API-шлюза РСУФЗ.
// Принимает gRPC-запросы клиентов и проксирует их в use cases.
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
	cfg := config.LoadAPIGateway()
	log := logger.New(cfg.LogLevel)

	ctx, stop := shutdown.NotifyContext(context.Background())
	defer stop()

	log.Info("rsufz apigateway starting", slog.String("addr", cfg.GRPCAddr))

	if err := app.RunAPIGateway(ctx, cfg, log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("apigateway exited with error", slog.Any("err", err))
		os.Exit(1)
	}
}

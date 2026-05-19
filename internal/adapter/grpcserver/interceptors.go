package grpcserver

import (
	"context"
	"log/slog"
	"runtime/debug"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RecoveryInterceptor перехватывает panic в обработчиках и возвращает codes.Internal
// вместо того, чтобы уронить весь сервер.
func RecoveryInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				log.ErrorContext(ctx, "grpc: panic recovered",
					slog.String("method", info.FullMethod),
					slog.Any("panic", r),
					slog.String("stack", string(debug.Stack())),
				)
				err = status.Error(codes.Internal, "internal server error")
			}
		}()
		return handler(ctx, req)
	}
}

// LoggingInterceptor логирует каждый входящий RPC: метод, статус и длительность.
func LoggingInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		dur := time.Since(start)

		code := codes.OK
		if err != nil {
			code = status.Code(err)
		}

		lvl := slog.LevelInfo
		if code != codes.OK && code != codes.NotFound && code != codes.InvalidArgument {
			lvl = slog.LevelError
		}

		log.Log(ctx, lvl, "grpc",
			slog.String("method", info.FullMethod),
			slog.String("code", code.String()),
			slog.Duration("duration", dur),
		)
		return resp, err
	}
}

// ChainUnaryInterceptors объединяет несколько interceptors в один (от первого к последнему).
func ChainUnaryInterceptors(interceptors ...grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		current := handler
		for i := len(interceptors) - 1; i >= 0; i-- {
			i := i
			prev := current
			current = func(ctx context.Context, req any) (any, error) {
				return interceptors[i](ctx, req, info, prev)
			}
		}
		return current(ctx, req)
	}
}

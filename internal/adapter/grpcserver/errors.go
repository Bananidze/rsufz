package grpcserver

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Bananidze/rsufz/internal/domain"
)

// domainToGRPC преобразует доменную ошибку в gRPC-статус.
// Незнакомые ошибки становятся codes.Internal (детали не раскрываются клиенту).
func domainToGRPC(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, domain.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())

	case errors.Is(err, domain.ErrInvalidArgument),
		errors.Is(err, domain.ErrInvalidPriority),
		errors.Is(err, domain.ErrCyclicDependency):
		return status.Error(codes.InvalidArgument, err.Error())

	case errors.Is(err, domain.ErrInvalidStateTransition):
		return status.Error(codes.FailedPrecondition, err.Error())

	case errors.Is(err, domain.ErrDuplicateTask):
		return status.Error(codes.AlreadyExists, err.Error())

	default:
		// Внутренние ошибки (БД, брокер) — логируем в interceptor, клиенту generic сообщение.
		return status.Error(codes.Internal, "internal server error")
	}
}

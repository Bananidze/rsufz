// Package grpcserver реализует gRPC-адаптер поверх use cases РСУФЗ.
// Задача адаптера: маппинг proto ↔ domain, трансляция ошибок, запуск сервера.
package grpcserver

import (
	"context"
	"io"
	"log/slog"
	"net"
	"time"

	"google.golang.org/grpc"

	rsufzv1 "github.com/Bananidze/rsufz/gen/go/rsufz/v1"
	"github.com/Bananidze/rsufz/internal/domain"
	"github.com/Bananidze/rsufz/internal/usecase"
)

// Server реализует rsufzv1.TaskServiceServer.
type Server struct {
	rsufzv1.UnimplementedTaskServiceServer

	enqueue   *usecase.EnqueueUseCase
	get       *usecase.GetUseCase
	cancel    *usecase.CancelUseCase
	republish *usecase.RepublishUseCase
	list      *usecase.ListUseCase
}

// New создаёт gRPC-сервер с инжектированными use cases.
func New(
	enqueue *usecase.EnqueueUseCase,
	get *usecase.GetUseCase,
	cancel *usecase.CancelUseCase,
	republish *usecase.RepublishUseCase,
	list *usecase.ListUseCase,
) *Server {
	return &Server{
		enqueue:   enqueue,
		get:       get,
		cancel:    cancel,
		republish: republish,
		list:      list,
	}
}

// Serve запускает gRPC-сервер и блокируется до закрытия ctx.
func Serve(ctx context.Context, addr string, svc *Server, log *slog.Logger) error {
	srv := grpc.NewServer(
		grpc.UnaryInterceptor(ChainUnaryInterceptors(
			RecoveryInterceptor(log),
			LoggingInterceptor(log),
		)),
	)
	rsufzv1.RegisterTaskServiceServer(srv, svc)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()

	return srv.Serve(ln)
}

// --- TaskService handlers ---

func (s *Server) EnqueueTask(ctx context.Context, req *rsufzv1.EnqueueTaskRequest) (*rsufzv1.EnqueueTaskResponse, error) {
	cmd := usecase.EnqueueCmd{
		Type:           req.Type,
		Payload:        req.Payload,
		Priority:       domain.Priority(req.Priority),
		RetryLimit:     int(req.RetryLimit),
		IdempotencyKey: req.IdempotencyKey,
	}
	for _, id := range req.DependencyIds {
		cmd.Dependencies = append(cmd.Dependencies, domain.TaskID(id))
	}
	if req.ScheduledAt != nil && req.ScheduledAt.IsValid() {
		cmd.ScheduledAt = req.ScheduledAt.AsTime()
	}

	taskID, err := s.enqueue.Handle(ctx, cmd)
	if err != nil {
		return nil, domainToGRPC(err)
	}
	return &rsufzv1.EnqueueTaskResponse{TaskId: string(taskID)}, nil
}

func (s *Server) GetTask(ctx context.Context, req *rsufzv1.GetTaskRequest) (*rsufzv1.Task, error) {
	task, err := s.get.Handle(ctx, domain.TaskID(req.TaskId))
	if err != nil {
		return nil, domainToGRPC(err)
	}
	return taskToProto(task), nil
}

func (s *Server) CancelTask(ctx context.Context, req *rsufzv1.CancelTaskRequest) (*rsufzv1.CancelTaskResponse, error) {
	task, err := s.cancel.Handle(ctx, domain.TaskID(req.TaskId))
	if err != nil {
		return nil, domainToGRPC(err)
	}
	return &rsufzv1.CancelTaskResponse{Task: taskToProto(task)}, nil
}

func (s *Server) RepublishTask(ctx context.Context, req *rsufzv1.RepublishTaskRequest) (*rsufzv1.Task, error) {
	task, err := s.republish.Handle(ctx, domain.TaskID(req.TaskId))
	if err != nil {
		return nil, domainToGRPC(err)
	}
	return taskToProto(task), nil
}

func (s *Server) ListTasks(ctx context.Context, req *rsufzv1.ListTasksRequest) (*rsufzv1.ListTasksResponse, error) {
	f := usecase.ListFilter{
		Status:   protoToStatus(req.StatusFilter),
		Type:     req.TypeFilter,
		Page:     int(req.Page),
		PageSize: int(req.PageSize),
	}
	tasks, total, err := s.list.Handle(ctx, f)
	if err != nil {
		return nil, domainToGRPC(err)
	}
	resp := &rsufzv1.ListTasksResponse{Total: int32(total)}
	for _, t := range tasks {
		resp.Tasks = append(resp.Tasks, taskToProto(t))
	}
	return resp, nil
}

// StreamTaskUpdates — сервер-стриминг обновлений задачи.
// На этапе 5 отправляет текущее состояние и закрывает поток.
// Полноценный pub/sub подключается в этапе 9 (observability).
func (s *Server) StreamTaskUpdates(req *rsufzv1.StreamTaskUpdatesRequest, stream rsufzv1.TaskService_StreamTaskUpdatesServer) error {
	ctx := stream.Context()

	task, err := s.get.Handle(ctx, domain.TaskID(req.TaskId))
	if err != nil {
		return domainToGRPC(err)
	}
	if err := stream.Send(taskToProto(task)); err != nil {
		if err == io.EOF {
			return nil
		}
		return err
	}

	// Ждём до отмены контекста или завершения задачи.
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			task, err = s.get.Handle(ctx, domain.TaskID(req.TaskId))
			if err != nil {
				return domainToGRPC(err)
			}
			if err := stream.Send(taskToProto(task)); err != nil {
				return err
			}
			if task.Status == domain.StatusCompleted || task.Status == domain.StatusFailed || task.Status == domain.StatusCancelled {
				return nil
			}
		}
	}
}

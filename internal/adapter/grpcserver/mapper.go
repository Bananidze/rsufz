package grpcserver

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	rsufzv1 "github.com/Bananidze/rsufz/gen/go/rsufz/v1"
	"github.com/Bananidze/rsufz/internal/domain"
)

// taskToProto конвертирует доменную задачу в proto-сообщение.
func taskToProto(t *domain.Task) *rsufzv1.Task {
	pt := &rsufzv1.Task{
		Id:             string(t.ID),
		Type:           t.Type,
		Payload:        t.Payload,
		Priority:       uint32(t.Priority),
		Status:         statusToProto(t.Status),
		AttemptCount:   int32(t.AttemptCount),
		RetryLimit:     int32(t.RetryLimit),
		LastError:      t.LastError,
		Result:         t.Result,
		WorkerId:       t.WorkerID,
		IdempotencyKey: t.IdempotencyKey,
		CreatedAt:      timeToProto(t.CreatedAt),
		UpdatedAt:      timeToProto(t.UpdatedAt),
		ScheduledAt:    timeToProto(t.ScheduledAt),
	}
	for _, dep := range t.Dependencies {
		pt.DependencyIds = append(pt.DependencyIds, string(dep))
	}
	return pt
}

func statusToProto(s domain.Status) rsufzv1.TaskStatus {
	switch s {
	case domain.StatusPending:
		return rsufzv1.TaskStatus_TASK_STATUS_PENDING
	case domain.StatusRunning:
		return rsufzv1.TaskStatus_TASK_STATUS_RUNNING
	case domain.StatusCompleted:
		return rsufzv1.TaskStatus_TASK_STATUS_COMPLETED
	case domain.StatusFailed:
		return rsufzv1.TaskStatus_TASK_STATUS_FAILED
	case domain.StatusCancelled:
		return rsufzv1.TaskStatus_TASK_STATUS_CANCELLED
	default:
		return rsufzv1.TaskStatus_TASK_STATUS_UNSPECIFIED
	}
}

func protoToStatus(s rsufzv1.TaskStatus) domain.Status {
	switch s {
	case rsufzv1.TaskStatus_TASK_STATUS_PENDING:
		return domain.StatusPending
	case rsufzv1.TaskStatus_TASK_STATUS_RUNNING:
		return domain.StatusRunning
	case rsufzv1.TaskStatus_TASK_STATUS_COMPLETED:
		return domain.StatusCompleted
	case rsufzv1.TaskStatus_TASK_STATUS_FAILED:
		return domain.StatusFailed
	case rsufzv1.TaskStatus_TASK_STATUS_CANCELLED:
		return domain.StatusCancelled
	default:
		return ""
	}
}

func timeToProto(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

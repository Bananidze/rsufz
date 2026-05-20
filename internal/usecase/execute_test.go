package usecase_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/Bananidze/rsufz/internal/domain"
	"github.com/Bananidze/rsufz/internal/usecase"
)

// mockClockTime — значение, которое возвращает mockClock.Now() (из mock_test.go).
var mockClockTime = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

const workerID = "test-worker"

// newExecute собирает ExecuteUseCase с подменёнными зависимостями.
func newExecute(repo *mockRepo, broker *mockBroker, registry *usecase.Registry) *usecase.ExecuteUseCase {
	return usecase.NewExecute(repo, broker, registry, mockClock{}, usecase.NopMetrics{}, workerID, slog.Default())
}

// setupSubscribe настраивает mockBroker.Subscribe на возврат канала с одним delivery.
func setupSubscribe(broker *mockBroker, d usecase.Delivery) {
	ch := make(chan usecase.Delivery, 1)
	ch <- d
	close(ch)
	broker.On("Subscribe", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return((<-chan usecase.Delivery)(ch), nil)
}

// МТ.3.1 — задача успешно выполнена → статус completed, ACK отправлен.
func TestExecute_Success(t *testing.T) {
	t.Parallel()

	task := &domain.Task{
		ID:         "task-success",
		Type:       "noop",
		Status:     domain.StatusRunning,
		RetryLimit: 3,
	}
	delivery := usecase.Delivery{ID: "msg-1", TaskID: "task-success"}

	repo := new(mockRepo)
	broker := new(mockBroker)
	registry := usecase.NewRegistry()

	setupSubscribe(broker, delivery)
	repo.On("GetByID", mock.Anything, domain.TaskID("task-success")).Return(task, nil)
	repo.On("UpdateTask", mock.Anything, domain.TaskID("task-success"), mock.Anything).
		Return(nil).
		Run(func(args mock.Arguments) {
			fn := args.Get(2).(func(*domain.Task) error)
			require.NoError(t, fn(task))
		})
	broker.On("Ack", mock.Anything, mock.Anything, mock.Anything, "msg-1").Return(nil)

	registry.Register("noop", func(_ context.Context, _ []byte) ([]byte, error) {
		return []byte(`{}`), nil
	})

	execute := newExecute(repo, broker, registry)
	require.NoError(t, execute.Run(context.Background()))

	assert.Equal(t, domain.StatusCompleted, task.Status)
	repo.AssertExpectations(t)
	broker.AssertExpectations(t)
}

// МТ.3.2 — ошибка хендлера при наличии попыток → статус pending (retry).
func TestExecute_RetryOnError(t *testing.T) {
	t.Parallel()

	task := &domain.Task{
		ID:           "task-retry",
		Type:         "flaky",
		Status:       domain.StatusRunning,
		AttemptCount: 0,
		RetryLimit:   3,
	}
	delivery := usecase.Delivery{ID: "msg-2", TaskID: "task-retry"}

	repo := new(mockRepo)
	broker := new(mockBroker)
	registry := usecase.NewRegistry()

	setupSubscribe(broker, delivery)
	repo.On("GetByID", mock.Anything, domain.TaskID("task-retry")).Return(task, nil)
	repo.On("UpdateTask", mock.Anything, domain.TaskID("task-retry"), mock.Anything).
		Return(nil).
		Run(func(args mock.Arguments) {
			fn := args.Get(2).(func(*domain.Task) error)
			require.NoError(t, fn(task))
		})
	broker.On("Ack", mock.Anything, mock.Anything, mock.Anything, "msg-2").Return(nil)

	registry.Register("flaky", func(_ context.Context, _ []byte) ([]byte, error) {
		return nil, errors.New("transient error")
	})

	execute := newExecute(repo, broker, registry)
	require.NoError(t, execute.Run(context.Background()))

	assert.Equal(t, domain.StatusPending, task.Status)
	assert.Equal(t, 1, task.AttemptCount)
	assert.NotEmpty(t, task.LastError)
	assert.True(t, task.ScheduledAt.After(mockClockTime),
		"ScheduledAt должен быть позже момента mockClock после retry")
	repo.AssertExpectations(t)
	broker.AssertExpectations(t)
}

// МТ.3.5 — все попытки исчерпаны → статус failed, сообщение в DLQ.
func TestExecute_DLQ(t *testing.T) {
	t.Parallel()

	task := &domain.Task{
		ID:           "task-dlq",
		Type:         "bad",
		Status:       domain.StatusRunning,
		AttemptCount: 3, // уже 3 попытки
		RetryLimit:   3, // лимит 3 → CanRetry() == false
	}
	delivery := usecase.Delivery{ID: "msg-3", TaskID: "task-dlq"}

	repo := new(mockRepo)
	broker := new(mockBroker)
	registry := usecase.NewRegistry()

	setupSubscribe(broker, delivery)
	repo.On("GetByID", mock.Anything, domain.TaskID("task-dlq")).Return(task, nil)
	repo.On("UpdateTask", mock.Anything, domain.TaskID("task-dlq"), mock.Anything).
		Return(nil).
		Run(func(args mock.Arguments) {
			fn := args.Get(2).(func(*domain.Task) error)
			require.NoError(t, fn(task))
		})
	broker.On("Publish", mock.Anything, "rsufz:dlq", domain.TaskID("task-dlq")).Return(nil)
	broker.On("Ack", mock.Anything, mock.Anything, mock.Anything, "msg-3").Return(nil)

	registry.Register("bad", func(_ context.Context, _ []byte) ([]byte, error) {
		return nil, errors.New("permanent failure")
	})

	execute := newExecute(repo, broker, registry)
	require.NoError(t, execute.Run(context.Background()))

	assert.Equal(t, domain.StatusFailed, task.Status)
	repo.AssertExpectations(t)
	broker.AssertExpectations(t)
}

// МТ.6.3 — повторная доставка уже completed задачи → только ACK, хендлер не вызывается.
func TestExecute_AlreadyCompleted(t *testing.T) {
	t.Parallel()

	task := &domain.Task{
		ID:     "task-done",
		Type:   "noop",
		Status: domain.StatusCompleted,
	}
	delivery := usecase.Delivery{ID: "msg-4", TaskID: "task-done"}

	repo := new(mockRepo)
	broker := new(mockBroker)
	registry := usecase.NewRegistry()

	setupSubscribe(broker, delivery)
	repo.On("GetByID", mock.Anything, domain.TaskID("task-done")).Return(task, nil)
	broker.On("Ack", mock.Anything, mock.Anything, mock.Anything, "msg-4").Return(nil)

	handlerCalled := false
	registry.Register("noop", func(_ context.Context, _ []byte) ([]byte, error) {
		handlerCalled = true
		return nil, nil
	})

	execute := newExecute(repo, broker, registry)
	require.NoError(t, execute.Run(context.Background()))

	assert.False(t, handlerCalled, "хендлер не должен вызываться для завершённой задачи")
	repo.AssertExpectations(t)
	broker.AssertExpectations(t)
}

// МТ.3.x — неизвестный тип задачи → задача переходит в failed, сообщение в DLQ.
func TestExecute_UnknownType(t *testing.T) {
	t.Parallel()

	task := &domain.Task{
		ID:         "task-unknown",
		Type:       "not-registered",
		Status:     domain.StatusRunning,
		RetryLimit: 0, // без retry
	}
	delivery := usecase.Delivery{ID: "msg-5", TaskID: "task-unknown"}

	repo := new(mockRepo)
	broker := new(mockBroker)
	registry := usecase.NewRegistry()

	setupSubscribe(broker, delivery)
	repo.On("GetByID", mock.Anything, domain.TaskID("task-unknown")).Return(task, nil)
	repo.On("UpdateTask", mock.Anything, domain.TaskID("task-unknown"), mock.Anything).
		Return(nil).
		Run(func(args mock.Arguments) {
			fn := args.Get(2).(func(*domain.Task) error)
			require.NoError(t, fn(task))
		})
	broker.On("Publish", mock.Anything, "rsufz:dlq", domain.TaskID("task-unknown")).Return(nil)
	broker.On("Ack", mock.Anything, mock.Anything, mock.Anything, "msg-5").Return(nil)

	execute := newExecute(repo, broker, registry)
	require.NoError(t, execute.Run(context.Background()))

	assert.Equal(t, domain.StatusFailed, task.Status)
	repo.AssertExpectations(t)
	broker.AssertExpectations(t)
}

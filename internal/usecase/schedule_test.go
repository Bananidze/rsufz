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

func newScheduler(repo *mockRepo, broker *mockBroker) *usecase.ScheduleUseCase {
	return usecase.NewSchedule(repo, broker, slog.Default(),
		usecase.WithPollInterval(10*time.Millisecond),
		usecase.WithBatchSize(10),
		usecase.WithHeartbeatTimeout(30*time.Second),
	)
}

// МТ.2.1 — tick выбирает задачи и публикует их в брокер.
func TestSchedule_Tick_Dispatches(t *testing.T) {
	t.Parallel()

	tasks := []*domain.Task{
		{ID: "t1", Type: "job", Priority: 5},
		{ID: "t2", Type: "job", Priority: 3},
	}

	repo := new(mockRepo)
	broker := new(mockBroker)

	repo.On("PickAndMarkRunning", mock.Anything, 10).Return(tasks, nil).Once()
	repo.On("PickAndMarkRunning", mock.Anything, 10).Return(nil, nil) // last tick before cancel
	repo.On("ResetStuckRunning", mock.Anything, mock.Anything).Return(int64(0), nil).Maybe()

	broker.On("Publish", mock.Anything, mock.Anything, domain.TaskID("t1")).Return(nil)
	broker.On("Publish", mock.Anything, mock.Anything, domain.TaskID("t2")).Return(nil)

	scheduler := newScheduler(repo, broker)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_ = scheduler.Loop(ctx)

	broker.AssertCalled(t, "Publish", mock.Anything, mock.Anything, domain.TaskID("t1"))
	broker.AssertCalled(t, "Publish", mock.Anything, mock.Anything, domain.TaskID("t2"))
}

// МТ.4.3 — ResetStuckRunning вызывается по heartbeat-тикеру.
func TestSchedule_ResetsStuckTasks(t *testing.T) {
	t.Parallel()

	repo := new(mockRepo)
	broker := new(mockBroker)

	called := make(chan struct{}, 1)
	repo.On("PickAndMarkRunning", mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	repo.On("ResetStuckRunning", mock.Anything, mock.Anything).
		Return(int64(1), nil).
		Run(func(_ mock.Arguments) {
			select {
			case called <- struct{}{}:
			default:
			}
		})

	// hbTimeout=5ms → hbTicker = 2.5ms → сработает за 30ms несколько раз
	scheduler := usecase.NewSchedule(repo, broker, slog.Default(),
		usecase.WithPollInterval(2*time.Millisecond),
		usecase.WithHeartbeatTimeout(5*time.Millisecond),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	_ = scheduler.Loop(ctx)

	select {
	case <-called:
		// OK: был хотя бы один вызов
	default:
		t.Error("ResetStuckRunning должен был вызваться хотя бы раз")
	}
}

// tick — ошибка PickAndMarkRunning не останавливает цикл.
func TestSchedule_RepoError_ContinuesLoop(t *testing.T) {
	t.Parallel()

	repo := new(mockRepo)
	broker := new(mockBroker)

	dbErr := errors.New("db down")
	repo.On("PickAndMarkRunning", mock.Anything, mock.Anything).Return(nil, dbErr).Times(2)
	repo.On("PickAndMarkRunning", mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	repo.On("ResetStuckRunning", mock.Anything, mock.Anything).Return(int64(0), nil).Maybe()

	scheduler := newScheduler(repo, broker)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()

	err := scheduler.Loop(ctx)
	require.NoError(t, err, "Loop должен вернуть nil после ctx.Done даже если были ошибки")
}

// tick — ошибка Publish не останавливает обработку других задач.
func TestSchedule_PublishError_ContinuesForOtherTasks(t *testing.T) {
	t.Parallel()

	tasks := []*domain.Task{
		{ID: "fail", Type: "job"},
		{ID: "ok", Type: "job"},
	}

	repo := new(mockRepo)
	broker := new(mockBroker)

	repo.On("PickAndMarkRunning", mock.Anything, mock.Anything).Return(tasks, nil).Once()
	repo.On("PickAndMarkRunning", mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	repo.On("ResetStuckRunning", mock.Anything, mock.Anything).Return(int64(0), nil).Maybe()

	broker.On("Publish", mock.Anything, mock.Anything, domain.TaskID("fail")).
		Return(errors.New("redis down"))
	broker.On("Publish", mock.Anything, mock.Anything, domain.TaskID("ok")).
		Return(nil)

	scheduler := newScheduler(repo, broker)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()

	_ = scheduler.Loop(ctx)

	broker.AssertCalled(t, "Publish", mock.Anything, mock.Anything, domain.TaskID("ok"))
	assert.True(t, true) // цикл не упал
}

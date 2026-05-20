package usecase_test

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/Bananidze/rsufz/internal/usecase"
)

// МТ.4.3 — HeartbeatUseCase вызывает ResetStuckRunning на каждом тике.
func TestHeartbeat_CallsResetOnTick(t *testing.T) {
	t.Parallel()

	repo := new(mockRepo)
	called := make(chan struct{}, 5)

	repo.On("ResetStuckRunning", mock.Anything, mock.Anything).
		Return(int64(0), nil).
		Run(func(_ mock.Arguments) {
			select {
			case called <- struct{}{}:
			default:
			}
		})

	hb := usecase.NewHeartbeat(repo, 30*time.Second, 5*time.Millisecond, slog.Default())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	require.NoError(t, hb.Run(ctx))

	// За 30ms при интервале 5ms должно быть хотя бы 3 вызова
	count := 0
	for {
		select {
		case <-called:
			count++
		default:
			goto done
		}
	}
done:
	if count < 3 {
		t.Errorf("ожидали ≥3 вызова ResetStuckRunning, получили %d", count)
	}
}

// МТ.4.2 — при n > 0 пишется warn-лог (поведение check проверяется через ожидание вызова).
func TestHeartbeat_LogsWhenTasksReset(t *testing.T) {
	t.Parallel()

	repo := new(mockRepo)
	repo.On("ResetStuckRunning", mock.Anything, mock.Anything).Return(int64(3), nil).Once()
	repo.On("ResetStuckRunning", mock.Anything, mock.Anything).Return(int64(0), nil).Maybe()

	hb := usecase.NewHeartbeat(repo, 30*time.Second, 5*time.Millisecond, slog.Default())

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	require.NoError(t, hb.Run(ctx))
	repo.AssertExpectations(t)
}

// МТ.4.4 — ошибка ResetStuckRunning не останавливает цикл мониторинга.
func TestHeartbeat_RepoError_ContinuesLoop(t *testing.T) {
	t.Parallel()

	repo := new(mockRepo)
	var calls atomic.Int32
	repo.On("ResetStuckRunning", mock.Anything, mock.Anything).
		Return(int64(0), errors.New("transient db error")).
		Run(func(_ mock.Arguments) { calls.Add(1) })

	// Интервал 2ms, таймаут 100ms → ≥10 тиков даже на Windows (15ms разрешение таймера)
	hb := usecase.NewHeartbeat(repo, 30*time.Second, 2*time.Millisecond, slog.Default())

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := hb.Run(ctx)
	require.NoError(t, err, "Run должен вернуть nil когда ctx отменён")
	if calls.Load() < 2 {
		t.Errorf("должен продолжать цикл после ошибки, вызовов: %d", calls.Load())
	}
}

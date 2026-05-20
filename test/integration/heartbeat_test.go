//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	repopostgres "github.com/Bananidze/rsufz/internal/adapter/repo/postgres"
	"github.com/Bananidze/rsufz/internal/domain"
)

// МТ.4.1 — Heartbeat обновляет updated_at для задачи в статусе running.
func TestHeartbeat_UpdatesUpdatedAt(t *testing.T) {
	t.Parallel()
	repo := repopostgres.New(setupDB(t))
	ctx := context.Background()

	task := newTask("hb-job", 5)
	require.NoError(t, repo.Create(ctx, task))

	// Переводим в running
	require.NoError(t, repo.UpdateTask(ctx, task.ID, func(t *domain.Task) error {
		return t.TransitionTo(domain.StatusRunning)
	}))

	before, err := repo.GetByID(ctx, task.ID)
	require.NoError(t, err)

	time.Sleep(10 * time.Millisecond) // ждём, чтобы updated_at точно изменился

	require.NoError(t, repo.Heartbeat(ctx, task.ID, "worker-1"))

	after, err := repo.GetByID(ctx, task.ID)
	require.NoError(t, err)

	assert.True(t, after.UpdatedAt.After(before.UpdatedAt),
		"Heartbeat должен обновить updated_at: было %v, стало %v", before.UpdatedAt, after.UpdatedAt)
}

// МТ.4.1 — Heartbeat на задачу не в статусе running возвращает ErrNotFound.
func TestHeartbeat_NotRunning_ReturnsError(t *testing.T) {
	t.Parallel()
	repo := repopostgres.New(setupDB(t))
	ctx := context.Background()

	task := newTask("hb-pending", 5)
	require.NoError(t, repo.Create(ctx, task)) // статус pending

	err := repo.Heartbeat(ctx, task.ID, "worker-1")
	assert.ErrorIs(t, err, domain.ErrNotFound,
		"Heartbeat на не-running задачу должен вернуть ErrNotFound")
}

// МТ.4.2 — задача без heartbeat'а переходит из running в pending после timeout.
func TestHeartbeat_StuckTaskResetToPending(t *testing.T) {
	t.Parallel()
	repo := repopostgres.New(setupDB(t))
	ctx := context.Background()

	task := newTask("hb-stuck", 5)
	require.NoError(t, repo.Create(ctx, task))

	// Переводим в running
	require.NoError(t, repo.UpdateTask(ctx, task.ID, func(t *domain.Task) error {
		return t.TransitionTo(domain.StatusRunning)
	}))

	// Имитируем «смерть» воркера: задача уже в running, обновлений не будет.
	// Устанавливаем очень короткий timeout — почти мгновенный.
	time.Sleep(5 * time.Millisecond)

	n, err := repo.ResetStuckRunning(ctx, 1*time.Millisecond) // timeout 1ms
	require.NoError(t, err)
	assert.EqualValues(t, 1, n, "должна быть сброшена 1 зависшая задача")

	got, err := repo.GetByID(ctx, task.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusPending, got.Status,
		"зависшая задача должна вернуться в pending")
	assert.Empty(t, got.WorkerID, "worker_id должен быть очищен")
}

// МТ.4.3 — ResetStuckRunning не трогает задачи, получившие heartbeat вовремя.
func TestHeartbeat_FreshTask_NotReset(t *testing.T) {
	t.Parallel()
	repo := repopostgres.New(setupDB(t))
	ctx := context.Background()

	task := newTask("hb-fresh", 5)
	require.NoError(t, repo.Create(ctx, task))

	// Переводим в running
	require.NoError(t, repo.UpdateTask(ctx, task.ID, func(t *domain.Task) error {
		return t.TransitionTo(domain.StatusRunning)
	}))

	// Отправляем heartbeat
	require.NoError(t, repo.Heartbeat(ctx, task.ID, "worker-1"))

	// Большой timeout — задача только что получила heartbeat, не должна сброситься
	n, err := repo.ResetStuckRunning(ctx, 10*time.Second)
	require.NoError(t, err)
	assert.EqualValues(t, 0, n, "свежая задача не должна сбрасываться")

	got, err := repo.GetByID(ctx, task.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusRunning, got.Status)
}

// МТ.4.4 — после сброса зависшей задачи планировщик может взять её снова.
func TestHeartbeat_ResetAllowsReassignment(t *testing.T) {
	t.Parallel()
	repo := repopostgres.New(setupDB(t))
	ctx := context.Background()

	task := newTask("hb-reassign", 5)
	require.NoError(t, repo.Create(ctx, task))

	// Переводим в running
	require.NoError(t, repo.UpdateTask(ctx, task.ID, func(t *domain.Task) error {
		return t.TransitionTo(domain.StatusRunning)
	}))

	time.Sleep(5 * time.Millisecond)

	// Сбрасываем зависшую задачу
	n, err := repo.ResetStuckRunning(ctx, 1*time.Millisecond)
	require.NoError(t, err)
	require.EqualValues(t, 1, n)

	// Планировщик должен снова видеть задачу как pending
	tasks, err := repo.LockNextPending(ctx, 10)
	require.NoError(t, err)

	found := false
	for _, tsk := range tasks {
		if tsk.ID == task.ID {
			found = true
		}
	}
	assert.True(t, found, "после сброса задача должна быть доступна планировщику")
}

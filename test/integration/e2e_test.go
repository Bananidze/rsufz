//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/sync/errgroup"

	redisbroker "github.com/Bananidze/rsufz/internal/adapter/broker/redis"
	repopostgres "github.com/Bananidze/rsufz/internal/adapter/repo/postgres"
	"github.com/Bananidze/rsufz/internal/domain"
	"github.com/Bananidze/rsufz/internal/platform/ids"
	"github.com/Bananidze/rsufz/internal/usecase"
)

// setupRedis поднимает Redis-контейнер и возвращает готовый клиент.
func setupRedis(t *testing.T) *goredis.Client {
	t.Helper()
	ctx := context.Background()

	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "redis:7-alpine",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForLog("Ready to accept connections"),
		},
		Started: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ctr.Terminate(ctx) })

	host, err := ctr.Host(ctx)
	require.NoError(t, err)
	port, err := ctr.MappedPort(ctx, "6379")
	require.NoError(t, err)

	rdb := goredis.NewClient(&goredis.Options{Addr: fmt.Sprintf("%s:%s", host, port.Port())})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// waitForStatus опрашивает репозиторий до тех пор, пока задача не достигнет нужного статуса.
func waitForStatus(t *testing.T, repo *repopostgres.Repo, id domain.TaskID, want domain.Status, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		task, err := repo.GetByID(context.Background(), id)
		if err == nil && task.Status == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	task, _ := repo.GetByID(context.Background(), id)
	var got domain.Status
	if task != nil {
		got = task.Status
	}
	t.Fatalf("timeout: task %s so and not reached status %s (current: %s)", id, want, got)
}

// newStack создаёт полный стек (repo + broker) с изолированными контейнерами.
func newStack(t *testing.T) (*repopostgres.Repo, *redisbroker.Broker) {
	t.Helper()
	repo := repopostgres.New(setupDB(t))
	broker := redisbroker.New(setupRedis(t))
	return repo, broker
}

func nopLog() *slog.Logger { return slog.New(slog.NewTextHandler(noopWriter{}, nil)) }

type noopWriter struct{}

func (noopWriter) Write(p []byte) (n int, err error) { return len(p), nil }

// ИТ.1.1 — полный цикл: enqueue → schedule → execute → completed.
func TestE2E_FullCycle(t *testing.T) {
	t.Parallel()

	repo, broker := newStack(t)
	log := nopLog()
	clock := usecase.SystemClock{}
	gen := ids.UUIDv7Gen{}

	registry := usecase.NewRegistry()
	registry.Register("ping", func(_ context.Context, _ []byte) ([]byte, error) {
		return []byte(`{"pong":true}`), nil
	})

	enq := usecase.NewEnqueue(repo, clock, gen, usecase.NopMetrics{}, log)
	scheduler := usecase.NewSchedule(repo, broker, usecase.NopMetrics{}, log,
		usecase.WithPollInterval(50*time.Millisecond),
		usecase.WithBatchSize(10),
	)
	execute := usecase.NewExecute(repo, broker, registry, clock, usecase.NopMetrics{}, "worker-1", log)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	grp, gctx := errgroup.WithContext(ctx)
	grp.Go(func() error { return scheduler.Loop(gctx) })
	grp.Go(func() error { return execute.Run(gctx) })

	taskID, err := enq.Handle(context.Background(), usecase.EnqueueCmd{
		Type:    "ping",
		Payload: []byte(`{}`),
	})
	require.NoError(t, err)

	waitForStatus(t, repo, taskID, domain.StatusCompleted, 10*time.Second)
	cancel()
	_ = grp.Wait()

	task, err := repo.GetByID(context.Background(), taskID)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusCompleted, task.Status)
	assert.Equal(t, []byte(`{"pong":true}`), task.Result)
}

// ИТ.2.2 — DAG: child-задача выполняется только после завершения dep.
func TestE2E_DAG(t *testing.T) {
	t.Parallel()

	repo, broker := newStack(t)
	log := nopLog()
	clock := usecase.SystemClock{}
	gen := ids.UUIDv7Gen{}

	var mu sync.Mutex
	var execOrder []string

	registry := usecase.NewRegistry()
	registry.Register("dep-job", func(_ context.Context, _ []byte) ([]byte, error) {
		mu.Lock()
		execOrder = append(execOrder, "dep")
		mu.Unlock()
		return []byte(`{}`), nil
	})
	registry.Register("child-job", func(_ context.Context, _ []byte) ([]byte, error) {
		mu.Lock()
		execOrder = append(execOrder, "child")
		mu.Unlock()
		return []byte(`{}`), nil
	})

	enq := usecase.NewEnqueue(repo, clock, gen, usecase.NopMetrics{}, log)
	scheduler := usecase.NewSchedule(repo, broker, usecase.NopMetrics{}, log,
		usecase.WithPollInterval(50*time.Millisecond),
		usecase.WithBatchSize(10),
	)
	execute := usecase.NewExecute(repo, broker, registry, clock, usecase.NopMetrics{}, "worker-dag", log)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	grp, gctx := errgroup.WithContext(ctx)
	grp.Go(func() error { return scheduler.Loop(gctx) })
	grp.Go(func() error { return execute.Run(gctx) })

	depID, err := enq.Handle(context.Background(), usecase.EnqueueCmd{
		Type:    "dep-job",
		Payload: []byte(`{}`),
	})
	require.NoError(t, err)

	childID, err := enq.Handle(context.Background(), usecase.EnqueueCmd{
		Type:         "child-job",
		Payload:      []byte(`{}`),
		Dependencies: []domain.TaskID{depID},
	})
	require.NoError(t, err)

	// Ждём, пока обе задачи завершатся
	waitForStatus(t, repo, depID, domain.StatusCompleted, 10*time.Second)
	waitForStatus(t, repo, childID, domain.StatusCompleted, 10*time.Second)

	cancel()
	_ = grp.Wait()

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, execOrder, 2)
	assert.Equal(t, "dep", execOrder[0], "dep должен выполниться первым")
	assert.Equal(t, "child", execOrder[1], "child должен выполниться после dep")
}

// ИТ.3.1 — Retry: хендлер падает 1 раз, затем успешно выполняется.
func TestE2E_Retry(t *testing.T) {
	t.Parallel()

	repo, broker := newStack(t)
	log := nopLog()
	clock := usecase.SystemClock{}
	gen := ids.UUIDv7Gen{}

	var callCount atomic.Int32
	registry := usecase.NewRegistry()
	registry.Register("flaky", func(_ context.Context, _ []byte) ([]byte, error) {
		if callCount.Add(1) <= 1 {
			return nil, errors.New("transient error")
		}
		return []byte(`{"ok":true}`), nil
	})

	enq := usecase.NewEnqueue(repo, clock, gen, usecase.NopMetrics{}, log)
	scheduler := usecase.NewSchedule(repo, broker, usecase.NopMetrics{}, log,
		usecase.WithPollInterval(50*time.Millisecond),
		usecase.WithBatchSize(10),
	)
	execute := usecase.NewExecute(repo, broker, registry, clock, usecase.NopMetrics{}, "worker-retry", log)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	grp, gctx := errgroup.WithContext(ctx)
	grp.Go(func() error { return scheduler.Loop(gctx) })
	grp.Go(func() error { return execute.Run(gctx) })

	taskID, err := enq.Handle(context.Background(), usecase.EnqueueCmd{
		Type:       "flaky",
		Payload:    []byte(`{}`),
		RetryLimit: 3,
	})
	require.NoError(t, err)

	// Хендлер сначала упадёт, задача вернётся в pending с backoff ~2s, затем завершится
	waitForStatus(t, repo, taskID, domain.StatusCompleted, 15*time.Second)
	cancel()
	_ = grp.Wait()

	task, err := repo.GetByID(context.Background(), taskID)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusCompleted, task.Status)
	assert.EqualValues(t, 1, task.AttemptCount, "хендлер вызывался 2 раза: 1 ошибка + 1 успех")
	assert.Equal(t, int32(2), callCount.Load(), "хендлер должен был вызван ровно 2 раза")
}

// ИТ.4.1 — Failover: зависший воркер → HeartbeatUseCase → задача возвращается в pending,
// второй воркер успешно завершает её.
func TestE2E_Failover(t *testing.T) {
	t.Parallel()

	repo, broker := newStack(t)
	log := nopLog()

	// Создаём задачу и вручную переводим в running (имитация: планировщик назначил воркеру,
	// воркер умер и не будет посылать heartbeat).
	task := newTask("recover-job", 5)
	require.NoError(t, repo.Create(context.Background(), task))
	require.NoError(t, repo.UpdateTask(context.Background(), task.ID, func(t *domain.Task) error {
		return t.TransitionTo(domain.StatusRunning)
	}))

	// Ждём чуть дольше timeout, чтобы задача стала «зависшей»
	time.Sleep(10 * time.Millisecond)

	// HeartbeatUseCase с timeout 1ms детектирует зависшую задачу и сбрасывает в pending
	hb := usecase.NewHeartbeat(repo, 1*time.Millisecond, 5*time.Millisecond, log)
	hbCtx, hbCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer hbCancel()
	_ = hb.Run(hbCtx)

	// Теперь задача должна быть pending снова
	got, err := repo.GetByID(context.Background(), task.ID)
	require.NoError(t, err)
	require.Equal(t, domain.StatusPending, got.Status, "задача должна сброситься в pending после heartbeat timeout")

	// Запускаем «второй» воркер, который завершит задачу
	clock := usecase.SystemClock{}
	registry := usecase.NewRegistry()
	registry.Register("recover-job", func(_ context.Context, _ []byte) ([]byte, error) {
		return []byte(`{}`), nil
	})
	scheduler := usecase.NewSchedule(repo, broker, usecase.NopMetrics{}, log,
		usecase.WithPollInterval(50*time.Millisecond),
		usecase.WithBatchSize(10),
	)
	execute := usecase.NewExecute(repo, broker, registry, clock, usecase.NopMetrics{}, "worker-failover-2", log)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	grp, gctx := errgroup.WithContext(ctx)
	grp.Go(func() error { return scheduler.Loop(gctx) })
	grp.Go(func() error { return execute.Run(gctx) })

	waitForStatus(t, repo, task.ID, domain.StatusCompleted, 10*time.Second)
	cancel()
	_ = grp.Wait()

	final, err := repo.GetByID(context.Background(), task.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusCompleted, final.Status)
}

// ИТ.5.1 — Конкурентность: N горутин ставят задачи, M воркеров обрабатывают.
// Каждая задача обрабатывается ровно один раз.
func TestE2E_Concurrent(t *testing.T) {
	t.Parallel()

	const producers = 3
	const tasksPerProducer = 5
	const totalTasks = producers * tasksPerProducer
	const workers = 2

	repo, broker := newStack(t)
	log := nopLog()
	clock := usecase.SystemClock{}
	gen := ids.UUIDv7Gen{}

	var processed atomic.Int32
	registry := usecase.NewRegistry()
	registry.Register("concurrent-job", func(_ context.Context, _ []byte) ([]byte, error) {
		processed.Add(1)
		return []byte(`{}`), nil
	})

	enq := usecase.NewEnqueue(repo, clock, gen, usecase.NopMetrics{}, log)
	scheduler := usecase.NewSchedule(repo, broker, usecase.NopMetrics{}, log,
		usecase.WithPollInterval(30*time.Millisecond),
		usecase.WithBatchSize(20),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	grp, gctx := errgroup.WithContext(ctx)
	grp.Go(func() error { return scheduler.Loop(gctx) })
	for i := 0; i < workers; i++ {
		workerID := fmt.Sprintf("worker-concurrent-%d", i)
		exec := usecase.NewExecute(repo, broker, registry, clock, usecase.NopMetrics{}, workerID, log)
		grp.Go(func() error { return exec.Run(gctx) })
	}

	// Параллельная постановка задач
	var wg sync.WaitGroup
	taskIDs := make(chan domain.TaskID, totalTasks)
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < tasksPerProducer; j++ {
				id, err := enq.Handle(context.Background(), usecase.EnqueueCmd{
					Type:    "concurrent-job",
					Payload: []byte(`{}`),
				})
				if err == nil {
					taskIDs <- id
				}
			}
		}()
	}
	wg.Wait()
	close(taskIDs)

	var allIDs []domain.TaskID
	for id := range taskIDs {
		allIDs = append(allIDs, id)
	}
	require.Len(t, allIDs, totalTasks, "все задачи должны были поставлены успешно")

	// Ждём завершения всех задач
	for _, id := range allIDs {
		waitForStatus(t, repo, id, domain.StatusCompleted, 20*time.Second)
	}
	cancel()
	_ = grp.Wait()

	assert.Equal(t, int32(totalTasks), processed.Load(),
		"каждая задача должна быть обработана ровно один раз")
}

// ИТ.6.1 — Брокер недоступен: планировщик не падает, продолжает работу.
func TestE2E_BrokerDown(t *testing.T) {
	t.Parallel()

	// Используем только Postgres, брокер указывает на несуществующий Redis
	repo := repopostgres.New(setupDB(t))
	log := nopLog()
	clock := usecase.SystemClock{}
	gen := ids.UUIDv7Gen{}

	// Redis на несуществующем порту — Publish будет падать с ошибкой
	badRDB := goredis.NewClient(&goredis.Options{
		Addr:        "localhost:1",
		DialTimeout: 50 * time.Millisecond,
	})
	t.Cleanup(func() { _ = badRDB.Close() })
	badBroker := redisbroker.New(badRDB)

	enq := usecase.NewEnqueue(repo, clock, gen, usecase.NopMetrics{}, log)
	scheduler := usecase.NewSchedule(repo, badBroker, usecase.NopMetrics{}, log,
		usecase.WithPollInterval(30*time.Millisecond),
		usecase.WithBatchSize(5),
	)

	// Ставим несколько задач в БД
	for i := 0; i < 3; i++ {
		_, err := enq.Handle(context.Background(), usecase.EnqueueCmd{
			Type:    "broker-down-job",
			Payload: []byte(`{}`),
		})
		require.NoError(t, err)
	}

	// Планировщик запускается при недоступном брокере
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	err := scheduler.Loop(ctx)
	assert.NoError(t, err, "Loop должен вернуть nil после ctx.Done, даже если брокер недоступен")
}

// spyMetrics — тестовая реализация usecase.Metrics с подсчётом вызовов.
type spyMetrics struct {
	usecase.NopMetrics
	enqueued   atomic.Int64
	completed  atomic.Int64
	retried    atomic.Int64
	failed     atomic.Int64
	dispatched atomic.Int64
}

func (m *spyMetrics) TaskEnqueued(_ time.Duration) { m.enqueued.Add(1) }
func (m *spyMetrics) TaskCompleted(_ time.Duration) { m.completed.Add(1) }
func (m *spyMetrics) TaskRetried()                  { m.retried.Add(1) }
func (m *spyMetrics) TaskFailed()                   { m.failed.Add(1) }
func (m *spyMetrics) TaskDispatched()               { m.dispatched.Add(1) }

// ИТ.7.1 — Метрики: счётчики enqueued / dispatched / completed инкрементируются.
func TestE2E_Metrics(t *testing.T) {
	t.Parallel()

	repo, broker := newStack(t)
	log := nopLog()
	clock := usecase.SystemClock{}
	gen := ids.UUIDv7Gen{}

	metrics := &spyMetrics{}
	registry := usecase.NewRegistry()
	registry.Register("metric-job", func(_ context.Context, _ []byte) ([]byte, error) {
		return []byte(`{}`), nil
	})

	enq := usecase.NewEnqueue(repo, clock, gen, metrics, log)
	scheduler := usecase.NewSchedule(repo, broker, metrics, log,
		usecase.WithPollInterval(50*time.Millisecond),
		usecase.WithBatchSize(10),
	)
	execute := usecase.NewExecute(repo, broker, registry, clock, metrics, "worker-metrics", log)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	grp, gctx := errgroup.WithContext(ctx)
	grp.Go(func() error { return scheduler.Loop(gctx) })
	grp.Go(func() error { return execute.Run(gctx) })

	taskID, err := enq.Handle(context.Background(), usecase.EnqueueCmd{
		Type:    "metric-job",
		Payload: []byte(`{}`),
	})
	require.NoError(t, err)

	waitForStatus(t, repo, taskID, domain.StatusCompleted, 10*time.Second)
	cancel()
	_ = grp.Wait()

	assert.Equal(t, int64(1), metrics.enqueued.Load(), "TaskEnqueued должен быть вызван 1 раз")
	assert.Equal(t, int64(1), metrics.dispatched.Load(), "TaskDispatched должен быть вызван 1 раз")
	assert.Equal(t, int64(1), metrics.completed.Load(), "TaskCompleted должен быть вызван 1 раз")
	assert.Equal(t, int64(0), metrics.failed.Load(), "TaskFailed не должен вызываться")
	assert.Equal(t, int64(0), metrics.retried.Load(), "TaskRetried не должен вызываться")
}

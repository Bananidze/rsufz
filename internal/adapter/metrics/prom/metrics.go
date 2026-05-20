// Package prom реализует usecase.Metrics поверх Prometheus client_golang.
package prom

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics реализует usecase.Metrics, обновляя Prometheus-счётчики и гистограммы.
type Metrics struct {
	tasksEnqueued   prometheus.Counter
	tasksCompleted  prometheus.Counter
	tasksFailed     prometheus.Counter
	tasksRetried    prometheus.Counter
	tasksDispatched prometheus.Counter
	taskDuration    prometheus.Histogram
	enqueueLatency  prometheus.Histogram
	tasksPending    prometheus.Gauge
}

// New регистрирует все метрики в переданном Registerer и возвращает Metrics.
// Передавайте prometheus.NewRegistry() для изоляции (тесты) или
// prometheus.DefaultRegisterer для глобального реестра.
func New(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	return &Metrics{
		tasksEnqueued: f.NewCounter(prometheus.CounterOpts{
			Name: "rsufz_tasks_enqueued_total",
			Help: "Общее число поставленных в очередь задач.",
		}),
		tasksCompleted: f.NewCounter(prometheus.CounterOpts{
			Name: "rsufz_tasks_completed_total",
			Help: "Общее число успешно завершённых задач.",
		}),
		tasksFailed: f.NewCounter(prometheus.CounterOpts{
			Name: "rsufz_tasks_failed_total",
			Help: "Общее число задач, завершившихся ошибкой (отправлены в DLQ).",
		}),
		tasksRetried: f.NewCounter(prometheus.CounterOpts{
			Name: "rsufz_tasks_retried_total",
			Help: "Общее число повторных попыток выполнения задач.",
		}),
		tasksDispatched: f.NewCounter(prometheus.CounterOpts{
			Name: "rsufz_tasks_dispatched_total",
			Help: "Общее число задач, отправленных воркерам планировщиком.",
		}),
		taskDuration: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "rsufz_task_duration_seconds",
			Help:    "Время выполнения задачи воркером (секунды).",
			Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}),
		enqueueLatency: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "rsufz_enqueue_latency_seconds",
			Help:    "Задержка постановки задачи в очередь (секунды). МТ.8.2",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25},
		}),
		tasksPending: f.NewGauge(prometheus.GaugeOpts{
			Name: "rsufz_tasks_pending",
			Help: "Текущее число задач в статусе pending.",
		}),
	}
}

// Методы реализуют usecase.Metrics.

func (m *Metrics) TaskEnqueued(latency time.Duration) {
	m.tasksEnqueued.Inc()
	m.enqueueLatency.Observe(latency.Seconds())
}

func (m *Metrics) TaskCompleted(duration time.Duration) {
	m.tasksCompleted.Inc()
	m.taskDuration.Observe(duration.Seconds())
}

func (m *Metrics) TaskFailed()      { m.tasksFailed.Inc() }
func (m *Metrics) TaskRetried()     { m.tasksRetried.Inc() }
func (m *Metrics) TaskDispatched()  { m.tasksDispatched.Inc() }
func (m *Metrics) SetPending(n int) { m.tasksPending.Set(float64(n)) }

// Serve запускает HTTP-сервер с эндпоинтом /metrics.
// Блокируется до ctx.Done(). МТ.8.1
func Serve(ctx context.Context, addr string, reg prometheus.Gatherer) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

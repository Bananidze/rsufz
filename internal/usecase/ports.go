// Package usecase содержит бизнес-сценарии РСУФЗ и интерфейсы их зависимостей.
// Ничего из этого пакета не знает про gRPC, PostgreSQL или Redis —
// только про доменные типы и стандартную библиотеку.
package usecase

import (
	"context"
	"time"

	"github.com/Bananidze/rsufz/internal/domain"
)

// TaskRepository — порт хранилища состояния задач.
// Реализуется в internal/adapter/repo/postgres (Этап 4).
type TaskRepository interface {
	// Create сохраняет новую задачу со статусом pending.
	Create(ctx context.Context, t *domain.Task) error

	// GetByID возвращает задачу по ID. При отсутствии — domain.ErrNotFound.
	GetByID(ctx context.Context, id domain.TaskID) (*domain.Task, error)

	// FindByIdempotencyKey ищет задачу по ключу идемпотентности (МТ.6.3).
	FindByIdempotencyKey(ctx context.Context, key string) (*domain.Task, error)

	// UpdateTask атомарно загружает задачу, применяет mutate и сохраняет результат.
	// Если mutate вернул ошибку — транзакция откатывается, изменений нет.
	UpdateTask(ctx context.Context, id domain.TaskID, mutate func(*domain.Task) error) error

	// Heartbeat обновляет updated_at для задачи в статусе running (воркер жив).
	Heartbeat(ctx context.Context, id domain.TaskID, workerID string) error

	// PickAndMarkRunning атомарно выбирает до limit pending-задач, готовых к запуску
	// (scheduled_at <= now, DAG-зависимости completed), переводит их в running и возвращает.
	// FOR UPDATE SKIP LOCKED предотвращает конкурентный выбор одной задачи (МТ.2.1–2.4).
	PickAndMarkRunning(ctx context.Context, limit int) ([]*domain.Task, error)

	// ResetStuckRunning переводит задачи из running в pending, если их updated_at
	// старше timeout — воркер умер без heartbeat (МТ.4.1–4.4).
	ResetStuckRunning(ctx context.Context, timeout time.Duration) (int64, error)

	// List возвращает задачи по фильтру и общее число совпадений.
	List(ctx context.Context, f ListFilter) ([]*domain.Task, int, error)

	// CleanupExpired удаляет завершённые/отменённые задачи старше ttl (МТ.5.3).
	CleanupExpired(ctx context.Context, ttl time.Duration) (int64, error)
}

// Broker — порт транспорта задач к воркерам (Redis Streams).
// Реализуется в internal/adapter/broker/redis (Этап 6).
type Broker interface {
	// Publish публикует task_id в Redis Stream. Вызывается планировщиком.
	Publish(ctx context.Context, stream string, taskID domain.TaskID) error

	// Subscribe возвращает канал доставки. Вызывается воркером.
	// Блокируется до закрытия ctx или ошибки.
	Subscribe(ctx context.Context, stream, group, consumer string) (<-chan Delivery, error)

	// Ack подтверждает обработку сообщения (XACK).
	Ack(ctx context.Context, stream, group, msgID string) error
}

// Delivery — одно сообщение из Redis Stream.
type Delivery struct {
	ID     string        // Redis Stream message ID (для ACK)
	TaskID domain.TaskID // ID задачи из поля "task_id"
}

// Clock предоставляет текущее время. Подменяется фейком в тестах.
type Clock interface {
	Now() time.Time
}

// IDGenerator генерирует уникальные ID задач (UUIDv7).
// Подменяется детерминированным генератором в тестах.
type IDGenerator interface {
	New() domain.TaskID
}

// Metrics — порт наблюдаемости (реализован в adapter/metrics/prom).
// Передавайте NopMetrics{} если метрики не нужны (тесты, заглушки).
type Metrics interface {
	TaskEnqueued(latency time.Duration)
	TaskCompleted(duration time.Duration)
	TaskFailed()
	TaskRetried()
	TaskDispatched()
	SetPending(n int)
}

// NopMetrics — заглушка для тестов и конфигураций без метрик.
type NopMetrics struct{}

func (NopMetrics) TaskEnqueued(_ time.Duration) {}
func (NopMetrics) TaskCompleted(_ time.Duration) {}
func (NopMetrics) TaskFailed()                  {}
func (NopMetrics) TaskRetried()                 {}
func (NopMetrics) TaskDispatched()              {}
func (NopMetrics) SetPending(_ int)             {}

// SystemClock реализует Clock с реальным временем.
type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }

// ListFilter — параметры выборки задач.
type ListFilter struct {
	Status   domain.Status // пустая строка = все статусы
	Type     string        // пустая строка = все типы
	Page     int
	PageSize int // 0 → используем дефолт 50
}

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

	// LockNextPending выбирает до limit задач со статусом pending, готовых к запуску,
	// и блокирует их (FOR UPDATE SKIP LOCKED) для планировщика.
	LockNextPending(ctx context.Context, limit int) ([]*domain.Task, error)

	// List возвращает задачи по фильтру и общее число совпадений.
	List(ctx context.Context, f ListFilter) ([]*domain.Task, int, error)

	// CleanupExpired удаляет завершённые/отменённые задачи старше ttl (МТ.5.3).
	CleanupExpired(ctx context.Context, ttl time.Duration) (int64, error)
}

// Broker — порт транспорта задач к воркерам.
// Реализуется в internal/adapter/broker/redis (Этап 6).
type Broker interface {
	Publish(ctx context.Context, queue string, payload []byte) error
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

// ListFilter — параметры выборки задач.
type ListFilter struct {
	Status   domain.Status // пустая строка = все статусы
	Type     string        // пустая строка = все типы
	Page     int
	PageSize int // 0 → используем дефолт 50
}

package domain

import (
	"fmt"
	"time"
)

// TaskID — UUID задачи. Используем UUIDv7: хронологически сортируемый,
// что улучшает локальность B-tree индекса в PostgreSQL.
type TaskID string

// Status — статус жизненного цикла задачи.
type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
)

// Priority — приоритет задачи в диапазоне 0..10.
// Больше — выше приоритет (планировщик сортирует DESC).
type Priority uint8

const (
	PriorityMin Priority = 0
	PriorityMax Priority = 10
)

// Validate возвращает ErrInvalidPriority, если приоритет вне диапазона 0..10 (МТ.1.7).
func (p Priority) Validate() error {
	if p > PriorityMax {
		return fmt.Errorf("%w: got %d", ErrInvalidPriority, p)
	}
	return nil
}

// allowedTransitions задаёт допустимые переходы для каждого статуса.
// Любой переход, не перечисленный здесь, запрещён (МТ.7.2).
var allowedTransitions = map[Status][]Status{
	StatusPending:   {StatusRunning, StatusCancelled},
	StatusRunning:   {StatusCompleted, StatusFailed, StatusPending},
	StatusFailed:    {StatusPending}, // ручной перезапуск (МТ.7.3)
	StatusCompleted: {},
	StatusCancelled: {},
}

// Task — агрегатный корень доменной модели РСУФЗ.
// Хранит полное состояние фоновой задачи на протяжении всего жизненного цикла.
type Task struct {
	ID             TaskID
	Type           string   // тип задачи: "send_email", "generate_report", ...
	Payload        []byte   // JSON-параметры бизнес-логики
	Priority       Priority // 0..10
	Status         Status
	Dependencies   []TaskID  // задачи, которые должны завершиться до запуска этой
	ScheduledAt    time.Time // zero = запустить немедленно
	CreatedAt      time.Time
	UpdatedAt      time.Time
	AttemptCount   int    // сколько попыток выполнения уже было (МТ.3.1)
	RetryLimit     int    // максимальное количество попыток (МТ.3.2)
	LastError      string // описание последней ошибки
	Result         []byte // JSON-результат успешного выполнения
	WorkerID       string // ID воркера, который сейчас выполняет задачу
	IdempotencyKey string // ключ дедупликации от клиента
}

// TransitionTo переводит задачу в новый статус.
// Возвращает ErrInvalidStateTransition, если переход не разрешён (МТ.7.2).
// При успехе обновляет UpdatedAt.
func (t *Task) TransitionTo(next Status) error {
	for _, allowed := range allowedTransitions[t.Status] {
		if allowed == next {
			t.Status = next
			t.UpdatedAt = time.Now().UTC()
			return nil
		}
	}
	return fmt.Errorf("%w: %s → %s", ErrInvalidStateTransition, t.Status, next)
}

// IncrementAttempt увеличивает счётчик попыток выполнения (МТ.3.1).
func (t *Task) IncrementAttempt() {
	t.AttemptCount++
}

// CanRetry сообщает, можно ли ещё повторить выполнение задачи (МТ.3.2).
func (t *Task) CanRetry() bool {
	return t.AttemptCount < t.RetryLimit
}

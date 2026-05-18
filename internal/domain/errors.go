package domain

import "errors"

// Sentinel-ошибки доменного слоя.
// Используются через errors.Is/As — никогда не сравниваем строки.

var (
	// ErrNotFound — задача с таким ID не найдена.
	ErrNotFound = errors.New("task not found")

	// ErrInvalidStateTransition — попытка перевести задачу в недопустимый статус.
	// Покрывает тест МТ.7.2 (completed → running).
	ErrInvalidStateTransition = errors.New("invalid state transition")

	// ErrCyclicDependency — граф зависимостей содержит цикл.
	// Покрывает тест МТ.2.5.
	ErrCyclicDependency = errors.New("cyclic dependency in DAG")

	// ErrDependencyNotReady — одна из зависимостей ещё не завершена.
	ErrDependencyNotReady = errors.New("dependency not completed")

	// ErrRetryLimitExhausted — попытки retry исчерпаны.
	ErrRetryLimitExhausted = errors.New("retry limit exhausted")

	// ErrDuplicateTask — задача с таким ключом идемпотентности уже существует.
	ErrDuplicateTask = errors.New("duplicate idempotency key")

	// ErrInvalidPriority — приоритет вне допустимого диапазона 0..10.
	// Покрывает тест МТ.1.7.
	ErrInvalidPriority = errors.New("priority must be between 0 and 10")
)

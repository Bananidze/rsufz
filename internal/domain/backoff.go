package domain

import "time"

// Backoff вычисляет задержку перед n-й повторной попыткой по формуле
// 2^attempt * Base, но не превышая Max (МТ.3.3, МТ.3.4).
type Backoff struct {
	Base time.Duration
	Max  time.Duration
}

// DefaultBackoff возвращает Backoff со стандартными параметрами из ПЗ §МТ.3.3.
func DefaultBackoff() Backoff {
	return Backoff{
		Base: time.Second,
		Max:  5 * time.Minute,
	}
}

// Delay возвращает задержку для заданного номера попытки.
// attempt < 0 приводится к 0; очень большие значения не вызывают переполнения.
func (b Backoff) Delay(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	// Сдвиг int64 на 63+ бит даёт 0 или отрицательное значение — оба случая
	// покрываются проверкой ниже, но ограничиваем явно для ясности.
	const maxShift = 62
	if attempt > maxShift || b.Base <= 0 {
		return b.Max
	}
	d := b.Base << uint(attempt)
	if d <= 0 || d > b.Max {
		return b.Max
	}
	return d
}

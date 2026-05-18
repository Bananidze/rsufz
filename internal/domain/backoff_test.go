package domain

import (
	"testing"
	"time"
)

// TestBackoff_Delay проверяет формулу 2^n * base (МТ.3.3).
func TestBackoff_Delay(t *testing.T) {
	t.Parallel()
	b := Backoff{Base: time.Second, Max: 24 * time.Hour}

	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 1 * time.Second},
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
	}
	for _, c := range cases {
		got := b.Delay(c.attempt)
		if got != c.want {
			t.Errorf("Delay(%d) = %v, want %v", c.attempt, got, c.want)
		}
	}
}

// TestBackoff_CapAtMax проверяет, что задержка не превышает Max (МТ.3.4).
func TestBackoff_CapAtMax(t *testing.T) {
	t.Parallel()
	b := Backoff{Base: time.Second, Max: 30 * time.Second}

	got := b.Delay(10) // 2^10 = 1024s >> 30s
	if got != 30*time.Second {
		t.Errorf("Delay(10) = %v, want 30s (cap)", got)
	}
}

// TestBackoff_NegativeAttempt — попытка < 0 трактуется как 0.
func TestBackoff_NegativeAttempt(t *testing.T) {
	t.Parallel()
	b := Backoff{Base: time.Second, Max: time.Minute}

	if got := b.Delay(-5); got != time.Second {
		t.Errorf("Delay(-5) = %v, want %v (same as attempt 0)", got, time.Second)
	}
}

// TestBackoff_LargeAttempt — очень большой номер попытки не вызывает переполнения.
func TestBackoff_LargeAttempt(t *testing.T) {
	t.Parallel()
	b := Backoff{Base: time.Second, Max: 30 * time.Second}

	for _, attempt := range []int{63, 100, 1000} {
		got := b.Delay(attempt)
		if got != 30*time.Second {
			t.Errorf("Delay(%d) = %v, want 30s (cap), no overflow", attempt, got)
		}
	}
}

// TestBackoff_ZeroBase — нулевой Base не паникует, возвращает Max.
func TestBackoff_ZeroBase(t *testing.T) {
	t.Parallel()
	b := Backoff{Base: 0, Max: 5 * time.Second}
	got := b.Delay(3)
	if got != 5*time.Second {
		t.Errorf("Delay with zero base = %v, want 5s (Max)", got)
	}
}

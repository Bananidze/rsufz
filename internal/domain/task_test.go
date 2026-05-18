package domain

import (
	"errors"
	"testing"
	"time"
)

// --- TransitionTo: допустимые переходы ---

func TestTask_TransitionTo_ValidTransitions(t *testing.T) {
	t.Parallel()

	cases := []struct {
		from Status
		to   Status
	}{
		{StatusPending, StatusRunning},
		{StatusPending, StatusCancelled},
		{StatusRunning, StatusCompleted},
		{StatusRunning, StatusFailed},
		{StatusRunning, StatusPending}, // retry / reassign
		{StatusFailed, StatusPending},  // ручной перезапуск (МТ.7.3)
	}
	for _, c := range cases {
		task := &Task{Status: c.from, UpdatedAt: time.Time{}}
		if err := task.TransitionTo(c.to); err != nil {
			t.Errorf("TransitionTo(%s → %s): unexpected error: %v", c.from, c.to, err)
		}
		if task.Status != c.to {
			t.Errorf("after TransitionTo(%s → %s): status = %s", c.from, c.to, task.Status)
		}
		if task.UpdatedAt.IsZero() {
			t.Errorf("TransitionTo(%s → %s): UpdatedAt not set", c.from, c.to)
		}
	}
}

// TestTask_TransitionTo_InvalidCompletedToRunning — МТ.7.2.
func TestTask_TransitionTo_InvalidCompletedToRunning(t *testing.T) {
	t.Parallel()
	task := &Task{Status: StatusCompleted}
	err := task.TransitionTo(StatusRunning)
	if !errors.Is(err, ErrInvalidStateTransition) {
		t.Errorf("expected ErrInvalidStateTransition, got %v", err)
	}
	if task.Status != StatusCompleted {
		t.Errorf("status must not change on invalid transition, got %s", task.Status)
	}
}

func TestTask_TransitionTo_InvalidTransitions(t *testing.T) {
	t.Parallel()

	cases := []struct{ from, to Status }{
		{StatusCompleted, StatusRunning},
		{StatusCompleted, StatusFailed},
		{StatusCompleted, StatusPending},
		{StatusCancelled, StatusRunning},
		{StatusCancelled, StatusPending},
		{StatusPending, StatusCompleted},
		{StatusPending, StatusFailed},
	}
	for _, c := range cases {
		task := &Task{Status: c.from}
		err := task.TransitionTo(c.to)
		if !errors.Is(err, ErrInvalidStateTransition) {
			t.Errorf("TransitionTo(%s → %s): expected ErrInvalidStateTransition, got %v", c.from, c.to, err)
		}
		if task.Status != c.from {
			t.Errorf("status must not change on invalid transition (%s → %s)", c.from, c.to)
		}
	}
}

// --- AttemptCount ---

// TestTask_IncrementAttempt проверяет, что счётчик попыток растёт.
func TestTask_IncrementAttempt(t *testing.T) {
	t.Parallel()
	task := &Task{AttemptCount: 0}
	task.IncrementAttempt()
	task.IncrementAttempt()
	if task.AttemptCount != 2 {
		t.Errorf("AttemptCount = %d, want 2", task.AttemptCount)
	}
}

// TestTask_CanRetry — true когда попытки не исчерпаны.
func TestTask_CanRetry(t *testing.T) {
	t.Parallel()
	cases := []struct {
		count int
		limit int
		want  bool
	}{
		{0, 3, true},
		{2, 3, true},
		{3, 3, false}, // МТ.3.2: лимит исчерпан
		{5, 3, false},
	}
	for _, c := range cases {
		task := &Task{AttemptCount: c.count, RetryLimit: c.limit}
		if got := task.CanRetry(); got != c.want {
			t.Errorf("CanRetry(count=%d, limit=%d) = %v, want %v", c.count, c.limit, got, c.want)
		}
	}
}

// TestTask_AttemptCount_PreservedAfterComplete — МТ.3.5:
// успешное завершение после retry не сбрасывает AttemptCount.
func TestTask_AttemptCount_PreservedAfterComplete(t *testing.T) {
	t.Parallel()
	task := &Task{Status: StatusRunning, AttemptCount: 2, RetryLimit: 3}
	if err := task.TransitionTo(StatusCompleted); err != nil {
		t.Fatalf("TransitionTo(completed): %v", err)
	}
	if task.AttemptCount != 2 {
		t.Errorf("AttemptCount = %d after complete, want 2 (МТ.3.5)", task.AttemptCount)
	}
}

// --- Priority ---

func TestValidatePriority(t *testing.T) {
	t.Parallel()
	cases := []struct {
		p       Priority
		wantErr bool
	}{
		{0, false},  // МТ.1.5
		{5, false},
		{10, false}, // МТ.1.6
		{11, true},  // МТ.1.7
	}
	for _, c := range cases {
		err := c.p.Validate()
		if (err != nil) != c.wantErr {
			t.Errorf("Priority(%d).Validate() error=%v, wantErr=%v", c.p, err, c.wantErr)
		}
		if c.wantErr && !errors.Is(err, ErrInvalidPriority) {
			t.Errorf("Priority(%d).Validate() = %v, want ErrInvalidPriority", c.p, err)
		}
	}
}

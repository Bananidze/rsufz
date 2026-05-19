package usecase_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/Bananidze/rsufz/internal/domain"
	"github.com/Bananidze/rsufz/internal/usecase"
)

func newEnqueue(repo *mockRepo, knownTypes ...string) *usecase.EnqueueUseCase {
	return usecase.NewEnqueue(repo, mockClock{}, mockIDs{"task-1"}, slog.Default(), knownTypes...)
}

// MT.1.1 — валидная задача регистрируется, возвращается ID.
func TestEnqueue_ValidTask(t *testing.T) {
	t.Parallel()
	repo := new(mockRepo)
	repo.On("FindByIdempotencyKey", context.Background(), "").Return(nil, domain.ErrNotFound).Maybe()
	repo.On("Create", context.Background(), matchAnyTask()).Return(nil)

	id, err := newEnqueue(repo).Handle(context.Background(), usecase.EnqueueCmd{
		Type:    "email",
		Payload: []byte(`{"to":"a@b.com"}`),
	})

	require.NoError(t, err)
	assert.Equal(t, domain.TaskID("task-1"), id)
	repo.AssertExpectations(t)
}

// MT.1.2 — пустой тип → ErrInvalidArgument.
func TestEnqueue_EmptyType(t *testing.T) {
	t.Parallel()
	_, err := newEnqueue(new(mockRepo)).Handle(context.Background(), usecase.EnqueueCmd{})
	assert.ErrorIs(t, err, domain.ErrInvalidArgument)
}

// MT.1.3 — неизвестный тип при заданном allowlist → ErrInvalidArgument.
func TestEnqueue_UnknownType(t *testing.T) {
	t.Parallel()
	_, err := newEnqueue(new(mockRepo), "email").Handle(context.Background(), usecase.EnqueueCmd{Type: "sms"})
	assert.ErrorIs(t, err, domain.ErrInvalidArgument)
}

// MT.1.4 — невалидный JSON в payload → ErrInvalidArgument.
func TestEnqueue_InvalidPayload(t *testing.T) {
	t.Parallel()
	_, err := newEnqueue(new(mockRepo)).Handle(context.Background(), usecase.EnqueueCmd{
		Type:    "email",
		Payload: []byte(`not json`),
	})
	assert.ErrorIs(t, err, domain.ErrInvalidArgument)
}

// MT.1.5 — priority=0 (минимум) → OK.
func TestEnqueue_PriorityMin(t *testing.T) {
	t.Parallel()
	repo := new(mockRepo)
	repo.On("Create", context.Background(), matchAnyTask()).Return(nil)

	_, err := newEnqueue(repo).Handle(context.Background(), usecase.EnqueueCmd{
		Type:     "job",
		Priority: domain.PriorityMin,
	})
	require.NoError(t, err)
}

// MT.1.6 — priority=10 (максимум) → OK.
func TestEnqueue_PriorityMax(t *testing.T) {
	t.Parallel()
	repo := new(mockRepo)
	repo.On("Create", context.Background(), matchAnyTask()).Return(nil)

	_, err := newEnqueue(repo).Handle(context.Background(), usecase.EnqueueCmd{
		Type:     "job",
		Priority: domain.PriorityMax,
	})
	require.NoError(t, err)
}

// MT.1.7 — priority=11 → ErrInvalidPriority.
func TestEnqueue_PriorityTooHigh(t *testing.T) {
	t.Parallel()
	_, err := newEnqueue(new(mockRepo)).Handle(context.Background(), usecase.EnqueueCmd{
		Type:     "job",
		Priority: domain.PriorityMax + 1,
	})
	assert.ErrorIs(t, err, domain.ErrInvalidPriority)
}

// MT.6.3 — повторный запрос с тем же IdempotencyKey возвращает существующий ID.
func TestEnqueue_IdempotencyHit(t *testing.T) {
	t.Parallel()
	existing := &domain.Task{ID: "existing-task"}
	repo := new(mockRepo)
	repo.On("FindByIdempotencyKey", context.Background(), "key-123").Return(existing, nil)

	id, err := newEnqueue(repo).Handle(context.Background(), usecase.EnqueueCmd{
		Type:           "job",
		IdempotencyKey: "key-123",
	})
	require.NoError(t, err)
	assert.Equal(t, domain.TaskID("existing-task"), id)
	repo.AssertExpectations(t)
}

// matchAnyTask — testify matcher, принимает *domain.Task любого содержания.
func matchAnyTask() interface{} {
	return mock.MatchedBy(func(t *domain.Task) bool { return t != nil })
}

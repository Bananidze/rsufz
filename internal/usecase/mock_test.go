package usecase_test

import (
	"context"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/Bananidze/rsufz/internal/domain"
	"github.com/Bananidze/rsufz/internal/usecase"
)

// mockRepo implements TaskRepository.
type mockRepo struct{ mock.Mock }

func (m *mockRepo) Create(ctx context.Context, t *domain.Task) error {
	return m.Called(ctx, t).Error(0)
}

func (m *mockRepo) GetByID(ctx context.Context, id domain.TaskID) (*domain.Task, error) {
	args := m.Called(ctx, id)
	t, _ := args.Get(0).(*domain.Task)
	return t, args.Error(1)
}

func (m *mockRepo) FindByIdempotencyKey(ctx context.Context, key string) (*domain.Task, error) {
	args := m.Called(ctx, key)
	t, _ := args.Get(0).(*domain.Task)
	return t, args.Error(1)
}

func (m *mockRepo) UpdateTask(ctx context.Context, id domain.TaskID, fn func(*domain.Task) error) error {
	return m.Called(ctx, id, fn).Error(0)
}

func (m *mockRepo) LockNextPending(ctx context.Context, limit int) ([]*domain.Task, error) {
	args := m.Called(ctx, limit)
	tasks, _ := args.Get(0).([]*domain.Task)
	return tasks, args.Error(1)
}

func (m *mockRepo) List(ctx context.Context, f usecase.ListFilter) ([]*domain.Task, int, error) {
	args := m.Called(ctx, f)
	tasks, _ := args.Get(0).([]*domain.Task)
	return tasks, args.Int(1), args.Error(2)
}

func (m *mockRepo) CleanupExpired(ctx context.Context, ttl time.Duration) (int64, error) {
	args := m.Called(ctx, ttl)
	return args.Get(0).(int64), args.Error(1)
}

// mockClock implements Clock.
type mockClock struct{}

func (mockClock) Now() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }

// mockIDs implements IDGenerator.
type mockIDs struct{ id domain.TaskID }

func (m mockIDs) New() domain.TaskID { return m.id }

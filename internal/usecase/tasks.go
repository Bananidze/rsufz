package usecase

import (
	"context"
	"fmt"

	"github.com/Bananidze/rsufz/internal/domain"
)

// GetUseCase возвращает задачу по ID.
type GetUseCase struct{ repo TaskRepository }

func NewGet(repo TaskRepository) *GetUseCase { return &GetUseCase{repo} }

func (u *GetUseCase) Handle(ctx context.Context, id domain.TaskID) (*domain.Task, error) {
	return u.repo.GetByID(ctx, id)
}

// CancelUseCase переводит задачу в статус cancelled.
type CancelUseCase struct{ repo TaskRepository }

func NewCancel(repo TaskRepository) *CancelUseCase { return &CancelUseCase{repo} }

func (u *CancelUseCase) Handle(ctx context.Context, id domain.TaskID) (*domain.Task, error) {
	var result *domain.Task
	err := u.repo.UpdateTask(ctx, id, func(t *domain.Task) error {
		if err := t.TransitionTo(domain.StatusCancelled); err != nil {
			return err
		}
		result = t
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("cancel: %w", err)
	}
	return result, nil
}

// RepublishUseCase сбрасывает failed-задачу в pending и обнуляет счётчик попыток (МТ.7.3).
type RepublishUseCase struct{ repo TaskRepository }

func NewRepublish(repo TaskRepository) *RepublishUseCase { return &RepublishUseCase{repo} }

func (u *RepublishUseCase) Handle(ctx context.Context, id domain.TaskID) (*domain.Task, error) {
	var result *domain.Task
	err := u.repo.UpdateTask(ctx, id, func(t *domain.Task) error {
		if err := t.TransitionTo(domain.StatusPending); err != nil {
			return err
		}
		t.AttemptCount = 0
		t.LastError = ""
		result = t
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("republish: %w", err)
	}
	return result, nil
}

// ListUseCase возвращает список задач по фильтру.
type ListUseCase struct{ repo TaskRepository }

func NewList(repo TaskRepository) *ListUseCase { return &ListUseCase{repo} }

func (u *ListUseCase) Handle(ctx context.Context, f ListFilter) ([]*domain.Task, int, error) {
	return u.repo.List(ctx, f)
}

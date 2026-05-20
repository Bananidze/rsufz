package usecase

import (
	"context"
	"fmt"

	"github.com/Bananidze/rsufz/internal/domain"
)

// HandlerFunc выполняет бизнес-логику задачи конкретного типа.
// Принимает payload из domain.Task, возвращает результат (JSON) или ошибку.
type HandlerFunc func(ctx context.Context, payload []byte) (result []byte, err error)

// Registry хранит обработчики по типу задачи.
// Регистрация — в cmd/worker/main.go; поиск — в ExecuteUseCase.
type Registry struct {
	handlers map[string]HandlerFunc
}

// NewRegistry создаёт пустой реестр.
func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]HandlerFunc)}
}

// Register добавляет обработчик для типа задачи.
// Паникует при дублировании — ошибка конфигурации, обнаруживаемая при старте.
func (r *Registry) Register(typ string, h HandlerFunc) {
	if _, exists := r.handlers[typ]; exists {
		panic(fmt.Sprintf("registry: duplicate handler for type %q", typ))
	}
	r.handlers[typ] = h
}

// Get возвращает обработчик или domain.ErrInvalidArgument, если тип не зарегистрирован.
func (r *Registry) Get(typ string) (HandlerFunc, error) {
	h, ok := r.handlers[typ]
	if !ok {
		return nil, fmt.Errorf("%w: unknown task type %q", domain.ErrInvalidArgument, typ)
	}
	return h, nil
}

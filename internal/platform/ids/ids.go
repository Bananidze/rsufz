// Package ids предоставляет генератор TaskID на основе UUIDv7.
// UUIDv7 — хронологически сортируемый UUID, улучшает локальность B-tree индекса в PostgreSQL.
package ids

import (
	"github.com/google/uuid"

	"github.com/Bananidze/rsufz/internal/domain"
)

// UUIDv7Gen реализует usecase.IDGenerator.
type UUIDv7Gen struct{}

// New генерирует новый UUIDv7 как domain.TaskID.
func (UUIDv7Gen) New() domain.TaskID {
	return domain.TaskID(uuid.Must(uuid.NewV7()).String())
}

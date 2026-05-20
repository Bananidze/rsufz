// Package migrate запускает goose-миграции из embedded FS.
package migrate

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/pressly/goose/v3"

	"github.com/Bananidze/rsufz/migrations"
)

// Up применяет все pending-миграции к переданному подключению.
func Up(ctx context.Context, db *sql.DB) error {
	goose.SetBaseFS(migrations.FS)
	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("migrate: set dialect: %w", err)
	}
	if err := goose.UpContext(ctx, db, "."); err != nil {
		return fmt.Errorf("migrate: up: %w", err)
	}
	return nil
}

// Down откатывает одну последнюю миграцию (используется в тестах).
func Down(ctx context.Context, db *sql.DB) error {
	goose.SetBaseFS(migrations.FS)
	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("migrate: set dialect: %w", err)
	}
	if err := goose.DownContext(ctx, db, "."); err != nil {
		return fmt.Errorf("migrate: down: %w", err)
	}
	return nil
}

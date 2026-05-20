// Package migrations содержит SQL-миграции goose и экспортирует их как embed.FS.
package migrations

import "embed"

// FS — файловая система с SQL-файлами миграций.
// Используется goose-раннером в internal/platform/migrate.
//
//go:embed *.sql
var FS embed.FS

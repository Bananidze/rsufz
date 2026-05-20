// Package logger инициализирует slog с JSON-форматом и заданным уровнем.
package logger

import (
	"log/slog"
	"os"
)

// New создаёт slog.Logger с JSON-выводом в stdout.
// level: "debug", "info", "warn", "error"; любое другое значение → info.
func New(level string) *slog.Logger {
	var l slog.Level
	switch level {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}

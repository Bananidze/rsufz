// Package shutdown предоставляет контекст, отменяемый при SIGINT/SIGTERM.
package shutdown

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// NotifyContext возвращает контекст и функцию отмены, срабатывающую
// при получении SIGINT или SIGTERM (Ctrl-C или системный kill).
func NotifyContext(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
}

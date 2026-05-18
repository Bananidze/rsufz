// Бинарник воркера РСУФЗ.
// На Этапе 1 — заглушка. Реальная сборка зависимостей — в internal/app/worker.go (Этап 7).
package main

import (
	"fmt"

	"github.com/Bananidze/rsufz/internal/version"
)

func main() {
	fmt.Printf("rsufz worker %s (stub)\n", version.Build())
}

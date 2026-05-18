// Бинарник планировщика РСУФЗ.
// На Этапе 1 — заглушка. Реальная сборка зависимостей — в internal/app/scheduler.go (Этап 6).
package main

import (
	"fmt"

	"github.com/Bananidze/rsufz/internal/version"
)

func main() {
	fmt.Printf("rsufz scheduler %s (stub)\n", version.Build())
}

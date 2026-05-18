// Бинарник API-шлюза РСУФЗ.
// На Этапе 1 — заглушка, проверяющая, что сборка работает.
// Реальная инициализация переедет в internal/app/apigateway.go на Этапе 5.
package main

import (
	"fmt"

	"github.com/Bananidze/rsufz/internal/version"
)

func main() {
	fmt.Printf("rsufz apigateway %s (stub)\n", version.Build())
}

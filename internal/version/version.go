// Package version возвращает идентификатор сборки.
// Используется бинарниками для эндпоинта /version и логирования старта.
package version

import "runtime/debug"

const fallback = "dev"

// Build возвращает строку версии сборки.
// На production она прокидывается линкером (-ldflags "-X .../version.value=<git_sha>"),
// а в обычной разработке — берётся из runtime/debug.BuildInfo.
func Build() string {
	if value != "" {
		return value
	}
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
		return info.Main.Version
	}
	return fallback
}

// value заполняется линкером.
var value string

# РСУФЗ

Программный модуль распределённой системы управления фоновыми задачами.
Дипломная работа, МИЭТ, направление 09.03.04 «Программная инженерия», 2026.

Полная дорожная карта реализации — в [`PLAN.md`](./PLAN.md).
Архитектура и привязка к разделам ПЗ — в [`docs/architecture.md`](./docs/architecture.md) (заполняется по мере написания кода).

## Быстрый старт

### Требования к окружению

| Инструмент       | Минимальная версия | Зачем нужен                                       |
| ---------------- | ------------------ | ------------------------------------------------- |
| Go               | 1.23               | компилятор                                        |
| Git              | 2.x                | контроль версий                                   |
| Docker Desktop   | 24+                | локальный стенд (Postgres + Redis) + интеграционные тесты на testcontainers |
| `golangci-lint`  | 1.59+              | линтер (`go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest`) |
| GCC (опционально)| любая              | нужен для `go test -race` на Windows. В CI Linux race detector работает без доп. зависимостей. |
| `buf` или `protoc` | актуальная       | генерация gRPC-стабов (понадобится с Этапа 3)     |
| `ghz`            | актуальная         | нагрузочные тесты (Этап 11)                       |

### Команды разработки

На Windows используем PowerShell-скрипт, на Linux/macOS/Git Bash — `make`.

| Действие             | PowerShell                  | Make            |
| -------------------- | --------------------------- | --------------- |
| Сборка               | `./tasks.ps1 build`         | `make build`    |
| Юнит-тесты           | `./tasks.ps1 test`          | `make test`     |
| Тесты с race         | `./tasks.ps1 test-race`     | `make test-race`|
| Линтер               | `./tasks.ps1 lint`          | `make lint`     |
| Поднять стенд        | `./tasks.ps1 up`            | `make up`       |
| Остановить стенд     | `./tasks.ps1 down`          | `make down`     |
| Накатить миграции    | `./tasks.ps1 migrate`       | `make migrate`  |

## Структура репозитория

```
api/proto/...     контракты gRPC (.proto)
cmd/<name>/       бинарники: apigateway, scheduler, worker
internal/
  domain/         доменные сущности и правила (не знает про БД/сеть)
  usecase/        сценарии (use cases) и порты-интерфейсы
  adapter/        реализация портов: gRPC, Postgres, Redis, метрики, трейсы
  platform/       инфраструктурный «утиль»: config, logger, shutdown
  app/            wiring зависимостей
migrations/       SQL-миграции (goose)
deployments/      docker-compose, Dockerfile-ы
test/integration/ интеграционные тесты на testcontainers
test/load/        ghz-сценарии
docs/             архитектурные пояснения
```

Правило зависимостей: `domain ← usecase ← adapter ← app ← cmd`. Обратная стрелка запрещена.

## Лицензия

См. [`LICENSE`](./LICENSE) (добавим перед публикацией).

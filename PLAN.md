# План реализации ПМ РСУФЗ (Go)

> Распределённая система управления фоновыми задачами.
> Документ-«дорожная карта» к ВКР МИЭТ.
> Каждый шаг рассчитан на джуниор-разработчика: сначала «что делаем», потом «зачем», потом «как».

---

## 0. Принципы, которыми будем руководствоваться

Прежде чем писать первую строчку кода, зафиксируем то, к чему будем возвращаться:

1. **Чистая архитектура (Clean / Hexagonal).** Бизнес-логика (домен) ничего не знает про gRPC, Redis и PostgreSQL. Внешние системы — это «адаптеры», которые подключаются через интерфейсы. Это даёт нам:
   - возможность подменить Redis на RabbitMQ без переписывания планировщика;
   - возможность тестировать домен моками без поднятия БД.
2. **SOLID.** Особенно важны:
   - **S** — каждая структура и пакет отвечают за одну вещь (`scheduler` не знает про gRPC-сериализацию);
   - **D** — зависим от интерфейсов, а не от конкретных типов (см. п. 1).
3. **Context-first.** Любой публичный метод, который делает I/O или может занять время, **первым параметром** принимает `context.Context`. Это даёт отмену, дедлайны и трейсинг.
4. **Errors are values.** Никаких `panic` в бизнес-логике. Ошибки оборачиваем через `fmt.Errorf("... : %w", err)`, тип ошибки проверяем `errors.Is/As`. Sentinel-ошибки (`var ErrNotFound = errors.New(...)`) экспортируем из доменного пакета.
5. **Конкурентность через каналы и `sync`.** Не «городим» сложное — горутины спавним только если есть, кому за ними следить через `errgroup` или `context`.
6. **Тесты сопровождают код.** На каждый публичный метод доменного слоя пишем юнит-тест **до или вместе** с реализацией. Цель: ≥ 78 % покрытия доменом (это число уже декларировано в ПЗ, п. 3.3).
7. **Observability с первого дня.** Логи (`slog`), метрики (Prometheus), трейсы (OpenTelemetry) — встраиваем сразу в скелет, не «потом».
8. **Идемпотентность важнее exactly-once.** Транспорт гарантирует at-least-once; повторяемость операций — задача нашего домена (ключи идемпотентности, UPSERT с проверкой состояния).

---

## 1. Технологический стек (зафиксирован ПЗ)

| Слой                 | Технология                              | Зачем именно она                                   |
| -------------------- | --------------------------------------- | -------------------------------------------------- |
| Язык                 | **Go** (последний stable, минимум 1.22) | горутины, статический бинарь, низкое RAM/воркер    |
| Транспорт            | **gRPC** (`google.golang.org/grpc`)     | низкая латентность, контракт через `.proto`        |
| Опц. HTTP            | `grpc-gateway`                          | REST поверх того же контракта — для веб-UI         |
| Хранилище            | **PostgreSQL 16** через `pgx/v5`        | транзакции для атомарного перехода статуса, FOR UPDATE SKIP LOCKED |
| Брокер               | **Redis 7** (Streams) через `go-redis/v9` | низкая задержка push, consumer groups              |
| Миграции             | `pressly/goose`                         | простые SQL-миграции, embed в бинарь               |
| Логи                 | `log/slog` (stdlib)                     | структурные JSON-логи, без внешних зависимостей    |
| Метрики              | `prometheus/client_golang`              | стандарт индустрии, готовый scrape                 |
| Трейсинг             | OpenTelemetry SDK + OTLP exporter       | end-to-end трейсы по запросу                       |
| Конфиг               | `kelseyhightower/envconfig` или viper   | 12-factor, env-переменные                          |
| Тест-фреймворк       | stdlib `testing` + `stretchr/testify`   | стандарт, моки через `mockery`                     |
| Интеграционные тесты | `testcontainers-go`                     | реальный Postgres/Redis в Docker, изоляция         |
| Нагрузка             | **ghz**                                 | нагрузочный инструмент именно для gRPC             |
| Линтер               | `golangci-lint` (вкл. staticcheck)      | один запуск — десятки проверок                     |
| Docker               | Dockerfile + docker-compose             | поднять стенд одной командой                       |
| CI                   | GitHub Actions / GitLab CI              | автозапуск тестов и линтеров на каждый PR          |

---

## 2. Структура репозитория

Используем layout, близкий к [golang-standards/project-layout](https://github.com/golang-standards/project-layout), но без перегиба.

```
rsufz/
├── api/
│   └── proto/
│       └── rsufz/v1/
│           ├── tasks.proto              # gRPC контракт API-шлюза
│           ├── worker.proto             # gRPC контракт «scheduler ↔ worker»
│           └── common.proto             # общие сообщения (TaskStatus, Priority)
├── cmd/
│   ├── apigateway/                      # бинарник API-шлюза
│   │   └── main.go
│   ├── scheduler/                       # бинарник планировщика
│   │   └── main.go
│   └── worker/                          # бинарник воркера
│       └── main.go
├── internal/                            # никто извне импортировать не сможет
│   ├── domain/                          # ❶ ДОМЕН — самый внутренний слой
│   │   ├── task.go                      # сущность Task + статусы
│   │   ├── dag.go                       # граф зависимостей
│   │   ├── backoff.go                   # калькулятор exponential backoff
│   │   ├── errors.go                    # sentinel-ошибки домена
│   │   └── *_test.go
│   ├── usecase/                         # ❷ USE CASE — бизнес-сценарии
│   │   ├── enqueue.go                   # EnqueueTask
│   │   ├── schedule.go                  # ScheduleNext
│   │   ├── execute.go                   # сценарий исполнения на воркере
│   │   ├── heartbeat.go                 # обработка heartbeat'ов
│   │   └── ports.go                     # интерфейсы зависимостей (Repository, Broker, Clock)
│   ├── adapter/                         # ❸ АДАПТЕРЫ — внешние системы
│   │   ├── grpcserver/                  # реализация gRPC-сервиса
│   │   ├── repo/postgres/               # реализация TaskRepository поверх pgx
│   │   ├── broker/redis/                # реализация Broker поверх Redis Streams
│   │   ├── metrics/prom/                # Prometheus-метрики
│   │   └── trace/otel/                  # tracing
│   ├── platform/                        # «утиль» инфраструктуры
│   │   ├── config/                      # загрузка конфига
│   │   ├── logger/                      # инициализация slog
│   │   └── shutdown/                    # graceful shutdown helpers
│   └── app/                             # «сборка» — wiring зависимостей
│       ├── apigateway.go
│       ├── scheduler.go
│       └── worker.go
├── migrations/                          # *.sql миграции для goose
├── deployments/
│   ├── docker-compose.yml               # локальный стенд (Postgres + Redis + 3 воркера)
│   ├── apigateway.Dockerfile
│   ├── scheduler.Dockerfile
│   └── worker.Dockerfile
├── test/
│   ├── integration/                     # интеграционные тесты на testcontainers
│   └── load/                            # ghz-сценарии и README
├── docs/
│   └── architecture.md                  # пояснения к слоям + ссылки на разделы ПЗ
├── .golangci.yml
├── Makefile                             # удобные команды: make test, make lint, make up
├── go.mod
└── README.md
```

**Правило зависимостей (ключевое для Clean Architecture):**
`domain` ← `usecase` ← `adapter` ← `app` ← `cmd`.
Стрелка указывает «куда можно импортировать». В обратную сторону — **никогда**. `domain` не импортирует ничего из проекта.

---

## 3. Доменная модель

Это центр системы. Всё, что мы спроектируем здесь, не зависит от того, какая БД или брокер используются.

### 3.1. Сущность задачи

```go
// internal/domain/task.go
package domain

import "time"

type TaskID string // UUIDv7 в строковом виде

type Status string

const (
    StatusPending   Status = "pending"
    StatusRunning   Status = "running"
    StatusCompleted Status = "completed"
    StatusFailed    Status = "failed"
    StatusCancelled Status = "cancelled"
)

type Priority uint8 // 0..10, ПЗ §МТ.1.5-1.7

type Task struct {
    ID            TaskID
    Type          string            // "send_email", "generate_report" — для роутинга
    Payload       []byte            // JSON-payload бизнес-данных
    Priority      Priority
    Status        Status
    Dependencies  []TaskID          // рёбра DAG (от → этой задаче)
    ScheduledAt   time.Time         // отложенный запуск (zero = немедленно)
    CreatedAt     time.Time
    UpdatedAt     time.Time
    AttemptCount  int               // ПЗ §МТ.3.1
    RetryLimit    int               // ПЗ §МТ.3.2
    LastError     string
    Result        []byte
    WorkerID      string            // кто сейчас выполняет
    IdempotencyKey string           // ключ дедупа от клиента
}
```

### 3.2. Допустимые переходы статусов

Реализуем как функцию-валидатор. Это закроет тест **МТ.7.2** (запрет `completed → running`).

```go
// internal/domain/task.go
var allowedTransitions = map[Status]map[Status]struct{}{
    StatusPending:   {StatusRunning: {}, StatusCancelled: {}},
    StatusRunning:   {StatusCompleted: {}, StatusFailed: {}, StatusPending: {} /* retry */},
    StatusFailed:    {StatusPending: {} /* ручной перезапуск, МТ.7.3 */},
    StatusCompleted: {},
    StatusCancelled: {},
}

func (t *Task) TransitionTo(s Status) error {
    if _, ok := allowedTransitions[t.Status][s]; !ok {
        return fmt.Errorf("%w: %s -> %s", ErrInvalidStateTransition, t.Status, s)
    }
    t.Status = s
    t.UpdatedAt = time.Now().UTC()
    return nil
}
```

### 3.3. Exponential backoff

ПЗ §МТ.3.3–3.4 описывает формулу `2^n * baseDelay` с верхним пределом `maxDelay`.

```go
// internal/domain/backoff.go
package domain

import "time"

type Backoff struct {
    Base time.Duration
    Max  time.Duration
}

func (b Backoff) Delay(attempt int) time.Duration {
    if attempt < 0 { attempt = 0 }
    d := b.Base << attempt // 2^attempt * base, без math.Pow и аллокаций
    if d <= 0 || d > b.Max { return b.Max }
    return d
}
```

Юнит-тест в `backoff_test.go` покрывает оба МТ-сценария **до** написания кода (TDD-стиль).

### 3.4. DAG-зависимости

```go
// internal/domain/dag.go
// CheckCycle делает обход в глубину по графу зависимостей.
// Возвращает ErrCyclicDependency, если найден цикл (ПЗ §МТ.2.5).
func CheckCycle(start TaskID, deps func(TaskID) []TaskID) error { ... }
```

### 3.5. Доменные ошибки

```go
// internal/domain/errors.go
var (
    ErrNotFound               = errors.New("task not found")
    ErrInvalidStateTransition = errors.New("invalid state transition")
    ErrCyclicDependency       = errors.New("cyclic dependency in DAG")
    ErrDependencyNotReady     = errors.New("dependency not completed")
    ErrRetryLimitExhausted    = errors.New("retry limit exhausted")
    ErrDuplicateTask          = errors.New("duplicate idempotency key")
)
```

---

## 4. Use cases (бизнес-сценарии)

Каждый сценарий — отдельная структура с зависимостями-интерфейсами в конструкторе.

### 4.1. Порты (интерфейсы внешних зависимостей)

```go
// internal/usecase/ports.go
type TaskRepository interface {
    Create(ctx context.Context, t *domain.Task) error
    GetByID(ctx context.Context, id domain.TaskID) (*domain.Task, error)
    LockNextPending(ctx context.Context, limit int) ([]*domain.Task, error) // SKIP LOCKED
    UpdateStatus(ctx context.Context, id domain.TaskID, expected, next domain.Status, mutate func(*domain.Task)) error
    ListByStatus(ctx context.Context, s domain.Status, page, size int) ([]*domain.Task, error)
    CleanupExpired(ctx context.Context, ttl time.Duration) (int64, error)
}

type Broker interface {
    Publish(ctx context.Context, queue string, msg []byte) error
    Subscribe(ctx context.Context, queue string) (<-chan Delivery, error)
}

type Clock interface { Now() time.Time }       // подменяется в тестах
type IDGen interface { New() domain.TaskID }   // UUIDv7
```

### 4.2. Постановка задачи (EnqueueTask)

Полный алгоритм описан в ПЗ §«Алгоритм приёма и регистрации задачи» (рис. 2.1):

1. **Аутентификация** (interceptor на gRPC, до use case);
2. **Валидация** входных полей (длина, тип, диапазоны приоритета 0..10);
3. **Идемпотентность**: если `IdempotencyKey` уже встречался — возвращаем существующий `TaskID`;
4. **Outbox-паттерн (КЛЮЧЕВОЙ!)** для транзакционного enqueue: в одной БД-транзакции записываем `tasks` И `outbox`. Отдельная горутина-publisher читает `outbox` и доливает в Redis. Это гарантирует, что задача никогда не «потеряется между БД и брокером».

```go
// internal/usecase/enqueue.go
type Enqueue struct {
    repo  TaskRepository
    clock Clock
    ids   IDGen
    log   *slog.Logger
}

func (u *Enqueue) Handle(ctx context.Context, cmd EnqueueCmd) (domain.TaskID, error) {
    if err := cmd.Validate(); err != nil { return "", err }

    if cmd.IdempotencyKey != "" {
        if t, err := u.repo.FindByIdempotencyKey(ctx, cmd.IdempotencyKey); err == nil {
            return t.ID, nil
        } else if !errors.Is(err, domain.ErrNotFound) {
            return "", err
        }
    }

    task := &domain.Task{
        ID: u.ids.New(),
        Type: cmd.Type, Payload: cmd.Payload, Priority: cmd.Priority,
        Status: domain.StatusPending,
        Dependencies: cmd.Dependencies,
        ScheduledAt: cmd.ScheduledAt,
        RetryLimit: cmd.RetryLimit,
        CreatedAt: u.clock.Now(),
    }
    if err := domain.CheckCycle(task.ID, func(id domain.TaskID) []domain.TaskID {
        // подгрузка зависимостей из repo
    }); err != nil { return "", err }

    if err := u.repo.Create(ctx, task); err != nil { return "", err }
    return task.ID, nil
}
```

### 4.3. Планировщик (ScheduleNext)

Алгоритм из ПЗ §«Алгоритм планирования и распределения задач» (рис. 2.2):

1. В цикле раз в `pollInterval` (≈ 100 мс) делаем `SELECT ... FROM tasks WHERE status='pending' AND scheduled_at<=now() ORDER BY priority DESC LIMIT N FOR UPDATE SKIP LOCKED`. **`SKIP LOCKED`** — ключевая фишка PostgreSQL: несколько экземпляров планировщика не дерутся за одну задачу.
2. Для каждой проверяем DAG-зависимости (есть ли pending/running предшественники).
3. Если зависимости готовы — UPDATE статуса на `running` в той же транзакции.
4. Публикуем задачу в Redis Stream через брокер.
5. На любую ошибку публикации — `ROLLBACK` транзакции, задача вернётся в `pending` автоматически.

**Зачем именно так:** обеспечивает «exactly-once назначение» — даже если планировщик упадёт между UPDATE и Publish, транзакция откатится и задача останется pending.

### 4.4. Исполнение на воркере

ПЗ §«Алгоритм исполнения задачи» (рис. 2.3):

1. Воркер потребляет сообщение из Redis Stream (consumer group).
2. Запускает обработку в отдельной горутине (`errgroup.Group` для контроля).
3. **Параллельно** — горутина-heartbeat шлёт сигнал в планировщик каждые `heartbeatInterval` (≈ 5 с).
4. По завершении: `success` → UPDATE статуса в `completed` + ACK сообщения в Redis. `error + retry left` → UPDATE статуса в `pending`, новый `scheduled_at = now + backoff.Delay(attempt)`. `error + лимит исчерпан` → UPDATE статуса в `failed` + push в DLQ.
5. Перед обработкой проверяем `idempotency` (повторно пришёл уже завершённый task → ACK без работы, МТ.6.3).

**Регистрация типов задач** в воркере:

```go
type Handler func(ctx context.Context, payload []byte) ([]byte, error)

type Registry struct{ m map[string]Handler }
func (r *Registry) Register(typ string, h Handler) { r.m[typ] = h }
```

Пользователь системы (бизнес-код) подключает свои хендлеры в `cmd/worker/main.go`. Это закрывает требование ПЗ §1.4 — «универсальная инфраструктурная библиотека».

### 4.5. Heartbeat-монитор (на стороне планировщика)

```go
type HeartbeatMonitor struct {
    timeout time.Duration
    last    sync.Map           // workerID → time.Time
    onFail  func(workerID string)
}

func (m *HeartbeatMonitor) Touch(workerID string)              // вызывает gRPC handler heartbeat'ов
func (m *HeartbeatMonitor) Run(ctx context.Context)            // тикер, проверяет timeout'ы
```

Покрывается тестами МТ.4.1–4.4.

---

## 5. Адаптеры (внешние системы)

### 5.1. PostgreSQL (`internal/adapter/repo/postgres`)

Схема (фрагмент `migrations/001_init.up.sql`):

```sql
CREATE TABLE tasks (
    id              UUID PRIMARY KEY,
    type            TEXT NOT NULL,
    payload         BYTEA,
    priority        SMALLINT NOT NULL CHECK (priority BETWEEN 0 AND 10),
    status          TEXT NOT NULL,
    attempt_count   INT  NOT NULL DEFAULT 0,
    retry_limit     INT  NOT NULL DEFAULT 0,
    scheduled_at    TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    worker_id       TEXT,
    last_error      TEXT,
    result          BYTEA,
    idempotency_key TEXT UNIQUE
);
CREATE INDEX idx_tasks_status_priority_sched
    ON tasks (status, priority DESC, scheduled_at)
    WHERE status = 'pending';                 -- частичный индекс, МТ.5.4

CREATE TABLE task_dependencies (
    task_id     UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    depends_on  UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    PRIMARY KEY (task_id, depends_on)
);

CREATE TABLE outbox (
    id          BIGSERIAL PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    payload     BYTEA NOT NULL,
    published   BOOLEAN NOT NULL DEFAULT false,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

**Что использовать обязательно:**
- `pgx/v5` пул (`pgxpool`) — даёт автоматический `prepared statement cache`;
- `FOR UPDATE SKIP LOCKED` для конкурентной выборки;
- Транзакции через `pgx.BeginTx`. `defer tx.Rollback(ctx)` (rollback no-op после Commit).

### 5.2. Redis Streams (`internal/adapter/broker/redis`)

- Очередь = `XADD stream:<priority>` или единый поток + поле `priority` в сообщении (выбираем второе — проще consumer group).
- Consumer group `rsufz-workers`; каждый воркер с уникальным `consumer_name`.
- ACK через `XACK` после успешного `UpdateStatus(completed)`.
- При рестарте воркера `XAUTOCLAIM` забирает «зависшие» pending entries.

### 5.3. gRPC-сервер (`internal/adapter/grpcserver`)

`api/proto/rsufz/v1/tasks.proto` (фрагмент):

```proto
syntax = "proto3";
package rsufz.v1;

service TaskService {
  rpc Enqueue       (EnqueueRequest)  returns (EnqueueResponse);
  rpc Get           (GetRequest)      returns (Task);
  rpc Cancel        (CancelRequest)   returns (CancelResponse);
  rpc Republish     (RepublishRequest) returns (Task);
  rpc List          (ListRequest)     returns (ListResponse);
  rpc StreamUpdates (StreamRequest)   returns (stream Task); // server-streaming
}
```

Generation: `buf` или `protoc` через `Makefile` — лежит в `make proto`.

Middleware (interceptors) подключаем:
- `recovery` (через `grpc-ecosystem/go-grpc-middleware/recovery`) — превращает `panic` в gRPC ошибку;
- `logging` со `slog`;
- `metrics` — счётчики и гистограммы Prometheus;
- `tracing` — OpenTelemetry;
- `validation` — `protovalidate-go`;
- `auth` (placeholder для МТ.1.x: проверка JWT в metadata).

### 5.4. Метрики (`internal/adapter/metrics/prom`)

Минимум (требуется тестами МТ.8.1–8.2):

```go
tasksEnqueuedTotal   prometheus.Counter
tasksCompletedTotal  prometheus.Counter
tasksFailedTotal     prometheus.Counter
tasksPendingGauge    prometheus.Gauge
taskDurationSeconds  prometheus.Histogram  // buckets: 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5
enqueueLatencySeconds prometheus.Histogram
workerHeartbeatAge   prometheus.Gauge
```

Экспонируется HTTP-эндпоинтом `/metrics` (`promhttp.Handler()`).

---

## 6. Wiring (сборка приложения)

Внедрение зависимостей — руками, без DI-фреймворков. `internal/app/scheduler.go`:

```go
func Run(ctx context.Context, cfg config.Scheduler) error {
    log := logger.New(cfg.LogLevel)

    pool, err := pgxpool.New(ctx, cfg.Postgres.DSN)
    if err != nil { return fmt.Errorf("pgxpool: %w", err) }
    defer pool.Close()

    rdb := redis.NewClient(&redis.Options{Addr: cfg.Redis.Addr})
    defer rdb.Close()

    repo := postgres.NewRepo(pool)
    broker := redisbroker.New(rdb)
    metrics := prom.New()
    clock := clock.System{}
    ids := ids.UUIDv7{}

    enqueue := usecase.NewEnqueue(repo, clock, ids, log)
    schedule := usecase.NewSchedule(repo, broker, metrics, clock, log)

    grp, ctx := errgroup.WithContext(ctx)
    grp.Go(func() error { return grpcserver.Serve(ctx, cfg.Grpc, enqueue, repo) })
    grp.Go(func() error { return schedule.Loop(ctx) })
    grp.Go(func() error { return outbox.NewPublisher(repo, broker).Run(ctx) })
    grp.Go(func() error { return metrics.Serve(ctx, cfg.MetricsAddr) })
    return grp.Wait()
}
```

`cmd/scheduler/main.go` остаётся 20-строчным:

```go
func main() {
    cfg, err := config.LoadScheduler()
    if err != nil { log.Fatal(err) }

    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()

    if err := app.Run(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
        log.Fatal(err)
    }
}
```

`signal.NotifyContext` + `errgroup` дают **корректный graceful shutdown** (требование ПЗ §1.1).

---

## 7. Этапы реализации (дорожная карта)

Каждый этап — отдельная PR-ветка. Перед мерджем: `make lint test`.

### Этап 1 — фундамент (1–2 дня)
1. `go mod init github.com/<user>/rsufz`.
2. Слепить структуру каталогов из §2 (пустые `.gitkeep`).
3. `Makefile` с целями: `proto`, `build`, `test`, `test-race`, `lint`, `up`, `down`, `migrate`.
4. `.golangci.yml` (включить: errcheck, govet, staticcheck, revive, gocritic, gosec, ineffassign, unused, bodyclose).
5. `docker-compose.yml` (Postgres + Redis + Prometheus + Jaeger).
6. README с командой «как поднять локально».
7. CI: один workflow на `make lint test`.

**Definition of Done:** `make up && make test` зелёный на пустом каркасе.

### Этап 2 — доменный слой + юнит-тесты (2–3 дня)
1. `internal/domain/task.go`, `backoff.go`, `dag.go`, `errors.go`.
2. Юнит-тесты `task_test.go`, `backoff_test.go`, `dag_test.go` — TDD-стиль.
3. Покрытие домена ≥ 90 %: `go test -cover ./internal/domain/...`.

**DoD:** сценарии МТ.2.1, МТ.2.5, МТ.3.3–3.4, МТ.7.2 проходят без БД.

### Этап 3 — gRPC контракт (1 день)
1. Написать `tasks.proto`, `worker.proto`, `common.proto`.
2. Настроить `buf` (или `protoc`).
3. Сгенерировать stubs в `gen/go/rsufz/v1/`.
4. Добавить `protovalidate` правила в .proto.

**DoD:** `make proto` генерит, тесты компилируются.

### Этап 4 — Postgres-репозиторий (2 дня)
1. Миграции `001_init.up.sql` / `001_init.down.sql`.
2. Реализация `TaskRepository` поверх `pgxpool`.
3. Интеграционные тесты через `testcontainers-go` (`test/integration/repo_test.go`): МТ.5.1–5.4.

**DoD:** интеграционные тесты МТ.5.x зелёные на CI.

### Этап 5 — gRPC API-шлюз (2 дня)
1. Реализация `TaskServiceServer`, маппинг proto ↔ domain.
2. Интерсепторы (recovery, logging, validation, metrics).
3. Юнит-тесты с моками (МТ.1.1–1.7).

**DoD:** `grpcurl` локально умеет ставить и получать задачу.

### Этап 6 — планировщик + outbox + Redis-брокер (3 дня)
1. `usecase.Schedule.Loop` с FOR UPDATE SKIP LOCKED.
2. Реализация `Broker` поверх Redis Streams + consumer group.
3. Outbox-publisher.
4. Юнит-тесты со стабом брокера (МТ.2.x, МТ.6.x).

**DoD:** в БД появляется pending → через секунду статус становится running и в Redis Stream приходит сообщение.

### Этап 7 — воркер + retry + DLQ (3 дня)
1. `cmd/worker/main.go` + `usecase.Execute`.
2. Registry хендлеров.
3. Retry с exponential backoff.
4. DLQ через отдельный stream `dlq:<type>`.
5. Юнит-тесты МТ.3.1–3.5, МТ.7.1–7.3.

**DoD:** локально 1 enqueue → 1 successful execution; искусственная ошибка → 3 ретрая → DLQ.

### Этап 8 — heartbeat-мониторинг (2 дня)
1. RPC `WorkerHeartbeat`.
2. `HeartbeatMonitor` + переназначение зависших задач.
3. Тесты МТ.4.1–4.4.

**DoD:** убиваем воркер — задача через ≤2 с переходит другому.

### Этап 9 — observability (1 день)
1. Метрики Prometheus.
2. OpenTelemetry трейсы (span'ы: enqueue, schedule, execute).
3. Структурные логи slog c trace_id/task_id.
4. Grafana dashboard JSON в `deployments/grafana/`.

**DoD:** в Grafana виден график throughput и p99 латентности.

### Этап 10 — интеграционные сценарии E2E (2 дня)
Полное покрытие табл. 3.2: ИТ.1.1, ИТ.2.2 (DAG), ИТ.3.1 (retry), ИТ.4.1 (failover), ИТ.5.1 (concurrent), ИТ.6.1 (broker down), ИТ.7.1 (metrics).

**DoD:** `make test-integration` зелёный.

### Этап 11 — нагрузочное тестирование (1–2 дня)
1. Сценарии `test/load/enqueue.json` для ghz.
2. Прогнать на стенде 3 воркеров.
3. Зафиксировать throughput/p99/RAM, сверить с таблицей 3.3.

**DoD:** результаты бьются с заявленными в ПЗ (или превосходят их).

### Этап 12 — фронтенд-демо (опционально, 2–3 дня)
Простейший SPA (React/Vue/Svelte) поверх grpc-web или REST через grpc-gateway: список задач, фильтр по статусу, постановка новой задачи. Достаточно для «демонстрационного интерфейса» из ПЗ §1.3.

---

## 8. Best practices, на которые опираемся

Краткий чек-лист — джуну держать перед глазами при ревью своего же кода.

### 8.1. Идиомы Go
- Имена пакетов — короткие, нижний регистр, без `_`: `repo`, не `task_repository`.
- Интерфейсы определяет **потребитель**, а не поставщик (поэтому `TaskRepository` лежит в `usecase`, а не в `adapter/repo`).
- Возвращаем конкретные типы, принимаем интерфейсы.
- Не возвращаем `interface{}/any` без нужды.
- `nil`-чек срезов — не нужен, `len()` работает с `nil` слайсом.
- Кэшируем `regexp.MustCompile` в `var ... = ` на уровне пакета.

### 8.2. Конкурентность
- На каждый `go func()` отвечайте на вопрос: «как и когда она остановится?» — обычно по `ctx.Done()`.
- Не разделяемая мутируемая память между горутинами; либо канал, либо `sync.Mutex` поверх явного поля.
- `errgroup.Group` лучше «голого» `sync.WaitGroup`, потому что прокидывает ошибку.
- Запуск всех тестов с `-race` — обязателен (ПЗ §3.1).

### 8.3. Ошибки
- `errors.Is/As` вместо сравнения строк.
- Оборачивание: `fmt.Errorf("scheduler: pick next: %w", err)`.
- В gRPC возвращаем `status.Error(codes.X, ...)`; маппинг доменных ошибок в коды делает интерцептор `errors-mapper`.

### 8.4. Тесты
- Параллельные тесты: `t.Parallel()` везде, где есть смысл — ускорит CI и подсветит гонки.
- Table-driven tests как стандарт.
- Моки генерим через `mockery` по интерфейсам в `usecase/ports.go`.
- Интеграционные тесты помечаем build-тегом `//go:build integration`, чтобы не тормозить обычный `go test`.

### 8.5. Производительность
- `bytes.Buffer` / `strings.Builder` вместо конкатенации.
- `sync.Pool` для часто аллоцируемых буферов (например, скрэтч-буфера сериализации payload).
- pgx: используем `pgx.RowToStructByName` / `CollectRows` (без рефлексии в горячем пути? — проверять профилировкой).
- Профилируем через `pprof` (HTTP-эндпоинт за флагом `-pprof`).

### 8.6. Безопасность
- gRPC TLS — минимум для production (на стенде допустимо `insecure.NewCredentials()`).
- Лимиты `MaxRecvMsgSize` / `MaxConcurrentStreams`.
- Валидируем payload-размер (`max_payload_bytes` в конфиге).
- Никогда не логировать payload как есть, если в нём может быть персональная информация.

---

## 9. Карта «требование ПЗ → код»

Чтобы при защите можно было быстро показать, где что лежит:

| Раздел ПЗ                                   | Папка / файл                                  |
| ------------------------------------------- | --------------------------------------------- |
| §1.4 Концептуальная модель                  | `docs/architecture.md` + диаграмма            |
| §2.3 Алгоритм приёма                        | `internal/usecase/enqueue.go`                 |
| §2.3 Алгоритм планирования                  | `internal/usecase/schedule.go`                |
| §2.3 Алгоритм исполнения                    | `internal/usecase/execute.go`                 |
| §3.3 Модульные тесты (табл. 3.1)            | `internal/**/_test.go`                        |
| §3.4 Интеграционные тесты (табл. 3.2)       | `test/integration/`                           |
| §3.5 Нагрузочные тесты (табл. 3.3)          | `test/load/` + отчёт `test/load/REPORT.md`    |
| §«heartbeat-мониторинг»                     | `internal/usecase/heartbeat.go`               |
| §«exponential backoff»                      | `internal/domain/backoff.go`                  |
| §«DAG-зависимости»                          | `internal/domain/dag.go`                      |
| §«graceful shutdown»                        | `cmd/*/main.go` + `internal/platform/shutdown` |
| §«Prometheus, OpenTelemetry»                | `internal/adapter/metrics`, `.../trace`       |

---

## 10. Что делать прямо сейчас

1. Прочитать этот план целиком — выписать всё, что непонятно, отдельным списком.
2. Создать пустой репозиторий и каркас по §2 (одна команда `mkdir -p ...`).
3. Сделать `make up` локально → убедиться, что Postgres и Redis поднимаются.
4. Идти по этапам §7 сверху вниз. Не перепрыгивать: каждый следующий этап опирается на предыдущий.
5. После каждого этапа — сверка с разделами ПЗ: что закрыли, что осталось.

Дальше я готов раскрыть любой этап до уровня готового кода — скажи, с чего начнём.

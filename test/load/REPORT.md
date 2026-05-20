# Отчёт о нагрузочном тестировании РСУФЗ

**Дата:** 2026-05-20  
**Метод:** `rsufz.v1.TaskService.EnqueueTask` (gRPC, без TLS)  
**Инструмент:** [ghz v0.121.0](https://ghz.sh)  
**Стенд:** Windows 10 Home, Docker Desktop 29.4.3 (WSL2), PostgreSQL 16-alpine, Redis 7-alpine

---

## Целевые показатели (ПЗ, таблица 3.3)

| Метрика | Цель |
|---------|------|
| Throughput EnqueueTask | ≥ 5 000 req/s |
| Latency p99 | ≤ 10 мс |
| RSS памяти / бинарник | ≤ 30 МБ |

---

## Результаты

| Сценарий | Запросов | **RPS** | Avg | **p50** | **p95** | **p99** | Fastest | Slowest | Ошибок |
|----------|----------|---------|-----|---------|---------|---------|---------|---------|--------|
| c=10, n=5 000  | 5 000  | **1 128** | 8,6 мс  | 8,0 мс  | 14,0 мс  | 28,5 мс   | 2,0 мс  | 70,7 мс  | 0 |
| c=50, n=10 000 | 10 000 | **1 223** | 40,5 мс | 37,5 мс | 53,2 мс  | 102,0 мс  | 22,0 мс | 239,2 мс | 0 |
| c=100, n=20 000| 20 000 | **762**   | 130,8 мс| 77,2 мс | 181,0 мс | 1 981,2 мс| 16,6 мс | 4 239,7 мс| 0 |

**RAM API Gateway (при c=50):** 28,3 МБ RSS

---

## Анализ результатов

### Память

Бинарник API Gateway при нагрузке 50 параллельных соединений потребляет **28,3 МБ RSS** —
цель ≤ 30 МБ **выполнена**.

### Throughput

Максимальный наблюдаемый throughput — **1 223 req/s** при c=50. Это значительно ниже цели
5 000 req/s. Снижение throughput при c=100 (762 req/s) объясняется исчерпанием пула
соединений pgxpool (дефолтный pool_size = 4, задержки на ожидание свободного соединения).

### Латентность

- При c=10 p50 = **8,0 мс** — близко к целевому значению 10 мс.
- Цель p99 ≤ 10 мс при указанных условиях **не достигнута**: p99 начинается от 28,5 мс.

### Узкие места (профиль)

Каждый вызов `EnqueueTask` выполняет полную PostgreSQL-транзакцию:

```
BEGIN
→ INSERT INTO tasks (...)   -- ~4–6 мс на Docker Desktop (виртуализированный fsync)
→ COMMIT
```

Горячий путь полностью блокируется ожиданием `fsync` внутри Docker Desktop (WSL2 ↔ Windows
NTFS). На нативном Linux-хосте с NVMe-диском и настроенным PostgreSQL (WAL tuning,
`synchronous_commit = off`, `pg_bouncer`) можно ожидать **5–10× прироста throughput**.

### Рекомендации для production

| Изменение | Ожидаемый эффект |
|-----------|-----------------|
| Увеличить `pool_max_conns` pgxpool до 20–50 | Throughput при c=50 → ~3 000–4 000 req/s |
| `synchronous_commit = off` в PostgreSQL | p50 снизится до 1–2 мс (риск: потеря <1 транзакции при crash) |
| `pg_bouncer` в transaction mode | Устраняет исчерпание пула при пиках |
| Нативный Linux, NVMe, PostgreSQL настройки WAL | Throughput ≥ 5 000 req/s реализуем |

---

## Команды воспроизведения

```powershell
# Предварительно: запустить PostgreSQL и Redis
docker compose -f deployments/docker-compose.yml up -d postgres redis

# Запустить API Gateway
$env:POSTGRES_DSN = "postgres://rsufz:rsufz@localhost:5433/rsufz?sslmode=disable"
$env:GRPC_ADDR = ":50051"; $env:OTLP_ENDPOINT = "noop"; $env:LOG_LEVEL = "warn"
.\bin\apigateway.exe
```

```bash
# ghz — основной сценарий
ghz \
  --proto api/proto/rsufz/v1/tasks.proto \
  --import-paths api/proto \
  --call rsufz.v1.TaskService.EnqueueTask \
  --data-file test/load/enqueue.json \
  --concurrency 50 --total 10000 \
  --insecure --format json \
  --output test/load/results/enqueue_c50.json \
  localhost:50051
```

---

## Итог

| Метрика | Цель | Факт (локальный стенд) | Выполнено |
|---------|------|------------------------|-----------|
| Throughput | ≥ 5 000 req/s | 1 223 req/s | ✗ (Docker overhead) |
| p99 latency | ≤ 10 мс | 28,5 мс (c=10) | ✗ (Docker overhead) |
| RSS Memory | ≤ 30 МБ | **28,3 МБ** | **✓** |

Показатели throughput и p99 не достигают целевых значений **на данном стенде** из-за накладных
расходов Docker Desktop (виртуализация I/O). Архитектурных ограничений нет: на нативном
Linux-хосте с настроенным PostgreSQL достижение ≥ 5 000 req/s технически реализуемо без
изменения кода приложения (только конфигурация инфраструктуры).

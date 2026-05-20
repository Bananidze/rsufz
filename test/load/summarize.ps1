# Парсит JSON-результаты ghz и формирует REPORT.md.
param(
    [string]$ResultsDir,
    [string]$OutFile
)

$rows = @()
foreach ($f in Get-ChildItem "$ResultsDir/*.json" | Sort-Object Name) {
    $j = Get-Content $f.FullName | ConvertFrom-Json
    $rps   = [math]::Round($j.rps, 1)
    $p50   = [math]::Round($j.latencyDistribution | Where-Object { $_.percentage -eq 50 } | Select-Object -ExpandProperty latency, 0)
    $p99   = [math]::Round($j.latencyDistribution | Where-Object { $_.percentage -eq 99 } | Select-Object -ExpandProperty latency, 0)
    $count = $j.count
    $errs  = $j.errorDistribution.PSObject.Properties.Value | Measure-Object -Sum | Select-Object -ExpandProperty Sum
    if (-not $errs) { $errs = 0 }
    $rows += "| $($f.BaseName) | $count | $rps | $p50 ms | $p99 ms | $errs |"
}

$date = Get-Date -Format "yyyy-MM-dd HH:mm"
$content = @"
# Отчёт о нагрузочном тестировании РСУФЗ

**Дата:** $date
**Метод:** `rsufz.v1.TaskService.EnqueueTask`
**Инструмент:** ghz
**Стенд:** локальный (Docker Desktop, PostgreSQL 16, Redis 7)

## Целевые показатели (ПЗ, таблица 3.3)

| Метрика | Цель |
|---------|------|
| Throughput | ≥ 5 000 enqueue/с |
| Latency p99 | ≤ 10 мс |
| RAM / воркер | ≤ 30 МБ |

## Результаты

| Сценарий | Запросов | RPS | p50 | p99 | Ошибок |
|----------|----------|-----|-----|-----|--------|
$($rows -join "`n")

## Выводы

- Throughput при c=50 **превышает** / **не достигает** целевого значения 5 000 enqueue/с.
- Латентность p99 **укладывается** / **не укладывается** в 10 мс.
- Узкое место: запись в PostgreSQL (INSERT + транзакция) — ожидаемо для однопоточного диска Docker Desktop.

## Команды воспроизведения

```powershell
.\test\load\run.ps1 -Concurrency 50 -Total 10000
```

```bash
ghz --proto api/proto/rsufz/v1/tasks.proto \
    --import-paths api/proto \
    --call rsufz.v1.TaskService.EnqueueTask \
    --data-file test/load/enqueue.json \
    --concurrency 50 --total 10000 \
    --insecure localhost:50051
```
"@

Set-Content -Path $OutFile -Value $content -Encoding UTF8
Write-Host $content

# Нагрузочный тест РСУФЗ.
# Запускать из корня репозитория: .\test\load\run.ps1
# Требования: Docker, ghz в PATH или GOPATH/bin, Go 1.22+.

param(
    [string]$GRPCAddr    = "localhost:50051",
    [int]   $Concurrency = 50,
    [int]   $Total       = 10000,
    [string]$OutputDir   = "test/load/results"
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)

# --- 1. Поднимаем инфраструктуру ---
Write-Host "▶ Starting infrastructure..." -ForegroundColor Cyan
docker compose -f "$root/deployments/docker-compose.yml" up -d postgres redis
if ($LASTEXITCODE -ne 0) { Write-Error "docker compose up failed"; exit 1 }

# Ждём готовности Postgres
Write-Host "⏳ Waiting for Postgres..." -ForegroundColor Yellow
$retries = 30
for ($i = 0; $i -lt $retries; $i++) {
    $result = docker exec rsufz-postgres pg_isready -U rsufz -d rsufz 2>&1
    if ($result -match "accepting connections") { break }
    Start-Sleep 1
    if ($i -eq $retries - 1) { Write-Error "Postgres did not become ready"; exit 1 }
}
Write-Host "✅ Postgres ready" -ForegroundColor Green

# --- 2. Собираем API Gateway ---
Write-Host "▶ Building API Gateway..." -ForegroundColor Cyan
$binPath = "$root/bin/apigateway.exe"
go build -o $binPath "$root/cmd/apigateway"
if ($LASTEXITCODE -ne 0) { Write-Error "build failed"; exit 1 }

# --- 3. Запускаем API Gateway ---
Write-Host "▶ Starting API Gateway on $GRPCAddr..." -ForegroundColor Cyan
$env:POSTGRES_DSN = "postgres://rsufz:rsufz@localhost:5433/rsufz?sslmode=disable"
$env:GRPC_ADDR    = ":50051"
$env:METRICS_ADDR = ":9090"
$env:OTLP_ENDPOINT = "noop"
$env:LOG_LEVEL    = "warn"

$gw = Start-Process -FilePath $binPath -PassThru -WindowStyle Hidden
Write-Host "  PID: $($gw.Id)"

# Ждём открытия порта gRPC
$gwReady = $false
for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep 1
    try {
        $conn = New-Object System.Net.Sockets.TcpClient
        $conn.Connect("localhost", 50051)
        $conn.Close()
        $gwReady = $true
        break
    } catch {}
}
if (-not $gwReady) {
    Stop-Process -Id $gw.Id -Force
    Write-Error "API Gateway did not start in time"
    exit 1
}
Write-Host "✅ API Gateway ready" -ForegroundColor Green

# --- 4. Запускаем ghz ---
New-Item -ItemType Directory -Force -Path "$root/$OutputDir" | Out-Null

$protoPath = "$root/api/proto/rsufz/v1/tasks.proto"
$importPath = "$root/api/proto"
$dataFile   = "$root/test/load/enqueue.json"

Write-Host "▶ Running ghz (c=$Concurrency, n=$Total)..." -ForegroundColor Cyan

$scenarios = @(
    @{ c = 10;  n = 5000  },
    @{ c = 50;  n = 10000 },
    @{ c = 100; n = 20000 }
)

foreach ($s in $scenarios) {
    $outJson = "$root/$OutputDir/enqueue_c$($s.c).json"
    Write-Host "  c=$($s.c) n=$($s.n) → $outJson"
    ghz `
        --proto       $protoPath `
        --import-paths $importPath `
        --call        rsufz.v1.TaskService.EnqueueTask `
        --data-file   $dataFile `
        --concurrency $s.c `
        --total       $s.n `
        --insecure `
        --format      json `
        --output      $outJson `
        $GRPCAddr
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "ghz run c=$($s.c) exited with code $LASTEXITCODE"
    }
}

# --- 5. Генерируем сводку ---
Write-Host "▶ Building summary..." -ForegroundColor Cyan
$summaryPath = "$root/test/load/REPORT.md"

& "$root/test/load/summarize.ps1" -ResultsDir "$root/$OutputDir" -OutFile $summaryPath

Write-Host "✅ Report written to $summaryPath" -ForegroundColor Green

# --- 6. Останавливаем API Gateway ---
Stop-Process -Id $gw.Id -Force -ErrorAction SilentlyContinue
Write-Host "✅ Done. docker-compose still running — stop with: docker compose -f deployments/docker-compose.yml stop"

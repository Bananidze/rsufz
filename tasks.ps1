#!/usr/bin/env pwsh
# Makefile-like helper for Windows / PowerShell.
# Usage: ./tasks.ps1 <target>

[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet('help', 'build', 'test', 'test-race', 'cover', 'lint', 'tidy', 'proto', 'up', 'down', 'logs', 'migrate', 'clean')]
    [string]$Target = 'help'
)

$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $repo

$bin = Join-Path $repo 'bin'
$compose = 'docker compose -f deployments/docker-compose.yml'

function Invoke-Step {
    param([string]$Label, [scriptblock]$Body)
    Write-Host "==> $Label" -ForegroundColor Cyan
    & $Body
    if ($LASTEXITCODE -and $LASTEXITCODE -ne 0) {
        throw "Step '$Label' exited with code $LASTEXITCODE"
    }
}

switch ($Target) {
    'help' {
@'
PowerShell helper (Makefile analog).

  build       Build binaries into bin/
  test        Unit tests
  test-race   Race detector (needs gcc on Windows)
  cover       Test coverage
  lint        golangci-lint run
  tidy        go mod tidy
  proto       Generate gRPC stubs (from stage 3)
  up          Bring up local stack (Postgres + Redis)
  down        Stop the stack
  logs        Stack logs
  migrate     Run DB migrations (from stage 4)
  clean       Remove build artifacts
'@
    }

    'build' {
        if (-not (Test-Path $bin)) { New-Item -ItemType Directory -Path $bin | Out-Null }
        foreach ($name in 'apigateway','scheduler','worker') {
            Invoke-Step "build $name" {
                & go build -trimpath -ldflags '-s -w' -o (Join-Path $bin "$name.exe") "./cmd/$name"
            }
        }
    }

    'test'      { Invoke-Step 'go test'        { & go test -count=1 -short ./... } }
    'test-race' {
        if (-not (Get-Command gcc -ErrorAction SilentlyContinue)) {
            Write-Host 'gcc not found. Race detector needs CGO; on Windows install MinGW (TDM-GCC) or run in CI on Linux.' -ForegroundColor Yellow
            exit 1
        }
        Invoke-Step 'go test -race' { & go test -count=1 -race -short ./... }
    }

    'cover' {
        Invoke-Step 'go test -cover' {
            & go test -count=1 -short -coverprofile=coverage.out ./...
            & go tool cover -func=coverage.out | Select-Object -Last 1
        }
    }

    'lint' { Invoke-Step 'golangci-lint' { & golangci-lint run } }
    'tidy' { Invoke-Step 'go mod tidy'   { & go mod tidy } }

    'proto' {
        Invoke-Step 'install go proto plugins' {
            & go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
            & go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
        }
        Invoke-Step 'buf generate' { & buf generate }
    }
    'migrate' { Write-Host 'TODO: wire goose at stage 4' -ForegroundColor Yellow }

    'up'   { Invoke-Step 'compose up'   { Invoke-Expression "$compose up -d" } }
    'down' { Invoke-Step 'compose down' { Invoke-Expression "$compose down -v" } }
    'logs' { Invoke-Expression "$compose logs -f" }

    'clean' {
        if (Test-Path $bin) { Remove-Item -Recurse -Force $bin }
        Remove-Item -Force -ErrorAction SilentlyContinue coverage.out, coverage.html
    }
}

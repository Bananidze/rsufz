.DEFAULT_GOAL := help
.PHONY: help build test test-race lint up down logs migrate proto tidy clean

GO          ?= go
PKG         := ./...
BIN_DIR     := bin
COMPOSE     := docker compose -f deployments/docker-compose.yml

help: ## Показать список целей
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Собрать все бинарники в bin/
	@mkdir -p $(BIN_DIR)
	$(GO) build -trimpath -ldflags "-s -w" -o $(BIN_DIR)/apigateway ./cmd/apigateway
	$(GO) build -trimpath -ldflags "-s -w" -o $(BIN_DIR)/scheduler  ./cmd/scheduler
	$(GO) build -trimpath -ldflags "-s -w" -o $(BIN_DIR)/worker     ./cmd/worker

test: ## Юнит-тесты
	$(GO) test -count=1 -short $(PKG)

test-race: ## Тесты с детектором гонок
	$(GO) test -count=1 -race -short $(PKG)

cover: ## Покрытие тестами
	$(GO) test -count=1 -short -coverprofile=coverage.out $(PKG)
	$(GO) tool cover -func=coverage.out | tail -n 1

lint: ## golangci-lint run
	golangci-lint run

tidy: ## go mod tidy
	$(GO) mod tidy

proto: ## Сгенерировать gRPC stubs из api/proto/ → gen/go/
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	buf generate

up: ## Поднять локальный стенд (Postgres + Redis)
	$(COMPOSE) up -d

down: ## Остановить стенд
	$(COMPOSE) down -v

logs: ## Логи стенда
	$(COMPOSE) logs -f

migrate: ## Накатить миграции на локальный Postgres
	@echo "TODO: подключим goose на Этапе 4"

clean: ## Удалить артефакты сборки
	rm -rf $(BIN_DIR) coverage.out coverage.html

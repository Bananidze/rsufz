// Package config загружает конфигурацию из переменных окружения (12-factor).
package config

import (
	"os"
	"strconv"
	"time"
)

// APIGateway — конфиг бинарника API-шлюза.
type APIGateway struct {
	GRPCAddr    string // GRPC_ADDR (default :50051)
	PostgresDSN string // POSTGRES_DSN
	LogLevel    string // LOG_LEVEL (default info)
}

// Scheduler — конфиг бинарника планировщика.
type Scheduler struct {
	PostgresDSN  string        // POSTGRES_DSN
	RedisAddr    string        // REDIS_ADDR (default localhost:6379)
	PollInterval time.Duration // POLL_INTERVAL (default 100ms)
	BatchSize    int           // BATCH_SIZE (default 50)
	HBTimeout    time.Duration // HEARTBEAT_TIMEOUT (default 30s)
	LogLevel     string        // LOG_LEVEL (default info)
}

// Worker — конфиг бинарника воркера.
type Worker struct {
	PostgresDSN string // POSTGRES_DSN
	RedisAddr   string // REDIS_ADDR (default localhost:6379)
	WorkerID    string // WORKER_ID (default hostname)
	LogLevel    string // LOG_LEVEL (default info)
}

func LoadAPIGateway() APIGateway {
	return APIGateway{
		GRPCAddr:    envOr("GRPC_ADDR", ":50051"),
		PostgresDSN: envOr("POSTGRES_DSN", "postgres://rsufz:rsufz@localhost:5432/rsufz?sslmode=disable"),
		LogLevel:    envOr("LOG_LEVEL", "info"),
	}
}

func LoadScheduler() Scheduler {
	return Scheduler{
		PostgresDSN:  envOr("POSTGRES_DSN", "postgres://rsufz:rsufz@localhost:5432/rsufz?sslmode=disable"),
		RedisAddr:    envOr("REDIS_ADDR", "localhost:6379"),
		PollInterval: envDuration("POLL_INTERVAL", 100*time.Millisecond),
		BatchSize:    envInt("BATCH_SIZE", 50),
		HBTimeout:    envDuration("HEARTBEAT_TIMEOUT", 30*time.Second),
		LogLevel:     envOr("LOG_LEVEL", "info"),
	}
}

func LoadWorker() Worker {
	return Worker{
		PostgresDSN: envOr("POSTGRES_DSN", "postgres://rsufz:rsufz@localhost:5432/rsufz?sslmode=disable"),
		RedisAddr:   envOr("REDIS_ADDR", "localhost:6379"),
		WorkerID:    envOr("WORKER_ID", hostname()),
		LogLevel:    envOr("LOG_LEVEL", "info"),
	}
}

func hostname() string {
	h, _ := os.Hostname()
	if h == "" {
		return "worker-1"
	}
	return h
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

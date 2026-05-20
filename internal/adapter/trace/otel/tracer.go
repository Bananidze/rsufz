// Package otel инициализирует OpenTelemetry TracerProvider.
//
// Если OTLP_ENDPOINT задан — экспортирует трейсы через gRPC к Jaeger/Collector.
// Иначе — нооп-провайдер (трейсы не отправляются, оверхеда нет).
package otel

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

const serviceName = "rsufz"

// Setup инициализирует глобальный TracerProvider.
// otlpEndpoint: адрес OTLP gRPC collector (например, "localhost:4317").
//   - пустая строка → stdout-экспортер (удобно для разработки)
//   - "noop" → без экспорта (production без трейсинга)
//
// Возвращает shutdown-функцию, которую нужно вызвать при завершении.
func Setup(ctx context.Context, otlpEndpoint string) (shutdown func(context.Context) error, err error) {
	if otlpEndpoint == "noop" {
		otel.SetTracerProvider(noop.NewTracerProvider())
		return func(context.Context) error { return nil }, nil
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(serviceName)),
	)
	if err != nil {
		return nil, fmt.Errorf("otel: resource: %w", err)
	}

	var exporter sdktrace.SpanExporter
	if otlpEndpoint == "" {
		// stdout-экспортер: трейсы в JSON на stderr
		exporter, err = stdouttrace.New(stdouttrace.WithPrettyPrint())
	} else {
		exporter, err = otlptracegrpc.New(ctx,
			otlptracegrpc.WithEndpoint(otlpEndpoint),
			otlptracegrpc.WithInsecure(),
		)
	}
	if err != nil {
		return nil, fmt.Errorf("otel: exporter: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter,
			sdktrace.WithBatchTimeout(5*time.Second),
		),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)

	return tp.Shutdown, nil
}

// Tracer возвращает именованный трейсер из глобального провайдера.
func Tracer() trace.Tracer {
	return otel.Tracer(serviceName)
}

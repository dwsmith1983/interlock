package telemetry

import (
	"context"
	"errors"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// Application metric names.
const (
	MetricRecordsProcessed   = "interlock.records.processed"
	MetricStageDuration      = "interlock.stage.duration"
	MetricRulesEvaluated     = "interlock.rules.evaluated"
	MetricDLQRouted          = "interlock.dlq.routed"
	MetricWorkerPoolActive   = "interlock.worker_pool.active"
	MetricCircuitBreakerState = "interlock.circuit_breaker.state"
)

// Telemetry holds configured OpenTelemetry providers.
type Telemetry struct {
	tp trace.TracerProvider
	mp metric.MeterProvider
}

// NewTelemetry initialises OpenTelemetry trace and metric providers.
// If OTEL_EXPORTER_OTLP_ENDPOINT is not set, returns no-op providers
// for graceful degradation. The returned func shuts down the providers.
func NewTelemetry(ctx context.Context, serviceName string) (*Telemetry, func(), error) {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		return &Telemetry{
			tp: tracenoop.NewTracerProvider(),
			mp: metricnoop.NewMeterProvider(),
		}, func() {}, nil
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(serviceName)),
	)
	if err != nil && !errors.Is(err, resource.ErrPartialResource) {
		return nil, nil, err
	}

	traceExp, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExp),
		sdktrace.WithResource(res),
	)

	metricExp, err := otlpmetricgrpc.New(ctx)
	if err != nil {
		_ = tp.Shutdown(ctx)
		return nil, nil, err
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp)),
		sdkmetric.WithResource(res),
	)

	// Set globals only after both providers are confirmed healthy.
	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)

	shutdown := func() {
		_ = tp.Shutdown(context.Background())
		_ = mp.Shutdown(context.Background())
	}

	return &Telemetry{tp: tp, mp: mp}, shutdown, nil
}

// Tracer returns a named tracer from the provider.
func (t *Telemetry) Tracer(name string) trace.Tracer {
	return t.tp.Tracer(name)
}

// Meter returns a named meter from the provider.
func (t *Telemetry) Meter(name string) metric.Meter {
	return t.mp.Meter(name)
}

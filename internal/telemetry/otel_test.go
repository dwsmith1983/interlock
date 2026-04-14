package telemetry_test

import (
	"context"
	"testing"

	"github.com/dwsmith1983/interlock/internal/telemetry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTelemetry_NoEndpoint_ReturnsNoop(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	tel, shutdown, err := telemetry.NewTelemetry(context.Background(), "test-service")
	require.NoError(t, err)
	assert.NotNil(t, tel)
	assert.NotNil(t, shutdown)
	shutdown()
}

func TestTelemetry_Tracer_NonNil(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	tel, shutdown, _ := telemetry.NewTelemetry(context.Background(), "test")
	defer shutdown()

	tracer := tel.Tracer("test-tracer")
	assert.NotNil(t, tracer)
}

func TestTelemetry_Meter_NonNil(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	tel, shutdown, _ := telemetry.NewTelemetry(context.Background(), "test")
	defer shutdown()

	meter := tel.Meter("test-meter")
	assert.NotNil(t, meter)
}

func TestMetricConstants_Defined(t *testing.T) {
	assert.Equal(t, "interlock.records.processed", telemetry.MetricRecordsProcessed)
	assert.Equal(t, "interlock.stage.duration", telemetry.MetricStageDuration)
	assert.Equal(t, "interlock.rules.evaluated", telemetry.MetricRulesEvaluated)
	assert.Equal(t, "interlock.dlq.routed", telemetry.MetricDLQRouted)
	assert.Equal(t, "interlock.worker_pool.active", telemetry.MetricWorkerPoolActive)
	assert.Equal(t, "interlock.circuit_breaker.state", telemetry.MetricCircuitBreakerState)
}

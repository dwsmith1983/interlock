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

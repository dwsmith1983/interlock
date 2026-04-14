package telemetry_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/dwsmith1983/interlock/internal/telemetry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCorrelationID_RoundTrip(t *testing.T) {
	ctx := telemetry.WithCorrelationID(context.Background(), "abc-123")
	got := telemetry.CorrelationIDFromContext(ctx)
	assert.Equal(t, "abc-123", got)
}

func TestCorrelationID_Missing(t *testing.T) {
	got := telemetry.CorrelationIDFromContext(context.Background())
	assert.Equal(t, "", got)
}

func TestCorrelationHandler_InjectsID(t *testing.T) {
	var buf bytes.Buffer
	logger := telemetry.NewLogger(&buf)

	ctx := telemetry.WithCorrelationID(context.Background(), "trace-42")
	logger.InfoContext(ctx, "test message")

	var entry map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &entry))
	assert.Equal(t, "trace-42", entry["correlation_id"])
	assert.Equal(t, "test message", entry["msg"])
}

func TestCorrelationHandler_NoID_NoPanic(t *testing.T) {
	var buf bytes.Buffer
	logger := telemetry.NewLogger(&buf)

	logger.InfoContext(context.Background(), "no correlation")

	var entry map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &entry))
	_, hasCorrelation := entry["correlation_id"]
	assert.False(t, hasCorrelation)
}

func TestNewLogger_NonNil(t *testing.T) {
	var buf bytes.Buffer
	logger := telemetry.NewLogger(&buf)
	assert.NotNil(t, logger)
}

package lambda

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateEnv_AllPresent(t *testing.T) {
	for _, v := range requiredEnvVars["event-sink"] {
		t.Setenv(v, "test-value")
	}
	err := ValidateEnv("event-sink")
	require.NoError(t, err)
}

func TestValidateEnv_MissingVars(t *testing.T) {
	// Clear all vars that stream-router needs
	for _, v := range requiredEnvVars["stream-router"] {
		t.Setenv(v, "")
	}
	err := ValidateEnv("stream-router")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "CONTROL_TABLE")
	assert.Contains(t, err.Error(), "STATE_MACHINE_ARN")
}

func TestValidateEnv_PartialMissing(t *testing.T) {
	t.Setenv("EVENTS_TABLE", "")
	err := ValidateEnv("event-sink")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "EVENTS_TABLE")
}

func TestValidateEnv_UnknownHandler(t *testing.T) {
	err := ValidateEnv("unknown-handler")
	require.NoError(t, err)
}

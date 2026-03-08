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

func TestValidateEnv_AlertDispatcherAllPresent(t *testing.T) {
	for _, v := range requiredEnvVars["alert-dispatcher"] {
		t.Setenv(v, "test-value")
	}
	err := ValidateEnv("alert-dispatcher")
	require.NoError(t, err)
}

func TestValidateEnv_AlertDispatcherMissingEventsVars(t *testing.T) {
	t.Setenv("SLACK_BOT_TOKEN", "xoxb-test")
	t.Setenv("SLACK_CHANNEL_ID", "C12345")
	t.Setenv("EVENTS_TABLE", "")
	t.Setenv("EVENTS_TTL_DAYS", "")
	err := ValidateEnv("alert-dispatcher")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "EVENTS_TABLE")
	assert.Contains(t, err.Error(), "EVENTS_TTL_DAYS")
}

func TestValidateEnv_UnknownHandler(t *testing.T) {
	err := ValidateEnv("unknown-handler")
	require.NoError(t, err)
}

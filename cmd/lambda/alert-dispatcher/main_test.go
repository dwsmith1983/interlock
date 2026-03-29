package main

import (
	"testing"

	ilambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateEnv_MissingVars(t *testing.T) {
	err := ilambda.ValidateEnv("alert-dispatcher")
	assert.Error(t, err, "should report missing env vars")
	assert.Contains(t, err.Error(), "SLACK_CHANNEL_ID")
	assert.Contains(t, err.Error(), "EVENTS_TABLE")
	assert.Contains(t, err.Error(), "EVENTS_TTL_DAYS")
}

func TestValidateEnv_AllSet(t *testing.T) {
	envVars := map[string]string{
		"SLACK_CHANNEL_ID": "C12345",
		"EVENTS_TABLE":     "events",
		"EVENTS_TTL_DAYS":  "90",
	}
	for k, v := range envVars {
		t.Setenv(k, v)
	}

	err := ilambda.ValidateEnv("alert-dispatcher")
	require.NoError(t, err, "should pass when all env vars are set")
}

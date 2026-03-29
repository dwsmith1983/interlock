package main

import (
	"testing"

	ilambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateEnv_MissingVars(t *testing.T) {
	err := ilambda.ValidateEnv("stream-router")
	assert.Error(t, err, "should report missing env vars")
	assert.Contains(t, err.Error(), "CONTROL_TABLE")
	assert.Contains(t, err.Error(), "JOBLOG_TABLE")
	assert.Contains(t, err.Error(), "RERUN_TABLE")
	assert.Contains(t, err.Error(), "STATE_MACHINE_ARN")
	assert.Contains(t, err.Error(), "EVENT_BUS_NAME")
}

func TestValidateEnv_AllSet(t *testing.T) {
	envVars := map[string]string{
		"CONTROL_TABLE":     "ctl",
		"JOBLOG_TABLE":      "jl",
		"RERUN_TABLE":       "rr",
		"STATE_MACHINE_ARN": "arn:aws:states:us-east-1:123:stateMachine:test",
		"EVENT_BUS_NAME":    "bus",
	}
	for k, v := range envVars {
		t.Setenv(k, v)
	}

	err := ilambda.ValidateEnv("stream-router")
	require.NoError(t, err, "should pass when all env vars are set")
}

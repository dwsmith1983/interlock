package main

import (
	"testing"

	ilambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateEnv_MissingVars(t *testing.T) {
	err := ilambda.ValidateEnv("event-sink")
	assert.Error(t, err, "should report missing env vars")
	assert.Contains(t, err.Error(), "EVENTS_TABLE")
}

func TestValidateEnv_AllSet(t *testing.T) {
	t.Setenv("EVENTS_TABLE", "events")

	err := ilambda.ValidateEnv("event-sink")
	require.NoError(t, err, "should pass when all env vars are set")
}

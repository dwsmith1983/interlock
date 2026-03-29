package main

import (
	"testing"

	ilambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateEnv_MissingVars(t *testing.T) {
	err := ilambda.ValidateEnv("sla-monitor")
	assert.Error(t, err, "should report missing env vars")
	assert.Contains(t, err.Error(), "CONTROL_TABLE")
	assert.Contains(t, err.Error(), "JOBLOG_TABLE")
	assert.Contains(t, err.Error(), "RERUN_TABLE")
	assert.Contains(t, err.Error(), "EVENT_BUS_NAME")
	assert.Contains(t, err.Error(), "SLA_MONITOR_ARN")
	assert.Contains(t, err.Error(), "SCHEDULER_ROLE_ARN")
	assert.Contains(t, err.Error(), "SCHEDULER_GROUP_NAME")
}

func TestValidateEnv_AllSet(t *testing.T) {
	envVars := map[string]string{
		"CONTROL_TABLE":        "ctl",
		"JOBLOG_TABLE":         "jl",
		"RERUN_TABLE":          "rr",
		"EVENT_BUS_NAME":       "bus",
		"SLA_MONITOR_ARN":      "arn:aws:lambda:us-east-1:123:function:sla",
		"SCHEDULER_ROLE_ARN":   "arn:aws:iam::123:role/sched",
		"SCHEDULER_GROUP_NAME": "interlock",
	}
	for k, v := range envVars {
		t.Setenv(k, v)
	}

	err := ilambda.ValidateEnv("sla-monitor")
	require.NoError(t, err, "should pass when all env vars are set")
}

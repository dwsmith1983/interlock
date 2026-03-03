package types_test

import (
	"testing"

	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestPipelinePK(t *testing.T) {
	assert.Equal(t, "PIPELINE#gold-revenue", types.PipelinePK("gold-revenue"))
	assert.Equal(t, "PIPELINE#bronze-ingest", types.PipelinePK("bronze-ingest"))
}

func TestConfigSK(t *testing.T) {
	assert.Equal(t, "CONFIG", types.ConfigSK)
}

func TestSensorSK(t *testing.T) {
	assert.Equal(t, "SENSOR#upstream-complete", types.SensorSK("upstream-complete"))
	assert.Equal(t, "SENSOR#record-count", types.SensorSK("record-count"))
}

func TestTriggerSK(t *testing.T) {
	assert.Equal(t, "TRIGGER#daily#2026-03-01", types.TriggerSK("daily", "2026-03-01"))
	assert.Equal(t, "TRIGGER#hourly#2026-03-01", types.TriggerSK("hourly", "2026-03-01"))
}

func TestJobSK(t *testing.T) {
	assert.Equal(t,
		"JOB#daily#2026-03-01#1709308800",
		types.JobSK("daily", "2026-03-01", "1709308800"),
	)
}

func TestRerunSK(t *testing.T) {
	tests := []struct {
		schedule string
		date     string
		attempt  int
		want     string
	}{
		{"daily", "2026-03-01", 0, "RERUN#daily#2026-03-01#0"},
		{"daily", "2026-03-01", 1, "RERUN#daily#2026-03-01#1"},
		{"daily", "2026-03-01", 9, "RERUN#daily#2026-03-01#9"},
		{"daily", "2026-03-01", 10, "RERUN#daily#2026-03-01#10"},
		{"hourly", "2026-06-15", 3, "RERUN#hourly#2026-06-15#3"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			assert.Equal(t, tt.want, types.RerunSK(tt.schedule, tt.date, tt.attempt))
		})
	}
}

func TestTriggerStatusConstants(t *testing.T) {
	assert.Equal(t, "RUNNING", types.TriggerStatusRunning)
	assert.Equal(t, "COMPLETED", types.TriggerStatusCompleted)
	assert.Equal(t, "FAILED_FINAL", types.TriggerStatusFailedFinal)
}

func TestJobEventConstants(t *testing.T) {
	assert.Equal(t, "success", types.JobEventSuccess)
	assert.Equal(t, "fail", types.JobEventFail)
	assert.Equal(t, "timeout", types.JobEventTimeout)
	assert.Equal(t, "infra-trigger-failure", types.JobEventInfraTriggerFailure)
}

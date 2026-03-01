package v2_test

import (
	"testing"

	v2 "github.com/dwsmith1983/interlock/pkg/types/v2"
	"github.com/stretchr/testify/assert"
)

func TestPipelinePK(t *testing.T) {
	assert.Equal(t, "PIPELINE#gold-revenue", v2.PipelinePK("gold-revenue"))
	assert.Equal(t, "PIPELINE#bronze-ingest", v2.PipelinePK("bronze-ingest"))
}

func TestConfigSK(t *testing.T) {
	assert.Equal(t, "CONFIG", v2.ConfigSK)
}

func TestSensorSK(t *testing.T) {
	assert.Equal(t, "SENSOR#upstream-complete", v2.SensorSK("upstream-complete"))
	assert.Equal(t, "SENSOR#record-count", v2.SensorSK("record-count"))
}

func TestTriggerSK(t *testing.T) {
	assert.Equal(t, "TRIGGER#daily#2026-03-01", v2.TriggerSK("daily", "2026-03-01"))
	assert.Equal(t, "TRIGGER#hourly#2026-03-01", v2.TriggerSK("hourly", "2026-03-01"))
}

func TestJobSK(t *testing.T) {
	assert.Equal(t,
		"JOB#daily#2026-03-01#1709308800",
		v2.JobSK("daily", "2026-03-01", "1709308800"),
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
			assert.Equal(t, tt.want, v2.RerunSK(tt.schedule, tt.date, tt.attempt))
		})
	}
}

func TestTriggerStatusConstants(t *testing.T) {
	assert.Equal(t, "RUNNING", v2.TriggerStatusRunning)
	assert.Equal(t, "COMPLETED", v2.TriggerStatusCompleted)
	assert.Equal(t, "FAILED_FINAL", v2.TriggerStatusFailedFinal)
}

func TestJobEventConstants(t *testing.T) {
	assert.Equal(t, "success", v2.JobEventSuccess)
	assert.Equal(t, "fail", v2.JobEventFail)
	assert.Equal(t, "timeout", v2.JobEventTimeout)
}

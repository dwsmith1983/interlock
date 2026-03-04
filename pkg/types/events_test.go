package types_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventDetailTypeConstants(t *testing.T) {
	tests := []struct {
		constant types.EventDetailType
		want     string
	}{
		{types.EventSLAWarning, "SLA_WARNING"},
		{types.EventSLABreach, "SLA_BREACH"},
		{types.EventSLAMet, "SLA_MET"},
		{types.EventValidationExhausted, "VALIDATION_EXHAUSTED"},
		{types.EventRetryExhausted, "RETRY_EXHAUSTED"},
		{types.EventSFNTimeout, "SFN_TIMEOUT"},
		{types.EventScheduleMissed, "SCHEDULE_MISSED"},
		{types.EventJobTriggered, "JOB_TRIGGERED"},
		{types.EventJobCompleted, "JOB_COMPLETED"},
		{types.EventJobFailed, "JOB_FAILED"},
		{types.EventValidationPassed, "VALIDATION_PASSED"},
		{types.EventInfraFailure, "INFRA_FAILURE"},
		{types.EventLateDataArrival, "LATE_DATA_ARRIVAL"},
		{types.EventRerunRejected, "RERUN_REJECTED"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			assert.Equal(t, tt.want, string(tt.constant))
		})
	}
}

func TestEventSource(t *testing.T) {
	assert.Equal(t, "interlock", types.EventSource)
}

func TestInterlockEvent_JSONRoundTrip(t *testing.T) {
	ts := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	original := types.InterlockEvent{
		PipelineID: "gold-revenue",
		ScheduleID: "daily",
		Date:       "2026-03-01",
		Deadline:   "08:00",
		Message:    "SLA breach: pipeline exceeded deadline",
		Timestamp:  ts,
		Detail: map[string]interface{}{
			"severity": "critical",
			"attempt":  float64(3),
		},
	}

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var decoded types.InterlockEvent
	require.NoError(t, json.Unmarshal(data, &decoded))

	assert.Equal(t, original.PipelineID, decoded.PipelineID)
	assert.Equal(t, original.ScheduleID, decoded.ScheduleID)
	assert.Equal(t, original.Date, decoded.Date)
	assert.Equal(t, original.Deadline, decoded.Deadline)
	assert.Equal(t, original.Message, decoded.Message)
	assert.True(t, original.Timestamp.Equal(decoded.Timestamp))
	assert.Equal(t, "critical", decoded.Detail["severity"])
	assert.Equal(t, float64(3), decoded.Detail["attempt"])
}

func TestInterlockEvent_JSONOmitsEmpty(t *testing.T) {
	ts := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	evt := types.InterlockEvent{
		PipelineID: "bronze-ingest",
		Timestamp:  ts,
	}

	data, err := json.Marshal(evt)
	require.NoError(t, err)

	var raw map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &raw))

	assert.Contains(t, raw, "pipelineId")
	assert.Contains(t, raw, "timestamp")
	assert.NotContains(t, raw, "scheduleId")
	assert.NotContains(t, raw, "date")
	assert.NotContains(t, raw, "deadline")
	assert.NotContains(t, raw, "message")
	assert.NotContains(t, raw, "detail")
}

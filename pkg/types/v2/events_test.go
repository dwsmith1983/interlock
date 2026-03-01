package v2_test

import (
	"encoding/json"
	"testing"
	"time"

	v2 "github.com/dwsmith1983/interlock/pkg/types/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventDetailTypeConstants(t *testing.T) {
	tests := []struct {
		constant v2.EventDetailType
		want     string
	}{
		{v2.EventSLAWarning, "SLA_WARNING"},
		{v2.EventSLABreach, "SLA_BREACH"},
		{v2.EventSLAResolved, "SLA_RESOLVED"},
		{v2.EventValidationExhausted, "VALIDATION_EXHAUSTED"},
		{v2.EventRetryExhausted, "RETRY_EXHAUSTED"},
		{v2.EventSFNTimeout, "SFN_TIMEOUT"},
		{v2.EventScheduleMissed, "SCHEDULE_MISSED"},
		{v2.EventJobTriggered, "JOB_TRIGGERED"},
		{v2.EventJobCompleted, "JOB_COMPLETED"},
		{v2.EventJobFailed, "JOB_FAILED"},
		{v2.EventValidationPassed, "VALIDATION_PASSED"},
		{v2.EventInfraFailure, "INFRA_FAILURE"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			assert.Equal(t, tt.want, string(tt.constant))
		})
	}
}

func TestEventSource(t *testing.T) {
	assert.Equal(t, "interlock", v2.EventSource)
}

func TestInterlockEvent_JSONRoundTrip(t *testing.T) {
	ts := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	original := v2.InterlockEvent{
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

	var decoded v2.InterlockEvent
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

	evt := v2.InterlockEvent{
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

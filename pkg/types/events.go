package types

import "time"

// EventDetailType enumerates all EventBridge detail-type values.
type EventDetailType string

const (
	EventSLAWarning          EventDetailType = "SLA_WARNING"
	EventSLABreach           EventDetailType = "SLA_BREACH"
	EventSLAMet              EventDetailType = "SLA_MET"
	EventValidationExhausted EventDetailType = "VALIDATION_EXHAUSTED"
	EventRetryExhausted      EventDetailType = "RETRY_EXHAUSTED"
	EventSFNTimeout          EventDetailType = "SFN_TIMEOUT"
	EventScheduleMissed      EventDetailType = "SCHEDULE_MISSED"
	EventJobTriggered        EventDetailType = "JOB_TRIGGERED"
	EventJobCompleted        EventDetailType = "JOB_COMPLETED"
	EventJobFailed           EventDetailType = "JOB_FAILED"
	EventValidationPassed    EventDetailType = "VALIDATION_PASSED"
	EventInfraFailure        EventDetailType = "INFRA_FAILURE"
)

// EventSource is the EventBridge source for all interlock events.
const EventSource = "interlock"

// InterlockEvent is the detail payload for EventBridge events.
type InterlockEvent struct {
	PipelineID string                 `json:"pipelineId"`
	ScheduleID string                 `json:"scheduleId,omitempty"`
	Date       string                 `json:"date,omitempty"`
	Deadline   string                 `json:"deadline,omitempty"`
	Message    string                 `json:"message,omitempty"`
	Timestamp  time.Time              `json:"timestamp"`
	Detail     map[string]interface{} `json:"detail,omitempty"`
}

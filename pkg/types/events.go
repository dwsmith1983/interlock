package types

import "time"

// EventDetailType enumerates all EventBridge detail-type values.
type EventDetailType string

const (
	EventSLAWarning              EventDetailType = "SLA_WARNING"
	EventSLABreach               EventDetailType = "SLA_BREACH"
	EventSLAMet                  EventDetailType = "SLA_MET"
	EventValidationExhausted     EventDetailType = "VALIDATION_EXHAUSTED"
	EventRetryExhausted          EventDetailType = "RETRY_EXHAUSTED"
	EventSFNTimeout              EventDetailType = "SFN_TIMEOUT"
	EventJobPollExhausted        EventDetailType = "JOB_POLL_EXHAUSTED"
	EventScheduleMissed          EventDetailType = "SCHEDULE_MISSED"
	EventJobTriggered            EventDetailType = "JOB_TRIGGERED"
	EventJobCompleted            EventDetailType = "JOB_COMPLETED"
	EventJobFailed               EventDetailType = "JOB_FAILED"
	EventValidationPassed        EventDetailType = "VALIDATION_PASSED"
	EventInfraFailure            EventDetailType = "INFRA_FAILURE"
	EventLateDataArrival         EventDetailType = "LATE_DATA_ARRIVAL"
	EventRerunRejected           EventDetailType = "RERUN_REJECTED"
	EventTriggerRecovered        EventDetailType = "TRIGGER_RECOVERED"
	EventDataDrift               EventDetailType = "DATA_DRIFT"
	EventBaselineCaptureFailed   EventDetailType = "BASELINE_CAPTURE_FAILED"
	EventPipelineExcluded        EventDetailType = "PIPELINE_EXCLUDED"
	EventPostRunBaselineCaptured EventDetailType = "POST_RUN_BASELINE_CAPTURED"
	EventPostRunPassed           EventDetailType = "POST_RUN_PASSED"
	EventPostRunFailed           EventDetailType = "POST_RUN_FAILED"
	EventPostRunDrift            EventDetailType = "POST_RUN_DRIFT"
	EventPostRunDriftInflight    EventDetailType = "POST_RUN_DRIFT_INFLIGHT"
	EventPostRunSensorMissing    EventDetailType = "POST_RUN_SENSOR_MISSING"
	EventRerunAccepted           EventDetailType = "RERUN_ACCEPTED"
	EventInfraAlarm              EventDetailType = "INFRA_ALARM"
	EventSensorDeadlineExpired   EventDetailType = "SENSOR_DEADLINE_EXPIRED"
	EventIrregularScheduleMissed EventDetailType = "IRREGULAR_SCHEDULE_MISSED"
	EventRelativeSLAWarning      EventDetailType = "RELATIVE_SLA_WARNING"
	EventRelativeSLABreach       EventDetailType = "RELATIVE_SLA_BREACH"
	EventDryRunWouldTrigger      EventDetailType = "DRY_RUN_WOULD_TRIGGER"
	EventDryRunLateData          EventDetailType = "DRY_RUN_LATE_DATA"
	EventDryRunSLAProjection     EventDetailType = "DRY_RUN_SLA_PROJECTION"
	EventDryRunDrift             EventDetailType = "DRY_RUN_DRIFT"
	EventDryRunCompleted         EventDetailType = "DRY_RUN_COMPLETED"
	EventDryRunWouldRerun        EventDetailType = "DRY_RUN_WOULD_RERUN"
	EventDryRunRerunRejected     EventDetailType = "DRY_RUN_RERUN_REJECTED"
	EventDryRunWouldRetry        EventDetailType = "DRY_RUN_WOULD_RETRY"
	EventDryRunRetryExhausted    EventDetailType = "DRY_RUN_RETRY_EXHAUSTED"
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

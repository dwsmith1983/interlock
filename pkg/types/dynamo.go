package types

import (
	"fmt"
	"time"
)

// ---------------------------------------------------------------------------
// Key helpers
// ---------------------------------------------------------------------------

// PipelinePK returns the partition key for a given pipeline.
func PipelinePK(pipelineID string) string { return "PIPELINE#" + pipelineID }

// ConfigSK is the sort key for the pipeline configuration record.
const ConfigSK = "CONFIG"

// SensorSK returns the sort key for a sensor data point.
func SensorSK(key string) string { return "SENSOR#" + key }

// TriggerSK returns the sort key for a trigger record.
func TriggerSK(schedule, date string) string { return "TRIGGER#" + schedule + "#" + date }

// JobSK returns the sort key for a job log record.
func JobSK(schedule, date, timestamp string) string {
	return "JOB#" + schedule + "#" + date + "#" + timestamp
}

// RerunSK returns the sort key for a re-run record.
func RerunSK(schedule, date string, attempt int) string {
	return fmt.Sprintf("RERUN#%s#%s#%d", schedule, date, attempt)
}

// RerunRequestSK returns the sort key for a manual re-run request.
func RerunRequestSK(schedule, date string) string {
	return "RERUN_REQUEST#" + schedule + "#" + date
}

// RunSK returns the sort key for an expected run date. runKey is YYYY-MM-DD,
// or YYYY-MM-DDThh for an hourly run, the same forms as the execution date.
func RunSK(runKey string) string { return "RUN#" + runKey }

// DryRunSK returns the sort key for a dry-run marker.
func DryRunSK(schedule, date string) string {
	return "DRY_RUN#" + schedule + "#" + date
}

// ---------------------------------------------------------------------------
// Status / event constants
// ---------------------------------------------------------------------------

const (
	// Trigger statuses (control table).
	TriggerStatusRunning     = "RUNNING"
	TriggerStatusCompleted   = "COMPLETED"
	TriggerStatusFailedFinal = "FAILED_FINAL"

	// Job event outcomes (job log table).
	JobEventSuccess               = "success"
	JobEventFail                  = "fail"
	JobEventTimeout               = "timeout"
	JobEventInfraTriggerFailure   = "infra-trigger-failure"
	JobEventInfraTriggerExhausted = "infra-trigger-exhausted"
	JobEventLateDataArrival       = "late-data-arrival" // no longer written; read from older rows
	JobEventRerunAccepted         = "rerun-accepted"
	JobEventRerunRejected         = "rerun-rejected" // no longer written; read from older rows
	JobEventValidationExhausted   = "validation-exhausted"
	JobEventEvaluationReleased    = "evaluation-released" // evaluation ran out before the cutoff; the lock was released
)

// ---------------------------------------------------------------------------
// DynamoDB record types
// ---------------------------------------------------------------------------

// ControlRecord represents a row in the control table.
type ControlRecord struct {
	PK     string                 `dynamodbav:"PK"`
	SK     string                 `dynamodbav:"SK"`
	Config string                 `dynamodbav:"config,omitempty"`
	Data   map[string]interface{} `dynamodbav:"data,omitempty"`
	Status string                 `dynamodbav:"status,omitempty"`
	// TTL is when DynamoDB deletes the row (epoch seconds). Rows that expire
	// carry it (DRY_RUN#, config-invalid markers, and Interlock's own SENSOR#
	// and RERUN_REQUEST# rows); user sensor rows and TRIGGER# rows are written
	// without one.
	TTL int64 `dynamodbav:"ttl,omitempty"`
	// StaleAt is, on a RUNNING TRIGGER# row, the epoch second after which the
	// watchdog treats the run as dead and closes it.
	StaleAt int64 `dynamodbav:"staleAt,omitempty"`
	// StartedAt is when the job last started (RFC3339 UTC), on TRIGGER# rows.
	StartedAt string `dynamodbav:"startedAt,omitempty"`
	// CompletedAt is when the run last completed (RFC3339 UTC), on TRIGGER# rows;
	// meaningful only while status is COMPLETED.
	CompletedAt string `dynamodbav:"completedAt,omitempty"`
}

// JobLogRecord represents a row in the job log table (append-only).
type JobLogRecord struct {
	PK       string `dynamodbav:"PK"`
	SK       string `dynamodbav:"SK"`
	Event    string `dynamodbav:"event"`
	Error    string `dynamodbav:"error,omitempty"`
	Duration int64  `dynamodbav:"duration,omitempty"`
	RunID    string `dynamodbav:"runId,omitempty"`
	TTL      int64  `dynamodbav:"ttl,omitempty"`
	Category string `dynamodbav:"category,omitempty"`
}

// RerunRecord represents a row in the re-run table (append-only).
type RerunRecord struct {
	PK             string    `dynamodbav:"PK"`
	SK             string    `dynamodbav:"SK"`
	Reason         string    `dynamodbav:"reason"`
	TriggeredAt    time.Time `dynamodbav:"triggeredAt"`
	SourceJobEvent string    `dynamodbav:"sourceJobEvent,omitempty"`
	TTL            int64     `dynamodbav:"ttl,omitempty"`
}

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

// ---------------------------------------------------------------------------
// Status / event constants
// ---------------------------------------------------------------------------

const (
	// Trigger statuses (control table).
	TriggerStatusRunning     = "RUNNING"
	TriggerStatusCompleted   = "COMPLETED"
	TriggerStatusFailedFinal = "FAILED_FINAL"

	// Job event outcomes (job log table).
	JobEventSuccess = "success"
	JobEventFail    = "fail"
	JobEventTimeout = "timeout"
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
	TTL    int64                  `dynamodbav:"ttl,omitempty"`
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

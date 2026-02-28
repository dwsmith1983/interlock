package types

import "time"

// QuarantineRecord tracks quarantined records written by a Glue ETL job.
type QuarantineRecord struct {
	PipelineID     string    `json:"pipelineId"`
	Date           string    `json:"date"`
	Hour           string    `json:"hour"`
	Count          int       `json:"count"`
	QuarantinePath string    `json:"quarantinePath"`
	Reasons        []string  `json:"reasons"`
	Timestamp      time.Time `json:"timestamp"`
}

// TraitEvaluation is the result of evaluating a single trait.
type TraitEvaluation struct {
	PipelineID      string                 `json:"pipelineId"`
	TraitType       string                 `json:"traitType"`
	Status          TraitStatus            `json:"status"`
	Value           map[string]interface{} `json:"value,omitempty"`
	Reason          string                 `json:"reason,omitempty"`
	FailureCategory FailureCategory        `json:"failureCategory,omitempty"`
	EvaluatedAt     time.Time              `json:"evaluatedAt"`
	ExpiresAt       *time.Time             `json:"expiresAt,omitempty"`
}

// ReadinessResult is the outcome of evaluating all traits for a pipeline.
type ReadinessResult struct {
	PipelineID  string            `json:"pipelineId"`
	Status      ReadinessStatus   `json:"status"`
	Traits      []TraitEvaluation `json:"traits"`
	Blocking    []string          `json:"blocking,omitempty"`
	EvaluatedAt time.Time         `json:"evaluatedAt"`
}

// RunState represents the lifecycle state of a single pipeline run.
type RunState struct {
	RunID      string                 `json:"runId"`
	PipelineID string                 `json:"pipelineId"`
	Status     RunStatus              `json:"status"`
	Version    int                    `json:"version"`
	CreatedAt  time.Time              `json:"createdAt"`
	UpdatedAt  time.Time              `json:"updatedAt"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
}

// EvaluatorOutput is the JSON expected from an evaluator subprocess on stdout.
type EvaluatorOutput struct {
	Status          TraitStatus            `json:"status"`
	Value           map[string]interface{} `json:"value,omitempty"`
	Reason          string                 `json:"reason,omitempty"`
	FailureCategory FailureCategory        `json:"failureCategory,omitempty"`
}

// Alert represents an alert event to be dispatched.
type Alert struct {
	AlertID    string                 `json:"alertId,omitempty"`
	Level      AlertLevel             `json:"level"`
	Category   string                 `json:"alertType,omitempty"`
	PipelineID string                 `json:"pipelineId,omitempty"`
	TraitType  string                 `json:"traitType,omitempty"`
	Message    string                 `json:"message"`
	Details    map[string]interface{} `json:"details,omitempty"`
	Timestamp  time.Time              `json:"timestamp"`
}

// TraitChangeEvent represents a trait state change for eventing.
type TraitChangeEvent struct {
	PipelineID string      `json:"pipelineId"`
	TraitType  string      `json:"traitType"`
	OldStatus  TraitStatus `json:"oldStatus,omitempty"`
	NewStatus  TraitStatus `json:"newStatus"`
	Timestamp  time.Time   `json:"timestamp"`
}

// Event is an append-only audit log entry recording what happened and when.
type Event struct {
	Kind       EventKind              `json:"kind"`
	PipelineID string                 `json:"pipelineId"`
	RunID      string                 `json:"runId,omitempty"`
	TraitType  string                 `json:"traitType,omitempty"`
	Status     string                 `json:"status,omitempty"`
	Message    string                 `json:"message,omitempty"`
	Details    map[string]interface{} `json:"details,omitempty"`
	Timestamp  time.Time              `json:"timestamp"`
}

// RetryAttempt tracks per-attempt details for retry history.
type RetryAttempt struct {
	Attempt         int             `json:"attempt"`
	Status          RunStatus       `json:"status"`
	RunID           string          `json:"runId"`
	FailureMessage  string          `json:"failureMessage,omitempty"`
	FailureCategory FailureCategory `json:"failureCategory,omitempty"`
	StartedAt       time.Time       `json:"startedAt"`
	CompletedAt     *time.Time      `json:"completedAt,omitempty"`
}

// LateArrival records detection of data arriving after a pipeline run completed.
type LateArrival struct {
	PipelineID  string    `json:"pipelineId"`
	Date        string    `json:"date"`
	ScheduleID  string    `json:"scheduleId"`
	DetectedAt  time.Time `json:"detectedAt"`
	RecordDelta int       `json:"recordDelta"`
	TraitType   string    `json:"traitType"`
}

// ReplayRequest represents a manual replay of a pipeline run.
type ReplayRequest struct {
	PipelineID  string    `json:"pipelineId"`
	Date        string    `json:"date"`
	ScheduleID  string    `json:"scheduleId"`
	Reason      string    `json:"reason"`
	RequestedBy string    `json:"requestedBy"`
	Status      string    `json:"status"` // pending, started, completed, failed
	CreatedAt   time.Time `json:"createdAt"`
}

// RunLogEntry tracks durable per-pipeline-per-date-per-schedule run state.
type RunLogEntry struct {
	PipelineID      string          `json:"pipelineId"`
	Date            string          `json:"date"`
	ScheduleID      string          `json:"scheduleId"`
	Status          RunStatus       `json:"status"`
	AttemptNumber   int             `json:"attemptNumber"`
	RunID           string          `json:"runId"`
	FailureMessage  string          `json:"failureMessage,omitempty"`
	FailureCategory FailureCategory `json:"failureCategory,omitempty"`
	AlertSent       bool            `json:"alertSent"`
	RetryHistory    []RetryAttempt  `json:"retryHistory,omitempty"`
	StartedAt       time.Time       `json:"startedAt"`
	CompletedAt     *time.Time      `json:"completedAt,omitempty"`
	UpdatedAt       time.Time       `json:"updatedAt"`
}

// RerunRecord tracks a pipeline rerun triggered by late-arriving data or corrections.
// It is a separate ledger from RunLog — the original run record is never modified.
type RerunRecord struct {
	RerunID       string                 `json:"rerunId"`
	PipelineID    string                 `json:"pipelineId"`
	OriginalDate  string                 `json:"originalDate"`
	OriginalRunID string                 `json:"originalRunId,omitempty"`
	Reason        string                 `json:"reason"`
	Description   string                 `json:"description,omitempty"`
	Status        RunStatus              `json:"status"`
	RerunRunID    string                 `json:"rerunRunId,omitempty"`
	RequestedAt   time.Time              `json:"requestedAt"`
	CompletedAt   *time.Time             `json:"completedAt,omitempty"`
	Metadata      map[string]interface{} `json:"metadata,omitempty"`
}

// EventRecord pairs a Redis stream ID with an event for cursor-based reading.
type EventRecord struct {
	StreamID string `json:"streamId"`
	Event    Event  `json:"event"`
}

// ControlRecord tracks per-pipeline health status for circuit-breaker gating.
type ControlRecord struct {
	PipelineID          string `json:"pipelineId"`
	ConsecutiveFailures int    `json:"consecutiveFailures"`
	LastStatus          string `json:"lastStatus"`
	LastSuccessfulRun   string `json:"lastSuccessfulRun,omitempty"`
	LastFailedRun       string `json:"lastFailedRun,omitempty"`
	Enabled             bool   `json:"enabled"`
}

// SensorData holds externally-landed sensor readings (e.g., lag, window IDs,
// record counts). Data engineers write sensor data; interlock reads it via
// trait evaluators.
type SensorData struct {
	PipelineID string                 `json:"pipelineId"`
	SensorType string                 `json:"sensorType"`
	Value      map[string]interface{} `json:"value"`
	UpdatedAt  time.Time              `json:"updatedAt"`
}

// EvaluationSession tracks a single evaluation pass — all trait results,
// readiness outcome, and timing for audit and debugging.
type EvaluationSession struct {
	SessionID     string                  `json:"sessionId"`
	PipelineID    string                  `json:"pipelineId"`
	TriggerSource string                  `json:"triggerSource,omitempty"`
	Status        EvaluationSessionStatus `json:"status"`
	TraitResults  []TraitEvaluation       `json:"traitResults,omitempty"`
	Readiness     ReadinessStatus         `json:"readiness,omitempty"`
	StartedAt     time.Time               `json:"startedAt"`
	CompletedAt   *time.Time              `json:"completedAt,omitempty"`
}

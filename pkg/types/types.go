// Package types defines the public domain types for the Interlock STAMP-based safety framework.
package types

import "time"

// DefaultScheduleID is the implicit schedule name for pipelines without explicit schedules.
const DefaultScheduleID = "daily"

// ScheduleConfig defines a named schedule window for a pipeline.
type ScheduleConfig struct {
	Name     string `yaml:"name" json:"name"`
	After    string `yaml:"after,omitempty" json:"after,omitempty"`       // "HH:MM" - eligible after this time
	Deadline string `yaml:"deadline,omitempty" json:"deadline,omitempty"` // "HH:MM" - SLA deadline for window
	Timezone string `yaml:"timezone,omitempty" json:"timezone,omitempty"` // e.g. "America/New_York"
}

// ResolveSchedules returns the explicit schedules for a pipeline, or a default
// single "daily" schedule if none are configured.
func ResolveSchedules(p PipelineConfig) []ScheduleConfig {
	if len(p.Schedules) > 0 {
		return p.Schedules
	}
	return []ScheduleConfig{{Name: DefaultScheduleID}}
}

// TraitStatus represents the evaluation outcome of a trait.
type TraitStatus string

// TraitStatus values enumerate the possible trait evaluation outcomes.
const (
	TraitPass    TraitStatus = "PASS"
	TraitFail    TraitStatus = "FAIL"
	TraitStale   TraitStatus = "STALE"
)

// ReadinessStatus represents whether a pipeline is ready to execute.
type ReadinessStatus string

// ReadinessStatus values indicate whether a pipeline is ready or not.
const (
	Ready    ReadinessStatus = "READY"
	NotReady ReadinessStatus = "NOT_READY"
)

// RunStatus represents the lifecycle state of a pipeline run.
type RunStatus string

// RunStatus values represent the lifecycle states of a pipeline run.
const (
	RunPending             RunStatus = "PENDING"
	RunTriggering          RunStatus = "TRIGGERING"
	RunRunning             RunStatus = "RUNNING"
	RunCompleted           RunStatus = "COMPLETED"
	RunCompletedMonitoring RunStatus = "COMPLETED_MONITORING"
	RunFailed              RunStatus = "FAILED"
	RunCancelled           RunStatus = "CANCELLED"
)

// ReadinessRuleType defines how traits are combined to determine readiness.
type ReadinessRuleType string

// ReadinessRuleType values define the supported readiness evaluation strategies.
const (
	AllRequiredPass ReadinessRuleType = "all-required-pass"
)

// TriggerType defines how a pipeline is triggered.
type TriggerType string

// TriggerType values enumerate the supported pipeline trigger mechanisms.
const (
	TriggerHTTP          TriggerType = "http"
	TriggerCommand       TriggerType = "command"
	TriggerAirflow       TriggerType = "airflow"
	TriggerGlue          TriggerType = "glue"
	TriggerEMR           TriggerType = "emr"
	TriggerEMRServerless TriggerType = "emr-serverless"
	TriggerDatabricks    TriggerType = "databricks"
	TriggerStepFunction  TriggerType = "step-function"
)

// AlertType defines the alert sink type.
type AlertType string

// AlertType values enumerate the supported alert sink backends.
const (
	AlertConsole AlertType = "console"
	AlertWebhook AlertType = "webhook"
	AlertFile    AlertType = "file"
	AlertSNS     AlertType = "sns"
)

// AlertLevel replaces string-typed alert levels with a proper enum.
type AlertLevel string

const (
	AlertLevelError   AlertLevel = "error"
	AlertLevelWarning AlertLevel = "warning"
	AlertLevelInfo    AlertLevel = "info"
)

// FailureCategory classifies why a trait evaluation or trigger failed.
type FailureCategory string

const (
	FailureTransient      FailureCategory = "TRANSIENT"
	FailurePermanent      FailureCategory = "PERMANENT"
	FailureTimeout        FailureCategory = "TIMEOUT"
	FailureEvaluatorCrash FailureCategory = "EVALUATOR_CRASH"
)

// TraitDefinition defines a trait within an archetype.
type TraitDefinition struct {
	Type           string                 `yaml:"type" json:"type"`
	Description    string                 `yaml:"description,omitempty" json:"description,omitempty"`
	DefaultConfig  map[string]interface{} `yaml:"defaultConfig,omitempty" json:"defaultConfig,omitempty"`
	DefaultTTL     int                    `yaml:"defaultTtl,omitempty" json:"defaultTtl,omitempty"`
	DefaultTimeout int                    `yaml:"defaultTimeout,omitempty" json:"defaultTimeout,omitempty"`
}

// ReadinessRule defines how trait results combine into a readiness decision.
type ReadinessRule struct {
	Type ReadinessRuleType `yaml:"type" json:"type"`
}

// Archetype defines a STAMP archetype — a template of required safety checks.
type Archetype struct {
	Name           string            `yaml:"name" json:"name"`
	RequiredTraits []TraitDefinition `yaml:"requiredTraits" json:"requiredTraits"`
	OptionalTraits []TraitDefinition `yaml:"optionalTraits,omitempty" json:"optionalTraits,omitempty"`
	ReadinessRule  ReadinessRule     `yaml:"readinessRule" json:"readinessRule"`
}

// TraitConfig is a pipeline-level trait configuration that overrides archetype defaults.
type TraitConfig struct {
	Evaluator string                 `yaml:"evaluator,omitempty" json:"evaluator,omitempty"`
	Config    map[string]interface{} `yaml:"config,omitempty" json:"config,omitempty"`
	TTL       int                    `yaml:"ttl,omitempty" json:"ttl,omitempty"`
	Timeout   int                    `yaml:"timeout,omitempty" json:"timeout,omitempty"`
}

// TriggerConfig defines how to trigger a pipeline.
type TriggerConfig struct {
	Type         TriggerType       `yaml:"type" json:"type"`
	Method       string            `yaml:"method,omitempty" json:"method,omitempty"`
	URL          string            `yaml:"url,omitempty" json:"url,omitempty"`
	Headers      map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`
	Body         string            `yaml:"body,omitempty" json:"body,omitempty"`
	Command      string            `yaml:"command,omitempty" json:"command,omitempty"`
	Timeout      int               `yaml:"timeout,omitempty" json:"timeout,omitempty"`
	DagID           string            `yaml:"dagID,omitempty" json:"dagID,omitempty"`
	PollInterval    string            `yaml:"pollInterval,omitempty" json:"pollInterval,omitempty"`
	JobName         string            `yaml:"jobName,omitempty" json:"jobName,omitempty"`
	ClusterID       string            `yaml:"clusterId,omitempty" json:"clusterId,omitempty"`
	ApplicationID   string            `yaml:"applicationId,omitempty" json:"applicationId,omitempty"`
	WorkspaceURL    string            `yaml:"workspaceUrl,omitempty" json:"workspaceUrl,omitempty"`
	StateMachineARN string            `yaml:"stateMachineArn,omitempty" json:"stateMachineArn,omitempty"`
	Arguments       map[string]string `yaml:"arguments,omitempty" json:"arguments,omitempty"`
}


// RetryPolicy configures automatic retry behavior.
type RetryPolicy struct {
	MaxAttempts       int               `yaml:"maxAttempts" json:"maxAttempts"`
	BackoffSeconds    int               `yaml:"backoffSeconds" json:"backoffSeconds"`
	BackoffMultiplier float64           `yaml:"backoffMultiplier,omitempty" json:"backoffMultiplier,omitempty"`
	RetryableFailures []FailureCategory `yaml:"retryableFailures,omitempty" json:"retryableFailures,omitempty"`
}

// SLAConfig defines per-pipeline deadlines.
type SLAConfig struct {
	EvaluationDeadline string `yaml:"evaluationDeadline" json:"evaluationDeadline"`
	CompletionDeadline string `yaml:"completionDeadline" json:"completionDeadline"`
	Timezone           string `yaml:"timezone,omitempty" json:"timezone,omitempty"`
}

// WatcherConfig configures the reactive evaluation watcher.
type WatcherConfig struct {
	Enabled         bool   `yaml:"enabled" json:"enabled"`
	DefaultInterval string `yaml:"defaultInterval" json:"defaultInterval"`
}

// MonitoringConfig configures post-completion trait drift detection.
type MonitoringConfig struct {
	Enabled  bool   `yaml:"enabled" json:"enabled"`
	Duration string `yaml:"duration" json:"duration"` // e.g. "2h", "30m"
}

// PipelineWatchConfig overrides watcher settings per pipeline.
type PipelineWatchConfig struct {
	Interval   string           `yaml:"interval,omitempty" json:"interval,omitempty"`
	Enabled    *bool            `yaml:"enabled,omitempty" json:"enabled,omitempty"`
	Monitoring *MonitoringConfig `yaml:"monitoring,omitempty" json:"monitoring,omitempty"`
}

// Calendar defines a named set of exclusion days and dates.
type Calendar struct {
	Name  string   `yaml:"name" json:"name"`
	Days  []string `yaml:"days,omitempty" json:"days,omitempty"`   // "saturday", "sunday"
	Dates []string `yaml:"dates,omitempty" json:"dates,omitempty"` // "2025-12-25"
}

// ExclusionConfig defines when a pipeline should be dormant.
type ExclusionConfig struct {
	Calendar string   `yaml:"calendar,omitempty" json:"calendar,omitempty"` // reference to named Calendar
	Days     []string `yaml:"days,omitempty" json:"days,omitempty"`         // inline day overrides
	Dates    []string `yaml:"dates,omitempty" json:"dates,omitempty"`       // inline date overrides
}

// PipelineConfig is the full configuration for a registered pipeline.
type PipelineConfig struct {
	Name       string                 `yaml:"name" json:"name"`
	Archetype  string                 `yaml:"archetype" json:"archetype"`
	Tier       int                    `yaml:"tier,omitempty" json:"tier,omitempty"`
	Traits     map[string]TraitConfig `yaml:"traits,omitempty" json:"traits,omitempty"`
	Trigger    *TriggerConfig         `yaml:"trigger,omitempty" json:"trigger,omitempty"`
	Retry      *RetryPolicy           `yaml:"retry,omitempty" json:"retry,omitempty"`
	SLA        *SLAConfig             `yaml:"sla,omitempty" json:"sla,omitempty"`
	Watch      *PipelineWatchConfig   `yaml:"watch,omitempty" json:"watch,omitempty"`
	Schedules  []ScheduleConfig       `yaml:"schedules,omitempty" json:"schedules,omitempty"`
	Exclusions *ExclusionConfig       `yaml:"exclusions,omitempty" json:"exclusions,omitempty"`
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

// EvaluatorInput is the JSON sent to an evaluator subprocess on stdin.
type EvaluatorInput struct {
	PipelineID string                 `json:"pipelineId"`
	TraitType  string                 `json:"traitType"`
	Config     map[string]interface{} `json:"config"`
}

// EvaluatorOutput is the JSON expected from an evaluator subprocess on stdout.
type EvaluatorOutput struct {
	Status          TraitStatus            `json:"status"`
	Value           map[string]interface{} `json:"value,omitempty"`
	Reason          string                 `json:"reason,omitempty"`
	FailureCategory FailureCategory        `json:"failureCategory,omitempty"`
}

// AlertConfig defines an alert sink configuration.
type AlertConfig struct {
	Type     AlertType `yaml:"type" json:"type"`
	URL      string    `yaml:"url,omitempty" json:"url,omitempty"`
	Path     string    `yaml:"path,omitempty" json:"path,omitempty"`
	TopicARN string    `yaml:"topicArn,omitempty" json:"topicArn,omitempty"`
}

// Alert represents an alert event to be dispatched.
type Alert struct {
	Level      AlertLevel             `json:"level"`
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

// EventKind classifies the type of audit event.
type EventKind string

// EventKind values enumerate the categories of recorded events.
const (
	EventTraitEvaluated    EventKind = "TRAIT_EVALUATED"
	EventReadinessChecked  EventKind = "READINESS_CHECKED"
	EventRunStateChanged   EventKind = "RUN_STATE_CHANGED"
	EventTriggerFired      EventKind = "TRIGGER_FIRED"
	EventTriggerFailed     EventKind = "TRIGGER_FAILED"
	EventCallbackReceived  EventKind = "CALLBACK_RECEIVED"
	EventRetryScheduled    EventKind = "RETRY_SCHEDULED"
	EventRetryExhausted    EventKind = "RETRY_EXHAUSTED"
	EventSLABreached       EventKind = "SLA_BREACHED"
	EventWatcherEvaluation EventKind = "WATCHER_EVALUATION"
	EventTraitPushed          EventKind = "TRAIT_PUSHED"
	EventRerunRequested       EventKind = "RERUN_REQUESTED"
	EventRerunCompleted       EventKind = "RERUN_COMPLETED"
	EventMonitoringStarted    EventKind = "MONITORING_STARTED"
	EventMonitoringDrift      EventKind = "MONITORING_DRIFT_DETECTED"
	EventMonitoringCompleted  EventKind = "MONITORING_COMPLETED"
)

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
	StartedAt       time.Time       `json:"startedAt"`
	CompletedAt     *time.Time      `json:"completedAt,omitempty"`
	UpdatedAt       time.Time       `json:"updatedAt"`
}

// EngineConfig holds engine-level settings.
type EngineConfig struct {
	DefaultTimeout string `yaml:"defaultTimeout,omitempty" json:"defaultTimeout,omitempty"`
}

// ProjectConfig represents the top-level interlock.yaml configuration.
type ProjectConfig struct {
	Provider      string          `yaml:"provider"`
	Redis         *RedisConfig    `yaml:"redis,omitempty"`
	DynamoDB      *DynamoDBConfig `yaml:"dynamodb,omitempty"`
	Server        *ServerConfig   `yaml:"server,omitempty"`
	Engine        *EngineConfig   `yaml:"engine,omitempty"`
	ArchetypeDirs []string        `yaml:"archetypeDirs"`
	EvaluatorDirs []string        `yaml:"evaluatorDirs"`
	PipelineDirs  []string        `yaml:"pipelineDirs"`
	CalendarDirs  []string        `yaml:"calendarDirs,omitempty"`
	Alerts        []AlertConfig   `yaml:"alerts,omitempty"`
	Watcher       *WatcherConfig  `yaml:"watcher,omitempty"`
	Archiver      *ArchiverConfig `yaml:"archiver,omitempty"`
}

// RedisConfig holds Redis/Valkey connection settings.
type RedisConfig struct {
	Addr           string `yaml:"addr"`
	Password       string `yaml:"password,omitempty"`
	DB             int    `yaml:"db,omitempty"`
	KeyPrefix      string `yaml:"keyPrefix"`
	ReadinessTTL   string `yaml:"readinessTtl,omitempty" json:"readinessTtl,omitempty"`
	RetentionTTL   string `yaml:"retentionTtl,omitempty" json:"retentionTtl,omitempty"` // default "168h" (7 days)
	RunIndexLimit  int    `yaml:"runIndexLimit,omitempty" json:"runIndexLimit,omitempty"`
	EventStreamMax int64  `yaml:"eventStreamMax,omitempty" json:"eventStreamMax,omitempty"`
}

// DynamoDBConfig holds DynamoDB connection and table settings.
type DynamoDBConfig struct {
	TableName    string `yaml:"tableName" json:"tableName"`
	Region       string `yaml:"region" json:"region"`
	Endpoint     string `yaml:"endpoint,omitempty" json:"endpoint,omitempty"`
	ReadinessTTL string `yaml:"readinessTtl,omitempty" json:"readinessTtl,omitempty"`
	RetentionTTL string `yaml:"retentionTtl,omitempty" json:"retentionTtl,omitempty"`
	CreateTable  bool   `yaml:"createTable,omitempty" json:"createTable,omitempty"`
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

// ArchiverConfig configures the background Postgres archiver.
type ArchiverConfig struct {
	Enabled  bool   `yaml:"enabled" json:"enabled"`
	Interval string `yaml:"interval" json:"interval"` // e.g. "5m"
	DSN      string `yaml:"dsn" json:"dsn"`
}

// EventRecord pairs a Redis stream ID with an event for cursor-based reading.
type EventRecord struct {
	StreamID string `json:"streamId"`
	Event    Event  `json:"event"`
}

// ServerConfig holds HTTP server settings.
type ServerConfig struct {
	Addr           string `yaml:"addr"`
	APIKey         string `yaml:"apiKey,omitempty" json:"apiKey,omitempty"`
	MaxRequestBody int64  `yaml:"maxRequestBody,omitempty" json:"maxRequestBody,omitempty"`
}

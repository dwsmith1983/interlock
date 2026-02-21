// Package types defines the public domain types for the Interlock STAMP-based safety framework.
package types

import "time"

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
	TriggerHTTP    TriggerType = "http"
	TriggerCommand TriggerType = "command"
	TriggerAirflow TriggerType = "airflow"
)

// AlertType defines the alert sink type.
type AlertType string

// AlertType values enumerate the supported alert sink backends.
const (
	AlertConsole AlertType = "console"
	AlertWebhook AlertType = "webhook"
	AlertFile    AlertType = "file"
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
	DagID        string            `yaml:"dagID,omitempty" json:"dagID,omitempty"`
	PollInterval string            `yaml:"pollInterval,omitempty" json:"pollInterval,omitempty"`
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

// PipelineWatchConfig overrides watcher settings per pipeline.
type PipelineWatchConfig struct {
	Interval string `yaml:"interval,omitempty" json:"interval,omitempty"`
	Enabled  *bool  `yaml:"enabled,omitempty" json:"enabled,omitempty"`
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
	Type AlertType `yaml:"type" json:"type"`
	URL  string    `yaml:"url,omitempty" json:"url,omitempty"`
	Path string    `yaml:"path,omitempty" json:"path,omitempty"`
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

// RunLogEntry tracks durable per-pipeline-per-date run state.
type RunLogEntry struct {
	PipelineID      string          `json:"pipelineId"`
	Date            string          `json:"date"`
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
	Provider      string         `yaml:"provider"`
	Redis         *RedisConfig   `yaml:"redis,omitempty"`
	Server        *ServerConfig  `yaml:"server,omitempty"`
	Engine        *EngineConfig  `yaml:"engine,omitempty"`
	ArchetypeDirs []string       `yaml:"archetypeDirs"`
	EvaluatorDirs []string       `yaml:"evaluatorDirs"`
	PipelineDirs  []string       `yaml:"pipelineDirs"`
	Alerts        []AlertConfig  `yaml:"alerts,omitempty"`
	Watcher       *WatcherConfig `yaml:"watcher,omitempty"`
}

// RedisConfig holds Redis/Valkey connection settings.
type RedisConfig struct {
	Addr           string `yaml:"addr"`
	Password       string `yaml:"password,omitempty"`
	DB             int    `yaml:"db,omitempty"`
	KeyPrefix      string `yaml:"keyPrefix"`
	ReadinessTTL   string `yaml:"readinessTtl,omitempty" json:"readinessTtl,omitempty"`
	RunIndexLimit  int    `yaml:"runIndexLimit,omitempty" json:"runIndexLimit,omitempty"`
	EventStreamMax int64  `yaml:"eventStreamMax,omitempty" json:"eventStreamMax,omitempty"`
}

// ServerConfig holds HTTP server settings.
type ServerConfig struct {
	Addr           string `yaml:"addr"`
	APIKey         string `yaml:"apiKey,omitempty" json:"apiKey,omitempty"`
	MaxRequestBody int64  `yaml:"maxRequestBody,omitempty" json:"maxRequestBody,omitempty"`
}

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
	TraitPending TraitStatus = "PENDING"
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
)

// AlertType defines the alert sink type.
type AlertType string

// AlertType values enumerate the supported alert sink backends.
const (
	AlertConsole AlertType = "console"
	AlertWebhook AlertType = "webhook"
	AlertFile    AlertType = "file"
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
	Type    TriggerType       `yaml:"type" json:"type"`
	Method  string            `yaml:"method,omitempty" json:"method,omitempty"`
	URL     string            `yaml:"url,omitempty" json:"url,omitempty"`
	Headers map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`
	Body    string            `yaml:"body,omitempty" json:"body,omitempty"`
	Command string            `yaml:"command,omitempty" json:"command,omitempty"`
}

// PostActionConfig defines post-run monitoring behavior.
type PostActionConfig struct {
	Policy string                 `yaml:"policy,omitempty" json:"policy,omitempty"`
	Config map[string]interface{} `yaml:"config,omitempty" json:"config,omitempty"`
}

// Extensions holds pipeline extension configurations.
type Extensions struct {
	PostAction *PostActionConfig `yaml:"postAction,omitempty" json:"postAction,omitempty"`
}

// PipelineConfig is the full configuration for a registered pipeline.
type PipelineConfig struct {
	Name       string                 `yaml:"name" json:"name"`
	Archetype  string                 `yaml:"archetype" json:"archetype"`
	Tier       int                    `yaml:"tier,omitempty" json:"tier,omitempty"`
	Traits     map[string]TraitConfig `yaml:"traits,omitempty" json:"traits,omitempty"`
	Extensions *Extensions            `yaml:"extensions,omitempty" json:"extensions,omitempty"`
	Trigger    *TriggerConfig         `yaml:"trigger,omitempty" json:"trigger,omitempty"`
}

// TraitEvaluation is the result of evaluating a single trait.
type TraitEvaluation struct {
	PipelineID  string                 `json:"pipelineId"`
	TraitType   string                 `json:"traitType"`
	Status      TraitStatus            `json:"status"`
	Value       map[string]interface{} `json:"value,omitempty"`
	Reason      string                 `json:"reason,omitempty"`
	EvaluatedAt time.Time              `json:"evaluatedAt"`
	ExpiresAt   *time.Time             `json:"expiresAt,omitempty"`
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
	Status TraitStatus            `json:"status"`
	Value  map[string]interface{} `json:"value,omitempty"`
	Reason string                 `json:"reason,omitempty"`
}

// AlertConfig defines an alert sink configuration.
type AlertConfig struct {
	Type AlertType `yaml:"type" json:"type"`
	URL  string    `yaml:"url,omitempty" json:"url,omitempty"`
	Path string    `yaml:"path,omitempty" json:"path,omitempty"`
}

// Alert represents an alert event to be dispatched.
type Alert struct {
	Level      string                 `json:"level"`
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

// SourceEvent represents activity on a monitored source.
type SourceEvent struct {
	Source    string    `json:"source"`
	Type      string    `json:"type"`
	Timestamp time.Time `json:"timestamp"`
}

// SourceMonitorConfig configures source activity monitoring.
type SourceMonitorConfig struct {
	Source string `json:"source"`
	Type   string `json:"type"`
}

// EventKind classifies the type of audit event.
type EventKind string

// EventKind values enumerate the categories of recorded events.
const (
	EventTraitEvaluated   EventKind = "TRAIT_EVALUATED"
	EventReadinessChecked EventKind = "READINESS_CHECKED"
	EventRunStateChanged  EventKind = "RUN_STATE_CHANGED"
	EventTriggerFired     EventKind = "TRIGGER_FIRED"
	EventTriggerFailed    EventKind = "TRIGGER_FAILED"
	EventCallbackReceived EventKind = "CALLBACK_RECEIVED"
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

// ProjectConfig represents the top-level interlock.yaml configuration.
type ProjectConfig struct {
	Provider      string        `yaml:"provider"`
	Redis         *RedisConfig  `yaml:"redis,omitempty"`
	Server        *ServerConfig `yaml:"server,omitempty"`
	ArchetypeDirs []string      `yaml:"archetypeDirs"`
	EvaluatorDirs []string      `yaml:"evaluatorDirs"`
	PipelineDirs  []string      `yaml:"pipelineDirs"`
	Alerts        []AlertConfig `yaml:"alerts,omitempty"`
}

// RedisConfig holds Redis/Valkey connection settings.
type RedisConfig struct {
	Addr      string `yaml:"addr"`
	Password  string `yaml:"password,omitempty"`
	DB        int    `yaml:"db,omitempty"`
	KeyPrefix string `yaml:"keyPrefix"`
}

// ServerConfig holds HTTP server settings.
type ServerConfig struct {
	Addr string `yaml:"addr"`
}

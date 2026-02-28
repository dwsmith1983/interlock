package types

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
	Type       ReadinessRuleType      `yaml:"type" json:"type"`
	Parameters map[string]interface{} `yaml:"parameters,omitempty" json:"parameters,omitempty"`
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
	Type            TriggerType       `yaml:"type" json:"type"`
	Method          string            `yaml:"method,omitempty" json:"method,omitempty"`
	URL             string            `yaml:"url,omitempty" json:"url,omitempty"`
	Headers         map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`
	Body            string            `yaml:"body,omitempty" json:"body,omitempty"`
	Command         string            `yaml:"command,omitempty" json:"command,omitempty"`
	Timeout         int               `yaml:"timeout,omitempty" json:"timeout,omitempty"`
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
	ValidationTimeout  string `yaml:"validationTimeout,omitempty" json:"validationTimeout,omitempty"` // hard stop, e.g. "+45m"
	AtRiskLeadTime     string `yaml:"atRiskLeadTime,omitempty" json:"atRiskLeadTime,omitempty"`       // e.g. "5m"
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
	Interval   string            `yaml:"interval,omitempty" json:"interval,omitempty"`
	Enabled    *bool             `yaml:"enabled,omitempty" json:"enabled,omitempty"`
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

// CascadeConfig controls backpressure for downstream cascade notifications.
type CascadeConfig struct {
	DelayBetween string `yaml:"delayBetween,omitempty" json:"delayBetween,omitempty"` // e.g. "2s", "500ms"
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
	Cascade    *CascadeConfig         `yaml:"cascade,omitempty" json:"cascade,omitempty"`
	Quarantine *QuarantineConfig      `yaml:"quarantine,omitempty" json:"quarantine,omitempty"`
}

// QuarantineConfig controls how quarantined records affect pipeline status.
type QuarantineConfig struct {
	Blocking bool `yaml:"blocking" json:"blocking"` // true = fail pipeline on quarantine, false = proceed with alert
}

// EvaluatorInput is the JSON sent to an evaluator subprocess on stdin.
type EvaluatorInput struct {
	PipelineID string                 `json:"pipelineId"`
	TraitType  string                 `json:"traitType"`
	Config     map[string]interface{} `json:"config"`
}

// AlertConfig defines an alert sink configuration.
type AlertConfig struct {
	Type       AlertType `yaml:"type" json:"type"`
	URL        string    `yaml:"url,omitempty" json:"url,omitempty"`
	Path       string    `yaml:"path,omitempty" json:"path,omitempty"`
	TopicARN   string    `yaml:"topicArn,omitempty" json:"topicArn,omitempty"`
	BucketName string    `yaml:"bucketName,omitempty" json:"bucketName,omitempty"`
	Prefix     string    `yaml:"prefix,omitempty" json:"prefix,omitempty"`
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
	Watchdog      *WatchdogConfig `yaml:"watchdog,omitempty"`
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

// ArchiverConfig configures the background Postgres archiver.
type ArchiverConfig struct {
	Enabled  bool   `yaml:"enabled" json:"enabled"`
	Interval string `yaml:"interval" json:"interval"` // e.g. "5m"
	DSN      string `yaml:"dsn" json:"dsn"`
}

// WatchdogConfig configures the SLA watchdog that detects missed pipeline schedules.
type WatchdogConfig struct {
	Enabled           bool   `yaml:"enabled" json:"enabled"`
	Interval          string `yaml:"interval" json:"interval"`                                       // e.g. "5m"
	StuckRunThreshold string `yaml:"stuckRunThreshold,omitempty" json:"stuckRunThreshold,omitempty"` // e.g. "30m"
}

// ServerConfig holds HTTP server settings.
type ServerConfig struct {
	Addr           string `yaml:"addr"`
	APIKey         string `yaml:"apiKey,omitempty" json:"apiKey,omitempty"`
	MaxRequestBody int64  `yaml:"maxRequestBody,omitempty" json:"maxRequestBody,omitempty"`
}

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
// Only the variant pointer matching Type should be non-nil.
type TriggerConfig struct {
	Type          TriggerType                 `yaml:"type" json:"type"`
	PollInterval  string                      `yaml:"pollInterval,omitempty" json:"pollInterval,omitempty"`
	HTTP          *HTTPTriggerConfig          `yaml:"http,omitempty" json:"http,omitempty"`
	Command       *CommandTriggerConfig       `yaml:"command,omitempty" json:"command,omitempty"`
	Airflow       *AirflowTriggerConfig       `yaml:"airflow,omitempty" json:"airflow,omitempty"`
	Glue          *GlueTriggerConfig          `yaml:"glue,omitempty" json:"glue,omitempty"`
	EMR           *EMRTriggerConfig           `yaml:"emr,omitempty" json:"emr,omitempty"`
	EMRServerless *EMRServerlessTriggerConfig `yaml:"emr_serverless,omitempty" json:"emrServerless,omitempty"`
	StepFunction  *StepFunctionTriggerConfig  `yaml:"step_function,omitempty" json:"stepFunction,omitempty"`
	Databricks    *DatabricksTriggerConfig    `yaml:"databricks,omitempty" json:"databricks,omitempty"`
}

// HTTPTriggerConfig holds configuration for HTTP triggers.
type HTTPTriggerConfig struct {
	Method  string            `yaml:"method" json:"method"`
	URL     string            `yaml:"url" json:"url"`
	Headers map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`
	Body    string            `yaml:"body,omitempty" json:"body,omitempty"`
	Timeout int               `yaml:"timeout,omitempty" json:"timeout,omitempty"`
}

// CommandTriggerConfig holds configuration for command triggers.
type CommandTriggerConfig struct {
	Command string `yaml:"command" json:"command"`
}

// AirflowTriggerConfig holds configuration for Airflow triggers.
type AirflowTriggerConfig struct {
	DagID   string            `yaml:"dag_id" json:"dagId"`
	URL     string            `yaml:"url" json:"url"`
	Headers map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`
	Body    string            `yaml:"body,omitempty" json:"body,omitempty"`
	Timeout int               `yaml:"timeout,omitempty" json:"timeout,omitempty"`
}

// GlueTriggerConfig holds configuration for AWS Glue triggers.
type GlueTriggerConfig struct {
	JobName   string            `yaml:"job_name" json:"jobName"`
	Arguments map[string]string `yaml:"arguments,omitempty" json:"arguments,omitempty"`
}

// EMRTriggerConfig holds configuration for AWS EMR triggers.
type EMRTriggerConfig struct {
	ClusterID string            `yaml:"cluster_id" json:"clusterId"`
	StepName  string            `yaml:"step_name,omitempty" json:"stepName,omitempty"`
	Command   string            `yaml:"command,omitempty" json:"command,omitempty"`
	Arguments map[string]string `yaml:"arguments,omitempty" json:"arguments,omitempty"`
}

// EMRServerlessTriggerConfig holds configuration for AWS EMR Serverless triggers.
type EMRServerlessTriggerConfig struct {
	ApplicationID string `yaml:"application_id" json:"applicationId"`
	JobName       string `yaml:"job_name,omitempty" json:"jobName,omitempty"`
}

// StepFunctionTriggerConfig holds configuration for AWS Step Functions triggers.
type StepFunctionTriggerConfig struct {
	StateMachineARN string            `yaml:"state_machine_arn" json:"stateMachineArn"`
	Arguments       map[string]string `yaml:"arguments,omitempty" json:"arguments,omitempty"`
}

// DatabricksTriggerConfig holds configuration for Databricks triggers.
type DatabricksTriggerConfig struct {
	WorkspaceURL string            `yaml:"workspace_url" json:"workspaceUrl"`
	JobID        string            `yaml:"job_id,omitempty" json:"jobId,omitempty"`
	Headers      map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`
	Arguments    map[string]string `yaml:"arguments,omitempty" json:"arguments,omitempty"`
}

// TriggerArguments returns the arguments map for the active trigger variant, or nil.
func (tc *TriggerConfig) TriggerArguments() map[string]string {
	if tc == nil {
		return nil
	}
	switch tc.Type {
	case TriggerGlue:
		if tc.Glue != nil {
			return tc.Glue.Arguments
		}
	case TriggerEMR:
		if tc.EMR != nil {
			return tc.EMR.Arguments
		}
	case TriggerStepFunction:
		if tc.StepFunction != nil {
			return tc.StepFunction.Arguments
		}
	case TriggerDatabricks:
		if tc.Databricks != nil {
			return tc.Databricks.Arguments
		}
	}
	return nil
}

// TriggerHeaders returns the headers map for the active trigger variant, or nil.
func (tc *TriggerConfig) TriggerHeaders() map[string]string {
	if tc == nil {
		return nil
	}
	switch tc.Type {
	case TriggerHTTP:
		if tc.HTTP != nil {
			return tc.HTTP.Headers
		}
	case TriggerAirflow:
		if tc.Airflow != nil {
			return tc.Airflow.Headers
		}
	case TriggerDatabricks:
		if tc.Databricks != nil {
			return tc.Databricks.Headers
		}
	}
	return nil
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
//
// Redis and DynamoDB hold provider-specific configuration. After YAML
// unmarshalling they are populated with concrete types from the internal
// provider packages (e.g. *redis.Config, *dynamodb.Config) via a two-pass
// decode in the config loader. Consumers should use type assertions.
type ProjectConfig struct {
	Provider      string          `yaml:"provider"`
	Redis         any             `yaml:"redis,omitempty"`
	DynamoDB      any             `yaml:"dynamodb,omitempty"`
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

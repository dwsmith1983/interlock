// Package types defines the public domain types for the Interlock STAMP-based safety framework.
package types

// PipelineConfig is the full configuration for a pipeline, loaded from YAML.
type PipelineConfig struct {
	Pipeline   PipelineIdentity `yaml:"pipeline" json:"pipeline"`
	Schedule   ScheduleConfig   `yaml:"schedule" json:"schedule"`
	SLA        *SLAConfig       `yaml:"sla,omitempty" json:"sla,omitempty"`
	Validation ValidationConfig `yaml:"validation" json:"validation"`
	Job        JobConfig        `yaml:"job" json:"job"`
	PostRun    *PostRunConfig   `yaml:"postRun,omitempty" json:"postRun,omitempty"`
}

// PipelineIdentity holds pipeline metadata.
type PipelineIdentity struct {
	ID          string `yaml:"id" json:"id"`
	Owner       string `yaml:"owner,omitempty" json:"owner,omitempty"`
	Description string `yaml:"description,omitempty" json:"description,omitempty"`
}

// ScheduleConfig defines when evaluation starts and how often to retry.
type ScheduleConfig struct {
	Cron       string            `yaml:"cron,omitempty" json:"cron,omitempty"`
	Timezone   string            `yaml:"timezone,omitempty" json:"timezone,omitempty"`
	Trigger    *TriggerCondition `yaml:"trigger,omitempty" json:"trigger,omitempty"`
	Exclude    *ExclusionConfig  `yaml:"exclude,omitempty" json:"exclude,omitempty"`
	Calendar   string            `yaml:"calendar,omitempty" json:"calendar,omitempty"`
	Time       string            `yaml:"time,omitempty" json:"time,omitempty"`
	Evaluation EvaluationWindow  `yaml:"evaluation" json:"evaluation"`
}

// TriggerCondition defines which sensor write starts evaluation.
type TriggerCondition struct {
	Key   string      `yaml:"key" json:"key"`
	Check CheckOp     `yaml:"check" json:"check"`
	Field string      `yaml:"field" json:"field"`
	Value interface{} `yaml:"value" json:"value"`
}

// ExclusionConfig defines when a pipeline should NOT run.
type ExclusionConfig struct {
	Weekends bool     `yaml:"weekends,omitempty" json:"weekends,omitempty"`
	Holidays string   `yaml:"holidays,omitempty" json:"holidays,omitempty"`
	Dates    []string `yaml:"dates,omitempty" json:"dates,omitempty"`
}

// EvaluationWindow defines how long and how often to validate.
type EvaluationWindow struct {
	Window   string `yaml:"window" json:"window"`     // e.g. "1h"
	Interval string `yaml:"interval" json:"interval"` // e.g. "5m"
}

// SLAConfig defines pipeline deadlines (observational, never stops execution).
type SLAConfig struct {
	Deadline         string `yaml:"deadline" json:"deadline"`                 // "HH:MM"
	ExpectedDuration string `yaml:"expectedDuration" json:"expectedDuration"` // e.g. "30m"
	Timezone         string `yaml:"timezone,omitempty" json:"timezone,omitempty"`
	Critical         bool   `yaml:"critical,omitempty" json:"critical,omitempty"`
}

// ValidationConfig defines the rules that must pass before triggering.
type ValidationConfig struct {
	Trigger string           `yaml:"trigger" json:"trigger"` // "ALL" or "ANY"
	Rules   []ValidationRule `yaml:"rules" json:"rules"`
}

// ValidationRule is a single declarative check against a sensor key.
type ValidationRule struct {
	Key   string      `yaml:"key" json:"key"`     // e.g. "SENSOR#silver-orders-complete"
	Check CheckOp     `yaml:"check" json:"check"` // equals, gte, lte, gt, lt, exists, age_lt
	Field string      `yaml:"field,omitempty" json:"field,omitempty"`
	Value interface{} `yaml:"value,omitempty" json:"value,omitempty"`
}

// CheckOp is a validation rule operator.
type CheckOp string

const (
	CheckEquals CheckOp = "equals"
	CheckGTE    CheckOp = "gte"
	CheckLTE    CheckOp = "lte"
	CheckGT     CheckOp = "gt"
	CheckLT     CheckOp = "lt"
	CheckExists CheckOp = "exists"
	CheckAgeLT  CheckOp = "age_lt"
)

// JobConfig defines what to trigger and how many retries.
type JobConfig struct {
	Type       TriggerType            `yaml:"type" json:"type"`
	Config     map[string]interface{} `yaml:"config" json:"config"`
	MaxRetries int                    `yaml:"maxRetries,omitempty" json:"maxRetries,omitempty"`
}

// PostRunConfig defines optional post-completion validation.
type PostRunConfig struct {
	Evaluation *EvaluationWindow `yaml:"evaluation,omitempty" json:"evaluation,omitempty"`
	Rules      []ValidationRule  `yaml:"rules" json:"rules"`
}

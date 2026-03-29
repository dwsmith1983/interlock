// Package types defines the public domain types for the Interlock STAMP-based safety framework.
package types

import (
	"encoding/json"
	"slices"
)

// PipelineConfig is the full configuration for a pipeline, loaded from YAML.
type PipelineConfig struct {
	Pipeline   PipelineIdentity `yaml:"pipeline" json:"pipeline"`
	Schedule   ScheduleConfig   `yaml:"schedule" json:"schedule"`
	SLA        *SLAConfig       `yaml:"sla,omitempty" json:"sla,omitempty"`
	Validation ValidationConfig `yaml:"validation" json:"validation"`
	Job        JobConfig        `yaml:"job" json:"job"`
	PostRun    *PostRunConfig   `yaml:"postRun,omitempty" json:"postRun,omitempty"`
	DryRun     bool             `yaml:"dryRun,omitempty" json:"dryRun,omitempty"`
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
	Include    *InclusionConfig  `yaml:"include,omitempty" json:"include,omitempty"`
	Calendar   string            `yaml:"calendar,omitempty" json:"calendar,omitempty"`
	Time       string            `yaml:"time,omitempty" json:"time,omitempty"`
	Evaluation EvaluationWindow  `yaml:"evaluation" json:"evaluation"`
}

// InclusionConfig defines explicit dates when a pipeline SHOULD run.
// Mutually exclusive with Cron.
type InclusionConfig struct {
	Dates []string `yaml:"dates" json:"dates"` // YYYY-MM-DD
}

// TriggerCondition defines which sensor write starts evaluation.
type TriggerCondition struct {
	Key      string      `yaml:"key" json:"key"`
	Check    CheckOp     `yaml:"check" json:"check"`
	Field    string      `yaml:"field" json:"field"`
	Value    interface{} `yaml:"value" json:"value"`
	Deadline string      `yaml:"deadline,omitempty" json:"deadline,omitempty"` // ":MM" or "HH:MM" — auto-trigger window closes after this deadline
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
	Deadline         string `yaml:"deadline,omitempty" json:"deadline,omitempty"`                 // "HH:MM" wall-clock deadline
	ExpectedDuration string `yaml:"expectedDuration,omitempty" json:"expectedDuration,omitempty"` // e.g. "30m"
	MaxDuration      string `yaml:"maxDuration,omitempty" json:"maxDuration,omitempty"`           // e.g. "2h"; relative SLA from first sensor arrival
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
	Type                 TriggerType            `yaml:"type" json:"type"`
	Config               map[string]interface{} `yaml:"config" json:"config"`
	MaxRetries           int                    `yaml:"maxRetries,omitempty" json:"maxRetries,omitempty"`
	JobPollWindowSeconds *int                   `yaml:"jobPollWindowSeconds,omitempty" json:"jobPollWindowSeconds,omitempty"`
	MaxDriftReruns       *int                   `yaml:"maxDriftReruns,omitempty" json:"maxDriftReruns,omitempty"`
	MaxManualReruns      *int                   `yaml:"maxManualReruns,omitempty" json:"maxManualReruns,omitempty"`
	MaxCodeRetries       *int                   `yaml:"maxCodeRetries,omitempty" json:"maxCodeRetries,omitempty"`
}

// IntOrDefault returns *p if non-nil, otherwise def.
func IntOrDefault(p *int, def int) int {
	if p != nil {
		return *p
	}
	return def
}

// PostRunConfig defines optional post-completion validation.
type PostRunConfig struct {
	Evaluation     *EvaluationWindow `yaml:"evaluation,omitempty" json:"evaluation,omitempty"`
	Rules          []ValidationRule  `yaml:"rules" json:"rules"`
	SensorTimeout  string            `yaml:"sensorTimeout,omitempty" json:"sensorTimeout,omitempty"`   // e.g. "2h"; default 2h
	DriftThreshold *float64          `yaml:"driftThreshold,omitempty" json:"driftThreshold,omitempty"` // minimum absolute delta to trigger drift; default 0 (any change)
	DriftField     string            `yaml:"driftField,omitempty" json:"driftField,omitempty"`         // sensor field for drift comparison; default "sensor_count"
}

// DeepCopy returns a deep copy of the PipelineConfig. Pointer fields, slices,
// and maps are copied so mutations to the copy do not affect the original.
func (c *PipelineConfig) DeepCopy() *PipelineConfig {
	cp := *c

	// Schedule pointer fields
	if c.Schedule.Trigger != nil {
		t := *c.Schedule.Trigger
		cp.Schedule.Trigger = &t
	}
	if c.Schedule.Exclude != nil {
		ex := *c.Schedule.Exclude
		ex.Dates = slices.Clone(c.Schedule.Exclude.Dates)
		cp.Schedule.Exclude = &ex
	}
	if c.Schedule.Include != nil {
		inc := *c.Schedule.Include
		inc.Dates = slices.Clone(c.Schedule.Include.Dates)
		cp.Schedule.Include = &inc
	}

	// SLA
	if c.SLA != nil {
		s := *c.SLA
		cp.SLA = &s
	}

	// Validation rules slice
	cp.Validation.Rules = slices.Clone(c.Validation.Rules)

	// Job pointer fields
	if c.Job.JobPollWindowSeconds != nil {
		v := *c.Job.JobPollWindowSeconds
		cp.Job.JobPollWindowSeconds = &v
	}
	if c.Job.MaxDriftReruns != nil {
		v := *c.Job.MaxDriftReruns
		cp.Job.MaxDriftReruns = &v
	}
	if c.Job.MaxManualReruns != nil {
		v := *c.Job.MaxManualReruns
		cp.Job.MaxManualReruns = &v
	}
	if c.Job.MaxCodeRetries != nil {
		v := *c.Job.MaxCodeRetries
		cp.Job.MaxCodeRetries = &v
	}

	// Job.Config map — JSON roundtrip for map[string]interface{} (unavoidable for dynamic maps)
	if c.Job.Config != nil {
		cp.Job.Config = make(map[string]interface{}, len(c.Job.Config))
		data, _ := json.Marshal(c.Job.Config)
		_ = json.Unmarshal(data, &cp.Job.Config)
	}

	// PostRun
	if c.PostRun != nil {
		pr := *c.PostRun
		pr.Rules = slices.Clone(c.PostRun.Rules)
		if c.PostRun.Evaluation != nil {
			ev := *c.PostRun.Evaluation
			pr.Evaluation = &ev
		}
		if c.PostRun.DriftThreshold != nil {
			v := *c.PostRun.DriftThreshold
			pr.DriftThreshold = &v
		}
		cp.PostRun = &pr
	}

	return &cp
}

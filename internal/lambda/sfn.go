package lambda

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// SFNInput is the top-level input for the Step Function state machine.
//
// The state machine runs in JSONPath mode: a Parameters reference to a missing
// path raises States.Runtime, which is neither retriable nor catchable by
// Catch: ["States.ALL"]. Every field the ASL dereferences is therefore emitted
// unconditionally (no omitempty), even when empty. Only Config.SLA keeps
// omitempty, because the CheckCancelSLA and CheckSLAForCompleteTriggerFailure
// Choice states use IsPresent on $.config.sla to decide whether to run the
// SLA branch at all.
type SFNInput struct {
	PipelineID      string    `json:"pipelineId"`
	ScheduleID      string    `json:"scheduleId"`
	Date            string    `json:"date"`
	SensorArrivalAt string    `json:"sensorArrivalAt"` // RFC3339; empty when unknown
	Config          SFNConfig `json:"config"`
}

// SFNConfig holds timing parameters for the SFN evaluation loop and SLA branch.
type SFNConfig struct {
	EvaluationIntervalSeconds int     `json:"evaluationIntervalSeconds"`
	EvaluationWindowSeconds   int     `json:"evaluationWindowSeconds"`
	JobCheckIntervalSeconds   int     `json:"jobCheckIntervalSeconds"`
	JobPollWindowSeconds      int     `json:"jobPollWindowSeconds"`
	SLA                       *SFNSLA `json:"sla,omitempty"`
}

// SFNSLA mirrors types.SLAConfig without omitempty. types.SLAConfig omits every
// empty field, which is what made CancelSLASchedules and
// CancelSLAOnCompleteTriggerFailure fail with States.Runtime for every
// SLA-configured execution.
type SFNSLA struct {
	Deadline         string `json:"deadline"`
	ExpectedDuration string `json:"expectedDuration"`
	MaxDuration      string `json:"maxDuration"`
	Timezone         string `json:"timezone"`
	Critical         bool   `json:"critical"`
}

// BuildSFNConfig converts a PipelineConfig into the config block for the SFN input.
func BuildSFNConfig(cfg *types.PipelineConfig) SFNConfig {
	sc := SFNConfig{
		EvaluationIntervalSeconds: DefaultEvalIntervalSec,
		EvaluationWindowSeconds:   DefaultEvalWindowSec,
		JobCheckIntervalSeconds:   DefaultJobCheckIntervalSec,
		JobPollWindowSeconds:      DefaultJobPollWindowSec,
	}

	if d, err := time.ParseDuration(cfg.Schedule.Evaluation.Interval); err == nil && d > 0 {
		sc.EvaluationIntervalSeconds = int(d.Seconds())
	}
	if d, err := time.ParseDuration(cfg.Schedule.Evaluation.Window); err == nil && d > 0 {
		sc.EvaluationWindowSeconds = int(d.Seconds())
	}

	if cfg.Job.JobPollWindowSeconds != nil && *cfg.Job.JobPollWindowSeconds > 0 {
		sc.JobPollWindowSeconds = *cfg.Job.JobPollWindowSeconds
	}

	if cfg.SLA != nil {
		tz := cfg.SLA.Timezone
		if tz == "" {
			tz = "UTC"
		}
		sc.SLA = &SFNSLA{
			Deadline:         cfg.SLA.Deadline,
			ExpectedDuration: cfg.SLA.ExpectedDuration,
			MaxDuration:      cfg.SLA.MaxDuration,
			Timezone:         tz,
			Critical:         cfg.SLA.Critical,
		}
	}

	return sc
}

// BuildSFNInput assembles the full Step Functions execution input.
func BuildSFNInput(cfg *types.PipelineConfig, pipelineID, scheduleID, date, sensorArrivalAt string) SFNInput {
	return SFNInput{
		PipelineID:      pipelineID,
		ScheduleID:      scheduleID,
		Date:            date,
		SensorArrivalAt: sensorArrivalAt,
		Config:          BuildSFNConfig(cfg),
	}
}

// TruncateExecName ensures an SFN execution name does not exceed the 80-character
// AWS limit. When truncation is needed the suffix (date + timestamp) is preserved
// by trimming characters from the beginning of the name.
func TruncateExecName(name string) string {
	if len(name) <= SFNExecNameMaxLen {
		return name
	}
	return name[len(name)-SFNExecNameMaxLen:]
}

// StartSFN starts a Step Function execution with a unique execution name.
// The name includes a Unix timestamp suffix to avoid ExecutionAlreadyExists
// errors when a previous execution for the same pipeline/schedule/date failed.
func StartSFN(ctx context.Context, d *Deps, cfg *types.PipelineConfig, pipelineID, scheduleID, date string) error {
	name := TruncateExecName(fmt.Sprintf("%s-%s-%s-%d", pipelineID, scheduleID, date, d.Now().Unix()))
	return StartSFNWithName(ctx, d, cfg, pipelineID, scheduleID, date, name)
}

// StartSFNWithName starts a Step Function execution with a custom execution name.
// Defense-in-depth: refuses to start if the pipeline is in dry-run mode.
func StartSFNWithName(ctx context.Context, d *Deps, cfg *types.PipelineConfig, pipelineID, scheduleID, date, name string) error {
	if cfg.DryRun {
		d.Logger.Warn("StartSFNWithName called for dry-run pipeline, suppressing execution",
			"pipelineId", pipelineID, "schedule", scheduleID, "date", date)
		return nil
	}

	input := BuildSFNInput(cfg, pipelineID, scheduleID, date, "")

	// Warn if the sum of evaluation + poll windows exceeds the SFN timeout.
	totalWindowSec := input.Config.EvaluationWindowSeconds + input.Config.JobPollWindowSeconds
	sfnTimeout := ResolveTriggerLockTTL() - TriggerLockBuffer // strip the buffer to get raw SFN timeout
	if sfnTimeout > 0 && time.Duration(totalWindowSec)*time.Second > sfnTimeout {
		d.Logger.Warn("combined pipeline windows exceed SFN timeout",
			"pipelineId", pipelineID,
			"evalWindowSec", input.Config.EvaluationWindowSeconds,
			"jobPollWindowSec", input.Config.JobPollWindowSeconds,
			"totalWindowSec", totalWindowSec,
			"sfnTimeoutSec", int(sfnTimeout.Seconds()),
		)
	}

	// Populate sensorArrivalAt for relative SLA passthrough.
	if input.Config.SLA != nil && input.Config.SLA.MaxDuration != "" && d.Store != nil {
		arrivalKey := "first-sensor-arrival#" + date
		arrivalData, readErr := d.Store.GetSensorData(ctx, pipelineID, arrivalKey)
		if readErr != nil {
			d.Logger.WarnContext(ctx, "failed to read first-sensor-arrival for SFN input",
				"pipelineId", pipelineID, "error", readErr)
		} else if arrivalData != nil {
			if at, ok := arrivalData["arrivedAt"].(string); ok {
				input.SensorArrivalAt = at
			}
		}
	}

	payload, err := json.Marshal(input)
	if err != nil {
		return fmt.Errorf("marshal SFN input: %w", err)
	}

	inputStr := string(payload)

	_, err = d.SFNClient.StartExecution(ctx, &sfn.StartExecutionInput{
		StateMachineArn: &d.StateMachineARN,
		Name:            &name,
		Input:           &inputStr,
	})
	if err != nil {
		return fmt.Errorf("StartExecution: %w", err)
	}
	return nil
}

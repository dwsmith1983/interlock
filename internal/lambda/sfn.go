package lambda

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// sfnInput is the top-level input for the Step Function state machine.
// It includes pipeline identity fields and a config block used by Wait states.
type sfnInput struct {
	PipelineID      string    `json:"pipelineId"`
	ScheduleID      string    `json:"scheduleId"`
	Date            string    `json:"date"`
	SensorArrivalAt string    `json:"sensorArrivalAt,omitempty"` // RFC3339; first sensor arrival for relative SLA
	Config          sfnConfig `json:"config"`
}

// sfnConfig holds timing parameters for the SFN evaluation loop and SLA branch.
type sfnConfig struct {
	EvaluationIntervalSeconds int              `json:"evaluationIntervalSeconds"`
	EvaluationWindowSeconds   int              `json:"evaluationWindowSeconds"`
	JobCheckIntervalSeconds   int              `json:"jobCheckIntervalSeconds"`
	JobPollWindowSeconds      int              `json:"jobPollWindowSeconds"`
	SLA                       *types.SLAConfig `json:"sla,omitempty"`
}

// buildSFNConfig converts a PipelineConfig into the config block for the SFN input.
func buildSFNConfig(cfg *types.PipelineConfig) sfnConfig {
	sc := sfnConfig{
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
		sla := *cfg.SLA
		if sla.Timezone == "" {
			sla.Timezone = "UTC"
		}
		sc.SLA = &sla
	}

	return sc
}

// truncateExecName ensures an SFN execution name does not exceed the 80-character
// AWS limit. When truncation is needed the suffix (date + timestamp) is preserved
// by trimming characters from the beginning of the name.
func truncateExecName(name string) string {
	if len(name) <= SFNExecNameMaxLen {
		return name
	}
	return name[len(name)-SFNExecNameMaxLen:]
}

// startSFN starts a Step Function execution with a unique execution name.
// The name includes a Unix timestamp suffix to avoid ExecutionAlreadyExists
// errors when a previous execution for the same pipeline/schedule/date failed.
func startSFN(ctx context.Context, d *Deps, cfg *types.PipelineConfig, pipelineID, scheduleID, date string) error {
	name := truncateExecName(fmt.Sprintf("%s-%s-%s-%d", pipelineID, scheduleID, date, d.now().Unix()))
	return startSFNWithName(ctx, d, cfg, pipelineID, scheduleID, date, name)
}

// startSFNWithName starts a Step Function execution with a custom execution name.
func startSFNWithName(ctx context.Context, d *Deps, cfg *types.PipelineConfig, pipelineID, scheduleID, date, name string) error {
	sc := buildSFNConfig(cfg)

	// Warn if the sum of evaluation + poll windows exceeds the SFN timeout.
	totalWindowSec := sc.EvaluationWindowSeconds + sc.JobPollWindowSeconds
	sfnTimeout := ResolveTriggerLockTTL() - TriggerLockBuffer // strip the buffer to get raw SFN timeout
	if sfnTimeout > 0 && time.Duration(totalWindowSec)*time.Second > sfnTimeout {
		d.Logger.Warn("combined pipeline windows exceed SFN timeout",
			"pipelineId", pipelineID,
			"evalWindowSec", sc.EvaluationWindowSeconds,
			"jobPollWindowSec", sc.JobPollWindowSeconds,
			"totalWindowSec", totalWindowSec,
			"sfnTimeoutSec", int(sfnTimeout.Seconds()),
		)
	}

	input := sfnInput{
		PipelineID: pipelineID,
		ScheduleID: scheduleID,
		Date:       date,
		Config:     sc,
	}

	// Populate sensorArrivalAt for relative SLA passthrough.
	if sc.SLA != nil && sc.SLA.MaxDuration != "" && d.Store != nil {
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

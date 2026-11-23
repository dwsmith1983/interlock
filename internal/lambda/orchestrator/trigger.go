package orchestrator

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/store"
	"github.com/dwsmith1983/interlock/internal/trigger"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/dwsmith1983/interlock/pkg/validation"
)

// handleTrigger builds a TriggerConfig from the JobConfig, executes it,
// publishes JOB_TRIGGERED, and returns the run ID.
func handleTrigger(ctx context.Context, d *lambda.Deps, input lambda.OrchestratorInput) (lambda.OrchestratorOutput, error) {
	// Configuration failures return a Lambda error rather than a partial
	// result: the HasTriggerResult Choice tests IsPresent on $.triggerResult,
	// so a partial result would send the execution to CheckJob, which
	// dereferences $.triggerResult.runId and raises an uncatchable
	// States.Runtime while the trigger lock stays RUNNING. A Lambda error is
	// retried by the Trigger state and then caught by TriggerRetryExhausted,
	// which ends the date FAILED_FINAL.
	cfg, err := d.Store.GetConfig(ctx, input.PipelineID)
	if err != nil {
		return lambda.OrchestratorOutput{}, fmt.Errorf("trigger get config: %w", err)
	}
	if cfg == nil {
		return lambda.OrchestratorOutput{}, fmt.Errorf("trigger: config not found for pipeline %q", input.PipelineID)
	}
	// The row is read fresh, so one rewritten after the run started is
	// validated again before anything is sent. Validation runs on the stored
	// row, before any INTERLOCK_ value is expanded, and its reasons never
	// quote the url, headers, body, payload or arguments.
	if errs := validation.ValidatePipelineConfig(cfg); len(errs) > 0 {
		return lambda.OrchestratorOutput{}, fmt.Errorf("trigger: config for pipeline %q is invalid: %s",
			input.PipelineID, store.CutReason(strings.Join(errs, "; ")))
	}

	triggerCfg, err := BuildTriggerConfig(cfg.Job)
	if err != nil {
		return lambda.OrchestratorOutput{}, fmt.Errorf("trigger build config: %w", err)
	}

	// The execution name identifies this run; a job that reports its own
	// result writes it as the runId of its JOB# row.
	runID := input.ExecutionName
	if runID == "" {
		runID = fmt.Sprintf("%s-%s-%s-%d", input.PipelineID, input.ScheduleID, input.Date, d.Now().UnixMilli())
	}
	run := types.RunIdentity{PipelineID: input.PipelineID, ScheduleID: input.ScheduleID, Date: input.Date, RunID: runID}
	if triggerCfg.HTTP != nil {
		triggerCfg.HTTP.Run = run
	}
	if triggerCfg.Lambda != nil {
		triggerCfg.Lambda.Run = run
	}
	if triggerCfg.Airflow != nil {
		triggerCfg.Airflow.Run = run
	}
	if triggerCfg.EMRServerless != nil {
		triggerCfg.EMRServerless.Run = run
	}
	if triggerCfg.Databricks != nil {
		triggerCfg.Databricks.Run = run
	}
	InjectDateArgs(&triggerCfg, input.Date)

	// startedAt marks the launch for the start-by check: set first, as a lambda job runs inside Execute.
	if markErr := d.Store.MarkTriggerStarted(ctx, input.PipelineID, input.ScheduleID, input.Date, d.Now()); markErr != nil {
		d.Logger.WarnContext(ctx, "failed to mark trigger started", "error", markErr,
			"pipeline", input.PipelineID, "schedule", input.ScheduleID, "date", input.Date, "runId", runID)
	}
	metadata, err := d.TriggerRunner.Execute(ctx, &triggerCfg)
	// A job that ran and failed is the run's result, which CompleteTrigger
	// decides on; every other error kept the job from starting.
	var jobErr *trigger.JobFailedError
	if err != nil && !errors.As(err, &jobErr) {
		errMsg := fmt.Sprintf("trigger execute: %v", err)
		// Log infra failure to joblog for audit trail, then return Lambda error
		// so Step Functions Retry handles exponential backoff.
		if writeErr := d.Store.WriteJobEvent(ctx, input.PipelineID, input.ScheduleID, input.Date, types.JobEventInfraTriggerFailure, runID, 0, errMsg); writeErr != nil {
			d.Logger.WarnContext(ctx, "failed to write infra trigger failure to joblog", "error", writeErr, "pipeline", input.PipelineID)
		}
		return lambda.OrchestratorOutput{}, fmt.Errorf("%s", errMsg)
	}

	if err := lambda.PublishEvent(ctx, d, string(types.EventJobTriggered), input.PipelineID, input.ScheduleID, input.Date, fmt.Sprintf("triggered %s job", cfg.Job.Type)); err != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventJobTriggered, "error", err)
	}

	switch cfg.Job.Type {
	case types.TriggerHTTP:
		// The job reports its own result: the run stays RUNNING until a
		// JOB# row for runID appears or the poll window closes.
	case types.TriggerLambda:
		event, msg := types.JobEventSuccess, fmt.Sprintf("%s trigger completed synchronously", cfg.Job.Type)
		var opts []store.JobEventOption
		if err != nil {
			event, msg = types.JobEventFail, err.Error()
			opts = append(opts, store.WithFailureCategory(cmp.Or(jobErr.Category, types.FailurePermanent)))
		}
		if err := d.Store.WriteJobEvent(ctx, input.PipelineID, input.ScheduleID, input.Date, event, runID, 0, msg, opts...); err != nil {
			// This log is then the only record of the job's result.
			d.Logger.ErrorContext(ctx, "failed to write job result joblog", "error", err, "event", event, "runId", runID,
				"jobResult", msg, "pipeline", input.PipelineID, "schedule", input.ScheduleID, "date", input.Date)
		}
	default:
		if id := ExtractRunID(metadata); id != "" {
			runID = id
		}
	}

	// CheckJob dereferences $.triggerResult.metadata, so it must be an object.
	if metadata == nil {
		metadata = map[string]interface{}{}
	}

	return lambda.OrchestratorOutput{
		Mode:     "trigger",
		RunID:    runID,
		JobType:  string(cfg.Job.Type),
		Metadata: metadata,
	}, nil
}

// BuildTriggerConfig converts a JobConfig into a TriggerConfig by
// JSON-marshalling the config map and unmarshalling it into the typed sub-struct.
// It delegates to the canonical TriggerUnmarshalers registry in the parent
// lambda package to avoid duplication.
func BuildTriggerConfig(job types.JobConfig) (types.TriggerConfig, error) {
	tc := types.TriggerConfig{Type: job.Type}

	unmarshal, ok := lambda.TriggerUnmarshalers[job.Type]
	if !ok {
		return tc, fmt.Errorf("unsupported trigger type: %s", job.Type)
	}

	if len(job.Config) == 0 {
		return tc, nil
	}

	data, err := json.Marshal(job.Config)
	if err != nil {
		return tc, fmt.Errorf("marshal job config: %w", err)
	}

	if err := unmarshal(data, &tc); err != nil {
		return tc, fmt.Errorf("unmarshal %s config: %w", job.Type, err)
	}

	return tc, nil
}

// ExtractRunID searches trigger metadata for a recognisable run identifier.
// It wraps lambda.ExtractRunID, whose runIDMetadataKeys is the single list of
// run ID keys.
func ExtractRunID(metadata map[string]interface{}) string {
	return lambda.ExtractRunID(metadata)
}

// InjectDateArgs gives the job the execution date it runs for. Glue and EMR
// arguments and EMR Serverless entry point arguments get --par_day (and
// --par_hour for hourly dates), a step function's input and a databricks
// job's parameters par_day (and par_hour), overwriting a configured value.
// An http body, lambda payload or airflow body left empty becomes the
// generated run object (see runObject), marked generated so it is sent as is.
func InjectDateArgs(tc *types.TriggerConfig, date string) {
	datePart, hourPart := lambda.ParseExecutionDate(date)
	parDay := strings.ReplaceAll(datePart, "-", "")

	if tc.Glue != nil {
		tc.Glue.Arguments = withDateArgs(tc.Glue.Arguments, "--", parDay, hourPart)
	}
	if tc.EMR != nil {
		tc.EMR.Arguments = withDateArgs(tc.EMR.Arguments, "--", parDay, hourPart)
	}
	if tc.EMRServerless != nil {
		tc.EMRServerless.EntryPointArguments = withDateFlags(tc.EMRServerless.EntryPointArguments, parDay, hourPart)
	}
	if tc.StepFunction != nil {
		tc.StepFunction.Arguments = withDateArgs(tc.StepFunction.Arguments, "", parDay, hourPart)
	}
	if tc.Databricks != nil {
		tc.Databricks.Arguments = withDateArgs(tc.Databricks.Arguments, "", parDay, hourPart)
	}
	if tc.HTTP != nil && tc.HTTP.Body == "" {
		tc.HTTP.Body, tc.HTTP.BodyGenerated = runObject(tc.HTTP.Run, parDay, hourPart), true
	}
	if tc.Lambda != nil && tc.Lambda.Payload == "" {
		tc.Lambda.Payload, tc.Lambda.PayloadGenerated = runObject(tc.Lambda.Run, parDay, hourPart), true
	}
	if tc.Airflow != nil && tc.Airflow.Body == "" {
		tc.Airflow.Body, tc.Airflow.BodyGenerated = runObject(tc.Airflow.Run, parDay, hourPart), true
	}
}

// withDateArgs sets prefix+"par_day" and, for an hourly date, prefix+"par_hour"
// in args, creating the map when nil.
func withDateArgs(args map[string]string, prefix, parDay, parHour string) map[string]string {
	if args == nil {
		args = make(map[string]string)
	}
	args[prefix+"par_day"] = parDay
	if parHour != "" {
		args[prefix+"par_hour"] = parHour
	}
	return args
}

// withDateFlags returns a copy of the argument list with "--par_day" and, for
// an hourly date, "--par_hour" set: a flag already in the list keeps its place
// and takes the new value in the next argument, otherwise flag and value are
// appended.
func withDateFlags(args []string, parDay, parHour string) []string {
	out := slices.Clone(args)
	set := func(flag, value string) {
		if i := slices.Index(out, flag); i >= 0 && i+1 < len(out) {
			out[i+1] = value
			return
		}
		out = append(out, flag, value)
	}
	set("--par_day", parDay)
	if parHour != "" {
		set("--par_hour", parHour)
	}
	return out
}

// runObject is the JSON object a body or payload job receives when none is
// configured: par_day, par_hour (hourly dates only) and the run identity.
func runObject(run types.RunIdentity, parDay, parHour string) string {
	obj := map[string]string{
		"par_day": parDay, "pipelineId": run.PipelineID, "scheduleId": run.ScheduleID, "date": run.Date, "runId": run.RunID,
	}
	if parHour != "" {
		obj["par_hour"] = parHour
	}
	b, _ := json.Marshal(obj) // json.Marshal is infallible for map[string]string (no channels, funcs, or complex types)
	return string(b)
}

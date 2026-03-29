package orchestrator

import (
	"context"

	"github.com/dwsmith1983/interlock/internal/store"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// handleCheckJob queries the job log for the latest event. If no event exists
// and a StatusChecker is configured, it polls the trigger API directly and
// writes terminal results (succeeded/failed) to the job log.
func handleCheckJob(ctx context.Context, d *lambda.Deps, input lambda.OrchestratorInput) (lambda.OrchestratorOutput, error) {
	record, err := d.Store.GetLatestJobEvent(ctx, input.PipelineID, input.ScheduleID, input.Date)
	if err != nil {
		return lambda.OrchestratorOutput{Mode: "check-job", Error: err.Error()}, nil
	}

	if record != nil {
		// Only return terminal events; skip intermediate events like
		// infra-trigger-failure so the StatusChecker can poll actual job status.
		switch record.Event {
		case types.JobEventSuccess, types.JobEventFail, types.JobEventTimeout:
			return lambda.OrchestratorOutput{
				Mode:  "check-job",
				Event: record.Event,
			}, nil
		}
	}

	// No terminal joblog entry — try polling the trigger API directly.
	if d.StatusChecker == nil || len(input.Metadata) == 0 {
		return lambda.OrchestratorOutput{Mode: "check-job"}, nil
	}

	cfg, err := d.Store.GetConfig(ctx, input.PipelineID)
	if err != nil {
		return lambda.OrchestratorOutput{Mode: "check-job", Error: err.Error()}, nil
	}
	if cfg == nil {
		return lambda.OrchestratorOutput{Mode: "check-job"}, nil
	}

	result, err := d.StatusChecker.CheckStatus(ctx, cfg.Job.Type, input.Metadata, nil)
	if err != nil {
		d.Logger.WarnContext(ctx, "status check failed", "error", err, "pipeline", input.PipelineID)
		return lambda.OrchestratorOutput{Mode: "check-job"}, nil
	}

	switch result.State {
	case "succeeded":
		if err := d.Store.WriteJobEvent(ctx, input.PipelineID, input.ScheduleID, input.Date, types.JobEventSuccess, input.RunID, 0, ""); err != nil {
			d.Logger.Warn("failed to write polled job success joblog", "error", err, "pipeline", input.PipelineID, "schedule", input.ScheduleID, "date", input.Date)
		}
		// JOB_COMPLETED is published by the stream-router when the JOB#
		// record arrives via DynamoDB stream (handleJobSuccess). Publishing
		// here as well would cause duplicate alerts for polled jobs.
		return lambda.OrchestratorOutput{Mode: "check-job", Event: "success"}, nil
	case "failed":
		var writeOpts []store.JobEventOption
		if result.FailureCategory != "" {
			writeOpts = append(writeOpts, store.WithFailureCategory(result.FailureCategory))
		}
		if err := d.Store.WriteJobEvent(ctx, input.PipelineID, input.ScheduleID, input.Date, types.JobEventFail, input.RunID, 0, result.Message, writeOpts...); err != nil {
			d.Logger.Warn("failed to write polled job failure joblog", "error", err, "pipeline", input.PipelineID, "schedule", input.ScheduleID, "date", input.Date)
		}
		if err := lambda.PublishEvent(ctx, d, string(types.EventJobFailed), input.PipelineID, input.ScheduleID, input.Date, "job failed: "+result.Message); err != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventJobFailed, "error", err)
		}
		return lambda.OrchestratorOutput{Mode: "check-job", Event: "fail"}, nil
	default:
		// Still running — return no event so SFN loops back to WaitForJob.
		return lambda.OrchestratorOutput{Mode: "check-job"}, nil
	}
}

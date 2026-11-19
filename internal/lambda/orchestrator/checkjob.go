package orchestrator

import (
	"context"

	"github.com/dwsmith1983/interlock/internal/store"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// handleCheckJob reads the newest terminal job log row for this run
// (input.RunID). Rows of earlier attempts for the same date are ignored, so a
// rerun is never decided by the previous attempt's result. If the run has no
// terminal row and a StatusChecker is configured, it polls the trigger API
// directly and writes terminal results (succeeded/failed) to the job log.
func handleCheckJob(ctx context.Context, d *lambda.Deps, input lambda.OrchestratorInput) (lambda.OrchestratorOutput, error) {
	record, err := d.Store.GetJobResult(ctx, input.PipelineID, input.ScheduleID, input.Date, input.RunID)
	if err != nil {
		d.Logger.WarnContext(ctx, "job result lookup failed", "error", err,
			"pipeline", input.PipelineID, "schedule", input.ScheduleID, "date", input.Date, "runId", input.RunID)
		return lambda.OrchestratorOutput{Mode: "check-job", Error: err.Error()}, nil
	}
	if record != nil {
		out := lambda.OrchestratorOutput{Mode: "check-job", Event: record.Event}
		// A success row may carry a message (synchronous jobs write one);
		// it is not an error.
		if record.Event != types.JobEventSuccess {
			out.Error = record.Error
		}
		return out, nil
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
		// record arrives via DynamoDB stream (handleJobLogEvent). Publishing
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
		// JOB_FAILED comes from the stream-router, like JOB_COMPLETED above.
		return lambda.OrchestratorOutput{Mode: "check-job", Event: "fail"}, nil
	default:
		// Still running — return no event so SFN loops back to WaitForJob.
		return lambda.OrchestratorOutput{Mode: "check-job"}, nil
	}
}

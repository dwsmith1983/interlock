package stream

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-lambda-go/events"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// handleJobLogEvent processes a JOB# stream record: success publishes
// JOB_COMPLETED; fail and timeout start nothing, since retries are decided by
// the run's CompleteTrigger.
func handleJobLogEvent(ctx context.Context, d *lambda.Deps, pk, sk string, record events.DynamoDBEventRecord) error {
	pipelineID := strings.TrimPrefix(pk, "PIPELINE#")
	if pipelineID == pk {
		return fmt.Errorf("unexpected PK format: %q", pk)
	}

	// Extract the "event" attribute from NewImage (success/fail/timeout).
	eventAttr, ok := record.Change.NewImage["event"]
	if !ok || eventAttr.DataType() != events.DataTypeString {
		d.Logger.Warn("JOB record missing event attribute", "pk", pk, "sk", sk)
		return nil
	}
	jobEvent := eventAttr.String()

	// Parse schedule and date from SK: JOB#<schedule>#<date>#<timestamp>
	schedule, date, err := parseJobSK(sk)
	if err != nil {
		return err
	}

	switch jobEvent {
	case types.JobEventFail, types.JobEventTimeout:
		return handleJobFailure(ctx, d, pipelineID, schedule, date, jobEvent, record.Change.NewImage)
	case types.JobEventSuccess:
		return handleJobSuccess(ctx, d, pipelineID, schedule, date)
	default:
		d.Logger.Warn("unknown job event", "event", jobEvent, "pipelineId", pipelineID)
		return nil
	}
}

// parseJobSK extracts schedule and date from a JOB# sort key.
// Expected format: JOB#<schedule>#<date>#<timestamp>
func parseJobSK(sk string) (schedule, date string, err error) {
	trimmed := strings.TrimPrefix(sk, "JOB#")
	parts := strings.SplitN(trimmed, "#", 3)
	if len(parts) < 3 {
		return "", "", fmt.Errorf("invalid JOB SK format: %q", sk)
	}
	return parts[0], parts[1], nil
}

// handleJobSuccess publishes a job-completed event to EventBridge.
func handleJobSuccess(ctx context.Context, d *lambda.Deps, pipelineID, schedule, date string) error {
	return lambda.PublishEvent(ctx, d, string(types.EventJobCompleted), pipelineID, schedule, date,
		fmt.Sprintf("job completed for %s", pipelineID))
}

// handleJobFailure handles a fail or timeout row. It starts nothing and
// writes nothing: the run's CompleteTrigger owns the retry, and a row that
// arrives after its execution ended changes nothing. Dry-run pipelines have no
// execution, so the stream reports what CompleteTrigger would decide.
func handleJobFailure(ctx context.Context, d *lambda.Deps, pipelineID, schedule, date, jobEvent string, image map[string]events.DynamoDBAttributeValue) error {
	cfg, err := lambda.GetValidatedConfig(ctx, d, pipelineID)
	if err != nil {
		return fmt.Errorf("load config for %q: %w", pipelineID, err)
	}
	str := func(attr string) string {
		if v, ok := image[attr]; ok && v.DataType() == events.DataTypeString {
			return v.String()
		}
		return ""
	}
	if cfg != nil && cfg.DryRun {
		return handleDryRunJobFailure(ctx, d, cfg, pipelineID, schedule, date, types.FailureCategory(str("category")))
	}

	d.Logger.Info("job failure left to the run's CompleteTrigger",
		"pipelineId", pipelineID, "schedule", schedule, "date", date, "event", jobEvent)
	return nil
}

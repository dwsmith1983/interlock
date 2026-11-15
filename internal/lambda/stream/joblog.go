package stream

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/aws/aws-lambda-go/events"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// maxReportedErrorBytes caps a row's error text in JOB_FAILED, so a job
// cannot push an oversized message into events and Slack.
const maxReportedErrorBytes = 2048

// handleJobLogEvent processes a JOB# stream record: success publishes
// JOB_COMPLETED, fail and timeout publish JOB_FAILED. Retries are decided by
// the run's CompleteTrigger, not here. Only a new row (INSERT) with a valid
// date for a configured pipeline is reported; anything else is logged and
// dropped.
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
	if record.EventName != insertEventName || !lambda.IsValidExecutionDate(date) {
		d.Logger.Info("JOB record not reported: not a new row, or an invalid date", "eventName", record.EventName, "pk", pk, "sk", sk)
		return nil
	}
	cfg, err := lambda.GetValidatedConfig(ctx, d, pipelineID)
	if err != nil {
		return fmt.Errorf("load config for %q: %w", pipelineID, err)
	}
	if cfg == nil {
		d.Logger.Info("JOB record not reported: no config", "pipelineId", pipelineID, "sk", sk)
		return nil
	}

	switch jobEvent {
	case types.JobEventFail, types.JobEventTimeout:
		return handleJobFailure(ctx, d, cfg, pipelineID, schedule, date, jobEvent, record.Change.NewImage)
	case types.JobEventSuccess:
		return reportJob(ctx, d, types.EventJobCompleted, pipelineID, schedule, date, fmt.Sprintf("job completed for %s", pipelineID), nil)
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

// handleJobFailure publishes JOB_FAILED for a fail or timeout row. It starts
// nothing and writes nothing: the run's CompleteTrigger owns the retry, and a
// row that arrives after its execution ended is only reported. Dry-run
// pipelines have no execution, so the stream reports what CompleteTrigger
// would decide.
func handleJobFailure(ctx context.Context, d *lambda.Deps, cfg *types.PipelineConfig, pipelineID, schedule, date, jobEvent string, image map[string]events.DynamoDBAttributeValue) error {
	str := func(attr string) string {
		if v, ok := image[attr]; ok && v.DataType() == events.DataTypeString {
			return v.String()
		}
		return ""
	}
	if cfg.DryRun {
		// The dry-run marker plays the trigger row's part: no marker, no run.
		marker, err := d.Store.GetDryRunMarker(ctx, pipelineID, schedule, date)
		if err != nil {
			return fmt.Errorf("read dry-run marker for %q/%s/%s: %w", pipelineID, schedule, date, err)
		}
		if marker == nil {
			d.Logger.Info("JOB record not reported: no dry-run marker", "pipelineId", pipelineID, "schedule", schedule, "date", date)
			return nil
		}
		return handleDryRunJobFailure(ctx, d, cfg, pipelineID, schedule, date, types.FailureCategory(str("category")))
	}

	errMsg := truncateUTF8(str("error"), maxReportedErrorBytes)
	detail := map[string]interface{}{"event": jobEvent, "error": errMsg, "runId": str("runId")}
	if category := str("category"); category != "" {
		detail["category"] = category
	}
	msg := fmt.Sprintf("job %s for %s", jobEvent, pipelineID)
	if errMsg != "" {
		msg += ": " + errMsg
	}
	return reportJob(ctx, d, types.EventJobFailed, pipelineID, schedule, date, msg, detail)
}

// reportJob publishes a job event only when the schedule and date have a
// trigger row, so a row for a date that never ran is not reported.
func reportJob(ctx context.Context, d *lambda.Deps, eventType types.EventDetailType, pipelineID, schedule, date, msg string, detail map[string]interface{}) error {
	trigger, err := d.Store.GetTrigger(ctx, pipelineID, schedule, date)
	if err != nil {
		return fmt.Errorf("read trigger for %q/%s/%s: %w", pipelineID, schedule, date, err)
	}
	if trigger == nil {
		d.Logger.Info("JOB record not reported: no trigger row", "pipelineId", pipelineID, "schedule", schedule, "date", date)
		return nil
	}
	return lambda.PublishEvent(ctx, d, string(eventType), pipelineID, schedule, date, msg, detail)
}

// truncateUTF8 cuts s to at most n bytes without splitting a rune.
func truncateUTF8(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

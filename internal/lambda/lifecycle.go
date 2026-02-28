package lambda

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/dwsmith1983/interlock/internal/metrics"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// PublishLifecycleEvent publishes a lifecycle event to SNS when a RUNLOG# record
// transitions to COMPLETED or FAILED. Best-effort: errors are logged, not returned.
// No-op when the SNS client or lifecycle topic ARN is not configured.
func PublishLifecycleEvent(ctx context.Context, d *Deps, logger *slog.Logger, pk string, newImage map[string]events.DynamoDBAttributeValue) {
	if d.SNSClient == nil || d.LifecycleTopicARN == "" {
		return
	}

	if newImage == nil {
		return
	}

	// Extract status from NewImage
	statusAttr, ok := newImage["status"]
	if !ok {
		return
	}
	status := statusAttr.String()

	// Only publish for terminal statuses
	if status != string(types.RunCompleted) && status != string(types.RunFailed) {
		return
	}

	pipelineID := strings.TrimPrefix(pk, "PIPELINE#")
	if pipelineID == pk {
		return
	}

	eventType := types.EventPipelineCompleted
	if status == string(types.RunFailed) {
		eventType = types.EventPipelineFailed
	}

	// Extract optional fields from NewImage
	scheduleID := ""
	if attr, ok := newImage["scheduleID"]; ok {
		scheduleID = attr.String()
	}
	date := ""
	if attr, ok := newImage["date"]; ok {
		date = attr.String()
	}
	runID := ""
	if attr, ok := newImage["runId"]; ok {
		runID = attr.String()
	}

	evt := LifecycleEvent{
		EventType:  eventType,
		PipelineID: pipelineID,
		ScheduleID: scheduleID,
		Date:       date,
		RunID:      runID,
		Status:     status,
		Timestamp:  time.Now(),
	}

	payload, err := json.Marshal(evt)
	if err != nil {
		logger.Error("failed to marshal lifecycle event",
			"pipeline", pipelineID, "error", err)
		return
	}

	msg := string(payload)
	_, err = d.SNSClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: &d.LifecycleTopicARN,
		Message:  &msg,
	})
	if err != nil {
		logger.Error("failed to publish lifecycle event",
			"pipeline", pipelineID, "status", status, "error", err)
		return
	}

	metrics.LifecycleEventsPublished.Add(1)
	logger.Info("published lifecycle event",
		"pipeline", pipelineID, "status", status, "eventType", eventType)
}

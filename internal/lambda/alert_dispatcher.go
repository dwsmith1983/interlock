package lambda

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/aws/aws-lambda-go/events"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// HandleAlertDispatcher processes SQS messages containing EventBridge alert events
// and sends Slack notifications.
func HandleAlertDispatcher(ctx context.Context, d *Deps, sqsEvent events.SQSEvent) (events.SQSEventResponse, error) {
	var failures []events.SQSBatchItemFailure

	for _, record := range sqsEvent.Records {
		if err := processAlertMessage(ctx, d, record); err != nil {
			d.Logger.WarnContext(ctx, "failed to process alert message",
				"messageId", record.MessageId, "error", err)
			failures = append(failures, events.SQSBatchItemFailure{
				ItemIdentifier: record.MessageId,
			})
		}
	}

	return events.SQSEventResponse{BatchItemFailures: failures}, nil
}

func processAlertMessage(ctx context.Context, d *Deps, record events.SQSMessage) error {
	var envelope EventBridgeInput
	if err := json.Unmarshal([]byte(record.Body), &envelope); err != nil {
		return fmt.Errorf("unmarshal EventBridge envelope: %w", err)
	}

	var detail types.InterlockEvent
	if err := json.Unmarshal(envelope.Detail, &detail); err != nil {
		return fmt.Errorf("unmarshal event detail: %w", err)
	}

	if d.SlackWebhookURL == "" {
		d.Logger.InfoContext(ctx, "alert (no webhook configured)",
			"eventType", envelope.DetailType, "pipeline", detail.PipelineID,
			"date", detail.Date, "message", detail.Message)
		return nil
	}

	emoji := alertEmoji(envelope.DetailType)
	text := fmt.Sprintf("%s *%s* | %s | %s\n%s",
		emoji, envelope.DetailType, detail.PipelineID, detail.Date, detail.Message)

	payload := map[string]interface{}{
		"blocks": []map[string]interface{}{
			{
				"type": "section",
				"text": map[string]string{
					"type": "mrkdwn",
					"text": text,
				},
			},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal slack payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.SlackWebhookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create slack request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("post to slack: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("slack returned status %d", resp.StatusCode)
	}

	d.Logger.InfoContext(ctx, "alert sent to Slack",
		"eventType", envelope.DetailType, "pipeline", detail.PipelineID, "date", detail.Date)
	return nil
}

func alertEmoji(detailType string) string {
	switch detailType {
	case string(types.EventSLABreach), string(types.EventJobFailed),
		string(types.EventValidationExhausted), string(types.EventRetryExhausted),
		string(types.EventInfraFailure), string(types.EventSFNTimeout),
		string(types.EventScheduleMissed):
		return "\xf0\x9f\x94\xb4" // red circle
	case string(types.EventSLAWarning):
		return "\xf0\x9f\x9f\xa1" // yellow circle
	case string(types.EventSLAMet):
		return "\xe2\x9c\x85" // check mark
	default:
		return "\xe2\x84\xb9\xef\xb8\x8f" // info
	}
}

package lambda

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

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

	if d.SlackBotToken == "" {
		d.Logger.InfoContext(ctx, "alert (no bot token configured)",
			"eventType", envelope.DetailType, "pipeline", detail.PipelineID,
			"date", detail.Date, "message", detail.Message)
		return nil
	}

	// Look up existing thread for this pipeline/schedule/date.
	threadTs := getThreadTs(ctx, d, detail.PipelineID, detail.ScheduleID, detail.Date)

	emoji := alertEmoji(envelope.DetailType)
	text := fmt.Sprintf("%s *%s* | %s | %s\n%s",
		emoji, envelope.DetailType, detail.PipelineID, detail.Date, detail.Message)

	type slackPayload struct {
		Channel  string                   `json:"channel"`
		Blocks   []map[string]interface{} `json:"blocks"`
		ThreadTs string                   `json:"thread_ts,omitempty"`
	}

	payload := slackPayload{
		Channel: d.SlackChannelID,
		Blocks: []map[string]interface{}{
			{
				"type": "section",
				"text": map[string]string{
					"type": "mrkdwn",
					"text": text,
				},
			},
		},
		ThreadTs: threadTs,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal slack payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://slack.com/api/chat.postMessage", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create slack request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+d.SlackBotToken)

	resp, err := d.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("post to slack: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("slack returned status %d", resp.StatusCode)
	}

	var slackResp struct {
		OK      bool   `json:"ok"`
		TS      string `json:"ts"`
		Channel string `json:"channel"`
		Error   string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&slackResp); err != nil {
		return fmt.Errorf("decode slack response: %w", err)
	}
	if !slackResp.OK {
		return fmt.Errorf("slack API error: %s", slackResp.Error)
	}

	// Save thread_ts on first message so subsequent messages thread under it.
	if threadTs == "" {
		saveThreadTs(ctx, d, detail.PipelineID, detail.ScheduleID, detail.Date, slackResp.TS, d.SlackChannelID)
	}

	d.Logger.InfoContext(ctx, "alert sent to Slack",
		"eventType", envelope.DetailType, "pipeline", detail.PipelineID, "date", detail.Date)
	return nil
}

// getThreadTs looks up an existing Slack thread timestamp for a pipeline/schedule/date.
// Returns "" if no thread exists or on error (errors are logged but don't fail the message).
func getThreadTs(ctx context.Context, d *Deps, pipelineID, scheduleID, date string) string {
	result, err := d.Store.Client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &d.Store.EventsTable,
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: fmt.Sprintf("THREAD#%s#%s", scheduleID, date)},
		},
	})
	if err != nil {
		d.Logger.WarnContext(ctx, "thread lookup failed", "error", err)
		return ""
	}
	if ts, ok := result.Item["threadTs"].(*ddbtypes.AttributeValueMemberS); ok {
		return ts.Value
	}
	return ""
}

// saveThreadTs persists a Slack thread timestamp for future message threading.
// Errors are logged but don't fail the message.
func saveThreadTs(ctx context.Context, d *Deps, pipelineID, scheduleID, date, threadTs, channelID string) {
	ttl := time.Now().Add(time.Duration(d.EventsTTLDays) * 24 * time.Hour).Unix()
	_, err := d.Store.Client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &d.Store.EventsTable,
		Item: map[string]ddbtypes.AttributeValue{
			"PK":        &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
			"SK":        &ddbtypes.AttributeValueMemberS{Value: fmt.Sprintf("THREAD#%s#%s", scheduleID, date)},
			"threadTs":  &ddbtypes.AttributeValueMemberS{Value: threadTs},
			"channelId": &ddbtypes.AttributeValueMemberS{Value: channelID},
			"createdAt": &ddbtypes.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339)},
			"ttl":       &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", ttl)},
		},
	})
	if err != nil {
		d.Logger.WarnContext(ctx, "failed to save thread_ts", "error", err)
	}
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

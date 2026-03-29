// Package alert implements the alert-dispatcher Lambda handler.
// It processes SQS messages containing EventBridge alert events
// and sends Slack notifications.
package alert

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

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// HandleAlertDispatcher processes SQS messages containing EventBridge alert events.
func HandleAlertDispatcher(ctx context.Context, d *lambda.Deps, sqsEvent events.SQSEvent) (events.SQSEventResponse, error) {
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

func processAlertMessage(ctx context.Context, d *lambda.Deps, record events.SQSMessage) error {
	var envelope lambda.EventBridgeInput
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

	threadTs := getThreadTs(ctx, d, detail.PipelineID, detail.ScheduleID, detail.Date)

	text := FormatAlertText(envelope.DetailType, detail)

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

	if threadTs == "" {
		saveThreadTs(ctx, d, detail.PipelineID, detail.ScheduleID, detail.Date, slackResp.TS, d.SlackChannelID)
	}

	d.Logger.InfoContext(ctx, "alert sent to Slack",
		"eventType", envelope.DetailType, "pipeline", detail.PipelineID, "date", detail.Date)
	return nil
}

func getThreadTs(ctx context.Context, d *lambda.Deps, pipelineID, scheduleID, date string) string {
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

func saveThreadTs(ctx context.Context, d *lambda.Deps, pipelineID, scheduleID, date, threadTs, channelID string) {
	ttl := d.Now().Add(time.Duration(d.EventsTTLDays) * 24 * time.Hour).Unix()
	_, err := d.Store.Client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &d.Store.EventsTable,
		Item: map[string]ddbtypes.AttributeValue{
			"PK":        &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
			"SK":        &ddbtypes.AttributeValueMemberS{Value: fmt.Sprintf("THREAD#%s#%s", scheduleID, date)},
			"threadTs":  &ddbtypes.AttributeValueMemberS{Value: threadTs},
			"channelId": &ddbtypes.AttributeValueMemberS{Value: channelID},
			"createdAt": &ddbtypes.AttributeValueMemberS{Value: d.Now().UTC().Format(time.RFC3339)},
			"ttl":       &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", ttl)},
		},
	})
	if err != nil {
		d.Logger.WarnContext(ctx, "failed to save thread_ts", "error", err)
	}
}

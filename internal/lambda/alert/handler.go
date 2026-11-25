// Package alert implements the alert-dispatcher Lambda handler.
// It processes SQS messages containing EventBridge alert events
// and sends Slack notifications.
package alert

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// slackPayload is a chat.postMessage request. ReplyBroadcast also shows a
// thread reply in the channel.
type slackPayload struct {
	Channel        string                   `json:"channel"`
	Blocks         []map[string]interface{} `json:"blocks"`
	ThreadTs       string                   `json:"thread_ts,omitempty"`
	ReplyBroadcast bool                     `json:"reply_broadcast,omitempty"`
}

// slackError is a Slack response that refused the message. retryAfter is the
// wait in seconds the response asked for, or 0.
type slackError struct {
	msg        string
	retryAfter int
}

func newSlackError(resp *http.Response, msg string) *slackError {
	return &slackError{msg: msg, retryAfter: parseRetryAfter(resp.Header.Get("Retry-After"))}
}

func (e *slackError) Error() string { return e.msg }

// HandleAlertDispatcher processes SQS messages containing EventBridge alert
// events. A message that fails is reported to SQS, which delivers it again
// after the delay set here.
func HandleAlertDispatcher(ctx context.Context, d *lambda.Deps, sqsEvent events.SQSEvent) (events.SQSEventResponse, error) {
	var failures []events.SQSBatchItemFailure

	for _, record := range sqsEvent.Records {
		if err := processAlertMessage(ctx, d, record); err != nil {
			d.Logger.WarnContext(ctx, "failed to process alert message",
				"messageId", record.MessageId, "error", err)
			failures = append(failures, events.SQSBatchItemFailure{
				ItemIdentifier: record.MessageId,
			})
			delayRetry(ctx, d, record, err)
		}
	}

	return events.SQSEventResponse{BatchItemFailures: failures}, nil
}

// delayRetry hides record, which failed with cause, until its next try. If that
// fails, the message returns after the queue's own visibility timeout.
func delayRetry(ctx context.Context, d *lambda.Deps, record events.SQSMessage, cause error) {
	receiveCount, err := strconv.Atoi(record.Attributes["ApproximateReceiveCount"])
	if err != nil {
		receiveCount = 1
	}
	retryAfter := 0
	var refused *slackError
	if errors.As(cause, &refused) {
		retryAfter = refused.retryAfter
	}
	if retryAfter > maxRetryAfterSeconds {
		d.Logger.WarnContext(ctx, "Slack asked for a longer wait than the limit",
			"retryAfterSeconds", retryAfter, "limitSeconds", maxRetryAfterSeconds)
	}

	_, err = d.SQS.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          &d.AlertQueueURL,
		ReceiptHandle:     &record.ReceiptHandle,
		VisibilityTimeout: retryDelaySeconds(receiveCount, retryAfter, rand.Float64()),
	})
	if err != nil {
		d.Logger.WarnContext(ctx, "could not set the retry delay of an alert message",
			"messageId", record.MessageId, "error", err)
	}
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

	sk := threadSK(d, record, detail)
	threadTs := getThreadTs(ctx, d, detail.PipelineID, sk)

	ts, err := postToSlack(ctx, d, slackPayload{
		Channel: d.SlackChannelID,
		Blocks: []map[string]interface{}{
			{
				"type": "section",
				"text": map[string]string{
					"type": "mrkdwn",
					"text": FormatAlertText(envelope.DetailType, detail),
				},
			},
		},
		ThreadTs: threadTs,
		// A day thread collects unrelated alerts, so its replies also go to
		// the channel.
		ReplyBroadcast: threadTs != "" && detail.Date == "",
	})
	if err != nil {
		return err
	}

	if threadTs == "" {
		saveThreadTs(ctx, d, detail.PipelineID, sk, ts)
	}

	d.Logger.InfoContext(ctx, "alert sent to Slack",
		"eventType", envelope.DetailType, "pipeline", detail.PipelineID, "date", detail.Date)
	return nil
}

// postToSlack sends payload with chat.postMessage and returns the ts of the
// posted message.
func postToSlack(ctx context.Context, d *lambda.Deps, payload slackPayload) (string, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal slack payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://slack.com/api/chat.postMessage", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("create slack request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+d.SlackBotToken)

	resp, err := d.HTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("post to slack: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return "", newSlackError(resp, fmt.Sprintf("slack returned status %d", resp.StatusCode))
	}

	var slackResp struct {
		OK      bool   `json:"ok"`
		TS      string `json:"ts"`
		Channel string `json:"channel"`
		Error   string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&slackResp); err != nil {
		return "", fmt.Errorf("decode slack response: %w", err)
	}
	if !slackResp.OK {
		return "", newSlackError(resp, "slack API error: "+slackResp.Error)
	}
	return slackResp.TS, nil
}

// threadSK returns the sort key of the alert's thread row. Alerts with a run
// date share a thread per schedule and date, the others one per UTC day.
func threadSK(d *lambda.Deps, record events.SQSMessage, detail types.InterlockEvent) string {
	if detail.Date != "" {
		return fmt.Sprintf("THREAD#%s#%s", detail.ScheduleID, detail.Date)
	}
	return "THREADDAY#" + sentAt(d, record).UTC().Format(time.DateOnly)
}

// sentAt returns when SQS received the message, which stays the same when the
// message is delivered again, or the current time when that is not known.
func sentAt(d *lambda.Deps, record events.SQSMessage) time.Time {
	millis, err := strconv.ParseInt(record.Attributes["SentTimestamp"], 10, 64)
	if err != nil {
		return d.Now()
	}
	return time.UnixMilli(millis)
}

// getThreadTs returns the ts of the thread saved under sk, or "" when there is
// none in the configured channel: a ts is only valid in its own channel.
func getThreadTs(ctx context.Context, d *lambda.Deps, pipelineID, sk string) string {
	result, err := d.Store.Client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &d.Store.EventsTable,
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: sk},
		},
	})
	if err != nil {
		d.Logger.WarnContext(ctx, "thread lookup failed", "error", err)
		return ""
	}
	channel, ok := result.Item["channelId"].(*ddbtypes.AttributeValueMemberS)
	if !ok || channel.Value != d.SlackChannelID {
		return ""
	}
	if ts, ok := result.Item["threadTs"].(*ddbtypes.AttributeValueMemberS); ok {
		return ts.Value
	}
	return ""
}

func saveThreadTs(ctx context.Context, d *lambda.Deps, pipelineID, sk, threadTs string) {
	ttl := d.Now().Add(time.Duration(d.EventsTTLDays) * 24 * time.Hour).Unix()
	_, err := d.Store.Client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &d.Store.EventsTable,
		Item: map[string]ddbtypes.AttributeValue{
			"PK":        &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
			"SK":        &ddbtypes.AttributeValueMemberS{Value: sk},
			"threadTs":  &ddbtypes.AttributeValueMemberS{Value: threadTs},
			"channelId": &ddbtypes.AttributeValueMemberS{Value: d.SlackChannelID},
			"createdAt": &ddbtypes.AttributeValueMemberS{Value: d.Now().UTC().Format(time.RFC3339)},
			"ttl":       &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", ttl)},
		},
	})
	if err != nil {
		d.Logger.WarnContext(ctx, "failed to save thread_ts", "error", err)
	}
}

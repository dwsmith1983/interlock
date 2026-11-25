package alert_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dwsmith1983/interlock/internal/lambda"
)

const alertQueueURL = "https://sqs.eu-west-1.amazonaws.com/123456789012/alerts"

// fakeSQS records the visibility changes and fails each with err.
type fakeSQS struct {
	calls []*sqs.ChangeMessageVisibilityInput
	err   error
}

func (f *fakeSQS) ChangeMessageVisibility(_ context.Context, in *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	f.calls = append(f.calls, in)
	return &sqs.ChangeMessageVisibilityOutput{}, f.err
}

// newRetryDeps is newAlertDeps with the SQS fake and the log captured.
func newRetryDeps(c *mockHTTPClient, botToken string) (*lambda.Deps, *fakeSQS, *bytes.Buffer) {
	d, _, _ := newAlertDeps(c, botToken)
	queue, log := &fakeSQS{}, &bytes.Buffer{}
	d.SQS = queue
	d.Logger = slog.New(slog.NewTextHandler(log, nil))
	return d, queue, log
}

// received sets the receipt handle and ApproximateReceiveCount of record. An
// empty count leaves the attribute out.
func received(record events.SQSMessage, count string) events.SQSMessage {
	record.ReceiptHandle = "receipt-" + record.MessageId
	if count != "" {
		record.Attributes = map[string]string{"ApproximateReceiveCount": count}
	}
	return record
}

// TestHandleAlertDispatcher_RetryDelay: a failed message stays a batch failure
// and is hidden for a delay that grows with its receive count, or for the
// Retry-After of Slack's response if that is longer.
func TestHandleAlertDispatcher_RetryDelay(t *testing.T) {
	retryAfter := func(v string) http.Header { return http.Header{"Retry-After": {v}} }
	malformed := events.SQSMessage{MessageId: "msg-bad", Body: "{invalid-json###"}

	tests := []struct {
		name     string
		client   *mockHTTPClient
		record   events.SQSMessage
		min, max int32 // the delay is in [min, max)
		wantLog  string
	}{
		{name: "503 on try 3", client: &mockHTTPClient{status: http.StatusServiceUnavailable},
			record: received(breachRecord(t, "msg-503"), "3"), min: 240, max: 360, wantLog: "slack returned status 503"},
		{name: "429 with Retry-After", client: &mockHTTPClient{status: http.StatusTooManyRequests, header: retryAfter("600")},
			record: received(breachRecord(t, "msg-429"), "1"), min: 600, max: 630, wantLog: "slack returned status 429"},
		{name: "ok:false with Retry-After",
			client: &mockHTTPClient{status: http.StatusOK, respBody: `{"ok":false,"error":"ratelimited"}`, header: retryAfter("600")},
			record: received(breachRecord(t, "msg-limited"), "1"), min: 600, max: 630, wantLog: "slack API error: ratelimited"},
		{name: "Retry-After over the ceiling", client: &mockHTTPClient{status: http.StatusTooManyRequests, header: retryAfter("7200")},
			record: received(breachRecord(t, "msg-long"), "1"), min: 3600, max: 3630, wantLog: "retryAfterSeconds=7200"},
		{name: "transport error", client: &mockHTTPClient{err: errors.New("connection refused")},
			record: received(breachRecord(t, "msg-net"), "2"), min: 120, max: 180, wantLog: "connection refused"},
		{name: "unreadable response", client: &mockHTTPClient{status: http.StatusOK, respBody: "<html>", header: retryAfter("600")},
			record: received(breachRecord(t, "msg-html"), "1"), min: 60, max: 90, wantLog: "decode slack response"},
		{name: "malformed message", client: &mockHTTPClient{},
			record: received(malformed, "2"), min: 120, max: 180, wantLog: "unmarshal EventBridge envelope"},
		{name: "no receive count", client: &mockHTTPClient{status: http.StatusServiceUnavailable},
			record: received(breachRecord(t, "msg-none"), ""), min: 60, max: 90},
		{name: "unreadable receive count", client: &mockHTTPClient{status: http.StatusServiceUnavailable},
			record: received(breachRecord(t, "msg-soon"), "soon"), min: 60, max: 90},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, queue, log := newRetryDeps(tt.client, "xoxb-test-token")

			assert.Equal(t, []string{tt.record.MessageId}, dispatch(t, d, tt.record))
			require.Len(t, queue.calls, 1)
			call := queue.calls[0]
			assert.Equal(t, alertQueueURL, *call.QueueUrl)
			assert.Equal(t, tt.record.ReceiptHandle, *call.ReceiptHandle)
			assert.GreaterOrEqual(t, call.VisibilityTimeout, tt.min)
			assert.Less(t, call.VisibilityTimeout, tt.max)
			assert.Contains(t, log.String(), tt.wantLog)
			assert.NotContains(t, log.String(), "xoxb-test-token")
		})
	}
}

// Messages that failed together must not all come back at the same moment.
func TestHandleAlertDispatcher_RetryDelaysDiffer(t *testing.T) {
	d, queue, _ := newRetryDeps(&mockHTTPClient{status: http.StatusServiceUnavailable}, "xoxb-test-token")
	var records []events.SQSMessage
	for i := range 20 {
		records = append(records, received(breachRecord(t, fmt.Sprint("msg-", i)), "5"))
	}

	assert.Len(t, dispatch(t, d, records...), len(records))
	require.Len(t, queue.calls, len(records))
	delays := map[int32]bool{}
	for _, call := range queue.calls {
		delays[call.VisibilityTimeout] = true
	}
	assert.Greater(t, len(delays), 1, "all delays are the same: %v", delays)
}

func TestHandleAlertDispatcher_NoRetryDelay(t *testing.T) {
	for name, token := range map[string]string{"posted": "xoxb-test-token", "logging only": ""} {
		t.Run(name, func(t *testing.T) {
			d, queue, _ := newRetryDeps(&mockHTTPClient{status: http.StatusOK}, token)

			assert.Empty(t, dispatch(t, d, received(breachRecord(t, "msg-ok"), "1")))
			assert.Empty(t, queue.calls)
		})
	}

	t.Run("only the failed message of two", func(t *testing.T) {
		d, queue, _ := newRetryDeps(&mockHTTPClient{status: http.StatusOK}, "xoxb-test-token")
		bad := received(events.SQSMessage{MessageId: "msg-bad", Body: "{invalid-json###"}, "1")

		assert.Equal(t, []string{"msg-bad"}, dispatch(t, d, received(breachRecord(t, "msg-good"), "1"), bad))
		require.Len(t, queue.calls, 1)
		assert.Equal(t, "receipt-msg-bad", *queue.calls[0].ReceiptHandle)
	})
}

func TestHandleAlertDispatcher_RetryDelayNotSet(t *testing.T) {
	d, queue, log := newRetryDeps(&mockHTTPClient{status: http.StatusServiceUnavailable}, "xoxb-test-token")
	queue.err = errors.New("receipt handle expired")

	assert.Equal(t, []string{"msg-a", "msg-b"}, dispatch(t, d,
		received(breachRecord(t, "msg-a"), "1"), received(breachRecord(t, "msg-b"), "1")))
	assert.Len(t, queue.calls, 2)
	assert.Contains(t, log.String(), `level=WARN msg="could not set the retry delay of an alert message" messageId=msg-a error="receipt handle expired"`)
}

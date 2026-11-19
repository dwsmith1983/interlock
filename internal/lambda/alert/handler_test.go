package alert_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/lambda/alert"
	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// mockHTTPClient records HTTP requests and returns a configurable response.
type mockHTTPClient struct {
	requests []*http.Request
	bodies   [][]byte
	status   int
	respBody string // configurable response body (defaults to Slack Bot API success)
	err      error
}

func (m *mockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	if m.err != nil {
		return nil, m.err
	}
	body, _ := io.ReadAll(req.Body)
	m.requests = append(m.requests, req)
	m.bodies = append(m.bodies, body)

	respBody := m.respBody
	if respBody == "" {
		respBody = `{"ok":true,"ts":"1234.5678","channel":"C123"}`
	}
	return &http.Response{
		StatusCode: m.status,
		Body:       io.NopCloser(strings.NewReader(respBody)),
	}, nil
}

func buildSQSRecord(t *testing.T, messageID, detailType string, detail types.InterlockEvent) events.SQSMessage {
	t.Helper()
	detailJSON, err := json.Marshal(detail)
	if err != nil {
		t.Fatalf("marshal detail: %v", err)
	}
	envelope := lambda.EventBridgeInput{
		Source:     "interlock",
		DetailType: detailType,
		Detail:     detailJSON,
	}
	envelopeJSON, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return events.SQSMessage{
		MessageId: messageID,
		Body:      string(envelopeJSON),
	}
}

const (
	threadPK = "PIPELINE#silver-cdr-hour"
	threadSK = "THREAD#hourly#2026-03-04T10"
)

var alertNow = time.Date(2026, 3, 4, 10, 31, 0, 0, time.UTC)

func newAlertDeps(httpClient *mockHTTPClient, botToken string) (*lambda.Deps, *storetest.FakeDynamo, *storetest.Mem) {
	fake, mem := storetest.NewMem()
	return &lambda.Deps{
		Store:          storetest.NewStore(fake),
		SlackBotToken:  botToken,
		SlackChannelID: "C123456",
		EventsTTLDays:  90,
		HTTPClient:     httpClient,
		NowFunc:        func() time.Time { return alertNow },
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, fake, mem
}

func breachRecord(t *testing.T, messageID string) events.SQSMessage {
	t.Helper()
	return buildSQSRecord(t, messageID, "SLA_BREACH", types.InterlockEvent{
		PipelineID: "silver-cdr-hour", ScheduleID: "hourly", Date: "2026-03-04T10",
		Message: "SLA breach for silver-cdr-hour",
	})
}

func dispatch(t *testing.T, d *lambda.Deps, records ...events.SQSMessage) []string {
	t.Helper()
	resp, err := alert.HandleAlertDispatcher(context.Background(), d, events.SQSEvent{Records: records})
	require.NoError(t, err)
	var ids []string
	for _, f := range resp.BatchItemFailures {
		ids = append(ids, f.ItemIdentifier)
	}
	return ids
}

// slackPayload decodes the i-th request body sent to Slack.
func slackPayload(t *testing.T, c *mockHTTPClient, i int) map[string]interface{} {
	t.Helper()
	var payload map[string]interface{}
	require.NoError(t, json.Unmarshal(c.bodies[i], &payload))
	return payload
}

func TestHandleAlertDispatcher(t *testing.T) {
	t.Run("sends Slack", func(t *testing.T) {
		c := &mockHTTPClient{status: http.StatusOK}
		d, _, _ := newAlertDeps(c, "xoxb-test-token")

		assert.Empty(t, dispatch(t, d, breachRecord(t, "msg-1")))
		require.Len(t, c.requests, 1)
		req := c.requests[0]
		assert.Equal(t, http.MethodPost, req.Method)
		assert.Equal(t, "https://slack.com/api/chat.postMessage", req.URL.String())
		assert.Equal(t, "application/json", req.Header.Get("Content-Type"))
		assert.Equal(t, "Bearer xoxb-test-token", req.Header.Get("Authorization"))

		payload := slackPayload(t, c, 0)
		assert.Equal(t, "C123456", payload["channel"])
		blocks, ok := payload["blocks"].([]interface{})
		require.True(t, ok)
		require.Len(t, blocks, 1)
		assert.Equal(t, map[string]interface{}{
			"type": "section",
			"text": map[string]interface{}{
				"type": "mrkdwn",
				"text": "\xf0\x9f\x94\xb4 *SLA_BREACH* | silver-cdr-hour | 2026-03-04T10\nSLA breach for silver-cdr-hour",
			},
		}, blocks[0])
		assert.NotContains(t, payload, "thread_ts")
	})

	t.Run("no token", func(t *testing.T) {
		c := &mockHTTPClient{status: http.StatusOK}
		d, _, mem := newAlertDeps(c, "")

		assert.Empty(t, dispatch(t, d, breachRecord(t, "msg-2")))
		assert.Empty(t, c.requests)
		assert.Nil(t, mem.Get("events", threadPK, threadSK))
	})

	failures := []struct {
		name         string
		client       *mockHTTPClient
		record       events.SQSMessage
		noRequests   bool
		noThreadRows bool
	}{
		{name: "Slack ok:false", client: &mockHTTPClient{status: http.StatusOK, respBody: `{"ok":false,"error":"channel_not_found"}`},
			record: breachRecord(t, "msg-3"), noThreadRows: true},
		{name: "invalid body", client: &mockHTTPClient{status: http.StatusOK},
			record: events.SQSMessage{MessageId: "msg-bad", Body: "not valid json{{{"}, noRequests: true},
		{name: "invalid detail", client: &mockHTTPClient{status: http.StatusOK},
			record: events.SQSMessage{MessageId: "msg-invalid-detail",
				Body: `{"source":"interlock","detail-type":"SLA_BREACH","detail":"{not valid json"}`}},
		{name: "HTTP error", client: &mockHTTPClient{err: errors.New("connection refused")}, record: breachRecord(t, "msg-net")},
		{name: "non-200", client: &mockHTTPClient{status: http.StatusServiceUnavailable}, record: breachRecord(t, "msg-503")},
	}
	for _, tt := range failures {
		t.Run(tt.name, func(t *testing.T) {
			d, _, mem := newAlertDeps(tt.client, "xoxb-test-token")

			assert.Equal(t, []string{tt.record.MessageId}, dispatch(t, d, tt.record))
			if tt.noRequests {
				assert.Empty(t, tt.client.requests)
			}
			if tt.noThreadRows {
				assert.Nil(t, mem.Get("events", threadPK, threadSK))
			}
		})
	}

	t.Run("two messages", func(t *testing.T) {
		c := &mockHTTPClient{status: http.StatusOK}
		d, _, _ := newAlertDeps(c, "xoxb-test-token")

		assert.Empty(t, dispatch(t, d,
			buildSQSRecord(t, "msg-a", "SLA_WARNING", types.InterlockEvent{PipelineID: "bronze-cdr", ScheduleID: "hourly", Date: "2026-03-04T10", Message: "Warning"}),
			buildSQSRecord(t, "msg-b", "SLA_MET", types.InterlockEvent{PipelineID: "silver-cdr", ScheduleID: "hourly", Date: "2026-03-04T10", Message: "Met"}),
		))
		assert.Len(t, c.requests, 2)
	})

	t.Run("empty input", func(t *testing.T) {
		c := &mockHTTPClient{status: http.StatusOK}
		d, _, _ := newAlertDeps(c, "xoxb-test-token")

		assert.Empty(t, dispatch(t, d))
		assert.Empty(t, c.requests)
	})

	t.Run("partial batch", func(t *testing.T) {
		c := &mockHTTPClient{status: http.StatusOK}
		d, _, _ := newAlertDeps(c, "xoxb-test-token")

		bad := events.SQSMessage{MessageId: "msg-bad", Body: "{invalid-json###"}
		assert.Equal(t, []string{"msg-bad"}, dispatch(t, d, breachRecord(t, "msg-good"), bad))
		assert.Len(t, c.requests, 1)
	})
}

// TestHandleAlertDispatcher_Threading: the first alert for a pipeline,
// schedule and date starts a Slack thread and stores its ts; later alerts
// reply in it. A failed lookup or save never fails the alert.
func TestHandleAlertDispatcher_Threading(t *testing.T) {
	t.Run("first post saves the thread", func(t *testing.T) {
		c := &mockHTTPClient{status: http.StatusOK}
		d, _, mem := newAlertDeps(c, "xoxb-test-token")

		assert.Empty(t, dispatch(t, d, breachRecord(t, "msg-thread-1")))
		assert.Equal(t, map[string]ddbtypes.AttributeValue{
			"PK":        &ddbtypes.AttributeValueMemberS{Value: threadPK},
			"SK":        &ddbtypes.AttributeValueMemberS{Value: threadSK},
			"threadTs":  &ddbtypes.AttributeValueMemberS{Value: "1234.5678"},
			"channelId": &ddbtypes.AttributeValueMemberS{Value: "C123456"},
			"createdAt": &ddbtypes.AttributeValueMemberS{Value: "2026-03-04T10:31:00Z"},
			"ttl":       &ddbtypes.AttributeValueMemberN{Value: "1780396260"},
		}, mem.Get("events", threadPK, threadSK))
	})

	t.Run("later post replies in the thread", func(t *testing.T) {
		c := &mockHTTPClient{status: http.StatusOK}
		d, _, mem := newAlertDeps(c, "xoxb-test-token")
		seeded := map[string]ddbtypes.AttributeValue{
			"PK":        &ddbtypes.AttributeValueMemberS{Value: threadPK},
			"SK":        &ddbtypes.AttributeValueMemberS{Value: threadSK},
			"threadTs":  &ddbtypes.AttributeValueMemberS{Value: "9999.1234"},
			"channelId": &ddbtypes.AttributeValueMemberS{Value: "C123456"},
		}
		mem.Put("events", seeded)

		assert.Empty(t, dispatch(t, d, breachRecord(t, "msg-thread-2")))
		require.Len(t, c.requests, 1)
		assert.Equal(t, "9999.1234", slackPayload(t, c, 0)["thread_ts"])
		assert.Equal(t, seeded, mem.Get("events", threadPK, threadSK))
	})

	t.Run("lookup error posts without a thread", func(t *testing.T) {
		c := &mockHTTPClient{status: http.StatusOK}
		d, fake, _ := newAlertDeps(c, "xoxb-test-token")
		fake.GetItemFn = func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
			return nil, errors.New("simulated DDB error")
		}

		assert.Empty(t, dispatch(t, d, breachRecord(t, "msg-thread-err")))
		require.Len(t, c.requests, 1)
		assert.NotContains(t, slackPayload(t, c, 0), "thread_ts")
	})

	t.Run("save error still succeeds", func(t *testing.T) {
		c := &mockHTTPClient{status: http.StatusOK}
		d, fake, _ := newAlertDeps(c, "xoxb-test-token")
		puts := 0
		fake.PutItemFn = func(context.Context, *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			puts++
			return nil, errors.New("write capacity exceeded")
		}

		assert.Empty(t, dispatch(t, d, breachRecord(t, "msg-save-err")))
		assert.Len(t, c.requests, 1)
		assert.Equal(t, 1, puts)
	})
}

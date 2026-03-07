package lambda_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"

	sqsevents "github.com/aws/aws-lambda-go/events"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/store"
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

func buildSQSRecord(t *testing.T, messageID, detailType string, detail types.InterlockEvent) sqsevents.SQSMessage {
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
	return sqsevents.SQSMessage{
		MessageId: messageID,
		Body:      string(envelopeJSON),
	}
}

func newAlertDeps(httpClient *mockHTTPClient, botToken, channelID string) *lambda.Deps {
	return &lambda.Deps{
		Store: &store.Store{
			Client:      newMockDDB(),
			EventsTable: "events",
		},
		SlackBotToken:  botToken,
		SlackChannelID: channelID,
		EventsTTLDays:  90,
		HTTPClient:     httpClient,
		Logger:         slog.New(slog.NewJSONHandler(os.Stdout, nil)),
	}
}

func TestHandleAlertDispatcher_SendsSlack(t *testing.T) {
	httpClient := &mockHTTPClient{status: 200}
	deps := newAlertDeps(httpClient, "xoxb-test-token", "C123456")

	detail := types.InterlockEvent{
		PipelineID: "silver-cdr-hour",
		ScheduleID: "hourly",
		Date:       "2026-03-04T10",
		Message:    "SLA breach for silver-cdr-hour",
		Timestamp:  time.Date(2026, 3, 4, 10, 30, 0, 0, time.UTC),
	}

	record := buildSQSRecord(t, "msg-1", "SLA_BREACH", detail)
	sqsEvent := sqsevents.SQSEvent{Records: []sqsevents.SQSMessage{record}}

	resp, err := lambda.HandleAlertDispatcher(context.Background(), deps, sqsEvent)
	if err != nil {
		t.Fatalf("HandleAlertDispatcher returned error: %v", err)
	}
	if len(resp.BatchItemFailures) != 0 {
		t.Errorf("expected 0 failures, got %d", len(resp.BatchItemFailures))
	}

	// Verify HTTP call was made to Slack Bot API.
	if len(httpClient.requests) != 1 {
		t.Fatalf("expected 1 HTTP request, got %d", len(httpClient.requests))
	}
	req := httpClient.requests[0]
	if req.URL.String() != "https://slack.com/api/chat.postMessage" {
		t.Errorf("URL = %q, want %q", req.URL.String(), "https://slack.com/api/chat.postMessage")
	}
	if req.Header.Get("Content-Type") != "application/json" {
		t.Errorf("Content-Type = %q, want %q", req.Header.Get("Content-Type"), "application/json")
	}
	if req.Header.Get("Authorization") != "Bearer xoxb-test-token" {
		t.Errorf("Authorization = %q, want %q", req.Header.Get("Authorization"), "Bearer xoxb-test-token")
	}

	// Verify payload contains expected fields.
	var payload map[string]interface{}
	if err := json.Unmarshal(httpClient.bodies[0], &payload); err != nil {
		t.Fatalf("unmarshal slack payload: %v", err)
	}

	// Check channel field.
	if ch, ok := payload["channel"].(string); !ok || ch != "C123456" {
		t.Errorf("payload channel = %v, want %q", payload["channel"], "C123456")
	}

	blocks, ok := payload["blocks"].([]interface{})
	if !ok || len(blocks) != 1 {
		t.Fatalf("expected 1 block, got %v", payload["blocks"])
	}
	block := blocks[0].(map[string]interface{})
	textObj := block["text"].(map[string]interface{})
	text := textObj["text"].(string)

	if !strings.Contains(text, "SLA_BREACH") {
		t.Errorf("payload text missing SLA_BREACH: %s", text)
	}
	if !strings.Contains(text, "silver-cdr-hour") {
		t.Errorf("payload text missing pipeline ID: %s", text)
	}
	if !strings.Contains(text, "2026-03-04T10") {
		t.Errorf("payload text missing date: %s", text)
	}
	if !strings.Contains(text, "SLA breach for silver-cdr-hour") {
		t.Errorf("payload text missing message: %s", text)
	}
}

func TestHandleAlertDispatcher_NoBotToken(t *testing.T) {
	httpClient := &mockHTTPClient{status: 200}
	deps := newAlertDeps(httpClient, "", "") // no bot token

	detail := types.InterlockEvent{
		PipelineID: "bronze-cdr",
		ScheduleID: "hourly",
		Date:       "2026-03-04T09",
		Message:    "SLA warning",
		Timestamp:  time.Now(),
	}

	record := buildSQSRecord(t, "msg-2", "SLA_WARNING", detail)
	sqsEvent := sqsevents.SQSEvent{Records: []sqsevents.SQSMessage{record}}

	resp, err := lambda.HandleAlertDispatcher(context.Background(), deps, sqsEvent)
	if err != nil {
		t.Fatalf("HandleAlertDispatcher returned error: %v", err)
	}
	if len(resp.BatchItemFailures) != 0 {
		t.Errorf("expected 0 failures, got %d", len(resp.BatchItemFailures))
	}
	if len(httpClient.requests) != 0 {
		t.Errorf("expected no HTTP requests in log-only mode, got %d", len(httpClient.requests))
	}
}

func TestHandleAlertDispatcher_SlackAPIError(t *testing.T) {
	// Slack Bot API returns HTTP 200 with {"ok":false,"error":"..."} on API errors.
	httpClient := &mockHTTPClient{
		status:   200,
		respBody: `{"ok":false,"error":"channel_not_found"}`,
	}
	deps := newAlertDeps(httpClient, "xoxb-test-token", "C123456")

	detail := types.InterlockEvent{
		PipelineID: "gold-report",
		ScheduleID: "daily",
		Date:       "2026-03-04",
		Message:    "Job failed",
		Timestamp:  time.Now(),
	}

	record := buildSQSRecord(t, "msg-3", "JOB_FAILED", detail)
	sqsEvent := sqsevents.SQSEvent{Records: []sqsevents.SQSMessage{record}}

	resp, err := lambda.HandleAlertDispatcher(context.Background(), deps, sqsEvent)
	if err != nil {
		t.Fatalf("HandleAlertDispatcher returned error: %v", err)
	}
	if len(resp.BatchItemFailures) != 1 {
		t.Fatalf("expected 1 batch failure, got %d", len(resp.BatchItemFailures))
	}
	if resp.BatchItemFailures[0].ItemIdentifier != "msg-3" {
		t.Errorf("failure ID = %q, want %q", resp.BatchItemFailures[0].ItemIdentifier, "msg-3")
	}
}

func TestHandleAlertDispatcher_InvalidBody(t *testing.T) {
	httpClient := &mockHTTPClient{status: 200}
	deps := newAlertDeps(httpClient, "xoxb-test-token", "C123456")

	sqsEvent := sqsevents.SQSEvent{
		Records: []sqsevents.SQSMessage{
			{
				MessageId: "msg-bad",
				Body:      "not valid json{{{",
			},
		},
	}

	resp, err := lambda.HandleAlertDispatcher(context.Background(), deps, sqsEvent)
	if err != nil {
		t.Fatalf("HandleAlertDispatcher returned error: %v", err)
	}
	if len(resp.BatchItemFailures) != 1 {
		t.Fatalf("expected 1 batch failure, got %d", len(resp.BatchItemFailures))
	}
	if resp.BatchItemFailures[0].ItemIdentifier != "msg-bad" {
		t.Errorf("failure ID = %q, want %q", resp.BatchItemFailures[0].ItemIdentifier, "msg-bad")
	}
	if len(httpClient.requests) != 0 {
		t.Errorf("expected no HTTP requests for invalid body, got %d", len(httpClient.requests))
	}
}

func TestHandleAlertDispatcher_HTTPError(t *testing.T) {
	httpClient := &mockHTTPClient{err: errors.New("connection refused")}
	deps := newAlertDeps(httpClient, "xoxb-test-token", "C123456")

	detail := types.InterlockEvent{
		PipelineID: "bronze-cdr",
		ScheduleID: "hourly",
		Date:       "2026-03-04T10",
		Message:    "Schedule missed",
		Timestamp:  time.Now(),
	}

	record := buildSQSRecord(t, "msg-net", "SCHEDULE_MISSED", detail)
	sqsEvent := sqsevents.SQSEvent{Records: []sqsevents.SQSMessage{record}}

	resp, err := lambda.HandleAlertDispatcher(context.Background(), deps, sqsEvent)
	if err != nil {
		t.Fatalf("HandleAlertDispatcher returned error: %v", err)
	}
	if len(resp.BatchItemFailures) != 1 {
		t.Fatalf("expected 1 batch failure, got %d", len(resp.BatchItemFailures))
	}
	if resp.BatchItemFailures[0].ItemIdentifier != "msg-net" {
		t.Errorf("failure ID = %q, want %q", resp.BatchItemFailures[0].ItemIdentifier, "msg-net")
	}
}

func TestHandleAlertDispatcher_MultipleMessages(t *testing.T) {
	httpClient := &mockHTTPClient{status: 200}
	deps := newAlertDeps(httpClient, "xoxb-test-token", "C123456")

	records := []sqsevents.SQSMessage{
		buildSQSRecord(t, "msg-a", "SLA_WARNING", types.InterlockEvent{
			PipelineID: "bronze-cdr", ScheduleID: "hourly", Date: "2026-03-04T10", Message: "Warning", Timestamp: time.Now(),
		}),
		buildSQSRecord(t, "msg-b", "SLA_MET", types.InterlockEvent{
			PipelineID: "silver-cdr", ScheduleID: "hourly", Date: "2026-03-04T10", Message: "Met", Timestamp: time.Now(),
		}),
	}

	sqsEvent := sqsevents.SQSEvent{Records: records}

	resp, err := lambda.HandleAlertDispatcher(context.Background(), deps, sqsEvent)
	if err != nil {
		t.Fatalf("HandleAlertDispatcher returned error: %v", err)
	}
	if len(resp.BatchItemFailures) != 0 {
		t.Errorf("expected 0 failures, got %d", len(resp.BatchItemFailures))
	}
	if len(httpClient.requests) != 2 {
		t.Errorf("expected 2 HTTP requests, got %d", len(httpClient.requests))
	}
}

// ---------------------------------------------------------------------------
// Threading tests
// ---------------------------------------------------------------------------

func TestHandleAlertDispatcher_Threading_FirstMessage(t *testing.T) {
	httpClient := &mockHTTPClient{status: 200}
	mock := newMockDDB()
	deps := &lambda.Deps{
		Store: &store.Store{
			Client:      mock,
			EventsTable: "events",
		},
		SlackBotToken:  "xoxb-test-token",
		SlackChannelID: "C123456",
		EventsTTLDays:  90,
		HTTPClient:     httpClient,
		Logger:         slog.New(slog.NewJSONHandler(os.Stdout, nil)),
	}

	detail := types.InterlockEvent{
		PipelineID: "silver-cdr-hour",
		ScheduleID: "hourly",
		Date:       "2026-03-04T10",
		Message:    "SLA breach for silver-cdr-hour",
		Timestamp:  time.Date(2026, 3, 4, 10, 30, 0, 0, time.UTC),
	}

	record := buildSQSRecord(t, "msg-thread-1", "SLA_BREACH", detail)
	sqsEvent := sqsevents.SQSEvent{Records: []sqsevents.SQSMessage{record}}

	resp, err := lambda.HandleAlertDispatcher(context.Background(), deps, sqsEvent)
	if err != nil {
		t.Fatalf("HandleAlertDispatcher returned error: %v", err)
	}
	if len(resp.BatchItemFailures) != 0 {
		t.Errorf("expected 0 failures, got %d", len(resp.BatchItemFailures))
	}

	// First message should NOT include thread_ts in the request body.
	if len(httpClient.bodies) != 1 {
		t.Fatalf("expected 1 request body, got %d", len(httpClient.bodies))
	}
	var payload map[string]interface{}
	if err := json.Unmarshal(httpClient.bodies[0], &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if _, hasThread := payload["thread_ts"]; hasThread {
		t.Error("first message should not include thread_ts")
	}

	// After posting, the thread_ts from the Slack response should be saved to DDB.
	threadPK := "PIPELINE#silver-cdr-hour"
	threadSK := "THREAD#hourly#2026-03-04T10"
	getOut, err := mock.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: strPtr("events"),
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: threadPK},
			"SK": &ddbtypes.AttributeValueMemberS{Value: threadSK},
		},
	})
	if err != nil {
		t.Fatalf("GetItem thread record: %v", err)
	}
	if getOut.Item == nil {
		t.Fatal("expected thread record in DDB, got nil")
	}

	ts, ok := getOut.Item["threadTs"].(*ddbtypes.AttributeValueMemberS)
	if !ok || ts.Value != "1234.5678" {
		t.Errorf("threadTs = %v, want %q", getOut.Item["threadTs"], "1234.5678")
	}
	ch, ok := getOut.Item["channelId"].(*ddbtypes.AttributeValueMemberS)
	if !ok || ch.Value != "C123456" {
		t.Errorf("channelId = %v, want %q", getOut.Item["channelId"], "C123456")
	}
}

func TestHandleAlertDispatcher_Threading_SubsequentMessage(t *testing.T) {
	httpClient := &mockHTTPClient{status: 200}
	mock := newMockDDB()

	// Pre-seed a thread record for this pipeline/schedule/date.
	mock.putRaw("events", map[string]ddbtypes.AttributeValue{
		"PK":        &ddbtypes.AttributeValueMemberS{Value: "PIPELINE#silver-cdr-hour"},
		"SK":        &ddbtypes.AttributeValueMemberS{Value: "THREAD#hourly#2026-03-04T10"},
		"threadTs":  &ddbtypes.AttributeValueMemberS{Value: "9999.1234"},
		"channelId": &ddbtypes.AttributeValueMemberS{Value: "C123456"},
	})

	deps := &lambda.Deps{
		Store: &store.Store{
			Client:      mock,
			EventsTable: "events",
		},
		SlackBotToken:  "xoxb-test-token",
		SlackChannelID: "C123456",
		EventsTTLDays:  90,
		HTTPClient:     httpClient,
		Logger:         slog.New(slog.NewJSONHandler(os.Stdout, nil)),
	}

	detail := types.InterlockEvent{
		PipelineID: "silver-cdr-hour",
		ScheduleID: "hourly",
		Date:       "2026-03-04T10",
		Message:    "SLA met for silver-cdr-hour",
		Timestamp:  time.Date(2026, 3, 4, 10, 45, 0, 0, time.UTC),
	}

	record := buildSQSRecord(t, "msg-thread-2", "SLA_MET", detail)
	sqsEvent := sqsevents.SQSEvent{Records: []sqsevents.SQSMessage{record}}

	resp, err := lambda.HandleAlertDispatcher(context.Background(), deps, sqsEvent)
	if err != nil {
		t.Fatalf("HandleAlertDispatcher returned error: %v", err)
	}
	if len(resp.BatchItemFailures) != 0 {
		t.Errorf("expected 0 failures, got %d", len(resp.BatchItemFailures))
	}

	// Subsequent message should include thread_ts from the pre-seeded record.
	if len(httpClient.bodies) != 1 {
		t.Fatalf("expected 1 request body, got %d", len(httpClient.bodies))
	}
	var payload map[string]interface{}
	if err := json.Unmarshal(httpClient.bodies[0], &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	threadTS, ok := payload["thread_ts"].(string)
	if !ok {
		t.Fatal("expected thread_ts in payload for subsequent message")
	}
	if threadTS != "9999.1234" {
		t.Errorf("thread_ts = %q, want %q", threadTS, "9999.1234")
	}
}

// errorDDB wraps mockDDB but returns an error on GetItem calls.
type errorDDB struct {
	*mockDDB
}

func (e *errorDDB) GetItem(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	return nil, errors.New("simulated DDB error")
}

func TestHandleAlertDispatcher_ThreadLookupFailsGracefully(t *testing.T) {
	httpClient := &mockHTTPClient{status: 200}
	errMock := &errorDDB{mockDDB: newMockDDB()}

	deps := &lambda.Deps{
		Store: &store.Store{
			Client:      errMock,
			EventsTable: "events",
		},
		SlackBotToken:  "xoxb-test-token",
		SlackChannelID: "C123456",
		EventsTTLDays:  90,
		HTTPClient:     httpClient,
		Logger:         slog.New(slog.NewJSONHandler(os.Stdout, nil)),
	}

	detail := types.InterlockEvent{
		PipelineID: "silver-cdr-hour",
		ScheduleID: "hourly",
		Date:       "2026-03-04T10",
		Message:    "SLA warning for silver-cdr-hour",
		Timestamp:  time.Now(),
	}

	record := buildSQSRecord(t, "msg-thread-err", "SLA_WARNING", detail)
	sqsEvent := sqsevents.SQSEvent{Records: []sqsevents.SQSMessage{record}}

	resp, err := lambda.HandleAlertDispatcher(context.Background(), deps, sqsEvent)
	if err != nil {
		t.Fatalf("HandleAlertDispatcher returned error: %v", err)
	}

	// Thread lookup failure should NOT cause the message to fail — post without thread_ts.
	if len(resp.BatchItemFailures) != 0 {
		t.Errorf("expected 0 batch failures (graceful degradation), got %d", len(resp.BatchItemFailures))
	}

	// Slack should still have been called.
	if len(httpClient.requests) != 1 {
		t.Fatalf("expected 1 HTTP request despite DDB error, got %d", len(httpClient.requests))
	}

	// Verify no thread_ts was included (since lookup failed).
	var payload map[string]interface{}
	if err := json.Unmarshal(httpClient.bodies[0], &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if _, hasThread := payload["thread_ts"]; hasThread {
		t.Error("should not include thread_ts when DDB lookup fails")
	}
}

func TestFormatAlertText_WithDetail(t *testing.T) {
	detail := types.InterlockEvent{
		PipelineID: "bronze-cdr",
		Date:       "2026-03-06T16",
		Message:    "pipeline bronze-cdr: SLA_WARNING",
		Detail: map[string]interface{}{
			"deadline":   ":15",
			"breachAt":   "2026-03-06T17:15:00Z",
			"status":     "not started",
			"source":     "watchdog",
			"actionHint": "pipeline not started — check sensor data",
		},
	}
	text := lambda.FormatAlertText("SLA_WARNING", detail)
	assert.Contains(t, text, "SLA_WARNING")
	assert.Contains(t, text, "bronze-cdr")
	assert.Contains(t, text, "not started")
	assert.Contains(t, text, "watchdog")
	assert.Contains(t, text, "check sensor data")
	assert.Contains(t, text, "Deadline")
	// Should NOT contain the raw message (detail overrides it).
	assert.NotContains(t, text, "pipeline bronze-cdr: SLA_WARNING")
}

func TestFormatAlertText_NoDetail(t *testing.T) {
	detail := types.InterlockEvent{
		PipelineID: "test-pipe",
		Date:       "2026-03-06",
		Message:    "pipeline test-pipe: SLA_MET",
	}
	text := lambda.FormatAlertText("SLA_MET", detail)
	assert.Contains(t, text, "SLA_MET")
	assert.Contains(t, text, "test-pipe")
	assert.Contains(t, text, "pipeline test-pipe: SLA_MET")
}

func TestAlertEmoji_DataDrift(t *testing.T) {
	// alertEmoji is unexported; verify DATA_DRIFT gets red circle via FormatAlertText.
	detail := types.InterlockEvent{
		PipelineID: "drift-pipe",
		Date:       "2026-03-06",
		Message:    "drift detected",
	}
	text := lambda.FormatAlertText("DATA_DRIFT", detail)
	assert.Contains(t, text, "\xf0\x9f\x94\xb4") // red circle emoji
}

// ---------------------------------------------------------------------------
// AlertEmoji: all event types
// ---------------------------------------------------------------------------

func TestAlertEmoji_AllEventTypes(t *testing.T) {
	redCircle := "\xf0\x9f\x94\xb4"
	yellowCircle := "\xf0\x9f\x9f\xa1"
	checkMark := "\xe2\x9c\x85"
	info := "\xe2\x84\xb9\xef\xb8\x8f"

	tests := []struct {
		eventType string
		emoji     string
	}{
		{"SLA_BREACH", redCircle},
		{"JOB_FAILED", redCircle},
		{"VALIDATION_EXHAUSTED", redCircle},
		{"RETRY_EXHAUSTED", redCircle},
		{"INFRA_FAILURE", redCircle},
		{"SFN_TIMEOUT", redCircle},
		{"SCHEDULE_MISSED", redCircle},
		{"DATA_DRIFT", redCircle},
		{"SLA_WARNING", yellowCircle},
		{"SLA_MET", checkMark},
		{"UNKNOWN_TYPE", info},
		{"JOB_TRIGGERED", info},
		{"JOB_COMPLETED", info},
		{"TRIGGER_RECOVERED", info},
	}

	for _, tc := range tests {
		t.Run(tc.eventType, func(t *testing.T) {
			detail := types.InterlockEvent{
				PipelineID: "test-pipe",
				Date:       "2026-03-06",
				Message:    "test",
			}
			text := lambda.FormatAlertText(tc.eventType, detail)
			assert.Contains(t, text, tc.emoji, "wrong emoji for %s", tc.eventType)
		})
	}
}

// ---------------------------------------------------------------------------
// FormatAlertText: detail field combinations
// ---------------------------------------------------------------------------

func TestFormatAlertText_DeadlineOnly(t *testing.T) {
	detail := types.InterlockEvent{
		PipelineID: "bronze-cdr",
		Date:       "2026-03-06",
		Message:    "test",
		Detail: map[string]interface{}{
			"deadline": ":30",
		},
	}
	text := lambda.FormatAlertText("SLA_WARNING", detail)
	assert.Contains(t, text, "Deadline :30")
	// Should NOT contain the "(breachAt)" format since breachAt is absent.
	assert.NotContains(t, text, "(")
}

func TestFormatAlertText_DeadlineAndBreachAt(t *testing.T) {
	detail := types.InterlockEvent{
		PipelineID: "bronze-cdr",
		Date:       "2026-03-06",
		Message:    "test",
		Detail: map[string]interface{}{
			"deadline": ":30",
			"breachAt": "2026-03-06T17:30:00Z",
		},
	}
	text := lambda.FormatAlertText("SLA_WARNING", detail)
	assert.Contains(t, text, "Deadline :30")
	assert.Contains(t, text, "2026-03-06T17:30:00Z")
}

func TestFormatAlertText_StatusOnly(t *testing.T) {
	detail := types.InterlockEvent{
		PipelineID: "gold-orders",
		Date:       "2026-03-06",
		Message:    "test",
		Detail: map[string]interface{}{
			"status": "RUNNING",
		},
	}
	text := lambda.FormatAlertText("SLA_BREACH", detail)
	assert.Contains(t, text, "Status: RUNNING")
}

func TestFormatAlertText_CronField(t *testing.T) {
	detail := types.InterlockEvent{
		PipelineID: "bronze-ingest",
		Date:       "2026-03-06",
		Message:    "test",
		Detail: map[string]interface{}{
			"cron": "0 6 * * *",
		},
	}
	text := lambda.FormatAlertText("SCHEDULE_MISSED", detail)
	assert.Contains(t, text, "Cron: 0 6 * * *")
}

func TestFormatAlertText_ActionHintOnly(t *testing.T) {
	detail := types.InterlockEvent{
		PipelineID: "bronze-cdr",
		Date:       "2026-03-06",
		Message:    "test",
		Detail: map[string]interface{}{
			"actionHint": "check sensor data",
		},
	}
	text := lambda.FormatAlertText("SLA_WARNING", detail)
	assert.Contains(t, text, "check sensor data")
	// actionHint uses the arrow prefix.
	assert.Contains(t, text, "\u2192 check sensor data")
}

func TestFormatAlertText_SourceField(t *testing.T) {
	detail := types.InterlockEvent{
		PipelineID: "test-pipe",
		Date:       "2026-03-06",
		Message:    "test",
		Detail: map[string]interface{}{
			"source": "reconciliation",
		},
	}
	text := lambda.FormatAlertText("SLA_BREACH", detail)
	assert.Contains(t, text, "Source: reconciliation")
}

func TestFormatAlertText_AllFieldsCombined(t *testing.T) {
	detail := types.InterlockEvent{
		PipelineID: "bronze-cdr",
		Date:       "2026-03-06T16",
		Message:    "pipeline bronze-cdr: SLA_BREACH",
		Detail: map[string]interface{}{
			"deadline":   ":15",
			"breachAt":   "2026-03-06T17:15:00Z",
			"status":     "not started",
			"source":     "schedule",
			"cron":       "5 * * * *",
			"actionHint": "pipeline not started — investigate trigger",
		},
	}
	text := lambda.FormatAlertText("SLA_BREACH", detail)
	assert.Contains(t, text, "SLA_BREACH")
	assert.Contains(t, text, "bronze-cdr")
	assert.Contains(t, text, "Deadline :15")
	assert.Contains(t, text, "2026-03-06T17:15:00Z")
	assert.Contains(t, text, "Status: not started")
	assert.Contains(t, text, "Source: schedule")
	assert.Contains(t, text, "Cron: 5 * * * *")
	assert.Contains(t, text, "investigate trigger")
}

// ---------------------------------------------------------------------------
// HandleAlertDispatcher: invalid detail JSON
// ---------------------------------------------------------------------------

func TestHandleAlertDispatcher_InvalidDetailJSON(t *testing.T) {
	httpClient := &mockHTTPClient{status: 200}
	deps := newAlertDeps(httpClient, "xoxb-test-token", "C123456")

	// Valid envelope structure, but the detail field is invalid JSON.
	// We construct the body string directly since json.Marshal would reject it.
	body := `{"source":"interlock","detail-type":"SLA_BREACH","detail":"{not valid json"}`

	sqsEvent := sqsevents.SQSEvent{
		Records: []sqsevents.SQSMessage{
			{MessageId: "msg-invalid-detail", Body: body},
		},
	}

	resp, err := lambda.HandleAlertDispatcher(context.Background(), deps, sqsEvent)
	assert.NoError(t, err)
	assert.Len(t, resp.BatchItemFailures, 1)
	assert.Equal(t, "msg-invalid-detail", resp.BatchItemFailures[0].ItemIdentifier)
}

// ---------------------------------------------------------------------------
// HandleAlertDispatcher: empty records
// ---------------------------------------------------------------------------

func TestHandleAlertDispatcher_EmptyRecords(t *testing.T) {
	httpClient := &mockHTTPClient{status: 200}
	deps := newAlertDeps(httpClient, "xoxb-test-token", "C123456")

	sqsEvent := sqsevents.SQSEvent{Records: []sqsevents.SQSMessage{}}

	resp, err := lambda.HandleAlertDispatcher(context.Background(), deps, sqsEvent)
	assert.NoError(t, err)
	assert.Empty(t, resp.BatchItemFailures)
	assert.Empty(t, httpClient.requests)
}

// ---------------------------------------------------------------------------
// HandleAlertDispatcher: partial failure
// ---------------------------------------------------------------------------

func TestHandleAlertDispatcher_PartialFailure(t *testing.T) {
	// First call succeeds, second fails with Slack API error.
	callCount := 0
	httpClient := &mockHTTPClient{status: 200}

	deps := newAlertDeps(httpClient, "xoxb-test-token", "C123456")

	goodRecord := buildSQSRecord(t, "msg-good", "SLA_MET", types.InterlockEvent{
		PipelineID: "bronze-cdr", ScheduleID: "hourly", Date: "2026-03-04T10",
		Message: "Met", Timestamp: time.Now(),
	})

	// For the bad record, we'll use a separate SQS event since we can't
	// make the mock fail selectively. Instead, use invalid body for second message.
	badRecord := sqsevents.SQSMessage{
		MessageId: "msg-bad",
		Body:      "{invalid-json###",
	}

	sqsEvent := sqsevents.SQSEvent{Records: []sqsevents.SQSMessage{goodRecord, badRecord}}

	resp, err := lambda.HandleAlertDispatcher(context.Background(), deps, sqsEvent)
	assert.NoError(t, err)
	assert.Len(t, resp.BatchItemFailures, 1)
	assert.Equal(t, "msg-bad", resp.BatchItemFailures[0].ItemIdentifier)
	// Good message should have been processed.
	assert.Len(t, httpClient.requests, 1)
	_ = callCount
}

// ---------------------------------------------------------------------------
// HandleAlertDispatcher: non-200 HTTP status
// ---------------------------------------------------------------------------

func TestHandleAlertDispatcher_Non200Status(t *testing.T) {
	httpClient := &mockHTTPClient{status: 503}
	deps := newAlertDeps(httpClient, "xoxb-test-token", "C123456")

	detail := types.InterlockEvent{
		PipelineID: "bronze-cdr",
		ScheduleID: "hourly",
		Date:       "2026-03-04T10",
		Message:    "SLA breach",
		Timestamp:  time.Now(),
	}

	record := buildSQSRecord(t, "msg-503", "SLA_BREACH", detail)
	sqsEvent := sqsevents.SQSEvent{Records: []sqsevents.SQSMessage{record}}

	resp, err := lambda.HandleAlertDispatcher(context.Background(), deps, sqsEvent)
	assert.NoError(t, err)
	assert.Len(t, resp.BatchItemFailures, 1)
	assert.Equal(t, "msg-503", resp.BatchItemFailures[0].ItemIdentifier)
}

// ---------------------------------------------------------------------------
// Threading: getThreadTs with nil/empty item
// ---------------------------------------------------------------------------

func TestHandleAlertDispatcher_ThreadLookup_NoItem(t *testing.T) {
	httpClient := &mockHTTPClient{status: 200}
	mock := newMockDDB()

	deps := &lambda.Deps{
		Store: &store.Store{
			Client:      mock,
			EventsTable: "events",
		},
		SlackBotToken:  "xoxb-test-token",
		SlackChannelID: "C123456",
		EventsTTLDays:  90,
		HTTPClient:     httpClient,
		Logger:         slog.New(slog.NewJSONHandler(os.Stdout, nil)),
	}

	// No thread record in DDB — getThreadTs should return "".
	detail := types.InterlockEvent{
		PipelineID: "new-pipeline",
		ScheduleID: "daily",
		Date:       "2026-03-07",
		Message:    "first alert",
		Timestamp:  time.Now(),
	}

	record := buildSQSRecord(t, "msg-no-thread", "SLA_BREACH", detail)
	sqsEvent := sqsevents.SQSEvent{Records: []sqsevents.SQSMessage{record}}

	resp, err := lambda.HandleAlertDispatcher(context.Background(), deps, sqsEvent)
	assert.NoError(t, err)
	assert.Empty(t, resp.BatchItemFailures)

	// Should have posted without thread_ts.
	var payload map[string]interface{}
	assert.NoError(t, json.Unmarshal(httpClient.bodies[0], &payload))
	_, hasThread := payload["thread_ts"]
	assert.False(t, hasThread, "should not have thread_ts when no thread record exists")

	// Thread should now be saved.
	getOut, err := mock.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: strPtr("events"),
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: "PIPELINE#new-pipeline"},
			"SK": &ddbtypes.AttributeValueMemberS{Value: "THREAD#daily#2026-03-07"},
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, getOut.Item, "thread should be saved after first message")
}

// ---------------------------------------------------------------------------
// Threading: saveThreadTs error (graceful degradation)
// ---------------------------------------------------------------------------

// putErrorDDB wraps mockDDB but returns an error on PutItem calls.
type putErrorDDB struct {
	*mockDDB
	putErr error
}

func (e *putErrorDDB) PutItem(_ context.Context, _ *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	return nil, e.putErr
}

func TestHandleAlertDispatcher_SaveThreadError_Graceful(t *testing.T) {
	httpClient := &mockHTTPClient{status: 200}
	errMock := &putErrorDDB{
		mockDDB: newMockDDB(),
		putErr:  errors.New("write capacity exceeded"),
	}

	deps := &lambda.Deps{
		Store: &store.Store{
			Client:      errMock,
			EventsTable: "events",
		},
		SlackBotToken:  "xoxb-test-token",
		SlackChannelID: "C123456",
		EventsTTLDays:  90,
		HTTPClient:     httpClient,
		Logger:         slog.New(slog.NewJSONHandler(os.Stdout, nil)),
	}

	detail := types.InterlockEvent{
		PipelineID: "silver-cdr-hour",
		ScheduleID: "hourly",
		Date:       "2026-03-04T10",
		Message:    "SLA breach",
		Timestamp:  time.Now(),
	}

	record := buildSQSRecord(t, "msg-save-err", "SLA_BREACH", detail)
	sqsEvent := sqsevents.SQSEvent{Records: []sqsevents.SQSMessage{record}}

	// saveThreadTs error should be logged but NOT fail the message.
	resp, err := lambda.HandleAlertDispatcher(context.Background(), deps, sqsEvent)
	assert.NoError(t, err)
	assert.Empty(t, resp.BatchItemFailures, "saveThreadTs error should not fail the message")
	assert.Len(t, httpClient.requests, 1, "Slack should still have been called")
}

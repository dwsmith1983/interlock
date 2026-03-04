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
	err      error
}

func (m *mockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	if m.err != nil {
		return nil, m.err
	}
	body, _ := io.ReadAll(req.Body)
	m.requests = append(m.requests, req)
	m.bodies = append(m.bodies, body)
	return &http.Response{
		StatusCode: m.status,
		Body:       io.NopCloser(strings.NewReader("ok")),
	}, nil
}

func buildSQSRecord(t *testing.T, messageID string, detailType string, detail types.InterlockEvent) sqsevents.SQSMessage {
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

func newAlertDeps(httpClient *mockHTTPClient, webhookURL string) *lambda.Deps {
	return &lambda.Deps{
		Store: &store.Store{
			Client:      newMockDDB(),
			EventsTable: "events",
		},
		SlackWebhookURL: webhookURL,
		HTTPClient:      httpClient,
		Logger:          slog.New(slog.NewJSONHandler(os.Stdout, nil)),
	}
}

func TestHandleAlertDispatcher_SendsSlack(t *testing.T) {
	httpClient := &mockHTTPClient{status: 200}
	deps := newAlertDeps(httpClient, "https://hooks.slack.com/test")

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

	// Verify HTTP call was made.
	if len(httpClient.requests) != 1 {
		t.Fatalf("expected 1 HTTP request, got %d", len(httpClient.requests))
	}
	req := httpClient.requests[0]
	if req.URL.String() != "https://hooks.slack.com/test" {
		t.Errorf("URL = %q, want %q", req.URL.String(), "https://hooks.slack.com/test")
	}
	if req.Header.Get("Content-Type") != "application/json" {
		t.Errorf("Content-Type = %q, want %q", req.Header.Get("Content-Type"), "application/json")
	}

	// Verify payload contains expected fields.
	var payload map[string]interface{}
	if err := json.Unmarshal(httpClient.bodies[0], &payload); err != nil {
		t.Fatalf("unmarshal slack payload: %v", err)
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

func TestHandleAlertDispatcher_NoWebhook(t *testing.T) {
	httpClient := &mockHTTPClient{status: 200}
	deps := newAlertDeps(httpClient, "") // no webhook

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

func TestHandleAlertDispatcher_SlackError(t *testing.T) {
	httpClient := &mockHTTPClient{status: 500}
	deps := newAlertDeps(httpClient, "https://hooks.slack.com/test")

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
	deps := newAlertDeps(httpClient, "https://hooks.slack.com/test")

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
	deps := newAlertDeps(httpClient, "https://hooks.slack.com/test")

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
	deps := newAlertDeps(httpClient, "https://hooks.slack.com/test")

	records := []sqsevents.SQSMessage{
		buildSQSRecord(t, "msg-a", "SLA_WARNING", types.InterlockEvent{
			PipelineID: "bronze-cdr", Date: "2026-03-04T10", Message: "Warning", Timestamp: time.Now(),
		}),
		buildSQSRecord(t, "msg-b", "SLA_MET", types.InterlockEvent{
			PipelineID: "silver-cdr", Date: "2026-03-04T10", Message: "Met", Timestamp: time.Now(),
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

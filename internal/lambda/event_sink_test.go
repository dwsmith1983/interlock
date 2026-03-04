package lambda_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strings"
	"testing"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/store"
	"github.com/dwsmith1983/interlock/pkg/types"
)

func TestHandleEventSink_WritesEvent(t *testing.T) {
	mock := newMockDDB()
	deps := &lambda.Deps{
		Store: &store.Store{
			Client:      mock,
			EventsTable: "events",
		},
		EventsTTLDays: 90,
		Logger:        slog.New(slog.NewJSONHandler(os.Stdout, nil)),
	}

	detail := types.InterlockEvent{
		PipelineID: "silver-cdr-hour",
		ScheduleID: "hourly",
		Date:       "2026-03-04T10",
		Message:    "SLA breach for silver-cdr-hour",
	}
	detailJSON, err := json.Marshal(detail)
	if err != nil {
		t.Fatalf("marshal detail: %v", err)
	}

	input := lambda.EventBridgeInput{
		Source:     "interlock",
		DetailType: "SLA_BREACH",
		Detail:     string(detailJSON),
	}

	err = lambda.HandleEventSink(context.Background(), deps, input)
	if err != nil {
		t.Fatalf("HandleEventSink returned error: %v", err)
	}

	// Find the written item in the mock store.
	var found map[string]ddbtypes.AttributeValue
	mock.mu.Lock()
	for k, item := range mock.items {
		if strings.HasPrefix(k, "events#PIPELINE#silver-cdr-hour#") {
			found = item
			break
		}
	}
	mock.mu.Unlock()

	if found == nil {
		t.Fatal("expected event item in mock store, found none")
	}

	// Verify PK.
	pk := found["PK"].(*ddbtypes.AttributeValueMemberS).Value
	if pk != "PIPELINE#silver-cdr-hour" {
		t.Errorf("PK = %q, want %q", pk, "PIPELINE#silver-cdr-hour")
	}

	// Verify SK format: {timestampMillis}#SLA_BREACH.
	sk := found["SK"].(*ddbtypes.AttributeValueMemberS).Value
	if !strings.HasSuffix(sk, "#SLA_BREACH") {
		t.Errorf("SK = %q, want suffix #SLA_BREACH", sk)
	}

	// Verify eventType.
	eventType := found["eventType"].(*ddbtypes.AttributeValueMemberS).Value
	if eventType != "SLA_BREACH" {
		t.Errorf("eventType = %q, want %q", eventType, "SLA_BREACH")
	}

	// Verify pipelineId.
	pid := found["pipelineId"].(*ddbtypes.AttributeValueMemberS).Value
	if pid != "silver-cdr-hour" {
		t.Errorf("pipelineId = %q, want %q", pid, "silver-cdr-hour")
	}

	// Verify scheduleId.
	sid := found["scheduleId"].(*ddbtypes.AttributeValueMemberS).Value
	if sid != "hourly" {
		t.Errorf("scheduleId = %q, want %q", sid, "hourly")
	}

	// Verify date.
	date := found["date"].(*ddbtypes.AttributeValueMemberS).Value
	if date != "2026-03-04T10" {
		t.Errorf("date = %q, want %q", date, "2026-03-04T10")
	}

	// Verify message.
	msg := found["message"].(*ddbtypes.AttributeValueMemberS).Value
	if msg != "SLA breach for silver-cdr-hour" {
		t.Errorf("message = %q, want %q", msg, "SLA breach for silver-cdr-hour")
	}

	// Verify timestamp is a number.
	if _, ok := found["timestamp"].(*ddbtypes.AttributeValueMemberN); !ok {
		t.Error("timestamp should be a number attribute")
	}

	// Verify ttl is a number.
	if _, ok := found["ttl"].(*ddbtypes.AttributeValueMemberN); !ok {
		t.Error("ttl should be a number attribute")
	}
}

func TestHandleEventSink_InvalidDetail(t *testing.T) {
	mock := newMockDDB()
	deps := &lambda.Deps{
		Store: &store.Store{
			Client:      mock,
			EventsTable: "events",
		},
		EventsTTLDays: 90,
		Logger:        slog.New(slog.NewJSONHandler(os.Stdout, nil)),
	}

	input := lambda.EventBridgeInput{
		Source:     "interlock",
		DetailType: "SLA_BREACH",
		Detail:     "not valid json{{{",
	}

	err := lambda.HandleEventSink(context.Background(), deps, input)
	if err == nil {
		t.Fatal("expected error for invalid JSON detail, got nil")
	}
	if !strings.Contains(err.Error(), "unmarshal event detail") {
		t.Errorf("error = %q, want it to contain 'unmarshal event detail'", err.Error())
	}
}

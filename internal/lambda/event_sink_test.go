package lambda_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/store"
	"github.com/dwsmith1983/interlock/pkg/types"
)

func newEventSinkDeps(ddb store.DynamoAPI) *lambda.Deps {
	return &lambda.Deps{
		Store: &store.Store{
			Client:      ddb,
			EventsTable: "events",
		},
		EventsTTLDays: 90,
		Logger:        slog.New(slog.NewJSONHandler(os.Stdout, nil)),
	}
}

func TestHandleEventSink_WritesEvent(t *testing.T) {
	mock := newMockDDB()
	deps := newEventSinkDeps(mock)

	eventTime := time.Date(2026, 3, 4, 10, 15, 0, 0, time.UTC)
	detail := types.InterlockEvent{
		PipelineID: "silver-cdr-hour",
		ScheduleID: "hourly",
		Date:       "2026-03-04T10",
		Message:    "SLA breach for silver-cdr-hour",
		Timestamp:  eventTime,
	}
	detailJSON, err := json.Marshal(detail)
	if err != nil {
		t.Fatalf("marshal detail: %v", err)
	}

	input := lambda.EventBridgeInput{
		Source:     "interlock",
		DetailType: "SLA_BREACH",
		Detail:     detailJSON,
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

	// Verify SK uses event timestamp, not ingestion time.
	sk := found["SK"].(*ddbtypes.AttributeValueMemberS).Value
	expectedPrefix := fmt.Sprintf("%d#", eventTime.UnixMilli())
	if !strings.HasPrefix(sk, expectedPrefix) {
		t.Errorf("SK = %q, want prefix %q (event timestamp)", sk, expectedPrefix)
	}
	if !strings.HasSuffix(sk, "#SLA_BREACH") {
		t.Errorf("SK = %q, want suffix #SLA_BREACH", sk)
	}

	// Verify fields.
	if v := found["eventType"].(*ddbtypes.AttributeValueMemberS).Value; v != "SLA_BREACH" {
		t.Errorf("eventType = %q, want %q", v, "SLA_BREACH")
	}
	if v := found["pipelineId"].(*ddbtypes.AttributeValueMemberS).Value; v != "silver-cdr-hour" {
		t.Errorf("pipelineId = %q, want %q", v, "silver-cdr-hour")
	}
	if v := found["scheduleId"].(*ddbtypes.AttributeValueMemberS).Value; v != "hourly" {
		t.Errorf("scheduleId = %q, want %q", v, "hourly")
	}
	if v := found["date"].(*ddbtypes.AttributeValueMemberS).Value; v != "2026-03-04T10" {
		t.Errorf("date = %q, want %q", v, "2026-03-04T10")
	}
	if v := found["message"].(*ddbtypes.AttributeValueMemberS).Value; v != "SLA breach for silver-cdr-hour" {
		t.Errorf("message = %q, want %q", v, "SLA breach for silver-cdr-hour")
	}

	// Verify timestamp matches event time.
	tsStr := found["timestamp"].(*ddbtypes.AttributeValueMemberN).Value
	ts, _ := strconv.ParseInt(tsStr, 10, 64)
	if ts != eventTime.UnixMilli() {
		t.Errorf("timestamp = %d, want %d", ts, eventTime.UnixMilli())
	}

	// Verify TTL is approximately 90 days from now.
	ttlStr := found["ttl"].(*ddbtypes.AttributeValueMemberN).Value
	ttlVal, _ := strconv.ParseInt(ttlStr, 10, 64)
	expectedTTL := time.Now().Add(90 * 24 * time.Hour).Unix()
	if abs(ttlVal-expectedTTL) > 60 { // allow 60s tolerance
		t.Errorf("ttl = %d, want ~%d (90 days from now)", ttlVal, expectedTTL)
	}
}

func TestHandleEventSink_InvalidDetail(t *testing.T) {
	mock := newMockDDB()
	deps := newEventSinkDeps(mock)

	input := lambda.EventBridgeInput{
		Source:     "interlock",
		DetailType: "SLA_BREACH",
		Detail:     json.RawMessage("not valid json{{{"),
	}

	err := lambda.HandleEventSink(context.Background(), deps, input)
	if err == nil {
		t.Fatal("expected error for invalid JSON detail, got nil")
	}
	if !strings.Contains(err.Error(), "unmarshal event detail") {
		t.Errorf("error = %q, want it to contain 'unmarshal event detail'", err.Error())
	}
}

func TestHandleEventSink_DynamoDBError(t *testing.T) {
	ddb := &mockDynamo{
		putItemFn: func(_ context.Context, _ *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			return nil, fmt.Errorf("dynamodb: provisioned throughput exceeded")
		},
	}
	deps := &lambda.Deps{
		Store: &store.Store{
			Client:      ddb,
			EventsTable: "events",
		},
		EventsTTLDays: 90,
		Logger:        slog.New(slog.NewJSONHandler(os.Stdout, nil)),
	}

	detail := types.InterlockEvent{
		PipelineID: "test-pipe",
		ScheduleID: "stream",
		Date:       "2026-03-04",
		Message:    "test",
		Timestamp:  time.Now(),
	}
	detailJSON, _ := json.Marshal(detail)

	input := lambda.EventBridgeInput{
		Source:     "interlock",
		DetailType: "JOB_COMPLETED",
		Detail:     detailJSON,
	}

	err := lambda.HandleEventSink(context.Background(), deps, input)
	if err == nil {
		t.Fatal("expected error when DynamoDB fails")
	}
	if !strings.Contains(err.Error(), "write event to events table") {
		t.Errorf("error = %q, want it to contain 'write event to events table'", err.Error())
	}
	if !strings.Contains(err.Error(), "pipeline=test-pipe") {
		t.Errorf("error = %q, want it to contain pipeline identifier", err.Error())
	}
}

func abs(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}

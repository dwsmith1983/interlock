package store

import (
	"context"
	"errors"
	"strings"
	"testing"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	v2 "github.com/dwsmith1983/interlock/pkg/types/v2"
)

func TestWriteJobEvent_Success(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	err := s.WriteJobEvent(context.Background(), "pipe-1", "daily", "2026-03-01", v2.JobEventSuccess, "run-abc", 12345, "")
	if err != nil {
		t.Fatalf("WriteJobEvent: %v", err)
	}

	// Find the written item in the mock.
	mock.mu.Lock()
	defer mock.mu.Unlock()

	var found map[string]ddbtypes.AttributeValue
	for k, item := range mock.items {
		if strings.HasPrefix(k, "joblog#"+v2.PipelinePK("pipe-1")+"#JOB#daily#2026-03-01#") {
			found = item
			break
		}
	}
	if found == nil {
		t.Fatal("expected job event item in mock store")
	}

	// Verify required attributes.
	event := found["event"].(*ddbtypes.AttributeValueMemberS).Value
	if event != v2.JobEventSuccess {
		t.Errorf("event = %q, want %q", event, v2.JobEventSuccess)
	}
	runID := found["runId"].(*ddbtypes.AttributeValueMemberS).Value
	if runID != "run-abc" {
		t.Errorf("runId = %q, want %q", runID, "run-abc")
	}
	dur := found["duration"].(*ddbtypes.AttributeValueMemberN).Value
	if dur != "12345" {
		t.Errorf("duration = %q, want %q", dur, "12345")
	}

	// error attribute should be absent (empty errMsg).
	if _, ok := found["error"]; ok {
		t.Error("expected error attribute to be absent for empty errMsg")
	}

	// TTL should be present.
	if _, ok := found["ttl"]; !ok {
		t.Error("expected ttl attribute to be present")
	}
}

func TestWriteJobEvent_WithError(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	err := s.WriteJobEvent(context.Background(), "pipe-1", "daily", "2026-03-01", v2.JobEventFail, "", 0, "null pointer")
	if err != nil {
		t.Fatalf("WriteJobEvent: %v", err)
	}

	// Find the written item.
	mock.mu.Lock()
	defer mock.mu.Unlock()

	var found map[string]ddbtypes.AttributeValue
	for k, item := range mock.items {
		if strings.HasPrefix(k, "joblog#"+v2.PipelinePK("pipe-1")+"#JOB#daily#2026-03-01#") {
			found = item
			break
		}
	}
	if found == nil {
		t.Fatal("expected job event item in mock store")
	}

	// error attribute should be present.
	errAttr, ok := found["error"]
	if !ok {
		t.Fatal("expected error attribute to be present")
	}
	errVal := errAttr.(*ddbtypes.AttributeValueMemberS).Value
	if errVal != "null pointer" {
		t.Errorf("error = %q, want %q", errVal, "null pointer")
	}

	// runId and duration should be absent (empty/zero values).
	if _, ok := found["runId"]; ok {
		t.Error("expected runId attribute to be absent for empty runID")
	}
	if _, ok := found["duration"]; ok {
		t.Error("expected duration attribute to be absent for zero duration")
	}
}

func TestGetLatestJobEvent_Found(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	pk := v2.PipelinePK("pipe-1")

	// Seed two events with different timestamps. The later one should be returned.
	mock.putRaw("joblog", map[string]ddbtypes.AttributeValue{
		"PK":    &ddbtypes.AttributeValueMemberS{Value: pk},
		"SK":    &ddbtypes.AttributeValueMemberS{Value: v2.JobSK("daily", "2026-03-01", "1000000000000")},
		"event": &ddbtypes.AttributeValueMemberS{Value: v2.JobEventSuccess},
		"runId": &ddbtypes.AttributeValueMemberS{Value: "run-old"},
	})
	mock.putRaw("joblog", map[string]ddbtypes.AttributeValue{
		"PK":    &ddbtypes.AttributeValueMemberS{Value: pk},
		"SK":    &ddbtypes.AttributeValueMemberS{Value: v2.JobSK("daily", "2026-03-01", "2000000000000")},
		"event": &ddbtypes.AttributeValueMemberS{Value: v2.JobEventFail},
		"runId": &ddbtypes.AttributeValueMemberS{Value: "run-new"},
		"error": &ddbtypes.AttributeValueMemberS{Value: "timeout"},
	})

	rec, err := s.GetLatestJobEvent(context.Background(), "pipe-1", "daily", "2026-03-01")
	if err != nil {
		t.Fatalf("GetLatestJobEvent: %v", err)
	}
	if rec == nil {
		t.Fatal("expected non-nil record")
	}
	if rec.RunID != "run-new" {
		t.Errorf("RunID = %q, want %q", rec.RunID, "run-new")
	}
	if rec.Event != v2.JobEventFail {
		t.Errorf("Event = %q, want %q", rec.Event, v2.JobEventFail)
	}
	if rec.Error != "timeout" {
		t.Errorf("Error = %q, want %q", rec.Error, "timeout")
	}
}

func TestGetLatestJobEvent_NotFound(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	rec, err := s.GetLatestJobEvent(context.Background(), "pipe-1", "daily", "2026-03-01")
	if err != nil {
		t.Fatalf("GetLatestJobEvent: %v", err)
	}
	if rec != nil {
		t.Errorf("expected nil record, got %+v", rec)
	}
}

func TestWriteJobEvent_DynamoError(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	injected := errors.New("throttled")
	mock.errFn = errOnOp("PutItem", injected)

	err := s.WriteJobEvent(context.Background(), "pipe-1", "daily", "2026-03-01", v2.JobEventSuccess, "run-1", 0, "")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, injected) {
		t.Errorf("expected wrapped injected error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "write job event") {
		t.Errorf("expected context in error message, got: %v", err)
	}
}

func TestGetLatestJobEvent_DynamoError(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	injected := errors.New("service unavailable")
	mock.errFn = errOnOp("Query", injected)

	rec, err := s.GetLatestJobEvent(context.Background(), "pipe-1", "daily", "2026-03-01")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if rec != nil {
		t.Errorf("expected nil record on error, got %+v", rec)
	}
	if !errors.Is(err, injected) {
		t.Errorf("expected wrapped injected error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "get latest job event") {
		t.Errorf("expected context in error message, got: %v", err)
	}
}

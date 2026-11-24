package store

import (
	"context"
	"errors"
	"strings"
	"testing"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/pkg/types"
)

func TestWriteJobEvent_Success(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	err := s.WriteJobEvent(context.Background(), "pipe-1", "daily", "2026-03-01", types.JobEventSuccess, "run-abc", 12345, "")
	if err != nil {
		t.Fatalf("WriteJobEvent: %v", err)
	}

	// Find the written item in the mock.
	mock.mu.Lock()
	defer mock.mu.Unlock()

	var found map[string]ddbtypes.AttributeValue
	for k, item := range mock.items {
		if strings.HasPrefix(k, "joblog#"+types.PipelinePK("pipe-1")+"#JOB#daily#2026-03-01#") {
			found = item
			break
		}
	}
	if found == nil {
		t.Fatal("expected job event item in mock store")
	}

	// Verify required attributes.
	event := found["event"].(*ddbtypes.AttributeValueMemberS).Value
	if event != types.JobEventSuccess {
		t.Errorf("event = %q, want %q", event, types.JobEventSuccess)
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

	err := s.WriteJobEvent(context.Background(), "pipe-1", "daily", "2026-03-01", types.JobEventFail, "", 0, "null pointer")
	if err != nil {
		t.Fatalf("WriteJobEvent: %v", err)
	}

	// Find the written item.
	mock.mu.Lock()
	defer mock.mu.Unlock()

	var found map[string]ddbtypes.AttributeValue
	for k, item := range mock.items {
		if strings.HasPrefix(k, "joblog#"+types.PipelinePK("pipe-1")+"#JOB#daily#2026-03-01#") {
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

func TestWriteJobEvent_DynamoError(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	injected := errors.New("throttled")
	mock.errFn = errOnOp("PutItem", injected)

	err := s.WriteJobEvent(context.Background(), "pipe-1", "daily", "2026-03-01", types.JobEventSuccess, "run-1", 0, "")
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

func TestWriteJobEvent_WithCategory(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	err := s.WriteJobEvent(context.Background(), "pipe-1", "daily", "2026-03-01", types.JobEventFail, "run-1", 0, "OOM killed",
		WithFailureCategory(types.FailurePermanent))
	if err != nil {
		t.Fatalf("WriteJobEvent: %v", err)
	}

	mock.mu.Lock()
	defer mock.mu.Unlock()

	var found map[string]ddbtypes.AttributeValue
	for k, item := range mock.items {
		if strings.HasPrefix(k, "joblog#"+types.PipelinePK("pipe-1")+"#JOB#daily#2026-03-01#") {
			found = item
			break
		}
	}
	if found == nil {
		t.Fatal("expected job event item in mock store")
	}

	catAttr, ok := found["category"]
	if !ok {
		t.Fatal("expected category attribute to be present")
	}
	catVal := catAttr.(*ddbtypes.AttributeValueMemberS).Value
	if catVal != "PERMANENT" {
		t.Errorf("category = %q, want %q", catVal, "PERMANENT")
	}
}

func TestWriteJobEvent_WithoutCategory(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	err := s.WriteJobEvent(context.Background(), "pipe-1", "daily", "2026-03-01", types.JobEventFail, "", 0, "some error")
	if err != nil {
		t.Fatalf("WriteJobEvent: %v", err)
	}

	mock.mu.Lock()
	defer mock.mu.Unlock()

	var found map[string]ddbtypes.AttributeValue
	for k, item := range mock.items {
		if strings.HasPrefix(k, "joblog#"+types.PipelinePK("pipe-1")+"#JOB#daily#2026-03-01#") {
			found = item
			break
		}
	}
	if found == nil {
		t.Fatal("expected job event item in mock store")
	}

	if _, ok := found["category"]; ok {
		t.Error("expected category attribute to be absent when no option provided")
	}
}

package store

import (
	"context"
	"errors"
	"strings"
	"testing"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/pkg/types"
)

func TestWriteRerun_First(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	attempt, err := s.WriteRerun(context.Background(), "pipe-1", "daily", "2026-03-01", "quality drop", "JOB#daily#2026-03-01#1709308800")
	if err != nil {
		t.Fatalf("WriteRerun: %v", err)
	}
	if attempt != 0 {
		t.Errorf("attempt = %d, want 0", attempt)
	}

	// Verify the record was written.
	key := itemKey("rerun", types.PipelinePK("pipe-1"), types.RerunSK("daily", "2026-03-01", 0))
	mock.mu.Lock()
	item, ok := mock.items[key]
	mock.mu.Unlock()

	if !ok {
		t.Fatal("expected rerun record to exist")
	}
	reason := item["reason"].(*ddbtypes.AttributeValueMemberS).Value
	if reason != "quality drop" {
		t.Errorf("reason = %q, want %q", reason, "quality drop")
	}
	src := item["sourceJobEvent"].(*ddbtypes.AttributeValueMemberS).Value
	if src != "JOB#daily#2026-03-01#1709308800" {
		t.Errorf("sourceJobEvent = %q, want %q", src, "JOB#daily#2026-03-01#1709308800")
	}
}

func TestWriteRerun_Subsequent(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	// Write first rerun.
	attempt0, err := s.WriteRerun(context.Background(), "pipe-1", "daily", "2026-03-01", "first rerun", "")
	if err != nil {
		t.Fatalf("first WriteRerun: %v", err)
	}
	if attempt0 != 0 {
		t.Errorf("first attempt = %d, want 0", attempt0)
	}

	// Write second rerun.
	attempt1, err := s.WriteRerun(context.Background(), "pipe-1", "daily", "2026-03-01", "second rerun", "")
	if err != nil {
		t.Fatalf("second WriteRerun: %v", err)
	}
	if attempt1 != 1 {
		t.Errorf("second attempt = %d, want 1", attempt1)
	}
}

func TestCountReruns_Empty(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	count, err := s.CountReruns(context.Background(), "pipe-1", "daily", "2026-03-01")
	if err != nil {
		t.Fatalf("CountReruns: %v", err)
	}
	if count != 0 {
		t.Errorf("count = %d, want 0", count)
	}
}

func TestCountReruns_Multiple(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	pk := types.PipelinePK("pipe-1")

	// Seed 3 rerun records directly.
	for i := 0; i < 3; i++ {
		mock.putRaw("rerun", map[string]ddbtypes.AttributeValue{
			"PK":     &ddbtypes.AttributeValueMemberS{Value: pk},
			"SK":     &ddbtypes.AttributeValueMemberS{Value: types.RerunSK("daily", "2026-03-01", i)},
			"reason": &ddbtypes.AttributeValueMemberS{Value: "test"},
		})
	}

	count, err := s.CountReruns(context.Background(), "pipe-1", "daily", "2026-03-01")
	if err != nil {
		t.Fatalf("CountReruns: %v", err)
	}
	if count != 3 {
		t.Errorf("count = %d, want 3", count)
	}
}

func TestWriteRerun_DynamoError(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	injected := errors.New("service unavailable")
	mock.errFn = errOnOp("PutItem", injected)

	_, err := s.WriteRerun(context.Background(), "pipe-1", "daily", "2026-03-01", "test", "")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, injected) {
		t.Errorf("expected wrapped injected error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "put rerun") {
		t.Errorf("expected context in error message, got: %v", err)
	}
}

func TestCountReruns_DynamoError(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	injected := errors.New("throttled")
	mock.errFn = errOnOp("Query", injected)

	_, err := s.CountReruns(context.Background(), "pipe-1", "daily", "2026-03-01")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, injected) {
		t.Errorf("expected wrapped injected error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "count reruns") {
		t.Errorf("expected context in error message, got: %v", err)
	}
}

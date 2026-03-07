package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/pkg/types"
)

func TestGetConfig_Found(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "gold-revenue", Owner: "data-eng"},
		Schedule: types.ScheduleConfig{Cron: "0 6 * * *", Timezone: "UTC"},
	}
	data, _ := json.Marshal(cfg)

	mock.putRaw("control", map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("gold-revenue")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.ConfigSK},
		"config": &ddbtypes.AttributeValueMemberS{Value: string(data)},
	})

	got, err := s.GetConfig(context.Background(), "gold-revenue")
	if err != nil {
		t.Fatalf("GetConfig: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil config")
	}
	if got.Pipeline.ID != "gold-revenue" {
		t.Errorf("Pipeline.ID = %q, want %q", got.Pipeline.ID, "gold-revenue")
	}
	if got.Pipeline.Owner != "data-eng" {
		t.Errorf("Pipeline.Owner = %q, want %q", got.Pipeline.Owner, "data-eng")
	}
	if got.Schedule.Cron != "0 6 * * *" {
		t.Errorf("Schedule.Cron = %q, want %q", got.Schedule.Cron, "0 6 * * *")
	}
}

func TestGetConfig_NotFound(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	got, err := s.GetConfig(context.Background(), "nonexistent")
	if err != nil {
		t.Fatalf("GetConfig: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil config, got %+v", got)
	}
}

func TestPutConfig_RoundTrip(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "bronze-ingest", Owner: "platform"},
		Schedule: types.ScheduleConfig{
			Cron:     "0 */2 * * *",
			Timezone: "America/New_York",
			Evaluation: types.EvaluationWindow{
				Window:   "1h",
				Interval: "5m",
			},
		},
		Validation: types.ValidationConfig{
			Trigger: "ALL",
			Rules: []types.ValidationRule{
				{Key: "SENSOR#upstream-complete", Check: types.CheckExists},
			},
		},
		Job: types.JobConfig{
			Type:       "glue",
			Config:     map[string]interface{}{"jobName": "bronze-etl"},
			MaxRetries: 3,
		},
	}

	if err := s.PutConfig(context.Background(), cfg); err != nil {
		t.Fatalf("PutConfig: %v", err)
	}

	got, err := s.GetConfig(context.Background(), "bronze-ingest")
	if err != nil {
		t.Fatalf("GetConfig: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil config after put")
	}
	if got.Pipeline.ID != "bronze-ingest" {
		t.Errorf("Pipeline.ID = %q, want %q", got.Pipeline.ID, "bronze-ingest")
	}
	if got.Pipeline.Owner != "platform" {
		t.Errorf("Pipeline.Owner = %q, want %q", got.Pipeline.Owner, "platform")
	}
	if got.Schedule.Cron != "0 */2 * * *" {
		t.Errorf("Schedule.Cron = %q, want %q", got.Schedule.Cron, "0 */2 * * *")
	}
	if got.Validation.Trigger != "ALL" {
		t.Errorf("Validation.Trigger = %q, want %q", got.Validation.Trigger, "ALL")
	}
	if len(got.Validation.Rules) != 1 {
		t.Fatalf("len(Rules) = %d, want 1", len(got.Validation.Rules))
	}
	if got.Job.MaxRetries != 3 {
		t.Errorf("Job.MaxRetries = %d, want 3", got.Job.MaxRetries)
	}
}

func TestGetSensorData_Found(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	// Seed a sensor row using attributevalue-compatible structure.
	mock.putRaw("control", map[string]ddbtypes.AttributeValue{
		"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("pipe-1")},
		"SK": &ddbtypes.AttributeValueMemberS{Value: types.SensorSK("upstream-complete")},
		"data": &ddbtypes.AttributeValueMemberM{Value: map[string]ddbtypes.AttributeValue{
			"status":    &ddbtypes.AttributeValueMemberS{Value: "done"},
			"rowCount":  &ddbtypes.AttributeValueMemberN{Value: "42000"},
			"timestamp": &ddbtypes.AttributeValueMemberS{Value: "2026-03-01T06:00:00Z"},
		}},
	})

	data, err := s.GetSensorData(context.Background(), "pipe-1", "upstream-complete")
	if err != nil {
		t.Fatalf("GetSensorData: %v", err)
	}
	if data == nil {
		t.Fatal("expected non-nil data")
	}
	if data["status"] != "done" {
		t.Errorf("status = %v, want %q", data["status"], "done")
	}
}

func TestGetSensorData_NotFound(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	data, err := s.GetSensorData(context.Background(), "pipe-1", "nonexistent")
	if err != nil {
		t.Fatalf("GetSensorData: %v", err)
	}
	if data != nil {
		t.Errorf("expected nil data, got %v", data)
	}
}

func TestGetAllSensors(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	pk := types.PipelinePK("pipe-1")

	// Seed 2 sensor rows + 1 CONFIG row (should be excluded).
	mock.putRaw("control", map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: pk},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.ConfigSK},
		"config": &ddbtypes.AttributeValueMemberS{Value: `{}`},
	})
	mock.putRaw("control", map[string]ddbtypes.AttributeValue{
		"PK": &ddbtypes.AttributeValueMemberS{Value: pk},
		"SK": &ddbtypes.AttributeValueMemberS{Value: types.SensorSK("sensor-a")},
		"data": &ddbtypes.AttributeValueMemberM{Value: map[string]ddbtypes.AttributeValue{
			"val": &ddbtypes.AttributeValueMemberS{Value: "alpha"},
		}},
	})
	mock.putRaw("control", map[string]ddbtypes.AttributeValue{
		"PK": &ddbtypes.AttributeValueMemberS{Value: pk},
		"SK": &ddbtypes.AttributeValueMemberS{Value: types.SensorSK("sensor-b")},
		"data": &ddbtypes.AttributeValueMemberM{Value: map[string]ddbtypes.AttributeValue{
			"val": &ddbtypes.AttributeValueMemberS{Value: "beta"},
		}},
	})

	sensors, err := s.GetAllSensors(context.Background(), "pipe-1")
	if err != nil {
		t.Fatalf("GetAllSensors: %v", err)
	}
	if len(sensors) != 2 {
		t.Fatalf("len(sensors) = %d, want 2", len(sensors))
	}
	if sensors["sensor-a"]["val"] != "alpha" {
		t.Errorf("sensor-a val = %v, want %q", sensors["sensor-a"]["val"], "alpha")
	}
	if sensors["sensor-b"]["val"] != "beta" {
		t.Errorf("sensor-b val = %v, want %q", sensors["sensor-b"]["val"], "beta")
	}
}

func TestAcquireTriggerLock_Success(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	acquired, err := s.AcquireTriggerLock(context.Background(), "pipe-1", "daily", "2026-03-01", 24*time.Hour)
	if err != nil {
		t.Fatalf("AcquireTriggerLock: %v", err)
	}
	if !acquired {
		t.Error("expected lock to be acquired")
	}
}

func TestAcquireTriggerLock_AlreadyHeld(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	// First acquire should succeed.
	acquired, err := s.AcquireTriggerLock(context.Background(), "pipe-1", "daily", "2026-03-01", 24*time.Hour)
	if err != nil {
		t.Fatalf("first AcquireTriggerLock: %v", err)
	}
	if !acquired {
		t.Fatal("expected first acquire to succeed")
	}

	// Second acquire should fail (lock already held).
	acquired, err = s.AcquireTriggerLock(context.Background(), "pipe-1", "daily", "2026-03-01", 24*time.Hour)
	if err != nil {
		t.Fatalf("second AcquireTriggerLock: %v", err)
	}
	if acquired {
		t.Error("expected second acquire to fail (lock already held)")
	}
}

func TestReleaseTriggerLock(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	// Acquire.
	acquired, err := s.AcquireTriggerLock(context.Background(), "pipe-1", "daily", "2026-03-01", 24*time.Hour)
	if err != nil {
		t.Fatalf("AcquireTriggerLock: %v", err)
	}
	if !acquired {
		t.Fatal("expected acquire to succeed")
	}

	// Release.
	if err := s.ReleaseTriggerLock(context.Background(), "pipe-1", "daily", "2026-03-01"); err != nil {
		t.Fatalf("ReleaseTriggerLock: %v", err)
	}

	// Re-acquire should succeed after release.
	acquired, err = s.AcquireTriggerLock(context.Background(), "pipe-1", "daily", "2026-03-01", 24*time.Hour)
	if err != nil {
		t.Fatalf("re-AcquireTriggerLock: %v", err)
	}
	if !acquired {
		t.Error("expected re-acquire to succeed after release")
	}
}

func TestSetTriggerStatus(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	// Seed a trigger row with TTL to verify UpdateItem preserves it.
	mock.putRaw("control", map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("pipe-1")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("daily", "2026-03-01")},
		"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusRunning},
		"ttl":    &ddbtypes.AttributeValueMemberN{Value: "1740000000"},
	})

	// Update status.
	err := s.SetTriggerStatus(context.Background(), "pipe-1", "daily", "2026-03-01", types.TriggerStatusCompleted)
	if err != nil {
		t.Fatalf("SetTriggerStatus: %v", err)
	}

	// Read back via the mock to verify.
	key := itemKey("control", types.PipelinePK("pipe-1"), types.TriggerSK("daily", "2026-03-01"))
	mock.mu.Lock()
	item, ok := mock.items[key]
	mock.mu.Unlock()

	if !ok {
		t.Fatal("expected trigger row to exist")
	}
	status := item["status"].(*ddbtypes.AttributeValueMemberS).Value
	if status != types.TriggerStatusCompleted {
		t.Errorf("status = %q, want %q", status, types.TriggerStatusCompleted)
	}

	// Verify TTL was preserved (not stripped by UpdateItem).
	ttlAttr, ok := item["ttl"]
	if !ok {
		t.Fatal("expected ttl attribute to be preserved")
	}
	ttlVal := ttlAttr.(*ddbtypes.AttributeValueMemberN).Value
	if ttlVal != "1740000000" {
		t.Errorf("ttl = %q, want %q", ttlVal, "1740000000")
	}
}

func TestGetConfig_DynamoError(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	injected := errors.New("throttled")
	mock.errFn = errOnOp("GetItem", injected)

	_, err := s.GetConfig(context.Background(), "pipe-1")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, injected) {
		t.Errorf("expected wrapped injected error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "get config") {
		t.Errorf("expected context in error message, got: %v", err)
	}
}

func TestAcquireTriggerLock_DynamoError(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	injected := errors.New("service unavailable")
	mock.errFn = errOnOp("PutItem", injected)

	acquired, err := s.AcquireTriggerLock(context.Background(), "pipe-1", "daily", "2026-03-01", 24*time.Hour)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if acquired {
		t.Error("expected acquired=false on DynamoDB error")
	}
	if !errors.Is(err, injected) {
		t.Errorf("expected wrapped injected error, got: %v", err)
	}
}

func TestWriteRerunRequest(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	err := s.WriteRerunRequest(context.Background(), "bronze-cdr", "stream", "2026-03-06T16", "data-drift")
	if err != nil {
		t.Fatalf("WriteRerunRequest: %v", err)
	}

	// Read back the item from the mock.
	key := itemKey("control", types.PipelinePK("bronze-cdr"), types.RerunRequestSK("stream", "2026-03-06T16"))
	mock.mu.Lock()
	item, ok := mock.items[key]
	mock.mu.Unlock()

	if !ok {
		t.Fatal("expected rerun request row to exist in mock")
	}

	pk := item["PK"].(*ddbtypes.AttributeValueMemberS).Value
	if pk != "PIPELINE#bronze-cdr" {
		t.Errorf("PK = %q, want %q", pk, "PIPELINE#bronze-cdr")
	}

	sk := item["SK"].(*ddbtypes.AttributeValueMemberS).Value
	if sk != "RERUN_REQUEST#stream#2026-03-06T16" {
		t.Errorf("SK = %q, want %q", sk, "RERUN_REQUEST#stream#2026-03-06T16")
	}

	reason := item["reason"].(*ddbtypes.AttributeValueMemberS).Value
	if reason != "data-drift" {
		t.Errorf("reason = %q, want %q", reason, "data-drift")
	}

	requestedAtAttr, ok := item["requestedAt"]
	if !ok {
		t.Fatal("expected requestedAt attribute to be present")
	}
	requestedAt := requestedAtAttr.(*ddbtypes.AttributeValueMemberS).Value
	if _, err := time.Parse(time.RFC3339, requestedAt); err != nil {
		t.Errorf("requestedAt %q is not valid RFC3339: %v", requestedAt, err)
	}
}

func TestGetConfig_InvalidJSON(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	// Seed a row with invalid JSON in the config attribute.
	mock.putRaw("control", map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("bad-pipe")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.ConfigSK},
		"config": &ddbtypes.AttributeValueMemberS{Value: `{not valid json`},
	})

	_, err := s.GetConfig(context.Background(), "bad-pipe")
	if err == nil {
		t.Fatal("expected unmarshal error, got nil")
	}
	if !strings.Contains(err.Error(), "unmarshal config") {
		t.Errorf("expected 'unmarshal config' in error, got: %v", err)
	}
}

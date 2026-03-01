package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	v2 "github.com/dwsmith1983/interlock/pkg/types/v2"
)

func TestGetConfig_Found(t *testing.T) {
	mock := newMockDDB()
	s := newTestStore(mock)

	cfg := v2.PipelineConfig{
		Pipeline: v2.PipelineIdentity{ID: "gold-revenue", Owner: "data-eng"},
		Schedule: v2.ScheduleConfig{Cron: "0 6 * * *", Timezone: "UTC"},
	}
	data, _ := json.Marshal(cfg)

	mock.putRaw("control", map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: v2.PipelinePK("gold-revenue")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: v2.ConfigSK},
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

	cfg := v2.PipelineConfig{
		Pipeline: v2.PipelineIdentity{ID: "bronze-ingest", Owner: "platform"},
		Schedule: v2.ScheduleConfig{
			Cron:     "0 */2 * * *",
			Timezone: "America/New_York",
			Evaluation: v2.EvaluationWindow{
				Window:   "1h",
				Interval: "5m",
			},
		},
		Validation: v2.ValidationConfig{
			Trigger: "ALL",
			Rules: []v2.ValidationRule{
				{Key: "SENSOR#upstream-complete", Check: v2.CheckExists},
			},
		},
		Job: v2.JobConfig{
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
		"PK": &ddbtypes.AttributeValueMemberS{Value: v2.PipelinePK("pipe-1")},
		"SK": &ddbtypes.AttributeValueMemberS{Value: v2.SensorSK("upstream-complete")},
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

	pk := v2.PipelinePK("pipe-1")

	// Seed 2 sensor rows + 1 CONFIG row (should be excluded).
	mock.putRaw("control", map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: pk},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: v2.ConfigSK},
		"config": &ddbtypes.AttributeValueMemberS{Value: `{}`},
	})
	mock.putRaw("control", map[string]ddbtypes.AttributeValue{
		"PK": &ddbtypes.AttributeValueMemberS{Value: pk},
		"SK": &ddbtypes.AttributeValueMemberS{Value: v2.SensorSK("sensor-a")},
		"data": &ddbtypes.AttributeValueMemberM{Value: map[string]ddbtypes.AttributeValue{
			"val": &ddbtypes.AttributeValueMemberS{Value: "alpha"},
		}},
	})
	mock.putRaw("control", map[string]ddbtypes.AttributeValue{
		"PK": &ddbtypes.AttributeValueMemberS{Value: pk},
		"SK": &ddbtypes.AttributeValueMemberS{Value: v2.SensorSK("sensor-b")},
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

	// Set status.
	err := s.SetTriggerStatus(context.Background(), "pipe-1", "daily", "2026-03-01", v2.TriggerStatusCompleted)
	if err != nil {
		t.Fatalf("SetTriggerStatus: %v", err)
	}

	// Read back via the mock to verify.
	key := itemKey("control", v2.PipelinePK("pipe-1"), v2.TriggerSK("daily", "2026-03-01"))
	mock.mu.Lock()
	item, ok := mock.items[key]
	mock.mu.Unlock()

	if !ok {
		t.Fatal("expected trigger row to exist")
	}
	status := item["status"].(*ddbtypes.AttributeValueMemberS).Value
	if status != v2.TriggerStatusCompleted {
		t.Errorf("status = %q, want %q", status, v2.TriggerStatusCompleted)
	}
}

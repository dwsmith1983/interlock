package config

import (
	"testing"
	"time"
)

func TestLoadHardening_Defaults(t *testing.T) {
	t.Setenv("STAGE_TIMEOUT", "")
	t.Setenv("SHUTDOWN_BUFFER", "")
	t.Setenv("WORKERS_PER_STAGE", "")
	t.Setenv("DLQ_QUEUE_URL", "")
	t.Setenv("DLQ_MAX_RETRIES", "")
	t.Setenv("CB_TRIP_AFTER", "")
	t.Setenv("CB_TIMEOUT", "")

	cfg, err := LoadHardening()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Timeouts.StageDefault != 30*time.Second {
		t.Errorf("StageDefault = %v, want 30s", cfg.Timeouts.StageDefault)
	}
	if cfg.Timeouts.ShutdownBuffer != 500*time.Millisecond {
		t.Errorf("ShutdownBuffer = %v, want 500ms", cfg.Timeouts.ShutdownBuffer)
	}
	if cfg.Workers.PerStage != 3 {
		t.Errorf("PerStage = %d, want 3", cfg.Workers.PerStage)
	}
	if cfg.DLQ.QueueURL != "" {
		t.Errorf("QueueURL = %q, want empty", cfg.DLQ.QueueURL)
	}
	if cfg.DLQ.MaxRetries != 3 {
		t.Errorf("MaxRetries = %d, want 3", cfg.DLQ.MaxRetries)
	}
	if cfg.CircuitBreaker.TripAfter != 5 {
		t.Errorf("TripAfter = %d, want 5", cfg.CircuitBreaker.TripAfter)
	}
	if cfg.CircuitBreaker.TimeoutSeconds != 60 {
		t.Errorf("TimeoutSeconds = %d, want 60", cfg.CircuitBreaker.TimeoutSeconds)
	}
}

func TestLoadHardening_EnvOverrides(t *testing.T) {
	t.Setenv("STAGE_TIMEOUT", "10s")
	t.Setenv("SHUTDOWN_BUFFER", "1s")
	t.Setenv("WORKERS_PER_STAGE", "50")
	t.Setenv("DLQ_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/dlq")
	t.Setenv("DLQ_MAX_RETRIES", "5")
	t.Setenv("CB_TRIP_AFTER", "10")
	t.Setenv("CB_TIMEOUT", "120")

	cfg, err := LoadHardening()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Timeouts.StageDefault != 10*time.Second {
		t.Errorf("StageDefault = %v, want 10s", cfg.Timeouts.StageDefault)
	}
	if cfg.Timeouts.ShutdownBuffer != 1*time.Second {
		t.Errorf("ShutdownBuffer = %v, want 1s", cfg.Timeouts.ShutdownBuffer)
	}
	if cfg.Workers.PerStage != 50 {
		t.Errorf("PerStage = %d, want 50", cfg.Workers.PerStage)
	}
	if cfg.DLQ.QueueURL != "https://sqs.us-east-1.amazonaws.com/123/dlq" {
		t.Errorf("QueueURL = %q, want SQS URL", cfg.DLQ.QueueURL)
	}
	if cfg.DLQ.MaxRetries != 5 {
		t.Errorf("MaxRetries = %d, want 5", cfg.DLQ.MaxRetries)
	}
	if cfg.CircuitBreaker.TripAfter != 10 {
		t.Errorf("TripAfter = %d, want 10", cfg.CircuitBreaker.TripAfter)
	}
	if cfg.CircuitBreaker.TimeoutSeconds != 120 {
		t.Errorf("TimeoutSeconds = %d, want 120", cfg.CircuitBreaker.TimeoutSeconds)
	}
}

func TestLoadHardening_ValidationRejectsLowTimeout(t *testing.T) {
	t.Setenv("STAGE_TIMEOUT", "50ms")
	t.Setenv("WORKERS_PER_STAGE", "")
	t.Setenv("SHUTDOWN_BUFFER", "")
	t.Setenv("DLQ_QUEUE_URL", "")
	t.Setenv("DLQ_MAX_RETRIES", "")
	t.Setenv("CB_TRIP_AFTER", "")
	t.Setenv("CB_TIMEOUT", "")

	_, err := LoadHardening()
	if err == nil {
		t.Fatal("expected validation error for STAGE_TIMEOUT=50ms, got nil")
	}
	t.Logf("got expected error: %v", err)
}

func TestLoadHardening_ValidationRejectsWorkersOutOfRange(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{"zero workers", "0"},
		{"negative workers", "-1"},
		{"over max workers", "101"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("STAGE_TIMEOUT", "")
			t.Setenv("SHUTDOWN_BUFFER", "")
			t.Setenv("WORKERS_PER_STAGE", tc.value)
			t.Setenv("DLQ_QUEUE_URL", "")
			t.Setenv("DLQ_MAX_RETRIES", "")
			t.Setenv("CB_TRIP_AFTER", "")
			t.Setenv("CB_TIMEOUT", "")

			_, err := LoadHardening()
			if err == nil {
				t.Fatalf("expected validation error for WORKERS_PER_STAGE=%s, got nil", tc.value)
			}
			t.Logf("got expected error: %v", err)
		})
	}
}

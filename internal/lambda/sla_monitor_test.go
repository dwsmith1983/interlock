package lambda_test

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/dwsmith1983/interlock/internal/lambda"
)

// ---------------------------------------------------------------------------
// Calculate tests
// ---------------------------------------------------------------------------

func TestSLAMonitor_Calculate_Basic(t *testing.T) {
	d := &lambda.Deps{Logger: slog.Default()}
	out, err := lambda.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
		Mode:             "calculate",
		PipelineID:       "gold-orders",
		Date:             "2026-03-01",
		Deadline:         "08:00",
		ExpectedDuration: "30m",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Expect full ISO 8601 timestamps for SFN TimestampPath
	if out.WarningAt != "2026-03-01T07:30:00Z" {
		t.Errorf("warningAt = %q, want %q", out.WarningAt, "2026-03-01T07:30:00Z")
	}
	if out.BreachAt != "2026-03-01T08:00:00Z" {
		t.Errorf("breachAt = %q, want %q", out.BreachAt, "2026-03-01T08:00:00Z")
	}
}

func TestSLAMonitor_Calculate_Midnight(t *testing.T) {
	d := &lambda.Deps{Logger: slog.Default()}
	out, err := lambda.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
		Mode:             "calculate",
		PipelineID:       "gold-orders",
		Date:             "2026-03-01",
		Deadline:         "00:30",
		ExpectedDuration: "1h",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Warning wraps to previous day
	if out.WarningAt != "2026-02-28T23:30:00Z" {
		t.Errorf("warningAt = %q, want %q", out.WarningAt, "2026-02-28T23:30:00Z")
	}
	if out.BreachAt != "2026-03-01T00:30:00Z" {
		t.Errorf("breachAt = %q, want %q", out.BreachAt, "2026-03-01T00:30:00Z")
	}
}

func TestSLAMonitor_Calculate_ReturnsRFC3339(t *testing.T) {
	d := &lambda.Deps{Logger: slog.Default()}
	out, err := lambda.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
		Mode:             "calculate",
		PipelineID:       "gold-orders",
		Date:             "2026-06-15",
		Deadline:         "14:00",
		ExpectedDuration: "15m",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Verify timestamps contain full date and T/Z markers
	if !strings.Contains(out.WarningAt, "2026-06-15T") || !strings.HasSuffix(out.WarningAt, "Z") {
		t.Errorf("warningAt %q not valid RFC3339", out.WarningAt)
	}
	if !strings.Contains(out.BreachAt, "2026-06-15T") || !strings.HasSuffix(out.BreachAt, "Z") {
		t.Errorf("breachAt %q not valid RFC3339", out.BreachAt)
	}
}

func TestSLAMonitor_Calculate_RelativeDeadline(t *testing.T) {
	d := &lambda.Deps{Logger: slog.Default()}
	out, err := lambda.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
		Mode:             "calculate",
		PipelineID:       "silver-cdr-hour",
		Date:             "2026-03-02",
		Deadline:         ":30",
		ExpectedDuration: "10m",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Relative deadline ":30" should produce valid RFC3339 timestamps
	if !strings.Contains(out.BreachAt, "T") || !strings.HasSuffix(out.BreachAt, "Z") {
		t.Errorf("breachAt %q not valid RFC3339", out.BreachAt)
	}
	if !strings.Contains(out.WarningAt, "T") || !strings.HasSuffix(out.WarningAt, "Z") {
		t.Errorf("warningAt %q not valid RFC3339", out.WarningAt)
	}
	// Breach minute should be :30
	if !strings.Contains(out.BreachAt, ":30:00Z") {
		t.Errorf("breachAt %q should have minute 30", out.BreachAt)
	}
}

func TestSLAMonitor_Calculate_InvalidDeadline(t *testing.T) {
	d := &lambda.Deps{Logger: slog.Default()}
	_, err := lambda.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
		Mode:             "calculate",
		PipelineID:       "gold-orders",
		Deadline:         "not-a-time",
		ExpectedDuration: "30m",
	})
	if err == nil {
		t.Fatal("expected error for invalid deadline")
	}
}

func TestSLAMonitor_Calculate_InvalidDuration(t *testing.T) {
	d := &lambda.Deps{Logger: slog.Default()}
	_, err := lambda.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
		Mode:             "calculate",
		PipelineID:       "gold-orders",
		Deadline:         "08:00",
		ExpectedDuration: "not-a-duration",
	})
	if err == nil {
		t.Fatal("expected error for invalid duration")
	}
}

// ---------------------------------------------------------------------------
// Fire-alert tests
// ---------------------------------------------------------------------------

func TestSLAMonitor_FireAlert_Warning(t *testing.T) {
	eb := &mockEventBridge{}
	d := &lambda.Deps{
		EventBridge:  eb,
		EventBusName: "test-bus",
		Logger:       slog.Default(),
	}

	out, err := lambda.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
		Mode:       "fire-alert",
		PipelineID: "gold-orders",
		ScheduleID: "daily",
		Date:       "2026-03-01",
		AlertType:  "SLA_WARNING",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.AlertType != "SLA_WARNING" {
		t.Errorf("alertType = %q, want %q", out.AlertType, "SLA_WARNING")
	}
	if out.FiredAt == "" {
		t.Error("firedAt should not be empty")
	}

	if len(eb.events) != 1 {
		t.Fatalf("expected 1 EventBridge call, got %d", len(eb.events))
	}
	entry := eb.events[0].Entries[0]
	if *entry.DetailType != "SLA_WARNING" {
		t.Errorf("detail type = %q, want %q", *entry.DetailType, "SLA_WARNING")
	}
}

func TestSLAMonitor_FireAlert_Breach(t *testing.T) {
	eb := &mockEventBridge{}
	d := &lambda.Deps{
		EventBridge:  eb,
		EventBusName: "test-bus",
		Logger:       slog.Default(),
	}

	out, err := lambda.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
		Mode:       "fire-alert",
		PipelineID: "gold-orders",
		ScheduleID: "daily",
		Date:       "2026-03-01",
		AlertType:  "SLA_BREACH",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.AlertType != "SLA_BREACH" {
		t.Errorf("alertType = %q, want %q", out.AlertType, "SLA_BREACH")
	}

	if len(eb.events) != 1 {
		t.Fatalf("expected 1 EventBridge call, got %d", len(eb.events))
	}
	entry := eb.events[0].Entries[0]
	if *entry.DetailType != "SLA_BREACH" {
		t.Errorf("detail type = %q, want %q", *entry.DetailType, "SLA_BREACH")
	}
}

func TestSLAMonitor_FireAlert_NoEventBridge(t *testing.T) {
	d := &lambda.Deps{
		EventBridge:  nil,
		EventBusName: "",
		Logger:       slog.Default(),
	}

	out, err := lambda.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
		Mode:       "fire-alert",
		PipelineID: "gold-orders",
		ScheduleID: "daily",
		Date:       "2026-03-01",
		AlertType:  "SLA_WARNING",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.AlertType != "SLA_WARNING" {
		t.Errorf("alertType = %q, want %q", out.AlertType, "SLA_WARNING")
	}
	if out.FiredAt == "" {
		t.Error("firedAt should not be empty even when EventBridge is nil")
	}
}

// ---------------------------------------------------------------------------
// Unknown mode test
// ---------------------------------------------------------------------------

func TestSLAMonitor_UnknownMode(t *testing.T) {
	d := &lambda.Deps{Logger: slog.Default()}
	_, err := lambda.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
		Mode:       "explode",
		PipelineID: "gold-orders",
	})
	if err == nil {
		t.Fatal("expected error for unknown mode")
	}
}

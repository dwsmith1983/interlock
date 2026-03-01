package v2

import (
	"context"
	"log/slog"
	"testing"
)

// ---------------------------------------------------------------------------
// Calculate tests
// ---------------------------------------------------------------------------

func TestSLAMonitor_Calculate_Basic(t *testing.T) {
	d := &Deps{Logger: slog.Default()}
	out, err := HandleSLAMonitor(context.Background(), d, SLAMonitorInput{
		Mode:             "calculate",
		PipelineID:       "gold-orders",
		Deadline:         "08:00",
		ExpectedDuration: "30m",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.WarningAt != "07:30" {
		t.Errorf("warningAt = %q, want %q", out.WarningAt, "07:30")
	}
	if out.BreachAt != "08:00" {
		t.Errorf("breachAt = %q, want %q", out.BreachAt, "08:00")
	}
}

func TestSLAMonitor_Calculate_Midnight(t *testing.T) {
	d := &Deps{Logger: slog.Default()}
	out, err := HandleSLAMonitor(context.Background(), d, SLAMonitorInput{
		Mode:             "calculate",
		PipelineID:       "gold-orders",
		Deadline:         "00:30",
		ExpectedDuration: "1h",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.WarningAt != "23:30" {
		t.Errorf("warningAt = %q, want %q", out.WarningAt, "23:30")
	}
	if out.BreachAt != "00:30" {
		t.Errorf("breachAt = %q, want %q", out.BreachAt, "00:30")
	}
}

func TestSLAMonitor_Calculate_InvalidDeadline(t *testing.T) {
	d := &Deps{Logger: slog.Default()}
	_, err := HandleSLAMonitor(context.Background(), d, SLAMonitorInput{
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
	d := &Deps{Logger: slog.Default()}
	_, err := HandleSLAMonitor(context.Background(), d, SLAMonitorInput{
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
	d := &Deps{
		EventBridge:  eb,
		EventBusName: "test-bus",
		Logger:       slog.Default(),
	}

	out, err := HandleSLAMonitor(context.Background(), d, SLAMonitorInput{
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

	if len(eb.calls) != 1 {
		t.Fatalf("expected 1 EventBridge call, got %d", len(eb.calls))
	}
	entry := eb.calls[0].Entries[0]
	if *entry.DetailType != "SLA_WARNING" {
		t.Errorf("detail type = %q, want %q", *entry.DetailType, "SLA_WARNING")
	}
}

func TestSLAMonitor_FireAlert_Breach(t *testing.T) {
	eb := &mockEventBridge{}
	d := &Deps{
		EventBridge:  eb,
		EventBusName: "test-bus",
		Logger:       slog.Default(),
	}

	out, err := HandleSLAMonitor(context.Background(), d, SLAMonitorInput{
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

	if len(eb.calls) != 1 {
		t.Fatalf("expected 1 EventBridge call, got %d", len(eb.calls))
	}
	entry := eb.calls[0].Entries[0]
	if *entry.DetailType != "SLA_BREACH" {
		t.Errorf("detail type = %q, want %q", *entry.DetailType, "SLA_BREACH")
	}
}

func TestSLAMonitor_FireAlert_NoEventBridge(t *testing.T) {
	d := &Deps{
		EventBridge:  nil,
		EventBusName: "",
		Logger:       slog.Default(),
	}

	out, err := HandleSLAMonitor(context.Background(), d, SLAMonitorInput{
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
	d := &Deps{Logger: slog.Default()}
	_, err := HandleSLAMonitor(context.Background(), d, SLAMonitorInput{
		Mode:       "explode",
		PipelineID: "gold-orders",
	})
	if err == nil {
		t.Fatal("expected error for unknown mode")
	}
}

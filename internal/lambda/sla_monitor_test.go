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
// Schedule tests
// ---------------------------------------------------------------------------

func TestSLAMonitor_Schedule_CreatesTwo(t *testing.T) {
	sched := &mockScheduler{}
	d := &lambda.Deps{
		Scheduler:          sched,
		SLAMonitorARN:      "arn:aws:lambda:us-east-1:123:function:sla-monitor",
		SchedulerRoleARN:   "arn:aws:iam::123:role/scheduler-role",
		SchedulerGroupName: "interlock-sla",
		Logger:             slog.Default(),
	}

	out, err := lambda.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
		Mode:             "schedule",
		PipelineID:       "gold-orders",
		ScheduleID:       "daily",
		Date:             "2026-06-15",
		Deadline:         "14:00",
		ExpectedDuration: "15m",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.WarningAt == "" || out.BreachAt == "" {
		t.Error("schedule should return warningAt and breachAt")
	}
	if len(sched.created) != 2 {
		t.Fatalf("expected 2 schedules created, got %d", len(sched.created))
	}
	// Verify schedule names
	if !strings.Contains(*sched.created[0].Name, "gold-orders") || !strings.Contains(*sched.created[0].Name, "warning") {
		t.Errorf("first schedule name %q should contain pipeline ID and 'warning'", *sched.created[0].Name)
	}
	if !strings.Contains(*sched.created[1].Name, "gold-orders") || !strings.Contains(*sched.created[1].Name, "breach") {
		t.Errorf("second schedule name %q should contain pipeline ID and 'breach'", *sched.created[1].Name)
	}
	// Verify schedule expressions are at() format
	if !strings.HasPrefix(*sched.created[0].ScheduleExpression, "at(") {
		t.Errorf("warning schedule expression %q should start with 'at('", *sched.created[0].ScheduleExpression)
	}
}

func TestSLAMonitor_Schedule_NoSchedulerConfigured(t *testing.T) {
	d := &lambda.Deps{
		Scheduler: nil,
		Logger:    slog.Default(),
	}

	out, err := lambda.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
		Mode:             "schedule",
		PipelineID:       "gold-orders",
		ScheduleID:       "daily",
		Date:             "2026-06-15",
		Deadline:         "14:00",
		ExpectedDuration: "15m",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should still return calculated deadlines even without scheduler
	if out.WarningAt == "" || out.BreachAt == "" {
		t.Error("schedule should return warningAt and breachAt even without scheduler")
	}
}

// ---------------------------------------------------------------------------
// Cancel tests
// ---------------------------------------------------------------------------

func TestSLAMonitor_Cancel_DeletesBoth(t *testing.T) {
	sched := &mockScheduler{}
	eb := &mockEventBridge{}
	d := &lambda.Deps{
		Scheduler:          sched,
		SchedulerGroupName: "interlock-sla",
		EventBridge:        eb,
		EventBusName:       "test-bus",
		Logger:             slog.Default(),
	}

	// Cancel with future deadlines — should be SLA_MET
	out, err := lambda.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
		Mode:       "cancel",
		PipelineID: "gold-orders",
		ScheduleID: "daily",
		Date:       "2026-06-15",
		WarningAt:  "2026-12-31T23:45:00Z",
		BreachAt:   "2026-12-31T23:59:00Z",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.AlertType != "SLA_MET" {
		t.Errorf("alertType = %q, want %q", out.AlertType, "SLA_MET")
	}
	if len(sched.deleted) != 2 {
		t.Fatalf("expected 2 schedules deleted, got %d", len(sched.deleted))
	}
	// Should publish SLA_MET event
	if len(eb.events) != 1 {
		t.Fatalf("expected 1 EventBridge call for SLA_MET, got %d", len(eb.events))
	}
	if *eb.events[0].Entries[0].DetailType != "SLA_MET" {
		t.Errorf("event detail type = %q, want %q", *eb.events[0].Entries[0].DetailType, "SLA_MET")
	}
}

func TestSLAMonitor_Cancel_PastBreach(t *testing.T) {
	sched := &mockScheduler{}
	eb := &mockEventBridge{}
	d := &lambda.Deps{
		Scheduler:          sched,
		SchedulerGroupName: "interlock-sla",
		EventBridge:        eb,
		EventBusName:       "test-bus",
		Logger:             slog.Default(),
	}

	// Cancel with past deadlines — should be SLA_BREACH
	out, err := lambda.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
		Mode:       "cancel",
		PipelineID: "gold-orders",
		ScheduleID: "daily",
		Date:       "2020-01-01",
		WarningAt:  "2020-01-01T00:50:00Z",
		BreachAt:   "2020-01-01T01:00:00Z",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.AlertType != "SLA_BREACH" {
		t.Errorf("alertType = %q, want %q", out.AlertType, "SLA_BREACH")
	}
	// Should NOT publish SLA_MET event (breach already fired by Scheduler)
	if len(eb.events) != 0 {
		t.Errorf("expected 0 EventBridge calls for past breach, got %d", len(eb.events))
	}
}

func TestSLAMonitor_Cancel_NoScheduler(t *testing.T) {
	eb := &mockEventBridge{}
	d := &lambda.Deps{
		Scheduler:    nil,
		EventBridge:  eb,
		EventBusName: "test-bus",
		Logger:       slog.Default(),
	}

	out, err := lambda.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
		Mode:       "cancel",
		PipelineID: "gold-orders",
		ScheduleID: "daily",
		Date:       "2026-06-15",
		WarningAt:  "2026-12-31T23:45:00Z",
		BreachAt:   "2026-12-31T23:59:00Z",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.AlertType != "SLA_MET" {
		t.Errorf("alertType = %q, want %q", out.AlertType, "SLA_MET")
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
// Reconcile tests
// ---------------------------------------------------------------------------

func TestSLAMonitor_Reconcile_SLAMet(t *testing.T) {
	// Use a deadline far in the future so "now" is always before it
	eb := &mockEventBridge{}
	d := &lambda.Deps{
		EventBridge:  eb,
		EventBusName: "test-bus",
		Logger:       slog.Default(),
	}

	out, err := lambda.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
		Mode:             "reconcile",
		PipelineID:       "silver-cdr-hour",
		ScheduleID:       "stream",
		Date:             "2026-12-31",
		Deadline:         "23:59",
		ExpectedDuration: "10m",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.AlertType != "SLA_MET" {
		t.Errorf("alertType = %q, want %q", out.AlertType, "SLA_MET")
	}
	if len(eb.events) != 0 {
		t.Errorf("expected 0 EventBridge calls for SLA_MET, got %d", len(eb.events))
	}
}

func TestSLAMonitor_Reconcile_Breach(t *testing.T) {
	// Use a deadline in the past so "now" is always after breach
	eb := &mockEventBridge{}
	d := &lambda.Deps{
		EventBridge:  eb,
		EventBusName: "test-bus",
		Logger:       slog.Default(),
	}

	out, err := lambda.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
		Mode:             "reconcile",
		PipelineID:       "silver-cdr-hour",
		ScheduleID:       "stream",
		Date:             "2020-01-01",
		Deadline:         "01:00",
		ExpectedDuration: "10m",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.AlertType != "SLA_BREACH" {
		t.Errorf("alertType = %q, want %q", out.AlertType, "SLA_BREACH")
	}
	// Should fire both warning and breach events
	if len(eb.events) != 2 {
		t.Errorf("expected 2 EventBridge calls for SLA_BREACH, got %d", len(eb.events))
	}
}

func TestSLAMonitor_Reconcile_ReturnsDeadlines(t *testing.T) {
	d := &lambda.Deps{Logger: slog.Default()}

	out, err := lambda.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
		Mode:             "reconcile",
		PipelineID:       "silver-cdr-hour",
		ScheduleID:       "stream",
		Date:             "2026-06-15",
		Deadline:         "14:00",
		ExpectedDuration: "15m",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.WarningAt == "" || out.BreachAt == "" {
		t.Error("reconcile should return warningAt and breachAt")
	}
	if out.FiredAt == "" {
		t.Error("reconcile should return firedAt")
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

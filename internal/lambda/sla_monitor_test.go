package lambda_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// ---------------------------------------------------------------------------
// Calculate tests
// ---------------------------------------------------------------------------

func TestSLAMonitor_Calculate_Basic(t *testing.T) {
	d := &lambda.Deps{Logger: slog.Default()}
	out, err := lambda.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
		Mode:             "calculate",
		PipelineID:       "gold-orders",
		Date:             "2030-03-01",
		Deadline:         "08:00",
		ExpectedDuration: "30m",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Expect full ISO 8601 timestamps for SFN TimestampPath
	if out.WarningAt != "2030-03-01T07:30:00Z" {
		t.Errorf("warningAt = %q, want %q", out.WarningAt, "2030-03-01T07:30:00Z")
	}
	if out.BreachAt != "2030-03-01T08:00:00Z" {
		t.Errorf("breachAt = %q, want %q", out.BreachAt, "2030-03-01T08:00:00Z")
	}
}

func TestSLAMonitor_Calculate_Midnight(t *testing.T) {
	d := &lambda.Deps{Logger: slog.Default()}
	out, err := lambda.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
		Mode:             "calculate",
		PipelineID:       "gold-orders",
		Date:             "2030-03-01",
		Deadline:         "00:30",
		ExpectedDuration: "1h",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Warning wraps to previous day
	if out.WarningAt != "2030-02-28T23:30:00Z" {
		t.Errorf("warningAt = %q, want %q", out.WarningAt, "2030-02-28T23:30:00Z")
	}
	if out.BreachAt != "2030-03-01T00:30:00Z" {
		t.Errorf("breachAt = %q, want %q", out.BreachAt, "2030-03-01T00:30:00Z")
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

func TestSLAMonitor_Calculate_DailyDeadlineRollsForward(t *testing.T) {
	// Daily pipeline with execution date in the past and a small-hour deadline.
	// The SLA deadline "02:00" for date 2026-03-04 means 2026-03-05T02:00:00Z
	// (next day) because 2026-03-04T02:00 is already past.
	d := &lambda.Deps{Logger: slog.Default()}
	out, err := lambda.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
		Mode:             "calculate",
		PipelineID:       "silver-cdr-day",
		Date:             "2024-01-15",
		Deadline:         "02:00",
		ExpectedDuration: "30m",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// breachAt should NOT be 2024-01-15T02:00:00Z (in the past).
	// It should roll forward by 24h to 2024-01-16T02:00:00Z.
	if out.BreachAt == "2024-01-15T02:00:00Z" {
		t.Errorf("breachAt = %q, should have rolled forward past now", out.BreachAt)
	}
	if out.BreachAt != "2024-01-16T02:00:00Z" {
		t.Errorf("breachAt = %q, want %q", out.BreachAt, "2024-01-16T02:00:00Z")
	}
	if out.WarningAt != "2024-01-16T01:30:00Z" {
		t.Errorf("warningAt = %q, want %q", out.WarningAt, "2024-01-16T01:30:00Z")
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
	// Verify warning payload includes BreachAt for suppression
	var warningPayload lambda.SLAMonitorInput
	if err := json.Unmarshal([]byte(*sched.created[0].Target.Input), &warningPayload); err != nil {
		t.Fatalf("unmarshal warning payload: %v", err)
	}
	if warningPayload.BreachAt == "" {
		t.Error("warning schedule payload should include breachAt for suppression")
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

func TestSLAMonitor_Cancel_RecalculatesWhenTimesNotProvided(t *testing.T) {
	sched := &mockScheduler{}
	eb := &mockEventBridge{}
	d := &lambda.Deps{
		Scheduler:          sched,
		SchedulerGroupName: "interlock-sla",
		EventBridge:        eb,
		EventBusName:       "test-bus",
		Logger:             slog.Default(),
	}

	// Cancel with deadline/expectedDuration instead of warningAt/breachAt.
	out, err := lambda.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
		Mode:             "cancel",
		PipelineID:       "silver-cdr-hour",
		ScheduleID:       "stream",
		Date:             "2026-12-31T23",
		Deadline:         ":30",
		ExpectedDuration: "10m",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.AlertType != "SLA_MET" {
		t.Errorf("alertType = %q, want %q", out.AlertType, "SLA_MET")
	}
	if out.WarningAt == "" || out.BreachAt == "" {
		t.Error("cancel with recalculation should return warningAt and breachAt")
	}
	if len(sched.deleted) != 2 {
		t.Fatalf("expected 2 schedules deleted, got %d", len(sched.deleted))
	}
	if len(eb.events) != 1 {
		t.Fatalf("expected 1 EventBridge call for SLA_MET, got %d", len(eb.events))
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

func TestSLAMonitor_FireAlert_WarningSuppressedPastBreach(t *testing.T) {
	// When fire-alert receives SLA_WARNING with a BreachAt in the past,
	// the warning should be suppressed (breach already fired or imminent).
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
		BreachAt:   "2020-01-01T00:00:00Z", // well in the past
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.AlertType != "SLA_WARNING" {
		t.Errorf("alertType = %q, want %q", out.AlertType, "SLA_WARNING")
	}
	// Warning should be suppressed — breach time already passed
	if len(eb.events) != 0 {
		t.Errorf("expected 0 EventBridge events (warning suppressed), got %d", len(eb.events))
	}
}

func TestSLAMonitor_FireAlert_WarningFiredBeforeBreach(t *testing.T) {
	// When fire-alert receives SLA_WARNING with a BreachAt in the future,
	// the warning should fire normally.
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
		BreachAt:   "2099-01-01T00:00:00Z", // far in the future
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.AlertType != "SLA_WARNING" {
		t.Errorf("alertType = %q, want %q", out.AlertType, "SLA_WARNING")
	}
	// Warning should fire normally — breach hasn't happened yet
	if len(eb.events) != 1 {
		t.Fatalf("expected 1 EventBridge event (warning fires), got %d", len(eb.events))
	}
	entry := eb.events[0].Entries[0]
	if *entry.DetailType != "SLA_WARNING" {
		t.Errorf("detail type = %q, want %q", *entry.DetailType, "SLA_WARNING")
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

func TestSLAMonitor_FireAlert_SuppressedWhenCompleted(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Seed a COMPLETED trigger for the pipeline/date.
	mock.putRaw(testControlTable, map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("gold-orders")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("daily", "2026-03-01")},
		"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusCompleted},
	})

	for _, alertType := range []string{"SLA_WARNING", "SLA_BREACH"} {
		out, err := lambda.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
			Mode:       "fire-alert",
			PipelineID: "gold-orders",
			ScheduleID: "daily",
			Date:       "2026-03-01",
			AlertType:  alertType,
		})
		if err != nil {
			t.Fatalf("unexpected error for %s: %v", alertType, err)
		}
		if out.AlertType != alertType {
			t.Errorf("alertType = %q, want %q", out.AlertType, alertType)
		}
		if out.FiredAt == "" {
			t.Errorf("firedAt should not be empty for suppressed %s", alertType)
		}
	}

	// No events should be published — both were suppressed.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	if len(ebMock.events) != 0 {
		t.Errorf("expected 0 EventBridge events (suppressed), got %d", len(ebMock.events))
	}
}

func TestSLAMonitor_FireAlert_SuppressedWhenFailedFinal(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Seed a FAILED_FINAL trigger for the pipeline/date.
	mock.putRaw(testControlTable, map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("gold-orders")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("daily", "2026-03-01")},
		"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusFailedFinal},
	})

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

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	if len(ebMock.events) != 0 {
		t.Errorf("expected 0 EventBridge events (suppressed), got %d", len(ebMock.events))
	}
}

func TestSLAMonitor_FireAlert_FiresWhenRunning(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Seed a RUNNING trigger — alert should fire normally.
	mock.putRaw(testControlTable, map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("gold-orders")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("daily", "2026-03-01")},
		"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusRunning},
	})

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

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	if len(ebMock.events) != 1 {
		t.Fatalf("expected 1 EventBridge event (fires normally), got %d", len(ebMock.events))
	}
	if *ebMock.events[0].Entries[0].DetailType != "SLA_BREACH" {
		t.Errorf("detail type = %q, want %q", *ebMock.events[0].Entries[0].DetailType, "SLA_BREACH")
	}
}

func TestSLAMonitor_FireAlert_FiresWhenNoTrigger(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// No trigger row exists — alert should fire (pipeline never started).
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

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	if len(ebMock.events) != 1 {
		t.Fatalf("expected 1 EventBridge event (fires normally), got %d", len(ebMock.events))
	}
}

func TestSLAMonitor_FireAlert_SuppressedByJoblogNoTrigger(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// No trigger row, but joblog shows success (cron pipeline or TTL-expired trigger).
	mock.putRaw("joblog", jobItem("gold-orders", "daily", "2026-03-01", types.JobEventSuccess))

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

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	if len(ebMock.events) != 0 {
		t.Errorf("expected 0 EventBridge events (suppressed by joblog), got %d", len(ebMock.events))
	}
}

func TestSLAMonitor_FireAlert_SuppressedByJoblogRunningTrigger(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Trigger still RUNNING but joblog shows success (race: status not yet updated).
	mock.putRaw(testControlTable, map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("gold-orders")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("daily", "2026-03-01")},
		"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusRunning},
	})
	mock.putRaw("joblog", jobItem("gold-orders", "daily", "2026-03-01", types.JobEventSuccess))

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

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	if len(ebMock.events) != 0 {
		t.Errorf("expected 0 EventBridge events (suppressed by joblog), got %d", len(ebMock.events))
	}
}

func TestSLAMonitor_FireAlert_NotSuppressedByInfraFailure(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Only infra-trigger-failure in joblog — should NOT suppress (still retrying).
	mock.putRaw("joblog", jobItem("gold-orders", "daily", "2026-03-01", types.JobEventInfraTriggerFailure))

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

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	if len(ebMock.events) != 1 {
		t.Fatalf("expected 1 EventBridge event (infra-failure is not terminal), got %d", len(ebMock.events))
	}
	_ = out
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
	// Should fire breach event only (warning suppressed — breach already past)
	if len(eb.events) != 1 {
		t.Errorf("expected 1 EventBridge call for SLA_BREACH, got %d", len(eb.events))
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
// Hourly date resolution tests
// ---------------------------------------------------------------------------

func TestSLACalculate_HourlyDate_RelativeDeadline(t *testing.T) {
	out, err := lambda.HandleSLAMonitor(context.Background(), &lambda.Deps{}, lambda.SLAMonitorInput{
		Mode:             "calculate",
		PipelineID:       "silver-cdr-hour",
		ScheduleID:       "stream",
		Date:             "2026-03-03T10",
		Deadline:         ":30",
		ExpectedDuration: "10m",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Hourly pipeline: data for hour 10 is processed in hour 11,
	// so ":30" means 11:30 (processing window), warning at 11:20.
	if !strings.Contains(out.BreachAt, "T11:30:00") {
		t.Errorf("breachAt = %q, want *T11:30:00*", out.BreachAt)
	}
	if !strings.Contains(out.WarningAt, "T11:20:00") {
		t.Errorf("warningAt = %q, want *T11:20:00*", out.WarningAt)
	}
}

func TestSLACalculate_HourlyDate_Hour23Rollover(t *testing.T) {
	// Hour 23 data is processed in hour 0 of the next day.
	out, err := lambda.HandleSLAMonitor(context.Background(), &lambda.Deps{}, lambda.SLAMonitorInput{
		Mode:             "calculate",
		PipelineID:       "silver-cdr-hour",
		ScheduleID:       "stream",
		Date:             "2026-03-03T23",
		Deadline:         ":30",
		ExpectedDuration: "10m",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// ":30" for hour 23 → breach at 2026-03-04T00:30:00Z (next day)
	if out.BreachAt != "2026-03-04T00:30:00Z" {
		t.Errorf("breachAt = %q, want %q", out.BreachAt, "2026-03-04T00:30:00Z")
	}
	if out.WarningAt != "2026-03-04T00:20:00Z" {
		t.Errorf("warningAt = %q, want %q", out.WarningAt, "2026-03-04T00:20:00Z")
	}
}

func TestSLACalculate_HourlyDate_AbsoluteDeadline(t *testing.T) {
	out, err := lambda.HandleSLAMonitor(context.Background(), &lambda.Deps{}, lambda.SLAMonitorInput{
		Mode:             "calculate",
		PipelineID:       "silver-cdr-hour",
		ScheduleID:       "stream",
		Date:             "2026-03-03T10",
		Deadline:         "11:00",
		ExpectedDuration: "15m",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out.BreachAt, "T11:00:00") {
		t.Errorf("breachAt = %q, want *T11:00:00*", out.BreachAt)
	}
}

func TestSLACalculate_DailyDate_Unchanged(t *testing.T) {
	out, err := lambda.HandleSLAMonitor(context.Background(), &lambda.Deps{}, lambda.SLAMonitorInput{
		Mode:             "calculate",
		PipelineID:       "silver-cdr-day",
		ScheduleID:       "cron",
		Date:             "2026-03-03",
		Deadline:         "02:00",
		ExpectedDuration: "30m",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out.BreachAt, "T02:00:00") {
		t.Errorf("breachAt = %q, want *T02:00:00*", out.BreachAt)
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

// ---------------------------------------------------------------------------
// Detail enrichment tests
// ---------------------------------------------------------------------------

func TestFireAlert_IncludesDetailInEvent(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// No trigger row and no joblog — pipeline never started.
	out, err := lambda.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
		Mode:       "fire-alert",
		PipelineID: "gold-orders",
		ScheduleID: "daily",
		Date:       "2026-03-01",
		AlertType:  "SLA_WARNING",
		BreachAt:   "2099-01-01T00:00:00Z", // far future so warning fires
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.AlertType != "SLA_WARNING" {
		t.Errorf("alertType = %q, want %q", out.AlertType, "SLA_WARNING")
	}

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	if len(ebMock.events) != 1 {
		t.Fatalf("expected 1 EventBridge event, got %d", len(ebMock.events))
	}

	// Parse the detail JSON to verify the Detail map.
	detailJSON := *ebMock.events[0].Entries[0].Detail
	var evt types.InterlockEvent
	if err := json.Unmarshal([]byte(detailJSON), &evt); err != nil {
		t.Fatalf("unmarshal event detail: %v", err)
	}

	if evt.Detail == nil {
		t.Fatal("expected Detail map to be present in event")
	}
	if s, ok := evt.Detail["status"].(string); !ok || s != "not started" {
		t.Errorf("detail.status = %v, want %q", evt.Detail["status"], "not started")
	}
	if s, ok := evt.Detail["source"].(string); !ok || s != "schedule" {
		t.Errorf("detail.source = %v, want %q", evt.Detail["source"], "schedule")
	}
	if s, ok := evt.Detail["actionHint"].(string); !ok || !strings.Contains(s, "check sensor data") {
		t.Errorf("detail.actionHint = %v, want to contain %q", evt.Detail["actionHint"], "check sensor data")
	}
	if _, ok := evt.Detail["breachAt"]; !ok {
		t.Error("detail.breachAt should be present when input.BreachAt is set")
	}
}

func TestFireAlert_RunningStatus_DetailHint(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Seed a RUNNING trigger.
	mock.putRaw(testControlTable, map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("gold-orders")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("daily", "2026-03-01")},
		"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusRunning},
	})

	_, err := lambda.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
		Mode:       "fire-alert",
		PipelineID: "gold-orders",
		ScheduleID: "daily",
		Date:       "2026-03-01",
		AlertType:  "SLA_WARNING",
		BreachAt:   "2099-01-01T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	if len(ebMock.events) != 1 {
		t.Fatalf("expected 1 EventBridge event, got %d", len(ebMock.events))
	}

	detailJSON := *ebMock.events[0].Entries[0].Detail
	var evt types.InterlockEvent
	if err := json.Unmarshal([]byte(detailJSON), &evt); err != nil {
		t.Fatalf("unmarshal event detail: %v", err)
	}

	if s, _ := evt.Detail["status"].(string); s != types.TriggerStatusRunning {
		t.Errorf("detail.status = %q, want %q", s, types.TriggerStatusRunning)
	}
	if s, _ := evt.Detail["actionHint"].(string); !strings.Contains(s, "may complete before breach") {
		t.Errorf("detail.actionHint = %q, want to contain %q", s, "may complete before breach")
	}
}

func TestFireAlert_BreachNotStarted_DetailHint(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// No trigger — pipeline not started, breach alert.
	_, err := lambda.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
		Mode:       "fire-alert",
		PipelineID: "gold-orders",
		ScheduleID: "daily",
		Date:       "2026-03-01",
		AlertType:  "SLA_BREACH",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	if len(ebMock.events) != 1 {
		t.Fatalf("expected 1 EventBridge event, got %d", len(ebMock.events))
	}

	detailJSON := *ebMock.events[0].Entries[0].Detail
	var evt types.InterlockEvent
	if err := json.Unmarshal([]byte(detailJSON), &evt); err != nil {
		t.Fatalf("unmarshal event detail: %v", err)
	}

	if s, _ := evt.Detail["actionHint"].(string); !strings.Contains(s, "investigate trigger") {
		t.Errorf("detail.actionHint = %q, want to contain %q", s, "investigate trigger")
	}
}

func TestReconcile_IncludesDetailInEvent(t *testing.T) {
	eb := &mockEventBridge{}
	d := &lambda.Deps{
		EventBridge:  eb,
		EventBusName: "test-bus",
		Logger:       slog.Default(),
	}

	// Past breach — reconcile should fire SLA_BREACH with detail.
	_, err := lambda.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
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

	if len(eb.events) != 1 {
		t.Fatalf("expected 1 EventBridge event, got %d", len(eb.events))
	}

	detailJSON := *eb.events[0].Entries[0].Detail
	var evt types.InterlockEvent
	if err := json.Unmarshal([]byte(detailJSON), &evt); err != nil {
		t.Fatalf("unmarshal event detail: %v", err)
	}

	if evt.Detail == nil {
		t.Fatal("expected Detail map to be present in reconcile event")
	}
	if s, _ := evt.Detail["source"].(string); s != "reconciliation" {
		t.Errorf("detail.source = %q, want %q", s, "reconciliation")
	}
	if s, _ := evt.Detail["actionHint"].(string); !strings.Contains(s, "Scheduler health") {
		t.Errorf("detail.actionHint = %q, want to contain %q", s, "Scheduler health")
	}
	if _, ok := evt.Detail["warningAt"]; !ok {
		t.Error("detail.warningAt should be present in reconcile event")
	}
	if _, ok := evt.Detail["breachAt"]; !ok {
		t.Error("detail.breachAt should be present in reconcile event")
	}
}

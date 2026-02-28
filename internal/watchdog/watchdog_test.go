package watchdog

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/dwsmith1983/interlock/internal/testutil"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// monitoringMock wraps testutil.MockProvider to track PutRunLog calls,
// used by completed-monitoring expiry tests that assert on call counts.
type monitoringMock struct {
	*testutil.MockProvider
	mu             sync.Mutex
	putRunLogCalls []types.RunLogEntry
}

func (m *monitoringMock) PutRunLog(ctx context.Context, entry types.RunLogEntry) error {
	m.mu.Lock()
	m.putRunLogCalls = append(m.putRunLogCalls, entry)
	m.mu.Unlock()
	return m.MockProvider.PutRunLog(ctx, entry)
}

// ---------------------------------------------------------------------------
// Helper builders
// ---------------------------------------------------------------------------

func boolPtr(v bool) *bool { return &v }

func pipelineWithMonitoring(duration string) types.PipelineConfig {
	return types.PipelineConfig{
		Name:    "p1",
		Trigger: &types.TriggerConfig{Type: types.TriggerHTTP},
		Watch: &types.PipelineWatchConfig{
			Monitoring: &types.MonitoringConfig{
				Enabled:  true,
				Duration: duration,
			},
		},
	}
}

func pipelineWithDeadline(deadline string) types.PipelineConfig {
	return types.PipelineConfig{
		Name:    "p1",
		Trigger: &types.TriggerConfig{Type: types.TriggerHTTP},
		SLA:     &types.SLAConfig{EvaluationDeadline: deadline},
	}
}

func collectAlerts(t *testing.T) (alertFn func(context.Context, types.Alert), getAlerts func() []types.Alert) {
	t.Helper()
	var mu sync.Mutex
	var alerts []types.Alert
	return func(_ context.Context, a types.Alert) {
			mu.Lock()
			alerts = append(alerts, a)
			mu.Unlock()
		}, func() []types.Alert {
			mu.Lock()
			defer mu.Unlock()
			return alerts
		}
}

func mustPutRunLog(t *testing.T, p interface {
	PutRunLog(context.Context, types.RunLogEntry) error
}, entry types.RunLogEntry) {
	t.Helper()
	if err := p.PutRunLog(context.Background(), entry); err != nil {
		t.Fatalf("PutRunLog: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestNoRunLog_FiresAlert(t *testing.T) {
	mp := testutil.NewMockProvider()

	alertFn, getAlerts := collectAlerts(t)
	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:10:00Z")

	missed := CheckMissedSchedules(context.Background(), CheckOptions{
		Provider:  mp,
		Pipelines: []types.PipelineConfig{pipelineWithDeadline("10:00")},
		AlertFn:   alertFn,
		Logger:    slog.Default(),
		Now:       now,
	})

	if len(missed) != 1 {
		t.Fatalf("expected 1 missed schedule, got %d", len(missed))
	}
	if missed[0].PipelineID != "p1" {
		t.Errorf("expected pipeline p1, got %s", missed[0].PipelineID)
	}
	alerts := getAlerts()
	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(alerts))
	}
	if alerts[0].Level != types.AlertLevelError {
		t.Errorf("expected error level, got %s", alerts[0].Level)
	}
	if alerts[0].Category != "schedule_missed" {
		t.Errorf("expected category schedule_missed, got %s", alerts[0].Category)
	}
}

func TestRunLogExists_NoAlert(t *testing.T) {
	mp := testutil.NewMockProvider()
	mustPutRunLog(t, mp, types.RunLogEntry{
		PipelineID: "p1", Date: "2026-02-24", ScheduleID: "daily",
		Status: types.RunPending,
	})

	alertFn, getAlerts := collectAlerts(t)
	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:30:00Z")

	missed := CheckMissedSchedules(context.Background(), CheckOptions{
		Provider:  mp,
		Pipelines: []types.PipelineConfig{pipelineWithDeadline("10:00")},
		AlertFn:   alertFn,
		Logger:    slog.Default(),
		Now:       now,
	})

	if len(missed) != 0 {
		t.Fatalf("expected 0 missed schedules, got %d", len(missed))
	}
	if len(getAlerts()) != 0 {
		t.Error("expected no alerts")
	}
}

func TestFailedRunLog_NoAlert(t *testing.T) {
	mp := testutil.NewMockProvider()
	mustPutRunLog(t, mp, types.RunLogEntry{
		PipelineID: "p1", Date: "2026-02-24", ScheduleID: "daily",
		Status: types.RunFailed,
	})

	alertFn, getAlerts := collectAlerts(t)
	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:30:00Z")

	missed := CheckMissedSchedules(context.Background(), CheckOptions{
		Provider:  mp,
		Pipelines: []types.PipelineConfig{pipelineWithDeadline("10:00")},
		AlertFn:   alertFn,
		Logger:    slog.Default(),
		Now:       now,
	})

	if len(missed) != 0 {
		t.Fatalf("expected 0 missed, got %d", len(missed))
	}
	if len(getAlerts()) != 0 {
		t.Error("expected no alerts for failed run log")
	}
}

func TestDeadlineNotPassed_NoAlert(t *testing.T) {
	mp := testutil.NewMockProvider()

	alertFn, getAlerts := collectAlerts(t)
	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:30:00Z")

	missed := CheckMissedSchedules(context.Background(), CheckOptions{
		Provider:  mp,
		Pipelines: []types.PipelineConfig{pipelineWithDeadline("15:00")},
		AlertFn:   alertFn,
		Logger:    slog.Default(),
		Now:       now,
	})

	if len(missed) != 0 {
		t.Fatalf("expected 0 missed, got %d", len(missed))
	}
	if len(getAlerts()) != 0 {
		t.Error("expected no alerts before deadline")
	}
}

func TestExcludedDay_NoAlert(t *testing.T) {
	mp := testutil.NewMockProvider()
	// 2026-02-24 is a Tuesday
	pl := pipelineWithDeadline("10:00")
	pl.Exclusions = &types.ExclusionConfig{Days: []string{"tuesday"}}

	alertFn, getAlerts := collectAlerts(t)
	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:30:00Z")

	missed := CheckMissedSchedules(context.Background(), CheckOptions{
		Provider:  mp,
		Pipelines: []types.PipelineConfig{pl},
		AlertFn:   alertFn,
		Logger:    slog.Default(),
		Now:       now,
	})

	if len(missed) != 0 {
		t.Fatalf("expected 0 missed, got %d", len(missed))
	}
	if len(getAlerts()) != 0 {
		t.Error("expected no alerts on excluded day")
	}
}

func TestWatchDisabled_NoAlert(t *testing.T) {
	mp := testutil.NewMockProvider()
	pl := pipelineWithDeadline("10:00")
	pl.Watch = &types.PipelineWatchConfig{Enabled: boolPtr(false)}

	alertFn, getAlerts := collectAlerts(t)
	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:30:00Z")

	missed := CheckMissedSchedules(context.Background(), CheckOptions{
		Provider:  mp,
		Pipelines: []types.PipelineConfig{pl},
		AlertFn:   alertFn,
		Logger:    slog.Default(),
		Now:       now,
	})

	if len(missed) != 0 {
		t.Fatalf("expected 0 missed, got %d", len(missed))
	}
	if len(getAlerts()) != 0 {
		t.Error("expected no alerts when watch disabled")
	}
}

func TestNoDeadlineConfigured_NoAlert(t *testing.T) {
	mp := testutil.NewMockProvider()

	alertFn, getAlerts := collectAlerts(t)
	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:30:00Z")

	missed := CheckMissedSchedules(context.Background(), CheckOptions{
		Provider: mp,
		Pipelines: []types.PipelineConfig{{
			Name:    "p1",
			Trigger: &types.TriggerConfig{Type: types.TriggerHTTP},
		}},
		AlertFn: alertFn,
		Logger:  slog.Default(),
		Now:     now,
	})

	if len(missed) != 0 {
		t.Fatalf("expected 0 missed, got %d", len(missed))
	}
	if len(getAlerts()) != 0 {
		t.Error("expected no alerts with no deadline")
	}
}

func TestScheduleDeadlinePrecedence(t *testing.T) {
	mp := testutil.NewMockProvider()
	// Schedule deadline at 14:00, SLA at 10:00. Now at 12:00.
	// Schedule deadline hasn't passed yet (14:00) so no alert,
	// even though SLA deadline (10:00) has passed.
	pl := types.PipelineConfig{
		Name:    "p1",
		Trigger: &types.TriggerConfig{Type: types.TriggerHTTP},
		SLA:     &types.SLAConfig{EvaluationDeadline: "10:00"},
		Schedules: []types.ScheduleConfig{
			{Name: "h14", Deadline: "14:00"},
		},
	}

	alertFn, getAlerts := collectAlerts(t)
	now, _ := time.Parse(time.RFC3339, "2026-02-24T12:00:00Z")

	missed := CheckMissedSchedules(context.Background(), CheckOptions{
		Provider:  mp,
		Pipelines: []types.PipelineConfig{pl},
		AlertFn:   alertFn,
		Logger:    slog.Default(),
		Now:       now,
	})

	if len(missed) != 0 {
		t.Fatalf("expected 0 missed (schedule deadline not passed), got %d", len(missed))
	}
	if len(getAlerts()) != 0 {
		t.Error("expected no alerts — schedule deadline takes precedence")
	}
}

func TestMultiScheduleIndependent(t *testing.T) {
	mp := testutil.NewMockProvider()
	pl := types.PipelineConfig{
		Name:    "p1",
		Trigger: &types.TriggerConfig{Type: types.TriggerHTTP},
		Schedules: []types.ScheduleConfig{
			{Name: "h10", Deadline: "10:30"},
			{Name: "h14", Deadline: "14:30"},
		},
	}
	// h10 has a run log entry; h14 does not.
	mustPutRunLog(t, mp, types.RunLogEntry{
		PipelineID: "p1", Date: "2026-02-24", ScheduleID: "h10",
		Status: types.RunCompleted,
	})

	alertFn, getAlerts := collectAlerts(t)
	now, _ := time.Parse(time.RFC3339, "2026-02-24T14:40:00Z")

	missed := CheckMissedSchedules(context.Background(), CheckOptions{
		Provider:  mp,
		Pipelines: []types.PipelineConfig{pl},
		AlertFn:   alertFn,
		Logger:    slog.Default(),
		Now:       now,
	})

	if len(missed) != 1 {
		t.Fatalf("expected 1 missed (h14 only), got %d", len(missed))
	}
	if missed[0].ScheduleID != "h14" {
		t.Errorf("expected schedule h14, got %s", missed[0].ScheduleID)
	}
	if len(getAlerts()) != 1 {
		t.Errorf("expected 1 alert, got %d", len(getAlerts()))
	}
}

func TestDedup_SecondCallNoAlert(t *testing.T) {
	mp := testutil.NewMockProvider()

	alertFn, getAlerts := collectAlerts(t)
	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:10:00Z")

	opts := CheckOptions{
		Provider:  mp,
		Pipelines: []types.PipelineConfig{pipelineWithDeadline("10:00")},
		AlertFn:   alertFn,
		Logger:    slog.Default(),
		Now:       now,
	}

	// First call should fire alert.
	missed1 := CheckMissedSchedules(context.Background(), opts)
	if len(missed1) != 1 {
		t.Fatalf("first call: expected 1 missed, got %d", len(missed1))
	}

	// Second call — lock already held, should produce no alert.
	missed2 := CheckMissedSchedules(context.Background(), opts)
	if len(missed2) != 0 {
		t.Fatalf("second call: expected 0 missed (dedup), got %d", len(missed2))
	}

	alerts := getAlerts()
	if len(alerts) != 1 {
		t.Errorf("expected exactly 1 alert across both calls, got %d", len(alerts))
	}
}

func TestHistoricalDeadline_NoAlert(t *testing.T) {
	mp := testutil.NewMockProvider()

	alertFn, getAlerts := collectAlerts(t)
	// Deadline was 2 hours ago — well outside the default 15m lookback.
	now, _ := time.Parse(time.RFC3339, "2026-02-24T12:00:00Z")

	missed := CheckMissedSchedules(context.Background(), CheckOptions{
		Provider:  mp,
		Pipelines: []types.PipelineConfig{pipelineWithDeadline("10:00")},
		AlertFn:   alertFn,
		Logger:    slog.Default(),
		Now:       now,
	})

	if len(missed) != 0 {
		t.Fatalf("expected 0 missed (historical deadline outside lookback), got %d", len(missed))
	}
	if len(getAlerts()) != 0 {
		t.Error("expected no alerts for historical deadline")
	}
}

// ---------------------------------------------------------------------------
// Stuck-run detection tests
// ---------------------------------------------------------------------------

func TestStuckRun_RunningPastThreshold(t *testing.T) {
	mp := testutil.NewMockProvider()
	pipelines := []types.PipelineConfig{{
		Name:    "p1",
		Trigger: &types.TriggerConfig{Type: types.TriggerHTTP},
	}}
	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:30:00Z")
	mustPutRunLog(t, mp, types.RunLogEntry{
		PipelineID: "p1", Date: "2026-02-24", ScheduleID: "daily",
		Status:    types.RunRunning,
		RunID:     "run-1",
		UpdatedAt: now.Add(-45 * time.Minute), // 45 min ago
	})

	alertFn, getAlerts := collectAlerts(t)
	stuck := CheckStuckRuns(context.Background(), CheckOptions{
		Provider:  mp,
		Pipelines: pipelines,
		AlertFn:   alertFn,
		Logger:    slog.Default(),
		Now:       now,
	})

	if len(stuck) != 1 {
		t.Fatalf("expected 1 stuck run, got %d", len(stuck))
	}
	if stuck[0].PipelineID != "p1" {
		t.Errorf("expected pipeline p1, got %s", stuck[0].PipelineID)
	}
	if stuck[0].Status != types.RunRunning {
		t.Errorf("expected RUNNING status, got %s", stuck[0].Status)
	}
	alerts := getAlerts()
	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(alerts))
	}
	if alerts[0].Category != "stuck_run" {
		t.Errorf("expected category stuck_run, got %s", alerts[0].Category)
	}
}

func TestStuckRun_NotYetStuck(t *testing.T) {
	mp := testutil.NewMockProvider()
	pipelines := []types.PipelineConfig{{
		Name:    "p1",
		Trigger: &types.TriggerConfig{Type: types.TriggerHTTP},
	}}
	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:30:00Z")
	mustPutRunLog(t, mp, types.RunLogEntry{
		PipelineID: "p1", Date: "2026-02-24", ScheduleID: "daily",
		Status:    types.RunRunning,
		UpdatedAt: now.Add(-10 * time.Minute), // only 10 min ago
	})

	alertFn, getAlerts := collectAlerts(t)
	stuck := CheckStuckRuns(context.Background(), CheckOptions{
		Provider:  mp,
		Pipelines: pipelines,
		AlertFn:   alertFn,
		Logger:    slog.Default(),
		Now:       now,
	})

	if len(stuck) != 0 {
		t.Fatalf("expected 0 stuck runs, got %d", len(stuck))
	}
	if len(getAlerts()) != 0 {
		t.Error("expected no alerts for run not yet stuck")
	}
}

func TestStuckRun_CompletedRun_NotStuck(t *testing.T) {
	mp := testutil.NewMockProvider()
	pipelines := []types.PipelineConfig{{
		Name:    "p1",
		Trigger: &types.TriggerConfig{Type: types.TriggerHTTP},
	}}
	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:30:00Z")
	mustPutRunLog(t, mp, types.RunLogEntry{
		PipelineID: "p1", Date: "2026-02-24", ScheduleID: "daily",
		Status:    types.RunCompleted,
		UpdatedAt: now.Add(-60 * time.Minute),
	})

	alertFn, getAlerts := collectAlerts(t)
	stuck := CheckStuckRuns(context.Background(), CheckOptions{
		Provider:  mp,
		Pipelines: pipelines,
		AlertFn:   alertFn,
		Logger:    slog.Default(),
		Now:       now,
	})

	if len(stuck) != 0 {
		t.Fatalf("expected 0 stuck runs, got %d", len(stuck))
	}
	if len(getAlerts()) != 0 {
		t.Error("expected no alerts for completed run")
	}
}

func TestStuckRun_NoRunLog(t *testing.T) {
	mp := testutil.NewMockProvider()
	pipelines := []types.PipelineConfig{{
		Name:    "p1",
		Trigger: &types.TriggerConfig{Type: types.TriggerHTTP},
	}}

	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:30:00Z")
	alertFn, getAlerts := collectAlerts(t)
	stuck := CheckStuckRuns(context.Background(), CheckOptions{
		Provider:  mp,
		Pipelines: pipelines,
		AlertFn:   alertFn,
		Logger:    slog.Default(),
		Now:       now,
	})

	if len(stuck) != 0 {
		t.Fatalf("expected 0 stuck runs (no run log), got %d", len(stuck))
	}
	if len(getAlerts()) != 0 {
		t.Error("expected no alerts when no run log exists")
	}
}

func TestStuckRun_Dedup(t *testing.T) {
	mp := testutil.NewMockProvider()
	pipelines := []types.PipelineConfig{{
		Name:    "p1",
		Trigger: &types.TriggerConfig{Type: types.TriggerHTTP},
	}}
	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:30:00Z")
	mustPutRunLog(t, mp, types.RunLogEntry{
		PipelineID: "p1", Date: "2026-02-24", ScheduleID: "daily",
		Status:    types.RunRunning,
		RunID:     "run-1",
		UpdatedAt: now.Add(-45 * time.Minute),
	})

	alertFn, getAlerts := collectAlerts(t)
	opts := CheckOptions{
		Provider:  mp,
		Pipelines: pipelines,
		AlertFn:   alertFn,
		Logger:    slog.Default(),
		Now:       now,
	}

	stuck1 := CheckStuckRuns(context.Background(), opts)
	if len(stuck1) != 1 {
		t.Fatalf("first call: expected 1 stuck, got %d", len(stuck1))
	}

	stuck2 := CheckStuckRuns(context.Background(), opts)
	if len(stuck2) != 0 {
		t.Fatalf("second call: expected 0 stuck (dedup), got %d", len(stuck2))
	}

	if len(getAlerts()) != 1 {
		t.Errorf("expected exactly 1 alert across both calls, got %d", len(getAlerts()))
	}
}

func TestStuckRun_PendingStatus(t *testing.T) {
	mp := testutil.NewMockProvider()
	pipelines := []types.PipelineConfig{{
		Name:    "p1",
		Trigger: &types.TriggerConfig{Type: types.TriggerHTTP},
	}}
	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:30:00Z")
	mustPutRunLog(t, mp, types.RunLogEntry{
		PipelineID: "p1", Date: "2026-02-24", ScheduleID: "daily",
		Status:    types.RunPending,
		RunID:     "run-1",
		UpdatedAt: now.Add(-40 * time.Minute),
	})

	alertFn, getAlerts := collectAlerts(t)
	stuck := CheckStuckRuns(context.Background(), CheckOptions{
		Provider:  mp,
		Pipelines: pipelines,
		AlertFn:   alertFn,
		Logger:    slog.Default(),
		Now:       now,
	})

	if len(stuck) != 1 {
		t.Fatalf("expected 1 stuck run (PENDING), got %d", len(stuck))
	}
	if stuck[0].Status != types.RunPending {
		t.Errorf("expected PENDING status, got %s", stuck[0].Status)
	}
	if len(getAlerts()) != 1 {
		t.Errorf("expected 1 alert, got %d", len(getAlerts()))
	}
}

// ---------------------------------------------------------------------------
// Completed-monitoring expiry tests
// ---------------------------------------------------------------------------

func TestCheckCompletedMonitoring_Expired(t *testing.T) {
	mp := &monitoringMock{MockProvider: testutil.NewMockProvider()}
	pipelines := []types.PipelineConfig{pipelineWithMonitoring("20m")}

	now, _ := time.Parse(time.RFC3339, "2026-02-24T11:00:00Z")
	// Run entered COMPLETED_MONITORING 25 minutes ago (past the 20m window).
	mustPutRunLog(t, mp.MockProvider, types.RunLogEntry{
		PipelineID: "p1",
		Date:       "2026-02-24",
		ScheduleID: "daily",
		Status:     types.RunCompletedMonitoring,
		RunID:      "run-1",
		UpdatedAt:  now.Add(-25 * time.Minute),
	})

	results := CheckCompletedMonitoring(context.Background(), CheckOptions{
		Provider:  mp,
		Pipelines: pipelines,
		Logger:    slog.Default(),
		Now:       now,
	})

	if len(results) != 1 {
		t.Fatalf("expected 1 monitoring result, got %d", len(results))
	}
	if results[0].Action != "expired" {
		t.Errorf("expected action 'expired', got %s", results[0].Action)
	}
	if results[0].PipelineID != "p1" {
		t.Errorf("expected pipeline p1, got %s", results[0].PipelineID)
	}

	// Verify run was transitioned to COMPLETED.
	mp.mu.Lock()
	defer mp.mu.Unlock()
	if len(mp.putRunLogCalls) != 1 {
		t.Fatalf("expected 1 PutRunLog call, got %d", len(mp.putRunLogCalls))
	}
	if mp.putRunLogCalls[0].Status != types.RunCompleted {
		t.Errorf("expected COMPLETED status in PutRunLog, got %s", mp.putRunLogCalls[0].Status)
	}
	// Verify audit event.
	events := mp.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Kind != types.EventMonitoringCompleted {
		t.Errorf("expected MONITORING_COMPLETED event, got %s", events[0].Kind)
	}
}

func TestCheckCompletedMonitoring_StillInWindow(t *testing.T) {
	mp := &monitoringMock{MockProvider: testutil.NewMockProvider()}
	pipelines := []types.PipelineConfig{pipelineWithMonitoring("20m")}

	now, _ := time.Parse(time.RFC3339, "2026-02-24T11:00:00Z")
	// Run entered COMPLETED_MONITORING only 10 minutes ago (within 20m window).
	mustPutRunLog(t, mp.MockProvider, types.RunLogEntry{
		PipelineID: "p1",
		Date:       "2026-02-24",
		ScheduleID: "daily",
		Status:     types.RunCompletedMonitoring,
		RunID:      "run-1",
		UpdatedAt:  now.Add(-10 * time.Minute),
	})

	results := CheckCompletedMonitoring(context.Background(), CheckOptions{
		Provider:  mp,
		Pipelines: pipelines,
		Logger:    slog.Default(),
		Now:       now,
	})

	if len(results) != 0 {
		t.Fatalf("expected 0 results (still in window), got %d", len(results))
	}
	// Verify no PutRunLog call was made.
	mp.mu.Lock()
	defer mp.mu.Unlock()
	if len(mp.putRunLogCalls) != 0 {
		t.Errorf("expected 0 PutRunLog calls, got %d", len(mp.putRunLogCalls))
	}
}

func TestCheckCompletedMonitoring_IgnoresCompleted(t *testing.T) {
	mp := &monitoringMock{MockProvider: testutil.NewMockProvider()}
	pipelines := []types.PipelineConfig{pipelineWithMonitoring("20m")}

	now, _ := time.Parse(time.RFC3339, "2026-02-24T11:00:00Z")
	// Run is already COMPLETED — should be ignored.
	mustPutRunLog(t, mp.MockProvider, types.RunLogEntry{
		PipelineID: "p1",
		Date:       "2026-02-24",
		ScheduleID: "daily",
		Status:     types.RunCompleted,
		RunID:      "run-1",
		UpdatedAt:  now.Add(-60 * time.Minute),
	})

	results := CheckCompletedMonitoring(context.Background(), CheckOptions{
		Provider:  mp,
		Pipelines: pipelines,
		Logger:    slog.Default(),
		Now:       now,
	})

	if len(results) != 0 {
		t.Fatalf("expected 0 results (already completed), got %d", len(results))
	}
	mp.mu.Lock()
	defer mp.mu.Unlock()
	if len(mp.putRunLogCalls) != 0 {
		t.Errorf("expected 0 PutRunLog calls, got %d", len(mp.putRunLogCalls))
	}
}

func TestStuckRun_CustomThreshold(t *testing.T) {
	mp := testutil.NewMockProvider()
	pipelines := []types.PipelineConfig{{
		Name:    "p1",
		Trigger: &types.TriggerConfig{Type: types.TriggerHTTP},
	}}
	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:30:00Z")
	mustPutRunLog(t, mp, types.RunLogEntry{
		PipelineID: "p1", Date: "2026-02-24", ScheduleID: "daily",
		Status:    types.RunRunning,
		RunID:     "run-1",
		UpdatedAt: now.Add(-20 * time.Minute), // 20 min ago
	})

	alertFn, getAlerts := collectAlerts(t)

	// Default 30m threshold: not stuck
	stuck := CheckStuckRuns(context.Background(), CheckOptions{
		Provider:  mp,
		Pipelines: pipelines,
		AlertFn:   alertFn,
		Logger:    slog.Default(),
		Now:       now,
	})
	if len(stuck) != 0 {
		t.Fatalf("expected 0 stuck with default threshold, got %d", len(stuck))
	}

	// Custom 15m threshold: stuck
	stuck = CheckStuckRuns(context.Background(), CheckOptions{
		Provider:          mp,
		Pipelines:         pipelines,
		AlertFn:           alertFn,
		Logger:            slog.Default(),
		Now:               now,
		StuckRunThreshold: 15 * time.Minute,
	})
	if len(stuck) != 1 {
		t.Fatalf("expected 1 stuck with 15m threshold, got %d", len(stuck))
	}
	if len(getAlerts()) != 1 {
		t.Errorf("expected 1 alert, got %d", len(getAlerts()))
	}
}

func TestStuckRun_WatchDisabled(t *testing.T) {
	mp := testutil.NewMockProvider()
	pipelines := []types.PipelineConfig{{
		Name:    "p1",
		Trigger: &types.TriggerConfig{Type: types.TriggerHTTP},
		Watch:   &types.PipelineWatchConfig{Enabled: boolPtr(false)},
	}}
	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:30:00Z")
	mustPutRunLog(t, mp, types.RunLogEntry{
		PipelineID: "p1", Date: "2026-02-24", ScheduleID: "daily",
		Status:    types.RunRunning,
		UpdatedAt: now.Add(-60 * time.Minute),
	})

	alertFn, getAlerts := collectAlerts(t)
	stuck := CheckStuckRuns(context.Background(), CheckOptions{
		Provider:  mp,
		Pipelines: pipelines,
		AlertFn:   alertFn,
		Logger:    slog.Default(),
		Now:       now,
	})

	if len(stuck) != 0 {
		t.Fatalf("expected 0 stuck when watch disabled, got %d", len(stuck))
	}
	if len(getAlerts()) != 0 {
		t.Error("expected no alerts when watch disabled")
	}
}

// ---------------------------------------------------------------------------
// Failed-run retrigger tests
// ---------------------------------------------------------------------------

func TestCheckFailedRuns_Retrigger(t *testing.T) {
	mp := testutil.NewMockProvider()
	pipelines := []types.PipelineConfig{{
		Name:    "p1",
		Trigger: &types.TriggerConfig{Type: types.TriggerHTTP},
		Retry: &types.RetryPolicy{
			MaxAttempts:       3,
			RetryableFailures: []types.FailureCategory{types.FailureTimeout},
		},
	}}
	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:30:00Z")
	mustPutRunLog(t, mp, types.RunLogEntry{
		PipelineID:      "p1",
		Date:            "2026-02-24",
		ScheduleID:      "daily",
		Status:          types.RunFailed,
		AttemptNumber:   1,
		FailureCategory: types.FailureTimeout,
		RunID:           "run-1",
	})

	alertFn, getAlerts := collectAlerts(t)
	var retriggerCalls []string
	retriggerFn := func(_ context.Context, pipelineID, scheduleID string) error {
		retriggerCalls = append(retriggerCalls, pipelineID+":"+scheduleID)
		return nil
	}

	result := CheckFailedRuns(context.Background(), CheckOptions{
		Provider:    mp,
		Pipelines:   pipelines,
		AlertFn:     alertFn,
		Logger:      slog.Default(),
		Now:         now,
		RetriggerFn: retriggerFn,
	})

	if len(result) != 1 {
		t.Fatalf("expected 1 retriggered run, got %d", len(result))
	}
	if result[0].PipelineID != "p1" {
		t.Errorf("expected pipeline p1, got %s", result[0].PipelineID)
	}
	if result[0].Attempt != 2 {
		t.Errorf("expected attempt 2, got %d", result[0].Attempt)
	}
	if len(retriggerCalls) != 1 || retriggerCalls[0] != "p1:daily" {
		t.Errorf("expected retrigger call for p1:daily, got %v", retriggerCalls)
	}
	alerts := getAlerts()
	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(alerts))
	}
	if alerts[0].Category != "watchdog_retrigger" {
		t.Errorf("expected category watchdog_retrigger, got %s", alerts[0].Category)
	}
}

func TestCheckFailedRuns_NilRetriggerFn(t *testing.T) {
	mp := testutil.NewMockProvider()
	pipelines := []types.PipelineConfig{{
		Name:    "p1",
		Trigger: &types.TriggerConfig{Type: types.TriggerHTTP},
		Retry:   &types.RetryPolicy{MaxAttempts: 3},
	}}
	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:30:00Z")
	mustPutRunLog(t, mp, types.RunLogEntry{
		PipelineID: "p1",
		Date:       "2026-02-24",
		ScheduleID: "daily",
		Status:     types.RunFailed,
	})

	result := CheckFailedRuns(context.Background(), CheckOptions{
		Provider:  mp,
		Pipelines: pipelines,
		Logger:    slog.Default(),
		Now:       now,
		// RetriggerFn is nil
	})

	if len(result) != 0 {
		t.Fatalf("expected 0 retriggered runs with nil RetriggerFn, got %d", len(result))
	}
}

func TestCheckFailedRuns_MaxAttempts(t *testing.T) {
	mp := testutil.NewMockProvider()
	pipelines := []types.PipelineConfig{{
		Name:    "p1",
		Trigger: &types.TriggerConfig{Type: types.TriggerHTTP},
		Retry: &types.RetryPolicy{
			MaxAttempts:       2,
			RetryableFailures: []types.FailureCategory{types.FailureTimeout},
		},
	}}
	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:30:00Z")
	mustPutRunLog(t, mp, types.RunLogEntry{
		PipelineID:      "p1",
		Date:            "2026-02-24",
		ScheduleID:      "daily",
		Status:          types.RunFailed,
		AttemptNumber:   2, // Already at max
		FailureCategory: types.FailureTimeout,
	})

	retriggerFn := func(_ context.Context, _, _ string) error { return nil }
	result := CheckFailedRuns(context.Background(), CheckOptions{
		Provider:    mp,
		Pipelines:   pipelines,
		Logger:      slog.Default(),
		Now:         now,
		RetriggerFn: retriggerFn,
	})

	if len(result) != 0 {
		t.Fatalf("expected 0 retriggered runs (max attempts reached), got %d", len(result))
	}
}

func TestCheckFailedRuns_NonRetryableCategory(t *testing.T) {
	mp := testutil.NewMockProvider()
	pipelines := []types.PipelineConfig{{
		Name:    "p1",
		Trigger: &types.TriggerConfig{Type: types.TriggerHTTP},
		Retry: &types.RetryPolicy{
			MaxAttempts:       3,
			RetryableFailures: []types.FailureCategory{types.FailureTransient},
		},
	}}
	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:30:00Z")
	mustPutRunLog(t, mp, types.RunLogEntry{
		PipelineID:      "p1",
		Date:            "2026-02-24",
		ScheduleID:      "daily",
		Status:          types.RunFailed,
		AttemptNumber:   1,
		FailureCategory: types.FailurePermanent, // Not in retryable list
	})

	retriggerFn := func(_ context.Context, _, _ string) error { return nil }
	result := CheckFailedRuns(context.Background(), CheckOptions{
		Provider:    mp,
		Pipelines:   pipelines,
		Logger:      slog.Default(),
		Now:         now,
		RetriggerFn: retriggerFn,
	})

	if len(result) != 0 {
		t.Fatalf("expected 0 retriggered runs (non-retryable category), got %d", len(result))
	}
}

func TestStartStop(t *testing.T) {
	mp := testutil.NewMockProvider()
	// Use a deadline 2 minutes in the past so it falls within the lookback window.
	// Use local time (not UTC) since ParseSLADeadline uses now.Location().
	recentDeadline := time.Now().Add(-2 * time.Minute).Format("15:04")
	if err := mp.RegisterPipeline(context.Background(), pipelineWithDeadline(recentDeadline)); err != nil {
		t.Fatalf("RegisterPipeline: %v", err)
	}

	alertFn, _ := collectAlerts(t)
	w := New(mp, nil, alertFn, slog.Default(), 50*time.Millisecond)

	ctx := context.Background()
	w.Start(ctx)

	// Poll until the watchdog has appended at least one event.
	testutil.WaitFor(t, 2*time.Second, func() bool {
		return len(mp.Events()) > 0
	}, "watchdog should append at least one event")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	w.Stop(shutdownCtx)

	// Verify events were appended (scan ran at least once).
	if len(mp.Events()) == 0 {
		t.Error("expected at least one event from watchdog scan")
	}
}

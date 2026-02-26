package watchdog

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// ---------------------------------------------------------------------------
// Minimal mock provider — only the methods the watchdog touches
// ---------------------------------------------------------------------------

type mockProvider struct {
	pipelines      []types.PipelineConfig
	runLogs        map[string]*types.RunLogEntry // key: "pipeline:date:schedule"
	locks          map[string]bool
	events         []types.Event
	putRunLogCalls []types.RunLogEntry
	mu             sync.Mutex
}

func newMockProvider() *mockProvider {
	return &mockProvider{
		runLogs: make(map[string]*types.RunLogEntry),
		locks:   make(map[string]bool),
	}
}

func (m *mockProvider) ListPipelines(_ context.Context) ([]types.PipelineConfig, error) {
	return m.pipelines, nil
}

func (m *mockProvider) GetRunLog(_ context.Context, pipelineID, date, scheduleID string) (*types.RunLogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := pipelineID + ":" + date + ":" + scheduleID
	return m.runLogs[key], nil
}

func (m *mockProvider) AcquireLock(_ context.Context, key string, _ time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.locks[key] {
		return false, nil
	}
	m.locks[key] = true
	return true, nil
}

func (m *mockProvider) AppendEvent(_ context.Context, event types.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, event)
	return nil
}

// Stubs for the rest of provider.Provider — unused by watchdog.
func (m *mockProvider) RegisterPipeline(context.Context, types.PipelineConfig) error { return nil }
func (m *mockProvider) GetPipeline(context.Context, string) (*types.PipelineConfig, error) {
	return nil, nil
}
func (m *mockProvider) DeletePipeline(context.Context, string) error { return nil }
func (m *mockProvider) PutTrait(context.Context, string, types.TraitEvaluation, time.Duration) error {
	return nil
}
func (m *mockProvider) GetTraits(context.Context, string) ([]types.TraitEvaluation, error) {
	return nil, nil
}
func (m *mockProvider) GetTrait(context.Context, string, string) (*types.TraitEvaluation, error) {
	return nil, nil
}
func (m *mockProvider) PutRunState(context.Context, types.RunState) error { return nil }
func (m *mockProvider) GetRunState(context.Context, string) (*types.RunState, error) {
	return nil, nil
}
func (m *mockProvider) ListRuns(context.Context, string, int) ([]types.RunState, error) {
	return nil, nil
}
func (m *mockProvider) CompareAndSwapRunState(context.Context, string, int, types.RunState) (bool, error) {
	return false, nil
}
func (m *mockProvider) ListEvents(context.Context, string, int) ([]types.Event, error) {
	return nil, nil
}
func (m *mockProvider) ReadEventsSince(context.Context, string, string, int64) ([]types.EventRecord, error) {
	return nil, nil
}
func (m *mockProvider) PutRunLog(_ context.Context, entry types.RunLogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := entry.PipelineID + ":" + entry.Date + ":" + entry.ScheduleID
	entryCopy := entry
	m.runLogs[key] = &entryCopy
	m.putRunLogCalls = append(m.putRunLogCalls, entry)
	return nil
}
func (m *mockProvider) ListRunLogs(context.Context, string, int) ([]types.RunLogEntry, error) {
	return nil, nil
}
func (m *mockProvider) PutRerun(context.Context, types.RerunRecord) error { return nil }
func (m *mockProvider) GetRerun(context.Context, string) (*types.RerunRecord, error) {
	return nil, nil
}
func (m *mockProvider) ListReruns(context.Context, string, int) ([]types.RerunRecord, error) {
	return nil, nil
}
func (m *mockProvider) ListAllReruns(context.Context, int) ([]types.RerunRecord, error) {
	return nil, nil
}
func (m *mockProvider) ReleaseLock(context.Context, string) error { return nil }
func (m *mockProvider) WriteCascadeMarker(context.Context, string, string, string, string) error {
	return nil
}
func (m *mockProvider) PutLateArrival(context.Context, types.LateArrival) error { return nil }
func (m *mockProvider) ListLateArrivals(context.Context, string, string, string) ([]types.LateArrival, error) {
	return nil, nil
}
func (m *mockProvider) PutReplay(context.Context, types.ReplayRequest) error { return nil }
func (m *mockProvider) GetReplay(context.Context, string, string, string) (*types.ReplayRequest, error) {
	return nil, nil
}
func (m *mockProvider) ListReplays(context.Context, int) ([]types.ReplayRequest, error) {
	return nil, nil
}
func (m *mockProvider) PutReadiness(context.Context, types.ReadinessResult) error { return nil }
func (m *mockProvider) GetReadiness(context.Context, string) (*types.ReadinessResult, error) {
	return nil, nil
}
func (m *mockProvider) Start(context.Context) error { return nil }
func (m *mockProvider) Stop(context.Context) error  { return nil }
func (m *mockProvider) Ping(context.Context) error  { return nil }

// New sub-interface stubs (unused by watchdog).
func (m *mockProvider) PutAlert(context.Context, types.Alert) error { return nil }
func (m *mockProvider) ListAlerts(context.Context, string, int) ([]types.Alert, error) {
	return nil, nil
}
func (m *mockProvider) ListAllAlerts(context.Context, int) ([]types.Alert, error) { return nil, nil }
func (m *mockProvider) ListTraitHistory(context.Context, string, string, int) ([]types.TraitEvaluation, error) {
	return nil, nil
}
func (m *mockProvider) PutEvaluationSession(context.Context, types.EvaluationSession) error {
	return nil
}
func (m *mockProvider) GetEvaluationSession(context.Context, string) (*types.EvaluationSession, error) {
	return nil, nil
}
func (m *mockProvider) ListEvaluationSessions(context.Context, string, int) ([]types.EvaluationSession, error) {
	return nil, nil
}
func (m *mockProvider) PutDependency(context.Context, string, string) error    { return nil }
func (m *mockProvider) RemoveDependency(context.Context, string, string) error { return nil }
func (m *mockProvider) ListDependents(context.Context, string) ([]string, error) {
	return nil, nil
}
func (m *mockProvider) PutSensorData(context.Context, types.SensorData) error { return nil }
func (m *mockProvider) GetSensorData(context.Context, string, string) (*types.SensorData, error) {
	return nil, nil
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

func collectAlerts(t *testing.T) (alertFn func(types.Alert), getAlerts func() []types.Alert) {
	t.Helper()
	var mu sync.Mutex
	var alerts []types.Alert
	return func(a types.Alert) {
			mu.Lock()
			alerts = append(alerts, a)
			mu.Unlock()
		}, func() []types.Alert {
			mu.Lock()
			defer mu.Unlock()
			return alerts
		}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestNoRunLog_FiresAlert(t *testing.T) {
	mp := newMockProvider()
	mp.pipelines = []types.PipelineConfig{pipelineWithDeadline("10:00")}

	alertFn, getAlerts := collectAlerts(t)
	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:30:00Z")

	missed := CheckMissedSchedules(context.Background(), CheckOptions{
		Provider: mp,
		AlertFn:  alertFn,
		Logger:   slog.Default(),
		Now:      now,
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
	mp := newMockProvider()
	mp.pipelines = []types.PipelineConfig{pipelineWithDeadline("10:00")}
	mp.runLogs["p1:2026-02-24:daily"] = &types.RunLogEntry{Status: types.RunPending}

	alertFn, getAlerts := collectAlerts(t)
	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:30:00Z")

	missed := CheckMissedSchedules(context.Background(), CheckOptions{
		Provider: mp,
		AlertFn:  alertFn,
		Logger:   slog.Default(),
		Now:      now,
	})

	if len(missed) != 0 {
		t.Fatalf("expected 0 missed schedules, got %d", len(missed))
	}
	if len(getAlerts()) != 0 {
		t.Error("expected no alerts")
	}
}

func TestFailedRunLog_NoAlert(t *testing.T) {
	mp := newMockProvider()
	mp.pipelines = []types.PipelineConfig{pipelineWithDeadline("10:00")}
	mp.runLogs["p1:2026-02-24:daily"] = &types.RunLogEntry{Status: types.RunFailed}

	alertFn, getAlerts := collectAlerts(t)
	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:30:00Z")

	missed := CheckMissedSchedules(context.Background(), CheckOptions{
		Provider: mp,
		AlertFn:  alertFn,
		Logger:   slog.Default(),
		Now:      now,
	})

	if len(missed) != 0 {
		t.Fatalf("expected 0 missed, got %d", len(missed))
	}
	if len(getAlerts()) != 0 {
		t.Error("expected no alerts for failed run log")
	}
}

func TestDeadlineNotPassed_NoAlert(t *testing.T) {
	mp := newMockProvider()
	mp.pipelines = []types.PipelineConfig{pipelineWithDeadline("15:00")}

	alertFn, getAlerts := collectAlerts(t)
	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:30:00Z")

	missed := CheckMissedSchedules(context.Background(), CheckOptions{
		Provider: mp,
		AlertFn:  alertFn,
		Logger:   slog.Default(),
		Now:      now,
	})

	if len(missed) != 0 {
		t.Fatalf("expected 0 missed, got %d", len(missed))
	}
	if len(getAlerts()) != 0 {
		t.Error("expected no alerts before deadline")
	}
}

func TestExcludedDay_NoAlert(t *testing.T) {
	mp := newMockProvider()
	// 2026-02-24 is a Tuesday
	pl := pipelineWithDeadline("10:00")
	pl.Exclusions = &types.ExclusionConfig{Days: []string{"tuesday"}}
	mp.pipelines = []types.PipelineConfig{pl}

	alertFn, getAlerts := collectAlerts(t)
	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:30:00Z")

	missed := CheckMissedSchedules(context.Background(), CheckOptions{
		Provider: mp,
		AlertFn:  alertFn,
		Logger:   slog.Default(),
		Now:      now,
	})

	if len(missed) != 0 {
		t.Fatalf("expected 0 missed, got %d", len(missed))
	}
	if len(getAlerts()) != 0 {
		t.Error("expected no alerts on excluded day")
	}
}

func TestWatchDisabled_NoAlert(t *testing.T) {
	mp := newMockProvider()
	pl := pipelineWithDeadline("10:00")
	pl.Watch = &types.PipelineWatchConfig{Enabled: boolPtr(false)}
	mp.pipelines = []types.PipelineConfig{pl}

	alertFn, getAlerts := collectAlerts(t)
	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:30:00Z")

	missed := CheckMissedSchedules(context.Background(), CheckOptions{
		Provider: mp,
		AlertFn:  alertFn,
		Logger:   slog.Default(),
		Now:      now,
	})

	if len(missed) != 0 {
		t.Fatalf("expected 0 missed, got %d", len(missed))
	}
	if len(getAlerts()) != 0 {
		t.Error("expected no alerts when watch disabled")
	}
}

func TestNoDeadlineConfigured_NoAlert(t *testing.T) {
	mp := newMockProvider()
	// Pipeline with trigger but no SLA/deadline
	mp.pipelines = []types.PipelineConfig{{
		Name:    "p1",
		Trigger: &types.TriggerConfig{Type: types.TriggerHTTP},
	}}

	alertFn, getAlerts := collectAlerts(t)
	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:30:00Z")

	missed := CheckMissedSchedules(context.Background(), CheckOptions{
		Provider: mp,
		AlertFn:  alertFn,
		Logger:   slog.Default(),
		Now:      now,
	})

	if len(missed) != 0 {
		t.Fatalf("expected 0 missed, got %d", len(missed))
	}
	if len(getAlerts()) != 0 {
		t.Error("expected no alerts with no deadline")
	}
}

func TestScheduleDeadlinePrecedence(t *testing.T) {
	mp := newMockProvider()
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
	mp.pipelines = []types.PipelineConfig{pl}

	alertFn, getAlerts := collectAlerts(t)
	now, _ := time.Parse(time.RFC3339, "2026-02-24T12:00:00Z")

	missed := CheckMissedSchedules(context.Background(), CheckOptions{
		Provider: mp,
		AlertFn:  alertFn,
		Logger:   slog.Default(),
		Now:      now,
	})

	if len(missed) != 0 {
		t.Fatalf("expected 0 missed (schedule deadline not passed), got %d", len(missed))
	}
	if len(getAlerts()) != 0 {
		t.Error("expected no alerts — schedule deadline takes precedence")
	}
}

func TestMultiScheduleIndependent(t *testing.T) {
	mp := newMockProvider()
	pl := types.PipelineConfig{
		Name:    "p1",
		Trigger: &types.TriggerConfig{Type: types.TriggerHTTP},
		Schedules: []types.ScheduleConfig{
			{Name: "h10", Deadline: "10:30"},
			{Name: "h14", Deadline: "14:30"},
		},
	}
	mp.pipelines = []types.PipelineConfig{pl}
	// h10 has a run log entry; h14 does not.
	mp.runLogs["p1:2026-02-24:h10"] = &types.RunLogEntry{Status: types.RunCompleted}

	alertFn, getAlerts := collectAlerts(t)
	now, _ := time.Parse(time.RFC3339, "2026-02-24T15:00:00Z")

	missed := CheckMissedSchedules(context.Background(), CheckOptions{
		Provider: mp,
		AlertFn:  alertFn,
		Logger:   slog.Default(),
		Now:      now,
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
	mp := newMockProvider()
	mp.pipelines = []types.PipelineConfig{pipelineWithDeadline("10:00")}

	alertFn, getAlerts := collectAlerts(t)
	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:30:00Z")

	opts := CheckOptions{
		Provider: mp,
		AlertFn:  alertFn,
		Logger:   slog.Default(),
		Now:      now,
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

// ---------------------------------------------------------------------------
// Stuck-run detection tests
// ---------------------------------------------------------------------------

func TestStuckRun_RunningPastThreshold(t *testing.T) {
	mp := newMockProvider()
	mp.pipelines = []types.PipelineConfig{{
		Name:    "p1",
		Trigger: &types.TriggerConfig{Type: types.TriggerHTTP},
	}}
	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:30:00Z")
	mp.runLogs["p1:2026-02-24:daily"] = &types.RunLogEntry{
		Status:    types.RunRunning,
		RunID:     "run-1",
		UpdatedAt: now.Add(-45 * time.Minute), // 45 min ago
	}

	alertFn, getAlerts := collectAlerts(t)
	stuck := CheckStuckRuns(context.Background(), CheckOptions{
		Provider: mp,
		AlertFn:  alertFn,
		Logger:   slog.Default(),
		Now:      now,
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
	mp := newMockProvider()
	mp.pipelines = []types.PipelineConfig{{
		Name:    "p1",
		Trigger: &types.TriggerConfig{Type: types.TriggerHTTP},
	}}
	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:30:00Z")
	mp.runLogs["p1:2026-02-24:daily"] = &types.RunLogEntry{
		Status:    types.RunRunning,
		UpdatedAt: now.Add(-10 * time.Minute), // only 10 min ago
	}

	alertFn, getAlerts := collectAlerts(t)
	stuck := CheckStuckRuns(context.Background(), CheckOptions{
		Provider: mp,
		AlertFn:  alertFn,
		Logger:   slog.Default(),
		Now:      now,
	})

	if len(stuck) != 0 {
		t.Fatalf("expected 0 stuck runs, got %d", len(stuck))
	}
	if len(getAlerts()) != 0 {
		t.Error("expected no alerts for run not yet stuck")
	}
}

func TestStuckRun_CompletedRun_NotStuck(t *testing.T) {
	mp := newMockProvider()
	mp.pipelines = []types.PipelineConfig{{
		Name:    "p1",
		Trigger: &types.TriggerConfig{Type: types.TriggerHTTP},
	}}
	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:30:00Z")
	mp.runLogs["p1:2026-02-24:daily"] = &types.RunLogEntry{
		Status:    types.RunCompleted,
		UpdatedAt: now.Add(-60 * time.Minute),
	}

	alertFn, getAlerts := collectAlerts(t)
	stuck := CheckStuckRuns(context.Background(), CheckOptions{
		Provider: mp,
		AlertFn:  alertFn,
		Logger:   slog.Default(),
		Now:      now,
	})

	if len(stuck) != 0 {
		t.Fatalf("expected 0 stuck runs, got %d", len(stuck))
	}
	if len(getAlerts()) != 0 {
		t.Error("expected no alerts for completed run")
	}
}

func TestStuckRun_NoRunLog(t *testing.T) {
	mp := newMockProvider()
	mp.pipelines = []types.PipelineConfig{{
		Name:    "p1",
		Trigger: &types.TriggerConfig{Type: types.TriggerHTTP},
	}}

	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:30:00Z")
	alertFn, getAlerts := collectAlerts(t)
	stuck := CheckStuckRuns(context.Background(), CheckOptions{
		Provider: mp,
		AlertFn:  alertFn,
		Logger:   slog.Default(),
		Now:      now,
	})

	if len(stuck) != 0 {
		t.Fatalf("expected 0 stuck runs (no run log), got %d", len(stuck))
	}
	if len(getAlerts()) != 0 {
		t.Error("expected no alerts when no run log exists")
	}
}

func TestStuckRun_Dedup(t *testing.T) {
	mp := newMockProvider()
	mp.pipelines = []types.PipelineConfig{{
		Name:    "p1",
		Trigger: &types.TriggerConfig{Type: types.TriggerHTTP},
	}}
	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:30:00Z")
	mp.runLogs["p1:2026-02-24:daily"] = &types.RunLogEntry{
		Status:    types.RunRunning,
		RunID:     "run-1",
		UpdatedAt: now.Add(-45 * time.Minute),
	}

	alertFn, getAlerts := collectAlerts(t)
	opts := CheckOptions{
		Provider: mp,
		AlertFn:  alertFn,
		Logger:   slog.Default(),
		Now:      now,
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
	mp := newMockProvider()
	mp.pipelines = []types.PipelineConfig{{
		Name:    "p1",
		Trigger: &types.TriggerConfig{Type: types.TriggerHTTP},
	}}
	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:30:00Z")
	mp.runLogs["p1:2026-02-24:daily"] = &types.RunLogEntry{
		Status:    types.RunPending,
		RunID:     "run-1",
		UpdatedAt: now.Add(-40 * time.Minute),
	}

	alertFn, getAlerts := collectAlerts(t)
	stuck := CheckStuckRuns(context.Background(), CheckOptions{
		Provider: mp,
		AlertFn:  alertFn,
		Logger:   slog.Default(),
		Now:      now,
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
	mp := newMockProvider()
	mp.pipelines = []types.PipelineConfig{pipelineWithMonitoring("20m")}

	now, _ := time.Parse(time.RFC3339, "2026-02-24T11:00:00Z")
	// Run entered COMPLETED_MONITORING 25 minutes ago (past the 20m window).
	mp.runLogs["p1:2026-02-24:daily"] = &types.RunLogEntry{
		PipelineID: "p1",
		Date:       "2026-02-24",
		ScheduleID: "daily",
		Status:     types.RunCompletedMonitoring,
		RunID:      "run-1",
		UpdatedAt:  now.Add(-25 * time.Minute),
	}

	results := CheckCompletedMonitoring(context.Background(), CheckOptions{
		Provider: mp,
		Logger:   slog.Default(),
		Now:      now,
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
	if len(mp.events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(mp.events))
	}
	if mp.events[0].Kind != types.EventMonitoringCompleted {
		t.Errorf("expected MONITORING_COMPLETED event, got %s", mp.events[0].Kind)
	}
}

func TestCheckCompletedMonitoring_StillInWindow(t *testing.T) {
	mp := newMockProvider()
	mp.pipelines = []types.PipelineConfig{pipelineWithMonitoring("20m")}

	now, _ := time.Parse(time.RFC3339, "2026-02-24T11:00:00Z")
	// Run entered COMPLETED_MONITORING only 10 minutes ago (within 20m window).
	mp.runLogs["p1:2026-02-24:daily"] = &types.RunLogEntry{
		PipelineID: "p1",
		Date:       "2026-02-24",
		ScheduleID: "daily",
		Status:     types.RunCompletedMonitoring,
		RunID:      "run-1",
		UpdatedAt:  now.Add(-10 * time.Minute),
	}

	results := CheckCompletedMonitoring(context.Background(), CheckOptions{
		Provider: mp,
		Logger:   slog.Default(),
		Now:      now,
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
	mp := newMockProvider()
	mp.pipelines = []types.PipelineConfig{pipelineWithMonitoring("20m")}

	now, _ := time.Parse(time.RFC3339, "2026-02-24T11:00:00Z")
	// Run is already COMPLETED — should be ignored.
	mp.runLogs["p1:2026-02-24:daily"] = &types.RunLogEntry{
		PipelineID: "p1",
		Date:       "2026-02-24",
		ScheduleID: "daily",
		Status:     types.RunCompleted,
		RunID:      "run-1",
		UpdatedAt:  now.Add(-60 * time.Minute),
	}

	results := CheckCompletedMonitoring(context.Background(), CheckOptions{
		Provider: mp,
		Logger:   slog.Default(),
		Now:      now,
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
	mp := newMockProvider()
	mp.pipelines = []types.PipelineConfig{{
		Name:    "p1",
		Trigger: &types.TriggerConfig{Type: types.TriggerHTTP},
	}}
	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:30:00Z")
	mp.runLogs["p1:2026-02-24:daily"] = &types.RunLogEntry{
		Status:    types.RunRunning,
		RunID:     "run-1",
		UpdatedAt: now.Add(-20 * time.Minute), // 20 min ago
	}

	alertFn, getAlerts := collectAlerts(t)

	// Default 30m threshold: not stuck
	stuck := CheckStuckRuns(context.Background(), CheckOptions{
		Provider: mp,
		AlertFn:  alertFn,
		Logger:   slog.Default(),
		Now:      now,
	})
	if len(stuck) != 0 {
		t.Fatalf("expected 0 stuck with default threshold, got %d", len(stuck))
	}

	// Custom 15m threshold: stuck
	stuck = CheckStuckRuns(context.Background(), CheckOptions{
		Provider:          mp,
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
	mp := newMockProvider()
	mp.pipelines = []types.PipelineConfig{{
		Name:    "p1",
		Trigger: &types.TriggerConfig{Type: types.TriggerHTTP},
		Watch:   &types.PipelineWatchConfig{Enabled: boolPtr(false)},
	}}
	now, _ := time.Parse(time.RFC3339, "2026-02-24T10:30:00Z")
	mp.runLogs["p1:2026-02-24:daily"] = &types.RunLogEntry{
		Status:    types.RunRunning,
		UpdatedAt: now.Add(-60 * time.Minute),
	}

	alertFn, getAlerts := collectAlerts(t)
	stuck := CheckStuckRuns(context.Background(), CheckOptions{
		Provider: mp,
		AlertFn:  alertFn,
		Logger:   slog.Default(),
		Now:      now,
	})

	if len(stuck) != 0 {
		t.Fatalf("expected 0 stuck when watch disabled, got %d", len(stuck))
	}
	if len(getAlerts()) != 0 {
		t.Error("expected no alerts when watch disabled")
	}
}

func TestStartStop(t *testing.T) {
	mp := newMockProvider()
	mp.pipelines = []types.PipelineConfig{pipelineWithDeadline("00:01")}

	alertFn, _ := collectAlerts(t)
	w := New(mp, nil, alertFn, slog.Default(), 50*time.Millisecond)

	ctx := context.Background()
	w.Start(ctx)

	// Give it a couple ticks.
	time.Sleep(150 * time.Millisecond)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	w.Stop(shutdownCtx)

	// Verify events were appended (scan ran at least once).
	mp.mu.Lock()
	eventCount := len(mp.events)
	mp.mu.Unlock()
	if eventCount == 0 {
		t.Error("expected at least one event from watchdog scan")
	}
}

package watcher_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/dwsmith1983/interlock/internal/archetype"
	"github.com/dwsmith1983/interlock/internal/calendar"
	"github.com/dwsmith1983/interlock/internal/engine"
	"github.com/dwsmith1983/interlock/internal/evaluator"
	"github.com/dwsmith1983/interlock/internal/testutil"
	"github.com/dwsmith1983/interlock/internal/watcher"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// alertCollector is a goroutine-safe alert collector for tests.
type alertCollector struct {
	mu     sync.Mutex
	alerts []types.Alert
}

func (a *alertCollector) collect(alert types.Alert) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.alerts = append(a.alerts, alert)
}

func (a *alertCollector) get() []types.Alert {
	a.mu.Lock()
	defer a.mu.Unlock()
	cp := make([]types.Alert, len(a.alerts))
	copy(cp, a.alerts)
	return cp
}

const pollTimeout = 2 * time.Second

func setupWatcher(t *testing.T, prov *testutil.MockProvider) (*watcher.Watcher, *alertCollector) {
	t.Helper()

	reg := archetype.NewRegistry()
	require.NoError(t, reg.Register(&types.Archetype{
		Name: "batch-ingestion",
		RequiredTraits: []types.TraitDefinition{
			{Type: "freshness", DefaultTimeout: 5},
		},
		ReadinessRule: types.ReadinessRule{Type: types.AllRequiredPass},
	}))

	runner := evaluator.NewRunner()
	eng := engine.New(prov, reg, runner, nil)

	ac := &alertCollector{}

	cfg := types.WatcherConfig{
		Enabled:         true,
		DefaultInterval: "100ms",
	}

	w := watcher.New(prov, eng, nil, nil, ac.collect, slog.Default(), cfg)
	return w, ac
}

func readyPipeline(name string) types.PipelineConfig {
	enabled := true
	return types.PipelineConfig{
		Name:      name,
		Archetype: "batch-ingestion",
		Traits: map[string]types.TraitConfig{
			"freshness": {Evaluator: "../../testdata/evaluators/pass", Timeout: 5},
		},
		Trigger: &types.TriggerConfig{
			Type:    types.TriggerCommand,
			Command: &types.CommandTriggerConfig{Command: "true"},
		},
		Watch: &types.PipelineWatchConfig{
			Enabled: &enabled,
		},
	}
}

func TestWatcher_ReadyPipeline_Triggers(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	pipeline := readyPipeline("trigger-test")
	require.NoError(t, prov.RegisterPipeline(ctx, pipeline))

	w, _ := setupWatcher(t, prov)
	w.Start(ctx)
	defer w.Stop(context.Background())

	testutil.WaitForRunStatus(t, prov, "trigger-test", types.RunCompleted, pollTimeout)

	testutil.WaitForEvent(t, prov, "trigger-test", types.EventWatcherEvaluation, pollTimeout)
	testutil.WaitForEvent(t, prov, "trigger-test", types.EventTriggerFired, pollTimeout)
}

func TestWatcher_NotReady_NoTrigger(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	enabled := true
	pipeline := types.PipelineConfig{
		Name:      "not-ready-test",
		Archetype: "batch-ingestion",
		Traits: map[string]types.TraitConfig{
			"freshness": {Evaluator: "../../testdata/evaluators/fail", Timeout: 5},
		},
		Trigger: &types.TriggerConfig{
			Type:    types.TriggerCommand,
			Command: &types.CommandTriggerConfig{Command: "true"},
		},
		Watch: &types.PipelineWatchConfig{
			Enabled: &enabled,
		},
	}
	require.NoError(t, prov.RegisterPipeline(ctx, pipeline))

	w, _ := setupWatcher(t, prov)
	w.Start(ctx)

	// Wait for at least one evaluation to complete
	testutil.WaitForEvent(t, prov, "not-ready-test", types.EventWatcherEvaluation, pollTimeout)
	w.Stop(context.Background())

	runs, err := prov.ListRuns(ctx, "not-ready-test", 10)
	require.NoError(t, err)
	assert.Empty(t, runs, "expected no runs for not-ready pipeline")
}

func TestWatcher_ActiveRun_SkipsTrigger(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	pipeline := readyPipeline("active-run-test")
	require.NoError(t, prov.RegisterPipeline(ctx, pipeline))

	now := time.Now()
	require.NoError(t, prov.PutRunState(ctx, types.RunState{
		RunID:      "active-run",
		PipelineID: "active-run-test",
		Status:     types.RunRunning,
		Version:    3,
		CreatedAt:  now,
		UpdatedAt:  now,
	}))
	require.NoError(t, prov.PutRunLog(ctx, types.RunLogEntry{
		PipelineID:    "active-run-test",
		Date:          now.Format("2006-01-02"),
		ScheduleID:    "daily",
		Status:        types.RunRunning,
		AttemptNumber: 1,
		RunID:         "active-run",
		StartedAt:     now,
		UpdatedAt:     now,
	}))

	w, _ := setupWatcher(t, prov)
	w.Start(ctx)
	testutil.WaitForPollCount(t, prov, 3, pollTimeout)
	w.Stop(context.Background())

	runs, err := prov.ListRuns(ctx, "active-run-test", 10)
	require.NoError(t, err)
	assert.Len(t, runs, 1, "expected no new runs when active run exists")
	assert.Equal(t, "active-run", runs[0].RunID)
}

func TestWatcher_AlreadySucceeded_Skips(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	pipeline := readyPipeline("succeeded-test")
	require.NoError(t, prov.RegisterPipeline(ctx, pipeline))

	today := time.Now().Format("2006-01-02")
	require.NoError(t, prov.PutRunLog(ctx, types.RunLogEntry{
		PipelineID:    "succeeded-test",
		Date:          today,
		Status:        types.RunCompleted,
		AttemptNumber: 1,
		RunID:         "old-run",
		StartedAt:     time.Now().Add(-1 * time.Hour),
		UpdatedAt:     time.Now().Add(-1 * time.Hour),
	}))

	w, _ := setupWatcher(t, prov)
	w.Start(ctx)
	testutil.WaitForPollCount(t, prov, 3, pollTimeout)
	w.Stop(context.Background())

	runs, err := prov.ListRuns(ctx, "succeeded-test", 10)
	require.NoError(t, err)
	assert.Empty(t, runs, "expected no new runs when already succeeded today")
}

func TestWatcher_MaxRetriesExhausted(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	pipeline := readyPipeline("exhausted-test")
	pipeline.Retry = &types.RetryPolicy{
		MaxAttempts:       2,
		BackoffSeconds:    1,
		BackoffMultiplier: 1.0,
		RetryableFailures: []types.FailureCategory{types.FailureTransient},
	}
	require.NoError(t, prov.RegisterPipeline(ctx, pipeline))

	today := time.Now().Format("2006-01-02")
	require.NoError(t, prov.PutRunLog(ctx, types.RunLogEntry{
		PipelineID:      "exhausted-test",
		Date:            today,
		Status:          types.RunFailed,
		AttemptNumber:   2,
		RunID:           "failed-run",
		FailureMessage:  "connection refused",
		FailureCategory: types.FailureTransient,
		StartedAt:       time.Now().Add(-5 * time.Minute),
		UpdatedAt:       time.Now().Add(-5 * time.Minute),
	}))

	w, ac := setupWatcher(t, prov)
	w.Start(ctx)

	testutil.WaitFor(t, pollTimeout, func() bool {
		return len(ac.get()) > 0
	}, "alert fired for exhausted retries")
	w.Stop(context.Background())

	runs, err := prov.ListRuns(ctx, "exhausted-test", 10)
	require.NoError(t, err)
	assert.Empty(t, runs, "expected no new runs after retries exhausted")

	alerts := ac.get()
	assert.NotEmpty(t, alerts, "expected alert when retries exhausted")
	if len(alerts) > 0 {
		assert.Equal(t, types.AlertLevelError, alerts[0].Level)
		assert.Contains(t, alerts[0].Message, "exhausted")
	}
}

func TestWatcher_PermanentFailure_NoRetry(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	pipeline := readyPipeline("permanent-test")
	require.NoError(t, prov.RegisterPipeline(ctx, pipeline))

	today := time.Now().Format("2006-01-02")
	require.NoError(t, prov.PutRunLog(ctx, types.RunLogEntry{
		PipelineID:      "permanent-test",
		Date:            today,
		Status:          types.RunFailed,
		AttemptNumber:   1,
		RunID:           "perm-run",
		FailureMessage:  "bad request",
		FailureCategory: types.FailurePermanent,
		StartedAt:       time.Now().Add(-5 * time.Minute),
		UpdatedAt:       time.Now().Add(-5 * time.Minute),
	}))

	w, ac := setupWatcher(t, prov)
	w.Start(ctx)

	testutil.WaitFor(t, pollTimeout, func() bool {
		return len(ac.get()) > 0
	}, "alert fired for permanent failure")
	w.Stop(context.Background())

	runs, err := prov.ListRuns(ctx, "permanent-test", 10)
	require.NoError(t, err)
	assert.Empty(t, runs, "expected no retry for permanent failure")

	assert.NotEmpty(t, ac.get(), "expected alert for non-retryable failure")
}

func TestWatcher_LockPreventsDoubleEval(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	pipeline := readyPipeline("lock-test")
	require.NoError(t, prov.RegisterPipeline(ctx, pipeline))

	acquired, err := prov.AcquireLock(ctx, "eval:lock-test:daily", 10*time.Second)
	require.NoError(t, err)
	require.True(t, acquired)

	w, _ := setupWatcher(t, prov)
	w.Start(ctx)
	testutil.WaitForPollCount(t, prov, 3, pollTimeout)
	w.Stop(context.Background())

	runs, err := prov.ListRuns(ctx, "lock-test", 10)
	require.NoError(t, err)
	assert.Empty(t, runs, "expected no runs when lock is held")
}

func TestWatcher_EvaluationSLABreach(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	enabled := true
	pipeline := types.PipelineConfig{
		Name:      "sla-eval-test",
		Archetype: "batch-ingestion",
		Traits: map[string]types.TraitConfig{
			"freshness": {Evaluator: "../../testdata/evaluators/fail", Timeout: 5},
		},
		Trigger: &types.TriggerConfig{
			Type:    types.TriggerCommand,
			Command: &types.CommandTriggerConfig{Command: "true"},
		},
		Watch: &types.PipelineWatchConfig{Enabled: &enabled},
		SLA: &types.SLAConfig{
			EvaluationDeadline: "00:01",
		},
	}
	require.NoError(t, prov.RegisterPipeline(ctx, pipeline))

	w, ac := setupWatcher(t, prov)
	w.Start(ctx)

	testutil.WaitForEvent(t, prov, "sla-eval-test", types.EventSLABreached, pollTimeout)
	w.Stop(context.Background())

	hasSLAAlert := false
	for _, a := range ac.get() {
		if a.Level == types.AlertLevelError {
			hasSLAAlert = true
		}
	}
	assert.True(t, hasSLAAlert, "expected SLA breach alert")
}

func TestWatcher_CompletionSLABreach(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	enabled := true
	pipeline := readyPipeline("sla-complete-test")
	pipeline.Watch = &types.PipelineWatchConfig{Enabled: &enabled}
	pipeline.SLA = &types.SLAConfig{
		CompletionDeadline: "00:01",
	}
	require.NoError(t, prov.RegisterPipeline(ctx, pipeline))

	now := time.Now()
	pastStart := now.Add(-1 * time.Hour)
	require.NoError(t, prov.PutRunState(ctx, types.RunState{
		RunID:      "running-sla",
		PipelineID: "sla-complete-test",
		Status:     types.RunRunning,
		Version:    3,
		CreatedAt:  pastStart,
		UpdatedAt:  pastStart,
	}))
	require.NoError(t, prov.PutRunLog(ctx, types.RunLogEntry{
		PipelineID:    "sla-complete-test",
		Date:          now.Format("2006-01-02"),
		ScheduleID:    "daily",
		Status:        types.RunRunning,
		AttemptNumber: 1,
		RunID:         "running-sla",
		StartedAt:     pastStart,
		UpdatedAt:     pastStart,
	}))

	w, ac := setupWatcher(t, prov)
	w.Start(ctx)

	testutil.WaitFor(t, pollTimeout, func() bool {
		for _, a := range ac.get() {
			if a.Level == types.AlertLevelError {
				return true
			}
		}
		return false
	}, "completion SLA breach alert")
	w.Stop(context.Background())

	hasSLAAlert := false
	for _, a := range ac.get() {
		if a.Level == types.AlertLevelError {
			hasSLAAlert = true
		}
	}
	assert.True(t, hasSLAAlert, "expected completion SLA breach alert")
}

func monitoringPipeline(name string) types.PipelineConfig {
	enabled := true
	return types.PipelineConfig{
		Name:      name,
		Archetype: "batch-ingestion",
		Traits: map[string]types.TraitConfig{
			"freshness": {Evaluator: "../../testdata/evaluators/pass", Timeout: 5},
		},
		Trigger: &types.TriggerConfig{
			Type:    types.TriggerCommand,
			Command: &types.CommandTriggerConfig{Command: "true"},
		},
		Watch: &types.PipelineWatchConfig{
			Enabled: &enabled,
			Monitoring: &types.MonitoringConfig{
				Enabled:  true,
				Duration: "1h",
			},
		},
	}
}

func setupWatcherWithInterval(t *testing.T, prov *testutil.MockProvider, interval string) (*watcher.Watcher, *alertCollector) {
	t.Helper()

	reg := archetype.NewRegistry()
	require.NoError(t, reg.Register(&types.Archetype{
		Name: "batch-ingestion",
		RequiredTraits: []types.TraitDefinition{
			{Type: "freshness", DefaultTimeout: 5},
		},
		ReadinessRule: types.ReadinessRule{Type: types.AllRequiredPass},
	}))

	runner := evaluator.NewRunner()
	eng := engine.New(prov, reg, runner, nil)

	ac := &alertCollector{}

	cfg := types.WatcherConfig{
		Enabled:         true,
		DefaultInterval: interval,
	}

	w := watcher.New(prov, eng, nil, nil, ac.collect, slog.Default(), cfg)
	return w, ac
}

func TestWatcher_MonitoringEnabled_TransitionsToMonitoring(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	pipeline := monitoringPipeline("monitor-transition-test")
	require.NoError(t, prov.RegisterPipeline(ctx, pipeline))

	w, _ := setupWatcherWithInterval(t, prov, "10s")
	w.Start(ctx)
	defer w.Stop(context.Background())

	run := testutil.WaitForRunStatus(t, prov, "monitor-transition-test", types.RunCompletedMonitoring, pollTimeout)

	_, hasMonitoringStart := run.Metadata["monitoringStartedAt"]
	assert.True(t, hasMonitoringStart, "expected monitoringStartedAt in metadata")

	testutil.WaitForEvent(t, prov, "monitor-transition-test", types.EventMonitoringStarted, pollTimeout)
}

func TestWatcher_MonitoringExpires_TransitionsToCompleted(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	pipeline := monitoringPipeline("monitor-expire-test")
	pipeline.Watch.Monitoring.Duration = "1ms"
	require.NoError(t, prov.RegisterPipeline(ctx, pipeline))

	pastStart := time.Now().Add(-1 * time.Hour)
	require.NoError(t, prov.PutRunState(ctx, types.RunState{
		RunID:      "monitor-run",
		PipelineID: "monitor-expire-test",
		Status:     types.RunCompletedMonitoring,
		Version:    3,
		CreatedAt:  pastStart,
		UpdatedAt:  pastStart,
		Metadata: map[string]interface{}{
			"monitoringStartedAt": pastStart.Format(time.RFC3339),
		},
	}))

	today := time.Now().Format("2006-01-02")
	require.NoError(t, prov.PutRunLog(ctx, types.RunLogEntry{
		PipelineID:    "monitor-expire-test",
		Date:          today,
		Status:        types.RunCompletedMonitoring,
		AttemptNumber: 1,
		RunID:         "monitor-run",
		StartedAt:     pastStart,
		UpdatedAt:     pastStart,
	}))

	w, _ := setupWatcher(t, prov)
	w.Start(ctx)

	testutil.WaitForEvent(t, prov, "monitor-expire-test", types.EventMonitoringCompleted, pollTimeout)
	w.Stop(context.Background())

	runs, err := prov.ListRuns(ctx, "monitor-expire-test", 10)
	require.NoError(t, err)
	require.NotEmpty(t, runs)
	assert.Equal(t, types.RunCompleted, runs[0].Status, "expected COMPLETED after monitoring window expires")
}

func TestWatcher_MonitoringDrift_CreatesRerun(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	enabled := true
	pipeline := types.PipelineConfig{
		Name:      "monitor-drift-test",
		Archetype: "batch-ingestion",
		Traits: map[string]types.TraitConfig{
			"freshness": {Evaluator: "../../testdata/evaluators/fail", Timeout: 5},
		},
		Trigger: &types.TriggerConfig{
			Type:    types.TriggerCommand,
			Command: &types.CommandTriggerConfig{Command: "true"},
		},
		Watch: &types.PipelineWatchConfig{
			Enabled: &enabled,
			Monitoring: &types.MonitoringConfig{
				Enabled:  true,
				Duration: "1h",
			},
		},
	}
	require.NoError(t, prov.RegisterPipeline(ctx, pipeline))

	recentStart := time.Now().Add(-5 * time.Minute)
	require.NoError(t, prov.PutRunState(ctx, types.RunState{
		RunID:      "drift-run",
		PipelineID: "monitor-drift-test",
		Status:     types.RunCompletedMonitoring,
		Version:    3,
		CreatedAt:  recentStart,
		UpdatedAt:  recentStart,
		Metadata: map[string]interface{}{
			"monitoringStartedAt": recentStart.Format(time.RFC3339),
		},
	}))

	today := time.Now().Format("2006-01-02")
	require.NoError(t, prov.PutRunLog(ctx, types.RunLogEntry{
		PipelineID:    "monitor-drift-test",
		Date:          today,
		Status:        types.RunCompletedMonitoring,
		AttemptNumber: 1,
		RunID:         "drift-run",
		StartedAt:     recentStart,
		UpdatedAt:     recentStart,
	}))

	w, ac := setupWatcher(t, prov)
	w.Start(ctx)

	testutil.WaitForEvent(t, prov, "monitor-drift-test", types.EventMonitoringDrift, pollTimeout)
	w.Stop(context.Background())

	runs, err := prov.ListRuns(ctx, "monitor-drift-test", 10)
	require.NoError(t, err)
	require.NotEmpty(t, runs)
	assert.Equal(t, types.RunCompleted, runs[0].Status, "expected COMPLETED after drift detection")

	reruns := prov.Reruns()
	require.NotEmpty(t, reruns, "expected a rerun record for drift")
	assert.Equal(t, "monitoring_drift_detected", reruns[0].Reason)
	assert.Equal(t, "monitor-drift-test", reruns[0].PipelineID)
	assert.Equal(t, "drift-run", reruns[0].OriginalRunID)

	hasWarningAlert := false
	for _, a := range ac.get() {
		if a.Level == types.AlertLevelWarning {
			hasWarningAlert = true
		}
	}
	assert.True(t, hasWarningAlert, "expected warning alert for drift")
}

func TestWatcher_MonitoringDisabled_NormalCompletion(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	pipeline := readyPipeline("no-monitor-test")
	require.NoError(t, prov.RegisterPipeline(ctx, pipeline))

	w, _ := setupWatcher(t, prov)
	w.Start(ctx)
	defer w.Stop(context.Background())

	testutil.WaitForRunStatus(t, prov, "no-monitor-test", types.RunCompleted, pollTimeout)

	// Should NOT have MONITORING_STARTED event
	events := prov.Events()
	for _, e := range events {
		assert.NotEqual(t, types.EventMonitoringStarted, e.Kind, "unexpected MONITORING_STARTED event")
	}
}

func TestWatcher_MultiSchedule_IndependentTriggers(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	enabled := true
	pipeline := types.PipelineConfig{
		Name:      "multi-sched-test",
		Archetype: "batch-ingestion",
		Traits: map[string]types.TraitConfig{
			"freshness": {Evaluator: "../../testdata/evaluators/pass", Timeout: 5},
		},
		Trigger: &types.TriggerConfig{
			Type:    types.TriggerCommand,
			Command: &types.CommandTriggerConfig{Command: "true"},
		},
		Watch: &types.PipelineWatchConfig{
			Enabled: &enabled,
		},
		Schedules: []types.ScheduleConfig{
			{Name: "morning"},
			{Name: "evening"},
		},
	}
	require.NoError(t, prov.RegisterPipeline(ctx, pipeline))

	w, _ := setupWatcher(t, prov)
	w.Start(ctx)

	// Wait for at least two completed runs (one per schedule)
	testutil.WaitFor(t, 5*time.Second, func() bool {
		runs, _ := prov.ListRuns(ctx, "multi-sched-test", 10)
		completed := 0
		for _, run := range runs {
			if run.Status == types.RunCompleted {
				completed++
			}
		}
		return completed >= 2
	}, "expected at least 2 completed runs (one per schedule)")

	w.Stop(context.Background())

	runs, err := prov.ListRuns(ctx, "multi-sched-test", 10)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(runs), 2)

	// Verify both runs have scheduleId metadata and they differ
	scheduleIDs := make(map[string]bool)
	for _, run := range runs {
		sid, _ := run.Metadata["scheduleId"].(string)
		if sid != "" && run.Status == types.RunCompleted {
			scheduleIDs[sid] = true
		}
	}
	assert.True(t, scheduleIDs["morning"], "expected a completed run for 'morning' schedule")
	assert.True(t, scheduleIDs["evening"], "expected a completed run for 'evening' schedule")

	// Verify independent run logs
	today := time.Now().Format("2006-01-02")
	morningLog, err := prov.GetRunLog(ctx, "multi-sched-test", today, "morning")
	require.NoError(t, err)
	assert.NotNil(t, morningLog, "expected run log for morning schedule")
	if morningLog != nil {
		assert.Equal(t, "morning", morningLog.ScheduleID)
	}

	eveningLog, err := prov.GetRunLog(ctx, "multi-sched-test", today, "evening")
	require.NoError(t, err)
	assert.NotNil(t, eveningLog, "expected run log for evening schedule")
	if eveningLog != nil {
		assert.Equal(t, "evening", eveningLog.ScheduleID)
	}
}

func TestWatcher_MultiSchedule_InactiveSkipped(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	enabled := true
	pipeline := types.PipelineConfig{
		Name:      "inactive-sched-test",
		Archetype: "batch-ingestion",
		Traits: map[string]types.TraitConfig{
			"freshness": {Evaluator: "../../testdata/evaluators/pass", Timeout: 5},
		},
		Trigger: &types.TriggerConfig{
			Type:    types.TriggerCommand,
			Command: &types.CommandTriggerConfig{Command: "true"},
		},
		Watch: &types.PipelineWatchConfig{
			Enabled: &enabled,
		},
		Schedules: []types.ScheduleConfig{
			{Name: "morning"},
			{Name: "late-night", After: "23:59"}, // almost never active
		},
	}
	require.NoError(t, prov.RegisterPipeline(ctx, pipeline))

	w, _ := setupWatcher(t, prov)
	w.Start(ctx)

	// Wait for at least one run
	testutil.WaitForRunStatus(t, prov, "inactive-sched-test", types.RunCompleted, pollTimeout)

	// Let watcher run a few more cycles to confirm no extra triggers
	baseline := prov.PollCount()
	testutil.WaitForPollCount(t, prov, baseline+3, pollTimeout)
	w.Stop(context.Background())

	runs, err := prov.ListRuns(ctx, "inactive-sched-test", 10)
	require.NoError(t, err)

	// Only runs with "morning" schedule should exist (unless running right at 23:59)
	for _, run := range runs {
		sid, _ := run.Metadata["scheduleId"].(string)
		assert.Equal(t, "morning", sid, "only morning schedule should trigger")
	}
}

func TestWatcher_DefaultSchedule_BackwardCompatible(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	// Pipeline with NO schedules field — should get implicit "daily"
	pipeline := readyPipeline("compat-test")
	require.NoError(t, prov.RegisterPipeline(ctx, pipeline))

	w, _ := setupWatcher(t, prov)
	w.Start(ctx)
	defer w.Stop(context.Background())

	run := testutil.WaitForRunStatus(t, prov, "compat-test", types.RunCompleted, pollTimeout)

	// Run should have scheduleId="daily" in metadata
	sid, _ := run.Metadata["scheduleId"].(string)
	assert.Equal(t, "daily", sid, "default schedule should be 'daily'")

	// Run log should be keyed by "daily" schedule
	today := time.Now().Format("2006-01-02")
	runLog, err := prov.GetRunLog(ctx, "compat-test", today, "daily")
	require.NoError(t, err)
	assert.NotNil(t, runLog)
	if runLog != nil {
		assert.Equal(t, "daily", runLog.ScheduleID)
	}
}

func setupWatcherWithCalendar(t *testing.T, prov *testutil.MockProvider, calReg *calendar.Registry) (*watcher.Watcher, *alertCollector) {
	t.Helper()

	reg := archetype.NewRegistry()
	require.NoError(t, reg.Register(&types.Archetype{
		Name: "batch-ingestion",
		RequiredTraits: []types.TraitDefinition{
			{Type: "freshness", DefaultTimeout: 5},
		},
		ReadinessRule: types.ReadinessRule{Type: types.AllRequiredPass},
	}))

	runner := evaluator.NewRunner()
	eng := engine.New(prov, reg, runner, nil)

	ac := &alertCollector{}

	cfg := types.WatcherConfig{
		Enabled:         true,
		DefaultInterval: "100ms",
	}

	w := watcher.New(prov, eng, calReg, nil, ac.collect, slog.Default(), cfg)
	return w, ac
}

func TestWatcher_ExcludedDay_NothingFires(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	// Build a calendar that excludes every day of the week
	calReg := calendar.NewRegistry()
	require.NoError(t, calReg.Register(&types.Calendar{
		Name: "always-excluded",
		Days: []string{"monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"},
	}))

	enabled := true
	pipeline := types.PipelineConfig{
		Name:      "excluded-test",
		Archetype: "batch-ingestion",
		Traits: map[string]types.TraitConfig{
			"freshness": {Evaluator: "../../testdata/evaluators/pass", Timeout: 5},
		},
		Trigger: &types.TriggerConfig{
			Type:    types.TriggerCommand,
			Command: &types.CommandTriggerConfig{Command: "true"},
		},
		Watch: &types.PipelineWatchConfig{
			Enabled: &enabled,
		},
		Exclusions: &types.ExclusionConfig{
			Calendar: "always-excluded",
		},
	}
	require.NoError(t, prov.RegisterPipeline(ctx, pipeline))

	w, ac := setupWatcherWithCalendar(t, prov, calReg)
	w.Start(ctx)
	testutil.WaitForPollCount(t, prov, 3, pollTimeout)
	w.Stop(context.Background())

	// No runs should have been created
	runs, err := prov.ListRuns(ctx, "excluded-test", 10)
	require.NoError(t, err)
	assert.Empty(t, runs, "expected no runs on excluded day")

	// No events should have been created
	events := prov.Events()
	for _, e := range events {
		assert.NotEqual(t, "excluded-test", e.PipelineID, "expected no events for excluded pipeline")
	}

	// No alerts
	assert.Empty(t, ac.get(), "expected no alerts on excluded day")
}

func TestWatcher_PerScheduleSLA_UsesScheduleDeadline(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	enabled := true
	pipeline := types.PipelineConfig{
		Name:      "sched-sla-test",
		Archetype: "batch-ingestion",
		Traits: map[string]types.TraitConfig{
			"freshness": {Evaluator: "../../testdata/evaluators/fail", Timeout: 5},
		},
		Trigger: &types.TriggerConfig{
			Type:    types.TriggerCommand,
			Command: &types.CommandTriggerConfig{Command: "true"},
		},
		Watch: &types.PipelineWatchConfig{Enabled: &enabled},
		Schedules: []types.ScheduleConfig{
			{
				Name:     "morning",
				Deadline: "00:01", // very early deadline — should breach immediately
			},
		},
	}
	require.NoError(t, prov.RegisterPipeline(ctx, pipeline))

	w, ac := setupWatcher(t, prov)
	w.Start(ctx)

	testutil.WaitForEvent(t, prov, "sched-sla-test", types.EventSLABreached, pollTimeout)
	w.Stop(context.Background())

	hasSLAAlert := false
	for _, a := range ac.get() {
		if a.Level == types.AlertLevelError {
			hasSLAAlert = true
		}
	}
	assert.True(t, hasSLAAlert, "expected SLA breach alert from schedule deadline")
}

func TestWatcher_ExcludedDay_SLANotChecked(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	// Calendar that excludes every day
	calReg := calendar.NewRegistry()
	require.NoError(t, calReg.Register(&types.Calendar{
		Name: "always-excluded",
		Days: []string{"monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"},
	}))

	enabled := true
	pipeline := types.PipelineConfig{
		Name:      "excluded-sla-test",
		Archetype: "batch-ingestion",
		Traits: map[string]types.TraitConfig{
			"freshness": {Evaluator: "../../testdata/evaluators/fail", Timeout: 5},
		},
		Trigger: &types.TriggerConfig{
			Type:    types.TriggerCommand,
			Command: &types.CommandTriggerConfig{Command: "true"},
		},
		Watch: &types.PipelineWatchConfig{Enabled: &enabled},
		SLA: &types.SLAConfig{
			EvaluationDeadline: "00:01", // would breach immediately if evaluated
		},
		Exclusions: &types.ExclusionConfig{
			Calendar: "always-excluded",
		},
	}
	require.NoError(t, prov.RegisterPipeline(ctx, pipeline))

	w, ac := setupWatcherWithCalendar(t, prov, calReg)
	w.Start(ctx)
	testutil.WaitForPollCount(t, prov, 3, pollTimeout)
	w.Stop(context.Background())

	// No SLA breach alerts should fire — exclusion suppresses all evaluation
	assert.Empty(t, ac.get(), "expected no SLA alerts on excluded day")

	// No SLA_BREACHED events
	for _, e := range prov.Events() {
		if e.PipelineID == "excluded-sla-test" {
			assert.NotEqual(t, types.EventSLABreached, e.Kind, "expected no SLA_BREACHED event on excluded day")
		}
	}
}

func TestWatcher_ExcludedDay_ActiveRunIgnored(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	// Calendar that excludes every day
	calReg := calendar.NewRegistry()
	require.NoError(t, calReg.Register(&types.Calendar{
		Name: "always-excluded",
		Days: []string{"monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"},
	}))

	enabled := true
	pipeline := types.PipelineConfig{
		Name:      "excluded-active-test",
		Archetype: "batch-ingestion",
		Traits: map[string]types.TraitConfig{
			"freshness": {Evaluator: "../../testdata/evaluators/pass", Timeout: 5},
		},
		Trigger: &types.TriggerConfig{
			Type:    types.TriggerCommand,
			Command: &types.CommandTriggerConfig{Command: "true"},
		},
		Watch: &types.PipelineWatchConfig{Enabled: &enabled},
		SLA: &types.SLAConfig{
			CompletionDeadline: "00:01", // would breach immediately if checked
		},
		Exclusions: &types.ExclusionConfig{
			Calendar: "always-excluded",
		},
	}
	require.NoError(t, prov.RegisterPipeline(ctx, pipeline))

	// Seed an active run that would normally trigger completion SLA breach
	require.NoError(t, prov.PutRunState(ctx, types.RunState{
		RunID:      "active-excluded-run",
		PipelineID: "excluded-active-test",
		Status:     types.RunRunning,
		Version:    3,
		CreatedAt:  time.Now().Add(-2 * time.Hour),
		UpdatedAt:  time.Now().Add(-2 * time.Hour),
	}))

	w, ac := setupWatcherWithCalendar(t, prov, calReg)
	w.Start(ctx)
	testutil.WaitForPollCount(t, prov, 3, pollTimeout)
	w.Stop(context.Background())

	// No completion SLA alerts — excluded day means handleActiveRun never runs
	assert.Empty(t, ac.get(), "expected no completion SLA alerts on excluded day")

	// Run state should be unchanged
	run, err := prov.GetRunState(ctx, "active-excluded-run")
	require.NoError(t, err)
	assert.Equal(t, types.RunRunning, run.Status, "active run should be untouched on excluded day")
}

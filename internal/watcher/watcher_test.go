package watcher_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/interlock-systems/interlock/internal/archetype"
	"github.com/interlock-systems/interlock/internal/engine"
	"github.com/interlock-systems/interlock/internal/evaluator"
	"github.com/interlock-systems/interlock/internal/testutil"
	"github.com/interlock-systems/interlock/internal/watcher"
	"github.com/interlock-systems/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupWatcher(t *testing.T, prov *testutil.MockProvider) (*watcher.Watcher, *[]types.Alert) {
	t.Helper()

	reg := archetype.NewRegistry()
	require.NoError(t, reg.Register(&types.Archetype{
		Name: "batch-ingestion",
		RequiredTraits: []types.TraitDefinition{
			{Type: "freshness", DefaultTimeout: 5},
		},
		ReadinessRule: types.ReadinessRule{Type: types.AllRequiredPass},
	}))

	runner := evaluator.NewRunner(nil)
	eng := engine.New(prov, reg, runner, nil)

	alerts := &[]types.Alert{}
	alertFn := func(a types.Alert) {
		*alerts = append(*alerts, a)
	}

	cfg := types.WatcherConfig{
		Enabled:         true,
		DefaultInterval: "100ms",
	}

	w := watcher.New(prov, eng, alertFn, slog.Default(), cfg)
	return w, alerts
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
			Command: "true",
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
	time.Sleep(500 * time.Millisecond)
	w.Stop(context.Background())

	// Should have created a run
	runs, err := prov.ListRuns(ctx, "trigger-test", 10)
	require.NoError(t, err)
	assert.NotEmpty(t, runs, "expected at least one run to be created")

	// For command triggers, the run should be completed
	if len(runs) > 0 {
		assert.Equal(t, types.RunCompleted, runs[0].Status)
	}

	// Should have trigger events
	events := prov.Events()
	hasEvalEvent := false
	hasTriggerEvent := false
	for _, e := range events {
		if e.Kind == types.EventWatcherEvaluation {
			hasEvalEvent = true
		}
		if e.Kind == types.EventTriggerFired {
			hasTriggerEvent = true
		}
	}
	assert.True(t, hasEvalEvent, "expected watcher evaluation event")
	assert.True(t, hasTriggerEvent, "expected trigger fired event")
}

func TestWatcher_NotReady_NoTrigger(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	// Pipeline with no trait data — will evaluate as NOT_READY
	enabled := true
	pipeline := types.PipelineConfig{
		Name:      "not-ready-test",
		Archetype: "batch-ingestion",
		Traits: map[string]types.TraitConfig{
			"freshness": {Evaluator: "../../testdata/evaluators/fail", Timeout: 5},
		},
		Trigger: &types.TriggerConfig{
			Type:    types.TriggerCommand,
			Command: "true",
		},
		Watch: &types.PipelineWatchConfig{
			Enabled: &enabled,
		},
	}
	require.NoError(t, prov.RegisterPipeline(ctx, pipeline))

	w, _ := setupWatcher(t, prov)
	w.Start(ctx)
	time.Sleep(500 * time.Millisecond)
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

	// Create an active (non-terminal) run
	require.NoError(t, prov.PutRunState(ctx, types.RunState{
		RunID:      "active-run",
		PipelineID: "active-run-test",
		Status:     types.RunRunning,
		Version:    3,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}))

	w, _ := setupWatcher(t, prov)
	w.Start(ctx)
	time.Sleep(500 * time.Millisecond)
	w.Stop(context.Background())

	// Should still only have the 1 run we created
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

	// Mark today as already succeeded
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
	time.Sleep(500 * time.Millisecond)
	w.Stop(context.Background())

	// Should not create new runs
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

	// Mark as failed with max attempts reached
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

	w, alerts := setupWatcher(t, prov)
	w.Start(ctx)
	time.Sleep(500 * time.Millisecond)
	w.Stop(context.Background())

	// Should not create new runs
	runs, err := prov.ListRuns(ctx, "exhausted-test", 10)
	require.NoError(t, err)
	assert.Empty(t, runs, "expected no new runs after retries exhausted")

	// Should have fired an alert
	assert.NotEmpty(t, *alerts, "expected alert when retries exhausted")
	if len(*alerts) > 0 {
		assert.Equal(t, types.AlertLevelError, (*alerts)[0].Level)
		assert.Contains(t, (*alerts)[0].Message, "exhausted")
	}
}

func TestWatcher_PermanentFailure_NoRetry(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	pipeline := readyPipeline("permanent-test")
	require.NoError(t, prov.RegisterPipeline(ctx, pipeline))

	// Mark as failed with permanent category
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

	w, alerts := setupWatcher(t, prov)
	w.Start(ctx)
	time.Sleep(500 * time.Millisecond)
	w.Stop(context.Background())

	// Should not create new runs
	runs, err := prov.ListRuns(ctx, "permanent-test", 10)
	require.NoError(t, err)
	assert.Empty(t, runs, "expected no retry for permanent failure")

	// Should alert about non-retryable error
	assert.NotEmpty(t, *alerts, "expected alert for non-retryable failure")
}

func TestWatcher_LockPreventsDoubleEval(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	pipeline := readyPipeline("lock-test")
	require.NoError(t, prov.RegisterPipeline(ctx, pipeline))

	// Pre-acquire the lock
	acquired, err := prov.AcquireLock(ctx, "eval:lock-test", 10*time.Second)
	require.NoError(t, err)
	require.True(t, acquired)

	w, _ := setupWatcher(t, prov)
	w.Start(ctx)
	time.Sleep(500 * time.Millisecond)
	w.Stop(context.Background())

	// Should not have created any runs (lock was held)
	runs, err := prov.ListRuns(ctx, "lock-test", 10)
	require.NoError(t, err)
	assert.Empty(t, runs, "expected no runs when lock is held")
}

func TestWatcher_EvaluationSLABreach(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	// Pipeline with an SLA deadline in the past
	enabled := true
	pipeline := types.PipelineConfig{
		Name:      "sla-eval-test",
		Archetype: "batch-ingestion",
		Traits: map[string]types.TraitConfig{
			"freshness": {Evaluator: "../../testdata/evaluators/fail", Timeout: 5},
		},
		Trigger: &types.TriggerConfig{
			Type:    types.TriggerCommand,
			Command: "true",
		},
		Watch: &types.PipelineWatchConfig{Enabled: &enabled},
		SLA: &types.SLAConfig{
			EvaluationDeadline: "00:01", // 00:01 AM — almost certainly past
		},
	}
	require.NoError(t, prov.RegisterPipeline(ctx, pipeline))

	w, alerts := setupWatcher(t, prov)
	w.Start(ctx)
	time.Sleep(500 * time.Millisecond)
	w.Stop(context.Background())

	// Should fire SLA breach alert
	hasSLAAlert := false
	for _, a := range *alerts {
		if a.Level == types.AlertLevelError {
			hasSLAAlert = true
		}
	}
	assert.True(t, hasSLAAlert, "expected SLA breach alert")

	// Should have SLA_BREACHED event
	events := prov.Events()
	hasSLAEvent := false
	for _, e := range events {
		if e.Kind == types.EventSLABreached {
			hasSLAEvent = true
		}
	}
	assert.True(t, hasSLAEvent, "expected SLA_BREACHED event")
}

func TestWatcher_CompletionSLABreach(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	enabled := true
	pipeline := readyPipeline("sla-complete-test")
	pipeline.Watch = &types.PipelineWatchConfig{Enabled: &enabled}
	pipeline.SLA = &types.SLAConfig{
		CompletionDeadline: "00:01", // past
	}
	require.NoError(t, prov.RegisterPipeline(ctx, pipeline))

	// Create an active run (still RUNNING)
	require.NoError(t, prov.PutRunState(ctx, types.RunState{
		RunID:      "running-sla",
		PipelineID: "sla-complete-test",
		Status:     types.RunRunning,
		Version:    3,
		CreatedAt:  time.Now().Add(-1 * time.Hour),
		UpdatedAt:  time.Now().Add(-1 * time.Hour),
	}))

	w, alerts := setupWatcher(t, prov)
	w.Start(ctx)
	time.Sleep(500 * time.Millisecond)
	w.Stop(context.Background())

	// Should fire completion SLA breach alert
	hasSLAAlert := false
	for _, a := range *alerts {
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
			Command: "true",
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

func setupWatcherWithInterval(t *testing.T, prov *testutil.MockProvider, interval string) (*watcher.Watcher, *[]types.Alert) {
	t.Helper()

	reg := archetype.NewRegistry()
	require.NoError(t, reg.Register(&types.Archetype{
		Name: "batch-ingestion",
		RequiredTraits: []types.TraitDefinition{
			{Type: "freshness", DefaultTimeout: 5},
		},
		ReadinessRule: types.ReadinessRule{Type: types.AllRequiredPass},
	}))

	runner := evaluator.NewRunner(nil)
	eng := engine.New(prov, reg, runner, nil)

	alerts := &[]types.Alert{}
	alertFn := func(a types.Alert) {
		*alerts = append(*alerts, a)
	}

	cfg := types.WatcherConfig{
		Enabled:         true,
		DefaultInterval: interval,
	}

	w := watcher.New(prov, eng, alertFn, slog.Default(), cfg)
	return w, alerts
}

func TestWatcher_MonitoringEnabled_TransitionsToMonitoring(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	pipeline := monitoringPipeline("monitor-transition-test")
	require.NoError(t, prov.RegisterPipeline(ctx, pipeline))

	// Use a long interval so only the initial immediate poll fires (trigger),
	// preventing rapid re-evaluations that can cause subprocess flakiness.
	w, _ := setupWatcherWithInterval(t, prov, "10s")
	w.Start(ctx)
	time.Sleep(500 * time.Millisecond)
	w.Stop(context.Background())

	// Should have created a run in COMPLETED_MONITORING status
	runs, err := prov.ListRuns(ctx, "monitor-transition-test", 10)
	require.NoError(t, err)
	require.NotEmpty(t, runs, "expected at least one run")
	assert.Equal(t, types.RunCompletedMonitoring, runs[0].Status, "expected COMPLETED_MONITORING status")

	// Should have monitoringStartedAt metadata
	_, hasMonitoringStart := runs[0].Metadata["monitoringStartedAt"]
	assert.True(t, hasMonitoringStart, "expected monitoringStartedAt in metadata")

	// Should have MONITORING_STARTED event
	events := prov.Events()
	hasMonitoringEvent := false
	for _, e := range events {
		if e.Kind == types.EventMonitoringStarted {
			hasMonitoringEvent = true
		}
	}
	assert.True(t, hasMonitoringEvent, "expected MONITORING_STARTED event")
}

func TestWatcher_MonitoringExpires_TransitionsToCompleted(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	pipeline := monitoringPipeline("monitor-expire-test")
	pipeline.Watch.Monitoring.Duration = "1ms" // expires immediately
	require.NoError(t, prov.RegisterPipeline(ctx, pipeline))

	// Create a run in COMPLETED_MONITORING with a past monitoring start
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

	// Add a run log entry
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
	time.Sleep(500 * time.Millisecond)
	w.Stop(context.Background())

	// Run should now be COMPLETED
	runs, err := prov.ListRuns(ctx, "monitor-expire-test", 10)
	require.NoError(t, err)
	require.NotEmpty(t, runs)
	assert.Equal(t, types.RunCompleted, runs[0].Status, "expected COMPLETED after monitoring window expires")

	// Should have MONITORING_COMPLETED event
	events := prov.Events()
	hasCompletedEvent := false
	for _, e := range events {
		if e.Kind == types.EventMonitoringCompleted {
			hasCompletedEvent = true
		}
	}
	assert.True(t, hasCompletedEvent, "expected MONITORING_COMPLETED event")
}

func TestWatcher_MonitoringDrift_CreatesRerun(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	// Pipeline with monitoring enabled and a FAIL evaluator (simulates drift)
	enabled := true
	pipeline := types.PipelineConfig{
		Name:      "monitor-drift-test",
		Archetype: "batch-ingestion",
		Traits: map[string]types.TraitConfig{
			"freshness": {Evaluator: "../../testdata/evaluators/fail", Timeout: 5},
		},
		Trigger: &types.TriggerConfig{
			Type:    types.TriggerCommand,
			Command: "true",
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

	// Create a run in COMPLETED_MONITORING with a recent start (still within window)
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

	w, alerts := setupWatcher(t, prov)
	w.Start(ctx)
	time.Sleep(500 * time.Millisecond)
	w.Stop(context.Background())

	// Run should transition to COMPLETED (monitoring duty done)
	runs, err := prov.ListRuns(ctx, "monitor-drift-test", 10)
	require.NoError(t, err)
	require.NotEmpty(t, runs)
	assert.Equal(t, types.RunCompleted, runs[0].Status, "expected COMPLETED after drift detection")

	// Should have created a rerun record
	reruns := prov.Reruns()
	require.NotEmpty(t, reruns, "expected a rerun record for drift")
	assert.Equal(t, "monitoring_drift_detected", reruns[0].Reason)
	assert.Equal(t, "monitor-drift-test", reruns[0].PipelineID)
	assert.Equal(t, "drift-run", reruns[0].OriginalRunID)

	// Should have MONITORING_DRIFT_DETECTED event
	events := prov.Events()
	hasDriftEvent := false
	for _, e := range events {
		if e.Kind == types.EventMonitoringDrift {
			hasDriftEvent = true
		}
	}
	assert.True(t, hasDriftEvent, "expected MONITORING_DRIFT_DETECTED event")

	// Should have fired a warning alert
	hasWarningAlert := false
	for _, a := range *alerts {
		if a.Level == types.AlertLevelWarning {
			hasWarningAlert = true
		}
	}
	assert.True(t, hasWarningAlert, "expected warning alert for drift")
}

func TestWatcher_MonitoringDisabled_NormalCompletion(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	// Use readyPipeline — no monitoring config
	pipeline := readyPipeline("no-monitor-test")
	require.NoError(t, prov.RegisterPipeline(ctx, pipeline))

	w, _ := setupWatcher(t, prov)
	w.Start(ctx)
	time.Sleep(500 * time.Millisecond)
	w.Stop(context.Background())

	// Should have created a run in COMPLETED status (not COMPLETED_MONITORING)
	runs, err := prov.ListRuns(ctx, "no-monitor-test", 10)
	require.NoError(t, err)
	require.NotEmpty(t, runs, "expected at least one run")
	assert.Equal(t, types.RunCompleted, runs[0].Status, "expected COMPLETED status without monitoring")

	// Should NOT have MONITORING_STARTED event
	events := prov.Events()
	for _, e := range events {
		assert.NotEqual(t, types.EventMonitoringStarted, e.Kind, "unexpected MONITORING_STARTED event")
	}
}

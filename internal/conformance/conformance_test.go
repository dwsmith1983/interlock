// Package conformance_test verifies that the watcher (local polling loop) and
// orchestrator (ASL Step Function actions) produce equivalent outcomes for
// identical initial states. Each scenario runs two sub-tests:
//
//   - Watcher: full watcher lifecycle (Start → poll → Stop)
//   - Orchestrator: manual invocation of shared packages (engine, schedule,
//     provider) that the orchestrator Lambda actions delegate to
//
// This ensures the two execution paths stay behaviourally aligned.
package conformance_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/dwsmith1983/interlock/internal/archetype"
	"github.com/dwsmith1983/interlock/internal/engine"
	"github.com/dwsmith1983/interlock/internal/evaluator"
	"github.com/dwsmith1983/interlock/internal/schedule"
	"github.com/dwsmith1983/interlock/internal/testutil"
	"github.com/dwsmith1983/interlock/internal/watcher"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const pollTimeout = 2 * time.Second

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

func newRegistry(t *testing.T) *archetype.Registry {
	t.Helper()
	reg := archetype.NewRegistry()
	require.NoError(t, reg.Register(&types.Archetype{
		Name: "batch-ingestion",
		RequiredTraits: []types.TraitDefinition{
			{Type: "freshness", DefaultTimeout: 5},
		},
		ReadinessRule: types.ReadinessRule{Type: types.AllRequiredPass},
	}))
	return reg
}

func newEngine(t *testing.T, prov *testutil.MockProvider) *engine.Engine {
	t.Helper()
	reg := newRegistry(t)
	runner := evaluator.NewRunner()
	return engine.New(prov, reg, runner, nil)
}

func newWatcherInstance(t *testing.T, prov *testutil.MockProvider, ac *alertCollector) *watcher.Watcher {
	t.Helper()
	eng := newEngine(t, prov)
	cfg := types.WatcherConfig{
		Enabled:         true,
		DefaultInterval: "100ms",
	}
	return watcher.New(prov, eng, nil, nil, ac.collect, slog.Default(), cfg)
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
		Watch: &types.PipelineWatchConfig{Enabled: &enabled},
	}
}

func notReadyPipeline(name string) types.PipelineConfig {
	enabled := true
	return types.PipelineConfig{
		Name:      name,
		Archetype: "batch-ingestion",
		Traits: map[string]types.TraitConfig{
			"freshness": {Evaluator: "../../testdata/evaluators/fail", Timeout: 5},
		},
		Trigger: &types.TriggerConfig{
			Type:    types.TriggerCommand,
			Command: "true",
		},
		Watch: &types.PipelineWatchConfig{Enabled: &enabled},
	}
}

// ---------------------------------------------------------------------------
// Scenario 1: Fresh ready pipeline → COMPLETED
// ---------------------------------------------------------------------------

func TestConformance_FreshReady_Completed(t *testing.T) {
	t.Run("Watcher", func(t *testing.T) {
		prov := testutil.NewMockProvider()
		ctx := context.Background()
		require.NoError(t, prov.RegisterPipeline(ctx, readyPipeline("fresh-ready-w")))

		ac := &alertCollector{}
		w := newWatcherInstance(t, prov, ac)
		w.Start(ctx)
		defer w.Stop(context.Background())

		testutil.WaitForRunStatus(t, prov, "fresh-ready-w", types.RunCompleted, pollTimeout)
		testutil.WaitForEvent(t, prov, "fresh-ready-w", types.EventTriggerFired, pollTimeout)
	})

	t.Run("Orchestrator", func(t *testing.T) {
		prov := testutil.NewMockProvider()
		ctx := context.Background()
		require.NoError(t, prov.RegisterPipeline(ctx, readyPipeline("fresh-ready-o")))

		// checkExclusion → proceed
		pipeline, err := prov.GetPipeline(ctx, "fresh-ready-o")
		require.NoError(t, err)
		assert.False(t, schedule.IsExcluded(*pipeline, nil, time.Now()), "not excluded")

		// checkRunLog → no entry, first attempt
		date := time.Now().UTC().Format("2006-01-02")
		entry, err := prov.GetRunLog(ctx, "fresh-ready-o", date, types.DefaultScheduleID)
		require.NoError(t, err)
		assert.Nil(t, entry, "no prior run log")

		// evaluate → READY
		eng := newEngine(t, prov)
		result, err := eng.Evaluate(ctx, "fresh-ready-o")
		require.NoError(t, err)
		assert.Equal(t, types.Ready, result.Status)
		assert.Empty(t, result.Blocking)
	})
}

// ---------------------------------------------------------------------------
// Scenario 2: Not-ready pipeline → no trigger
// ---------------------------------------------------------------------------

func TestConformance_NotReady_NoTrigger(t *testing.T) {
	t.Run("Watcher", func(t *testing.T) {
		prov := testutil.NewMockProvider()
		ctx := context.Background()
		require.NoError(t, prov.RegisterPipeline(ctx, notReadyPipeline("not-ready-w")))

		ac := &alertCollector{}
		w := newWatcherInstance(t, prov, ac)
		w.Start(ctx)
		testutil.WaitForEvent(t, prov, "not-ready-w", types.EventWatcherEvaluation, pollTimeout)
		w.Stop(context.Background())

		runs, err := prov.ListRuns(ctx, "not-ready-w", 10)
		require.NoError(t, err)
		assert.Empty(t, runs, "no runs for not-ready pipeline")
	})

	t.Run("Orchestrator", func(t *testing.T) {
		prov := testutil.NewMockProvider()
		ctx := context.Background()
		require.NoError(t, prov.RegisterPipeline(ctx, notReadyPipeline("not-ready-o")))

		eng := newEngine(t, prov)
		result, err := eng.Evaluate(ctx, "not-ready-o")
		require.NoError(t, err)
		assert.Equal(t, types.NotReady, result.Status)
		assert.NotEmpty(t, result.Blocking, "should report blocking traits")
		assert.Contains(t, result.Blocking, "freshness")
	})
}

// ---------------------------------------------------------------------------
// Scenario 3: Evaluation SLA breach → alert
// ---------------------------------------------------------------------------

func TestConformance_EvaluationSLABreach_Alert(t *testing.T) {
	slaPipeline := func(name string) types.PipelineConfig {
		p := notReadyPipeline(name)
		p.SLA = &types.SLAConfig{
			EvaluationDeadline: "00:01", // past midnight → already breached
		}
		return p
	}

	t.Run("Watcher", func(t *testing.T) {
		prov := testutil.NewMockProvider()
		ctx := context.Background()
		require.NoError(t, prov.RegisterPipeline(ctx, slaPipeline("sla-breach-w")))

		ac := &alertCollector{}
		w := newWatcherInstance(t, prov, ac)
		w.Start(ctx)
		testutil.WaitForEvent(t, prov, "sla-breach-w", types.EventSLABreached, pollTimeout)
		w.Stop(context.Background())

		hasSLAAlert := false
		for _, a := range ac.get() {
			if a.Level == types.AlertLevelError {
				hasSLAAlert = true
			}
		}
		assert.True(t, hasSLAAlert, "watcher should fire SLA breach alert")
	})

	t.Run("Orchestrator", func(t *testing.T) {
		prov := testutil.NewMockProvider()
		ctx := context.Background()
		p := slaPipeline("sla-breach-o")
		require.NoError(t, prov.RegisterPipeline(ctx, p))

		// Evaluate → NOT_READY (same as watcher path)
		eng := newEngine(t, prov)
		result, err := eng.Evaluate(ctx, "sla-breach-o")
		require.NoError(t, err)
		assert.Equal(t, types.NotReady, result.Status)

		// checkEvaluationSLA → breached (simulates orchestrator SLA check)
		now := time.Now()
		deadline, err := schedule.ParseSLADeadline(p.SLA.EvaluationDeadline, p.SLA.Timezone, now)
		require.NoError(t, err)
		assert.True(t, schedule.IsBreached(deadline, now),
			"orchestrator SLA check should detect breach at the same time")
	})
}

// ---------------------------------------------------------------------------
// Scenario 4: Retry after failure → next attempt
// ---------------------------------------------------------------------------

func TestConformance_RetryAfterFailure_NextAttempt(t *testing.T) {
	retryPipeline := func(name string) types.PipelineConfig {
		p := readyPipeline(name)
		p.Retry = &types.RetryPolicy{
			MaxAttempts:       3,
			BackoffSeconds:    0,
			BackoffMultiplier: 1.0,
			RetryableFailures: []types.FailureCategory{types.FailureTransient},
		}
		return p
	}

	today := time.Now().Format("2006-01-02")
	seedFailedLog := func(t *testing.T, prov *testutil.MockProvider, name string) {
		t.Helper()
		require.NoError(t, prov.PutRunLog(context.Background(), types.RunLogEntry{
			PipelineID:      name,
			Date:            today,
			ScheduleID:      types.DefaultScheduleID,
			Status:          types.RunFailed,
			AttemptNumber:   1,
			RunID:           "failed-run",
			FailureMessage:  "connection timeout",
			FailureCategory: types.FailureTransient,
			StartedAt:       time.Now().Add(-5 * time.Minute),
			UpdatedAt:       time.Now().Add(-5 * time.Minute),
		}))
	}

	t.Run("Watcher", func(t *testing.T) {
		prov := testutil.NewMockProvider()
		ctx := context.Background()
		require.NoError(t, prov.RegisterPipeline(ctx, retryPipeline("retry-w")))
		seedFailedLog(t, prov, "retry-w")

		ac := &alertCollector{}
		w := newWatcherInstance(t, prov, ac)
		w.Start(ctx)
		defer w.Stop(context.Background())

		// The watcher should re-evaluate, find READY, and trigger a new run.
		testutil.WaitForRunStatus(t, prov, "retry-w", types.RunCompleted, pollTimeout)
	})

	t.Run("Orchestrator", func(t *testing.T) {
		prov := testutil.NewMockProvider()
		ctx := context.Background()
		require.NoError(t, prov.RegisterPipeline(ctx, retryPipeline("retry-o")))
		seedFailedLog(t, prov, "retry-o")

		// checkRunLog: failed + retryable + attempts remaining → proceed
		entry, err := prov.GetRunLog(ctx, "retry-o", today, types.DefaultScheduleID)
		require.NoError(t, err)
		require.NotNil(t, entry)

		retryPolicy := types.RetryPolicy{
			MaxAttempts:       3,
			BackoffSeconds:    0,
			BackoffMultiplier: 1.0,
			RetryableFailures: []types.FailureCategory{types.FailureTransient},
		}
		assert.True(t, schedule.IsRetryable(retryPolicy, entry.FailureCategory),
			"failure should be retryable")
		assert.Less(t, entry.AttemptNumber, retryPolicy.MaxAttempts,
			"should have attempts remaining")

		// Evaluate → READY
		eng := newEngine(t, prov)
		result, err := eng.Evaluate(ctx, "retry-o")
		require.NoError(t, err)
		assert.Equal(t, types.Ready, result.Status, "pipeline should be ready for retry")
	})
}

// ---------------------------------------------------------------------------
// Scenario 5: Already completed → skip
// ---------------------------------------------------------------------------

func TestConformance_AlreadyCompleted_Skip(t *testing.T) {
	today := time.Now().Format("2006-01-02")
	seedCompletedLog := func(t *testing.T, prov *testutil.MockProvider, name string) {
		t.Helper()
		require.NoError(t, prov.PutRunLog(context.Background(), types.RunLogEntry{
			PipelineID:    name,
			Date:          today,
			ScheduleID:    types.DefaultScheduleID,
			Status:        types.RunCompleted,
			AttemptNumber: 1,
			RunID:         "completed-run",
			StartedAt:     time.Now().Add(-1 * time.Hour),
			UpdatedAt:     time.Now().Add(-1 * time.Hour),
		}))
	}

	t.Run("Watcher", func(t *testing.T) {
		prov := testutil.NewMockProvider()
		ctx := context.Background()
		require.NoError(t, prov.RegisterPipeline(ctx, readyPipeline("completed-w")))
		seedCompletedLog(t, prov, "completed-w")

		ac := &alertCollector{}
		w := newWatcherInstance(t, prov, ac)
		w.Start(ctx)
		testutil.WaitForPollCount(t, prov, 3, pollTimeout)
		w.Stop(context.Background())

		runs, err := prov.ListRuns(ctx, "completed-w", 10)
		require.NoError(t, err)
		assert.Empty(t, runs, "no new runs when already completed")
	})

	t.Run("Orchestrator", func(t *testing.T) {
		prov := testutil.NewMockProvider()
		ctx := context.Background()
		require.NoError(t, prov.RegisterPipeline(ctx, readyPipeline("completed-o")))
		seedCompletedLog(t, prov, "completed-o")

		// checkRunLog: already completed → skip
		entry, err := prov.GetRunLog(ctx, "completed-o", today, types.DefaultScheduleID)
		require.NoError(t, err)
		require.NotNil(t, entry)
		assert.Equal(t, types.RunCompleted, entry.Status, "run log shows completed — orchestrator would skip")
	})
}

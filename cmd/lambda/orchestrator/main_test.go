package main

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/dwsmith1983/interlock/internal/archetype"
	intlambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/testutil"
	"github.com/dwsmith1983/interlock/internal/trigger"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testDeps(t *testing.T) *intlambda.Deps {
	t.Helper()
	prov := testutil.NewMockProvider()
	return &intlambda.Deps{
		Provider:     prov,
		Runner:       trigger.NewRunner(),
		ArchetypeReg: archetype.NewRegistry(),
		AlertFn:      func(a types.Alert) {},
		Logger:       slog.Default(),
	}
}

func seedPipeline(t *testing.T, d *intlambda.Deps, pipeline types.PipelineConfig) {
	t.Helper()
	require.NoError(t, d.Provider.RegisterPipeline(context.Background(), pipeline))
}

func TestUnknownAction(t *testing.T) {
	d := testDeps(t)
	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action: "nonexistent",
	})
	require.NoError(t, err)
	assert.Equal(t, "error", resp.Result)
}

// --- checkExclusion ---

func TestCheckExclusion_NotExcluded(t *testing.T) {
	d := testDeps(t)
	seedPipeline(t, d, types.PipelineConfig{Name: "pipe-a"})

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "checkExclusion",
		PipelineID: "pipe-a",
	})
	require.NoError(t, err)
	assert.Equal(t, "proceed", resp.Result)
}

func TestCheckExclusion_Excluded(t *testing.T) {
	d := testDeps(t)
	// Exclude today's weekday
	today := time.Now().UTC().Weekday().String()
	seedPipeline(t, d, types.PipelineConfig{
		Name:       "pipe-a",
		Exclusions: &types.ExclusionConfig{Days: []string{today}},
	})

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "checkExclusion",
		PipelineID: "pipe-a",
	})
	require.NoError(t, err)
	assert.Equal(t, "skip", resp.Result)
}

func TestCheckExclusion_PipelineNotFound(t *testing.T) {
	d := testDeps(t)
	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "checkExclusion",
		PipelineID: "nonexistent",
	})
	require.NoError(t, err)
	assert.Equal(t, "error", resp.Result)
}

// --- acquireLock ---

func TestAcquireLock_Success(t *testing.T) {
	d := testDeps(t)

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "acquireLock",
		PipelineID: "pipe-a",
		ScheduleID: "daily",
	})
	require.NoError(t, err)
	assert.Equal(t, "proceed", resp.Result)
}

func TestAcquireLock_AlreadyHeld(t *testing.T) {
	d := testDeps(t)

	// Acquire first
	_, err := d.Provider.AcquireLock(context.Background(), "eval:pipe-a:daily", 5*time.Minute)
	require.NoError(t, err)

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "acquireLock",
		PipelineID: "pipe-a",
		ScheduleID: "daily",
	})
	require.NoError(t, err)
	assert.Equal(t, "skip", resp.Result)
}

// --- checkRunLog ---

func TestCheckRunLog_NoEntry(t *testing.T) {
	d := testDeps(t)
	seedPipeline(t, d, types.PipelineConfig{Name: "pipe-a"})

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "checkRunLog",
		PipelineID: "pipe-a",
		ScheduleID: "daily",
	})
	require.NoError(t, err)
	assert.Equal(t, "proceed", resp.Result)
	assert.Equal(t, 1, resp.Payload["attemptNumber"])
}

func TestCheckRunLog_AlreadyCompleted(t *testing.T) {
	d := testDeps(t)
	seedPipeline(t, d, types.PipelineConfig{Name: "pipe-a"})

	date := time.Now().UTC().Format("2006-01-02")
	require.NoError(t, d.Provider.PutRunLog(context.Background(), types.RunLogEntry{
		PipelineID: "pipe-a",
		Date:       date,
		ScheduleID: "daily",
		Status:     types.RunCompleted,
	}))

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "checkRunLog",
		PipelineID: "pipe-a",
		ScheduleID: "daily",
	})
	require.NoError(t, err)
	assert.Equal(t, "skip", resp.Result)
}

func TestCheckRunLog_NonRetryableFailure(t *testing.T) {
	d := testDeps(t)
	seedPipeline(t, d, types.PipelineConfig{Name: "pipe-a"})

	date := time.Now().UTC().Format("2006-01-02")
	require.NoError(t, d.Provider.PutRunLog(context.Background(), types.RunLogEntry{
		PipelineID:      "pipe-a",
		Date:            date,
		ScheduleID:      "daily",
		Status:          types.RunFailed,
		FailureCategory: types.FailurePermanent,
		AttemptNumber:   1,
	}))

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "checkRunLog",
		PipelineID: "pipe-a",
		ScheduleID: "daily",
	})
	require.NoError(t, err)
	assert.Equal(t, "skip", resp.Result)
}

// --- checkReadiness ---

func TestCheckReadiness_AllPass(t *testing.T) {
	d := testDeps(t)

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "checkReadiness",
		PipelineID: "pipe-a",
		Payload: map[string]interface{}{
			"traitResults": []interface{}{
				map[string]interface{}{"traitType": "freshness", "status": "PASS", "required": true},
				map[string]interface{}{"traitType": "schema", "status": "PASS", "required": true},
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "proceed", resp.Result)
}

func TestCheckReadiness_HasBlocking(t *testing.T) {
	d := testDeps(t)

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "checkReadiness",
		PipelineID: "pipe-a",
		Payload: map[string]interface{}{
			"traitResults": []interface{}{
				map[string]interface{}{"traitType": "freshness", "status": "PASS", "required": true},
				map[string]interface{}{"traitType": "schema", "status": "FAIL", "required": true},
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "not_ready", resp.Result)
	assert.Equal(t, true, resp.Payload["pollAdvised"])
	failedTraits := resp.Payload["failedTraits"].([]string)
	assert.Contains(t, failedTraits, "schema")
}

func TestCheckReadiness_OptionalFail(t *testing.T) {
	d := testDeps(t)

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "checkReadiness",
		PipelineID: "pipe-a",
		Payload: map[string]interface{}{
			"traitResults": []interface{}{
				map[string]interface{}{"traitType": "freshness", "status": "PASS", "required": true},
				map[string]interface{}{"traitType": "optional-check", "status": "FAIL", "required": false},
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "proceed", resp.Result)
}

// --- checkEvaluationSLA ---

func TestCheckEvaluationSLA_NoSLA(t *testing.T) {
	d := testDeps(t)
	seedPipeline(t, d, types.PipelineConfig{Name: "pipe-a"})

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "checkEvaluationSLA",
		PipelineID: "pipe-a",
	})
	require.NoError(t, err)
	assert.Equal(t, "proceed", resp.Result)
	assert.Equal(t, false, resp.Payload["breached"])
}

func TestCheckEvaluationSLA_Breached(t *testing.T) {
	d := testDeps(t)
	var alerts []types.Alert
	d.AlertFn = func(a types.Alert) { alerts = append(alerts, a) }

	// Set deadline to 1 hour ago
	now := time.Now()
	deadline := now.Add(-1 * time.Hour).Format("15:04")
	seedPipeline(t, d, types.PipelineConfig{
		Name: "pipe-a",
		SLA:  &types.SLAConfig{EvaluationDeadline: deadline},
	})

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "checkEvaluationSLA",
		PipelineID: "pipe-a",
	})
	require.NoError(t, err)
	assert.Equal(t, "proceed", resp.Result)
	assert.Equal(t, true, resp.Payload["breached"])
	require.Len(t, alerts, 1)
	assert.Equal(t, "evaluation_sla_breach", alerts[0].Category)
}

// --- checkCompletionSLA ---

func TestCheckCompletionSLA_NoDeadline(t *testing.T) {
	d := testDeps(t)
	seedPipeline(t, d, types.PipelineConfig{Name: "pipe-a"})

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "checkCompletionSLA",
		PipelineID: "pipe-a",
		ScheduleID: "daily",
	})
	require.NoError(t, err)
	assert.Equal(t, "proceed", resp.Result)
	assert.Equal(t, false, resp.Payload["breached"])
}

// --- logResult ---

func TestLogResult(t *testing.T) {
	d := testDeps(t)

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "logResult",
		PipelineID: "pipe-a",
		ScheduleID: "daily",
		Payload: map[string]interface{}{
			"status": string(types.RunCompleted),
			"runID":  "run-1",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "proceed", resp.Result)
}

func TestLogResult_FailedWithCategory(t *testing.T) {
	d := testDeps(t)
	seedPipeline(t, d, types.PipelineConfig{Name: "pipe-a"})

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "logResult",
		PipelineID: "pipe-a",
		ScheduleID: "daily",
		Payload: map[string]interface{}{
			"status":          string(types.RunFailed),
			"runID":           "run-2",
			"message":         "glue timeout",
			"failureCategory": string(types.FailureTimeout),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "proceed", resp.Result)
	assert.Equal(t, true, resp.Payload["retryable"])

	// Verify category was persisted
	date := time.Now().UTC().Format("2006-01-02")
	entry, err := d.Provider.GetRunLog(context.Background(), "pipe-a", date, "daily")
	require.NoError(t, err)
	assert.Equal(t, types.FailureTimeout, entry.FailureCategory)
}

func TestLogResult_FailedEmptyCategoryDefaultsTransient(t *testing.T) {
	d := testDeps(t)
	seedPipeline(t, d, types.PipelineConfig{Name: "pipe-a"})

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "logResult",
		PipelineID: "pipe-a",
		ScheduleID: "daily",
		Payload: map[string]interface{}{
			"status":  string(types.RunFailed),
			"runID":   "run-3",
			"message": "unknown error",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "proceed", resp.Result)
	assert.Equal(t, true, resp.Payload["retryable"])

	// Verify empty category defaulted to transient
	date := time.Now().UTC().Format("2006-01-02")
	entry, err := d.Provider.GetRunLog(context.Background(), "pipe-a", date, "daily")
	require.NoError(t, err)
	assert.Equal(t, types.FailureTransient, entry.FailureCategory)
}

// --- releaseLock ---

func TestReleaseLock(t *testing.T) {
	d := testDeps(t)

	// Acquire then release
	_, _ = d.Provider.AcquireLock(context.Background(), "eval:pipe-a:daily", 5*time.Minute)

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "releaseLock",
		PipelineID: "pipe-a",
		ScheduleID: "daily",
	})
	require.NoError(t, err)
	assert.Equal(t, "proceed", resp.Result)

	// Lock should be released — can re-acquire
	acquired, _ := d.Provider.AcquireLock(context.Background(), "eval:pipe-a:daily", 5*time.Minute)
	assert.True(t, acquired)
}

// --- checkDrift ---

func TestCheckDrift_NoDrift(t *testing.T) {
	d := testDeps(t)

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "checkDrift",
		PipelineID: "pipe-a",
		Payload: map[string]interface{}{
			"runID": "run-1",
			"traitResults": []interface{}{
				map[string]interface{}{"traitType": "freshness", "originalStatus": "PASS", "currentStatus": "PASS"},
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "proceed", resp.Result)
	assert.Equal(t, false, resp.Payload["driftDetected"])
}

func TestCheckDrift_DriftDetected(t *testing.T) {
	d := testDeps(t)
	var alerts []types.Alert
	d.AlertFn = func(a types.Alert) { alerts = append(alerts, a) }

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "checkDrift",
		PipelineID: "pipe-a",
		Payload: map[string]interface{}{
			"runID": "run-1",
			"traitResults": []interface{}{
				map[string]interface{}{"traitType": "freshness", "originalStatus": "PASS", "currentStatus": "FAIL"},
				map[string]interface{}{"traitType": "schema", "originalStatus": "PASS", "currentStatus": "PASS"},
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "proceed", resp.Result)
	assert.Equal(t, true, resp.Payload["driftDetected"])
	require.Len(t, alerts, 1)
	assert.Equal(t, "trait_drift", alerts[0].Category)
}

// --- resolvePipeline ---

func TestResolvePipeline_Success(t *testing.T) {
	d := testDeps(t)

	// Register an archetype
	require.NoError(t, d.ArchetypeReg.Register(&types.Archetype{
		Name: "batch-etl",
		RequiredTraits: []types.TraitDefinition{
			{Type: "freshness", Description: "data freshness", DefaultTimeout: 30, DefaultTTL: 300},
			{Type: "schema", Description: "schema check", DefaultTimeout: 15, DefaultTTL: 600},
		},
		OptionalTraits: []types.TraitDefinition{
			{Type: "volume", Description: "row count check", DefaultTimeout: 10, DefaultTTL: 300},
		},
		ReadinessRule: types.ReadinessRule{Type: types.AllRequiredPass},
	}))

	seedPipeline(t, d, types.PipelineConfig{
		Name:      "pipe-a",
		Archetype: "batch-etl",
		Traits: map[string]types.TraitConfig{
			"freshness": {Evaluator: "check-freshness"},
			"schema":    {Evaluator: "check-schema"},
		},
		Trigger: &types.TriggerConfig{
			Type: types.TriggerHTTP,
			URL:  "https://example.com/trigger",
		},
	})

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "resolvePipeline",
		PipelineID: "pipe-a",
	})
	require.NoError(t, err)
	assert.Equal(t, "proceed", resp.Result)

	// Check traits
	traits, ok := resp.Payload["traits"].([]interface{})
	require.True(t, ok)
	assert.Len(t, traits, 2) // 2 required (optional "volume" not opted in)

	// Check runID is a UUID-like string
	runID, ok := resp.Payload["runID"].(string)
	require.True(t, ok)
	assert.Len(t, runID, 36) // UUID format: 8-4-4-4-12

	// Check trigger is present
	assert.NotNil(t, resp.Payload["trigger"])
}

func TestResolvePipeline_WithOptionalTrait(t *testing.T) {
	d := testDeps(t)

	require.NoError(t, d.ArchetypeReg.Register(&types.Archetype{
		Name: "batch-etl",
		RequiredTraits: []types.TraitDefinition{
			{Type: "freshness", Description: "data freshness"},
		},
		OptionalTraits: []types.TraitDefinition{
			{Type: "volume", Description: "row count check"},
		},
		ReadinessRule: types.ReadinessRule{Type: types.AllRequiredPass},
	}))

	seedPipeline(t, d, types.PipelineConfig{
		Name:      "pipe-a",
		Archetype: "batch-etl",
		Traits: map[string]types.TraitConfig{
			"freshness": {Evaluator: "check-freshness"},
			"volume":    {Evaluator: "check-volume"}, // opt in to optional trait
		},
	})

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "resolvePipeline",
		PipelineID: "pipe-a",
	})
	require.NoError(t, err)
	assert.Equal(t, "proceed", resp.Result)

	traits := resp.Payload["traits"].([]interface{})
	assert.Len(t, traits, 2) // freshness + volume

	// Verify trigger is absent when not configured
	assert.Nil(t, resp.Payload["trigger"])
}

func TestResolvePipeline_PipelineNotFound(t *testing.T) {
	d := testDeps(t)

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "resolvePipeline",
		PipelineID: "nonexistent",
	})
	require.NoError(t, err)
	assert.Equal(t, "error", resp.Result)
}

func TestResolvePipeline_NoArchetype(t *testing.T) {
	d := testDeps(t)
	seedPipeline(t, d, types.PipelineConfig{Name: "pipe-a"})

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "resolvePipeline",
		PipelineID: "pipe-a",
	})
	require.NoError(t, err)
	assert.Equal(t, "error", resp.Result)
}

func TestResolvePipeline_UnknownArchetype(t *testing.T) {
	d := testDeps(t)
	seedPipeline(t, d, types.PipelineConfig{
		Name:      "pipe-a",
		Archetype: "nonexistent-archetype",
	})

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "resolvePipeline",
		PipelineID: "pipe-a",
	})
	require.NoError(t, err)
	assert.Equal(t, "error", resp.Result)
}

func TestGenerateRunID(t *testing.T) {
	id, err := generateRunID()
	require.NoError(t, err)
	assert.Len(t, id, 36)
	// UUID v4 format check: position 14 should be '4'
	assert.Equal(t, byte('4'), id[14])

	// Generate multiple and check uniqueness
	ids := make(map[string]bool)
	for i := 0; i < 100; i++ {
		id, err := generateRunID()
		require.NoError(t, err)
		assert.False(t, ids[id], "duplicate run ID generated")
		ids[id] = true
	}
}

// --- errProvider wraps MockProvider to inject errors into specific methods ---

type errProvider struct {
	*testutil.MockProvider
	listPipelinesErr error
	putLateArrivalFn func(context.Context, types.LateArrival) error
}

func (e *errProvider) ListPipelines(ctx context.Context) ([]types.PipelineConfig, error) {
	if e.listPipelinesErr != nil {
		return nil, e.listPipelinesErr
	}
	return e.MockProvider.ListPipelines(ctx)
}

func (e *errProvider) PutLateArrival(ctx context.Context, entry types.LateArrival) error {
	if e.putLateArrivalFn != nil {
		return e.putLateArrivalFn(ctx, entry)
	}
	return e.MockProvider.PutLateArrival(ctx, entry)
}

// --- notifyDownstream ---

func TestNotifyDownstream_FoundDownstream(t *testing.T) {
	d := testDeps(t)
	mock := d.Provider.(*testutil.MockProvider)

	// Upstream pipeline
	seedPipeline(t, d, types.PipelineConfig{Name: "pipe-a"})

	// Downstream pipeline with upstreamPipeline trait pointing to pipe-a
	seedPipeline(t, d, types.PipelineConfig{
		Name: "pipe-b",
		Traits: map[string]types.TraitConfig{
			"upstream": {
				Evaluator: "check-upstream",
				Config:    map[string]interface{}{"upstreamPipeline": "pipe-a"},
			},
		},
	})

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "notifyDownstream",
		PipelineID: "pipe-a",
		ScheduleID: "daily",
	})
	require.NoError(t, err)
	assert.Equal(t, "proceed", resp.Result)

	notified, ok := resp.Payload["notified"].([]string)
	require.True(t, ok)
	assert.Contains(t, notified, "pipe-b/daily")

	// Verify cascade marker was written
	markers := mock.CascadeMarkers()
	require.Len(t, markers, 1)
	assert.Equal(t, "pipe-b", markers[0].PipelineID)
	assert.Equal(t, "daily", markers[0].ScheduleID)
	assert.Equal(t, "pipe-a", markers[0].Source)
}

func TestNotifyDownstream_NoDownstream(t *testing.T) {
	d := testDeps(t)

	seedPipeline(t, d, types.PipelineConfig{Name: "pipe-a"})

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "notifyDownstream",
		PipelineID: "pipe-a",
		ScheduleID: "daily",
	})
	require.NoError(t, err)
	assert.Equal(t, "proceed", resp.Result)

	// notified should be nil (no downstream found)
	assert.Nil(t, resp.Payload["notified"])
}

func TestNotifyDownstream_ListPipelinesError(t *testing.T) {
	mock := testutil.NewMockProvider()
	ep := &errProvider{
		MockProvider:     mock,
		listPipelinesErr: fmt.Errorf("dynamodb unavailable"),
	}
	d := &intlambda.Deps{
		Provider:     ep,
		Runner:       trigger.NewRunner(),
		ArchetypeReg: archetype.NewRegistry(),
		AlertFn:      func(a types.Alert) {},
		Logger:       slog.Default(),
	}

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "notifyDownstream",
		PipelineID: "pipe-a",
		ScheduleID: "daily",
	})
	require.NoError(t, err)
	assert.Equal(t, "error", resp.Result)
}

// --- checkValidationTimeout ---

func TestCheckValidationTimeout_NoSLA(t *testing.T) {
	d := testDeps(t)
	seedPipeline(t, d, types.PipelineConfig{Name: "pipe-a"})

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "checkValidationTimeout",
		PipelineID: "pipe-a",
	})
	require.NoError(t, err)
	assert.Equal(t, "proceed", resp.Result)
	assert.Equal(t, false, resp.Payload["validationTimedOut"])
}

func TestCheckValidationTimeout_NotBreached(t *testing.T) {
	d := testDeps(t)

	// Set validation timeout to 1 hour from now. Cap at 23:59 to avoid
	// wrapping past midnight (ParseSLADeadline anchors HH:MM to today).
	futureDeadline := time.Now().Add(1 * time.Hour)
	if futureDeadline.Day() != time.Now().Day() {
		futureDeadline = time.Date(time.Now().Year(), time.Now().Month(), time.Now().Day(), 23, 59, 0, 0, time.Now().Location())
	}
	deadline := futureDeadline.Format("15:04")
	seedPipeline(t, d, types.PipelineConfig{
		Name: "pipe-a",
		SLA:  &types.SLAConfig{ValidationTimeout: deadline},
	})

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "checkValidationTimeout",
		PipelineID: "pipe-a",
	})
	require.NoError(t, err)
	assert.Equal(t, "proceed", resp.Result)
	assert.Equal(t, false, resp.Payload["validationTimedOut"])
}

func TestCheckValidationTimeout_Breached(t *testing.T) {
	d := testDeps(t)
	var alerts []types.Alert
	d.AlertFn = func(a types.Alert) { alerts = append(alerts, a) }

	// Set validation timeout to 1 hour ago
	deadline := time.Now().Add(-1 * time.Hour).Format("15:04")
	seedPipeline(t, d, types.PipelineConfig{
		Name: "pipe-a",
		SLA:  &types.SLAConfig{ValidationTimeout: deadline},
	})

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "checkValidationTimeout",
		PipelineID: "pipe-a",
	})
	require.NoError(t, err)
	assert.Equal(t, "proceed", resp.Result)
	assert.Equal(t, true, resp.Payload["validationTimedOut"])
	require.Len(t, alerts, 1)
	assert.Equal(t, types.AlertLevelError, alerts[0].Level)
	assert.Equal(t, "validation_timeout", alerts[0].Category)
}

// --- checkMonitoringExpired ---

func TestCheckMonitoringExpired_NoMonitoringConfig(t *testing.T) {
	d := testDeps(t)
	seedPipeline(t, d, types.PipelineConfig{Name: "pipe-a"})

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "checkMonitoringExpired",
		PipelineID: "pipe-a",
	})
	require.NoError(t, err)
	assert.Equal(t, "proceed", resp.Result)
	assert.Equal(t, true, resp.Payload["expired"])
}

func TestCheckMonitoringExpired_NotExpired(t *testing.T) {
	d := testDeps(t)
	seedPipeline(t, d, types.PipelineConfig{
		Name: "pipe-a",
		Watch: &types.PipelineWatchConfig{
			Monitoring: &types.MonitoringConfig{
				Enabled:  true,
				Duration: "2h",
			},
		},
	})

	startedAt := time.Now().Add(-1 * time.Minute).Format(time.RFC3339)

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "checkMonitoringExpired",
		PipelineID: "pipe-a",
		Payload: map[string]interface{}{
			"monitoringStartedAt": startedAt,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "proceed", resp.Result)
	assert.Equal(t, false, resp.Payload["expired"])
}

func TestCheckMonitoringExpired_Expired(t *testing.T) {
	d := testDeps(t)
	seedPipeline(t, d, types.PipelineConfig{
		Name: "pipe-a",
		Watch: &types.PipelineWatchConfig{
			Monitoring: &types.MonitoringConfig{
				Enabled:  true,
				Duration: "2h",
			},
		},
	})

	startedAt := time.Now().Add(-3 * time.Hour).Format(time.RFC3339)

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "checkMonitoringExpired",
		PipelineID: "pipe-a",
		Payload: map[string]interface{}{
			"monitoringStartedAt": startedAt,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "proceed", resp.Result)
	assert.Equal(t, true, resp.Payload["expired"])
}

// --- handleLateArrival ---

func TestHandleLateArrival_StoresAndCascades(t *testing.T) {
	d := testDeps(t)
	mock := d.Provider.(*testutil.MockProvider)

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "handleLateArrival",
		PipelineID: "pipe-a",
		ScheduleID: "daily",
		Payload: map[string]interface{}{
			"drifted": []interface{}{"freshness", "schema"},
			"runID":   "run-42",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "proceed", resp.Result)
	assert.Equal(t, true, resp.Payload["lateArrivalHandled"])

	// Verify late arrivals were stored
	date := time.Now().UTC().Format("2006-01-02")
	arrivals, err := d.Provider.ListLateArrivals(context.Background(), "pipe-a", date, "daily")
	require.NoError(t, err)
	assert.Len(t, arrivals, 2)

	traitTypes := []string{arrivals[0].TraitType, arrivals[1].TraitType}
	assert.Contains(t, traitTypes, "freshness")
	assert.Contains(t, traitTypes, "schema")

	// Verify rerun record was created
	reruns := mock.Reruns()
	require.Len(t, reruns, 1)
	assert.Equal(t, "rerun-run-42", reruns[0].RerunID)
	assert.Equal(t, "pipe-a", reruns[0].PipelineID)
	assert.Equal(t, types.RunPending, reruns[0].Status)

	// Verify cascade marker was written for self-re-evaluation
	markers := mock.CascadeMarkers()
	require.Len(t, markers, 1)
	assert.Equal(t, "pipe-a", markers[0].PipelineID)
	assert.Equal(t, "daily", markers[0].ScheduleID)
	assert.Equal(t, "late-arrival", markers[0].Source)
}

func TestHandleLateArrival_PutLateArrivalError(t *testing.T) {
	mock := testutil.NewMockProvider()
	ep := &errProvider{
		MockProvider: mock,
		putLateArrivalFn: func(_ context.Context, _ types.LateArrival) error {
			return fmt.Errorf("storage write failed")
		},
	}
	d := &intlambda.Deps{
		Provider:     ep,
		Runner:       trigger.NewRunner(),
		ArchetypeReg: archetype.NewRegistry(),
		AlertFn:      func(a types.Alert) {},
		Logger:       slog.Default(),
	}

	resp, err := handleOrchestrator(context.Background(), d, intlambda.OrchestratorRequest{
		Action:     "handleLateArrival",
		PipelineID: "pipe-a",
		ScheduleID: "daily",
		Payload: map[string]interface{}{
			"drifted": []interface{}{"freshness"},
			"runID":   "run-99",
		},
	})
	require.NoError(t, err)
	// Should still proceed despite PutLateArrival errors
	assert.Equal(t, "proceed", resp.Result)
	assert.Equal(t, true, resp.Payload["lateArrivalHandled"])

	// Rerun and cascade should still be written (those go through MockProvider, not errProvider)
	reruns := mock.Reruns()
	require.Len(t, reruns, 1)
	assert.Equal(t, "rerun-run-99", reruns[0].RerunID)
}

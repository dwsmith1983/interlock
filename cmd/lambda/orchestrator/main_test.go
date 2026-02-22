package main

import (
	"context"
	"log/slog"
	"testing"
	"time"

	intlambda "github.com/interlock-systems/interlock/internal/lambda"
	"github.com/interlock-systems/interlock/internal/testutil"
	"github.com/interlock-systems/interlock/internal/trigger"
	"github.com/interlock-systems/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testDeps(t *testing.T) *intlambda.Deps {
	t.Helper()
	prov := testutil.NewMockProvider()
	return &intlambda.Deps{
		Provider: prov,
		Runner:   trigger.NewRunner(),
		AlertFn:  func(a types.Alert) {},
		Logger:   slog.Default(),
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
	assert.Equal(t, "skip", resp.Result)
	blocking := resp.Payload["blocking"].([]string)
	assert.Contains(t, blocking, "schema")
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
	assert.Len(t, alerts, 1)
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
	assert.Len(t, alerts, 1)
}

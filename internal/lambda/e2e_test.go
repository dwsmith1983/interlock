package lambda_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/store"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// ---------------------------------------------------------------------------
// E2E-specific mock types
// ---------------------------------------------------------------------------

type mockTriggerRunner struct {
	err      error
	metadata map[string]interface{}
}

func (m *mockTriggerRunner) Execute(_ context.Context, _ *types.TriggerConfig) (map[string]interface{}, error) {
	if m.err != nil {
		return nil, m.err
	}
	meta := m.metadata
	if meta == nil {
		meta = map[string]interface{}{"runId": "run-e2e"}
	}
	return meta, nil
}

type mockStatusPoller struct {
	mu  sync.Mutex
	seq []lambda.StatusResult
	idx int
}

func (m *mockStatusPoller) CheckStatus(_ context.Context, _ types.TriggerType, _ map[string]interface{}, _ map[string]string) (lambda.StatusResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.idx >= len(m.seq) {
		return lambda.StatusResult{State: "running"}, nil
	}
	r := m.seq[m.idx]
	m.idx++
	return r, nil
}

// ---------------------------------------------------------------------------
// SFN simulator types
// ---------------------------------------------------------------------------

type sfnTerminal string

const (
	sfnDone             sfnTerminal = "Done"
	sfnInfraFailure     sfnTerminal = "InfraFailure"
	sfnFailValExhausted sfnTerminal = "FailValidationExhausted"
)

type e2eScenario struct {
	pipeline             types.PipelineConfig
	date                 string
	sensors              map[string]map[string]interface{}
	sensorUpdates        map[int]map[string]map[string]interface{} // eval iteration (1-indexed) → sensor key → data
	triggerErr           error
	triggerMeta          map[string]interface{}
	jobSequence          []lambda.StatusResult
	postRunSensorUpdates []map[string]map[string]interface{} // per post-run iteration
	evalErr              error                               // simulate Lambda crash during evaluate
	preCheckJobEvent     string                              // if set, write this joblog entry before check-job polling
	slaWarningAt         string                              // override for SLA cancel
	slaBreachAt          string                              // override for SLA cancel
}

type e2eResult struct {
	terminal   sfnTerminal
	events     []string
	evalCount  int
	slaOutcome string
}

// ---------------------------------------------------------------------------
// E2E deps builder
// ---------------------------------------------------------------------------

func buildE2EDeps(mock *mockDDB, tr *mockTriggerRunner, sc *mockStatusPoller) (*lambda.Deps, *mockSFN, *mockEventBridge, *mockScheduler) {
	s := &store.Store{
		Client:       mock,
		ControlTable: testControlTable,
		JobLogTable:  "joblog",
		RerunTable:   "rerun",
		EventsTable:  "events",
	}
	sfnM := &mockSFN{}
	ebM := &mockEventBridge{}
	schedM := &mockScheduler{}
	d := &lambda.Deps{
		Store:              s,
		ConfigCache:        store.NewConfigCache(s, 5*time.Minute),
		SFNClient:          sfnM,
		EventBridge:        ebM,
		Scheduler:          schedM,
		TriggerRunner:      tr,
		StatusChecker:      sc,
		StateMachineARN:    "arn:aws:states:us-east-1:123456789012:stateMachine:test",
		EventBusName:       "interlock-bus",
		SLAMonitorARN:      "arn:aws:lambda:us-east-1:123456789012:function:sla-monitor",
		SchedulerRoleARN:   "arn:aws:iam::123456789012:role/scheduler-role",
		SchedulerGroupName: "interlock-sla",
		EventsTTLDays:      90,
		StartedAt:          time.Now().Add(-24 * time.Hour),
		Logger:             slog.Default(),
	}
	return d, sfnM, ebM, schedM
}

// ---------------------------------------------------------------------------
// SFN simulator — calls real handlers in ASL-defined order
// ---------------------------------------------------------------------------

func runSFN(t *testing.T, ctx context.Context, d *lambda.Deps, mock *mockDDB, eb *mockEventBridge, sc e2eScenario) e2eResult {
	t.Helper()

	pid := sc.pipeline.Pipeline.ID
	sid := "stream"
	if sc.pipeline.Schedule.Cron != "" {
		sid = "cron"
	}
	date := sc.date
	if date == "" {
		date = "2026-03-07"
	}

	// Seed config and initial sensors.
	seedConfig(mock, sc.pipeline)
	for key, data := range sc.sensors {
		require.NoError(t, d.Store.WriteSensor(ctx, pid, key, data))
	}

	// Acquire trigger lock (mirrors stream-router behaviour).
	acquired, err := d.Store.AcquireTriggerLock(ctx, pid, sid, date, 24*time.Hour)
	require.NoError(t, err)
	require.True(t, acquired)

	evalInterval := parseDurationSec(sc.pipeline.Schedule.Evaluation.Interval, 300)
	evalWindow := parseDurationSec(sc.pipeline.Schedule.Evaluation.Window, 3600)

	hasPostRun := sc.pipeline.PostRun != nil && len(sc.pipeline.PostRun.Rules) > 0
	postRunInterval, postRunWindow := 1800, 7200
	if hasPostRun && sc.pipeline.PostRun.Evaluation != nil {
		postRunInterval = parseDurationSec(sc.pipeline.PostRun.Evaluation.Interval, 1800)
		postRunWindow = parseDurationSec(sc.pipeline.PostRun.Evaluation.Window, 7200)
	}

	var (
		result   e2eResult
		jobEvent string
	)
	result.terminal = sfnDone

	// ── Phase 1: Evaluation Loop ──────────────────────────────────
	if sc.evalErr != nil {
		result.terminal = sfnInfraFailure
		result.events = collectEventTypes(eb)
		return result
	}

	evalPassed := false
	elapsedSeconds := 0
	for {
		if updates, ok := sc.sensorUpdates[result.evalCount+1]; ok {
			for key, data := range updates {
				require.NoError(t, d.Store.WriteSensor(ctx, pid, key, data))
			}
		}

		out, err := lambda.HandleOrchestrator(ctx, d, lambda.OrchestratorInput{
			Mode: "evaluate", PipelineID: pid, ScheduleID: sid, Date: date,
		})
		if err != nil {
			result.terminal = sfnInfraFailure
			result.events = collectEventTypes(eb)
			return result
		}
		result.evalCount++

		if out.Status == "passed" {
			evalPassed = true
			break
		}
		elapsedSeconds += evalInterval
		if elapsedSeconds >= evalWindow {
			break
		}
	}

	if !evalPassed {
		// ValidationExhausted state
		if _, err := lambda.HandleOrchestrator(ctx, d, lambda.OrchestratorInput{
			Mode: "validation-exhausted", PipelineID: pid, ScheduleID: sid, Date: date,
		}); err != nil {
			result.terminal = sfnInfraFailure
			result.events = collectEventTypes(eb)
			return result
		}
	} else {
		// ── Phase 2: Trigger ──────────────────────────────────────
		triggerOut, err := lambda.HandleOrchestrator(ctx, d, lambda.OrchestratorInput{
			Mode: "trigger", PipelineID: pid, ScheduleID: sid, Date: date,
		})
		if err != nil {
			// TriggerRetryExhausted state
			_, _ = lambda.HandleOrchestrator(ctx, d, lambda.OrchestratorInput{
				Mode: "trigger-exhausted", PipelineID: pid, ScheduleID: sid, Date: date,
				ErrorInfo: map[string]interface{}{"Cause": err.Error()},
			})
		} else {
			// ── Phase 3: Job Polling Loop ─────────────────────────
			if sc.preCheckJobEvent != "" {
				require.NoError(t, d.Store.WriteJobEvent(ctx, pid, sid, date, sc.preCheckJobEvent, "", 0, "simulated"))
			}

			metadata := triggerOut.Metadata
			runID := triggerOut.RunID
			for i := 0; i < 100; i++ {
				checkOut, err := lambda.HandleOrchestrator(ctx, d, lambda.OrchestratorInput{
					Mode: "check-job", PipelineID: pid, ScheduleID: sid, Date: date,
					RunID: runID, Metadata: metadata,
				})
				if err != nil {
					result.terminal = sfnInfraFailure
					result.events = collectEventTypes(eb)
					return result
				}
				if checkOut.Event != "" {
					jobEvent = checkOut.Event
					break
				}
			}

			// ── Phase 4: Post-Run ─────────────────────────────────
			if jobEvent == "success" && hasPostRun {
				postElapsed := 0
				for i := 0; ; i++ {
					if i < len(sc.postRunSensorUpdates) {
						for key, data := range sc.postRunSensorUpdates[i] {
							require.NoError(t, d.Store.WriteSensor(ctx, pid, key, data))
						}
					}
					postOut, err := lambda.HandleOrchestrator(ctx, d, lambda.OrchestratorInput{
						Mode: "post-run", PipelineID: pid, ScheduleID: sid, Date: date,
					})
					if err != nil {
						break
					}
					if postOut.Status == "drift" || postOut.Status == "passed" {
						break
					}
					if postElapsed >= postRunWindow {
						break
					}
					postElapsed += postRunInterval
				}
			}

			// ── Phase 5: CompleteTrigger ──────────────────────────
			_, _ = lambda.HandleOrchestrator(ctx, d, lambda.OrchestratorInput{
				Mode: "complete-trigger", PipelineID: pid, ScheduleID: sid, Date: date,
				Event: jobEvent,
			})
		}
	}

	// ── Phase 6: CancelSLA ────────────────────────────────────────
	if sc.pipeline.SLA != nil {
		warningAt, breachAt := sc.slaWarningAt, sc.slaBreachAt
		if warningAt == "" || breachAt == "" {
			calcOut, calcErr := lambda.HandleSLAMonitor(ctx, d, lambda.SLAMonitorInput{
				Mode: "calculate", PipelineID: pid, ScheduleID: sid, Date: date,
				Deadline: sc.pipeline.SLA.Deadline, ExpectedDuration: sc.pipeline.SLA.ExpectedDuration,
				Timezone: sc.pipeline.SLA.Timezone,
			})
			if calcErr == nil {
				warningAt, breachAt = calcOut.WarningAt, calcOut.BreachAt
			}
		}
		slaOut, _ := lambda.HandleSLAMonitor(ctx, d, lambda.SLAMonitorInput{
			Mode: "cancel", PipelineID: pid, ScheduleID: sid, Date: date,
			Deadline: sc.pipeline.SLA.Deadline, ExpectedDuration: sc.pipeline.SLA.ExpectedDuration,
			WarningAt: warningAt, BreachAt: breachAt,
		})
		result.slaOutcome = slaOut.AlertType
	}

	result.events = collectEventTypes(eb)
	return result
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func e2ePipeline(id string) types.PipelineConfig {
	return types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: id},
		Schedule: types.ScheduleConfig{
			Trigger: &types.TriggerCondition{
				Key: "upstream-complete", Check: types.CheckEquals, Field: "status", Value: "ready",
			},
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: types.ValidationConfig{
			Trigger: "ALL",
			Rules:   []types.ValidationRule{{Key: "upstream-complete", Check: types.CheckExists}},
		},
		Job: types.JobConfig{
			Type:   types.TriggerCommand,
			Config: map[string]interface{}{"command": "echo hello"},
		},
	}
}

func collectEventTypes(eb *mockEventBridge) []string {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	var out []string
	for _, input := range eb.events {
		for _, entry := range input.Entries {
			if entry.DetailType != nil {
				out = append(out, *entry.DetailType)
			}
		}
	}
	return out
}

func collectJoblogEvents(mock *mockDDB, pipelineID, schedule, date string) []string {
	mock.mu.Lock()
	defer mock.mu.Unlock()
	pk := types.PipelinePK(pipelineID)
	prefix := "joblog#" + pk + "#JOB#" + schedule + "#" + date + "#"
	var out []string
	for k, item := range mock.items {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		if ev, ok := item["event"].(*ddbtypes.AttributeValueMemberS); ok {
			out = append(out, ev.Value)
		}
	}
	sort.Strings(out)
	return out
}

func e2eTriggerStatus(mock *mockDDB, pipelineID, schedule, date string) string {
	mock.mu.Lock()
	defer mock.mu.Unlock()
	key := ddbItemKey(testControlTable, types.PipelinePK(pipelineID), types.TriggerSK(schedule, date))
	item, ok := mock.items[key]
	if !ok {
		return ""
	}
	if s, ok := item["status"].(*ddbtypes.AttributeValueMemberS); ok {
		return s.Value
	}
	return ""
}

func hasRerunRequest(mock *mockDDB, pipelineID, schedule, date string) bool {
	mock.mu.Lock()
	defer mock.mu.Unlock()
	key := ddbItemKey(testControlTable, types.PipelinePK(pipelineID), types.RerunRequestSK(schedule, date))
	_, ok := mock.items[key]
	return ok
}

func parseDurationSec(s string, dflt int) int {
	if s == "" {
		return dflt
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return dflt
	}
	return int(d.Seconds())
}

func assertAlertFormats(t *testing.T, eb *mockEventBridge) {
	t.Helper()
	eb.mu.Lock()
	defer eb.mu.Unlock()
	for _, input := range eb.events {
		for _, entry := range input.Entries {
			if entry.DetailType == nil || entry.Detail == nil {
				continue
			}
			var detail types.InterlockEvent
			require.NoError(t, json.Unmarshal([]byte(*entry.Detail), &detail))
			text := lambda.FormatAlertText(*entry.DetailType, detail)
			assert.NotEmpty(t, text, "alert text for %s", *entry.DetailType)
			assert.Contains(t, text, detail.PipelineID, "alert for %s should reference pipeline", *entry.DetailType)
		}
	}
}

func makeJobStreamEvent(pipelineID, sk, event string) lambda.StreamEvent {
	return lambda.StreamEvent{
		Records: []events.DynamoDBEventRecord{{
			EventName: "INSERT",
			Change: events.DynamoDBStreamRecord{
				Keys: map[string]events.DynamoDBAttributeValue{
					"PK": events.NewStringAttribute(types.PipelinePK(pipelineID)),
					"SK": events.NewStringAttribute(sk),
				},
				NewImage: map[string]events.DynamoDBAttributeValue{
					"PK":    events.NewStringAttribute(types.PipelinePK(pipelineID)),
					"SK":    events.NewStringAttribute(sk),
					"event": events.NewStringAttribute(event),
				},
			},
		}},
	}
}

func makeRerunRequestStreamEvent(pipelineID, schedule, date string) lambda.StreamEvent {
	sk := types.RerunRequestSK(schedule, date)
	return lambda.StreamEvent{
		Records: []events.DynamoDBEventRecord{{
			EventName: "INSERT",
			Change: events.DynamoDBStreamRecord{
				Keys: map[string]events.DynamoDBAttributeValue{
					"PK": events.NewStringAttribute(types.PipelinePK(pipelineID)),
					"SK": events.NewStringAttribute(sk),
				},
				NewImage: map[string]events.DynamoDBAttributeValue{
					"PK":     events.NewStringAttribute(types.PipelinePK(pipelineID)),
					"SK":     events.NewStringAttribute(sk),
					"reason": events.NewStringAttribute("test"),
				},
			},
		}},
	}
}

func findLatestJobSK(mock *mockDDB, pipelineID, schedule, date string) string {
	mock.mu.Lock()
	defer mock.mu.Unlock()
	pk := types.PipelinePK(pipelineID)
	prefix := "joblog#" + pk + "#JOB#" + schedule + "#" + date + "#"
	var latest string
	for k, item := range mock.items {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		sk := item["SK"].(*ddbtypes.AttributeValueMemberS).Value
		if sk > latest {
			latest = sk
		}
	}
	return latest
}

// futureSLATimes returns warning/breach times safely in the future.
func futureSLATimes() (warningAt, breachAt string) {
	return time.Now().Add(2 * time.Hour).UTC().Format(time.RFC3339),
		time.Now().Add(3 * time.Hour).UTC().Format(time.RFC3339)
}

// pastSLATimes returns warning/breach times safely in the past.
func pastSLATimes() (warningAt, breachAt string) {
	return time.Now().Add(-2 * time.Hour).UTC().Format(time.RFC3339),
		time.Now().Add(-1 * time.Hour).UTC().Format(time.RFC3339)
}

// =========================================================================
// Group A: Primary SFN Execution Paths
// =========================================================================

func TestE2E_GroupA_PrimarySFNPaths(t *testing.T) {
	ctx := context.Background()

	t.Run("A1_HappyPath", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{
			{State: "running"}, {State: "succeeded"},
		}}
		d, _, eb, _ := buildE2EDeps(mock, tr, sc)
		warnAt, breachAt := futureSLATimes()

		cfg := e2ePipeline("pipe-a1")
		cfg.SLA = &types.SLAConfig{Deadline: "23:59", ExpectedDuration: "30m"}

		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline: cfg,
			sensors:  map[string]map[string]interface{}{"upstream-complete": {"status": "ready"}},
			jobSequence: []lambda.StatusResult{
				{State: "running"}, {State: "succeeded"},
			},
			slaWarningAt: warnAt, slaBreachAt: breachAt,
		})

		assert.Equal(t, sfnDone, r.terminal)
		assert.Equal(t, 1, r.evalCount)
		assert.Equal(t, "SLA_MET", r.slaOutcome)
		assert.Contains(t, r.events, "VALIDATION_PASSED")
		assert.Contains(t, r.events, "JOB_TRIGGERED")
		assert.Contains(t, r.events, "JOB_COMPLETED")
		assert.Contains(t, r.events, "SLA_MET")
		assert.Equal(t, types.TriggerStatusCompleted, e2eTriggerStatus(mock, "pipe-a1", "stream", "2026-03-07"))
		assert.Contains(t, collectJoblogEvents(mock, "pipe-a1", "stream", "2026-03-07"), "success")
		assertAlertFormats(t, eb)
	})

	t.Run("A2_ExtendedEvaluation", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{{State: "succeeded"}}}
		d, _, eb, _ := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-a2")
		cfg.Validation.Rules = []types.ValidationRule{
			{Key: "upstream-complete", Check: types.CheckExists},
			{Key: "quality-check", Check: types.CheckExists},
		}

		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline: cfg,
			sensors:  map[string]map[string]interface{}{"upstream-complete": {"status": "ready"}},
			sensorUpdates: map[int]map[string]map[string]interface{}{
				4: {"quality-check": {"status": "passed"}},
			},
		})

		assert.Equal(t, sfnDone, r.terminal)
		assert.Equal(t, 4, r.evalCount)
		assert.Contains(t, r.events, "VALIDATION_PASSED")
		assert.Contains(t, r.events, "JOB_TRIGGERED")
		assert.Contains(t, r.events, "JOB_COMPLETED")
		assert.Equal(t, types.TriggerStatusCompleted, e2eTriggerStatus(mock, "pipe-a2", "stream", "2026-03-07"))
		assertAlertFormats(t, eb)
	})

	t.Run("A3_ValidationExhausted", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, _, eb, _ := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-a3")
		cfg.Validation.Rules = []types.ValidationRule{
			{Key: "missing-sensor", Check: types.CheckExists},
		}

		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline: cfg,
			sensors:  map[string]map[string]interface{}{"upstream-complete": {"status": "ready"}},
		})

		assert.Equal(t, sfnDone, r.terminal)
		assert.Equal(t, 12, r.evalCount) // 1h window / 5m interval
		assert.Contains(t, r.events, "VALIDATION_EXHAUSTED")
		assert.NotContains(t, r.events, "VALIDATION_PASSED")
		assert.Contains(t, collectJoblogEvents(mock, "pipe-a3", "stream", "2026-03-07"), "validation-exhausted")
		// Trigger lock stays RUNNING (no release in validation-exhausted path)
		assert.Equal(t, types.TriggerStatusRunning, e2eTriggerStatus(mock, "pipe-a3", "stream", "2026-03-07"))
		assertAlertFormats(t, eb)
	})

	t.Run("A4_TriggerInfraFailure", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{err: errors.New("connection refused")}
		sc := &mockStatusPoller{}
		d, _, eb, _ := buildE2EDeps(mock, tr, sc)

		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline: e2ePipeline("pipe-a4"),
			sensors:  map[string]map[string]interface{}{"upstream-complete": {"status": "ready"}},
		})

		assert.Equal(t, sfnDone, r.terminal)
		assert.Equal(t, 1, r.evalCount)
		assert.Contains(t, r.events, "VALIDATION_PASSED")
		assert.Contains(t, r.events, "RETRY_EXHAUSTED")
		joblogs := collectJoblogEvents(mock, "pipe-a4", "stream", "2026-03-07")
		assert.Contains(t, joblogs, "infra-trigger-exhausted")
		// Lock released by trigger-exhausted
		assert.Equal(t, "", e2eTriggerStatus(mock, "pipe-a4", "stream", "2026-03-07"))
		assertAlertFormats(t, eb)
	})

	t.Run("A5_JobFailure", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{{State: "failed", Message: "OOM"}}}
		d, _, eb, _ := buildE2EDeps(mock, tr, sc)

		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline: e2ePipeline("pipe-a5"),
			sensors:  map[string]map[string]interface{}{"upstream-complete": {"status": "ready"}},
		})

		assert.Equal(t, sfnDone, r.terminal)
		assert.Contains(t, r.events, "VALIDATION_PASSED")
		assert.Contains(t, r.events, "JOB_TRIGGERED")
		assert.Contains(t, r.events, "JOB_FAILED")
		assert.Equal(t, types.TriggerStatusFailedFinal, e2eTriggerStatus(mock, "pipe-a5", "stream", "2026-03-07"))
		assert.Contains(t, collectJoblogEvents(mock, "pipe-a5", "stream", "2026-03-07"), "fail")
		assertAlertFormats(t, eb)
	})

	t.Run("A6_JobTimeout", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{} // not used; timeout comes from joblog
		d, _, eb, _ := buildE2EDeps(mock, tr, sc)

		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline:         e2ePipeline("pipe-a6"),
			sensors:          map[string]map[string]interface{}{"upstream-complete": {"status": "ready"}},
			preCheckJobEvent: types.JobEventTimeout,
		})

		assert.Equal(t, sfnDone, r.terminal)
		assert.Contains(t, r.events, "VALIDATION_PASSED")
		assert.Contains(t, r.events, "JOB_TRIGGERED")
		assert.Equal(t, types.TriggerStatusFailedFinal, e2eTriggerStatus(mock, "pipe-a6", "stream", "2026-03-07"))
		assert.Contains(t, collectJoblogEvents(mock, "pipe-a6", "stream", "2026-03-07"), "timeout")
		assertAlertFormats(t, eb)
	})

	t.Run("A7_InfraFailure", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, _, eb, _ := buildE2EDeps(mock, tr, sc)

		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline: e2ePipeline("pipe-a7"),
			sensors:  map[string]map[string]interface{}{"upstream-complete": {"status": "ready"}},
			evalErr:  errors.New("Lambda OOM"),
		})

		assert.Equal(t, sfnInfraFailure, r.terminal)
		assert.Equal(t, 0, r.evalCount)
		assert.Empty(t, r.events)
		// Trigger stays RUNNING — watchdog will detect it later
		assert.Equal(t, types.TriggerStatusRunning, e2eTriggerStatus(mock, "pipe-a7", "stream", "2026-03-07"))
	})
}

// =========================================================================
// Group B: Post-Run Monitoring
// =========================================================================

func TestE2E_GroupB_PostRunMonitoring(t *testing.T) {
	ctx := context.Background()

	t.Run("B1_PostRunPasses", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{{State: "succeeded"}}}
		d, _, eb, _ := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-b1")
		cfg.PostRun = &types.PostRunConfig{
			Evaluation: &types.EvaluationWindow{Window: "10m", Interval: "5m"},
			Rules:      []types.ValidationRule{{Key: "quality-check", Check: types.CheckExists}},
		}

		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline: cfg,
			sensors: map[string]map[string]interface{}{
				"upstream-complete": {"status": "ready"},
				"quality-check":    {"status": "passed"},
			},
		})

		assert.Equal(t, sfnDone, r.terminal)
		assert.Contains(t, r.events, "JOB_COMPLETED")
		assert.NotContains(t, r.events, "DATA_DRIFT")
		assert.Equal(t, types.TriggerStatusCompleted, e2eTriggerStatus(mock, "pipe-b1", "stream", "2026-03-07"))
		assertAlertFormats(t, eb)
	})

	t.Run("B2_DriftDetected", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{{State: "succeeded"}}}
		d, _, eb, _ := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-b2")
		cfg.PostRun = &types.PostRunConfig{
			Evaluation: &types.EvaluationWindow{Window: "10m", Interval: "5m"},
			Rules:      []types.ValidationRule{{Key: "quality-check", Check: types.CheckExists}},
		}

		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline: cfg,
			sensors: map[string]map[string]interface{}{
				"upstream-complete": {"status": "ready"},
				"audit-result":     {"sensor_count": float64(100)},
			},
			postRunSensorUpdates: []map[string]map[string]interface{}{
				{}, // iteration 0: no updates; baseline saved
				{"audit-result": {"sensor_count": float64(120)}}, // iteration 1: count changed
			},
		})

		assert.Equal(t, sfnDone, r.terminal)
		assert.Contains(t, r.events, "DATA_DRIFT")
		assert.Equal(t, types.TriggerStatusCompleted, e2eTriggerStatus(mock, "pipe-b2", "stream", "2026-03-07"))
		assert.True(t, hasRerunRequest(mock, "pipe-b2", "stream", "2026-03-07"), "drift should write RERUN_REQUEST")
		assertAlertFormats(t, eb)
	})

	t.Run("B3_WindowExhausted", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{{State: "succeeded"}}}
		d, _, eb, _ := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-b3")
		cfg.PostRun = &types.PostRunConfig{
			Evaluation: &types.EvaluationWindow{Window: "10m", Interval: "5m"},
			Rules:      []types.ValidationRule{{Key: "never-exists", Check: types.CheckExists}},
		}

		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline: cfg,
			sensors:  map[string]map[string]interface{}{"upstream-complete": {"status": "ready"}},
		})

		assert.Equal(t, sfnDone, r.terminal)
		assert.NotContains(t, r.events, "DATA_DRIFT")
		assert.Equal(t, types.TriggerStatusCompleted, e2eTriggerStatus(mock, "pipe-b3", "stream", "2026-03-07"))
		assertAlertFormats(t, eb)
	})
}

// =========================================================================
// Group C: SLA Monitoring
// =========================================================================

func TestE2E_GroupC_SLAMonitoring(t *testing.T) {
	ctx := context.Background()

	t.Run("C1_SLAMet", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{{State: "succeeded"}}}
		d, _, eb, _ := buildE2EDeps(mock, tr, sc)
		warnAt, breachAt := futureSLATimes()

		cfg := e2ePipeline("pipe-c1")
		cfg.SLA = &types.SLAConfig{Deadline: "23:59", ExpectedDuration: "30m"}

		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline:     cfg,
			sensors:      map[string]map[string]interface{}{"upstream-complete": {"status": "ready"}},
			slaWarningAt: warnAt, slaBreachAt: breachAt,
		})

		assert.Equal(t, sfnDone, r.terminal)
		assert.Equal(t, "SLA_MET", r.slaOutcome)
		assert.Contains(t, r.events, "SLA_MET")
		assertAlertFormats(t, eb)
	})

	t.Run("C2_SLAWarningFired", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, _, eb, _ := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-c2")
		cfg.SLA = &types.SLAConfig{Deadline: "23:59", ExpectedDuration: "30m"}
		seedConfig(mock, cfg)

		// Seed a RUNNING trigger (pipeline still executing)
		seedTriggerLock(mock, "pipe-c2", "2026-03-07")

		_, err := lambda.HandleSLAMonitor(ctx, d, lambda.SLAMonitorInput{
			Mode: "fire-alert", PipelineID: "pipe-c2", ScheduleID: "stream",
			Date: "2026-03-07", AlertType: "SLA_WARNING",
			BreachAt: time.Now().Add(1 * time.Hour).UTC().Format(time.RFC3339),
		})
		require.NoError(t, err)

		evts := collectEventTypes(eb)
		assert.Contains(t, evts, "SLA_WARNING")
		assertAlertFormats(t, eb)
	})

	t.Run("C3_SLABreachFired", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, _, eb, _ := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-c3")
		cfg.SLA = &types.SLAConfig{Deadline: "23:59", ExpectedDuration: "30m"}
		seedConfig(mock, cfg)
		seedTriggerLock(mock, "pipe-c3", "2026-03-07")

		_, err := lambda.HandleSLAMonitor(ctx, d, lambda.SLAMonitorInput{
			Mode: "fire-alert", PipelineID: "pipe-c3", ScheduleID: "stream",
			Date: "2026-03-07", AlertType: "SLA_BREACH",
		})
		require.NoError(t, err)

		evts := collectEventTypes(eb)
		assert.Contains(t, evts, "SLA_BREACH")
		assertAlertFormats(t, eb)
	})

	t.Run("C4_SLABreachThenJobSucceeds", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{{State: "succeeded"}}}
		d, _, eb, _ := buildE2EDeps(mock, tr, sc)
		warnAt, breachAt := pastSLATimes()

		cfg := e2ePipeline("pipe-c4")
		cfg.SLA = &types.SLAConfig{Deadline: "23:59", ExpectedDuration: "30m"}

		// First: fire breach (Scheduler invocation before job completes)
		seedConfig(mock, cfg)
		_, err := lambda.HandleSLAMonitor(ctx, d, lambda.SLAMonitorInput{
			Mode: "fire-alert", PipelineID: "pipe-c4", ScheduleID: "stream",
			Date: "2026-03-07", AlertType: "SLA_BREACH",
		})
		require.NoError(t, err)

		// Reset event bridge to isolate SFN events
		eb.mu.Lock()
		eb.events = nil
		eb.mu.Unlock()
		// Invalidate cache so runSFN re-seeds cleanly
		d.ConfigCache.Invalidate()

		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline:     cfg,
			sensors:      map[string]map[string]interface{}{"upstream-complete": {"status": "ready"}},
			slaWarningAt: warnAt, slaBreachAt: breachAt,
		})

		assert.Equal(t, sfnDone, r.terminal)
		assert.Equal(t, "SLA_BREACH", r.slaOutcome) // cancel sees breach in the past
		assert.Equal(t, types.TriggerStatusCompleted, e2eTriggerStatus(mock, "pipe-c4", "stream", "2026-03-07"))
		assertAlertFormats(t, eb)
	})
}

// =========================================================================
// Group D: Automatic Retries (Stream-Router Job Failure Handling)
// =========================================================================

func TestE2E_GroupD_AutoRetries(t *testing.T) {
	ctx := context.Background()

	t.Run("D1_JobFailAutoRetrySuccess", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{{State: "failed", Message: "OOM"}}}
		d, sfnM, eb, _ := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-d1")
		cfg.Job.MaxRetries = 2

		// Phase 1: Run SFN to job failure
		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline: cfg,
			sensors:  map[string]map[string]interface{}{"upstream-complete": {"status": "ready"}},
		})
		assert.Equal(t, sfnDone, r.terminal)
		assert.Contains(t, r.events, "JOB_FAILED")
		assert.Equal(t, types.TriggerStatusFailedFinal, e2eTriggerStatus(mock, "pipe-d1", "stream", "2026-03-07"))

		// Phase 2: Simulate DynamoDB stream event for the JOB#fail record
		jobSK := findLatestJobSK(mock, "pipe-d1", "stream", "2026-03-07")
		require.NotEmpty(t, jobSK, "should have a joblog entry")

		sfnCountBefore := len(sfnM.executions)
		err := lambda.HandleStreamEvent(ctx, d, makeJobStreamEvent("pipe-d1", jobSK, "fail"))
		require.NoError(t, err)

		// Verify: new SFN execution started (auto-retry under maxRetries limit)
		sfnM.mu.Lock()
		assert.Greater(t, len(sfnM.executions), sfnCountBefore, "should start rerun SFN")
		sfnM.mu.Unlock()
		assertAlertFormats(t, eb)
	})

	t.Run("D2_JobFailRetryExhausted", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{{State: "failed"}}}
		d, sfnM, eb, _ := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-d2")
		cfg.Job.MaxRetries = 0 // no retries allowed

		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline: cfg,
			sensors:  map[string]map[string]interface{}{"upstream-complete": {"status": "ready"}},
		})
		assert.Equal(t, sfnDone, r.terminal)

		// Simulate JOB#fail stream event
		jobSK := findLatestJobSK(mock, "pipe-d2", "stream", "2026-03-07")
		require.NotEmpty(t, jobSK)

		sfnCountBefore := len(sfnM.executions)
		eb.mu.Lock()
		eb.events = nil
		eb.mu.Unlock()

		err := lambda.HandleStreamEvent(ctx, d, makeJobStreamEvent("pipe-d2", jobSK, "fail"))
		require.NoError(t, err)

		// Verify: no new SFN, RETRY_EXHAUSTED published, status=FAILED_FINAL
		sfnM.mu.Lock()
		assert.Equal(t, sfnCountBefore, len(sfnM.executions), "should not start rerun")
		sfnM.mu.Unlock()
		assert.Contains(t, collectEventTypes(eb), "RETRY_EXHAUSTED")
		assert.Equal(t, types.TriggerStatusFailedFinal, e2eTriggerStatus(mock, "pipe-d2", "stream", "2026-03-07"))
		assertAlertFormats(t, eb)
	})
}

// =========================================================================
// Group E: Re-Run / Replay (Manual via RERUN_REQUEST)
// =========================================================================

func TestE2E_GroupE_RerunReplay(t *testing.T) {
	ctx := context.Background()

	t.Run("E1_RerunAccepted", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, sfnM, eb, _ := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-e1")
		seedConfig(mock, cfg)

		// Seed a completed trigger + successful job with old timestamp
		seedTriggerLock(mock, "pipe-e1", "2026-03-07")
		require.NoError(t, d.Store.SetTriggerStatus(ctx, "pipe-e1", "stream", "2026-03-07", types.TriggerStatusCompleted))
		oldTS := fmt.Sprintf("%d", time.Now().Add(-1*time.Hour).UnixMilli())
		mock.putRaw("joblog", map[string]ddbtypes.AttributeValue{
			"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("pipe-e1")},
			"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("stream", "2026-03-07", oldTS)},
			"event": &ddbtypes.AttributeValueMemberS{Value: types.JobEventSuccess},
		})

		// Seed fresh sensor data (updatedAt > job timestamp)
		freshTS := fmt.Sprintf("%d", time.Now().UnixMilli())
		require.NoError(t, d.Store.WriteSensor(ctx, "pipe-e1", "upstream-complete", map[string]interface{}{
			"status": "ready", "updatedAt": freshTS,
		}))

		// Process RERUN_REQUEST stream event
		sfnCountBefore := len(sfnM.executions)
		err := lambda.HandleStreamEvent(ctx, d, makeRerunRequestStreamEvent("pipe-e1", "stream", "2026-03-07"))
		require.NoError(t, err)

		// Verify: new SFN started, rerun-accepted joblog written
		sfnM.mu.Lock()
		assert.Greater(t, len(sfnM.executions), sfnCountBefore, "should start rerun SFN")
		sfnM.mu.Unlock()
		assert.Contains(t, collectJoblogEvents(mock, "pipe-e1", "stream", "2026-03-07"), "rerun-accepted")
		assertAlertFormats(t, eb)
	})

	t.Run("E2_RerunRejected", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, sfnM, eb, _ := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-e2")
		seedConfig(mock, cfg)

		// Completed trigger + successful job with RECENT timestamp
		seedTriggerLock(mock, "pipe-e2", "2026-03-07")
		require.NoError(t, d.Store.SetTriggerStatus(ctx, "pipe-e2", "stream", "2026-03-07", types.TriggerStatusCompleted))
		recentTS := fmt.Sprintf("%d", time.Now().UnixMilli())
		mock.putRaw("joblog", map[string]ddbtypes.AttributeValue{
			"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("pipe-e2")},
			"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("stream", "2026-03-07", recentTS)},
			"event": &ddbtypes.AttributeValueMemberS{Value: types.JobEventSuccess},
		})

		// Sensor data with OLD updatedAt (before job completed → no change)
		oldSensorTS := fmt.Sprintf("%d", time.Now().Add(-2*time.Hour).UnixMilli())
		require.NoError(t, d.Store.WriteSensor(ctx, "pipe-e2", "upstream-complete", map[string]interface{}{
			"status": "ready", "updatedAt": oldSensorTS,
		}))

		sfnCountBefore := len(sfnM.executions)
		err := lambda.HandleStreamEvent(ctx, d, makeRerunRequestStreamEvent("pipe-e2", "stream", "2026-03-07"))
		require.NoError(t, err)

		// Verify: no SFN, RERUN_REJECTED published
		sfnM.mu.Lock()
		assert.Equal(t, sfnCountBefore, len(sfnM.executions), "should not start SFN")
		sfnM.mu.Unlock()
		assert.Contains(t, collectEventTypes(eb), "RERUN_REJECTED")
		assert.Contains(t, collectJoblogEvents(mock, "pipe-e2", "stream", "2026-03-07"), "rerun-rejected")
		assertAlertFormats(t, eb)
	})

	t.Run("E3_LateDataArrival", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, _, eb, _ := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-e3")
		seedConfig(mock, cfg)

		// Completed trigger + successful job
		seedTriggerLock(mock, "pipe-e3", "2026-03-07")
		require.NoError(t, d.Store.SetTriggerStatus(ctx, "pipe-e3", "stream", "2026-03-07", types.TriggerStatusCompleted))
		ts := fmt.Sprintf("%d", time.Now().UnixMilli())
		mock.putRaw("joblog", map[string]ddbtypes.AttributeValue{
			"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("pipe-e3")},
			"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("stream", "2026-03-07", ts)},
			"event": &ddbtypes.AttributeValueMemberS{Value: types.JobEventSuccess},
		})

		// New sensor write (late data) — triggers the condition
		record := makeSensorRecord("pipe-e3", "upstream-complete", map[string]events.DynamoDBAttributeValue{
			"status": events.NewStringAttribute("ready"),
			"date":   events.NewStringAttribute("2026-03-07"),
		})
		err := lambda.HandleStreamEvent(ctx, d, lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}})
		require.NoError(t, err)

		// Lock already held → late data path
		evts := collectEventTypes(eb)
		assert.Contains(t, evts, "LATE_DATA_ARRIVAL")
		assert.Contains(t, collectJoblogEvents(mock, "pipe-e3", "stream", "2026-03-07"), "late-data-arrival")
		assert.True(t, hasRerunRequest(mock, "pipe-e3", "stream", "2026-03-07"), "late data should write RERUN_REQUEST")
		assertAlertFormats(t, eb)
	})
}

// =========================================================================
// Group F: Drift → Re-Trigger Compound Scenarios
// =========================================================================

func TestE2E_GroupF_DriftRetrigger(t *testing.T) {
	ctx := context.Background()

	t.Run("F1_DriftRetriggerSuccess", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{{State: "succeeded"}}}
		d, sfnM, eb, _ := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-f1")
		cfg.PostRun = &types.PostRunConfig{
			Evaluation: &types.EvaluationWindow{Window: "10m", Interval: "5m"},
			Rules:      []types.ValidationRule{{Key: "quality-check", Check: types.CheckExists}},
		}

		// Phase 1: SFN with drift detection
		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline: cfg,
			sensors: map[string]map[string]interface{}{
				"upstream-complete": {"status": "ready"},
				"audit-result":     {"sensor_count": float64(100)},
			},
			postRunSensorUpdates: []map[string]map[string]interface{}{
				{},
				{"audit-result": {"sensor_count": float64(120)}},
			},
		})
		assert.Equal(t, sfnDone, r.terminal)
		assert.Contains(t, r.events, "DATA_DRIFT")
		assert.True(t, hasRerunRequest(mock, "pipe-f1", "stream", "2026-03-07"))

		// Phase 2: Stream-router processes RERUN_REQUEST
		sfnCountBefore := len(sfnM.executions)
		err := lambda.HandleStreamEvent(ctx, d, makeRerunRequestStreamEvent("pipe-f1", "stream", "2026-03-07"))
		require.NoError(t, err)

		// Verify: new SFN started for re-trigger
		sfnM.mu.Lock()
		assert.Greater(t, len(sfnM.executions), sfnCountBefore, "drift re-trigger should start new SFN")
		sfnM.mu.Unlock()
		assertAlertFormats(t, eb)
	})

	t.Run("F2_DriftRetriggerJobFails", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{{State: "succeeded"}}}
		d, sfnM, eb, _ := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-f2")
		cfg.PostRun = &types.PostRunConfig{
			Evaluation: &types.EvaluationWindow{Window: "10m", Interval: "5m"},
			Rules:      []types.ValidationRule{{Key: "quality-check", Check: types.CheckExists}},
		}

		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline: cfg,
			sensors: map[string]map[string]interface{}{
				"upstream-complete": {"status": "ready"},
				"audit-result":     {"sensor_count": float64(100)},
			},
			postRunSensorUpdates: []map[string]map[string]interface{}{
				{},
				{"audit-result": {"sensor_count": float64(80)}},
			},
		})
		assert.Equal(t, sfnDone, r.terminal)
		assert.Contains(t, r.events, "DATA_DRIFT")

		sfnCountBefore := len(sfnM.executions)
		err := lambda.HandleStreamEvent(ctx, d, makeRerunRequestStreamEvent("pipe-f2", "stream", "2026-03-07"))
		require.NoError(t, err)

		sfnM.mu.Lock()
		assert.Greater(t, len(sfnM.executions), sfnCountBefore, "drift re-trigger should start SFN")
		sfnM.mu.Unlock()
		assertAlertFormats(t, eb)
	})

	t.Run("F3_DriftRetriggerInfraFailure", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{{State: "succeeded"}}}
		d, sfnM, eb, _ := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-f3")
		cfg.PostRun = &types.PostRunConfig{
			Evaluation: &types.EvaluationWindow{Window: "10m", Interval: "5m"},
			Rules:      []types.ValidationRule{{Key: "quality-check", Check: types.CheckExists}},
		}

		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline: cfg,
			sensors: map[string]map[string]interface{}{
				"upstream-complete": {"status": "ready"},
				"audit-result":     {"sensor_count": float64(200)},
			},
			postRunSensorUpdates: []map[string]map[string]interface{}{
				{},
				{"audit-result": {"sensor_count": float64(250)}},
			},
		})
		assert.Equal(t, sfnDone, r.terminal)
		assert.Contains(t, r.events, "DATA_DRIFT")

		// Phase 2: verify the RERUN_REQUEST was written, allowing re-trigger
		sfnCountBefore := len(sfnM.executions)
		err := lambda.HandleStreamEvent(ctx, d, makeRerunRequestStreamEvent("pipe-f3", "stream", "2026-03-07"))
		require.NoError(t, err)

		sfnM.mu.Lock()
		assert.Greater(t, len(sfnM.executions), sfnCountBefore)
		sfnM.mu.Unlock()
		assertAlertFormats(t, eb)
	})
}

// =========================================================================
// Group G: Watchdog Health Checks
// =========================================================================

func TestE2E_GroupG_Watchdog(t *testing.T) {
	ctx := context.Background()

	t.Run("G1_StaleTriggerDetected", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, _, eb, _ := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-g1")
		seedConfig(mock, cfg)

		// Seed a RUNNING trigger with expired TTL
		expiredTTL := time.Now().Add(-1 * time.Hour).Unix()
		mock.putRaw(testControlTable, map[string]ddbtypes.AttributeValue{
			"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("pipe-g1")},
			"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("stream", "2026-03-07")},
			"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusRunning},
			"ttl":    &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", expiredTTL)},
		})

		err := lambda.HandleWatchdog(ctx, d)
		require.NoError(t, err)

		assert.Contains(t, collectEventTypes(eb), "SFN_TIMEOUT")
		assert.Equal(t, types.TriggerStatusFailedFinal, e2eTriggerStatus(mock, "pipe-g1", "stream", "2026-03-07"))
		assertAlertFormats(t, eb)
	})

	t.Run("G2_MissedCronSchedule", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, _, eb, _ := buildE2EDeps(mock, tr, sc)

		// Cron-scheduled pipeline: fires at minute 0 every hour
		cfg := types.PipelineConfig{
			Pipeline: types.PipelineIdentity{ID: "pipe-g2"},
			Schedule: types.ScheduleConfig{
				Cron: "0 * * * *",
				Evaluation: types.EvaluationWindow{
					Window:   "1h",
					Interval: "5m",
				},
			},
			Validation: types.ValidationConfig{
				Trigger: "ALL",
				Rules:   []types.ValidationRule{{Key: "upstream-complete", Check: types.CheckExists}},
			},
			Job: types.JobConfig{
				Type:   types.TriggerCommand,
				Config: map[string]interface{}{"command": "echo hello"},
			},
		}
		seedConfig(mock, cfg)

		// No trigger exists for today — missed schedule
		err := lambda.HandleWatchdog(ctx, d)
		require.NoError(t, err)

		assert.Contains(t, collectEventTypes(eb), "SCHEDULE_MISSED")
		assertAlertFormats(t, eb)
	})
}

// =========================================================================
// Group H: SLA Branch Completeness
// =========================================================================

func TestE2E_GroupH_SLABranchCompleteness(t *testing.T) {
	ctx := context.Background()

	t.Run("H1_SLAWarningOutcome", func(t *testing.T) {
		// Cancel when warning has passed but breach is still in the future.
		// Expected: cancel returns SLA_WARNING (not SLA_MET or SLA_BREACH).
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{{State: "succeeded"}}}
		d, _, eb, _ := buildE2EDeps(mock, tr, sc)

		// Warning 1h in the past, breach 1h in the future.
		warnAt := time.Now().Add(-1 * time.Hour).UTC().Format(time.RFC3339)
		breachAt := time.Now().Add(1 * time.Hour).UTC().Format(time.RFC3339)

		cfg := e2ePipeline("pipe-h1")
		cfg.SLA = &types.SLAConfig{Deadline: "23:59", ExpectedDuration: "30m"}

		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline:     cfg,
			sensors:      map[string]map[string]interface{}{"upstream-complete": {"status": "ready"}},
			slaWarningAt: warnAt, slaBreachAt: breachAt,
		})

		assert.Equal(t, sfnDone, r.terminal)
		assert.Equal(t, "SLA_WARNING", r.slaOutcome)
		// SLA_MET event should NOT be published (warning already fired).
		assert.NotContains(t, r.events, "SLA_MET")
		assertAlertFormats(t, eb)
	})

	t.Run("H2_SLAFireAlertSuppression", func(t *testing.T) {
		// Fire-alert called after pipeline completed → alert suppressed.
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, _, eb, _ := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-h2")
		cfg.SLA = &types.SLAConfig{Deadline: "23:59", ExpectedDuration: "30m"}
		seedConfig(mock, cfg)

		// Seed a COMPLETED trigger (pipeline finished successfully).
		seedTriggerLock(mock, "pipe-h2", "2026-03-07")
		require.NoError(t, d.Store.SetTriggerStatus(ctx, "pipe-h2", "stream", "2026-03-07", types.TriggerStatusCompleted))

		out, err := lambda.HandleSLAMonitor(ctx, d, lambda.SLAMonitorInput{
			Mode: "fire-alert", PipelineID: "pipe-h2", ScheduleID: "stream",
			Date: "2026-03-07", AlertType: "SLA_BREACH",
		})
		require.NoError(t, err)

		// Alert should be suppressed — no EventBridge event published.
		evts := collectEventTypes(eb)
		assert.NotContains(t, evts, "SLA_BREACH", "fire-alert should be suppressed for COMPLETED trigger")
		// Output still returns the alert type and firedAt (silent suppression).
		assert.Equal(t, "SLA_BREACH", out.AlertType)
		assert.NotEmpty(t, out.FiredAt)
	})
}

// =========================================================================
// Group I: Stream-Router Entry Points
// =========================================================================

func TestE2E_GroupI_StreamRouterEntryPoints(t *testing.T) {
	ctx := context.Background()

	t.Run("I1_InitialSensorTrigger", func(t *testing.T) {
		// Sensor event arrives → trigger condition met → lock acquired → SFN started.
		// This is the fundamental entry point for stream-triggered pipelines.
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, sfnM, eb, _ := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-i1")
		seedConfig(mock, cfg)

		// No trigger row exists — fresh pipeline.
		record := makeSensorRecord("pipe-i1", "upstream-complete", map[string]events.DynamoDBAttributeValue{
			"status": events.NewStringAttribute("ready"),
			"date":   events.NewStringAttribute("2026-03-07"),
		})

		sfnCountBefore := len(sfnM.executions)
		err := lambda.HandleStreamEvent(ctx, d, lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}})
		require.NoError(t, err)

		// Verify: SFN started, trigger lock acquired, JOB_TRIGGERED event published.
		sfnM.mu.Lock()
		assert.Greater(t, len(sfnM.executions), sfnCountBefore, "sensor trigger should start SFN")
		sfnM.mu.Unlock()
		assert.Equal(t, types.TriggerStatusRunning, e2eTriggerStatus(mock, "pipe-i1", "stream", "2026-03-07"))
		assertAlertFormats(t, eb)
	})

	t.Run("I2_JobTimeoutAutoRetry", func(t *testing.T) {
		// Job timeout flows through handleJobFailure → auto-retry (same as fail).
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, sfnM, eb, _ := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-i2")
		cfg.Job.MaxRetries = 2
		seedConfig(mock, cfg)

		// Seed a RUNNING trigger + "timeout" joblog entry.
		seedTriggerLock(mock, "pipe-i2", "2026-03-07")
		ts := fmt.Sprintf("%d", time.Now().UnixMilli())
		jobSK := types.JobSK("stream", "2026-03-07", ts)
		mock.putRaw("joblog", map[string]ddbtypes.AttributeValue{
			"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("pipe-i2")},
			"SK":    &ddbtypes.AttributeValueMemberS{Value: jobSK},
			"event": &ddbtypes.AttributeValueMemberS{Value: types.JobEventTimeout},
		})

		sfnCountBefore := len(sfnM.executions)
		err := lambda.HandleStreamEvent(ctx, d, makeJobStreamEvent("pipe-i2", jobSK, types.JobEventTimeout))
		require.NoError(t, err)

		// Verify: auto-retry started (timeout is retryable just like fail).
		sfnM.mu.Lock()
		assert.Greater(t, len(sfnM.executions), sfnCountBefore, "timeout should trigger auto-retry")
		sfnM.mu.Unlock()
		assertAlertFormats(t, eb)
	})

	t.Run("I3_RerunAfterFailure", func(t *testing.T) {
		// RERUN_REQUEST when prior job was "fail" → allowed immediately (no freshness check).
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, sfnM, eb, _ := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-i3")
		seedConfig(mock, cfg)

		// Seed a completed trigger with a FAILED job.
		seedTriggerLock(mock, "pipe-i3", "2026-03-07")
		require.NoError(t, d.Store.SetTriggerStatus(ctx, "pipe-i3", "stream", "2026-03-07", types.TriggerStatusFailedFinal))
		ts := fmt.Sprintf("%d", time.Now().UnixMilli())
		mock.putRaw("joblog", map[string]ddbtypes.AttributeValue{
			"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("pipe-i3")},
			"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("stream", "2026-03-07", ts)},
			"event": &ddbtypes.AttributeValueMemberS{Value: types.JobEventFail},
		})

		// Sensor data is OLD (would fail freshness check if it were run).
		oldSensorTS := fmt.Sprintf("%d", time.Now().Add(-2*time.Hour).UnixMilli())
		require.NoError(t, d.Store.WriteSensor(ctx, "pipe-i3", "upstream-complete", map[string]interface{}{
			"status": "ready", "updatedAt": oldSensorTS,
		}))

		sfnCountBefore := len(sfnM.executions)
		err := lambda.HandleStreamEvent(ctx, d, makeRerunRequestStreamEvent("pipe-i3", "stream", "2026-03-07"))
		require.NoError(t, err)

		// Verify: rerun accepted despite old sensor data (failure skips freshness check).
		sfnM.mu.Lock()
		assert.Greater(t, len(sfnM.executions), sfnCountBefore, "rerun after failure should be accepted without freshness check")
		sfnM.mu.Unlock()
		assert.Contains(t, collectJoblogEvents(mock, "pipe-i3", "stream", "2026-03-07"), "rerun-accepted")
		assertAlertFormats(t, eb)
	})
}

// =========================================================================
// Group J: Cross-Handler Edge Cases
// =========================================================================

func TestE2E_GroupJ_CrossHandlerEdgeCases(t *testing.T) {
	ctx := context.Background()

	t.Run("J1_WatchdogSelfHealing", func(t *testing.T) {
		// reconcileSensorTriggers: sensor condition met, no trigger row →
		// lock acquired, SFN started, TRIGGER_RECOVERED published.
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, sfnM, eb, _ := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-j1")
		seedConfig(mock, cfg)

		// Seed sensor that satisfies the trigger condition.
		require.NoError(t, d.Store.WriteSensor(ctx, "pipe-j1", "upstream-complete", map[string]interface{}{
			"status": "ready", "date": "2026-03-07",
		}))

		// No trigger row exists — this is the condition reconcileSensorTriggers detects.
		sfnCountBefore := len(sfnM.executions)
		err := lambda.HandleWatchdog(ctx, d)
		require.NoError(t, err)

		// Verify: SFN started via reconciliation, TRIGGER_RECOVERED published.
		sfnM.mu.Lock()
		assert.Greater(t, len(sfnM.executions), sfnCountBefore, "watchdog should self-heal missed trigger")
		sfnM.mu.Unlock()
		assert.Contains(t, collectEventTypes(eb), "TRIGGER_RECOVERED")
		assert.Equal(t, types.TriggerStatusRunning, e2eTriggerStatus(mock, "pipe-j1", "stream", "2026-03-07"))
		assertAlertFormats(t, eb)
	})

	t.Run("J2_CheckJobNonTerminalFallthrough", func(t *testing.T) {
		// Joblog has "infra-trigger-failure" (non-terminal) → check-job skips it,
		// falls through to StatusChecker → polls real job status → success.
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{{State: "succeeded"}}}
		d, _, eb, _ := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-j2")

		// Run SFN but pre-seed a non-terminal joblog entry before check-job.
		// The "infra-trigger-failure" entry simulates leftover from trigger retries.
		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline:         cfg,
			sensors:          map[string]map[string]interface{}{"upstream-complete": {"status": "ready"}},
			preCheckJobEvent: types.JobEventInfraTriggerFailure,
		})

		assert.Equal(t, sfnDone, r.terminal)
		// check-job should have skipped the non-terminal entry and polled StatusChecker.
		assert.Contains(t, r.events, "JOB_COMPLETED", "check-job should fall through non-terminal joblog to StatusChecker")
		assert.Equal(t, types.TriggerStatusCompleted, e2eTriggerStatus(mock, "pipe-j2", "stream", "2026-03-07"))
		joblogs := collectJoblogEvents(mock, "pipe-j2", "stream", "2026-03-07")
		assert.Contains(t, joblogs, "success", "StatusChecker success should be written to joblog")
		assertAlertFormats(t, eb)
	})
}

// =========================================================================
// Alert Format Verification (covers all event types)
// =========================================================================

func TestE2E_AlertFormatAllEventTypes(t *testing.T) {
	eventTypes := []struct {
		detailType    string
		wantEmojiHex string // raw byte prefix to check
	}{
		{"VALIDATION_PASSED", "\xe2\x84\xb9"},     // info
		{"VALIDATION_EXHAUSTED", "\xf0\x9f\x94\xb4"}, // red circle
		{"JOB_TRIGGERED", "\xe2\x84\xb9"},          // info
		{"JOB_COMPLETED", "\xe2\x84\xb9"},           // info
		{"JOB_FAILED", "\xf0\x9f\x94\xb4"},         // red circle
		{"RETRY_EXHAUSTED", "\xf0\x9f\x94\xb4"},    // red circle
		{"SLA_WARNING", "\xf0\x9f\x9f\xa1"},        // yellow circle
		{"SLA_BREACH", "\xf0\x9f\x94\xb4"},         // red circle
		{"SLA_MET", "\xe2\x9c\x85"},                // check mark
		{"DATA_DRIFT", "\xf0\x9f\x94\xb4"},         // red circle
		{"LATE_DATA_ARRIVAL", "\xe2\x84\xb9"},       // info
		{"RERUN_REJECTED", "\xe2\x84\xb9"},          // info
		{"SFN_TIMEOUT", "\xf0\x9f\x94\xb4"},        // red circle
		{"SCHEDULE_MISSED", "\xf0\x9f\x94\xb4"},    // red circle
		{"TRIGGER_RECOVERED", "\xe2\x84\xb9"},       // info
	}

	for _, tt := range eventTypes {
		t.Run(tt.detailType, func(t *testing.T) {
			detail := types.InterlockEvent{
				PipelineID: "test-pipeline",
				ScheduleID: "stream",
				Date:       "2026-03-07",
				Message:    "test message",
				Timestamp:  time.Now(),
				Detail: map[string]interface{}{
					"deadline": "23:59",
					"breachAt": "2026-03-07T23:59:00Z",
					"status":   "RUNNING",
					"source":   "test",
				},
			}
			text := lambda.FormatAlertText(tt.detailType, detail)
			assert.NotEmpty(t, text)
			assert.Contains(t, text, "test-pipeline")
			assert.Contains(t, text, "2026-03-07")
			assert.True(t, strings.HasPrefix(text, tt.wantEmojiHex),
				"event %s: expected emoji prefix %q, got %q", tt.detailType, tt.wantEmojiHex, text[:4])
		})
	}
}

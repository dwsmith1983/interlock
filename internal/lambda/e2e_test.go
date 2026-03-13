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

func buildE2EDeps(mock *mockDDB, tr *mockTriggerRunner, sc *mockStatusPoller) (*lambda.Deps, *mockSFN, *mockEventBridge) {
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
	return d, sfnM, ebM
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
	acquired, err := d.Store.AcquireTriggerLock(ctx, pid, sid, date, lambda.ResolveTriggerLockTTL())
	require.NoError(t, err)
	require.True(t, acquired)

	evalInterval := parseDurationSec(sc.pipeline.Schedule.Evaluation.Interval, 300)
	evalWindow := parseDurationSec(sc.pipeline.Schedule.Evaluation.Window, 3600)

	hasPostRun := sc.pipeline.PostRun != nil && len(sc.pipeline.PostRun.Rules) > 0

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
			jobCheckInterval := 60 // must match buildSFNConfig JobCheckIntervalSeconds default
			jobPollWindow := 3600  // must match buildSFNConfig JobPollWindowSeconds default
			if sc.pipeline.Job.JobPollWindowSeconds != nil && *sc.pipeline.Job.JobPollWindowSeconds > 0 {
				jobPollWindow = *sc.pipeline.Job.JobPollWindowSeconds
			}
			jobPollElapsed := 0
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
				jobPollElapsed += jobCheckInterval
				if jobPollElapsed >= jobPollWindow {
					// JobPollExhausted → InjectTimeoutEvent
					_, _ = lambda.HandleOrchestrator(ctx, d, lambda.OrchestratorInput{
						Mode: "job-poll-exhausted", PipelineID: pid, ScheduleID: sid, Date: date,
						RunID: runID,
					})
					jobEvent = "timeout"
					break
				}
			}

			// ── Phase 4: CompleteTrigger (captures baseline if PostRun configured) ──
			_, _ = lambda.HandleOrchestrator(ctx, d, lambda.OrchestratorInput{
				Mode: "complete-trigger", PipelineID: pid, ScheduleID: sid, Date: date,
				Event: jobEvent,
			})

			// ── Phase 5: Stream-based post-run ───────────────────
			// Simulate sensor updates arriving after completion via DynamoDB Stream.
			if jobEvent == "success" && hasPostRun && len(sc.postRunSensorUpdates) > 0 {
				for _, updates := range sc.postRunSensorUpdates {
					for key, data := range updates {
						require.NoError(t, d.Store.WriteSensor(ctx, pid, key, data))
						// Simulate stream event for each sensor update.
						sensorRecord := makeSensorRecord(pid, key, toStreamAttributes(data))
						streamEvt := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{sensorRecord}}
						_, _ = lambda.HandleStreamEvent(ctx, d, streamEvt)
					}
				}
			}
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

func collectJoblogEvents(mock *mockDDB, pipelineID string) []string {
	mock.mu.Lock()
	defer mock.mu.Unlock()
	pk := types.PipelinePK(pipelineID)
	prefix := "joblog#" + pk + "#JOB#stream#2026-03-07#"
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

func e2eTriggerStatus(mock *mockDDB, pipelineID string) string {
	mock.mu.Lock()
	defer mock.mu.Unlock()
	key := ddbItemKey(testControlTable, types.PipelinePK(pipelineID), types.TriggerSK("stream", "2026-03-07"))
	item, ok := mock.items[key]
	if !ok {
		return ""
	}
	if s, ok := item["status"].(*ddbtypes.AttributeValueMemberS); ok {
		return s.Value
	}
	return ""
}

// toStreamAttributes wraps a data map in the canonical "data" envelope
// used by extractSensorData for stream-based post-run evaluation.
func toStreamAttributes(data map[string]interface{}) map[string]events.DynamoDBAttributeValue {
	inner := make(map[string]events.DynamoDBAttributeValue, len(data))
	for k, v := range data {
		switch val := v.(type) {
		case string:
			inner[k] = events.NewStringAttribute(val)
		case float64:
			inner[k] = events.NewNumberAttribute(fmt.Sprintf("%g", val))
		case bool:
			inner[k] = events.NewBooleanAttribute(val)
		}
	}
	return map[string]events.DynamoDBAttributeValue{
		"data": events.NewMapAttribute(inner),
	}
}

func hasRerunRequest(mock *mockDDB, pipelineID string) bool {
	mock.mu.Lock()
	defer mock.mu.Unlock()
	key := ddbItemKey(testControlTable, types.PipelinePK(pipelineID), types.RerunRequestSK("stream", "2026-03-07"))
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

func makeRerunRequestStreamEvent(pipelineID string) lambda.StreamEvent {
	sk := types.RerunRequestSK("stream", "2026-03-07")
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

func makeRerunRequestWithReasonE2E(pipelineID, reason string) lambda.StreamEvent {
	sk := types.RerunRequestSK("stream", "2026-03-07")
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
					"reason": events.NewStringAttribute(reason),
				},
			},
		}},
	}
}

func intPtr(v int) *int { return &v }

func findLatestJobSK(mock *mockDDB, pipelineID string) string {
	mock.mu.Lock()
	defer mock.mu.Unlock()
	pk := types.PipelinePK(pipelineID)
	prefix := "joblog#" + pk + "#JOB#stream#2026-03-07#"
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
// Primary SFN Execution Paths
// =========================================================================

func TestE2E_PrimarySFNPaths(t *testing.T) {
	ctx := context.Background()

	t.Run("HappyPath", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{
			{State: "running"}, {State: "succeeded"},
		}}
		d, _, eb := buildE2EDeps(mock, tr, sc)
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
		// JOB_COMPLETED is emitted by stream-router (not SFN), verified in stream_router_test.go.
		assert.Contains(t, r.events, "SLA_MET")
		assert.Equal(t, types.TriggerStatusCompleted, e2eTriggerStatus(mock, "pipe-a1"))
		assert.Contains(t, collectJoblogEvents(mock, "pipe-a1"), "success")
		assertAlertFormats(t, eb)
	})

	t.Run("ExtendedEvaluation", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{{State: "succeeded"}}}
		d, _, eb := buildE2EDeps(mock, tr, sc)

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
		assert.Equal(t, types.TriggerStatusCompleted, e2eTriggerStatus(mock, "pipe-a2"))
		assertAlertFormats(t, eb)
	})

	t.Run("ValidationExhausted", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, _, eb := buildE2EDeps(mock, tr, sc)

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
		assert.Contains(t, collectJoblogEvents(mock, "pipe-a3"), "validation-exhausted")
		// Trigger lock stays RUNNING (no release in validation-exhausted path)
		assert.Equal(t, types.TriggerStatusRunning, e2eTriggerStatus(mock, "pipe-a3"))
		assertAlertFormats(t, eb)
	})

	t.Run("TriggerInfraFailure", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{err: errors.New("connection refused")}
		sc := &mockStatusPoller{}
		d, _, eb := buildE2EDeps(mock, tr, sc)

		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline: e2ePipeline("pipe-a4"),
			sensors:  map[string]map[string]interface{}{"upstream-complete": {"status": "ready"}},
		})

		assert.Equal(t, sfnDone, r.terminal)
		assert.Equal(t, 1, r.evalCount)
		assert.Contains(t, r.events, "VALIDATION_PASSED")
		assert.Contains(t, r.events, "RETRY_EXHAUSTED")
		joblogs := collectJoblogEvents(mock, "pipe-a4")
		assert.Contains(t, joblogs, "infra-trigger-exhausted")
		// Lock released by trigger-exhausted
		assert.Equal(t, "", e2eTriggerStatus(mock, "pipe-a4"))
		assertAlertFormats(t, eb)
	})

	t.Run("JobFailure", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{{State: "failed", Message: "OOM"}}}
		d, _, eb := buildE2EDeps(mock, tr, sc)

		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline: e2ePipeline("pipe-a5"),
			sensors:  map[string]map[string]interface{}{"upstream-complete": {"status": "ready"}},
		})

		assert.Equal(t, sfnDone, r.terminal)
		assert.Contains(t, r.events, "VALIDATION_PASSED")
		assert.Contains(t, r.events, "JOB_TRIGGERED")
		assert.Contains(t, r.events, "JOB_FAILED")
		assert.Equal(t, types.TriggerStatusFailedFinal, e2eTriggerStatus(mock, "pipe-a5"))
		assert.Contains(t, collectJoblogEvents(mock, "pipe-a5"), "fail")
		assertAlertFormats(t, eb)
	})

	t.Run("JobTimeout", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{} // not used; timeout comes from joblog
		d, _, eb := buildE2EDeps(mock, tr, sc)

		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline:         e2ePipeline("pipe-a6"),
			sensors:          map[string]map[string]interface{}{"upstream-complete": {"status": "ready"}},
			preCheckJobEvent: types.JobEventTimeout,
		})

		assert.Equal(t, sfnDone, r.terminal)
		assert.Contains(t, r.events, "VALIDATION_PASSED")
		assert.Contains(t, r.events, "JOB_TRIGGERED")
		assert.Equal(t, types.TriggerStatusFailedFinal, e2eTriggerStatus(mock, "pipe-a6"))
		assert.Contains(t, collectJoblogEvents(mock, "pipe-a6"), "timeout")
		assertAlertFormats(t, eb)
	})

	t.Run("InfraFailure", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, _, eb := buildE2EDeps(mock, tr, sc)

		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline: e2ePipeline("pipe-a7"),
			sensors:  map[string]map[string]interface{}{"upstream-complete": {"status": "ready"}},
			evalErr:  errors.New("Lambda OOM"),
		})

		assert.Equal(t, sfnInfraFailure, r.terminal)
		assert.Equal(t, 0, r.evalCount)
		assert.Empty(t, r.events)
		// Trigger stays RUNNING — watchdog will detect it later
		assert.Equal(t, types.TriggerStatusRunning, e2eTriggerStatus(mock, "pipe-a7"))
	})

	t.Run("JobPollWindowExhausted", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{} // empty seq → always returns "running"
		d, _, eb := buildE2EDeps(mock, tr, sc)

		pollWindow := 120 // 2 minutes
		cfg := e2ePipeline("pipe-poll-exhaust")
		cfg.Job.JobPollWindowSeconds = &pollWindow

		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline: cfg,
			sensors:  map[string]map[string]interface{}{"upstream-complete": {"status": "ready"}},
		})

		assert.Equal(t, sfnDone, r.terminal)
		assert.Contains(t, r.events, "VALIDATION_PASSED")
		assert.Contains(t, r.events, "JOB_TRIGGERED")
		assert.Contains(t, r.events, "JOB_POLL_EXHAUSTED")
		assert.Equal(t, types.TriggerStatusFailedFinal, e2eTriggerStatus(mock, "pipe-poll-exhaust"))
		assert.Contains(t, collectJoblogEvents(mock, "pipe-poll-exhaust"), types.JobEventJobPollExhausted)
		assertAlertFormats(t, eb)
	})
}

// =========================================================================
// Post-Run Monitoring
// =========================================================================

func TestE2E_PostRunMonitoring(t *testing.T) {
	ctx := context.Background()

	t.Run("PostRunPasses", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{{State: "succeeded"}}}
		d, _, eb := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-b1")
		cfg.PostRun = &types.PostRunConfig{
			Evaluation: &types.EvaluationWindow{Window: "10m", Interval: "5m"},
			Rules:      []types.ValidationRule{{Key: "quality-check", Check: types.CheckExists}},
		}

		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline: cfg,
			sensors: map[string]map[string]interface{}{
				"upstream-complete": {"status": "ready"},
				"quality-check":     {"status": "passed"},
			},
		})

		assert.Equal(t, sfnDone, r.terminal)
		assert.NotContains(t, r.events, "POST_RUN_DRIFT")
		assert.Equal(t, types.TriggerStatusCompleted, e2eTriggerStatus(mock, "pipe-b1"))
		assertAlertFormats(t, eb)
	})

	t.Run("DriftDetected", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{{State: "succeeded"}}}
		d, _, eb := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-b2")
		cfg.PostRun = &types.PostRunConfig{
			Evaluation: &types.EvaluationWindow{Window: "10m", Interval: "5m"},
			Rules:      []types.ValidationRule{{Key: "audit-result", Check: types.CheckExists}},
		}

		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline: cfg,
			sensors: map[string]map[string]interface{}{
				"upstream-complete": {"status": "ready"},
				"audit-result":      {"sensor_count": float64(100)},
			},
			postRunSensorUpdates: []map[string]map[string]interface{}{
				{}, // iteration 0: no updates; baseline saved
				{"audit-result": {"sensor_count": float64(120), "date": "2026-03-07"}}, // iteration 1: count changed
			},
		})

		assert.Equal(t, sfnDone, r.terminal)
		assert.Contains(t, r.events, "POST_RUN_DRIFT")
		assert.Equal(t, types.TriggerStatusCompleted, e2eTriggerStatus(mock, "pipe-b2"))
		assert.True(t, hasRerunRequest(mock, "pipe-b2"), "drift should write RERUN_REQUEST")
		assertAlertFormats(t, eb)
	})

	t.Run("DriftDetected_CustomField", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{{State: "succeeded"}}}
		d, _, eb := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-b2-cf")
		cfg.PostRun = &types.PostRunConfig{
			Evaluation: &types.EvaluationWindow{Window: "10m", Interval: "5m"},
			Rules:      []types.ValidationRule{{Key: "audit-result", Check: types.CheckExists}},
			DriftField: "count",
		}

		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline: cfg,
			sensors: map[string]map[string]interface{}{
				"upstream-complete": {"status": "ready"},
				"audit-result":      {"count": float64(100)},
			},
			postRunSensorUpdates: []map[string]map[string]interface{}{
				{}, // iteration 0: no updates; baseline saved
				{"audit-result": {"count": float64(120), "date": "2026-03-07"}}, // iteration 1: count changed
			},
		})

		assert.Equal(t, sfnDone, r.terminal)
		assert.Contains(t, r.events, "POST_RUN_DRIFT")
		assert.Equal(t, types.TriggerStatusCompleted, e2eTriggerStatus(mock, "pipe-b2-cf"))
		assert.True(t, hasRerunRequest(mock, "pipe-b2-cf"), "drift on custom field should write RERUN_REQUEST")
		assertAlertFormats(t, eb)
	})

	t.Run("DriftDetected_DefaultField", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{{State: "succeeded"}}}
		d, _, eb := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-b2-df")
		cfg.PostRun = &types.PostRunConfig{
			Evaluation: &types.EvaluationWindow{Window: "10m", Interval: "5m"},
			Rules:      []types.ValidationRule{{Key: "audit-result", Check: types.CheckExists}},
			// DriftField intentionally left empty — should default to "sensor_count"
		}

		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline: cfg,
			sensors: map[string]map[string]interface{}{
				"upstream-complete": {"status": "ready"},
				"audit-result":      {"sensor_count": float64(100)},
			},
			postRunSensorUpdates: []map[string]map[string]interface{}{
				{}, // iteration 0: no updates; baseline saved
				{"audit-result": {"sensor_count": float64(120), "date": "2026-03-07"}}, // iteration 1: count changed
			},
		})

		assert.Equal(t, sfnDone, r.terminal)
		assert.Contains(t, r.events, "POST_RUN_DRIFT")
		assert.Equal(t, types.TriggerStatusCompleted, e2eTriggerStatus(mock, "pipe-b2-df"))
		assert.True(t, hasRerunRequest(mock, "pipe-b2-df"), "drift on default field should write RERUN_REQUEST")
		assertAlertFormats(t, eb)
	})

	t.Run("WindowExhausted", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{{State: "succeeded"}}}
		d, _, eb := buildE2EDeps(mock, tr, sc)

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
		assert.NotContains(t, r.events, "POST_RUN_DRIFT")
		assert.Equal(t, types.TriggerStatusCompleted, e2eTriggerStatus(mock, "pipe-b3"))
		assertAlertFormats(t, eb)
	})
}

// =========================================================================
// SLA Monitoring
// =========================================================================

func TestE2E_SLAMonitoring(t *testing.T) {
	ctx := context.Background()

	t.Run("SLAMet", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{{State: "succeeded"}}}
		d, _, eb := buildE2EDeps(mock, tr, sc)
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

	t.Run("SLAWarningFired", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, _, eb := buildE2EDeps(mock, tr, sc)

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

	t.Run("SLABreachFired", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, _, eb := buildE2EDeps(mock, tr, sc)

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

	t.Run("SLABreachThenJobSucceeds", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{{State: "succeeded"}}}
		d, _, eb := buildE2EDeps(mock, tr, sc)
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
		assert.Equal(t, types.TriggerStatusCompleted, e2eTriggerStatus(mock, "pipe-c4"))
		assertAlertFormats(t, eb)
	})
}

// =========================================================================
// Automatic Retries (Stream-Router Job Failure Handling)
// =========================================================================

func TestE2E_AutoRetries(t *testing.T) {
	ctx := context.Background()

	t.Run("JobFailAutoRetrySuccess", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{{State: "failed", Message: "OOM"}}}
		d, sfnM, eb := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-d1")
		cfg.Job.MaxRetries = 2

		// Phase 1: Run SFN to job failure
		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline: cfg,
			sensors:  map[string]map[string]interface{}{"upstream-complete": {"status": "ready"}},
		})
		assert.Equal(t, sfnDone, r.terminal)
		assert.Contains(t, r.events, "JOB_FAILED")
		assert.Equal(t, types.TriggerStatusFailedFinal, e2eTriggerStatus(mock, "pipe-d1"))

		// Phase 2: Simulate DynamoDB stream event for the JOB#fail record
		jobSK := findLatestJobSK(mock, "pipe-d1")
		require.NotEmpty(t, jobSK, "should have a joblog entry")

		sfnCountBefore := len(sfnM.executions)
		_, err := lambda.HandleStreamEvent(ctx, d, makeJobStreamEvent("pipe-d1", jobSK, "fail"))
		require.NoError(t, err)

		// Verify: new SFN execution started (auto-retry under maxRetries limit)
		sfnM.mu.Lock()
		assert.Greater(t, len(sfnM.executions), sfnCountBefore, "should start rerun SFN")
		sfnM.mu.Unlock()
		assertAlertFormats(t, eb)
	})

	t.Run("JobFailRetryExhausted", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{{State: "failed"}}}
		d, sfnM, eb := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-d2")
		cfg.Job.MaxRetries = 0 // no retries allowed

		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline: cfg,
			sensors:  map[string]map[string]interface{}{"upstream-complete": {"status": "ready"}},
		})
		assert.Equal(t, sfnDone, r.terminal)

		// Simulate JOB#fail stream event
		jobSK := findLatestJobSK(mock, "pipe-d2")
		require.NotEmpty(t, jobSK)

		sfnCountBefore := len(sfnM.executions)
		eb.mu.Lock()
		eb.events = nil
		eb.mu.Unlock()

		_, err := lambda.HandleStreamEvent(ctx, d, makeJobStreamEvent("pipe-d2", jobSK, "fail"))
		require.NoError(t, err)

		// Verify: no new SFN, RETRY_EXHAUSTED published, status=FAILED_FINAL
		sfnM.mu.Lock()
		assert.Equal(t, sfnCountBefore, len(sfnM.executions), "should not start rerun")
		sfnM.mu.Unlock()
		assert.Contains(t, collectEventTypes(eb), "RETRY_EXHAUSTED")
		assert.Equal(t, types.TriggerStatusFailedFinal, e2eTriggerStatus(mock, "pipe-d2"))
		assertAlertFormats(t, eb)
	})
}

// =========================================================================
// Failure Classification (PERMANENT category routing)
// =========================================================================

func TestE2E_FailureClassification(t *testing.T) {
	ctx := context.Background()

	t.Run("PermanentFailureNoRetry", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{{
			State: "failed", Message: "syntax error", FailureCategory: types.FailurePermanent,
		}}}
		d, sfnM, eb := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-fc1")
		cfg.Job.MaxRetries = 3
		cfg.Job.MaxCodeRetries = intPtr(0)

		// Phase 1: SFN runs, job fails with PERMANENT category
		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline: cfg,
			sensors:  map[string]map[string]interface{}{"upstream-complete": {"status": "ready"}},
		})
		assert.Equal(t, sfnDone, r.terminal)
		assert.Contains(t, r.events, "JOB_FAILED")

		// Phase 2: Stream event from JOB#fail record
		jobSK := findLatestJobSK(mock, "pipe-fc1")
		require.NotEmpty(t, jobSK)

		sfnCountBefore := len(sfnM.executions)
		eb.mu.Lock()
		eb.events = nil
		eb.mu.Unlock()

		_, err := lambda.HandleStreamEvent(ctx, d, makeJobStreamEvent("pipe-fc1", jobSK, "fail"))
		require.NoError(t, err)

		// Verify: no retry (MaxCodeRetries=0), RETRY_EXHAUSTED event
		sfnM.mu.Lock()
		assert.Equal(t, sfnCountBefore, len(sfnM.executions), "PERMANENT with MaxCodeRetries=0 should not retry")
		sfnM.mu.Unlock()
		assert.Contains(t, collectEventTypes(eb), "RETRY_EXHAUSTED")
		assert.Equal(t, types.TriggerStatusFailedFinal, e2eTriggerStatus(mock, "pipe-fc1"))
		assertAlertFormats(t, eb)
	})

	t.Run("PermanentFailureOneRetry", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{{
			State: "failed", Message: "import error", FailureCategory: types.FailurePermanent,
		}}}
		d, sfnM, eb := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-fc2")
		cfg.Job.MaxRetries = 3
		cfg.Job.MaxCodeRetries = intPtr(1)

		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline: cfg,
			sensors:  map[string]map[string]interface{}{"upstream-complete": {"status": "ready"}},
		})
		assert.Equal(t, sfnDone, r.terminal)
		assert.Contains(t, r.events, "JOB_FAILED")

		// Phase 2: First failure should retry (0 < 1)
		jobSK := findLatestJobSK(mock, "pipe-fc2")
		require.NotEmpty(t, jobSK)

		sfnCountBefore := len(sfnM.executions)
		_, err := lambda.HandleStreamEvent(ctx, d, makeJobStreamEvent("pipe-fc2", jobSK, "fail"))
		require.NoError(t, err)

		sfnM.mu.Lock()
		assert.Greater(t, len(sfnM.executions), sfnCountBefore, "PERMANENT with MaxCodeRetries=1 should retry once")
		sfnM.mu.Unlock()
		assertAlertFormats(t, eb)
	})
}

// =========================================================================
// Re-Run / Replay (Manual via RERUN_REQUEST)
// =========================================================================

func TestE2E_RerunReplay(t *testing.T) {
	ctx := context.Background()

	t.Run("RerunAccepted", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, sfnM, eb := buildE2EDeps(mock, tr, sc)

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
		_, err := lambda.HandleStreamEvent(ctx, d, makeRerunRequestStreamEvent("pipe-e1"))
		require.NoError(t, err)

		// Verify: new SFN started, rerun-accepted joblog written
		sfnM.mu.Lock()
		assert.Greater(t, len(sfnM.executions), sfnCountBefore, "should start rerun SFN")
		sfnM.mu.Unlock()
		assert.Contains(t, collectJoblogEvents(mock, "pipe-e1"), "rerun-accepted")
		assertAlertFormats(t, eb)
	})

	t.Run("RerunRejected", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, sfnM, eb := buildE2EDeps(mock, tr, sc)

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
		_, err := lambda.HandleStreamEvent(ctx, d, makeRerunRequestStreamEvent("pipe-e2"))
		require.NoError(t, err)

		// Verify: no SFN, RERUN_REJECTED published
		sfnM.mu.Lock()
		assert.Equal(t, sfnCountBefore, len(sfnM.executions), "should not start SFN")
		sfnM.mu.Unlock()
		assert.Contains(t, collectEventTypes(eb), "RERUN_REJECTED")
		assert.Contains(t, collectJoblogEvents(mock, "pipe-e2"), "rerun-rejected")
		assertAlertFormats(t, eb)
	})

	t.Run("LateDataArrival", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, _, eb := buildE2EDeps(mock, tr, sc)

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
		_, err := lambda.HandleStreamEvent(ctx, d, lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}})
		require.NoError(t, err)

		// Lock already held → late data path
		evts := collectEventTypes(eb)
		assert.Contains(t, evts, "LATE_DATA_ARRIVAL")
		assert.Contains(t, collectJoblogEvents(mock, "pipe-e3"), "late-data-arrival")
		assert.True(t, hasRerunRequest(mock, "pipe-e3"), "late data should write RERUN_REQUEST")
		assertAlertFormats(t, eb)
	})
}

// =========================================================================
// Drift → Re-Trigger Compound Scenarios
// =========================================================================

func TestE2E_DriftRetrigger(t *testing.T) {
	ctx := context.Background()

	t.Run("DriftRetriggerSuccess", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{{State: "succeeded"}}}
		d, sfnM, eb := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-f1")
		cfg.PostRun = &types.PostRunConfig{
			Evaluation: &types.EvaluationWindow{Window: "10m", Interval: "5m"},
			Rules:      []types.ValidationRule{{Key: "audit-result", Check: types.CheckExists}},
		}

		// Phase 1: SFN with drift detection
		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline: cfg,
			sensors: map[string]map[string]interface{}{
				"upstream-complete": {"status": "ready"},
				"audit-result":      {"sensor_count": float64(100)},
			},
			postRunSensorUpdates: []map[string]map[string]interface{}{
				{},
				{"audit-result": {"sensor_count": float64(120), "date": "2026-03-07"}},
			},
		})
		assert.Equal(t, sfnDone, r.terminal)
		assert.Contains(t, r.events, "POST_RUN_DRIFT")
		assert.True(t, hasRerunRequest(mock, "pipe-f1"))

		// Phase 2: Stream-router processes RERUN_REQUEST
		sfnCountBefore := len(sfnM.executions)
		_, err := lambda.HandleStreamEvent(ctx, d, makeRerunRequestStreamEvent("pipe-f1"))
		require.NoError(t, err)

		// Verify: new SFN started for re-trigger
		sfnM.mu.Lock()
		assert.Greater(t, len(sfnM.executions), sfnCountBefore, "drift re-trigger should start new SFN")
		sfnM.mu.Unlock()
		assertAlertFormats(t, eb)
	})

	t.Run("DriftRetriggerJobFails", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{{State: "succeeded"}}}
		d, sfnM, eb := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-f2")
		cfg.PostRun = &types.PostRunConfig{
			Evaluation: &types.EvaluationWindow{Window: "10m", Interval: "5m"},
			Rules:      []types.ValidationRule{{Key: "audit-result", Check: types.CheckExists}},
		}

		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline: cfg,
			sensors: map[string]map[string]interface{}{
				"upstream-complete": {"status": "ready"},
				"audit-result":      {"sensor_count": float64(100)},
			},
			postRunSensorUpdates: []map[string]map[string]interface{}{
				{},
				{"audit-result": {"sensor_count": float64(80), "date": "2026-03-07"}},
			},
		})
		assert.Equal(t, sfnDone, r.terminal)
		assert.Contains(t, r.events, "POST_RUN_DRIFT")

		sfnCountBefore := len(sfnM.executions)
		_, err := lambda.HandleStreamEvent(ctx, d, makeRerunRequestStreamEvent("pipe-f2"))
		require.NoError(t, err)

		sfnM.mu.Lock()
		assert.Greater(t, len(sfnM.executions), sfnCountBefore, "drift re-trigger should start SFN")
		sfnM.mu.Unlock()
		assertAlertFormats(t, eb)
	})

	t.Run("DriftRetriggerInfraFailure", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{{State: "succeeded"}}}
		d, sfnM, eb := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-f3")
		cfg.PostRun = &types.PostRunConfig{
			Evaluation: &types.EvaluationWindow{Window: "10m", Interval: "5m"},
			Rules:      []types.ValidationRule{{Key: "audit-result", Check: types.CheckExists}},
		}

		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline: cfg,
			sensors: map[string]map[string]interface{}{
				"upstream-complete": {"status": "ready"},
				"audit-result":      {"sensor_count": float64(200)},
			},
			postRunSensorUpdates: []map[string]map[string]interface{}{
				{},
				{"audit-result": {"sensor_count": float64(250), "date": "2026-03-07"}},
			},
		})
		assert.Equal(t, sfnDone, r.terminal)
		assert.Contains(t, r.events, "POST_RUN_DRIFT")

		// Phase 2: verify the RERUN_REQUEST was written, allowing re-trigger
		sfnCountBefore := len(sfnM.executions)
		_, err := lambda.HandleStreamEvent(ctx, d, makeRerunRequestStreamEvent("pipe-f3"))
		require.NoError(t, err)

		sfnM.mu.Lock()
		assert.Greater(t, len(sfnM.executions), sfnCountBefore)
		sfnM.mu.Unlock()
		assertAlertFormats(t, eb)
	})
}

// =========================================================================
// Rerun Limit Enforcement
// =========================================================================

func TestE2E_RerunLimits(t *testing.T) {
	ctx := context.Background()

	t.Run("DriftRerunLimitExceeded", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, sfnM, eb := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-rl1")
		cfg.Job.MaxDriftReruns = intPtr(1)
		cfg.PostRun = &types.PostRunConfig{
			Evaluation: &types.EvaluationWindow{Window: "10m", Interval: "5m"},
			Rules:      []types.ValidationRule{{Key: "quality-check", Check: types.CheckExists}},
		}
		seedConfig(mock, cfg)

		// Seed a completed trigger + successful job
		seedTriggerLock(mock, "pipe-rl1", "2026-03-07")
		require.NoError(t, d.Store.SetTriggerStatus(ctx, "pipe-rl1", "stream", "2026-03-07", types.TriggerStatusCompleted))
		oldTS := fmt.Sprintf("%d", time.Now().Add(-1*time.Hour).UnixMilli())
		mock.putRaw("joblog", map[string]ddbtypes.AttributeValue{
			"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("pipe-rl1")},
			"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("stream", "2026-03-07", oldTS)},
			"event": &ddbtypes.AttributeValueMemberS{Value: types.JobEventSuccess},
		})

		// Seed fresh sensor data (so circuit breaker would pass if reached)
		freshTS := fmt.Sprintf("%d", time.Now().UnixMilli())
		require.NoError(t, d.Store.WriteSensor(ctx, "pipe-rl1", "upstream-complete", map[string]interface{}{
			"status": "ready", "updatedAt": freshTS,
		}))

		// Seed one existing drift rerun — at limit (budget=1, count=1)
		seedRerunWithReason(mock, "pipe-rl1", "2026-03-07", "data-drift")

		// Send a data-drift RERUN_REQUEST — should be rejected
		sfnCountBefore := len(sfnM.executions)
		_, err := lambda.HandleStreamEvent(ctx, d, makeRerunRequestWithReasonE2E("pipe-rl1", "data-drift"))
		require.NoError(t, err)

		// Verify: no SFN started, RERUN_REJECTED event + joblog entry
		sfnM.mu.Lock()
		assert.Equal(t, sfnCountBefore, len(sfnM.executions), "should not start SFN when drift limit exceeded")
		sfnM.mu.Unlock()
		assert.Contains(t, collectEventTypes(eb), "RERUN_REJECTED")
		assert.Contains(t, collectJoblogEvents(mock, "pipe-rl1"), "rerun-rejected")
		assertAlertFormats(t, eb)
	})

	t.Run("LateDataCountedAsDrift", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, sfnM, eb := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-rl2")
		cfg.Job.MaxDriftReruns = intPtr(1)
		cfg.PostRun = &types.PostRunConfig{
			Evaluation: &types.EvaluationWindow{Window: "10m", Interval: "5m"},
			Rules:      []types.ValidationRule{{Key: "quality-check", Check: types.CheckExists}},
		}
		seedConfig(mock, cfg)

		// Seed a completed trigger + successful job
		seedTriggerLock(mock, "pipe-rl2", "2026-03-07")
		require.NoError(t, d.Store.SetTriggerStatus(ctx, "pipe-rl2", "stream", "2026-03-07", types.TriggerStatusCompleted))
		oldTS := fmt.Sprintf("%d", time.Now().Add(-1*time.Hour).UnixMilli())
		mock.putRaw("joblog", map[string]ddbtypes.AttributeValue{
			"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("pipe-rl2")},
			"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("stream", "2026-03-07", oldTS)},
			"event": &ddbtypes.AttributeValueMemberS{Value: types.JobEventSuccess},
		})

		// Seed fresh sensor data (so circuit breaker would pass if reached)
		freshTS := fmt.Sprintf("%d", time.Now().UnixMilli())
		require.NoError(t, d.Store.WriteSensor(ctx, "pipe-rl2", "upstream-complete", map[string]interface{}{
			"status": "ready", "updatedAt": freshTS,
		}))

		// Seed one existing data-drift rerun — uses up the drift budget
		seedRerunWithReason(mock, "pipe-rl2", "2026-03-07", "data-drift")

		// Send a late-data RERUN_REQUEST — should be rejected because
		// late-data shares the drift budget (count 1 >= budget 1)
		sfnCountBefore := len(sfnM.executions)
		_, err := lambda.HandleStreamEvent(ctx, d, makeRerunRequestWithReasonE2E("pipe-rl2", "late-data"))
		require.NoError(t, err)

		// Verify: no SFN started, RERUN_REJECTED event + joblog entry
		sfnM.mu.Lock()
		assert.Equal(t, sfnCountBefore, len(sfnM.executions), "should not start SFN when drift budget exhausted by late-data")
		sfnM.mu.Unlock()
		assert.Contains(t, collectEventTypes(eb), "RERUN_REJECTED")
		assert.Contains(t, collectJoblogEvents(mock, "pipe-rl2"), "rerun-rejected")
		assertAlertFormats(t, eb)
	})
}

// =========================================================================
// Watchdog Health Checks
// =========================================================================

func TestE2E_Watchdog(t *testing.T) {
	ctx := context.Background()

	t.Run("StaleTriggerDetected", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, _, eb := buildE2EDeps(mock, tr, sc)

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
		assert.Equal(t, types.TriggerStatusFailedFinal, e2eTriggerStatus(mock, "pipe-g1"))
		assertAlertFormats(t, eb)
	})

	t.Run("MissedCronSchedule", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, _, eb := buildE2EDeps(mock, tr, sc)

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

	t.Run("ReconcileSkipsCompletedPipeline", func(t *testing.T) {
		// Sensor condition met, no trigger row (simulates TTL expiry), but
		// joblog has terminal "success" event → watchdog must NOT re-trigger.
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, sfnM, eb := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-g3")
		seedConfig(mock, cfg)

		// Seed sensor that satisfies the trigger condition.
		require.NoError(t, d.Store.WriteSensor(ctx, "pipe-g3", "upstream-complete", map[string]interface{}{
			"status": "ready", "date": "2026-03-07",
		}))

		// No trigger row — simulates TTL expiry after completion.
		// Seed terminal joblog entry to prove pipeline already ran.
		ts := fmt.Sprintf("%d", time.Now().UnixMilli())
		sk := types.JobSK("stream", "2026-03-07", ts)
		mock.putRaw("joblog", map[string]ddbtypes.AttributeValue{
			"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("pipe-g3")},
			"SK":    &ddbtypes.AttributeValueMemberS{Value: sk},
			"event": &ddbtypes.AttributeValueMemberS{Value: types.JobEventSuccess},
		})

		sfnM.mu.Lock()
		sfnCountBefore := len(sfnM.executions)
		sfnM.mu.Unlock()

		err := lambda.HandleWatchdog(ctx, d)
		require.NoError(t, err)

		// No TRIGGER_RECOVERED event, no SFN started.
		assert.NotContains(t, collectEventTypes(eb), "TRIGGER_RECOVERED")
		sfnM.mu.Lock()
		assert.Equal(t, sfnCountBefore, len(sfnM.executions), "should not re-trigger completed pipeline")
		sfnM.mu.Unlock()
	})

	t.Run("ReconcileSkipsFailedPipeline", func(t *testing.T) {
		// Same as above but with a terminal "fail" joblog event.
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, sfnM, eb := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-g4")
		seedConfig(mock, cfg)

		require.NoError(t, d.Store.WriteSensor(ctx, "pipe-g4", "upstream-complete", map[string]interface{}{
			"status": "ready", "date": "2026-03-07",
		}))

		ts := fmt.Sprintf("%d", time.Now().UnixMilli())
		sk := types.JobSK("stream", "2026-03-07", ts)
		mock.putRaw("joblog", map[string]ddbtypes.AttributeValue{
			"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("pipe-g4")},
			"SK":    &ddbtypes.AttributeValueMemberS{Value: sk},
			"event": &ddbtypes.AttributeValueMemberS{Value: types.JobEventFail},
		})

		sfnM.mu.Lock()
		sfnCountBefore := len(sfnM.executions)
		sfnM.mu.Unlock()

		err := lambda.HandleWatchdog(ctx, d)
		require.NoError(t, err)

		assert.NotContains(t, collectEventTypes(eb), "TRIGGER_RECOVERED")
		sfnM.mu.Lock()
		assert.Equal(t, sfnCountBefore, len(sfnM.executions), "should not re-trigger failed pipeline")
		sfnM.mu.Unlock()
	})

	t.Run("ReconcileRecoversGenuineMiss", func(t *testing.T) {
		// Sensor condition met, no trigger, no joblog → genuine miss → recovery.
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, sfnM, eb := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-g5")
		seedConfig(mock, cfg)

		require.NoError(t, d.Store.WriteSensor(ctx, "pipe-g5", "upstream-complete", map[string]interface{}{
			"status": "ready", "date": "2026-03-07",
		}))

		sfnM.mu.Lock()
		sfnCountBefore := len(sfnM.executions)
		sfnM.mu.Unlock()

		err := lambda.HandleWatchdog(ctx, d)
		require.NoError(t, err)

		assert.Contains(t, collectEventTypes(eb), "TRIGGER_RECOVERED")
		sfnM.mu.Lock()
		assert.Greater(t, len(sfnM.executions), sfnCountBefore, "should recover missed trigger")
		sfnM.mu.Unlock()
		assert.Equal(t, types.TriggerStatusRunning, e2eTriggerStatus(mock, "pipe-g5"))
	})

	t.Run("TerminalTriggerRetainsRecord", func(t *testing.T) {
		// Full SFN run → COMPLETED status → trigger record persists (TTL removed).
		// Then watchdog runs → no TRIGGER_RECOVERED because HasTriggerForDate returns true.
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{{State: "succeeded"}}}
		d, _, eb := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-g6")
		r := runSFN(t, ctx, d, mock, eb, e2eScenario{
			pipeline: cfg,
			sensors:  map[string]map[string]interface{}{"upstream-complete": {"status": "ready", "date": "2026-03-07"}},
		})
		assert.Equal(t, sfnDone, r.terminal)
		assert.Equal(t, types.TriggerStatusCompleted, e2eTriggerStatus(mock, "pipe-g6"))

		// Verify trigger record still exists (TTL removed by SetTriggerStatus).
		mock.mu.Lock()
		trigKey := ddbItemKey(testControlTable, types.PipelinePK("pipe-g6"), types.TriggerSK("stream", "2026-03-07"))
		trigItem, exists := mock.items[trigKey]
		hasTTL := false
		if exists {
			_, hasTTL = trigItem["ttl"]
		}
		mock.mu.Unlock()
		assert.True(t, exists, "trigger record should persist after terminal status")
		assert.False(t, hasTTL, "TTL should be removed on terminal trigger")

		// Run watchdog — should NOT re-trigger because HasTriggerForDate finds the record.
		eb2 := &mockEventBridge{}
		d.EventBridge = eb2
		err := lambda.HandleWatchdog(ctx, d)
		require.NoError(t, err)
		assert.NotContains(t, collectEventTypes(eb2), "TRIGGER_RECOVERED")
	})
}

// =========================================================================
// SLA Branch Completeness
// =========================================================================

func TestE2E_SLABranchCompleteness(t *testing.T) {
	ctx := context.Background()

	t.Run("SLACancelPastWarningBeforeBreach", func(t *testing.T) {
		// Cancel when warning has passed but breach is still in the future.
		// Expected: cancel returns SLA_MET (completed before breach = MET).
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{{State: "succeeded"}}}
		d, _, eb := buildE2EDeps(mock, tr, sc)

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
		assert.Equal(t, "SLA_MET", r.slaOutcome)
		// SLA_MET event should be published (binary verdict: completed before breach).
		assert.Contains(t, r.events, "SLA_MET")
		assertAlertFormats(t, eb)
	})

	t.Run("SLAFireAlertSuppression", func(t *testing.T) {
		// Fire-alert called after pipeline completed → alert suppressed.
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, _, eb := buildE2EDeps(mock, tr, sc)

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
// Stream-Router Entry Points
// =========================================================================

func TestE2E_StreamRouterEntryPoints(t *testing.T) {
	ctx := context.Background()

	t.Run("InitialSensorTrigger", func(t *testing.T) {
		// Sensor event arrives → trigger condition met → lock acquired → SFN started.
		// This is the fundamental entry point for stream-triggered pipelines.
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, sfnM, eb := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-i1")
		seedConfig(mock, cfg)

		// No trigger row exists — fresh pipeline.
		record := makeSensorRecord("pipe-i1", "upstream-complete", map[string]events.DynamoDBAttributeValue{
			"status": events.NewStringAttribute("ready"),
			"date":   events.NewStringAttribute("2026-03-07"),
		})

		sfnCountBefore := len(sfnM.executions)
		_, err := lambda.HandleStreamEvent(ctx, d, lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}})
		require.NoError(t, err)

		// Verify: SFN started, trigger lock acquired, JOB_TRIGGERED event published.
		sfnM.mu.Lock()
		assert.Greater(t, len(sfnM.executions), sfnCountBefore, "sensor trigger should start SFN")
		sfnM.mu.Unlock()
		assert.Equal(t, types.TriggerStatusRunning, e2eTriggerStatus(mock, "pipe-i1"))
		assertAlertFormats(t, eb)
	})

	t.Run("JobTimeoutAutoRetry", func(t *testing.T) {
		// Job timeout flows through handleJobFailure → auto-retry (same as fail).
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, sfnM, eb := buildE2EDeps(mock, tr, sc)

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
		_, err := lambda.HandleStreamEvent(ctx, d, makeJobStreamEvent("pipe-i2", jobSK, types.JobEventTimeout))
		require.NoError(t, err)

		// Verify: auto-retry started (timeout is retryable just like fail).
		sfnM.mu.Lock()
		assert.Greater(t, len(sfnM.executions), sfnCountBefore, "timeout should trigger auto-retry")
		sfnM.mu.Unlock()
		assertAlertFormats(t, eb)
	})

	t.Run("RerunAfterFailure", func(t *testing.T) {
		// RERUN_REQUEST when prior job was "fail" → allowed immediately (no freshness check).
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, sfnM, eb := buildE2EDeps(mock, tr, sc)

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
		_, err := lambda.HandleStreamEvent(ctx, d, makeRerunRequestStreamEvent("pipe-i3"))
		require.NoError(t, err)

		// Verify: rerun accepted despite old sensor data (failure skips freshness check).
		sfnM.mu.Lock()
		assert.Greater(t, len(sfnM.executions), sfnCountBefore, "rerun after failure should be accepted without freshness check")
		sfnM.mu.Unlock()
		assert.Contains(t, collectJoblogEvents(mock, "pipe-i3"), "rerun-accepted")
		assertAlertFormats(t, eb)
	})
}

// =========================================================================
// Cross-Handler Edge Cases
// =========================================================================

func TestE2E_CrossHandlerEdgeCases(t *testing.T) {
	ctx := context.Background()

	t.Run("WatchdogSelfHealing", func(t *testing.T) {
		// reconcileSensorTriggers: sensor condition met, no trigger row →
		// lock acquired, SFN started, TRIGGER_RECOVERED published.
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, sfnM, eb := buildE2EDeps(mock, tr, sc)

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
		assert.Equal(t, types.TriggerStatusRunning, e2eTriggerStatus(mock, "pipe-j1"))
		assertAlertFormats(t, eb)
	})

	t.Run("CheckJobNonTerminalFallthrough", func(t *testing.T) {
		// Joblog has "infra-trigger-failure" (non-terminal) → check-job skips it,
		// falls through to StatusChecker → polls real job status → success.
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{seq: []lambda.StatusResult{{State: "succeeded"}}}
		d, _, eb := buildE2EDeps(mock, tr, sc)

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
		// JOB_COMPLETED is emitted by stream-router when the JOB# record arrives, not by the orchestrator.
		assert.Equal(t, types.TriggerStatusCompleted, e2eTriggerStatus(mock, "pipe-j2"))
		joblogs := collectJoblogEvents(mock, "pipe-j2")
		assert.Contains(t, joblogs, "success", "StatusChecker success should be written to joblog")
		assertAlertFormats(t, eb)
	})
}

// =========================================================================
// Alert Format Verification (covers all event types)
// =========================================================================

func TestE2E_AlertFormatAllEventTypes(t *testing.T) {
	eventTypes := []struct {
		detailType   string
		wantEmojiHex string // raw byte prefix to check
	}{
		{"VALIDATION_PASSED", "\xe2\x84\xb9"},        // info
		{"VALIDATION_EXHAUSTED", "\xf0\x9f\x94\xb4"}, // red circle
		{"JOB_TRIGGERED", "\xe2\x84\xb9"},            // info
		{"JOB_COMPLETED", "\xe2\x84\xb9"},            // info
		{"JOB_FAILED", "\xf0\x9f\x94\xb4"},           // red circle
		{"RETRY_EXHAUSTED", "\xf0\x9f\x94\xb4"},      // red circle
		{"SLA_WARNING", "\xf0\x9f\x9f\xa1"},          // yellow circle
		{"SLA_BREACH", "\xf0\x9f\x94\xb4"},           // red circle
		{"SLA_MET", "\xe2\x9c\x85"},                  // check mark
		{"DATA_DRIFT", "\xf0\x9f\x94\xb4"},           // red circle
		{"LATE_DATA_ARRIVAL", "\xe2\x84\xb9"},        // info
		{"RERUN_REJECTED", "\xe2\x84\xb9"},           // info
		{"SFN_TIMEOUT", "\xf0\x9f\x94\xb4"},          // red circle
		{"SCHEDULE_MISSED", "\xf0\x9f\x94\xb4"},      // red circle
		{"TRIGGER_RECOVERED", "\xe2\x84\xb9"},        // info
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

// =========================================================================
// Helper: count SFN executions (thread-safe)
// =========================================================================

func countSFNExecutions(sfnM *mockSFN) int {
	sfnM.mu.Lock()
	defer sfnM.mu.Unlock()
	return len(sfnM.executions)
}

// resetEventBridge clears all collected events for phase-based assertions.
func resetEventBridge(eb *mockEventBridge) {
	eb.mu.Lock()
	eb.events = nil
	eb.mu.Unlock()
}

// seedCompletedPipelineE2E sets up a pipeline that has completed: COMPLETED trigger,
// successful job event, and fresh sensor data (for circuit breaker freshness check).
func seedCompletedPipelineE2E(t *testing.T, ctx context.Context, d *lambda.Deps, mock *mockDDB, pipelineID, date string) {
	t.Helper()
	seedTriggerWithStatus(mock, pipelineID, date, types.TriggerStatusCompleted)
	oldTS := fmt.Sprintf("%d", time.Now().Add(-1*time.Hour).UnixMilli())
	mock.putRaw("joblog", map[string]ddbtypes.AttributeValue{
		"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
		"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("stream", date, oldTS)},
		"event": &ddbtypes.AttributeValueMemberS{Value: types.JobEventSuccess},
	})
	freshTS := fmt.Sprintf("%d", time.Now().UnixMilli())
	require.NoError(t, d.Store.WriteSensor(ctx, pipelineID, "upstream-complete", map[string]interface{}{
		"status": "ready", "updatedAt": freshTS,
	}))
}

// assertTriggerLockExists checks that a trigger lock exists for the given key.
func assertTriggerLockExists(t *testing.T, mock *mockDDB, pipelineID, schedule, date string) {
	t.Helper()
	mock.mu.Lock()
	defer mock.mu.Unlock()
	key := ddbItemKey(testControlTable, types.PipelinePK(pipelineID), types.TriggerSK(schedule, date))
	_, ok := mock.items[key]
	assert.True(t, ok, "expected trigger lock for %s/%s/%s", pipelineID, schedule, date)
}

// assertNoTriggerLock checks that no trigger lock exists for the given key.
func assertNoTriggerLock(t *testing.T, mock *mockDDB, pipelineID, schedule, date string) {
	t.Helper()
	mock.mu.Lock()
	defer mock.mu.Unlock()
	key := ddbItemKey(testControlTable, types.PipelinePK(pipelineID), types.TriggerSK(schedule, date))
	_, ok := mock.items[key]
	assert.False(t, ok, "expected no trigger lock for %s/%s/%s", pipelineID, schedule, date)
}

// =========================================================================
// Gap 1: Manual rerun vs drift rerun budget separation
// =========================================================================

func TestE2E_RerunBudgetSeparation(t *testing.T) {
	ctx := context.Background()

	t.Run("ExhaustDriftThenManualStillWorks", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, sfnM, eb := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-bs1")
		cfg.Job.MaxDriftReruns = intPtr(1)
		cfg.Job.MaxManualReruns = intPtr(2)
		cfg.PostRun = &types.PostRunConfig{
			Rules: []types.ValidationRule{{Key: "quality-check", Check: types.CheckExists}},
		}
		seedConfig(mock, cfg)
		seedCompletedPipelineE2E(t, ctx, d, mock, "pipe-bs1", "2026-03-07")

		// Phase 1: First drift rerun — accepted (0 < budget 1).
		sfnBefore := countSFNExecutions(sfnM)
		_, handleErr := lambda.HandleStreamEvent(ctx, d, makeRerunRequestWithReasonE2E("pipe-bs1", "data-drift"))
		require.NoError(t, handleErr)
		assert.Greater(t, countSFNExecutions(sfnM), sfnBefore, "first drift rerun should start SFN")

		// Phase 2: Second drift rerun — rejected (1 >= budget 1).
		resetEventBridge(eb)
		sfnBefore = countSFNExecutions(sfnM)
		_, handleErr = lambda.HandleStreamEvent(ctx, d, makeRerunRequestWithReasonE2E("pipe-bs1", "data-drift"))
		require.NoError(t, handleErr)
		assert.Equal(t, sfnBefore, countSFNExecutions(sfnM), "second drift rerun should NOT start SFN")
		assert.Contains(t, collectEventTypes(eb), "RERUN_REJECTED")

		// Phase 3: First manual rerun — accepted despite drift budget exhausted.
		resetEventBridge(eb)
		sfnBefore = countSFNExecutions(sfnM)
		_, handleErr = lambda.HandleStreamEvent(ctx, d, makeRerunRequestWithReasonE2E("pipe-bs1", "manual"))
		require.NoError(t, handleErr)
		assert.Greater(t, countSFNExecutions(sfnM), sfnBefore, "first manual rerun should succeed")

		// Phase 4: Second manual rerun — accepted (1 < budget 2).
		resetEventBridge(eb)
		sfnBefore = countSFNExecutions(sfnM)
		_, handleErr = lambda.HandleStreamEvent(ctx, d, makeRerunRequestWithReasonE2E("pipe-bs1", "manual"))
		require.NoError(t, handleErr)
		assert.Greater(t, countSFNExecutions(sfnM), sfnBefore, "second manual rerun should succeed")

		// Phase 5: Third manual rerun — rejected (2 >= budget 2).
		resetEventBridge(eb)
		sfnBefore = countSFNExecutions(sfnM)
		_, handleErr = lambda.HandleStreamEvent(ctx, d, makeRerunRequestWithReasonE2E("pipe-bs1", "manual"))
		require.NoError(t, handleErr)
		assert.Equal(t, sfnBefore, countSFNExecutions(sfnM), "third manual rerun should NOT start SFN")
		assert.Contains(t, collectEventTypes(eb), "RERUN_REJECTED")
		assertAlertFormats(t, eb)
	})
}

// =========================================================================
// Gap 2: Post-run inflight drift (sensor changes while job is RUNNING)
// =========================================================================

func TestE2E_PostRunInflight(t *testing.T) {
	ctx := context.Background()

	t.Run("DriftWhileRunning_PublishesEvent_NoRerun", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, sfnM, eb := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-inf1")
		cfg.PostRun = &types.PostRunConfig{
			Rules: []types.ValidationRule{{Key: "audit-result", Check: types.CheckExists}},
		}
		seedConfig(mock, cfg)
		seedTriggerWithStatus(mock, "pipe-inf1", "2026-03-07", types.TriggerStatusRunning)

		// Baseline from a previous run (namespaced by rule key).
		require.NoError(t, d.Store.WriteSensor(ctx, "pipe-inf1", "postrun-baseline#2026-03-07",
			map[string]interface{}{"audit-result": map[string]interface{}{"sensor_count": float64(100)}}))

		// Sensor arrives with different count while job is running.
		record := makeSensorRecord("pipe-inf1", "audit-result", toStreamAttributes(map[string]interface{}{
			"sensor_count": float64(200),
			"date":         "2026-03-07",
		}))
		_, handleErr := lambda.HandleStreamEvent(ctx, d, lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}})
		require.NoError(t, handleErr)

		assert.Contains(t, collectEventTypes(eb), "POST_RUN_DRIFT_INFLIGHT")
		assert.False(t, hasRerunRequest(mock, "pipe-inf1"), "should NOT write rerun request while running")
		assert.Equal(t, 0, countSFNExecutions(sfnM))
		assertAlertFormats(t, eb)
	})

	t.Run("InflightDrift_CustomField", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, sfnM, eb := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-inf-cf")
		cfg.PostRun = &types.PostRunConfig{
			Rules:      []types.ValidationRule{{Key: "audit-result", Check: types.CheckExists}},
			DriftField: "count",
		}
		seedConfig(mock, cfg)
		seedTriggerWithStatus(mock, "pipe-inf-cf", "2026-03-07", types.TriggerStatusRunning)

		// Baseline uses custom field "count" (namespaced by rule key).
		require.NoError(t, d.Store.WriteSensor(ctx, "pipe-inf-cf", "postrun-baseline#2026-03-07",
			map[string]interface{}{"audit-result": map[string]interface{}{"count": float64(100)}}))

		// Sensor arrives with different count while job is running.
		record := makeSensorRecord("pipe-inf-cf", "audit-result", toStreamAttributes(map[string]interface{}{
			"count": float64(200),
			"date":  "2026-03-07",
		}))
		_, handleErr := lambda.HandleStreamEvent(ctx, d, lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}})
		require.NoError(t, handleErr)

		assert.Contains(t, collectEventTypes(eb), "POST_RUN_DRIFT_INFLIGHT")
		assert.False(t, hasRerunRequest(mock, "pipe-inf-cf"), "should NOT write rerun request while running")
		assert.Equal(t, 0, countSFNExecutions(sfnM))
		assertAlertFormats(t, eb)
	})

	t.Run("NoDriftWhileRunning_NoEvent", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, sfnM, eb := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-inf2")
		cfg.PostRun = &types.PostRunConfig{
			Rules: []types.ValidationRule{{Key: "audit-result", Check: types.CheckExists}},
		}
		seedConfig(mock, cfg)
		seedTriggerWithStatus(mock, "pipe-inf2", "2026-03-07", types.TriggerStatusRunning)

		// Baseline matches incoming sensor — no drift (namespaced by rule key).
		require.NoError(t, d.Store.WriteSensor(ctx, "pipe-inf2", "postrun-baseline#2026-03-07",
			map[string]interface{}{"audit-result": map[string]interface{}{"sensor_count": float64(100)}}))

		record := makeSensorRecord("pipe-inf2", "audit-result", toStreamAttributes(map[string]interface{}{
			"sensor_count": float64(100),
			"date":         "2026-03-07",
		}))
		_, handleErr := lambda.HandleStreamEvent(ctx, d, lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}})
		require.NoError(t, handleErr)

		assert.Empty(t, collectEventTypes(eb))
		assert.Equal(t, 0, countSFNExecutions(sfnM))
	})
}

// =========================================================================
// Gap 3: Calendar exclusion full window skip
// =========================================================================

func TestE2E_CalendarExclusionFullSkip(t *testing.T) {
	ctx := context.Background()

	t.Run("ExcludedDate_NoTriggerLock_NoSFN", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, sfnM, eb := buildE2EDeps(mock, tr, sc)

		today := time.Now().Format("2006-01-02")
		cfg := e2ePipeline("pipe-cal1")
		cfg.Schedule.Exclude = &types.ExclusionConfig{
			Dates: []string{today},
		}
		seedConfig(mock, cfg)

		record := makeSensorRecord("pipe-cal1", "upstream-complete",
			map[string]events.DynamoDBAttributeValue{"status": events.NewStringAttribute("ready")})
		_, handleErr := lambda.HandleStreamEvent(ctx, d, lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}})
		require.NoError(t, handleErr)

		assert.Equal(t, 0, countSFNExecutions(sfnM))
		assertNoTriggerLock(t, mock, "pipe-cal1", "stream", today)
		// Phase 2: PIPELINE_EXCLUDED event is published when the sensor trigger
		// is suppressed by a calendar exclusion.
		evts := collectEventTypes(eb)
		assert.Equal(t, []string{string(types.EventPipelineExcluded)}, evts)
	})
}

// =========================================================================
// Gap 4: Hour boundary rollover (per-hour execution)
// =========================================================================

func TestE2E_HourBoundaryRollover(t *testing.T) {
	ctx := context.Background()

	t.Run("TwoHoursGetIndependentTriggers", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, sfnM, _ := buildE2EDeps(mock, tr, sc)

		cfg := types.PipelineConfig{
			Pipeline: types.PipelineIdentity{ID: "pipe-hr1"},
			Schedule: types.ScheduleConfig{
				Trigger: &types.TriggerCondition{
					Key: "hourly-status", Check: types.CheckEquals, Field: "status", Value: "ready",
				},
				Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
			},
			Validation: types.ValidationConfig{
				Trigger: "ALL",
				Rules:   []types.ValidationRule{{Key: "hourly-status", Check: types.CheckExists}},
			},
			Job: types.JobConfig{
				Type:   types.TriggerCommand,
				Config: map[string]interface{}{"command": "echo hello"},
			},
		}
		seedConfig(mock, cfg)

		// Hour 23 sensor arrives.
		record23 := makeSensorRecord("pipe-hr1", "hourly-status#20260307T23", toStreamAttributes(map[string]interface{}{
			"status": "ready",
			"date":   "20260307",
			"hour":   "23",
		}))
		_, handleErr := lambda.HandleStreamEvent(ctx, d, lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record23}})
		require.NoError(t, handleErr)

		// Hour 00 (next day) sensor arrives.
		record00 := makeSensorRecord("pipe-hr1", "hourly-status#20260308T00", toStreamAttributes(map[string]interface{}{
			"status": "ready",
			"date":   "20260308",
			"hour":   "00",
		}))
		_, handleErr = lambda.HandleStreamEvent(ctx, d, lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record00}})
		require.NoError(t, handleErr)

		// Two independent SFN executions.
		sfnM.mu.Lock()
		require.Len(t, sfnM.executions, 2, "each hour should get its own SFN execution")
		var dates []string
		for _, exec := range sfnM.executions {
			var input map[string]interface{}
			require.NoError(t, json.Unmarshal([]byte(*exec.Input), &input))
			dates = append(dates, input["date"].(string))
		}
		sfnM.mu.Unlock()

		assert.Contains(t, dates, "2026-03-07T23")
		assert.Contains(t, dates, "2026-03-08T00")

		// Independent trigger locks.
		assertTriggerLockExists(t, mock, "pipe-hr1", "stream", "2026-03-07T23")
		assertTriggerLockExists(t, mock, "pipe-hr1", "stream", "2026-03-08T00")
	})
}

// =========================================================================
// Gap 5: Concurrent drift dedup via rerun budget
// =========================================================================

func TestE2E_ConcurrentDriftDedup(t *testing.T) {
	ctx := context.Background()

	t.Run("TwoDriftSensors_OnlyOneRerunAccepted", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, sfnM, eb := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-cd1")
		cfg.Job.MaxDriftReruns = intPtr(1)
		cfg.PostRun = &types.PostRunConfig{
			Rules: []types.ValidationRule{{Key: "audit-result", Check: types.CheckExists}},
		}
		seedConfig(mock, cfg)
		seedCompletedPipelineE2E(t, ctx, d, mock, "pipe-cd1", "2026-03-07")

		// Baseline captured at completion (namespaced by rule key).
		require.NoError(t, d.Store.WriteSensor(ctx, "pipe-cd1", "postrun-baseline#2026-03-07",
			map[string]interface{}{"audit-result": map[string]interface{}{"sensor_count": float64(100)}}))

		// First drift sensor arrives.
		record1 := makeSensorRecord("pipe-cd1", "audit-result", toStreamAttributes(map[string]interface{}{
			"sensor_count": float64(200),
			"date":         "2026-03-07",
		}))
		_, handleErr := lambda.HandleStreamEvent(ctx, d, lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record1}})
		require.NoError(t, handleErr)
		assert.Contains(t, collectEventTypes(eb), "POST_RUN_DRIFT")

		// Process first rerun request — accepted.
		resetEventBridge(eb)
		sfnBefore := countSFNExecutions(sfnM)
		_, handleErr = lambda.HandleStreamEvent(ctx, d, makeRerunRequestWithReasonE2E("pipe-cd1", "data-drift"))
		require.NoError(t, handleErr)
		assert.Greater(t, countSFNExecutions(sfnM), sfnBefore, "first drift rerun accepted")

		// Process second rerun request — rejected (budget exhausted).
		resetEventBridge(eb)
		sfnBefore = countSFNExecutions(sfnM)
		_, handleErr = lambda.HandleStreamEvent(ctx, d, makeRerunRequestWithReasonE2E("pipe-cd1", "data-drift"))
		require.NoError(t, handleErr)
		assert.Equal(t, sfnBefore, countSFNExecutions(sfnM), "second drift rerun rejected")
		assert.Contains(t, collectEventTypes(eb), "RERUN_REJECTED")
		assertAlertFormats(t, eb)
	})
}

// =========================================================================
// Gap 6: Post-run sensor arriving before baseline exists
// =========================================================================

func TestE2E_PostRunBeforeBaseline(t *testing.T) {
	ctx := context.Background()

	t.Run("NoBaseline_GracefulSkip", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, sfnM, eb := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-nb1")
		cfg.PostRun = &types.PostRunConfig{
			Rules: []types.ValidationRule{{Key: "audit-result", Check: types.CheckExists}},
		}
		seedConfig(mock, cfg)
		seedTriggerWithStatus(mock, "pipe-nb1", "2026-03-07", types.TriggerStatusRunning)
		// No baseline — job hasn't completed yet.

		record := makeSensorRecord("pipe-nb1", "audit-result", toStreamAttributes(map[string]interface{}{
			"sensor_count": float64(500),
			"date":         "2026-03-07",
		}))
		_, handleErr := lambda.HandleStreamEvent(ctx, d, lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}})
		require.NoError(t, handleErr)

		assert.Empty(t, collectEventTypes(eb), "should not publish any event when baseline is missing")
		assert.False(t, hasRerunRequest(mock, "pipe-nb1"))
		assert.Equal(t, 0, countSFNExecutions(sfnM))
	})
}

// =========================================================================
// Gap 7: Rerun after trigger TTL expiry
// =========================================================================

func TestE2E_RerunAfterTriggerTTLExpiry(t *testing.T) {
	ctx := context.Background()

	// When the trigger lock row has been deleted by DynamoDB TTL, ResetTriggerLock
	// returns (false, nil) and an INFRA_FAILURE event is published. No SFN is
	// started because the lock cannot be atomically reset.
	t.Run("TriggerDeleted_PublishesInfraFailure", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, sfnM, eb := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-ttl1")
		cfg.Job.MaxManualReruns = intPtr(2)
		seedConfig(mock, cfg)
		// NO trigger lock — simulates DynamoDB TTL deletion.

		// Seed a successful job event so circuit breaker can check freshness.
		oldTS := fmt.Sprintf("%d", time.Now().Add(-1*time.Hour).UnixMilli())
		mock.putRaw("joblog", map[string]ddbtypes.AttributeValue{
			"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("pipe-ttl1")},
			"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("stream", "2026-03-07", oldTS)},
			"event": &ddbtypes.AttributeValueMemberS{Value: types.JobEventSuccess},
		})
		freshTS := fmt.Sprintf("%d", time.Now().UnixMilli())
		require.NoError(t, d.Store.WriteSensor(ctx, "pipe-ttl1", "upstream-complete", map[string]interface{}{
			"status": "ready", "updatedAt": freshTS,
		}))

		sfnBefore := countSFNExecutions(sfnM)
		_, handleErr := lambda.HandleStreamEvent(ctx, d, makeRerunRequestWithReasonE2E("pipe-ttl1", "manual"))
		require.NoError(t, handleErr)
		assert.Equal(t, sfnBefore, countSFNExecutions(sfnM), "no SFN when trigger lock row was deleted by TTL")

		// Should have published an INFRA_FAILURE event.
		eb.mu.Lock()
		defer eb.mu.Unlock()
		require.NotEmpty(t, eb.events, "expected INFRA_FAILURE event when trigger lock is missing")
		found := false
		for _, ev := range eb.events {
			for _, entry := range ev.Entries {
				if entry.DetailType != nil && *entry.DetailType == string(types.EventInfraFailure) {
					found = true
				}
			}
		}
		assert.True(t, found, "INFRA_FAILURE event should be published when lock reset fails")
	})

	// Happy path: trigger lock exists, rerun proceeds atomically via ResetTriggerLock.
	t.Run("TriggerExists_ManualRerunSucceeds", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, sfnM, eb := buildE2EDeps(mock, tr, sc)

		cfg := e2ePipeline("pipe-ttl2")
		cfg.Job.MaxManualReruns = intPtr(2)
		seedConfig(mock, cfg)

		// Seed trigger lock — required for ResetTriggerLock to succeed.
		mock.putRaw(testControlTable, map[string]ddbtypes.AttributeValue{
			"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("pipe-ttl2")},
			"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("stream", "2026-03-07")},
			"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusRunning},
		})

		// Seed a failed job event so circuit breaker allows the rerun.
		oldTS := fmt.Sprintf("%d", time.Now().Add(-1*time.Hour).UnixMilli())
		mock.putRaw("joblog", map[string]ddbtypes.AttributeValue{
			"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("pipe-ttl2")},
			"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("stream", "2026-03-07", oldTS)},
			"event": &ddbtypes.AttributeValueMemberS{Value: types.JobEventFail},
		})

		sfnBefore := countSFNExecutions(sfnM)
		_, handleErr := lambda.HandleStreamEvent(ctx, d, makeRerunRequestWithReasonE2E("pipe-ttl2", "manual"))
		require.NoError(t, handleErr)
		assert.Greater(t, countSFNExecutions(sfnM), sfnBefore, "rerun should start SFN when trigger lock exists")
		assertAlertFormats(t, eb)
	})
}

// =========================================================================
// Gap 8: SLA with hourly pipeline (relative deadline)
// =========================================================================

func TestE2E_SLAHourlyDeadline(t *testing.T) {
	ctx := context.Background()

	t.Run("WatchdogSchedulesPreviousHourSLA", func(t *testing.T) {
		mock := newMockDDB()
		tr := &mockTriggerRunner{}
		sc := &mockStatusPoller{}
		d, _, _ := buildE2EDeps(mock, tr, sc)

		// Fix time at 14:10 UTC.
		fixedNow := time.Date(2026, 3, 7, 14, 10, 0, 0, time.UTC)
		d.NowFunc = func() time.Time { return fixedNow }
		d.StartedAt = fixedNow.Add(-5 * time.Minute)

		cfg := types.PipelineConfig{
			Pipeline: types.PipelineIdentity{ID: "pipe-sla-hr"},
			Schedule: types.ScheduleConfig{
				Cron:       "5 * * * *",
				Evaluation: types.EvaluationWindow{Window: "5m", Interval: "1m"},
			},
			SLA: &types.SLAConfig{
				Deadline:         ":30",
				ExpectedDuration: "15m",
			},
			Validation: types.ValidationConfig{Trigger: "ALL"},
			Job:        types.JobConfig{Type: "glue", Config: map[string]interface{}{"jobName": "test"}},
		}
		seedConfig(mock, cfg)

		require.NoError(t, lambda.HandleWatchdog(ctx, d))

		// Verify SLA calculation produces previous hour (T13) composite date.
		calcOut, err := lambda.HandleSLAMonitor(ctx, d, lambda.SLAMonitorInput{
			Mode:             "calculate",
			PipelineID:       "pipe-sla-hr",
			ScheduleID:       "cron",
			Date:             "2026-03-07T13",
			Deadline:         ":30",
			ExpectedDuration: "15m",
		})
		require.NoError(t, err)

		breachAt, err := time.Parse(time.RFC3339, calcOut.BreachAt)
		require.NoError(t, err)
		// Hour 13 data + :30 deadline → breach at 13:30 + 1 hour = 14:30.
		assert.Equal(t, 30, breachAt.Minute(), "breach minute should be 30")

		warningAt, err := time.Parse(time.RFC3339, calcOut.WarningAt)
		require.NoError(t, err)
		// Warning = breach - expectedDuration (15m) → 14:15.
		assert.Equal(t, 15, warningAt.Minute(), "warning minute should be 15")
	})
}

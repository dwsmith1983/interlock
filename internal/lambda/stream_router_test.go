package lambda_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
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

const testControlTable = "control"

// testDeps builds a *Deps wired to in-memory mocks.
func testDeps(mock *mockDDB) (*lambda.Deps, *mockSFN, *mockEventBridge) {
	s := &store.Store{
		Client:       mock,
		ControlTable: testControlTable,
		JobLogTable:  "joblog",
		RerunTable:   "rerun",
	}
	cache := store.NewConfigCache(s, 5*time.Minute)
	sfnMock := &mockSFN{}
	ebMock := &mockEventBridge{}

	d := &lambda.Deps{
		Store:           s,
		ConfigCache:     cache,
		SFNClient:       sfnMock,
		EventBridge:     ebMock,
		StateMachineARN: "arn:aws:states:us-east-1:123456789012:stateMachine:test",
		EventBusName:    "interlock-bus",
		StartedAt:       time.Now().Add(-24 * time.Hour),
		Logger:          slog.Default(),
	}
	return d, sfnMock, ebMock
}

// seedConfig stores a PipelineConfig in the mock DynamoDB control table.
func seedConfig(mock *mockDDB, cfg types.PipelineConfig) {
	data, _ := json.Marshal(cfg)
	mock.putRaw(testControlTable, map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(cfg.Pipeline.ID)},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.ConfigSK},
		"config": &ddbtypes.AttributeValueMemberS{Value: string(data)},
	})
}

// seedTriggerLock pre-inserts a trigger lock row to simulate a held lock.
func seedTriggerLock(mock *mockDDB, pipelineID, date string) {
	mock.putRaw(testControlTable, map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("stream", date)},
		"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusRunning},
	})
}

// makeSensorRecord builds a DynamoDB stream event record for a sensor write.
func makeSensorRecord(pipelineID, sensorKey string, fields map[string]events.DynamoDBAttributeValue) events.DynamoDBEventRecord {
	newImage := map[string]events.DynamoDBAttributeValue{
		"PK": events.NewStringAttribute(types.PipelinePK(pipelineID)),
		"SK": events.NewStringAttribute(types.SensorSK(sensorKey)),
	}
	for k, v := range fields {
		newImage[k] = v
	}

	return events.DynamoDBEventRecord{
		EventName: "INSERT",
		Change: events.DynamoDBStreamRecord{
			Keys: map[string]events.DynamoDBAttributeValue{
				"PK": events.NewStringAttribute(types.PipelinePK(pipelineID)),
				"SK": events.NewStringAttribute(types.SensorSK(sensorKey)),
			},
			NewImage: newImage,
		},
	}
}

// makeConfigRecord builds a DynamoDB stream event record for a CONFIG change.
func makeConfigRecord(pipelineID string) events.DynamoDBEventRecord {
	return events.DynamoDBEventRecord{
		EventName: "MODIFY",
		Change: events.DynamoDBStreamRecord{
			Keys: map[string]events.DynamoDBAttributeValue{
				"PK": events.NewStringAttribute(types.PipelinePK(pipelineID)),
				"SK": events.NewStringAttribute(types.ConfigSK),
			},
		},
	}
}

// testStreamConfig returns a PipelineConfig with a stream trigger on "upstream-complete".
func testStreamConfig() types.PipelineConfig {
	return types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "gold-revenue"},
		Schedule: types.ScheduleConfig{
			Trigger: &types.TriggerCondition{
				Key:   "upstream-complete",
				Check: types.CheckEquals,
				Field: "status",
				Value: "ready",
			},
			Evaluation: types.EvaluationWindow{
				Window:   "1h",
				Interval: "5m",
			},
		},
		Validation: types.ValidationConfig{
			Trigger: "ALL",
			Rules: []types.ValidationRule{
				{Key: "upstream-complete", Check: types.CheckExists},
			},
		},
		Job: types.JobConfig{
			Type:   "command",
			Config: map[string]interface{}{"command": "echo hello"},
		},
	}
}

func TestStreamRouter_SensorMatch_StartsSFN(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, ebMock := testDeps(mock)

	cfg := testStreamConfig()
	seedConfig(mock, cfg)

	record := makeSensorRecord("gold-revenue", "upstream-complete", map[string]events.DynamoDBAttributeValue{
		"status": events.NewStringAttribute("ready"),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1, "expected exactly one SFN execution")

	// Verify the SFN input payload.
	var input map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(*sfnMock.executions[0].Input), &input))
	assert.Equal(t, "gold-revenue", input["pipelineId"])
	assert.Equal(t, "stream", input["scheduleId"])
	assert.Equal(t, time.Now().Format("2006-01-02"), input["date"])
	// Config block must be present for SFN Wait states.
	cfgMap, ok := input["config"].(map[string]interface{})
	require.True(t, ok, "expected config object in SFN input")
	assert.Greater(t, cfgMap["evaluationIntervalSeconds"], float64(0))
	assert.Greater(t, cfgMap["evaluationWindowSeconds"], float64(0))

	// Verify EventBridge event was published.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	require.Len(t, ebMock.events, 1, "expected one EventBridge event")
}

func TestStreamRouter_SensorPrefixMatch_PerPeriodKey(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testStreamConfig()
	seedConfig(mock, cfg)

	// Per-period sensor key: "upstream-complete#2026-03-03T18" should prefix-match
	// the trigger key "upstream-complete".
	record := makeSensorRecord("gold-revenue", "upstream-complete#2026-03-03T18", map[string]events.DynamoDBAttributeValue{
		"status": events.NewStringAttribute("ready"),
		"date":   events.NewStringAttribute("20260303"),
		"hour":   events.NewStringAttribute("18"),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1, "per-period sensor key should match via prefix")

	var input map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(*sfnMock.executions[0].Input), &input))
	assert.Equal(t, "2026-03-03T18", input["date"], "date should come from sensor data")
}

func TestStreamRouter_SensorNoMatch_NoSFN(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testStreamConfig()
	seedConfig(mock, cfg)

	// Wrong sensor key: "other-sensor" instead of "upstream-complete".
	record := makeSensorRecord("gold-revenue", "other-sensor", map[string]events.DynamoDBAttributeValue{
		"status": events.NewStringAttribute("ready"),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Empty(t, sfnMock.executions, "expected no SFN executions for non-matching sensor")
}

func TestStreamRouter_SensorMatch_LockHeld_NoSFN(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testStreamConfig()
	seedConfig(mock, cfg)

	// Pre-insert a trigger lock.
	date := time.Now().Format("2006-01-02")
	seedTriggerLock(mock, "gold-revenue", date)

	record := makeSensorRecord("gold-revenue", "upstream-complete", map[string]events.DynamoDBAttributeValue{
		"status": events.NewStringAttribute("ready"),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Empty(t, sfnMock.executions, "expected no SFN executions when lock is held")
}

func TestStreamRouter_CalendarExcluded_NoSFN(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	// Create a config that excludes the current date.
	cfg := testStreamConfig()
	today := time.Now().Format("2006-01-02")
	cfg.Schedule.Exclude = &types.ExclusionConfig{
		Dates: []string{today},
	}
	seedConfig(mock, cfg)

	record := makeSensorRecord("gold-revenue", "upstream-complete", map[string]events.DynamoDBAttributeValue{
		"status": events.NewStringAttribute("ready"),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Empty(t, sfnMock.executions, "expected no SFN executions when calendar-excluded")
}

func TestStreamRouter_NoPipelineConfig_NoSFN(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	// No config seeded for this pipeline.
	record := makeSensorRecord("unknown-pipeline", "upstream-complete", map[string]events.DynamoDBAttributeValue{
		"status": events.NewStringAttribute("ready"),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Empty(t, sfnMock.executions, "expected no SFN executions for missing config")
}

func TestStreamRouter_ConfigChange_InvalidatesCache(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	// Seed initial config (no trigger, so sensor events won't fire SFN).
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "gold-revenue"},
		Schedule: types.ScheduleConfig{
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo hello"}},
	}
	seedConfig(mock, cfg)

	// Warm up the cache by sending a sensor event (won't trigger since no trigger condition).
	sensorRecord := makeSensorRecord("gold-revenue", "upstream-complete", map[string]events.DynamoDBAttributeValue{
		"status": events.NewStringAttribute("ready"),
	})
	err := lambda.HandleStreamEvent(context.Background(), d, lambda.StreamEvent{
		Records: []events.DynamoDBEventRecord{sensorRecord},
	})
	require.NoError(t, err)

	sfnMock.mu.Lock()
	assert.Empty(t, sfnMock.executions, "no SFN before config update")
	sfnMock.mu.Unlock()

	// Now update config to add a trigger condition.
	cfgUpdated := testStreamConfig()
	seedConfig(mock, cfgUpdated)

	// Send a CONFIG change event to invalidate the cache.
	configRecord := makeConfigRecord("gold-revenue")
	err = lambda.HandleStreamEvent(context.Background(), d, lambda.StreamEvent{
		Records: []events.DynamoDBEventRecord{configRecord},
	})
	require.NoError(t, err)

	// Now send the sensor event again — should trigger SFN with the updated config.
	err = lambda.HandleStreamEvent(context.Background(), d, lambda.StreamEvent{
		Records: []events.DynamoDBEventRecord{sensorRecord},
	})
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1, "SFN should have been triggered after cache invalidation")
}

// ---------------------------------------------------------------------------
// Job log event helpers and tests
// ---------------------------------------------------------------------------

// makeJobRecord builds a DynamoDB stream event record for a JOB# write.
func makeJobRecord(pipelineID, jobEvent string) events.DynamoDBEventRecord {
	const date = "2026-03-01"
	const timestamp = "1709312400"
	pk := types.PipelinePK(pipelineID)
	sk := types.JobSK("stream", date, timestamp)

	return events.DynamoDBEventRecord{
		EventName: "INSERT",
		Change: events.DynamoDBStreamRecord{
			Keys: map[string]events.DynamoDBAttributeValue{
				"PK": events.NewStringAttribute(pk),
				"SK": events.NewStringAttribute(sk),
			},
			NewImage: map[string]events.DynamoDBAttributeValue{
				"PK":    events.NewStringAttribute(pk),
				"SK":    events.NewStringAttribute(sk),
				"event": events.NewStringAttribute(jobEvent),
			},
		},
	}
}

// seedRerun inserts an existing RERUN# row into the mock rerun table
// with reason "job-fail-retry" (the source used by handleJobFailure).
func seedRerun(mock *mockDDB, attempt int) {
	mock.putRaw("rerun", map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("gold-revenue")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.RerunSK("stream", "2026-03-01", attempt)},
		"reason": &ddbtypes.AttributeValueMemberS{Value: "job-fail-retry"},
	})
}

// testJobConfig returns a PipelineConfig with maxRetries set.
func testJobConfig() types.PipelineConfig {
	return types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "gold-revenue"},
		Schedule: types.ScheduleConfig{
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: types.ValidationConfig{
			Trigger: "ALL",
			Rules:   []types.ValidationRule{{Key: "upstream-complete", Check: types.CheckExists}},
		},
		Job: types.JobConfig{
			Type:       "command",
			Config:     map[string]interface{}{"command": "echo hello"},
			MaxRetries: 2,
		},
	}
}

func TestStreamRouter_JobFail_UnderRetryLimit_Reruns(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, ebMock := testDeps(mock)

	cfg := testJobConfig()
	seedConfig(mock, cfg)
	// Seed trigger lock — required for ResetTriggerLock to succeed.
	seedTriggerLock(mock, "gold-revenue", "2026-03-01")

	// No existing reruns — first failure should trigger a rerun.
	record := makeJobRecord("gold-revenue", types.JobEventFail)
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// Should have started a new SFN execution for the rerun.
	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1, "expected one SFN execution for rerun")

	// Verify the execution name includes the rerun attempt.
	assert.Contains(t, *sfnMock.executions[0].Name, "rerun-0")

	// Verify no EventBridge events for retry exhaustion.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	assert.Empty(t, ebMock.events, "expected no EventBridge events on successful rerun")
}

func TestStreamRouter_JobFail_OverRetryLimit_Alerts(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, ebMock := testDeps(mock)

	cfg := testJobConfig()
	seedConfig(mock, cfg)

	// Seed 2 existing reruns — at limit, so next failure should be final.
	seedRerun(mock, 0)
	seedRerun(mock, 1)

	record := makeJobRecord("gold-revenue", types.JobEventFail)
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// No SFN execution should be started.
	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Empty(t, sfnMock.executions, "expected no SFN execution when retries exhausted")

	// Should have published a RETRY_EXHAUSTED event.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	require.Len(t, ebMock.events, 1, "expected one EventBridge event")
	assert.Equal(t, string(types.EventRetryExhausted), *ebMock.events[0].Entries[0].DetailType)
}

func TestStreamRouter_JobSuccess_PublishesEvent(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, ebMock := testDeps(mock)

	cfg := testJobConfig()
	seedConfig(mock, cfg)

	record := makeJobRecord("gold-revenue", types.JobEventSuccess)
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// No SFN execution for success.
	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Empty(t, sfnMock.executions, "expected no SFN execution on success")

	// Should have published a JOB_COMPLETED event.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	require.Len(t, ebMock.events, 1, "expected one EventBridge event")
	assert.Equal(t, string(types.EventJobCompleted), *ebMock.events[0].Entries[0].DetailType)
}

func TestStreamRouter_JobTimeout_TreatedAsFailure(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testJobConfig()
	seedConfig(mock, cfg)
	seedTriggerLock(mock, "gold-revenue", "2026-03-01")

	// Timeout event should be treated like a failure — triggers rerun.
	record := makeJobRecord("gold-revenue", types.JobEventTimeout)
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1, "expected one SFN execution for timeout rerun")
	assert.Contains(t, *sfnMock.executions[0].Name, "rerun-0")
}

func TestStreamRouter_JobFail_DriftRerunsIgnored(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testJobConfig()
	seedConfig(mock, cfg)
	seedTriggerLock(mock, "gold-revenue", "2026-03-01")

	// Seed a drift rerun (should NOT count toward failure budget).
	mock.putRaw("rerun", map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("gold-revenue")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.RerunSK("stream", "2026-03-01", 0)},
		"reason": &ddbtypes.AttributeValueMemberS{Value: "data-drift"},
	})
	// Seed a manual rerun (should NOT count toward failure budget).
	mock.putRaw("rerun", map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("gold-revenue")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.RerunSK("stream", "2026-03-01", 1)},
		"reason": &ddbtypes.AttributeValueMemberS{Value: "manual"},
	})

	// Job failure should still trigger a rerun (drift/manual don't consume failure budget).
	record := makeJobRecord("gold-revenue", types.JobEventFail)
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1, "drift/manual reruns should not consume failure budget")
}

func TestStreamRouter_JobFail_NoConfig_Skips(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, ebMock := testDeps(mock)

	// No config seeded — should not crash, return nil.
	record := makeJobRecord("unknown-pipeline", types.JobEventFail)
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Empty(t, sfnMock.executions, "expected no SFN for missing config")

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	assert.Empty(t, ebMock.events, "expected no EventBridge events for missing config")
}

func TestStreamRouter_TriggerValueMismatch_NoSFN(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testStreamConfig()
	seedConfig(mock, cfg)

	// Right sensor key but wrong value (trigger expects status=ready).
	record := makeSensorRecord("gold-revenue", "upstream-complete", map[string]events.DynamoDBAttributeValue{
		"status": events.NewStringAttribute("not-ready"),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Empty(t, sfnMock.executions, "expected no SFN when trigger value does not match")
}

// ---------------------------------------------------------------------------
// Late data arrival tests
// ---------------------------------------------------------------------------

func TestStreamRouter_LateDataArrival_CompletedSuccess(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, ebMock := testDeps(mock)

	cfg := testStreamConfig()
	seedConfig(mock, cfg)

	date := time.Now().Format("2006-01-02")

	// Seed a COMPLETED trigger (pipeline finished successfully).
	mock.putRaw(testControlTable, map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("gold-revenue")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("stream", date)},
		"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusCompleted},
	})

	// Seed a successful job event in the joblog.
	mock.putRaw("joblog", map[string]ddbtypes.AttributeValue{
		"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("gold-revenue")},
		"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("stream", date, "1709280000000")},
		"event": &ddbtypes.AttributeValueMemberS{Value: types.JobEventSuccess},
	})

	// Send a sensor write — lock is held, pipeline completed with success.
	record := makeSensorRecord("gold-revenue", "upstream-complete", map[string]events.DynamoDBAttributeValue{
		"status": events.NewStringAttribute("ready"),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// No SFN execution (lock held).
	sfnMock.mu.Lock()
	assert.Empty(t, sfnMock.executions, "should not start SFN when lock held")
	sfnMock.mu.Unlock()

	// Should have published LATE_DATA_ARRIVAL event.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	require.Len(t, ebMock.events, 1, "expected one EventBridge event")
	assert.Equal(t, string(types.EventLateDataArrival), *ebMock.events[0].Entries[0].DetailType)
}

func TestStreamRouter_LateDataArrival_WritesRerunRequest(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	cfg := testStreamConfig()
	seedConfig(mock, cfg)

	date := time.Now().Format("2006-01-02")

	// Seed a COMPLETED trigger (pipeline finished successfully).
	mock.putRaw(testControlTable, map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("gold-revenue")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("stream", date)},
		"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusCompleted},
	})

	// Seed a successful job event in the joblog.
	mock.putRaw("joblog", map[string]ddbtypes.AttributeValue{
		"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("gold-revenue")},
		"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("stream", date, "1709280000000")},
		"event": &ddbtypes.AttributeValueMemberS{Value: types.JobEventSuccess},
	})

	// Send a sensor write — lock is held, pipeline completed with success.
	record := makeSensorRecord("gold-revenue", "upstream-complete", map[string]events.DynamoDBAttributeValue{
		"status": events.NewStringAttribute("ready"),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// Should have published LATE_DATA_ARRIVAL event (existing behavior).
	ebMock.mu.Lock()
	require.Len(t, ebMock.events, 1, "expected one EventBridge event")
	assert.Equal(t, string(types.EventLateDataArrival), *ebMock.events[0].Entries[0].DetailType)
	ebMock.mu.Unlock()

	// Should have written a RERUN_REQUEST to the control table.
	pk := types.PipelinePK("gold-revenue")
	sk := types.RerunRequestSK("stream", date)
	key := ddbItemKey(testControlTable, pk, sk)

	mock.mu.Lock()
	item, ok := mock.items[key]
	mock.mu.Unlock()

	require.True(t, ok, "expected RERUN_REQUEST item in control table")
	assert.Equal(t, "late-data", item["reason"].(*ddbtypes.AttributeValueMemberS).Value)
}

func TestStreamRouter_LateDataArrival_StillRunning_Silent(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, ebMock := testDeps(mock)

	cfg := testStreamConfig()
	seedConfig(mock, cfg)

	date := time.Now().Format("2006-01-02")

	// Seed a RUNNING trigger (normal in-flight execution).
	seedTriggerLock(mock, "gold-revenue", date)

	record := makeSensorRecord("gold-revenue", "upstream-complete", map[string]events.DynamoDBAttributeValue{
		"status": events.NewStringAttribute("ready"),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	assert.Empty(t, sfnMock.executions)
	sfnMock.mu.Unlock()

	// No late data event — this is normal operation.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	assert.Empty(t, ebMock.events, "should not publish late data event for RUNNING trigger")
}

func TestStreamRouter_LateDataArrival_CompletedFailed_Silent(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	cfg := testStreamConfig()
	seedConfig(mock, cfg)

	date := time.Now().Format("2006-01-02")

	// Seed a COMPLETED trigger but with a failed job.
	mock.putRaw(testControlTable, map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("gold-revenue")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("stream", date)},
		"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusCompleted},
	})

	// Seed a failed job event.
	mock.putRaw("joblog", map[string]ddbtypes.AttributeValue{
		"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("gold-revenue")},
		"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("stream", date, "1709280000000")},
		"event": &ddbtypes.AttributeValueMemberS{Value: types.JobEventFail},
	})

	record := makeSensorRecord("gold-revenue", "upstream-complete", map[string]events.DynamoDBAttributeValue{
		"status": events.NewStringAttribute("ready"),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// No late data event — pipeline didn't succeed.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	assert.Empty(t, ebMock.events, "should not publish late data event for failed job")
}

// ---------------------------------------------------------------------------
// ResolveExecutionDate tests
// ---------------------------------------------------------------------------

func TestResolveExecutionDate_WithDateAndHour(t *testing.T) {
	data := map[string]interface{}{"date": "20260303", "hour": "10", "complete": true}
	got := lambda.ResolveExecutionDate(data, time.Now())
	if got != "2026-03-03T10" {
		t.Errorf("got %q, want %q", got, "2026-03-03T10")
	}
}

func TestResolveExecutionDate_DashedDate(t *testing.T) {
	data := map[string]interface{}{"date": "2026-03-03", "hour": "10"}
	got := lambda.ResolveExecutionDate(data, time.Now())
	if got != "2026-03-03T10" {
		t.Errorf("got %q, want %q", got, "2026-03-03T10")
	}
}

func TestResolveExecutionDate_DateOnly(t *testing.T) {
	data := map[string]interface{}{"date": "20260303"}
	got := lambda.ResolveExecutionDate(data, time.Now())
	if got != "2026-03-03" {
		t.Errorf("got %q, want %q", got, "2026-03-03")
	}
}

func TestResolveExecutionDate_NoFields(t *testing.T) {
	data := map[string]interface{}{"complete": true}
	now := time.Now()
	got := lambda.ResolveExecutionDate(data, now)
	want := now.Format("2006-01-02")
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestResolveExecutionDate_HourWithLeadingZero(t *testing.T) {
	data := map[string]interface{}{"date": "20260303", "hour": "03"}
	got := lambda.ResolveExecutionDate(data, time.Now())
	if got != "2026-03-03T03" {
		t.Errorf("got %q, want %q", got, "2026-03-03T03")
	}
}

func TestResolveExecutionDate_InvalidDate(t *testing.T) {
	data := map[string]interface{}{"date": "not-a-date"}
	now := time.Now()
	got := lambda.ResolveExecutionDate(data, now)
	// Should fall back to today's date.
	want := now.Format("2006-01-02")
	if got != want {
		t.Errorf("got %q, want today %q", got, want)
	}
}

func TestResolveExecutionDate_InvalidHour(t *testing.T) {
	data := map[string]interface{}{"date": "20260303", "hour": "25"}
	got := lambda.ResolveExecutionDate(data, time.Now())
	// Invalid hour should be ignored; return date only.
	if got != "2026-03-03" {
		t.Errorf("got %q, want %q", got, "2026-03-03")
	}
}

func TestResolveExecutionDate_NonNumericHour(t *testing.T) {
	data := map[string]interface{}{"date": "20260303", "hour": "ab"}
	got := lambda.ResolveExecutionDate(data, time.Now())
	if got != "2026-03-03" {
		t.Errorf("got %q, want %q", got, "2026-03-03")
	}
}

// ---------------------------------------------------------------------------
// Rerun request helpers and tests
// ---------------------------------------------------------------------------

// makeDefaultRerunRequestRecord is a convenience alias used by tests written
// before Phase 3 introduced the parameterized makeRerunRequestRecordFull helper.
// It uses the fixed defaults: pipelineID="gold-revenue", schedule="stream",
// date="2026-03-01", reason="" (no explicit reason).
func makeDefaultRerunRequestRecord() events.DynamoDBEventRecord {
	return makeRerunRequestRecordFull("")
}

// seedJobEvent inserts a job log record into the mock joblog table.
func seedJobEvent(mock *mockDDB, timestamp, event string) {
	mock.putRaw("joblog", map[string]ddbtypes.AttributeValue{
		"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("gold-revenue")},
		"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("stream", "2026-03-01", timestamp)},
		"event": &ddbtypes.AttributeValueMemberS{Value: event},
	})
}

// seedSensor inserts a sensor record with a data map into the mock control table.
func seedSensor(mock *mockDDB, pipelineID, sensorKey string, data map[string]interface{}) {
	item := map[string]ddbtypes.AttributeValue{
		"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
		"SK": &ddbtypes.AttributeValueMemberS{Value: types.SensorSK(sensorKey)},
	}
	if data != nil {
		dataAV := make(map[string]ddbtypes.AttributeValue, len(data))
		for k, v := range data {
			switch val := v.(type) {
			case string:
				dataAV[k] = &ddbtypes.AttributeValueMemberS{Value: val}
			case float64:
				dataAV[k] = &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%g", val)}
			case int64:
				dataAV[k] = &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", val)}
			}
		}
		item["data"] = &ddbtypes.AttributeValueMemberM{Value: dataAV}
	}
	mock.putRaw(testControlTable, item)
}

func TestStreamRouter_RerunRequest_FailedJob_Allowed(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testJobConfig()
	seedConfig(mock, cfg)
	seedTriggerLock(mock, "gold-revenue", "2026-03-01")

	// Seed a failed job event.
	seedJobEvent(mock, "1709280000000", types.JobEventFail)

	record := makeDefaultRerunRequestRecord()
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// Should have started a new SFN execution.
	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1, "expected one SFN execution for manual rerun of failed job")
	assert.Contains(t, *sfnMock.executions[0].Name, "manual-rerun")
}

func TestStreamRouter_RerunRequest_SuccessDataChanged_Allowed(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testJobConfig()
	seedConfig(mock, cfg)
	seedTriggerLock(mock, "gold-revenue", "2026-03-01")

	// Seed a successful job event with timestamp 1000000.
	seedJobEvent(mock, "1000000", types.JobEventSuccess)

	// Seed a sensor with updatedAt AFTER the job timestamp.
	seedSensor(mock, "gold-revenue", "upstream-complete", map[string]interface{}{
		"updatedAt": float64(2000000), // newer than job timestamp
		"status":    "ready",
	})

	record := makeDefaultRerunRequestRecord()
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// Data changed — SFN should start.
	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1, "expected one SFN execution when sensor data changed after success")
	assert.Contains(t, *sfnMock.executions[0].Name, "manual-rerun")
}

func TestStreamRouter_RerunRequest_SuccessDataUnchanged_Rejected(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, ebMock := testDeps(mock)

	cfg := testJobConfig()
	seedConfig(mock, cfg)

	// Seed a successful job event with timestamp 2000000.
	seedJobEvent(mock, "2000000", types.JobEventSuccess)

	// Seed a sensor with updatedAt BEFORE the job timestamp.
	seedSensor(mock, "gold-revenue", "upstream-complete", map[string]interface{}{
		"updatedAt": float64(1000000), // older than job timestamp
		"status":    "ready",
	})

	record := makeDefaultRerunRequestRecord()
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// No SFN execution — data unchanged.
	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Empty(t, sfnMock.executions, "expected no SFN execution when data unchanged after success")

	// Should have published RERUN_REJECTED event.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	require.Len(t, ebMock.events, 1, "expected one EventBridge event for rejection")
	assert.Equal(t, string(types.EventRerunRejected), *ebMock.events[0].Entries[0].DetailType)
}

func TestStreamRouter_RerunRequest_InfraExhausted_Allowed(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testJobConfig()
	seedConfig(mock, cfg)
	seedTriggerLock(mock, "gold-revenue", "2026-03-01")

	// Seed an infra-trigger-exhausted job event.
	seedJobEvent(mock, "1709280000000", types.JobEventInfraTriggerExhausted)

	record := makeDefaultRerunRequestRecord()
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// Should have started a new SFN execution.
	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1, "expected one SFN execution for manual rerun of infra-exhausted job")
	assert.Contains(t, *sfnMock.executions[0].Name, "manual-rerun")
}

// ---------------------------------------------------------------------------
// handleRecord routing: unknown SK prefix
// ---------------------------------------------------------------------------

func TestStreamRouter_UnknownSKPrefix_Silent(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, ebMock := testDeps(mock)

	// Record with an unrecognized SK prefix should be silently ignored.
	record := events.DynamoDBEventRecord{
		EventName: "INSERT",
		Change: events.DynamoDBStreamRecord{
			Keys: map[string]events.DynamoDBAttributeValue{
				"PK": events.NewStringAttribute("PIPELINE#gold-revenue"),
				"SK": events.NewStringAttribute("UNKNOWN#something"),
			},
		},
	}
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Empty(t, sfnMock.executions, "unknown SK prefix should not trigger any SFN")

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	assert.Empty(t, ebMock.events, "unknown SK prefix should not publish any events")
}

func TestStreamRouter_MissingPKOrSK_LogsError(t *testing.T) {
	mock := newMockDDB()
	d, _, _ := testDeps(mock)

	// Record with no keys at all — should log error but HandleStreamEvent returns nil.
	record := events.DynamoDBEventRecord{
		EventName: "INSERT",
		Change:    events.DynamoDBStreamRecord{},
	}
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err, "HandleStreamEvent always returns nil; errors are logged")
}

// ---------------------------------------------------------------------------
// checkLateDataArrival edge cases
// ---------------------------------------------------------------------------

func TestLateData_TriggerNil(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	cfg := testStreamConfig()
	seedConfig(mock, cfg)

	date := time.Now().Format("2006-01-02")

	// No trigger record exists — GetTrigger returns nil.
	// Seed only the lock so AcquireTriggerLock fails (lock held), triggering checkLateDataArrival.
	seedTriggerLock(mock, "gold-revenue", date)

	record := makeSensorRecord("gold-revenue", "upstream-complete", map[string]events.DynamoDBAttributeValue{
		"status": events.NewStringAttribute("ready"),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// No late data event — GetTrigger returned nil (trigger row doesn't match COMPLETED).
	// Actually the seedTriggerLock uses RUNNING status, so it won't match COMPLETED branch.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	assert.Empty(t, ebMock.events, "should not publish late data event when trigger is not COMPLETED")
}

// ---------------------------------------------------------------------------
// handleSensorEvent branches
// ---------------------------------------------------------------------------

func TestSensor_NoTriggerCondition(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	// Config with NO trigger condition.
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "gold-revenue"},
		Schedule: types.ScheduleConfig{
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo hello"}},
	}
	seedConfig(mock, cfg)

	record := makeSensorRecord("gold-revenue", "upstream-complete", map[string]events.DynamoDBAttributeValue{
		"status": events.NewStringAttribute("ready"),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Empty(t, sfnMock.executions, "no SFN when config has no trigger condition")
}

func TestSensor_SensorKeyMismatch(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testStreamConfig() // trigger key is "upstream-complete"
	seedConfig(mock, cfg)

	// Sensor key "other-sensor" does not prefix-match "upstream-complete".
	record := makeSensorRecord("gold-revenue", "other-sensor", map[string]events.DynamoDBAttributeValue{
		"status": events.NewStringAttribute("ready"),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Empty(t, sfnMock.executions, "non-matching sensor key should not start SFN")
}

func TestSensor_StartSFNError(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testStreamConfig()
	seedConfig(mock, cfg)

	// Make SFN client return an error.
	sfnMock.err = fmt.Errorf("SFN throttled")

	record := makeSensorRecord("gold-revenue", "upstream-complete", map[string]events.DynamoDBAttributeValue{
		"status": events.NewStringAttribute("ready"),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	// HandleStreamEvent logs errors but always returns nil.
	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err, "HandleStreamEvent swallows errors")

	// SFN was called but failed.
	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Empty(t, sfnMock.executions, "SFN error means no execution recorded")
}

func TestSensor_StartSFNError_ReleasesLock(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testStreamConfig()
	seedConfig(mock, cfg)

	// Make SFN client return an error.
	sfnMock.err = fmt.Errorf("SFN throttled")

	record := makeSensorRecord("gold-revenue", "upstream-complete", map[string]events.DynamoDBAttributeValue{
		"status": events.NewStringAttribute("ready"),
		"date":   events.NewStringAttribute("2026-03-08"),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err, "HandleStreamEvent swallows errors")

	// The trigger lock must have been released after SFN failure.
	// Schedule ID for stream-triggered pipelines is "stream".
	lockKey := ddbItemKey(testControlTable,
		types.PipelinePK("gold-revenue"),
		types.TriggerSK("stream", "2026-03-08"))
	mock.mu.Lock()
	_, lockExists := mock.items[lockKey]
	mock.mu.Unlock()
	assert.False(t, lockExists, "trigger lock must be released after SFN start failure")
}

func TestSensor_PerHour_DateOnly(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testStreamConfig()
	seedConfig(mock, cfg)

	// Sensor has "date" but no "hour" — should produce daily execution date.
	record := makeSensorRecord("gold-revenue", "upstream-complete", map[string]events.DynamoDBAttributeValue{
		"status": events.NewStringAttribute("ready"),
		"date":   events.NewStringAttribute("2026-03-05"),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1)

	var input map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(*sfnMock.executions[0].Input), &input))
	assert.Equal(t, "2026-03-05", input["date"], "date-only sensor should produce daily date")
}

func TestSensor_PerHour_NoDate(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testStreamConfig()
	seedConfig(mock, cfg)

	// Sensor missing "date" entirely — should fall back to today.
	record := makeSensorRecord("gold-revenue", "upstream-complete", map[string]events.DynamoDBAttributeValue{
		"status": events.NewStringAttribute("ready"),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1)

	var input map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(*sfnMock.executions[0].Input), &input))
	assert.Equal(t, time.Now().Format("2006-01-02"), input["date"], "missing date should fall back to today")
}

// ---------------------------------------------------------------------------
// handleRerunRequest branches
// ---------------------------------------------------------------------------

func TestRerun_NoJobRecord_Allowed(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testJobConfig()
	seedConfig(mock, cfg)
	seedTriggerLock(mock, "gold-revenue", "2026-03-01")

	// No job events seeded — GetLatestJobEvent returns nil → allow (never ran).
	record := makeDefaultRerunRequestRecord()
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1, "no prior job → allow rerun")
	assert.Contains(t, *sfnMock.executions[0].Name, "manual-rerun")
}

func TestRerun_NoConfig_Skips(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	// No config seeded for this pipeline.
	record := makeDefaultRerunRequestRecord() // uses "gold-revenue"
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Empty(t, sfnMock.executions, "no config → skip rerun request")
}

func TestRerun_ParseSKError(t *testing.T) {
	mock := newMockDDB()
	d, _, _ := testDeps(mock)

	// Malformed RERUN_REQUEST SK with too few parts.
	pk := types.PipelinePK("gold-revenue")
	sk := "RERUN_REQUEST#malformed" // missing second # part
	record := events.DynamoDBEventRecord{
		EventName: "INSERT",
		Change: events.DynamoDBStreamRecord{
			Keys: map[string]events.DynamoDBAttributeValue{
				"PK": events.NewStringAttribute(pk),
				"SK": events.NewStringAttribute(sk),
			},
			NewImage: map[string]events.DynamoDBAttributeValue{
				"PK": events.NewStringAttribute(pk),
				"SK": events.NewStringAttribute(sk),
			},
		},
	}
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	// Error is logged, HandleStreamEvent returns nil.
	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)
}

func TestRerun_TimeoutJob_Allowed(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testJobConfig()
	seedConfig(mock, cfg)
	seedTriggerLock(mock, "gold-revenue", "2026-03-01")

	// Seed a timeout job event.
	seedJobEvent(mock, "1709280000000", types.JobEventTimeout)

	record := makeDefaultRerunRequestRecord()
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1, "timeout job → allow rerun")
	assert.Contains(t, *sfnMock.executions[0].Name, "manual-rerun")
}

func TestRerun_UnknownJobEvent_Allowed(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testJobConfig()
	seedConfig(mock, cfg)
	seedTriggerLock(mock, "gold-revenue", "2026-03-01")

	// Seed a job event with an unknown event type.
	seedJobEvent(mock, "1709280000000", "some-future-event")

	record := makeDefaultRerunRequestRecord()
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1, "unknown job event → allow rerun (safe default)")
	assert.Contains(t, *sfnMock.executions[0].Name, "manual-rerun")
}

func TestRerun_StartSFNError(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testJobConfig()
	seedConfig(mock, cfg)

	seedJobEvent(mock, "1709280000000", types.JobEventFail)

	// Make SFN client return an error.
	sfnMock.err = fmt.Errorf("SFN service error")

	record := makeDefaultRerunRequestRecord()
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	// Error is logged, HandleStreamEvent still returns nil.
	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Empty(t, sfnMock.executions, "SFN error → no execution recorded")
}

// ---------------------------------------------------------------------------
// checkSensorFreshness
// ---------------------------------------------------------------------------

func TestSensorFreshness_NoSensors(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testJobConfig()
	seedConfig(mock, cfg)
	seedTriggerLock(mock, "gold-revenue", "2026-03-01")

	// Seed a successful job event — no sensors exist.
	seedJobEvent(mock, "1000000", types.JobEventSuccess)

	record := makeDefaultRerunRequestRecord()
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// No sensors → can't prove unchanged → allow rerun.
	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1, "no sensors → allow rerun")
}

func TestSensorFreshness_NoUpdatedAtField(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testJobConfig()
	seedConfig(mock, cfg)
	seedTriggerLock(mock, "gold-revenue", "2026-03-01")

	// Seed a successful job event.
	seedJobEvent(mock, "1000000", types.JobEventSuccess)

	// Seed sensor WITHOUT updatedAt field.
	seedSensor(mock, "gold-revenue", "upstream-complete", map[string]interface{}{
		"status": "ready",
	})

	record := makeDefaultRerunRequestRecord()
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// No updatedAt → can't prove unchanged → allow rerun.
	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1, "no updatedAt → allow rerun")
}

func TestSensorFreshness_FreshSensor_Float(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testJobConfig()
	seedConfig(mock, cfg)
	seedTriggerLock(mock, "gold-revenue", "2026-03-01")

	// Seed a successful job event with timestamp 1000000.
	seedJobEvent(mock, "1000000", types.JobEventSuccess)

	// Seed a sensor with updatedAt as float64 > jobTimestamp.
	seedSensor(mock, "gold-revenue", "upstream-complete", map[string]interface{}{
		"updatedAt": float64(2000000),
	})

	record := makeDefaultRerunRequestRecord()
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1, "fresh sensor (float) → allow rerun")
}

func TestSensorFreshness_StaleSensor_Float(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, ebMock := testDeps(mock)

	cfg := testJobConfig()
	seedConfig(mock, cfg)

	// Seed a successful job event with timestamp 2000000.
	seedJobEvent(mock, "2000000", types.JobEventSuccess)

	// Seed a sensor with updatedAt as float64 < jobTimestamp.
	seedSensor(mock, "gold-revenue", "upstream-complete", map[string]interface{}{
		"updatedAt": float64(1000000),
	})

	record := makeDefaultRerunRequestRecord()
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Empty(t, sfnMock.executions, "stale sensor → reject rerun")

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	require.Len(t, ebMock.events, 1, "should publish RERUN_REJECTED event")
	assert.Equal(t, string(types.EventRerunRejected), *ebMock.events[0].Entries[0].DetailType)
}

func TestSensorFreshness_FreshSensor_String(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testJobConfig()
	seedConfig(mock, cfg)
	seedTriggerLock(mock, "gold-revenue", "2026-03-01")

	// Seed a successful job event with timestamp 1000000.
	seedJobEvent(mock, "1000000", types.JobEventSuccess)

	// Seed a sensor with updatedAt as string > jobTimestamp.
	seedSensor(mock, "gold-revenue", "upstream-complete", map[string]interface{}{
		"updatedAt": "2000000",
	})

	record := makeDefaultRerunRequestRecord()
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1, "fresh sensor (string) → allow rerun")
}

func TestSensorFreshness_InvalidJobSK(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testJobConfig()
	seedConfig(mock, cfg)
	seedTriggerLock(mock, "gold-revenue", "2026-03-01")

	// Seed a successful job event with a short SK that has < 4 parts.
	// The SK format should be JOB#schedule#date#timestamp, but we create a malformed one.
	mock.putRaw("joblog", map[string]ddbtypes.AttributeValue{
		"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("gold-revenue")},
		"SK":    &ddbtypes.AttributeValueMemberS{Value: "JOB#stream#2026-03-01"},
		"event": &ddbtypes.AttributeValueMemberS{Value: types.JobEventSuccess},
	})

	// Seed a sensor with stale updatedAt.
	seedSensor(mock, "gold-revenue", "upstream-complete", map[string]interface{}{
		"updatedAt": float64(1),
	})

	record := makeDefaultRerunRequestRecord()
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// Invalid job SK → can't parse timestamp → allow to be safe.
	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1, "invalid job SK → allow rerun (safe default)")
}

func TestSensorFreshness_InvalidTimestamp(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testJobConfig()
	seedConfig(mock, cfg)
	seedTriggerLock(mock, "gold-revenue", "2026-03-01")

	// Seed a successful job event with non-numeric timestamp.
	mock.putRaw("joblog", map[string]ddbtypes.AttributeValue{
		"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("gold-revenue")},
		"SK":    &ddbtypes.AttributeValueMemberS{Value: "JOB#stream#2026-03-01#not-a-number"},
		"event": &ddbtypes.AttributeValueMemberS{Value: types.JobEventSuccess},
	})

	// Seed a sensor with stale updatedAt.
	seedSensor(mock, "gold-revenue", "upstream-complete", map[string]interface{}{
		"updatedAt": float64(1),
	})

	record := makeDefaultRerunRequestRecord()
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// Non-numeric timestamp → allow to be safe.
	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1, "non-numeric timestamp → allow rerun (safe default)")
}

// ---------------------------------------------------------------------------
// handleJobLogEvent routing
// ---------------------------------------------------------------------------

func TestJobLog_InfraExhaustedEvent(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, ebMock := testDeps(mock)

	cfg := testJobConfig()
	seedConfig(mock, cfg)

	// The infra-trigger-exhausted event is not one of the known job events
	// in handleJobLogEvent's switch — it falls to default (no action).
	record := makeJobRecord("gold-revenue", types.JobEventInfraTriggerExhausted)
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Empty(t, sfnMock.executions, "infra-trigger-exhausted is not fail/timeout/success → no action")

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	assert.Empty(t, ebMock.events, "infra-trigger-exhausted → no EventBridge event")
}

func TestJobLog_OtherEvent(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, ebMock := testDeps(mock)

	cfg := testJobConfig()
	seedConfig(mock, cfg)

	// "rerun-accepted" is not handled by handleJobLogEvent's switch.
	record := makeJobRecord("gold-revenue", types.JobEventRerunAccepted)
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Empty(t, sfnMock.executions)

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	assert.Empty(t, ebMock.events)
}

func TestJobLog_ParseSKError(t *testing.T) {
	mock := newMockDDB()
	d, _, _ := testDeps(mock)

	// Malformed JOB SK with too few parts.
	pk := types.PipelinePK("gold-revenue")
	sk := "JOB#malformed" // missing date and timestamp parts
	record := events.DynamoDBEventRecord{
		EventName: "INSERT",
		Change: events.DynamoDBStreamRecord{
			Keys: map[string]events.DynamoDBAttributeValue{
				"PK": events.NewStringAttribute(pk),
				"SK": events.NewStringAttribute(sk),
			},
			NewImage: map[string]events.DynamoDBAttributeValue{
				"PK":    events.NewStringAttribute(pk),
				"SK":    events.NewStringAttribute(sk),
				"event": events.NewStringAttribute(types.JobEventFail),
			},
		},
	}
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	// Error is logged, HandleStreamEvent returns nil.
	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)
}

func TestJobLog_MissingEventAttribute(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, ebMock := testDeps(mock)

	cfg := testJobConfig()
	seedConfig(mock, cfg)

	// JOB record without "event" attribute.
	pk := types.PipelinePK("gold-revenue")
	sk := types.JobSK("stream", "2026-03-01", "1709312400")
	record := events.DynamoDBEventRecord{
		EventName: "INSERT",
		Change: events.DynamoDBStreamRecord{
			Keys: map[string]events.DynamoDBAttributeValue{
				"PK": events.NewStringAttribute(pk),
				"SK": events.NewStringAttribute(sk),
			},
			NewImage: map[string]events.DynamoDBAttributeValue{
				"PK": events.NewStringAttribute(pk),
				"SK": events.NewStringAttribute(sk),
				// no "event" key
			},
		},
	}
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// Missing event attribute → logged as warning, no action.
	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Empty(t, sfnMock.executions)

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	assert.Empty(t, ebMock.events)
}

// ---------------------------------------------------------------------------
// handleJobSuccess
// ---------------------------------------------------------------------------

func TestJobSuccess_PublishesJobCompleted(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	cfg := testJobConfig()
	seedConfig(mock, cfg)

	record := makeJobRecord("gold-revenue", types.JobEventSuccess)
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	require.Len(t, ebMock.events, 1)
	assert.Equal(t, string(types.EventJobCompleted), *ebMock.events[0].Entries[0].DetailType)

	// Verify the event detail contains pipeline information.
	var detail types.InterlockEvent
	require.NoError(t, json.Unmarshal([]byte(*ebMock.events[0].Entries[0].Detail), &detail))
	assert.Equal(t, "gold-revenue", detail.PipelineID)
	assert.Equal(t, "stream", detail.ScheduleID)
	assert.Equal(t, "2026-03-01", detail.Date)
}

// ---------------------------------------------------------------------------
// handleJobFailure edge cases
// ---------------------------------------------------------------------------

func TestJobFailure_NoConfig(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, ebMock := testDeps(mock)

	// No config for this pipeline.
	record := makeJobRecord("unknown-pipeline", types.JobEventFail)
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Empty(t, sfnMock.executions)

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	assert.Empty(t, ebMock.events)
}

// ---------------------------------------------------------------------------
// buildSFNConfig (tested indirectly through SFN input payload)
// ---------------------------------------------------------------------------

func TestBuildSFNConfig_NoPostRunFields(t *testing.T) {
	// SFN config should not contain post-run fields (post-run is now stream-driven).
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testStreamConfig()
	cfg.PostRun = &types.PostRunConfig{
		Rules: []types.ValidationRule{
			{Key: "quality-check", Check: types.CheckExists},
		},
	}
	seedConfig(mock, cfg)

	record := makeSensorRecord("gold-revenue", "upstream-complete", map[string]events.DynamoDBAttributeValue{
		"status": events.NewStringAttribute("ready"),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1)

	var input map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(*sfnMock.executions[0].Input), &input))
	cfgMap := input["config"].(map[string]interface{})

	// Post-run fields should NOT be in SFN config.
	_, hasPostRun := cfgMap["hasPostRun"]
	assert.False(t, hasPostRun, "hasPostRun should not be in SFN config")
	_, hasPostInterval := cfgMap["postRunIntervalSeconds"]
	assert.False(t, hasPostInterval, "postRunIntervalSeconds should not be in SFN config")
	_, hasPostWindow := cfgMap["postRunWindowSeconds"]
	assert.False(t, hasPostWindow, "postRunWindowSeconds should not be in SFN config")
}

func TestBuildSFNConfig_CustomTimings(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testStreamConfig()
	cfg.Schedule.Evaluation = types.EvaluationWindow{
		Interval: "2m",
		Window:   "30m",
	}
	seedConfig(mock, cfg)

	record := makeSensorRecord("gold-revenue", "upstream-complete", map[string]events.DynamoDBAttributeValue{
		"status": events.NewStringAttribute("ready"),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1)

	var input map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(*sfnMock.executions[0].Input), &input))
	cfgMap := input["config"].(map[string]interface{})

	assert.Equal(t, float64(120), cfgMap["evaluationIntervalSeconds"]) // 2m = 120s
	assert.Equal(t, float64(1800), cfgMap["evaluationWindowSeconds"])  // 30m = 1800s
}

func TestBuildSFNConfig_WithSLA(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testStreamConfig()
	cfg.SLA = &types.SLAConfig{
		Deadline:         "08:00",
		ExpectedDuration: "30m",
		Critical:         true,
	}
	seedConfig(mock, cfg)

	record := makeSensorRecord("gold-revenue", "upstream-complete", map[string]events.DynamoDBAttributeValue{
		"status": events.NewStringAttribute("ready"),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1)

	var input map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(*sfnMock.executions[0].Input), &input))
	cfgMap := input["config"].(map[string]interface{})

	sla, ok := cfgMap["sla"].(map[string]interface{})
	require.True(t, ok, "SLA config should be present")
	assert.Equal(t, "08:00", sla["deadline"])
	assert.Equal(t, "30m", sla["expectedDuration"])
	assert.Equal(t, true, sla["critical"])
	assert.Equal(t, "UTC", sla["timezone"], "SLA timezone defaults to UTC")
}

func TestBuildSFNConfig_JobPollWindowDefault(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testStreamConfig()
	seedConfig(mock, cfg)

	record := makeSensorRecord("gold-revenue", "upstream-complete", map[string]events.DynamoDBAttributeValue{
		"status": events.NewStringAttribute("ready"),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1)

	var input map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(*sfnMock.executions[0].Input), &input))
	cfgMap := input["config"].(map[string]interface{})
	assert.Equal(t, float64(3600), cfgMap["jobPollWindowSeconds"], "default job poll window should be 1h")
}

func TestBuildSFNConfig_JobPollWindowOverride(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	pollWindow := 7200
	cfg := testStreamConfig()
	cfg.Job.JobPollWindowSeconds = &pollWindow
	seedConfig(mock, cfg)

	record := makeSensorRecord("gold-revenue", "upstream-complete", map[string]events.DynamoDBAttributeValue{
		"status": events.NewStringAttribute("ready"),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1)

	var input map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(*sfnMock.executions[0].Input), &input))
	cfgMap := input["config"].(map[string]interface{})
	assert.Equal(t, float64(7200), cfgMap["jobPollWindowSeconds"], "should use config override")
}

func TestBuildSFNConfig_JobPollWindowZeroUsesDefault(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	zero := 0
	cfg := testStreamConfig()
	cfg.Job.JobPollWindowSeconds = &zero
	seedConfig(mock, cfg)

	record := makeSensorRecord("gold-revenue", "upstream-complete", map[string]events.DynamoDBAttributeValue{
		"status": events.NewStringAttribute("ready"),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1)

	var input map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(*sfnMock.executions[0].Input), &input))
	cfgMap := input["config"].(map[string]interface{})
	assert.Equal(t, float64(3600), cfgMap["jobPollWindowSeconds"], "zero should fall back to default")
}

// ---------------------------------------------------------------------------
// extractSensorData (tested indirectly through sensor trigger evaluation)
// ---------------------------------------------------------------------------

func TestExtractSensorData_DataMapUnwrap(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	// Configure trigger to check for "status" = "ready" on "upstream-complete".
	cfg := testStreamConfig()
	seedConfig(mock, cfg)

	// Sensor record with a nested "data" map — extractSensorData should unwrap it.
	record := events.DynamoDBEventRecord{
		EventName: "INSERT",
		Change: events.DynamoDBStreamRecord{
			Keys: map[string]events.DynamoDBAttributeValue{
				"PK": events.NewStringAttribute(types.PipelinePK("gold-revenue")),
				"SK": events.NewStringAttribute(types.SensorSK("upstream-complete")),
			},
			NewImage: map[string]events.DynamoDBAttributeValue{
				"PK": events.NewStringAttribute(types.PipelinePK("gold-revenue")),
				"SK": events.NewStringAttribute(types.SensorSK("upstream-complete")),
				"data": events.NewMapAttribute(map[string]events.DynamoDBAttributeValue{
					"status": events.NewStringAttribute("ready"),
				}),
			},
		},
	}
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// Trigger should fire because "data" map was unwrapped, exposing "status" = "ready".
	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1, "data map unwrap should expose fields for trigger evaluation")
}

func TestExtractSensorData_NoDataMap(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testStreamConfig()
	seedConfig(mock, cfg)

	// Sensor record without a "data" map — fields should be used directly.
	record := makeSensorRecord("gold-revenue", "upstream-complete", map[string]events.DynamoDBAttributeValue{
		"status": events.NewStringAttribute("ready"),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1, "flat sensor data (no data map) should work for trigger evaluation")
}

func TestExtractSensorData_SkipsPKSKTTL(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	// Configure trigger to check for "status" = "ready".
	cfg := testStreamConfig()
	seedConfig(mock, cfg)

	// Create a sensor with PK, SK, ttl, and actual data fields.
	record := events.DynamoDBEventRecord{
		EventName: "INSERT",
		Change: events.DynamoDBStreamRecord{
			Keys: map[string]events.DynamoDBAttributeValue{
				"PK": events.NewStringAttribute(types.PipelinePK("gold-revenue")),
				"SK": events.NewStringAttribute(types.SensorSK("upstream-complete")),
			},
			NewImage: map[string]events.DynamoDBAttributeValue{
				"PK":     events.NewStringAttribute(types.PipelinePK("gold-revenue")),
				"SK":     events.NewStringAttribute(types.SensorSK("upstream-complete")),
				"ttl":    events.NewNumberAttribute("1709280000"),
				"status": events.NewStringAttribute("ready"),
			},
		},
	}
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// PK, SK, ttl should be stripped; "status" remains for trigger evaluation.
	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1, "PK/SK/ttl should be stripped but data fields preserved")
}

// ---------------------------------------------------------------------------
// convertAttributeValue (tested indirectly through sensor data extraction)
// ---------------------------------------------------------------------------

func TestConvertAV_String(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testStreamConfig() // trigger checks status equals "ready"
	seedConfig(mock, cfg)

	record := makeSensorRecord("gold-revenue", "upstream-complete", map[string]events.DynamoDBAttributeValue{
		"status": events.NewStringAttribute("ready"),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1, "string attribute should be correctly converted")
}

func TestConvertAV_Number(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	// Trigger checks count >= 5.
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "gold-revenue"},
		Schedule: types.ScheduleConfig{
			Trigger: &types.TriggerCondition{
				Key:   "upstream-complete",
				Check: types.CheckGTE,
				Field: "count",
				Value: float64(5),
			},
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: types.ValidationConfig{Trigger: "ALL", Rules: []types.ValidationRule{
			{Key: "upstream-complete", Check: types.CheckExists},
		}},
		Job: types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo hello"}},
	}
	seedConfig(mock, cfg)

	record := makeSensorRecord("gold-revenue", "upstream-complete", map[string]events.DynamoDBAttributeValue{
		"count": events.NewNumberAttribute("10"),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1, "number attribute should be correctly converted to float64")
}

func TestConvertAV_Bool(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	// Trigger checks "complete" equals true.
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "gold-revenue"},
		Schedule: types.ScheduleConfig{
			Trigger: &types.TriggerCondition{
				Key:   "upstream-complete",
				Check: types.CheckEquals,
				Field: "complete",
				Value: true,
			},
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: types.ValidationConfig{Trigger: "ALL", Rules: []types.ValidationRule{
			{Key: "upstream-complete", Check: types.CheckExists},
		}},
		Job: types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo hello"}},
	}
	seedConfig(mock, cfg)

	record := makeSensorRecord("gold-revenue", "upstream-complete", map[string]events.DynamoDBAttributeValue{
		"complete": events.NewBooleanAttribute(true),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1, "boolean attribute should be correctly converted")
}

func TestConvertAV_Map(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testStreamConfig()
	seedConfig(mock, cfg)

	// A sensor whose NewImage has a map containing "status" = "ready" inside "data".
	record := events.DynamoDBEventRecord{
		EventName: "INSERT",
		Change: events.DynamoDBStreamRecord{
			Keys: map[string]events.DynamoDBAttributeValue{
				"PK": events.NewStringAttribute(types.PipelinePK("gold-revenue")),
				"SK": events.NewStringAttribute(types.SensorSK("upstream-complete")),
			},
			NewImage: map[string]events.DynamoDBAttributeValue{
				"PK": events.NewStringAttribute(types.PipelinePK("gold-revenue")),
				"SK": events.NewStringAttribute(types.SensorSK("upstream-complete")),
				"data": events.NewMapAttribute(map[string]events.DynamoDBAttributeValue{
					"status": events.NewStringAttribute("ready"),
					"nested": events.NewMapAttribute(map[string]events.DynamoDBAttributeValue{
						"inner": events.NewStringAttribute("value"),
					}),
				}),
			},
		},
	}
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// The data map gets unwrapped; "status" should be accessible at top level.
	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1, "map attribute should be correctly converted and unwrapped")
}

func TestConvertAV_List(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	// Use exists check so we don't need list comparison.
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "gold-revenue"},
		Schedule: types.ScheduleConfig{
			Trigger: &types.TriggerCondition{
				Key:   "upstream-complete",
				Check: types.CheckExists,
			},
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: types.ValidationConfig{Trigger: "ALL", Rules: []types.ValidationRule{
			{Key: "upstream-complete", Check: types.CheckExists},
		}},
		Job: types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo hello"}},
	}
	seedConfig(mock, cfg)

	record := makeSensorRecord("gold-revenue", "upstream-complete", map[string]events.DynamoDBAttributeValue{
		"items": events.NewListAttribute([]events.DynamoDBAttributeValue{
			events.NewStringAttribute("a"),
			events.NewStringAttribute("b"),
		}),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1, "list attribute should be correctly converted")
}

func TestConvertAV_Null(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	// Use exists check so we can trigger even with a null attribute.
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "gold-revenue"},
		Schedule: types.ScheduleConfig{
			Trigger: &types.TriggerCondition{
				Key:   "upstream-complete",
				Check: types.CheckExists,
			},
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: types.ValidationConfig{Trigger: "ALL", Rules: []types.ValidationRule{
			{Key: "upstream-complete", Check: types.CheckExists},
		}},
		Job: types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo hello"}},
	}
	seedConfig(mock, cfg)

	record := makeSensorRecord("gold-revenue", "upstream-complete", map[string]events.DynamoDBAttributeValue{
		"someField": events.NewNullAttribute(),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1, "null attribute should be handled without error")
}

// ---------------------------------------------------------------------------
// normalizeDate (tested indirectly through ResolveExecutionDate)
// ---------------------------------------------------------------------------

func TestNormalizeDate_AlreadyNormalized(t *testing.T) {
	data := map[string]interface{}{"date": "2026-03-03"}
	got := lambda.ResolveExecutionDate(data, time.Now())
	assert.Equal(t, "2026-03-03", got)
}

func TestNormalizeDate_Compact(t *testing.T) {
	data := map[string]interface{}{"date": "20260303"}
	got := lambda.ResolveExecutionDate(data, time.Now())
	assert.Equal(t, "2026-03-03", got)
}

func TestNormalizeDate_CompactWithHour(t *testing.T) {
	data := map[string]interface{}{"date": "20260303", "hour": "07"}
	got := lambda.ResolveExecutionDate(data, time.Now())
	assert.Equal(t, "2026-03-03T07", got)
}

// ---------------------------------------------------------------------------
// resolveScheduleID (tested indirectly through SFN input payload)
// ---------------------------------------------------------------------------

func TestResolveScheduleID_StreamTriggered(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testStreamConfig() // has trigger, no cron → scheduleID should be "stream"
	seedConfig(mock, cfg)

	record := makeSensorRecord("gold-revenue", "upstream-complete", map[string]events.DynamoDBAttributeValue{
		"status": events.NewStringAttribute("ready"),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1)

	var input map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(*sfnMock.executions[0].Input), &input))
	assert.Equal(t, "stream", input["scheduleId"])
}

func TestResolveScheduleID_CronTriggered(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testStreamConfig()
	cfg.Schedule.Cron = "0 8 * * *" // add cron → scheduleID should be "cron"
	seedConfig(mock, cfg)

	record := makeSensorRecord("gold-revenue", "upstream-complete", map[string]events.DynamoDBAttributeValue{
		"status": events.NewStringAttribute("ready"),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1)

	var input map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(*sfnMock.executions[0].Input), &input))
	assert.Equal(t, "cron", input["scheduleId"])
}

// ---------------------------------------------------------------------------
// publishEvent
// ---------------------------------------------------------------------------

func TestPublishEvent_EventBridgeError(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	cfg := testJobConfig()
	seedConfig(mock, cfg)

	// Make EventBridge return an error.
	ebMock.err = fmt.Errorf("EventBridge throttled")

	// JobSuccess publishes an event — if EventBridge fails, handleJobSuccess returns error,
	// but HandleStreamEvent logs it and returns nil.
	record := makeJobRecord("gold-revenue", types.JobEventSuccess)
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err, "HandleStreamEvent swallows errors")
}

func TestPublishEvent_NilEventBridge(t *testing.T) {
	mock := newMockDDB()
	d, _, _ := testDeps(mock)

	cfg := testJobConfig()
	seedConfig(mock, cfg)

	// Set EventBridge to nil — publishEvent should be a no-op.
	d.EventBridge = nil

	record := makeJobRecord("gold-revenue", types.JobEventSuccess)
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)
}

func TestPublishEvent_EmptyEventBusName(t *testing.T) {
	mock := newMockDDB()
	d, _, _ := testDeps(mock)

	cfg := testJobConfig()
	seedConfig(mock, cfg)

	// Set EventBusName to empty — publishEvent should be a no-op.
	d.EventBusName = ""

	record := makeJobRecord("gold-revenue", types.JobEventSuccess)
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// isExcluded edge cases
// ---------------------------------------------------------------------------

func TestIsExcluded_WeekendExclusion(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testStreamConfig()
	cfg.Schedule.Exclude = &types.ExclusionConfig{
		Weekends: true,
	}
	seedConfig(mock, cfg)

	record := makeSensorRecord("gold-revenue", "upstream-complete", map[string]events.DynamoDBAttributeValue{
		"status": events.NewStringAttribute("ready"),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()

	// Depending on the day of the week, this may or may not trigger.
	now := time.Now()
	if now.Weekday() == time.Saturday || now.Weekday() == time.Sunday {
		assert.Empty(t, sfnMock.executions, "should be excluded on weekends")
	} else {
		require.Len(t, sfnMock.executions, 1, "should not be excluded on weekdays")
	}
}

// ---------------------------------------------------------------------------
// Multiple records in a single event
// ---------------------------------------------------------------------------

func TestStreamRouter_MultipleRecords(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, ebMock := testDeps(mock)

	cfg := testStreamConfig()
	seedConfig(mock, cfg)

	cfg2 := testJobConfig()
	cfg2.Pipeline.ID = "silver-orders"
	seedConfig(mock, cfg2)

	// Two sensor records in one event batch.
	record1 := makeSensorRecord("gold-revenue", "upstream-complete", map[string]events.DynamoDBAttributeValue{
		"status": events.NewStringAttribute("ready"),
	})
	// Unknown sensor for silver-orders — no trigger, no SFN.
	record2 := makeSensorRecord("silver-orders", "upstream-complete", map[string]events.DynamoDBAttributeValue{
		"status": events.NewStringAttribute("ready"),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record1, record2}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	// gold-revenue has trigger → 1 SFN; silver-orders has no trigger → 0 SFN.
	require.Len(t, sfnMock.executions, 1, "only gold-revenue should trigger SFN")

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	require.Len(t, ebMock.events, 1, "only gold-revenue should publish event")
}

// ---------------------------------------------------------------------------
// handleJobLogEvent: unexpected PK format
// ---------------------------------------------------------------------------

func TestJobLog_UnexpectedPKFormat(t *testing.T) {
	mock := newMockDDB()
	d, _, _ := testDeps(mock)

	// PK without "PIPELINE#" prefix.
	record := events.DynamoDBEventRecord{
		EventName: "INSERT",
		Change: events.DynamoDBStreamRecord{
			Keys: map[string]events.DynamoDBAttributeValue{
				"PK": events.NewStringAttribute("BADFORMAT#gold-revenue"),
				"SK": events.NewStringAttribute(types.JobSK("stream", "2026-03-01", "1709312400")),
			},
			NewImage: map[string]events.DynamoDBAttributeValue{
				"PK":    events.NewStringAttribute("BADFORMAT#gold-revenue"),
				"SK":    events.NewStringAttribute(types.JobSK("stream", "2026-03-01", "1709312400")),
				"event": events.NewStringAttribute(types.JobEventFail),
			},
		},
	}
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	// Error is logged, HandleStreamEvent returns nil.
	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// handleRerunRequest: unexpected PK format
// ---------------------------------------------------------------------------

func TestRerun_UnexpectedPKFormat(t *testing.T) {
	mock := newMockDDB()
	d, _, _ := testDeps(mock)

	// PK without "PIPELINE#" prefix.
	sk := types.RerunRequestSK("stream", "2026-03-01")
	record := events.DynamoDBEventRecord{
		EventName: "INSERT",
		Change: events.DynamoDBStreamRecord{
			Keys: map[string]events.DynamoDBAttributeValue{
				"PK": events.NewStringAttribute("BADFORMAT#gold-revenue"),
				"SK": events.NewStringAttribute(sk),
			},
			NewImage: map[string]events.DynamoDBAttributeValue{
				"PK": events.NewStringAttribute("BADFORMAT#gold-revenue"),
				"SK": events.NewStringAttribute(sk),
			},
		},
	}
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// handleSensorEvent: unexpected PK format
// ---------------------------------------------------------------------------

func TestSensor_UnexpectedPKFormat(t *testing.T) {
	mock := newMockDDB()
	d, _, _ := testDeps(mock)

	record := events.DynamoDBEventRecord{
		EventName: "INSERT",
		Change: events.DynamoDBStreamRecord{
			Keys: map[string]events.DynamoDBAttributeValue{
				"PK": events.NewStringAttribute("BADFORMAT#gold-revenue"),
				"SK": events.NewStringAttribute(types.SensorSK("upstream-complete")),
			},
		},
	}
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Rerun limit tests
// ---------------------------------------------------------------------------

// makeRerunRequestWithReason builds a DynamoDB stream event record for a
// RERUN_REQUEST# write with a specific reason attribute in NewImage.
func makeRerunRequestWithReason(reason string) events.DynamoDBEventRecord {
	pk := types.PipelinePK("gold-revenue")
	sk := types.RerunRequestSK("stream", "2026-03-01")
	img := map[string]events.DynamoDBAttributeValue{
		"PK": events.NewStringAttribute(pk),
		"SK": events.NewStringAttribute(sk),
	}
	if reason != "" {
		img["reason"] = events.NewStringAttribute(reason)
	}
	return events.DynamoDBEventRecord{
		EventName: "INSERT",
		Change: events.DynamoDBStreamRecord{
			Keys: map[string]events.DynamoDBAttributeValue{
				"PK": events.NewStringAttribute(pk),
				"SK": events.NewStringAttribute(sk),
			},
			NewImage: img,
		},
	}
}

// seedRerunWithReason inserts a RERUN# row with a specific reason into the
// mock rerun table. Uses schedule "stream" and attempt 0 (test defaults).
func seedRerunWithReason(mock *mockDDB, pipelineID, date, reason string) {
	mock.putRaw("rerun", map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.RerunSK("stream", date, 0)},
		"reason": &ddbtypes.AttributeValueMemberS{Value: reason},
	})
}

// testRerunLimitConfig returns a PipelineConfig with rerun limits set.
func testRerunLimitConfig(maxDrift, maxManual int) types.PipelineConfig {
	cfg := testJobConfig()
	cfg.Job.MaxDriftReruns = &maxDrift
	cfg.Job.MaxManualReruns = &maxManual
	return cfg
}

func TestRerun_DriftLimitExceeded(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, ebMock := testDeps(mock)

	cfg := testRerunLimitConfig(1, 3)
	seedConfig(mock, cfg)

	// Seed 1 existing drift rerun — at limit.
	seedRerunWithReason(mock, "gold-revenue", "2026-03-01", "data-drift")

	record := makeRerunRequestWithReason("data-drift")
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// No SFN — drift limit exceeded.
	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Empty(t, sfnMock.executions, "expected no SFN execution when drift limit exceeded")

	// Should have published RERUN_REJECTED event.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	require.Len(t, ebMock.events, 1, "expected one EventBridge event for drift limit rejection")
	assert.Equal(t, string(types.EventRerunRejected), *ebMock.events[0].Entries[0].DetailType)
}

func TestRerun_ManualLimitExceeded(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, ebMock := testDeps(mock)

	cfg := testRerunLimitConfig(3, 1)
	seedConfig(mock, cfg)

	// Seed 1 existing manual rerun — at limit.
	seedRerunWithReason(mock, "gold-revenue", "2026-03-01", "manual")

	record := makeRerunRequestWithReason("manual")
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// No SFN — manual limit exceeded.
	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Empty(t, sfnMock.executions, "expected no SFN execution when manual limit exceeded")

	// Should have published RERUN_REJECTED event.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	require.Len(t, ebMock.events, 1, "expected one EventBridge event for manual limit rejection")
	assert.Equal(t, string(types.EventRerunRejected), *ebMock.events[0].Entries[0].DetailType)
}

func TestRerun_DriftUnderLimit(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testRerunLimitConfig(2, 1)
	seedConfig(mock, cfg)
	seedTriggerLock(mock, "gold-revenue", "2026-03-01")

	// No existing drift reruns — under limit.
	// Seed a failed job so circuit breaker allows the rerun.
	seedJobEvent(mock, "1709280000000", types.JobEventFail)

	record := makeRerunRequestWithReason("data-drift")
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// SFN should have started — under drift limit.
	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1, "expected one SFN execution when under drift limit")
	assert.Contains(t, *sfnMock.executions[0].Name, "data-drift-rerun")
}

func TestRerun_LateDataCountsAsDrift(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, ebMock := testDeps(mock)

	cfg := testRerunLimitConfig(1, 3)
	seedConfig(mock, cfg)

	// Seed 1 existing late-data rerun — shares drift budget.
	seedRerunWithReason(mock, "gold-revenue", "2026-03-01", "late-data")

	// Submit a data-drift request — should be rejected because late-data
	// and data-drift share the same budget.
	record := makeRerunRequestWithReason("data-drift")
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Empty(t, sfnMock.executions, "expected no SFN when shared drift budget exceeded")

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	require.Len(t, ebMock.events, 1, "expected RERUN_REJECTED event")
	assert.Equal(t, string(types.EventRerunRejected), *ebMock.events[0].Entries[0].DetailType)
}

func TestRerun_WritesRerunBeforeLockRelease(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testRerunLimitConfig(3, 3)
	seedConfig(mock, cfg)
	seedTriggerLock(mock, "gold-revenue", "2026-03-01")

	// Seed a failed job so circuit breaker allows the rerun.
	seedJobEvent(mock, "1709280000000", types.JobEventFail)

	record := makeRerunRequestWithReason("manual")
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// SFN should have started.
	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1, "expected SFN execution")

	// Verify the rerun record was written to the rerun table.
	pk := types.PipelinePK("gold-revenue")
	rerunSK := types.RerunSK("stream", "2026-03-01", 0)
	key := ddbItemKey("rerun", pk, rerunSK)

	mock.mu.Lock()
	item, ok := mock.items[key]
	mock.mu.Unlock()

	require.True(t, ok, "expected rerun record in rerun table")
	assert.Equal(t, "manual", item["reason"].(*ddbtypes.AttributeValueMemberS).Value)
}

func TestRerun_DeletesPostrunBaseline(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testRerunLimitConfig(3, 3)
	cfg.PostRun = &types.PostRunConfig{
		Rules: []types.ValidationRule{
			{Key: "quality-check", Check: types.CheckExists},
		},
	}
	seedConfig(mock, cfg)
	seedTriggerLock(mock, "gold-revenue", "2026-03-01")

	// Seed a failed job so circuit breaker allows the rerun.
	seedJobEvent(mock, "1709280000000", types.JobEventFail)

	// Seed a date-scoped postrun-baseline sensor that should be deleted on rerun acceptance.
	seedSensor(mock, "gold-revenue", "postrun-baseline#2026-03-01", map[string]interface{}{
		"status": "captured",
	})

	// Verify the sensor exists before the rerun.
	sensorKey := ddbItemKey(testControlTable, types.PipelinePK("gold-revenue"), types.SensorSK("postrun-baseline#2026-03-01"))
	mock.mu.Lock()
	_, sensorExists := mock.items[sensorKey]
	mock.mu.Unlock()
	require.True(t, sensorExists, "postrun-baseline sensor should exist before rerun")

	record := makeRerunRequestWithReason("manual")
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// SFN should have started.
	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1, "expected SFN execution")

	// Verify the postrun-baseline sensor was deleted.
	mock.mu.Lock()
	_, sensorExists = mock.items[sensorKey]
	mock.mu.Unlock()
	assert.False(t, sensorExists, "postrun-baseline sensor should be deleted after rerun acceptance")
}

func TestStreamRouter_JobFail_PermanentUsesCodeRetries(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, ebMock := testDeps(mock)

	cfg := testJobConfig()
	cfg.Job.MaxRetries = 3
	cfg.Job.MaxCodeRetries = intPtr(0) // disabled — immediate FAILED_FINAL
	seedConfig(mock, cfg)

	// Seed a joblog entry with PERMANENT category.
	mock.putRaw("joblog", map[string]ddbtypes.AttributeValue{
		"PK":       &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("gold-revenue")},
		"SK":       &ddbtypes.AttributeValueMemberS{Value: types.JobSK("stream", "2026-03-01", fmt.Sprintf("%d", time.Now().UnixMilli()))},
		"event":    &ddbtypes.AttributeValueMemberS{Value: types.JobEventFail},
		"category": &ddbtypes.AttributeValueMemberS{Value: "PERMANENT"},
	})

	record := makeJobRecord("gold-revenue", types.JobEventFail)
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// MaxCodeRetries=0 → immediate FAILED_FINAL, no SFN started
	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Empty(t, sfnMock.executions, "PERMANENT with MaxCodeRetries=0 should not retry")

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	assert.True(t, len(ebMock.events) > 0, "should publish RETRY_EXHAUSTED")
}

func TestStreamRouter_JobFail_TransientUsesMaxRetries(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testJobConfig()
	cfg.Job.MaxRetries = 3
	cfg.Job.MaxCodeRetries = intPtr(0) // would block if category was checked wrong
	seedConfig(mock, cfg)
	seedTriggerLock(mock, "gold-revenue", "2026-03-01")

	// Seed a joblog entry with TRANSIENT category.
	mock.putRaw("joblog", map[string]ddbtypes.AttributeValue{
		"PK":       &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("gold-revenue")},
		"SK":       &ddbtypes.AttributeValueMemberS{Value: types.JobSK("stream", "2026-03-01", fmt.Sprintf("%d", time.Now().UnixMilli()))},
		"event":    &ddbtypes.AttributeValueMemberS{Value: types.JobEventFail},
		"category": &ddbtypes.AttributeValueMemberS{Value: "TRANSIENT"},
	})

	record := makeJobRecord("gold-revenue", types.JobEventFail)
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// TRANSIENT uses MaxRetries=3, no reruns yet → should retry
	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Len(t, sfnMock.executions, 1, "TRANSIENT should use MaxRetries and retry")
}

func TestStreamRouter_JobFail_EmptyCategoryUsesMaxRetries(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testJobConfig()
	cfg.Job.MaxRetries = 3
	cfg.Job.MaxCodeRetries = intPtr(0)
	seedConfig(mock, cfg)
	seedTriggerLock(mock, "gold-revenue", "2026-03-01")

	// Seed a joblog entry WITHOUT category (backward compat).
	mock.putRaw("joblog", map[string]ddbtypes.AttributeValue{
		"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("gold-revenue")},
		"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("stream", "2026-03-01", fmt.Sprintf("%d", time.Now().UnixMilli()))},
		"event": &ddbtypes.AttributeValueMemberS{Value: types.JobEventFail},
	})

	record := makeJobRecord("gold-revenue", types.JobEventFail)
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// No category → uses MaxRetries=3, no reruns → should retry
	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Len(t, sfnMock.executions, 1, "empty category should use MaxRetries (backward compat)")
}

// ---------------------------------------------------------------------------
// ResolveTriggerLockTTL
// ---------------------------------------------------------------------------

func TestResolveTriggerLockTTL_Default(t *testing.T) {
	t.Setenv("SFN_TIMEOUT_SECONDS", "")
	got := lambda.ResolveTriggerLockTTL()
	assert.Equal(t, 4*time.Hour+30*time.Minute, got)
}

func TestResolveTriggerLockTTL_FromEnv(t *testing.T) {
	t.Setenv("SFN_TIMEOUT_SECONDS", "14400")
	got := lambda.ResolveTriggerLockTTL()
	assert.Equal(t, 14400*time.Second+30*time.Minute, got)
}

func TestResolveTriggerLockTTL_InvalidEnv(t *testing.T) {
	t.Setenv("SFN_TIMEOUT_SECONDS", "not-a-number")
	got := lambda.ResolveTriggerLockTTL()
	assert.Equal(t, 4*time.Hour+30*time.Minute, got)
}

func TestResolveTriggerLockTTL_ZeroFallsBackToDefault(t *testing.T) {
	t.Setenv("SFN_TIMEOUT_SECONDS", "0")
	got := lambda.ResolveTriggerLockTTL()
	assert.Equal(t, 4*time.Hour+30*time.Minute, got)
}

func TestResolveTriggerLockTTL_NegativeFallsBackToDefault(t *testing.T) {
	t.Setenv("SFN_TIMEOUT_SECONDS", "-100")
	got := lambda.ResolveTriggerLockTTL()
	assert.Equal(t, 4*time.Hour+30*time.Minute, got)
}

// ---------------------------------------------------------------------------
// Stream-based post-run evaluation tests
// ---------------------------------------------------------------------------

// seedTriggerWithStatus inserts a trigger row with a specific status.
func seedTriggerWithStatus(mock *mockDDB, pipelineID, date, status string) {
	mock.putRaw(testControlTable, map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("stream", date)},
		"status": &ddbtypes.AttributeValueMemberS{Value: status},
	})
}

// postRunConfig returns a PipelineConfig with stream trigger + post-run rules.
func postRunConfig() types.PipelineConfig {
	return types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "gold-revenue"},
		Schedule: types.ScheduleConfig{
			Trigger: &types.TriggerCondition{
				Key:   "upstream-complete",
				Check: types.CheckEquals,
				Field: "status",
				Value: "ready",
			},
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
			Type:   "command",
			Config: map[string]interface{}{"command": "echo hello"},
		},
		PostRun: &types.PostRunConfig{
			Rules: []types.ValidationRule{
				{Key: "audit-result", Check: types.CheckGTE, Field: "sensor_count", Value: float64(100)},
			},
		},
	}
}

func TestPostRunSensor_Completed_DriftDetected(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	cfg := postRunConfig()
	seedConfig(mock, cfg)
	seedTriggerWithStatus(mock, "gold-revenue", "2026-03-01", types.TriggerStatusCompleted)

	// Seed baseline captured at completion time.
	seedSensor(mock, "gold-revenue", "postrun-baseline#2026-03-01", map[string]interface{}{
		"sensor_count": float64(100),
	})

	// Sensor arrives with different count → drift.
	record := makeSensorRecord("gold-revenue", "audit-result", map[string]events.DynamoDBAttributeValue{
		"data": events.NewMapAttribute(map[string]events.DynamoDBAttributeValue{
			"sensor_count": events.NewNumberAttribute("150"),
			"date":         events.NewStringAttribute("2026-03-01"),
		}),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}
	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// Should publish POST_RUN_DRIFT event.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	found := false
	for _, evt := range ebMock.events {
		if *evt.Entries[0].DetailType == string(types.EventPostRunDrift) {
			found = true
			break
		}
	}
	assert.True(t, found, "expected POST_RUN_DRIFT event")

	// Should write rerun request.
	rerunKey := ddbItemKey(testControlTable, types.PipelinePK("gold-revenue"), types.RerunRequestSK("stream", "2026-03-01"))
	mock.mu.Lock()
	_, rerunExists := mock.items[rerunKey]
	mock.mu.Unlock()
	assert.True(t, rerunExists, "expected rerun request to be written on drift")
}

func TestPostRunSensor_Completed_NoDrift_RulesPass(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	cfg := postRunConfig()
	seedConfig(mock, cfg)
	seedTriggerWithStatus(mock, "gold-revenue", "2026-03-01", types.TriggerStatusCompleted)

	// Baseline with same count as incoming sensor.
	seedSensor(mock, "gold-revenue", "postrun-baseline#2026-03-01", map[string]interface{}{
		"sensor_count": float64(150),
	})
	// Seed the actual sensor so EvaluateRules can find it.
	seedSensor(mock, "gold-revenue", "audit-result", map[string]interface{}{
		"sensor_count": float64(150),
	})

	record := makeSensorRecord("gold-revenue", "audit-result", map[string]events.DynamoDBAttributeValue{
		"data": events.NewMapAttribute(map[string]events.DynamoDBAttributeValue{
			"sensor_count": events.NewNumberAttribute("150"),
			"date":         events.NewStringAttribute("2026-03-01"),
		}),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}
	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// Should publish POST_RUN_PASSED event.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	found := false
	for _, evt := range ebMock.events {
		if *evt.Entries[0].DetailType == string(types.EventPostRunPassed) {
			found = true
			break
		}
	}
	assert.True(t, found, "expected POST_RUN_PASSED event")
}

func TestPostRunSensor_Running_InflightDrift(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	cfg := postRunConfig()
	seedConfig(mock, cfg)
	seedTriggerWithStatus(mock, "gold-revenue", "2026-03-01", types.TriggerStatusRunning)

	// Baseline from a previous run.
	seedSensor(mock, "gold-revenue", "postrun-baseline#2026-03-01", map[string]interface{}{
		"sensor_count": float64(100),
	})

	record := makeSensorRecord("gold-revenue", "audit-result", map[string]events.DynamoDBAttributeValue{
		"data": events.NewMapAttribute(map[string]events.DynamoDBAttributeValue{
			"sensor_count": events.NewNumberAttribute("200"),
			"date":         events.NewStringAttribute("2026-03-01"),
		}),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}
	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// Should publish informational POST_RUN_DRIFT_INFLIGHT event (no rerun).
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	found := false
	for _, evt := range ebMock.events {
		if *evt.Entries[0].DetailType == string(types.EventPostRunDriftInflight) {
			found = true
			break
		}
	}
	assert.True(t, found, "expected POST_RUN_DRIFT_INFLIGHT event")

	// Should NOT write rerun request when trigger is still running.
	rerunKey := ddbItemKey(testControlTable, types.PipelinePK("gold-revenue"), types.RerunRequestSK("stream", "2026-03-01"))
	mock.mu.Lock()
	_, rerunExists := mock.items[rerunKey]
	mock.mu.Unlock()
	assert.False(t, rerunExists, "should NOT write rerun request when trigger is running")
}

func TestPostRunSensor_FailedFinal_Skipped(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	cfg := postRunConfig()
	seedConfig(mock, cfg)
	seedTriggerWithStatus(mock, "gold-revenue", "2026-03-01", types.TriggerStatusFailedFinal)

	record := makeSensorRecord("gold-revenue", "audit-result", map[string]events.DynamoDBAttributeValue{
		"data": events.NewMapAttribute(map[string]events.DynamoDBAttributeValue{
			"sensor_count": events.NewNumberAttribute("200"),
			"date":         events.NewStringAttribute("2026-03-01"),
		}),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}
	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// No post-run events should be published for FAILED_FINAL trigger.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	for _, evt := range ebMock.events {
		dt := *evt.Entries[0].DetailType
		if dt == string(types.EventPostRunDrift) || dt == string(types.EventPostRunPassed) || dt == string(types.EventPostRunFailed) {
			t.Errorf("unexpected post-run event %q for FAILED_FINAL trigger", dt)
		}
	}
}

func TestPostRunSensor_NoTrigger_Skipped(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	cfg := postRunConfig()
	seedConfig(mock, cfg)
	// No trigger seeded for this date.

	record := makeSensorRecord("gold-revenue", "audit-result", map[string]events.DynamoDBAttributeValue{
		"data": events.NewMapAttribute(map[string]events.DynamoDBAttributeValue{
			"sensor_count": events.NewNumberAttribute("200"),
			"date":         events.NewStringAttribute("2026-03-01"),
		}),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}
	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// No events published when no trigger exists.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	assert.Empty(t, ebMock.events, "no events expected when trigger doesn't exist")
}

func TestPostRunSensor_NoPostRunConfig_GoesToTrigger(t *testing.T) {
	mock := newMockDDB()
	d, _, _ := testDeps(mock)

	// Config WITHOUT post-run rules — non-matching sensor key should be silently ignored.
	cfg := testStreamConfig()
	seedConfig(mock, cfg)

	record := makeSensorRecord("gold-revenue", "audit-result", map[string]events.DynamoDBAttributeValue{
		"data": events.NewMapAttribute(map[string]events.DynamoDBAttributeValue{
			"sensor_count": events.NewNumberAttribute("200"),
			"date":         events.NewStringAttribute("2026-03-01"),
		}),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}
	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)
	// No error, just silently ignored.
}

// ---------------------------------------------------------------------------
// Phase 3: Atomic lock reset tests (ResetTriggerLock replaces delete+create)
// ---------------------------------------------------------------------------

// seedTriggerLockWithSchedule inserts a trigger lock for a given schedule (not
// just "stream"). Used by Phase 3 tests where the schedule name matters.
func seedTriggerLockWithSchedule(mock *mockDDB) {
	mock.putRaw(testControlTable, map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("gold-revenue")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("stream", "2026-03-01")},
		"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusRunning},
		"ttl":    &ddbtypes.AttributeValueMemberN{Value: "9999999999"},
	})
}

// makeJobRecordWithScheduleDate builds a JOB# stream record with an explicit
// schedule and date encoded in the SK.
func makeJobRecordWithScheduleDate(pipelineID, jobEvent, schedule, date string) events.DynamoDBEventRecord {
	const timestamp = "1709312400"
	pk := types.PipelinePK(pipelineID)
	sk := types.JobSK(schedule, date, timestamp)

	return events.DynamoDBEventRecord{
		EventName: "INSERT",
		Change: events.DynamoDBStreamRecord{
			Keys: map[string]events.DynamoDBAttributeValue{
				"PK": events.NewStringAttribute(pk),
				"SK": events.NewStringAttribute(sk),
			},
			NewImage: map[string]events.DynamoDBAttributeValue{
				"PK":    events.NewStringAttribute(pk),
				"SK":    events.NewStringAttribute(sk),
				"event": events.NewStringAttribute(jobEvent),
			},
		},
	}
}

// makeRerunRequestRecordFull builds a RERUN_REQUEST# stream record with
// explicit pipeline, schedule, date, and reason parameters. Unlike the
// zero-argument makeRerunRequestRecord helper, this version is parameterized
// for Phase 3 tests.
func makeRerunRequestRecordFull(reason string) events.DynamoDBEventRecord {
	pk := types.PipelinePK("gold-revenue")
	sk := types.RerunRequestSK("stream", "2026-03-01")

	newImage := map[string]events.DynamoDBAttributeValue{
		"PK": events.NewStringAttribute(pk),
		"SK": events.NewStringAttribute(sk),
	}
	if reason != "" {
		newImage["reason"] = events.NewStringAttribute(reason)
	}

	return events.DynamoDBEventRecord{
		EventName: "INSERT",
		Change: events.DynamoDBStreamRecord{
			Keys: map[string]events.DynamoDBAttributeValue{
				"PK": events.NewStringAttribute(pk),
				"SK": events.NewStringAttribute(sk),
			},
			NewImage: newImage,
		},
	}
}

// triggerLockExists returns true if the trigger lock row is present in the mock.
func triggerLockExists(mock *mockDDB) bool {
	mock.mu.Lock()
	defer mock.mu.Unlock()
	key := ddbItemKey(testControlTable, types.PipelinePK("gold-revenue"), types.TriggerSK("stream", "2026-03-01"))
	_, ok := mock.items[key]
	return ok
}

// TestJobFailure_AtomicLockReset_Success verifies that a job failure under the
// retry limit uses ResetTriggerLock (atomic UpdateItem) instead of the
// delete+create pattern. The trigger row must still exist after the operation
// (it was updated in place, not deleted and re-created).
func TestJobFailure_AtomicLockReset_Success(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	const (
		pipeline = "gold-revenue"
		schedule = "stream"
		date     = "2026-03-01"
	)

	cfg := testJobConfig()
	seedConfig(mock, cfg)

	// Seed existing trigger lock — ResetTriggerLock requires it to exist.
	seedTriggerLockWithSchedule(mock)

	record := makeJobRecordWithScheduleDate(pipeline, types.JobEventFail, schedule, date)
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// SFN must have started for the rerun.
	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1, "expected one SFN execution for rerun")
	assert.Contains(t, *sfnMock.executions[0].Name, "rerun-0")

	// The trigger lock row must still exist — ResetTriggerLock updates in place.
	assert.True(t, triggerLockExists(mock),
		"trigger lock row must persist after atomic reset (not deleted)")
}

// TestJobFailure_LockResetFails_NoSFN verifies that when there is no trigger
// row for the pipeline/schedule/date (lock was never held or expired via TTL),
// ResetTriggerLock returns false and no SFN execution is started.
func TestJobFailure_LockResetFails_NoSFN(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	const (
		pipeline = "gold-revenue"
		schedule = "stream"
		date     = "2026-03-01"
	)

	cfg := testJobConfig()
	seedConfig(mock, cfg)

	// No trigger lock seeded — ResetTriggerLock should return (false, nil).
	record := makeJobRecordWithScheduleDate(pipeline, types.JobEventFail, schedule, date)
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Empty(t, sfnMock.executions, "no SFN when trigger lock does not exist")
}

// TestRerunRequest_AtomicLockReset verifies that a manual rerun request uses
// ResetTriggerLock instead of delete+create.
func TestRerunRequest_AtomicLockReset(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testJobConfig()
	seedConfig(mock, cfg)

	// Seed trigger lock so ResetTriggerLock can find it.
	seedTriggerLockWithSchedule(mock)

	record := makeRerunRequestRecordFull("manual")
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// SFN must have started.
	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1, "expected one SFN execution for rerun")

	// Trigger lock row must still exist after reset.
	assert.True(t, triggerLockExists(mock),
		"trigger lock row must persist after atomic reset")
}

// TestRerunRequest_LockResetFails_PublishesInfraFailure verifies that when the
// trigger lock row does not exist, ResetTriggerLock returns false and an
// INFRA_FAILURE event is published instead of starting a new SFN execution.
func TestRerunRequest_LockResetFails_PublishesInfraFailure(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, ebMock := testDeps(mock)

	cfg := testJobConfig()
	seedConfig(mock, cfg)

	// No trigger lock seeded — ResetTriggerLock returns (false, nil).
	record := makeRerunRequestRecordFull("manual")
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// No SFN execution.
	sfnMock.mu.Lock()
	sfnExecs := len(sfnMock.executions)
	sfnMock.mu.Unlock()
	assert.Equal(t, 0, sfnExecs, "no SFN when lock reset fails")

	// Only INFRA_FAILURE — RERUN_ACCEPTED is emitted after lock reset, which
	// did not succeed here.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	require.Len(t, ebMock.events, 1, "expected one INFRA_FAILURE event")
	assert.Equal(t, string(types.EventInfraFailure), *ebMock.events[0].Entries[0].DetailType)
}

// TestJobFailure_SFNStartFails_ReleasesLock verifies that when the SFN
// StartExecution call fails after ResetTriggerLock succeeds, the trigger lock
// is released so the pipeline is not permanently stuck.
func TestJobFailure_SFNStartFails_ReleasesLock(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	const (
		pipeline = "gold-revenue"
		schedule = "stream"
		date     = "2026-03-01"
	)

	cfg := testJobConfig()
	seedConfig(mock, cfg)

	// Seed trigger lock.
	seedTriggerLockWithSchedule(mock)

	// Make SFN fail.
	sfnMock.err = fmt.Errorf("SFN unavailable")

	record := makeJobRecordWithScheduleDate(pipeline, types.JobEventFail, schedule, date)
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	// HandleStreamEvent swallows per-record errors — the handler returns nil.
	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	// Trigger lock must be released after SFN failure (so next attempt can acquire it).
	assert.False(t, triggerLockExists(mock),
		"trigger lock must be released after SFN start failure")
}

// TestRerunRequest_SFNStartFails_ReleasesLock mirrors the above test for the
// rerun-request path.
func TestRerunRequest_SFNStartFails_ReleasesLock(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testJobConfig()
	seedConfig(mock, cfg)

	seedTriggerLockWithSchedule(mock)

	sfnMock.err = fmt.Errorf("SFN unavailable")

	record := makeRerunRequestRecordFull("manual")
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	assert.False(t, triggerLockExists(mock),
		"trigger lock must be released after SFN start failure")
}

// ---------------------------------------------------------------------------
// Phase 2: isExcludedDate unit tests (Task A / C1)
// ---------------------------------------------------------------------------

func TestIsExcludedDate_NoExclusions(t *testing.T) {
	cfg := &types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Schedule: types.ScheduleConfig{Exclude: nil},
	}
	assert.False(t, lambda.IsExcludedDate(cfg, "2026-03-07"), "nil exclusion must not exclude")
	assert.False(t, lambda.IsExcludedDate(cfg, "2026-03-08"), "nil exclusion must not exclude")
}

func TestIsExcludedDate_WeekendSaturday(t *testing.T) {
	cfg := &types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Schedule: types.ScheduleConfig{
			Exclude: &types.ExclusionConfig{Weekends: true},
		},
	}
	// 2026-03-07 is a Saturday.
	assert.True(t, lambda.IsExcludedDate(cfg, "2026-03-07"), "Saturday must be excluded when Weekends=true")
}

func TestIsExcludedDate_WeekendSunday(t *testing.T) {
	cfg := &types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Schedule: types.ScheduleConfig{
			Exclude: &types.ExclusionConfig{Weekends: true},
		},
	}
	// 2026-03-08 is a Sunday.
	assert.True(t, lambda.IsExcludedDate(cfg, "2026-03-08"), "Sunday must be excluded when Weekends=true")
}

func TestIsExcludedDate_WeekdayFriday(t *testing.T) {
	cfg := &types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Schedule: types.ScheduleConfig{
			Exclude: &types.ExclusionConfig{Weekends: true},
		},
	}
	// 2026-03-06 is a Friday.
	assert.False(t, lambda.IsExcludedDate(cfg, "2026-03-06"), "Friday must NOT be excluded when Weekends=true")
}

func TestIsExcludedDate_SpecificDate(t *testing.T) {
	cfg := &types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Schedule: types.ScheduleConfig{
			Exclude: &types.ExclusionConfig{Dates: []string{"2026-12-25", "2026-01-01"}},
		},
	}
	assert.True(t, lambda.IsExcludedDate(cfg, "2026-12-25"), "Christmas must be excluded")
	assert.True(t, lambda.IsExcludedDate(cfg, "2026-01-01"), "New Year's Day must be excluded")
	assert.False(t, lambda.IsExcludedDate(cfg, "2026-03-06"), "regular day must not be excluded")
}

func TestIsExcludedDate_CompositePerHourDate(t *testing.T) {
	cfg := &types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Schedule: types.ScheduleConfig{
			Exclude: &types.ExclusionConfig{Dates: []string{"2026-03-07"}},
		},
	}
	// Per-hour composite date "2026-03-07T10" — date portion is 2026-03-07.
	assert.True(t, lambda.IsExcludedDate(cfg, "2026-03-07T10"), "composite date must match date portion")
	assert.False(t, lambda.IsExcludedDate(cfg, "2026-03-06T10"), "non-excluded composite date must not be excluded")
}

func TestIsExcludedDate_InvalidDate(t *testing.T) {
	cfg := &types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Schedule: types.ScheduleConfig{
			Exclude: &types.ExclusionConfig{Weekends: true, Dates: []string{"2026-03-07"}},
		},
	}
	// Unparseable date — safe default is false (do not exclude).
	assert.False(t, lambda.IsExcludedDate(cfg, "not-a-date"), "invalid date must return false (safe default)")
}

func TestIsExcludedDate_EmptyDate(t *testing.T) {
	cfg := &types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Schedule: types.ScheduleConfig{
			Exclude: &types.ExclusionConfig{Weekends: true},
		},
	}
	// Empty string is too short to parse — safe default is false.
	assert.False(t, lambda.IsExcludedDate(cfg, ""), "empty date must return false (safe default)")
}

func TestIsExcludedDate_ShortDate(t *testing.T) {
	cfg := &types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Schedule: types.ScheduleConfig{
			Exclude: &types.ExclusionConfig{Weekends: true},
		},
	}
	assert.False(t, lambda.IsExcludedDate(cfg, "2026-03"), "short date string must return false (safe default)")
}

func TestIsExcludedDate_Timezone(t *testing.T) {
	cfg := &types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Schedule: types.ScheduleConfig{
			Timezone: "America/Los_Angeles",
			Exclude:  &types.ExclusionConfig{Weekends: true},
		},
	}
	// 2026-03-07 is Saturday regardless of timezone.
	assert.True(t, lambda.IsExcludedDate(cfg, "2026-03-07"), "Saturday is excluded in any timezone")
	assert.False(t, lambda.IsExcludedDate(cfg, "2026-03-06"), "Friday is not excluded")
}

// ---------------------------------------------------------------------------
// Phase 2: Integration tests for handler calendar exclusion (Task B / C2)
// ---------------------------------------------------------------------------

func TestRerunRequest_CalendarExclusion(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, ebMock := testDeps(mock)

	cfg := testJobConfig()
	cfg.Schedule.Exclude = &types.ExclusionConfig{Dates: []string{"2026-03-01"}}
	seedConfig(mock, cfg)

	record := makeDefaultRerunRequestRecord() // schedule=stream, date=2026-03-01
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	assert.Empty(t, sfnMock.executions, "rerun must not start SFN for excluded execution date")
	sfnMock.mu.Unlock()

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	require.NotEmpty(t, ebMock.events, "expected PIPELINE_EXCLUDED event")
	assert.Equal(t, string(types.EventPipelineExcluded), *ebMock.events[0].Entries[0].DetailType)
}

func TestRerunRequest_CalendarExclusion_WritesJobEvent(t *testing.T) {
	mock := newMockDDB()
	d, _, _ := testDeps(mock)

	cfg := testJobConfig()
	cfg.Schedule.Exclude = &types.ExclusionConfig{Dates: []string{"2026-03-01"}}
	seedConfig(mock, cfg)

	record := makeDefaultRerunRequestRecord()
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	mock.mu.Lock()
	defer mock.mu.Unlock()
	found := false
	for key := range mock.items {
		if strings.Contains(key, "joblog") && strings.Contains(key, "JOB#") {
			found = true
			break
		}
	}
	assert.True(t, found, "expected a JOB# entry in the joblog table for calendar exclusion rejection")
}

func TestRerunRequest_WeekendExclusion(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, ebMock := testDeps(mock)

	cfg := testJobConfig()
	cfg.Schedule.Exclude = &types.ExclusionConfig{Weekends: true}
	seedConfig(mock, cfg)

	// 2026-03-07 is a Saturday.
	pk := types.PipelinePK("gold-revenue")
	sk := types.RerunRequestSK("stream", "2026-03-07")
	record := events.DynamoDBEventRecord{
		EventName: "INSERT",
		Change: events.DynamoDBStreamRecord{
			Keys: map[string]events.DynamoDBAttributeValue{
				"PK": events.NewStringAttribute(pk),
				"SK": events.NewStringAttribute(sk),
			},
			NewImage: map[string]events.DynamoDBAttributeValue{
				"PK": events.NewStringAttribute(pk),
				"SK": events.NewStringAttribute(sk),
			},
		},
	}
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	assert.Empty(t, sfnMock.executions, "rerun on Saturday must be blocked by weekend exclusion")
	sfnMock.mu.Unlock()

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	require.NotEmpty(t, ebMock.events)
	assert.Equal(t, string(types.EventPipelineExcluded), *ebMock.events[0].Entries[0].DetailType)
}

func TestJobFailure_CalendarExclusion(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, ebMock := testDeps(mock)

	cfg := testJobConfig()
	cfg.Schedule.Exclude = &types.ExclusionConfig{Dates: []string{"2026-03-01"}}
	seedConfig(mock, cfg)

	record := makeJobRecord("gold-revenue", types.JobEventFail)
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	assert.Empty(t, sfnMock.executions, "job failure rerun must not start SFN for excluded execution date")
	sfnMock.mu.Unlock()

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	require.NotEmpty(t, ebMock.events, "expected PIPELINE_EXCLUDED event")
	assert.Equal(t, string(types.EventPipelineExcluded), *ebMock.events[0].Entries[0].DetailType)
}

func TestJobFailure_CalendarExclusion_RetryLimitBeatsExclusion(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, ebMock := testDeps(mock)

	cfg := testJobConfig()
	cfg.Schedule.Exclude = &types.ExclusionConfig{Dates: []string{"2026-03-01"}}
	seedConfig(mock, cfg)

	seedRerun(mock, 0)
	seedRerun(mock, 1)

	record := makeJobRecord("gold-revenue", types.JobEventFail)
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	assert.Empty(t, sfnMock.executions, "no SFN when retries exhausted")
	sfnMock.mu.Unlock()

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	require.NotEmpty(t, ebMock.events)
	assert.Equal(t, string(types.EventRetryExhausted), *ebMock.events[0].Entries[0].DetailType,
		"retry-limit check must run before calendar exclusion in handleJobFailure")
}

func TestPostRunDrift_CalendarExclusion(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	cfg := postRunConfig()
	cfg.Schedule.Exclude = &types.ExclusionConfig{Dates: []string{"2026-03-01"}}
	seedConfig(mock, cfg)
	seedTriggerWithStatus(mock, "gold-revenue", "2026-03-01", types.TriggerStatusCompleted)

	seedSensor(mock, "gold-revenue", "postrun-baseline#2026-03-01", map[string]interface{}{
		"sensor_count": float64(100),
	})

	record := makeSensorRecord("gold-revenue", "audit-result", map[string]events.DynamoDBAttributeValue{
		"data": events.NewMapAttribute(map[string]events.DynamoDBAttributeValue{
			"sensor_count": events.NewNumberAttribute("150"),
			"date":         events.NewStringAttribute("2026-03-01"),
		}),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}
	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	found := false
	for _, evt := range ebMock.events {
		if *evt.Entries[0].DetailType == string(types.EventPostRunDrift) {
			found = true
			break
		}
	}
	assert.True(t, found, "POST_RUN_DRIFT event must be published even when execution date is excluded")

	// PIPELINE_EXCLUDED event must be published when drift rerun is skipped.
	excludedFound := false
	for _, evt := range ebMock.events {
		if *evt.Entries[0].DetailType == string(types.EventPipelineExcluded) {
			excludedFound = true
			break
		}
	}
	assert.True(t, excludedFound, "PIPELINE_EXCLUDED event must be published when drift rerun is skipped by calendar")

	rerunKey := ddbItemKey(testControlTable, types.PipelinePK("gold-revenue"), types.RerunRequestSK("stream", "2026-03-01"))
	mock.mu.Lock()
	_, rerunExists := mock.items[rerunKey]
	mock.mu.Unlock()
	assert.False(t, rerunExists, "rerun request must NOT be written when execution date is excluded")
}

func TestPostRunDrift_NotExcluded_WritesRerun(t *testing.T) {
	mock := newMockDDB()
	d, _, _ := testDeps(mock)

	cfg := postRunConfig()
	seedConfig(mock, cfg)
	seedTriggerWithStatus(mock, "gold-revenue", "2026-03-01", types.TriggerStatusCompleted)

	seedSensor(mock, "gold-revenue", "postrun-baseline#2026-03-01", map[string]interface{}{
		"sensor_count": float64(100),
	})

	record := makeSensorRecord("gold-revenue", "audit-result", map[string]events.DynamoDBAttributeValue{
		"data": events.NewMapAttribute(map[string]events.DynamoDBAttributeValue{
			"sensor_count": events.NewNumberAttribute("150"),
			"date":         events.NewStringAttribute("2026-03-01"),
		}),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}
	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	rerunKey := ddbItemKey(testControlTable, types.PipelinePK("gold-revenue"), types.RerunRequestSK("stream", "2026-03-01"))
	mock.mu.Lock()
	_, rerunExists := mock.items[rerunKey]
	mock.mu.Unlock()
	assert.True(t, rerunExists, "rerun request MUST be written for non-excluded dates with drift")
}

func TestSensorEvent_CalendarExclusion_PublishesEvent(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, ebMock := testDeps(mock)

	cfg := testStreamConfig()
	today := time.Now().Format("2006-01-02")
	cfg.Schedule.Exclude = &types.ExclusionConfig{Dates: []string{today}}
	seedConfig(mock, cfg)

	record := makeSensorRecord("gold-revenue", "upstream-complete", map[string]events.DynamoDBAttributeValue{
		"status": events.NewStringAttribute("ready"),
	})
	event := lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}}

	err := lambda.HandleStreamEvent(context.Background(), d, event)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	assert.Empty(t, sfnMock.executions, "no SFN when calendar-excluded")
	sfnMock.mu.Unlock()

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	require.NotEmpty(t, ebMock.events, "PIPELINE_EXCLUDED event must be published on sensor exclusion")
	assert.Equal(t, string(types.EventPipelineExcluded), *ebMock.events[0].Entries[0].DetailType)
}

package lambda_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
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

// seedRerun inserts an existing RERUN# row into the mock rerun table.
func seedRerun(mock *mockDDB, pipelineID, schedule, date string, attempt int) {
	mock.putRaw("rerun", map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.RerunSK(schedule, date, attempt)},
		"reason": &ddbtypes.AttributeValueMemberS{Value: "fail"},
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
	seedRerun(mock, "gold-revenue", "stream", "2026-03-01", 0)
	seedRerun(mock, "gold-revenue", "stream", "2026-03-01", 1)

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
	got := lambda.ResolveExecutionDate(data)
	if got != "2026-03-03T10" {
		t.Errorf("got %q, want %q", got, "2026-03-03T10")
	}
}

func TestResolveExecutionDate_DashedDate(t *testing.T) {
	data := map[string]interface{}{"date": "2026-03-03", "hour": "10"}
	got := lambda.ResolveExecutionDate(data)
	if got != "2026-03-03T10" {
		t.Errorf("got %q, want %q", got, "2026-03-03T10")
	}
}

func TestResolveExecutionDate_DateOnly(t *testing.T) {
	data := map[string]interface{}{"date": "20260303"}
	got := lambda.ResolveExecutionDate(data)
	if got != "2026-03-03" {
		t.Errorf("got %q, want %q", got, "2026-03-03")
	}
}

func TestResolveExecutionDate_NoFields(t *testing.T) {
	data := map[string]interface{}{"complete": true}
	got := lambda.ResolveExecutionDate(data)
	today := time.Now().Format("2006-01-02")
	if got != today {
		t.Errorf("got %q, want %q", got, today)
	}
}

func TestResolveExecutionDate_HourWithLeadingZero(t *testing.T) {
	data := map[string]interface{}{"date": "20260303", "hour": "03"}
	got := lambda.ResolveExecutionDate(data)
	if got != "2026-03-03T03" {
		t.Errorf("got %q, want %q", got, "2026-03-03T03")
	}
}

// ---------------------------------------------------------------------------
// Rerun request helpers and tests
// ---------------------------------------------------------------------------

// makeRerunRequestRecord builds a DynamoDB stream event record for a RERUN_REQUEST# write.
func makeRerunRequestRecord() events.DynamoDBEventRecord {
	pk := types.PipelinePK("gold-revenue")
	sk := types.RerunRequestSK("stream", "2026-03-01")
	return events.DynamoDBEventRecord{
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

	// Seed a failed job event.
	seedJobEvent(mock, "1709280000000", types.JobEventFail)

	record := makeRerunRequestRecord()
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

	// Seed a successful job event with timestamp 1000000.
	seedJobEvent(mock, "1000000", types.JobEventSuccess)

	// Seed a sensor with updatedAt AFTER the job timestamp.
	seedSensor(mock, "gold-revenue", "upstream-complete", map[string]interface{}{
		"updatedAt": float64(2000000), // newer than job timestamp
		"status":    "ready",
	})

	record := makeRerunRequestRecord()
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

	record := makeRerunRequestRecord()
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

	// Seed an infra-trigger-exhausted job event.
	seedJobEvent(mock, "1709280000000", types.JobEventInfraTriggerExhausted)

	record := makeRerunRequestRecord()
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

	// No job events seeded — GetLatestJobEvent returns nil → allow (never ran).
	record := makeRerunRequestRecord()
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
	record := makeRerunRequestRecord() // uses "gold-revenue"
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

	// Seed a timeout job event.
	seedJobEvent(mock, "1709280000000", types.JobEventTimeout)

	record := makeRerunRequestRecord()
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

	// Seed a job event with an unknown event type.
	seedJobEvent(mock, "1709280000000", "some-future-event")

	record := makeRerunRequestRecord()
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

	record := makeRerunRequestRecord()
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

	// Seed a successful job event — no sensors exist.
	seedJobEvent(mock, "1000000", types.JobEventSuccess)

	record := makeRerunRequestRecord()
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

	// Seed a successful job event.
	seedJobEvent(mock, "1000000", types.JobEventSuccess)

	// Seed sensor WITHOUT updatedAt field.
	seedSensor(mock, "gold-revenue", "upstream-complete", map[string]interface{}{
		"status": "ready",
	})

	record := makeRerunRequestRecord()
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

	// Seed a successful job event with timestamp 1000000.
	seedJobEvent(mock, "1000000", types.JobEventSuccess)

	// Seed a sensor with updatedAt as float64 > jobTimestamp.
	seedSensor(mock, "gold-revenue", "upstream-complete", map[string]interface{}{
		"updatedAt": float64(2000000),
	})

	record := makeRerunRequestRecord()
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

	record := makeRerunRequestRecord()
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

	// Seed a successful job event with timestamp 1000000.
	seedJobEvent(mock, "1000000", types.JobEventSuccess)

	// Seed a sensor with updatedAt as string > jobTimestamp.
	seedSensor(mock, "gold-revenue", "upstream-complete", map[string]interface{}{
		"updatedAt": "2000000",
	})

	record := makeRerunRequestRecord()
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

	record := makeRerunRequestRecord()
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

	record := makeRerunRequestRecord()
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

func TestBuildSFNConfig_WithPostRun(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testStreamConfig()
	cfg.PostRun = &types.PostRunConfig{
		Evaluation: &types.EvaluationWindow{
			Interval: "10m",
			Window:   "1h",
		},
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
	assert.Equal(t, true, cfgMap["hasPostRun"])
	assert.Equal(t, float64(600), cfgMap["postRunIntervalSeconds"]) // 10m = 600s
	assert.Equal(t, float64(3600), cfgMap["postRunWindowSeconds"])  // 1h = 3600s
}

func TestBuildSFNConfig_WithoutPostRun(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testStreamConfig() // no PostRun
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
	assert.Equal(t, false, cfgMap["hasPostRun"])
	// postRunIntervalSeconds and postRunWindowSeconds should be omitted (zero value).
	_, hasPostInterval := cfgMap["postRunIntervalSeconds"]
	assert.False(t, hasPostInterval, "postRunIntervalSeconds should be omitted when no PostRun")
}

func TestBuildSFNConfig_PostRunDefaults(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testStreamConfig()
	cfg.PostRun = &types.PostRunConfig{
		// No Evaluation block — should use defaults (30m interval, 2h window).
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
	assert.Equal(t, true, cfgMap["hasPostRun"])
	assert.Equal(t, float64(1800), cfgMap["postRunIntervalSeconds"]) // 30m default
	assert.Equal(t, float64(7200), cfgMap["postRunWindowSeconds"])   // 2h default
}

func TestBuildSFNConfig_CustomTimings(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := testStreamConfig()
	cfg.Schedule.Evaluation = types.EvaluationWindow{
		Interval: "2m",
		Window:   "30m",
	}
	cfg.PostRun = &types.PostRunConfig{
		Evaluation: &types.EvaluationWindow{
			Interval: "15m",
			Window:   "3h",
		},
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

	assert.Equal(t, float64(120), cfgMap["evaluationIntervalSeconds"]) // 2m = 120s
	assert.Equal(t, float64(1800), cfgMap["evaluationWindowSeconds"])  // 30m = 1800s
	assert.Equal(t, float64(900), cfgMap["postRunIntervalSeconds"])    // 15m = 900s
	assert.Equal(t, float64(10800), cfgMap["postRunWindowSeconds"])    // 3h = 10800s
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
	got := lambda.ResolveExecutionDate(data)
	assert.Equal(t, "2026-03-03", got)
}

func TestNormalizeDate_Compact(t *testing.T) {
	data := map[string]interface{}{"date": "20260303"}
	got := lambda.ResolveExecutionDate(data)
	assert.Equal(t, "2026-03-03", got)
}

func TestNormalizeDate_CompactWithHour(t *testing.T) {
	data := map[string]interface{}{"date": "20260303", "hour": "07"}
	got := lambda.ResolveExecutionDate(data)
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

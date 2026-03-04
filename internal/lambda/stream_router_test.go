package lambda_test

import (
	"context"
	"encoding/json"
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
func seedTriggerLock(mock *mockDDB, pipelineID, schedule, date string) {
	mock.putRaw(testControlTable, map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK(schedule, date)},
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
	seedTriggerLock(mock, "gold-revenue", "stream", date)

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

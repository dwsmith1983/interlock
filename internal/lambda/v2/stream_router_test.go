package v2_test

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

	lambda "github.com/dwsmith1983/interlock/internal/lambda/v2"
	store "github.com/dwsmith1983/interlock/internal/store/v2"
	v2 "github.com/dwsmith1983/interlock/pkg/types/v2"
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
func seedConfig(mock *mockDDB, cfg v2.PipelineConfig) {
	data, _ := json.Marshal(cfg)
	mock.putRaw(testControlTable, map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: v2.PipelinePK(cfg.Pipeline.ID)},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: v2.ConfigSK},
		"config": &ddbtypes.AttributeValueMemberS{Value: string(data)},
	})
}

// seedTriggerLock pre-inserts a trigger lock row to simulate a held lock.
func seedTriggerLock(mock *mockDDB, pipelineID, schedule, date string) {
	mock.putRaw(testControlTable, map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: v2.PipelinePK(pipelineID)},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: v2.TriggerSK(schedule, date)},
		"status": &ddbtypes.AttributeValueMemberS{Value: v2.TriggerStatusRunning},
	})
}

// makeSensorRecord builds a DynamoDB stream event record for a sensor write.
func makeSensorRecord(pipelineID, sensorKey string, fields map[string]events.DynamoDBAttributeValue) events.DynamoDBEventRecord {
	newImage := map[string]events.DynamoDBAttributeValue{
		"PK": events.NewStringAttribute(v2.PipelinePK(pipelineID)),
		"SK": events.NewStringAttribute(v2.SensorSK(sensorKey)),
	}
	for k, v := range fields {
		newImage[k] = v
	}

	return events.DynamoDBEventRecord{
		EventName: "INSERT",
		Change: events.DynamoDBStreamRecord{
			Keys: map[string]events.DynamoDBAttributeValue{
				"PK": events.NewStringAttribute(v2.PipelinePK(pipelineID)),
				"SK": events.NewStringAttribute(v2.SensorSK(sensorKey)),
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
				"PK": events.NewStringAttribute(v2.PipelinePK(pipelineID)),
				"SK": events.NewStringAttribute(v2.ConfigSK),
			},
		},
	}
}

// testStreamConfig returns a PipelineConfig with a stream trigger on "upstream-complete".
func testStreamConfig() v2.PipelineConfig {
	return v2.PipelineConfig{
		Pipeline: v2.PipelineIdentity{ID: "gold-revenue"},
		Schedule: v2.ScheduleConfig{
			Trigger: &v2.TriggerCondition{
				Key:   "upstream-complete",
				Check: v2.CheckEquals,
				Field: "status",
				Value: "ready",
			},
			Evaluation: v2.EvaluationWindow{
				Window:   "1h",
				Interval: "5m",
			},
		},
		Validation: v2.ValidationConfig{
			Trigger: "ALL",
			Rules: []v2.ValidationRule{
				{Key: "upstream-complete", Check: v2.CheckExists},
			},
		},
		Job: v2.JobConfig{
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
	var input lambda.OrchestratorInput
	require.NoError(t, json.Unmarshal([]byte(*sfnMock.executions[0].Input), &input))
	assert.Equal(t, "evaluate", input.Mode)
	assert.Equal(t, "gold-revenue", input.PipelineID)
	assert.Equal(t, "stream", input.ScheduleID)
	assert.Equal(t, time.Now().Format("2006-01-02"), input.Date)

	// Verify EventBridge event was published.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	require.Len(t, ebMock.events, 1, "expected one EventBridge event")
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
	cfg.Schedule.Exclude = &v2.ExclusionConfig{
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
	cfg := v2.PipelineConfig{
		Pipeline: v2.PipelineIdentity{ID: "gold-revenue"},
		Schedule: v2.ScheduleConfig{
			Evaluation: v2.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: v2.ValidationConfig{Trigger: "ALL"},
		Job:        v2.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo hello"}},
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

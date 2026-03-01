package v2_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	lambda "github.com/dwsmith1983/interlock/internal/lambda/v2"
	v2 "github.com/dwsmith1983/interlock/pkg/types/v2"
)

// seedTriggerRow inserts a TRIGGER# row with status and TTL into the mock.
func seedTriggerRow(mock *mockDDB, pipelineID, schedule, date, status string, ttl int64) {
	item := map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: v2.PipelinePK(pipelineID)},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: v2.TriggerSK(schedule, date)},
		"status": &ddbtypes.AttributeValueMemberS{Value: status},
	}
	if ttl > 0 {
		item["ttl"] = &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", ttl)}
	}
	mock.putRaw(testControlTable, item)
}

func TestWatchdog_StaleTrigger_PublishesSFNTimeout(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Seed a TRIGGER# row with a TTL in the past.
	pastTTL := time.Now().Add(-1 * time.Hour).Unix()
	seedTriggerRow(mock, "gold-revenue", "cron", "2026-03-01", v2.TriggerStatusRunning, pastTTL)

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	// Verify EventBridge SFN_TIMEOUT event was published.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	require.Len(t, ebMock.events, 1, "expected one EventBridge event")
	assert.Equal(t, string(v2.EventSFNTimeout), *ebMock.events[0].Entries[0].DetailType)

	// Verify the trigger status was updated to FAILED_FINAL.
	key := ddbItemKey(testControlTable, v2.PipelinePK("gold-revenue"), v2.TriggerSK("cron", "2026-03-01"))
	mock.mu.Lock()
	item, ok := mock.items[key]
	mock.mu.Unlock()
	require.True(t, ok, "trigger row should exist")
	statusVal := item["status"].(*ddbtypes.AttributeValueMemberS).Value
	assert.Equal(t, v2.TriggerStatusFailedFinal, statusVal)
}

func TestWatchdog_FreshTrigger_NoAlert(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Seed a TRIGGER# row with a TTL in the future.
	futureTTL := time.Now().Add(12 * time.Hour).Unix()
	seedTriggerRow(mock, "gold-revenue", "cron", "2026-03-01", v2.TriggerStatusRunning, futureTTL)

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	// No events should be published.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	assert.Empty(t, ebMock.events, "expected no EventBridge events for fresh trigger")
}

func TestWatchdog_MissedSchedule_PublishesAlert(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Seed a cron-scheduled pipeline config.
	cfg := v2.PipelineConfig{
		Pipeline: v2.PipelineIdentity{ID: "bronze-ingest"},
		Schedule: v2.ScheduleConfig{
			Cron:       "0 6 * * *",
			Timezone:   "UTC",
			Evaluation: v2.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: v2.ValidationConfig{Trigger: "ALL"},
		Job:        v2.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo hello"}},
	}
	seedConfig(mock, cfg)

	// No TRIGGER# row for today — should be detected as missed.
	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	require.Len(t, ebMock.events, 1, "expected one EventBridge event for missed schedule")
	assert.Equal(t, string(v2.EventScheduleMissed), *ebMock.events[0].Entries[0].DetailType)
}

func TestWatchdog_SchedulePresent_NoAlert(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Seed a cron-scheduled pipeline config.
	cfg := v2.PipelineConfig{
		Pipeline: v2.PipelineIdentity{ID: "bronze-ingest"},
		Schedule: v2.ScheduleConfig{
			Cron:       "0 6 * * *",
			Timezone:   "UTC",
			Evaluation: v2.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: v2.ValidationConfig{Trigger: "ALL"},
		Job:        v2.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo hello"}},
	}
	seedConfig(mock, cfg)

	// Seed a TRIGGER# row for today.
	today := time.Now().Format("2006-01-02")
	seedTriggerRow(mock, "bronze-ingest", "cron", today, v2.TriggerStatusRunning, time.Now().Add(12*time.Hour).Unix())

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	assert.Empty(t, ebMock.events, "expected no EventBridge events when trigger exists for today")
}

func TestWatchdog_CalendarExcluded_NoAlert(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	today := time.Now().Format("2006-01-02")

	// Seed a cron-scheduled pipeline with today excluded.
	cfg := v2.PipelineConfig{
		Pipeline: v2.PipelineIdentity{ID: "bronze-ingest"},
		Schedule: v2.ScheduleConfig{
			Cron:     "0 6 * * *",
			Timezone: "UTC",
			Exclude: &v2.ExclusionConfig{
				Dates: []string{today},
			},
			Evaluation: v2.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: v2.ValidationConfig{Trigger: "ALL"},
		Job:        v2.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo hello"}},
	}
	seedConfig(mock, cfg)

	// No TRIGGER# for today, but should be skipped because of exclusion.
	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	assert.Empty(t, ebMock.events, "expected no EventBridge events for calendar-excluded day")
}

func TestWatchdog_StreamTriggered_NoMissedCheck(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Seed a stream-triggered pipeline config (no cron).
	cfg := v2.PipelineConfig{
		Pipeline: v2.PipelineIdentity{ID: "gold-revenue"},
		Schedule: v2.ScheduleConfig{
			Trigger: &v2.TriggerCondition{
				Key:   "upstream-complete",
				Check: v2.CheckEquals,
				Field: "status",
				Value: "ready",
			},
			Evaluation: v2.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: v2.ValidationConfig{Trigger: "ALL"},
		Job:        v2.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo hello"}},
	}
	seedConfig(mock, cfg)

	// No TRIGGER# — but this is stream-triggered, not cron, so no missed schedule alert.
	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	assert.Empty(t, ebMock.events, "expected no EventBridge events for stream-triggered pipeline")
}

package lambda_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// seedTriggerRow inserts a TRIGGER# row with status and TTL into the mock.
func seedTriggerRow(mock *mockDDB, pipelineID, date, status string, ttl int64) {
	item := map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("cron", date)},
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
	seedTriggerRow(mock, "gold-revenue", "2026-03-01", types.TriggerStatusRunning, pastTTL)

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	// Verify EventBridge SFN_TIMEOUT event was published.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	require.Len(t, ebMock.events, 1, "expected one EventBridge event")
	assert.Equal(t, string(types.EventSFNTimeout), *ebMock.events[0].Entries[0].DetailType)

	// Verify the trigger status was updated to FAILED_FINAL.
	key := ddbItemKey(testControlTable, types.PipelinePK("gold-revenue"), types.TriggerSK("cron", "2026-03-01"))
	mock.mu.Lock()
	item, ok := mock.items[key]
	mock.mu.Unlock()
	require.True(t, ok, "trigger row should exist")
	statusVal := item["status"].(*ddbtypes.AttributeValueMemberS).Value
	assert.Equal(t, types.TriggerStatusFailedFinal, statusVal)
}

func TestWatchdog_FreshTrigger_NoAlert(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Seed a TRIGGER# row with a TTL in the future.
	futureTTL := time.Now().Add(12 * time.Hour).Unix()
	seedTriggerRow(mock, "gold-revenue", "2026-03-01", types.TriggerStatusRunning, futureTTL)

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
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "bronze-ingest"},
		Schedule: types.ScheduleConfig{
			Cron:       "0 6 * * *",
			Timezone:   "UTC",
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo hello"}},
	}
	seedConfig(mock, cfg)

	// No TRIGGER# row for today — should be detected as missed.
	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	require.Len(t, ebMock.events, 1, "expected one EventBridge event for missed schedule")
	assert.Equal(t, string(types.EventScheduleMissed), *ebMock.events[0].Entries[0].DetailType)
}

func TestWatchdog_SchedulePresent_NoAlert(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Seed a cron-scheduled pipeline config.
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "bronze-ingest"},
		Schedule: types.ScheduleConfig{
			Cron:       "0 6 * * *",
			Timezone:   "UTC",
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo hello"}},
	}
	seedConfig(mock, cfg)

	// Seed a TRIGGER# row for today.
	today := time.Now().Format("2006-01-02")
	seedTriggerRow(mock, "bronze-ingest", today, types.TriggerStatusRunning, time.Now().Add(12*time.Hour).Unix())

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	assert.Empty(t, ebMock.events, "expected no EventBridge events when trigger exists for today")
}

func TestWatchdog_CalendarExcluded_NoAlert(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Use UTC date since the pipeline's timezone is UTC and isExcluded
	// resolves the date in the pipeline's configured timezone.
	today := time.Now().UTC().Format("2006-01-02")

	// Seed a cron-scheduled pipeline with today excluded.
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "bronze-ingest"},
		Schedule: types.ScheduleConfig{
			Cron:     "0 6 * * *",
			Timezone: "UTC",
			Exclude: &types.ExclusionConfig{
				Dates: []string{today},
			},
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo hello"}},
	}
	seedConfig(mock, cfg)

	// No TRIGGER# for today, but should be skipped because of exclusion.
	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	assert.Empty(t, ebMock.events, "expected no EventBridge events for calendar-excluded day")
}

func TestWatchdog_HourlyTriggerRows_NoMissedAlert(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Seed an hourly cron-scheduled pipeline config.
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "bronze-cdr"},
		Schedule: types.ScheduleConfig{
			Cron:       "10 * * * *",
			Timezone:   "UTC",
			Evaluation: types.EvaluationWindow{Window: "5m", Interval: "1m"},
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "http", Config: map[string]interface{}{"url": "http://example.com"}},
	}
	seedConfig(mock, cfg)

	// Seed per-hour TRIGGER# rows (as created by per-hour execution model).
	today := time.Now().Format("2006-01-02")
	seedTriggerRow(mock, "bronze-cdr", today+"T00", types.TriggerStatusRunning, time.Now().Add(12*time.Hour).Unix())
	seedTriggerRow(mock, "bronze-cdr", today+"T01", types.TriggerStatusRunning, time.Now().Add(12*time.Hour).Unix())

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	// No SCHEDULE_MISSED alert — per-hour triggers satisfy the daily check.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	assert.Empty(t, ebMock.events, "expected no EventBridge events when per-hour triggers exist")
}

func TestWatchdog_StreamTriggered_NoMissedCheck(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Seed a stream-triggered pipeline config (no cron).
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "gold-revenue"},
		Schedule: types.ScheduleConfig{
			Trigger: &types.TriggerCondition{
				Key:   "upstream-complete",
				Check: types.CheckEquals,
				Field: "status",
				Value: "ready",
			},
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo hello"}},
	}
	seedConfig(mock, cfg)

	// No TRIGGER# — but this is stream-triggered, not cron, so no missed schedule alert.
	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	assert.Empty(t, ebMock.events, "expected no EventBridge events for stream-triggered pipeline")
}

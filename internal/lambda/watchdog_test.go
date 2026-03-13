package lambda_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	schedulerTypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// seedTriggerRow inserts a running TRIGGER# row with TTL into the mock.
func seedTriggerRow(mock *mockDDB, pipelineID, date string, ttl int64) {
	item := map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("cron", date)},
		"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusRunning},
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
	seedTriggerRow(mock, "gold-revenue", "2026-03-01", pastTTL)

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
	seedTriggerRow(mock, "gold-revenue", "2026-03-01", futureTTL)

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
	seedTriggerRow(mock, "bronze-ingest", today, time.Now().Add(12*time.Hour).Unix())

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
	seedTriggerRow(mock, "bronze-cdr", today+"T00", time.Now().Add(12*time.Hour).Unix())
	seedTriggerRow(mock, "bronze-cdr", today+"T01", time.Now().Add(12*time.Hour).Unix())

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

// ---------------------------------------------------------------------------
// Sensor trigger reconciliation tests
// ---------------------------------------------------------------------------

func TestWatchdog_Reconcile_TriggersRecovery(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, ebMock := testDeps(mock)

	// Seed a sensor-triggered pipeline config.
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

	// Seed sensor that meets the trigger condition.
	seedSensor(mock, "gold-revenue", "upstream-complete", map[string]interface{}{
		"status": "ready",
		"date":   "2026-03-04",
	})

	// No trigger lock — reconciliation should recover this trigger.
	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	// Expect 1 SFN execution for the recovered trigger.
	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	require.Len(t, sfnMock.executions, 1, "expected one SFN execution for recovered trigger")

	// Expect 1 TRIGGER_RECOVERED event.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	var recoveredCount int
	for _, ev := range ebMock.events {
		if *ev.Entries[0].DetailType != string(types.EventTriggerRecovered) {
			continue
		}
		recoveredCount++
	}
	assert.Equal(t, 1, recoveredCount, "expected one TRIGGER_RECOVERED event")
}

func TestWatchdog_Reconcile_SkipsAlreadyTriggered(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, ebMock := testDeps(mock)

	// Seed a sensor-triggered pipeline config.
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

	// Seed sensor that meets the trigger condition.
	seedSensor(mock, "gold-revenue", "upstream-complete", map[string]interface{}{
		"status": "ready",
		"date":   "2026-03-04",
	})

	// Seed existing trigger lock — already triggered for today.
	seedTriggerLock(mock, "gold-revenue", "2026-03-04")

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	// Expect no SFN executions — lock already held.
	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Empty(t, sfnMock.executions, "expected no SFN executions when trigger lock already held")

	// Expect no TRIGGER_RECOVERED events.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	for _, ev := range ebMock.events {
		assert.NotEqual(t, string(types.EventTriggerRecovered), *ev.Entries[0].DetailType,
			"should not publish TRIGGER_RECOVERED when lock exists")
	}
}

func TestWatchdog_Reconcile_ConditionNotMet(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, ebMock := testDeps(mock)

	// Seed a sensor-triggered pipeline config expecting status=ready.
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

	// Seed sensor with status=pending (does NOT match trigger condition).
	seedSensor(mock, "gold-revenue", "upstream-complete", map[string]interface{}{
		"status": "pending",
		"date":   "2026-03-04",
	})

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	// Expect no SFN executions — condition not met.
	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Empty(t, sfnMock.executions, "expected no SFN executions when trigger condition not met")

	// Expect no TRIGGER_RECOVERED events.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	for _, ev := range ebMock.events {
		assert.NotEqual(t, string(types.EventTriggerRecovered), *ev.Entries[0].DetailType,
			"should not publish TRIGGER_RECOVERED when condition not met")
	}
}

func TestWatchdog_Reconcile_CalendarExcluded(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, ebMock := testDeps(mock)

	todayUTC := time.Now().UTC().Format("2006-01-02")

	// Seed a sensor-triggered pipeline with today excluded.
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "gold-revenue"},
		Schedule: types.ScheduleConfig{
			Trigger: &types.TriggerCondition{
				Key:   "upstream-complete",
				Check: types.CheckEquals,
				Field: "status",
				Value: "ready",
			},
			Timezone: "UTC",
			Exclude: &types.ExclusionConfig{
				Dates: []string{todayUTC},
			},
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo hello"}},
	}
	seedConfig(mock, cfg)

	// Seed sensor that meets the trigger condition.
	seedSensor(mock, "gold-revenue", "upstream-complete", map[string]interface{}{
		"status": "ready",
		"date":   todayUTC,
	})

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	// Expect no SFN executions — today is excluded.
	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Empty(t, sfnMock.executions, "expected no SFN executions when calendar-excluded")

	// Expect no TRIGGER_RECOVERED events.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	for _, ev := range ebMock.events {
		assert.NotEqual(t, string(types.EventTriggerRecovered), *ev.Entries[0].DetailType,
			"should not publish TRIGGER_RECOVERED on excluded day")
	}
}

func TestWatchdog_Reconcile_PerHour_MultipleRecoveries(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, ebMock := testDeps(mock)

	// Seed a sensor-triggered pipeline with trigger key "hourly-status".
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "bronze-cdr"},
		Schedule: types.ScheduleConfig{
			Trigger: &types.TriggerCondition{
				Key:   "hourly-status",
				Check: types.CheckEquals,
				Field: "status",
				Value: "ready",
			},
			Evaluation: types.EvaluationWindow{Window: "5m", Interval: "1m"},
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo hello"}},
	}
	seedConfig(mock, cfg)

	// Seed TWO per-hour sensors that prefix-match the trigger key.
	seedSensor(mock, "bronze-cdr", "hourly-status#2026-03-04T10", map[string]interface{}{
		"status": "ready",
		"date":   "2026-03-04",
		"hour":   "10",
	})
	seedSensor(mock, "bronze-cdr", "hourly-status#2026-03-04T11", map[string]interface{}{
		"status": "ready",
		"date":   "2026-03-04",
		"hour":   "11",
	})

	// No trigger locks — both should be recovered.
	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	// Expect 2 SFN executions (one per hour).
	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Len(t, sfnMock.executions, 2, "expected two SFN executions for two per-hour recoveries")

	// Expect 2 TRIGGER_RECOVERED events.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	var recoveredCount int
	for _, ev := range ebMock.events {
		if *ev.Entries[0].DetailType != string(types.EventTriggerRecovered) {
			continue
		}
		recoveredCount++
	}
	assert.Equal(t, 2, recoveredCount, "expected two TRIGGER_RECOVERED events")
}

func TestWatchdog_Reconcile_PerHour_PartiallyTriggered(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, ebMock := testDeps(mock)

	// Seed a sensor-triggered pipeline with trigger key "hourly-status".
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "bronze-cdr"},
		Schedule: types.ScheduleConfig{
			Trigger: &types.TriggerCondition{
				Key:   "hourly-status",
				Check: types.CheckEquals,
				Field: "status",
				Value: "ready",
			},
			Evaluation: types.EvaluationWindow{Window: "5m", Interval: "1m"},
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo hello"}},
	}
	seedConfig(mock, cfg)

	// Seed TWO per-hour sensors that prefix-match the trigger key.
	seedSensor(mock, "bronze-cdr", "hourly-status#2026-03-04T10", map[string]interface{}{
		"status": "ready",
		"date":   "2026-03-04",
		"hour":   "10",
	})
	seedSensor(mock, "bronze-cdr", "hourly-status#2026-03-04T11", map[string]interface{}{
		"status": "ready",
		"date":   "2026-03-04",
		"hour":   "11",
	})

	// Seed trigger lock for T10 — only T11 should be recovered.
	seedTriggerLock(mock, "bronze-cdr", "2026-03-04T10")

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	// Expect 1 SFN execution (only T11).
	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Len(t, sfnMock.executions, 1, "expected one SFN execution for T11 recovery only")

	// Expect 1 TRIGGER_RECOVERED event.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	var recoveredCount int
	for _, ev := range ebMock.events {
		if *ev.Entries[0].DetailType != string(types.EventTriggerRecovered) {
			continue
		}
		recoveredCount++
	}
	assert.Equal(t, 1, recoveredCount, "expected one TRIGGER_RECOVERED event for T11 only")
}

func TestWatchdog_MissedSchedule_SkipsPreDeployment(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Seed an hourly cron-scheduled pipeline.
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

	// Compute last :10 fire in UTC (matches production code's timezone handling).
	nowUTC := time.Now().UTC()
	lastFire := time.Date(nowUTC.Year(), nowUTC.Month(), nowUTC.Day(), nowUTC.Hour(), 10, 0, 0, time.UTC)
	if lastFire.After(nowUTC) {
		lastFire = lastFire.Add(-time.Hour)
	}
	// StartedAt is AFTER the last :10 fire — should suppress the alert.
	d.StartedAt = lastFire.Add(1 * time.Minute)

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	// No SCHEDULE_MISSED alert — the :10 fire was before deployment.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	for _, ev := range ebMock.events {
		assert.NotEqual(t, string(types.EventScheduleMissed), *ev.Entries[0].DetailType,
			"should not publish SCHEDULE_MISSED for pre-deployment schedule")
	}
}

func TestWatchdog_MissedSchedule_AlertsPostDeployment(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Seed an hourly cron-scheduled pipeline.
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

	// Compute last :10 fire in UTC (matches production code's timezone handling).
	nowUTC := time.Now().UTC()
	lastFire := time.Date(nowUTC.Year(), nowUTC.Month(), nowUTC.Day(), nowUTC.Hour(), 10, 0, 0, time.UTC)
	if lastFire.After(nowUTC) {
		lastFire = lastFire.Add(-time.Hour)
	}
	// StartedAt is BEFORE the last :10 fire — should alert.
	d.StartedAt = lastFire.Add(-1 * time.Minute)

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	// Should have a SCHEDULE_MISSED alert.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	var missedCount int
	for _, ev := range ebMock.events {
		if *ev.Entries[0].DetailType != string(types.EventScheduleMissed) {
			continue
		}
		missedCount++
	}
	assert.Equal(t, 1, missedCount, "expected one SCHEDULE_MISSED event for post-deployment schedule")
}

func TestWatchdog_ScheduleSLAAlerts_CreatesSchedules(t *testing.T) {
	mock := newMockDDB()
	d, _, _ := testDeps(mock)
	schedMock := &mockScheduler{}
	d.Scheduler = schedMock
	d.SLAMonitorARN = "arn:aws:lambda:us-east-1:123:function:sla-monitor"
	d.SchedulerRoleARN = "arn:aws:iam::123:role/scheduler-role"
	d.SchedulerGroupName = "interlock-sla"

	// Use a daily absolute deadline ("02:00") — handleSLACalculate rolls it
	// forward when past, so breach is always in the future regardless of
	// when this test runs. Hourly ":MM" deadlines are time-dependent and
	// would fail if the previous hour's breach is already past.
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "silver-cdr-day"},
		Schedule: types.ScheduleConfig{
			Cron:       "0 2 * * *",
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		SLA: &types.SLAConfig{
			Deadline:         "02:00",
			ExpectedDuration: "30m",
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "glue", Config: map[string]interface{}{"jobName": "test"}},
	}
	seedConfig(mock, cfg)

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	schedMock.mu.Lock()
	defer schedMock.mu.Unlock()
	assert.Len(t, schedMock.created, 2, "expected 2 SLA schedules (warning + breach)")

	for _, s := range schedMock.created {
		assert.Contains(t, *s.Name, "silver-cdr-day")
	}
}

func TestWatchdog_ScheduleSLAAlerts_NoSLAConfig_Skips(t *testing.T) {
	mock := newMockDDB()
	d, _, _ := testDeps(mock)
	schedMock := &mockScheduler{}
	d.Scheduler = schedMock
	d.SchedulerGroupName = "interlock-sla"

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "bronze-ingest"},
		Schedule: types.ScheduleConfig{
			Cron:       "0 6 * * *",
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo hello"}},
	}
	seedConfig(mock, cfg)

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	schedMock.mu.Lock()
	defer schedMock.mu.Unlock()
	assert.Empty(t, schedMock.created, "expected no SLA schedules for pipeline without SLA config")
}

func TestWatchdog_ScheduleSLAAlerts_ConflictSkips(t *testing.T) {
	mock := newMockDDB()
	d, _, _ := testDeps(mock)
	schedMock := &mockScheduler{
		createErr: &schedulerTypes.ConflictException{Message: strPtr("already exists")},
	}
	d.Scheduler = schedMock
	d.SLAMonitorARN = "arn:aws:lambda:us-east-1:123:function:sla-monitor"
	d.SchedulerRoleARN = "arn:aws:iam::123:role/scheduler-role"
	d.SchedulerGroupName = "interlock-sla"

	// Use daily deadline to avoid time-dependent breach-past skip.
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "silver-cdr-day"},
		Schedule: types.ScheduleConfig{
			Cron:       "0 2 * * *",
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		SLA: &types.SLAConfig{
			Deadline:         "02:00",
			ExpectedDuration: "30m",
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "glue", Config: map[string]interface{}{"jobName": "test"}},
	}
	seedConfig(mock, cfg)

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)
}

func TestWatchdog_ScheduleSLAAlerts_DailyPipeline(t *testing.T) {
	mock := newMockDDB()
	d, _, _ := testDeps(mock)
	schedMock := &mockScheduler{}
	d.Scheduler = schedMock
	d.SLAMonitorARN = "arn:aws:lambda:us-east-1:123:function:sla-monitor"
	d.SchedulerRoleARN = "arn:aws:iam::123:role/scheduler-role"
	d.SchedulerGroupName = "interlock-sla"

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "silver-cdr-day"},
		Schedule: types.ScheduleConfig{
			Cron:       "0 2 * * *",
			Timezone:   "UTC",
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		SLA: &types.SLAConfig{
			Deadline:         "02:00",
			ExpectedDuration: "30m",
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "glue", Config: map[string]interface{}{"jobName": "test-day"}},
	}
	seedConfig(mock, cfg)

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	schedMock.mu.Lock()
	defer schedMock.mu.Unlock()
	assert.Len(t, schedMock.created, 2, "expected 2 SLA schedules for daily pipeline")
}

func TestWatchdog_ScheduleSLAAlerts_NoSchedulerClient_Skips(t *testing.T) {
	mock := newMockDDB()
	d, _, _ := testDeps(mock)

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "silver-cdr-day"},
		Schedule: types.ScheduleConfig{
			Cron:       "0 2 * * *",
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		SLA: &types.SLAConfig{
			Deadline:         "02:00",
			ExpectedDuration: "30m",
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "glue", Config: map[string]interface{}{"jobName": "test"}},
	}
	seedConfig(mock, cfg)

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)
}

func TestWatchdog_ScheduleSLAAlerts_SkipsCompletedTrigger(t *testing.T) {
	mock := newMockDDB()
	d, _, _ := testDeps(mock)
	schedMock := &mockScheduler{}
	d.Scheduler = schedMock
	d.SLAMonitorARN = "arn:aws:lambda:us-east-1:123:function:sla-monitor"
	d.SchedulerRoleARN = "arn:aws:iam::123:role/scheduler-role"
	d.SchedulerGroupName = "interlock-sla"

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "silver-cdr-day"},
		Schedule: types.ScheduleConfig{
			Cron:       "0 2 * * *",
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		SLA: &types.SLAConfig{
			Deadline:         "02:00",
			ExpectedDuration: "30m",
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "glue", Config: map[string]interface{}{"jobName": "test"}},
	}
	seedConfig(mock, cfg)

	// Seed a COMPLETED trigger for the resolved date.
	today := time.Now().Format("2006-01-02")
	mock.putRaw(testControlTable, map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("silver-cdr-day")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("cron", today)},
		"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusCompleted},
	})

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	schedMock.mu.Lock()
	defer schedMock.mu.Unlock()
	assert.Empty(t, schedMock.created, "expected no SLA schedules for completed pipeline")
}

func TestWatchdog_ScheduleSLAAlerts_SkipsByJoblogNoTrigger(t *testing.T) {
	mock := newMockDDB()
	d, _, _ := testDeps(mock)
	schedMock := &mockScheduler{}
	d.Scheduler = schedMock
	d.SLAMonitorARN = "arn:aws:lambda:us-east-1:123:function:sla-monitor"
	d.SchedulerRoleARN = "arn:aws:iam::123:role/scheduler-role"
	d.SchedulerGroupName = "interlock-sla"

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "bronze-cdr"},
		Schedule: types.ScheduleConfig{
			Cron:       "5 * * * *",
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		SLA: &types.SLAConfig{
			Deadline:         ":30",
			ExpectedDuration: "10m",
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "glue", Config: map[string]interface{}{"jobName": "test"}},
	}
	seedConfig(mock, cfg)

	// No trigger row, but joblog has a success event (cron pipeline finished).
	prevHour := time.Now().Add(-time.Hour)
	date := prevHour.Format("2006-01-02") + "T" + fmt.Sprintf("%02d", prevHour.Hour())
	mock.putRaw("joblog", map[string]ddbtypes.AttributeValue{
		"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("bronze-cdr")},
		"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("cron", date, "1709280000000")},
		"event": &ddbtypes.AttributeValueMemberS{Value: types.JobEventSuccess},
	})

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	schedMock.mu.Lock()
	defer schedMock.mu.Unlock()
	assert.Empty(t, schedMock.created, "expected no SLA schedules when joblog shows completion")
}

func TestWatchdog_ScheduleSLAAlerts_SkipsFailedFinalTrigger(t *testing.T) {
	mock := newMockDDB()
	d, _, _ := testDeps(mock)
	schedMock := &mockScheduler{}
	d.Scheduler = schedMock
	d.SLAMonitorARN = "arn:aws:lambda:us-east-1:123:function:sla-monitor"
	d.SchedulerRoleARN = "arn:aws:iam::123:role/scheduler-role"
	d.SchedulerGroupName = "interlock-sla"

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "silver-cdr-day"},
		Schedule: types.ScheduleConfig{
			Cron:       "0 2 * * *",
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		SLA: &types.SLAConfig{
			Deadline:         "02:00",
			ExpectedDuration: "30m",
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "glue", Config: map[string]interface{}{"jobName": "test"}},
	}
	seedConfig(mock, cfg)

	// Seed a FAILED_FINAL trigger for the resolved date.
	today := time.Now().Format("2006-01-02")
	mock.putRaw(testControlTable, map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("silver-cdr-day")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("cron", today)},
		"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusFailedFinal},
	})

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	schedMock.mu.Lock()
	defer schedMock.mu.Unlock()
	assert.Empty(t, schedMock.created, "expected no SLA schedules for FAILED_FINAL pipeline")
}

// ---------------------------------------------------------------------------
// Stale trigger: TTL edge cases
// ---------------------------------------------------------------------------

func TestWatchdog_StaleTrigger_ZeroTTL_NoAlert(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Seed a TRIGGER# row with zero TTL (no explicit TTL set).
	seedTriggerRow(mock, "gold-revenue", "2026-03-01", 0)

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	// Zero TTL should not be considered stale — isStaleTrigger returns false.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	for _, ev := range ebMock.events {
		assert.NotEqual(t, string(types.EventSFNTimeout), *ev.Entries[0].DetailType,
			"zero TTL should not trigger SFN_TIMEOUT")
	}
}

func TestWatchdog_StaleTrigger_TTLExactlyNow_NoAlert(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Seed with TTL slightly in the future (isStaleTrigger uses > not >=).
	// Use +5s buffer to avoid flaky races between seed and internal time.Now().
	seedTriggerRow(mock, "gold-revenue", "2026-03-01", time.Now().Add(5*time.Second).Unix())

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	// TTL exactly at now: now.Unix() > tr.TTL is false (equal, not greater).
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	for _, ev := range ebMock.events {
		assert.NotEqual(t, string(types.EventSFNTimeout), *ev.Entries[0].DetailType,
			"TTL exactly at now should not trigger SFN_TIMEOUT")
	}
}

// ---------------------------------------------------------------------------
// Stale trigger: unparseable trigger records
// ---------------------------------------------------------------------------

func TestWatchdog_StaleTrigger_UnparseablePK_Skipped(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Seed a trigger with bad PK format (no PIPELINE# prefix).
	pastTTL := time.Now().Add(-1 * time.Hour).Unix()
	mock.putRaw(testControlTable, map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: "BADPREFIX#gold-revenue"},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("cron", "2026-03-01")},
		"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusRunning},
		"ttl":    &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", pastTTL)},
	})

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	// Unparseable PK should be skipped — no events published.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	for _, ev := range ebMock.events {
		assert.NotEqual(t, string(types.EventSFNTimeout), *ev.Entries[0].DetailType,
			"unparseable PK should be skipped")
	}
}

func TestWatchdog_StaleTrigger_UnparseableSK_Skipped(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Seed a trigger with bad SK format (TRIGGER# but no second # delimiter).
	pastTTL := time.Now().Add(-1 * time.Hour).Unix()
	mock.putRaw(testControlTable, map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("gold-revenue")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: "TRIGGER#nodatepart"},
		"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusRunning},
		"ttl":    &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", pastTTL)},
	})

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	// Unparseable SK should be skipped — no events published.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	for _, ev := range ebMock.events {
		assert.NotEqual(t, string(types.EventSFNTimeout), *ev.Entries[0].DetailType,
			"unparseable SK should be skipped")
	}
}

func TestWatchdog_StaleTrigger_NoTriggerPrefix_Skipped(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Seed a trigger with SK that doesn't start with TRIGGER#.
	pastTTL := time.Now().Add(-1 * time.Hour).Unix()
	mock.putRaw(testControlTable, map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("gold-revenue")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: "SENSOR#upstream"},
		"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusRunning},
		"ttl":    &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", pastTTL)},
	})

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	// Wrong SK prefix should be skipped — no SFN_TIMEOUT events.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	for _, ev := range ebMock.events {
		assert.NotEqual(t, string(types.EventSFNTimeout), *ev.Entries[0].DetailType,
			"wrong SK prefix should be skipped")
	}
}

// ---------------------------------------------------------------------------
// Stale trigger: detail map verification
// ---------------------------------------------------------------------------

func TestWatchdog_StaleTrigger_DetailFields(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	pastTTL := time.Now().Add(-1 * time.Hour).Unix()
	seedTriggerRow(mock, "gold-revenue", "2026-03-01", pastTTL)

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	require.Len(t, ebMock.events, 1)

	detailJSON := *ebMock.events[0].Entries[0].Detail
	var evt types.InterlockEvent
	require.NoError(t, json.Unmarshal([]byte(detailJSON), &evt))

	require.NotNil(t, evt.Detail)
	assert.Equal(t, "watchdog", evt.Detail["source"])
	assert.Contains(t, evt.Detail["actionHint"], "step function exceeded TTL")
	assert.NotEmpty(t, evt.Detail["ttlExpired"], "ttlExpired should be present when TTL > 0")
}

// ---------------------------------------------------------------------------
// Missed schedule: detail map verification
// ---------------------------------------------------------------------------

func TestWatchdog_MissedSchedule_DetailFields(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)
	d.NowFunc = func() time.Time { return time.Date(2026, 3, 10, 0, 30, 0, 0, time.UTC) }
	// Push StartedAt far into the past so the pre-deployment filter never skips.
	d.StartedAt = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "bronze-ingest"},
		Schedule: types.ScheduleConfig{
			Cron:       "0 0 * * *",
			Timezone:   "UTC",
			Time:       "00:01",
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo hello"}},
	}
	seedConfig(mock, cfg)

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()

	var missedEvent *types.InterlockEvent
	for _, ev := range ebMock.events {
		if *ev.Entries[0].DetailType != string(types.EventScheduleMissed) {
			continue
		}
		detailJSON := *ev.Entries[0].Detail
		var evt types.InterlockEvent
		require.NoError(t, json.Unmarshal([]byte(detailJSON), &evt))
		missedEvent = &evt
		break
	}

	require.NotNil(t, missedEvent, "expected a SCHEDULE_MISSED event to be published")
	require.NotNil(t, missedEvent.Detail)
	assert.Equal(t, "watchdog", missedEvent.Detail["source"])
	assert.Equal(t, "0 0 * * *", missedEvent.Detail["cron"])
	assert.Contains(t, missedEvent.Detail["actionHint"], "cron")
	assert.Equal(t, "00:01", missedEvent.Detail["expectedTime"])
}

// ---------------------------------------------------------------------------
// Missed schedule: Schedule.Time before current time
// ---------------------------------------------------------------------------

func TestWatchdog_MissedSchedule_BeforeScheduleTime_NoAlert(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Use a schedule time far in the future (23:59 UTC) so the test always
	// runs before it. The cron fires at :10 of any hour, but Schedule.Time
	// says the pipeline shouldn't start until 23:59.
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "bronze-late"},
		Schedule: types.ScheduleConfig{
			Cron:       "10 * * * *",
			Timezone:   "UTC",
			Time:       "23:59",
			Evaluation: types.EvaluationWindow{Window: "5m", Interval: "1m"},
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo hello"}},
	}
	seedConfig(mock, cfg)

	now := time.Now().UTC()
	// Only run this test when we're before 23:59 UTC.
	if now.Hour() == 23 && now.Minute() >= 59 {
		t.Skip("skipping: test requires running before 23:59 UTC")
	}

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	for _, ev := range ebMock.events {
		if *ev.Entries[0].DetailType != string(types.EventScheduleMissed) {
			continue
		}
		detailJSON := *ev.Entries[0].Detail
		var evt types.InterlockEvent
		_ = json.Unmarshal([]byte(detailJSON), &evt)
		if evt.PipelineID == "bronze-late" {
			t.Error("should not publish SCHEDULE_MISSED when current time is before Schedule.Time")
		}
	}
}

// ---------------------------------------------------------------------------
// Sensor trigger reconciliation: error paths
// ---------------------------------------------------------------------------

func TestWatchdog_Reconcile_CronPipeline_SkipsReconciliation(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	// Cron pipeline should be skipped by reconciliation (only sensor-triggered).
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "bronze-cron"},
		Schedule: types.ScheduleConfig{
			Cron:       "0 6 * * *",
			Timezone:   "UTC",
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo hello"}},
	}
	seedConfig(mock, cfg)

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Empty(t, sfnMock.executions, "cron pipeline should not trigger reconciliation")
}

func TestWatchdog_Reconcile_SFNError_Continues(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	// Make SFN always fail.
	sfnMock.err = errors.New("execution limit exceeded")

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

	seedSensor(mock, "gold-revenue", "upstream-complete", map[string]interface{}{
		"status": "ready",
		"date":   "2026-03-04",
	})

	// HandleWatchdog should not return error even when SFN fails.
	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)
}

func TestWatchdog_Reconcile_NoTriggerCondition_Skips(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	// Pipeline with nil trigger and no cron (unusual but possible).
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "manual-pipeline"},
		Schedule: types.ScheduleConfig{
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo hello"}},
	}
	seedConfig(mock, cfg)

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Empty(t, sfnMock.executions, "pipeline with no trigger should be skipped by reconciliation")
}

// ---------------------------------------------------------------------------
// Reconcile: joblog terminal guard
// ---------------------------------------------------------------------------

// seedJoblogEntry inserts a joblog event into the mock.
func seedJoblogEntry(mock *mockDDB, pipelineID, event string) {
	ts := fmt.Sprintf("%d", time.Now().UnixMilli())
	sk := types.JobSK("stream", "2026-03-04", ts)
	mock.putRaw("joblog", map[string]ddbtypes.AttributeValue{
		"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
		"SK":    &ddbtypes.AttributeValueMemberS{Value: sk},
		"event": &ddbtypes.AttributeValueMemberS{Value: event},
	})
}

func TestWatchdog_Reconcile_SkipsTerminalSuccess(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, ebMock := testDeps(mock)

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "gold-revenue"},
		Schedule: types.ScheduleConfig{
			Trigger: &types.TriggerCondition{
				Key: "upstream-complete", Check: types.CheckEquals, Field: "status", Value: "ready",
			},
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo hello"}},
	}
	seedConfig(mock, cfg)
	seedSensor(mock, "gold-revenue", "upstream-complete", map[string]interface{}{
		"status": "ready", "date": "2026-03-04",
	})

	// No trigger row (simulates TTL expiry), but joblog has terminal success.
	seedJoblogEntry(mock, "gold-revenue", types.JobEventSuccess)

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Empty(t, sfnMock.executions, "reconcile should skip pipeline with terminal success joblog")

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	for _, ev := range ebMock.events {
		assert.NotEqual(t, string(types.EventTriggerRecovered), *ev.Entries[0].DetailType,
			"should not publish TRIGGER_RECOVERED for completed pipeline")
	}
}

func TestWatchdog_Reconcile_SkipsTerminalFail(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "silver-transform"},
		Schedule: types.ScheduleConfig{
			Trigger: &types.TriggerCondition{
				Key: "upstream-complete", Check: types.CheckEquals, Field: "status", Value: "ready",
			},
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo hello"}},
	}
	seedConfig(mock, cfg)
	seedSensor(mock, "silver-transform", "upstream-complete", map[string]interface{}{
		"status": "ready", "date": "2026-03-04",
	})

	seedJoblogEntry(mock, "silver-transform", types.JobEventFail)

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Empty(t, sfnMock.executions, "reconcile should skip pipeline with terminal fail joblog")
}

func TestWatchdog_Reconcile_SkipsTerminalTimeout(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, _ := testDeps(mock)

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "gold-revenue"},
		Schedule: types.ScheduleConfig{
			Trigger: &types.TriggerCondition{
				Key: "upstream-complete", Check: types.CheckEquals, Field: "status", Value: "ready",
			},
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo hello"}},
	}
	seedConfig(mock, cfg)
	seedSensor(mock, "gold-revenue", "upstream-complete", map[string]interface{}{
		"status": "ready", "date": "2026-03-04",
	})

	seedJoblogEntry(mock, "gold-revenue", types.JobEventTimeout)

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Empty(t, sfnMock.executions, "reconcile should skip pipeline with terminal timeout joblog")
}

func TestWatchdog_Reconcile_ProceedsOnNonTerminalJoblog(t *testing.T) {
	mock := newMockDDB()
	d, sfnMock, ebMock := testDeps(mock)

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "gold-revenue"},
		Schedule: types.ScheduleConfig{
			Trigger: &types.TriggerCondition{
				Key: "upstream-complete", Check: types.CheckEquals, Field: "status", Value: "ready",
			},
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo hello"}},
	}
	seedConfig(mock, cfg)
	seedSensor(mock, "gold-revenue", "upstream-complete", map[string]interface{}{
		"status": "ready", "date": "2026-03-04",
	})

	// Non-terminal joblog event should NOT prevent recovery.
	seedJoblogEntry(mock, "gold-revenue", types.JobEventInfraTriggerFailure)

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	sfnMock.mu.Lock()
	defer sfnMock.mu.Unlock()
	assert.Len(t, sfnMock.executions, 1, "reconcile should proceed when joblog has only non-terminal events")

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	var recovered int
	for _, ev := range ebMock.events {
		if *ev.Entries[0].DetailType != string(types.EventTriggerRecovered) {
			continue
		}
		recovered++
	}
	assert.Equal(t, 1, recovered, "should publish TRIGGER_RECOVERED for non-terminal joblog")
}

// ---------------------------------------------------------------------------
// SLA scheduling: error paths
// ---------------------------------------------------------------------------

func TestWatchdog_ScheduleSLAAlerts_InvalidSLAConfig_Continues(t *testing.T) {
	mock := newMockDDB()
	d, _, _ := testDeps(mock)
	schedMock := &mockScheduler{}
	d.Scheduler = schedMock
	d.SLAMonitorARN = "arn:aws:lambda:us-east-1:123:function:sla-monitor"
	d.SchedulerRoleARN = "arn:aws:iam::123:role/scheduler-role"
	d.SchedulerGroupName = "interlock-sla"

	// Bad SLA config: invalid deadline format.
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "bad-sla-pipe"},
		Schedule: types.ScheduleConfig{
			Cron:       "0 2 * * *",
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		SLA: &types.SLAConfig{
			Deadline:         "INVALID",
			ExpectedDuration: "30m",
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "glue", Config: map[string]interface{}{"jobName": "test"}},
	}
	seedConfig(mock, cfg)

	// Should not return error — logs and continues.
	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	schedMock.mu.Lock()
	defer schedMock.mu.Unlock()
	assert.Empty(t, schedMock.created, "invalid SLA config should not create schedules")
}

func TestWatchdog_ScheduleSLAAlerts_NonConflictError_Continues(t *testing.T) {
	mock := newMockDDB()
	d, _, _ := testDeps(mock)
	schedMock := &mockScheduler{
		createErr: errors.New("service unavailable"),
	}
	d.Scheduler = schedMock
	d.SLAMonitorARN = "arn:aws:lambda:us-east-1:123:function:sla-monitor"
	d.SchedulerRoleARN = "arn:aws:iam::123:role/scheduler-role"
	d.SchedulerGroupName = "interlock-sla"

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "silver-cdr-day"},
		Schedule: types.ScheduleConfig{
			Cron:       "0 2 * * *",
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		SLA: &types.SLAConfig{
			Deadline:         "02:00",
			ExpectedDuration: "30m",
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "glue", Config: map[string]interface{}{"jobName": "test"}},
	}
	seedConfig(mock, cfg)

	// Non-conflict error should not return error — logs and continues.
	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)
}

func TestWatchdog_ScheduleSLAAlerts_HourlyPipeline_UsesCompositeDate(t *testing.T) {
	mock := newMockDDB()
	d, _, _ := testDeps(mock)
	schedMock := &mockScheduler{}
	d.Scheduler = schedMock
	d.SLAMonitorARN = "arn:aws:lambda:us-east-1:123:function:sla-monitor"
	d.SchedulerRoleARN = "arn:aws:iam::123:role/scheduler-role"
	d.SchedulerGroupName = "interlock-sla"

	// Fix time at 14:10 UTC so the :30 deadline (14:30) is still in the future
	// and the cron "5 * * * *" last fired at 14:05 (after StartedAt).
	fixedNow := time.Date(2026, 3, 7, 14, 10, 0, 0, time.UTC)
	d.NowFunc = func() time.Time { return fixedNow }
	d.StartedAt = fixedNow.Add(-5 * time.Minute)

	// Hourly pipeline with relative deadline — resolveWatchdogSLADate should
	// produce a composite date like "2026-03-07T13".
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "bronze-cdr"},
		Schedule: types.ScheduleConfig{
			Cron:       "5 * * * *",
			Evaluation: types.EvaluationWindow{Window: "5m", Interval: "1m"},
		},
		SLA: &types.SLAConfig{
			Deadline:         ":30",
			ExpectedDuration: "10m",
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "glue", Config: map[string]interface{}{"jobName": "test"}},
	}
	seedConfig(mock, cfg)

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	schedMock.mu.Lock()
	defer schedMock.mu.Unlock()

	require.NotEmpty(t, schedMock.created, "expected SLA schedules to be created for hourly pipeline")
	// Verify the schedule name contains a composite date (has T).
	name := *schedMock.created[0].Name
	assert.Contains(t, name, "T", "hourly pipeline SLA schedule name should contain composite date")
}

func TestWatchdog_ScheduleSLAAlerts_CalendarExcluded_Skips(t *testing.T) {
	mock := newMockDDB()
	d, _, _ := testDeps(mock)
	schedMock := &mockScheduler{}
	d.Scheduler = schedMock
	d.SLAMonitorARN = "arn:aws:lambda:us-east-1:123:function:sla-monitor"
	d.SchedulerRoleARN = "arn:aws:iam::123:role/scheduler-role"
	d.SchedulerGroupName = "interlock-sla"

	todayUTC := time.Now().UTC().Format("2006-01-02")

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "silver-cdr-day"},
		Schedule: types.ScheduleConfig{
			Cron:     "0 2 * * *",
			Timezone: "UTC",
			Exclude: &types.ExclusionConfig{
				Dates: []string{todayUTC},
			},
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		SLA: &types.SLAConfig{
			Deadline:         "02:00",
			ExpectedDuration: "30m",
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "glue", Config: map[string]interface{}{"jobName": "test"}},
	}
	seedConfig(mock, cfg)

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	schedMock.mu.Lock()
	defer schedMock.mu.Unlock()
	assert.Empty(t, schedMock.created, "calendar-excluded day should skip SLA scheduling")
}

func TestWatchdog_ScheduleSLAAlerts_SensorTriggered_CreatesSLASchedules(t *testing.T) {
	mock := newMockDDB()
	d, _, _ := testDeps(mock)
	schedMock := &mockScheduler{}
	d.Scheduler = schedMock
	d.SLAMonitorARN = "arn:aws:lambda:us-east-1:123:function:sla-monitor"
	d.SchedulerRoleARN = "arn:aws:iam::123:role/scheduler-role"
	d.SchedulerGroupName = "interlock-sla"

	// Fix time at 14:10 UTC — :30 deadline (T14:30 breach) is in the future.
	fixedNow := time.Date(2026, 3, 9, 14, 10, 0, 0, time.UTC)
	d.NowFunc = func() time.Time { return fixedNow }
	d.StartedAt = fixedNow.Add(-5 * time.Minute)

	// Sensor-triggered pipeline (no cron) with SLA — should get proactive
	// SLA scheduling from the watchdog.
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "silver-seq-hour"},
		Schedule: types.ScheduleConfig{
			Trigger: &types.TriggerCondition{
				Key:   "audit-result",
				Check: "equals",
				Field: "match",
				Value: true,
			},
			Evaluation: types.EvaluationWindow{Window: "15m", Interval: "2m"},
		},
		SLA: &types.SLAConfig{
			Deadline:         ":30",
			ExpectedDuration: "10m",
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "glue", Config: map[string]interface{}{"jobName": "seq-agg-hour"}},
	}
	seedConfig(mock, cfg)

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	schedMock.mu.Lock()
	defer schedMock.mu.Unlock()
	assert.Len(t, schedMock.created, 2, "sensor-triggered pipeline should get proactive SLA scheduling (warning + breach)")
	for _, s := range schedMock.created {
		assert.Contains(t, *s.Name, "silver-seq-hour")
	}
}

func TestWatchdog_ScheduleSLAAlerts_SensorDeadlineExpired_WritesFAILEDFINAL(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)
	schedMock := &mockScheduler{}
	d.Scheduler = schedMock
	d.SLAMonitorARN = "arn:aws:lambda:us-east-1:123:function:sla-monitor"
	d.SchedulerRoleARN = "arn:aws:iam::123:role/scheduler-role"
	d.SchedulerGroupName = "interlock-sla"

	// Fix time at 14:50 UTC — both SLA breach (:30 → T14:30) and trigger
	// deadline (:45 → T14:45) are in the past.
	fixedNow := time.Date(2026, 3, 9, 14, 50, 0, 0, time.UTC)
	d.NowFunc = func() time.Time { return fixedNow }
	d.StartedAt = fixedNow.Add(-5 * time.Minute)

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "silver-seq-hour"},
		Schedule: types.ScheduleConfig{
			Trigger: &types.TriggerCondition{
				Key:      "audit-result",
				Check:    "equals",
				Field:    "match",
				Value:    true,
				Deadline: ":45",
			},
			Evaluation: types.EvaluationWindow{Window: "15m", Interval: "2m"},
		},
		SLA: &types.SLAConfig{
			Deadline:         ":30",
			ExpectedDuration: "10m",
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "glue", Config: map[string]interface{}{"jobName": "seq-agg-hour"}},
	}
	seedConfig(mock, cfg)

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	// Verify FAILED_FINAL trigger was written for the resolved date.
	// resolveWatchdogSLADate at 14:50 with ":30" deadline → "2026-03-09T13"
	triggerKey := ddbItemKey(testControlTable,
		types.PipelinePK("silver-seq-hour"),
		types.TriggerSK("stream", "2026-03-09T13"))
	mock.mu.Lock()
	item, ok := mock.items[triggerKey]
	mock.mu.Unlock()
	require.True(t, ok, "FAILED_FINAL trigger row should have been written")
	statusVal := item["status"].(*ddbtypes.AttributeValueMemberS).Value
	assert.Equal(t, types.TriggerStatusFailedFinal, statusVal)

	// Verify SENSOR_DEADLINE_EXPIRED event was published.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	var found bool
	for _, ev := range ebMock.events {
		if *ev.Entries[0].DetailType != string(types.EventSensorDeadlineExpired) {
			continue
		}
		found = true
		break
	}
	assert.True(t, found, "expected SENSOR_DEADLINE_EXPIRED event")
}

func TestWatchdog_ScheduleSLAAlerts_SensorDeadlineExpired_SkipsIfTriggerExists(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)
	schedMock := &mockScheduler{}
	d.Scheduler = schedMock
	d.SLAMonitorARN = "arn:aws:lambda:us-east-1:123:function:sla-monitor"
	d.SchedulerRoleARN = "arn:aws:iam::123:role/scheduler-role"
	d.SchedulerGroupName = "interlock-sla"

	// Fix time at 14:50 UTC — trigger deadline (:45) is past.
	fixedNow := time.Date(2026, 3, 9, 14, 50, 0, 0, time.UTC)
	d.NowFunc = func() time.Time { return fixedNow }
	d.StartedAt = fixedNow.Add(-5 * time.Minute)

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "silver-seq-hour"},
		Schedule: types.ScheduleConfig{
			Trigger: &types.TriggerCondition{
				Key:      "audit-result",
				Check:    "equals",
				Field:    "match",
				Value:    true,
				Deadline: ":45",
			},
			Evaluation: types.EvaluationWindow{Window: "15m", Interval: "2m"},
		},
		SLA: &types.SLAConfig{
			Deadline:         ":30",
			ExpectedDuration: "10m",
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "glue", Config: map[string]interface{}{"jobName": "seq-agg-hour"}},
	}
	seedConfig(mock, cfg)

	// Seed a RUNNING trigger for the resolved date — pipeline started but is
	// still running. The deadline-expired logic should NOT write FAILED_FINAL.
	mock.putRaw(testControlTable, map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("silver-seq-hour")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("stream", "2026-03-09T13")},
		"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusRunning},
	})

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	// No SENSOR_DEADLINE_EXPIRED event — pipeline already has a trigger.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	for _, ev := range ebMock.events {
		assert.NotEqual(t, string(types.EventSensorDeadlineExpired), *ev.Entries[0].DetailType,
			"should not publish SENSOR_DEADLINE_EXPIRED when trigger exists")
	}
}

func TestWatchdog_ScheduleSLAAlerts_SensorDeadlineNotExpired_SchedulesSLAOnly(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)
	schedMock := &mockScheduler{}
	d.Scheduler = schedMock
	d.SLAMonitorARN = "arn:aws:lambda:us-east-1:123:function:sla-monitor"
	d.SchedulerRoleARN = "arn:aws:iam::123:role/scheduler-role"
	d.SchedulerGroupName = "interlock-sla"

	// Fix time at 14:10 UTC — SLA deadline :30 (breach T14:30) is future,
	// trigger deadline :45 (T14:45) is also future. Should create SLA
	// schedules but NOT write FAILED_FINAL.
	fixedNow := time.Date(2026, 3, 9, 14, 10, 0, 0, time.UTC)
	d.NowFunc = func() time.Time { return fixedNow }
	d.StartedAt = fixedNow.Add(-5 * time.Minute)

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "silver-seq-hour"},
		Schedule: types.ScheduleConfig{
			Trigger: &types.TriggerCondition{
				Key:      "audit-result",
				Check:    "equals",
				Field:    "match",
				Value:    true,
				Deadline: ":45",
			},
			Evaluation: types.EvaluationWindow{Window: "15m", Interval: "2m"},
		},
		SLA: &types.SLAConfig{
			Deadline:         ":30",
			ExpectedDuration: "10m",
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "glue", Config: map[string]interface{}{"jobName": "seq-agg-hour"}},
	}
	seedConfig(mock, cfg)

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	// SLA schedules should be created.
	schedMock.mu.Lock()
	defer schedMock.mu.Unlock()
	assert.Len(t, schedMock.created, 2, "expected SLA schedules (warning + breach)")

	// No FAILED_FINAL trigger should exist.
	triggerKey := ddbItemKey(testControlTable,
		types.PipelinePK("silver-seq-hour"),
		types.TriggerSK("stream", "2026-03-09T13"))
	mock.mu.Lock()
	_, ok := mock.items[triggerKey]
	mock.mu.Unlock()
	assert.False(t, ok, "no FAILED_FINAL trigger should exist when deadline not expired")

	// No SENSOR_DEADLINE_EXPIRED event.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	for _, ev := range ebMock.events {
		assert.NotEqual(t, string(types.EventSensorDeadlineExpired), *ev.Entries[0].DetailType)
	}
}

func TestWatchdog_ScheduleSLAAlerts_DailySensorDeadlineExpired_WritesFAILEDFINAL(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)
	schedMock := &mockScheduler{}
	d.Scheduler = schedMock
	d.SLAMonitorARN = "arn:aws:lambda:us-east-1:123:function:sla-monitor"
	d.SchedulerRoleARN = "arn:aws:iam::123:role/scheduler-role"
	d.SchedulerGroupName = "interlock-sla"

	// Fix time at 10:00 UTC — both SLA breach (08:00) and trigger deadline
	// (09:00) are in the past. Daily pipeline with absolute deadlines.
	fixedNow := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)
	d.NowFunc = func() time.Time { return fixedNow }
	d.StartedAt = fixedNow.Add(-5 * time.Minute)

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "silver-seq-day"},
		Schedule: types.ScheduleConfig{
			Trigger: &types.TriggerCondition{
				Key:      "daily-status",
				Check:    "equals",
				Field:    "all_hours_complete",
				Value:    true,
				Deadline: "09:00",
			},
			Evaluation: types.EvaluationWindow{Window: "30m", Interval: "5m"},
		},
		SLA: &types.SLAConfig{
			Deadline:         "08:00",
			ExpectedDuration: "30m",
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "glue", Config: map[string]interface{}{"jobName": "seq-agg-day"}},
	}
	seedConfig(mock, cfg)

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	// Verify FAILED_FINAL trigger for daily date (no composite T).
	triggerKey := ddbItemKey(testControlTable,
		types.PipelinePK("silver-seq-day"),
		types.TriggerSK("stream", "2026-03-09"))
	mock.mu.Lock()
	item, ok := mock.items[triggerKey]
	mock.mu.Unlock()
	require.True(t, ok, "FAILED_FINAL trigger row should have been written for daily pipeline")
	statusVal := item["status"].(*ddbtypes.AttributeValueMemberS).Value
	assert.Equal(t, types.TriggerStatusFailedFinal, statusVal)

	// Verify SENSOR_DEADLINE_EXPIRED event.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	var found bool
	for _, ev := range ebMock.events {
		if *ev.Entries[0].DetailType != string(types.EventSensorDeadlineExpired) {
			continue
		}
		found = true
		break
	}
	assert.True(t, found, "expected SENSOR_DEADLINE_EXPIRED event for daily pipeline")
}

// ---------------------------------------------------------------------------
// Post-run sensor absence detection tests
// ---------------------------------------------------------------------------

// postRunWatchdogConfig returns a pipeline config with PostRun rules for
// watchdog absence detection tests.
func postRunWatchdogConfig(sensorTimeout string) types.PipelineConfig {
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
		PostRun: &types.PostRunConfig{
			Rules: []types.ValidationRule{
				{Key: "quality-check", Check: types.CheckEquals, Field: "status", Value: "passed"},
			},
		},
	}
	if sensorTimeout != "" {
		cfg.PostRun.SensorTimeout = sensorTimeout
	}
	return cfg
}

// seedJobEventForPipeline inserts a job log record with a custom pipeline, schedule, and date.
func seedJobEventForPipeline(mock *mockDDB, pipelineID, schedule, date, timestamp, event string) {
	mock.putRaw("joblog", map[string]ddbtypes.AttributeValue{
		"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipelineID)},
		"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK(schedule, date, timestamp)},
		"event": &ddbtypes.AttributeValueMemberS{Value: event},
	})
}

func TestWatchdog_PostRunSensorMissing(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Fix the clock so today is deterministic.
	fixedNow := time.Date(2026, 3, 8, 14, 0, 0, 0, time.UTC)
	d.NowFunc = func() time.Time { return fixedNow }

	today := fixedNow.Format("2006-01-02")

	// Seed pipeline config with PostRun (1h timeout to trigger easily).
	cfg := postRunWatchdogConfig("1h")
	seedConfig(mock, cfg)

	// Seed COMPLETED trigger for today.
	seedTriggerWithStatus(mock, "gold-revenue", today, types.TriggerStatusCompleted)

	// Seed baseline (written at completion time).
	seedSensor(mock, "gold-revenue", "postrun-baseline#"+today, map[string]interface{}{
		"sensor_count": float64(100),
	})

	// Seed a success job event with timestamp 3h before now (well past the 1h timeout).
	completionMillis := fmt.Sprintf("%d", fixedNow.Add(-3*time.Hour).UnixMilli())
	seedJobEventForPipeline(mock, "gold-revenue", "stream", today, completionMillis, types.JobEventSuccess)

	// No post-run sensor "quality-check" exists → should detect absence.
	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	// Verify POST_RUN_SENSOR_MISSING event was published.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	var missingCount int
	for _, ev := range ebMock.events {
		if *ev.Entries[0].DetailType != string(types.EventPostRunSensorMissing) {
			continue
		}
		missingCount++
	}
	assert.Equal(t, 1, missingCount, "expected one POST_RUN_SENSOR_MISSING event")

	// Verify dedup marker was written.
	dedupKey := ddbItemKey(testControlTable, types.PipelinePK("gold-revenue"), types.SensorSK("postrun-check#"+today))
	mock.mu.Lock()
	_, dedupExists := mock.items[dedupKey]
	mock.mu.Unlock()
	assert.True(t, dedupExists, "expected postrun-check dedup marker to be written")
}

func TestWatchdog_PostRunSensorPresent(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Fix the clock so today is deterministic.
	fixedNow := time.Date(2026, 3, 8, 14, 0, 0, 0, time.UTC)
	d.NowFunc = func() time.Time { return fixedNow }

	today := fixedNow.Format("2006-01-02")

	// Seed pipeline config with PostRun (1h timeout).
	cfg := postRunWatchdogConfig("1h")
	seedConfig(mock, cfg)

	// Seed COMPLETED trigger for today.
	seedTriggerWithStatus(mock, "gold-revenue", today, types.TriggerStatusCompleted)

	// Seed baseline.
	seedSensor(mock, "gold-revenue", "postrun-baseline#"+today, map[string]interface{}{
		"sensor_count": float64(100),
	})

	// Seed a success job event 3h before now.
	completionMillis := fmt.Sprintf("%d", fixedNow.Add(-3*time.Hour).UnixMilli())
	seedJobEventForPipeline(mock, "gold-revenue", "stream", today, completionMillis, types.JobEventSuccess)

	// Seed post-run sensor with updatedAt AFTER completion → sensor arrived.
	postRunSensorTS := fixedNow.Add(-2 * time.Hour).UnixMilli()
	seedSensor(mock, "gold-revenue", "quality-check", map[string]interface{}{
		"status":    "passed",
		"updatedAt": float64(postRunSensorTS),
	})

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	// No POST_RUN_SENSOR_MISSING event should be published.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	for _, ev := range ebMock.events {
		assert.NotEqual(t, string(types.EventPostRunSensorMissing), *ev.Entries[0].DetailType,
			"should not publish POST_RUN_SENSOR_MISSING when sensor has recent updatedAt")
	}
}

func TestWatchdog_ScheduleSLAAlerts_SensorDaily_DeadlineNextDay(t *testing.T) {
	mock := newMockDDB()
	d, _, _ := testDeps(mock)
	schedMock := &mockScheduler{}
	d.Scheduler = schedMock
	d.SLAMonitorARN = "arn:aws:lambda:us-east-1:123:function:sla-monitor"
	d.SchedulerRoleARN = "arn:aws:iam::123:role/scheduler-role"
	d.SchedulerGroupName = "interlock-sla"

	// Fix time at 00:30 UTC on 2026-03-10. For a sensor-triggered daily
	// pipeline with SLA deadline "02:00", data for 2026-03-10 won't complete
	// until ~00:05 on 2026-03-11. The breach should be 2026-03-11T02:00:00Z
	// (tomorrow), NOT 2026-03-10T02:00:00Z (today).
	fixedNow := time.Date(2026, 3, 10, 0, 30, 0, 0, time.UTC)
	d.NowFunc = func() time.Time { return fixedNow }
	d.StartedAt = fixedNow.Add(-5 * time.Minute)

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "gold-daily-sensor"},
		Schedule: types.ScheduleConfig{
			// No cron — sensor-triggered daily pipeline.
			Trigger: &types.TriggerCondition{
				Key:   "upstream-complete",
				Check: types.CheckEquals,
				Field: "status",
				Value: "ready",
			},
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		SLA: &types.SLAConfig{
			Deadline:         "02:00",
			ExpectedDuration: "30m",
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "glue", Config: map[string]interface{}{"jobName": "gold-daily"}},
	}
	seedConfig(mock, cfg)

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	schedMock.mu.Lock()
	defer schedMock.mu.Unlock()
	require.Len(t, schedMock.created, 2, "expected 2 SLA schedules (warning + breach)")

	// Find the breach schedule and verify it targets tomorrow (2026-03-11),
	// not today (2026-03-10).
	for _, s := range schedMock.created {
		name := *s.Name
		if !strings.Contains(name, "breach") {
			continue
		}
		// The schedule expression is "at(2026-03-11T02:00:00)" for the
		// correct (next-day) deadline.
		expr := *s.ScheduleExpression
		assert.Contains(t, expr, "2026-03-11T02:00:00",
			"sensor-triggered daily pipeline breach should target next day; got %s", expr)
		assert.NotContains(t, expr, "2026-03-10T02:00:00",
			"sensor-triggered daily pipeline breach should NOT target today; got %s", expr)
	}
}

// ---------------------------------------------------------------------------
// SLA date boundary tests for the T+1 fix
// ---------------------------------------------------------------------------

func TestWatchdog_SLADateBoundaries(t *testing.T) {
	// Helper to set up a scheduler-enabled Deps with a fixed clock.
	setupDeps := func(mock *mockDDB, fixedNow time.Time) (*lambda.Deps, *mockScheduler) {
		d, _, _ := testDeps(mock)
		schedMock := &mockScheduler{}
		d.Scheduler = schedMock
		d.SLAMonitorARN = "arn:aws:lambda:us-east-1:123:function:sla-monitor"
		d.SchedulerRoleARN = "arn:aws:iam::123:role/scheduler-role"
		d.SchedulerGroupName = "interlock-sla"
		d.NowFunc = func() time.Time { return fixedNow }
		d.StartedAt = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		return d, schedMock
	}

	// sensorDailyCfg builds a sensor-triggered daily pipeline config.
	sensorDailyCfg := func(id string) types.PipelineConfig {
		return types.PipelineConfig{
			Pipeline: types.PipelineIdentity{ID: id},
			Schedule: types.ScheduleConfig{
				Trigger: &types.TriggerCondition{
					Key:   "daily-status",
					Check: types.CheckExists,
				},
				Evaluation: types.EvaluationWindow{Window: "30m", Interval: "5m"},
			},
			SLA: &types.SLAConfig{
				Deadline:         "02:00",
				ExpectedDuration: "30m",
			},
			Validation: types.ValidationConfig{Trigger: "ALL"},
			Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo test"}},
		}
	}

	// findBreachExpr returns the ScheduleExpression from the breach schedule.
	findBreachExpr := func(t *testing.T, schedMock *mockScheduler) string {
		t.Helper()
		schedMock.mu.Lock()
		defer schedMock.mu.Unlock()
		for _, s := range schedMock.created {
			if strings.Contains(*s.Name, "breach") {
				return *s.ScheduleExpression
			}
		}
		t.Fatal("no breach schedule found")
		return ""
	}

	// --- Positive cases: sensor-triggered daily, SHOULD shift T+1 ---

	t.Run("MidnightWindow", func(t *testing.T) {
		mock := newMockDDB()
		fixedNow := time.Date(2026, 3, 10, 0, 30, 0, 0, time.UTC)
		d, schedMock := setupDeps(mock, fixedNow)

		cfg := sensorDailyCfg("sla-boundary-midnight")
		seedConfig(mock, cfg)

		err := lambda.HandleWatchdog(context.Background(), d)
		require.NoError(t, err)

		expr := findBreachExpr(t, schedMock)
		assert.Contains(t, expr, "2026-03-11T02:00:00",
			"midnight window: breach should be next day; got %s", expr)
		assert.NotContains(t, expr, "2026-03-10T02:00:00",
			"midnight window: breach should NOT be today; got %s", expr)
	})

	t.Run("AfterDeadlineHour", func(t *testing.T) {
		mock := newMockDDB()
		fixedNow := time.Date(2026, 3, 10, 10, 0, 0, 0, time.UTC)
		d, schedMock := setupDeps(mock, fixedNow)

		cfg := sensorDailyCfg("sla-boundary-after-deadline")
		seedConfig(mock, cfg)

		err := lambda.HandleWatchdog(context.Background(), d)
		require.NoError(t, err)

		expr := findBreachExpr(t, schedMock)
		assert.Contains(t, expr, "2026-03-11T02:00:00",
			"after deadline: breach should be next day; got %s", expr)
	})

	t.Run("JustBeforeMidnight", func(t *testing.T) {
		mock := newMockDDB()
		fixedNow := time.Date(2026, 3, 10, 23, 59, 0, 0, time.UTC)
		d, schedMock := setupDeps(mock, fixedNow)

		cfg := sensorDailyCfg("sla-boundary-before-midnight")
		seedConfig(mock, cfg)

		err := lambda.HandleWatchdog(context.Background(), d)
		require.NoError(t, err)

		expr := findBreachExpr(t, schedMock)
		assert.Contains(t, expr, "2026-03-11T02:00:00",
			"just before midnight: breach should be next day; got %s", expr)
	})

	t.Run("MonthBoundary", func(t *testing.T) {
		mock := newMockDDB()
		fixedNow := time.Date(2026, 3, 31, 12, 0, 0, 0, time.UTC)
		d, schedMock := setupDeps(mock, fixedNow)

		cfg := sensorDailyCfg("sla-boundary-month")
		seedConfig(mock, cfg)

		err := lambda.HandleWatchdog(context.Background(), d)
		require.NoError(t, err)

		expr := findBreachExpr(t, schedMock)
		assert.Contains(t, expr, "2026-04-01",
			"month boundary: breach should cross into April; got %s", expr)
	})

	t.Run("YearBoundary", func(t *testing.T) {
		mock := newMockDDB()
		fixedNow := time.Date(2026, 12, 31, 12, 0, 0, 0, time.UTC)
		d, schedMock := setupDeps(mock, fixedNow)

		cfg := sensorDailyCfg("sla-boundary-year")
		seedConfig(mock, cfg)

		err := lambda.HandleWatchdog(context.Background(), d)
		require.NoError(t, err)

		expr := findBreachExpr(t, schedMock)
		assert.Contains(t, expr, "2027-01-01",
			"year boundary: breach should cross into 2027; got %s", expr)
	})

	t.Run("LeapYear", func(t *testing.T) {
		mock := newMockDDB()
		fixedNow := time.Date(2028, 2, 28, 12, 0, 0, 0, time.UTC)
		d, schedMock := setupDeps(mock, fixedNow)

		cfg := sensorDailyCfg("sla-boundary-leap")
		seedConfig(mock, cfg)

		err := lambda.HandleWatchdog(context.Background(), d)
		require.NoError(t, err)

		expr := findBreachExpr(t, schedMock)
		assert.Contains(t, expr, "2028-02-29",
			"leap year: breach should land on Feb 29; got %s", expr)
	})

	t.Run("NonLeapYear", func(t *testing.T) {
		mock := newMockDDB()
		fixedNow := time.Date(2027, 2, 28, 12, 0, 0, 0, time.UTC)
		d, schedMock := setupDeps(mock, fixedNow)

		cfg := sensorDailyCfg("sla-boundary-nonleap")
		seedConfig(mock, cfg)

		err := lambda.HandleWatchdog(context.Background(), d)
		require.NoError(t, err)

		expr := findBreachExpr(t, schedMock)
		assert.Contains(t, expr, "2027-03-01",
			"non-leap year: breach should roll to Mar 1; got %s", expr)
	})

	// --- Negative cases: should NOT shift ---

	t.Run("CronDailyNoShift", func(t *testing.T) {
		mock := newMockDDB()
		fixedNow := time.Date(2026, 3, 10, 0, 30, 0, 0, time.UTC)
		d, schedMock := setupDeps(mock, fixedNow)

		cfg := types.PipelineConfig{
			Pipeline: types.PipelineIdentity{ID: "sla-boundary-cron"},
			Schedule: types.ScheduleConfig{
				Cron:       "0 2 * * *",
				Evaluation: types.EvaluationWindow{Window: "30m", Interval: "5m"},
			},
			SLA: &types.SLAConfig{
				Deadline:         "02:00",
				ExpectedDuration: "30m",
			},
			Validation: types.ValidationConfig{Trigger: "ALL"},
			Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo test"}},
		}
		seedConfig(mock, cfg)

		err := lambda.HandleWatchdog(context.Background(), d)
		require.NoError(t, err)

		expr := findBreachExpr(t, schedMock)
		assert.Contains(t, expr, "2026-03-10T02:00:00",
			"cron daily: breach should be same day (no T+1 shift); got %s", expr)
		assert.NotContains(t, expr, "2026-03-11",
			"cron daily: breach should NOT be next day; got %s", expr)
	})

	t.Run("HourlySensorNoShift", func(t *testing.T) {
		mock := newMockDDB()
		fixedNow := time.Date(2026, 3, 10, 10, 0, 0, 0, time.UTC)
		d, schedMock := setupDeps(mock, fixedNow)

		cfg := types.PipelineConfig{
			Pipeline: types.PipelineIdentity{ID: "sla-boundary-hourly"},
			Schedule: types.ScheduleConfig{
				Trigger: &types.TriggerCondition{
					Key:   "hourly-status",
					Check: types.CheckExists,
				},
				Evaluation: types.EvaluationWindow{Window: "5m", Interval: "1m"},
			},
			SLA: &types.SLAConfig{
				Deadline:         ":45",
				ExpectedDuration: "10m",
			},
			Validation: types.ValidationConfig{Trigger: "ALL"},
			Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo test"}},
		}
		seedConfig(mock, cfg)

		err := lambda.HandleWatchdog(context.Background(), d)
		require.NoError(t, err)

		// For hourly sensor at 10:00, resolveWatchdogSLADate returns
		// "2026-03-10T09" (previous hour). The T+1 shift does NOT apply
		// because the deadline starts with ":". The breach should be at
		// :45 of the processing hour (T09's data processed in T10),
		// i.e. 2026-03-10T10:45:00.
		schedMock.mu.Lock()
		defer schedMock.mu.Unlock()
		require.NotEmpty(t, schedMock.created, "expected SLA schedules for hourly sensor pipeline")

		for _, s := range schedMock.created {
			if !strings.Contains(*s.Name, "breach") {
				continue
			}
			expr := *s.ScheduleExpression
			// Breach should stay on 2026-03-10, NOT shift to 2026-03-11.
			assert.Contains(t, expr, "2026-03-10",
				"hourly sensor: breach should stay on same day; got %s", expr)
			assert.NotContains(t, expr, "2026-03-11",
				"hourly sensor: breach should NOT shift to next day; got %s", expr)
		}
	})
}

func TestWatchdog_PostRunNoConfig(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Fix the clock.
	fixedNow := time.Date(2026, 3, 8, 14, 0, 0, 0, time.UTC)
	d.NowFunc = func() time.Time { return fixedNow }

	today := fixedNow.Format("2006-01-02")

	// Seed pipeline config WITHOUT PostRun.
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

	// Seed COMPLETED trigger for today (even though no PostRun config).
	seedTriggerRow(mock, "bronze-ingest", today, 0)
	mock.putRaw(testControlTable, map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("bronze-ingest")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("cron", today)},
		"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusCompleted},
	})

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	// No POST_RUN_SENSOR_MISSING event should be published.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	for _, ev := range ebMock.events {
		assert.NotEqual(t, string(types.EventPostRunSensorMissing), *ev.Entries[0].DetailType,
			"should not publish POST_RUN_SENSOR_MISSING for pipeline without PostRun config")
	}
}

// ---------------------------------------------------------------------------
// Inclusion schedule missed detection tests
// ---------------------------------------------------------------------------

func TestWatchdog_InclusionSchedule_NilInclude_Skipped(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Pipeline with no Include config — should be ignored by inclusion detection.
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "monthly-close"},
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
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo run"}},
	}
	seedConfig(mock, cfg)

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	// No IRREGULAR_SCHEDULE_MISSED events.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	for _, ev := range ebMock.events {
		assert.NotEqual(t, string(types.EventIrregularScheduleMissed), *ev.Entries[0].DetailType,
			"should not publish IRREGULAR_SCHEDULE_MISSED when Include is nil")
	}
}

func TestWatchdog_InclusionSchedule_TriggerExists_NoAlert(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Use a fixed time for deterministic behavior.
	fixedNow := time.Date(2026, 3, 10, 14, 0, 0, 0, time.UTC)
	d.NowFunc = func() time.Time { return fixedNow }

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "monthly-close"},
		Schedule: types.ScheduleConfig{
			Include: &types.InclusionConfig{
				Dates: []string{"2026-03-05", "2026-03-15"},
			},
			Trigger: &types.TriggerCondition{
				Key:   "upstream-complete",
				Check: types.CheckEquals,
				Field: "status",
				Value: "ready",
			},
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo run"}},
	}
	seedConfig(mock, cfg)

	// Most recent inclusion date on or before now is 2026-03-05.
	// Seed a trigger for that date so no alert is expected.
	mock.putRaw(testControlTable, map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("monthly-close")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("stream", "2026-03-05")},
		"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusCompleted},
	})

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	// No IRREGULAR_SCHEDULE_MISSED events.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	for _, ev := range ebMock.events {
		assert.NotEqual(t, string(types.EventIrregularScheduleMissed), *ev.Entries[0].DetailType,
			"should not publish IRREGULAR_SCHEDULE_MISSED when trigger exists for date")
	}
}

func TestWatchdog_InclusionSchedule_TriggerMissing_PublishesAlert(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Use a fixed time for deterministic behavior.
	fixedNow := time.Date(2026, 3, 10, 14, 0, 0, 0, time.UTC)
	d.NowFunc = func() time.Time { return fixedNow }

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "monthly-close"},
		Schedule: types.ScheduleConfig{
			Include: &types.InclusionConfig{
				Dates: []string{"2026-03-05", "2026-03-15"},
			},
			Trigger: &types.TriggerCondition{
				Key:   "upstream-complete",
				Check: types.CheckEquals,
				Field: "status",
				Value: "ready",
			},
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo run"}},
	}
	seedConfig(mock, cfg)

	// No trigger seeded for 2026-03-05 — should detect as missed.
	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	// Expect IRREGULAR_SCHEDULE_MISSED event.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()

	var irregularMissedCount int
	for _, ev := range ebMock.events {
		if *ev.Entries[0].DetailType != string(types.EventIrregularScheduleMissed) {
			continue
		}
		irregularMissedCount++

		// Verify event detail contains the expected pipeline and date.
		var detail types.InterlockEvent
		_ = json.Unmarshal([]byte(*ev.Entries[0].Detail), &detail)
		assert.Equal(t, "monthly-close", detail.PipelineID)
		assert.Equal(t, "2026-03-05", detail.Date)
	}
	assert.Equal(t, 1, irregularMissedCount, "expected exactly one IRREGULAR_SCHEDULE_MISSED event")
}

func TestWatchdog_InclusionSchedule_DedupPreventsRepeat(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	fixedNow := time.Date(2026, 3, 10, 14, 0, 0, 0, time.UTC)
	d.NowFunc = func() time.Time { return fixedNow }

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "monthly-close"},
		Schedule: types.ScheduleConfig{
			Include: &types.InclusionConfig{
				Dates: []string{"2026-03-05"},
			},
			Trigger: &types.TriggerCondition{
				Key:   "upstream-complete",
				Check: types.CheckEquals,
				Field: "status",
				Value: "ready",
			},
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo run"}},
	}
	seedConfig(mock, cfg)

	// Seed dedup marker to simulate a previous watchdog run already detected this.
	seedSensor(mock, "monthly-close", "irregular-missed-check#2026-03-05", map[string]interface{}{
		"alerted": "true",
	})

	// No trigger for 2026-03-05, but dedup marker exists — should NOT alert again.
	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	for _, ev := range ebMock.events {
		assert.NotEqual(t, string(types.EventIrregularScheduleMissed), *ev.Entries[0].DetailType,
			"should not publish IRREGULAR_SCHEDULE_MISSED when dedup marker exists")
	}
}

func TestWatchdog_InclusionSchedule_AllFutureDates_NoAlert(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	fixedNow := time.Date(2026, 3, 10, 14, 0, 0, 0, time.UTC)
	d.NowFunc = func() time.Time { return fixedNow }

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "quarterly-filing"},
		Schedule: types.ScheduleConfig{
			Include: &types.InclusionConfig{
				Dates: []string{"2026-03-15", "2026-06-15", "2026-09-15"},
			},
			Trigger: &types.TriggerCondition{
				Key:   "upstream-complete",
				Check: types.CheckEquals,
				Field: "status",
				Value: "ready",
			},
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo run"}},
	}
	seedConfig(mock, cfg)

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	// No inclusion date on or before now — no alert.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	for _, ev := range ebMock.events {
		assert.NotEqual(t, string(types.EventIrregularScheduleMissed), *ev.Entries[0].DetailType,
			"should not publish IRREGULAR_SCHEDULE_MISSED when all dates are in the future")
	}
}

func TestWatchdog_InclusionSchedule_MultiDateGap(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Fix time so both 2026-03-05 and 2026-03-08 are in the past.
	fixedNow := time.Date(2026, 3, 10, 14, 0, 0, 0, time.UTC)
	d.NowFunc = func() time.Time { return fixedNow }

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "monthly-close"},
		Schedule: types.ScheduleConfig{
			Include: &types.InclusionConfig{
				Dates: []string{"2026-03-05", "2026-03-08", "2026-03-15"},
			},
			Trigger: &types.TriggerCondition{
				Key:   "upstream-complete",
				Check: types.CheckEquals,
				Field: "status",
				Value: "ready",
			},
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo run"}},
	}
	seedConfig(mock, cfg)

	// No triggers seeded for either date — should detect BOTH as missed.
	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	// Expect IRREGULAR_SCHEDULE_MISSED events for both dates.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()

	var missedDates []string
	for _, ev := range ebMock.events {
		if *ev.Entries[0].DetailType != string(types.EventIrregularScheduleMissed) {
			continue
		}
		var detail types.InterlockEvent
		_ = json.Unmarshal([]byte(*ev.Entries[0].Detail), &detail)
		missedDates = append(missedDates, detail.Date)
	}
	assert.Len(t, missedDates, 2, "expected IRREGULAR_SCHEDULE_MISSED events for both past dates")
	assert.Contains(t, missedDates, "2026-03-05")
	assert.Contains(t, missedDates, "2026-03-08")
}

// ---------------------------------------------------------------------------
// Inclusion schedule: Schedule.Time grace period
// ---------------------------------------------------------------------------

func TestWatchdog_InclusionSchedule_RespectsScheduleTime(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Inclusion date is today. Schedule.Time is "10:00" but current time is 08:00 UTC.
	// Should NOT fire an alert because we haven't reached the expected start time yet.
	fixedNow := time.Date(2026, 3, 31, 8, 0, 0, 0, time.UTC)
	d.NowFunc = func() time.Time { return fixedNow }

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "monthly-close"},
		Schedule: types.ScheduleConfig{
			Include: &types.InclusionConfig{
				Dates: []string{"2026-03-31"},
			},
			Time: "10:00",
			Trigger: &types.TriggerCondition{
				Key:   "upstream-complete",
				Check: types.CheckEquals,
				Field: "status",
				Value: "ready",
			},
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo run"}},
	}
	seedConfig(mock, cfg)

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	// No IRREGULAR_SCHEDULE_MISSED events — too early.
	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	for _, ev := range ebMock.events {
		if *ev.Entries[0].DetailType != string(types.EventIrregularScheduleMissed) {
			continue
		}
		var detail types.InterlockEvent
		_ = json.Unmarshal([]byte(*ev.Entries[0].Detail), &detail)
		if detail.PipelineID == "monthly-close" {
			t.Error("should not publish IRREGULAR_SCHEDULE_MISSED before Schedule.Time")
		}
	}
}

func TestWatchdog_InclusionSchedule_PastScheduleTime(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Inclusion date is today. Schedule.Time is "10:00" and current time is 11:00 UTC.
	// No trigger exists — should fire an alert.
	fixedNow := time.Date(2026, 3, 31, 11, 0, 0, 0, time.UTC)
	d.NowFunc = func() time.Time { return fixedNow }

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "monthly-close"},
		Schedule: types.ScheduleConfig{
			Include: &types.InclusionConfig{
				Dates: []string{"2026-03-31"},
			},
			Time: "10:00",
			Trigger: &types.TriggerCondition{
				Key:   "upstream-complete",
				Check: types.CheckEquals,
				Field: "status",
				Value: "ready",
			},
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo run"}},
	}
	seedConfig(mock, cfg)

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()

	var irregularMissedCount int
	for _, ev := range ebMock.events {
		if *ev.Entries[0].DetailType != string(types.EventIrregularScheduleMissed) {
			continue
		}
		irregularMissedCount++
		var detail types.InterlockEvent
		_ = json.Unmarshal([]byte(*ev.Entries[0].Detail), &detail)
		assert.Equal(t, "monthly-close", detail.PipelineID)
		assert.Equal(t, "2026-03-31", detail.Date)
	}
	assert.Equal(t, 1, irregularMissedCount, "expected exactly one IRREGULAR_SCHEDULE_MISSED event")
}

func TestWatchdog_InclusionSchedule_WithTimezone(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Inclusion date is today (2026-03-31). Schedule.Time is "10:00",
	// Timezone is "Asia/Tokyo" (UTC+9). Current time is 01:30 UTC = 10:30 JST.
	// Since 10:30 JST is past 10:00 JST and no trigger exists, should fire alert.
	fixedNow := time.Date(2026, 3, 31, 1, 30, 0, 0, time.UTC)
	d.NowFunc = func() time.Time { return fixedNow }

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "monthly-close"},
		Schedule: types.ScheduleConfig{
			Include: &types.InclusionConfig{
				Dates: []string{"2026-03-31"},
			},
			Timezone: "Asia/Tokyo",
			Time:     "10:00",
			Trigger: &types.TriggerCondition{
				Key:   "upstream-complete",
				Check: types.CheckEquals,
				Field: "status",
				Value: "ready",
			},
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo run"}},
	}
	seedConfig(mock, cfg)

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()

	var irregularMissedCount int
	for _, ev := range ebMock.events {
		if *ev.Entries[0].DetailType != string(types.EventIrregularScheduleMissed) {
			continue
		}
		irregularMissedCount++
		var detail types.InterlockEvent
		_ = json.Unmarshal([]byte(*ev.Entries[0].Detail), &detail)
		assert.Equal(t, "monthly-close", detail.PipelineID)
		assert.Equal(t, "2026-03-31", detail.Date)
	}
	assert.Equal(t, 1, irregularMissedCount, "expected exactly one IRREGULAR_SCHEDULE_MISSED event")
}

func TestWatchdog_InclusionSchedule_NoTimeConfig(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// Inclusion date is today. No Schedule.Time set.
	// Should fire alert immediately (backward compatible).
	fixedNow := time.Date(2026, 3, 31, 0, 5, 0, 0, time.UTC)
	d.NowFunc = func() time.Time { return fixedNow }

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "monthly-close"},
		Schedule: types.ScheduleConfig{
			Include: &types.InclusionConfig{
				Dates: []string{"2026-03-31"},
			},
			Trigger: &types.TriggerCondition{
				Key:   "upstream-complete",
				Check: types.CheckEquals,
				Field: "status",
				Value: "ready",
			},
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo run"}},
	}
	seedConfig(mock, cfg)

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()

	var irregularMissedCount int
	for _, ev := range ebMock.events {
		if *ev.Entries[0].DetailType != string(types.EventIrregularScheduleMissed) {
			continue
		}
		irregularMissedCount++
		var detail types.InterlockEvent
		_ = json.Unmarshal([]byte(*ev.Entries[0].Detail), &detail)
		assert.Equal(t, "monthly-close", detail.PipelineID)
		assert.Equal(t, "2026-03-31", detail.Date)
	}
	assert.Equal(t, 1, irregularMissedCount, "expected exactly one IRREGULAR_SCHEDULE_MISSED event")
}

// TestWatchdog_InclusionSchedule_TimezoneUTCDateMismatch verifies that the
// grace-period check uses the pipeline's timezone for "today", not UTC.
// Scenario: UTC is 2026-04-01 00:30, but US/Eastern is 2026-03-31 20:30.
// Inclusion date is 2026-03-31 with Time "10:00" Eastern. Since 20:30 > 10:00
// in Eastern and no trigger exists, the alert should fire.
func TestWatchdog_InclusionSchedule_TimezoneUTCDateMismatch(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// UTC is April 1, but US/Eastern is still March 31.
	fixedNow := time.Date(2026, 4, 1, 0, 30, 0, 0, time.UTC)
	d.NowFunc = func() time.Time { return fixedNow }

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "quarterly-close"},
		Schedule: types.ScheduleConfig{
			Include: &types.InclusionConfig{
				Dates: []string{"2026-03-31"},
			},
			Timezone: "America/New_York",
			Time:     "10:00",
			Trigger: &types.TriggerCondition{
				Key:   "upstream-complete",
				Check: types.CheckEquals,
				Field: "status",
				Value: "ready",
			},
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo run"}},
	}
	seedConfig(mock, cfg)

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()

	var irregularMissedCount int
	for _, ev := range ebMock.events {
		if *ev.Entries[0].DetailType != string(types.EventIrregularScheduleMissed) {
			continue
		}
		irregularMissedCount++
		var detail types.InterlockEvent
		_ = json.Unmarshal([]byte(*ev.Entries[0].Detail), &detail)
		assert.Equal(t, "quarterly-close", detail.PipelineID)
		assert.Equal(t, "2026-03-31", detail.Date)
	}
	assert.Equal(t, 1, irregularMissedCount,
		"expected IRREGULAR_SCHEDULE_MISSED: UTC is April 1 but Eastern is still March 31 past 10:00")
}

// ---------------------------------------------------------------------------
// Relative SLA breach detection (defense-in-depth)
// ---------------------------------------------------------------------------

func TestWatchdog_RelativeSLA_NoMaxDuration_Skips(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	fixedNow := time.Date(2026, 3, 10, 16, 0, 0, 0, time.UTC)
	d.NowFunc = func() time.Time { return fixedNow }

	// Pipeline with no MaxDuration — relative SLA check should be skipped.
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "standard-pipeline"},
		Schedule: types.ScheduleConfig{
			Trigger: &types.TriggerCondition{
				Key:   "upstream-complete",
				Check: types.CheckEquals,
				Field: "status",
				Value: "ready",
			},
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		SLA: &types.SLAConfig{
			Deadline:         "18:00",
			ExpectedDuration: "30m",
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo hello"}},
	}
	seedConfig(mock, cfg)

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	for _, ev := range ebMock.events {
		assert.NotEqual(t, string(types.EventRelativeSLABreach), *ev.Entries[0].DetailType,
			"should not publish RELATIVE_SLA_BREACH when no maxDuration configured")
	}
}

func TestWatchdog_RelativeSLA_SensorArrival_NotYetBreached(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	fixedNow := time.Date(2026, 3, 10, 15, 0, 0, 0, time.UTC)
	d.NowFunc = func() time.Time { return fixedNow }

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "adhoc-pipeline"},
		Schedule: types.ScheduleConfig{
			Trigger: &types.TriggerCondition{
				Key:   "upstream-complete",
				Check: types.CheckEquals,
				Field: "status",
				Value: "ready",
			},
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		SLA: &types.SLAConfig{
			MaxDuration: "2h",
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo hello"}},
	}
	seedConfig(mock, cfg)

	// Seed first-sensor-arrival at 14:00. With maxDuration=2h, breach is at 16:00.
	// Current time is 15:00, so not yet breached.
	date := fixedNow.Format("2006-01-02")
	seedSensor(mock, "adhoc-pipeline", "first-sensor-arrival#"+date, map[string]interface{}{
		"arrivedAt": "2026-03-10T14:00:00Z",
	})

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	for _, ev := range ebMock.events {
		assert.NotEqual(t, string(types.EventRelativeSLABreach), *ev.Entries[0].DetailType,
			"should not publish RELATIVE_SLA_BREACH before maxDuration elapses")
	}
}

func TestWatchdog_RelativeSLA_SensorArrival_Breached(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	fixedNow := time.Date(2026, 3, 10, 17, 0, 0, 0, time.UTC)
	d.NowFunc = func() time.Time { return fixedNow }

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "adhoc-pipeline"},
		Schedule: types.ScheduleConfig{
			Trigger: &types.TriggerCondition{
				Key:   "upstream-complete",
				Check: types.CheckEquals,
				Field: "status",
				Value: "ready",
			},
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		SLA: &types.SLAConfig{
			MaxDuration: "2h",
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo hello"}},
	}
	seedConfig(mock, cfg)

	// Seed first-sensor-arrival at 14:00. With maxDuration=2h, breach is at 16:00.
	// Current time is 17:00, so breached.
	date := fixedNow.Format("2006-01-02")
	seedSensor(mock, "adhoc-pipeline", "first-sensor-arrival#"+date, map[string]interface{}{
		"arrivedAt": "2026-03-10T14:00:00Z",
	})

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	var breachCount int
	for _, ev := range ebMock.events {
		if *ev.Entries[0].DetailType != string(types.EventRelativeSLABreach) {
			continue
		}
		breachCount++
	}
	assert.Equal(t, 1, breachCount, "expected one RELATIVE_SLA_BREACH event")
}

func TestWatchdog_RelativeSLA_Breached_AlreadyCompleted_NoAlert(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	fixedNow := time.Date(2026, 3, 10, 17, 0, 0, 0, time.UTC)
	d.NowFunc = func() time.Time { return fixedNow }

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "adhoc-pipeline"},
		Schedule: types.ScheduleConfig{
			Trigger: &types.TriggerCondition{
				Key:   "upstream-complete",
				Check: types.CheckEquals,
				Field: "status",
				Value: "ready",
			},
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		SLA: &types.SLAConfig{
			MaxDuration: "2h",
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo hello"}},
	}
	seedConfig(mock, cfg)

	date := fixedNow.Format("2006-01-02")
	seedSensor(mock, "adhoc-pipeline", "first-sensor-arrival#"+date, map[string]interface{}{
		"arrivedAt": "2026-03-10T14:00:00Z",
	})

	// Seed a COMPLETED trigger — pipeline already finished.
	mock.putRaw(testControlTable, map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("adhoc-pipeline")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("stream", date)},
		"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusCompleted},
	})

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	for _, ev := range ebMock.events {
		assert.NotEqual(t, string(types.EventRelativeSLABreach), *ev.Entries[0].DetailType,
			"should not publish RELATIVE_SLA_BREACH when pipeline already completed")
	}
}

func TestWatchdog_RelativeSLA_CrossDayArrival(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// It's now March 11 at 03:00 UTC. The sensor arrived yesterday (March 10)
	// at 23:00 UTC. With maxDuration=2h the breach was at 01:00 UTC on March 11.
	// stream_router wrote the arrival key under yesterday's date (T+1 pattern).
	fixedNow := time.Date(2026, 3, 11, 3, 0, 0, 0, time.UTC)
	d.NowFunc = func() time.Time { return fixedNow }

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "t1-pipeline"},
		Schedule: types.ScheduleConfig{
			Trigger: &types.TriggerCondition{
				Key:   "upstream-complete",
				Check: types.CheckEquals,
				Field: "status",
				Value: "ready",
			},
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		SLA: &types.SLAConfig{
			MaxDuration: "2h",
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo hello"}},
	}
	seedConfig(mock, cfg)

	// Seed the arrival key under YESTERDAY's date (T-1), as stream_router does
	// for T+1 sensor-triggered pipelines.
	yesterday := fixedNow.AddDate(0, 0, -1).Format("2006-01-02")
	seedSensor(mock, "t1-pipeline", "first-sensor-arrival#"+yesterday, map[string]interface{}{
		"arrivedAt": "2026-03-10T23:00:00Z",
	})

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	var breachCount int
	for _, ev := range ebMock.events {
		if *ev.Entries[0].DetailType != string(types.EventRelativeSLABreach) {
			continue
		}
		breachCount++
	}
	assert.Equal(t, 1, breachCount, "expected one RELATIVE_SLA_BREACH event for cross-day sensor arrival")
}

func TestWatchdog_RelativeSLA_CrossDayNotBreached(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)

	// It's now March 11 at 00:30 UTC. The sensor arrived yesterday (March 10)
	// at 23:00 UTC. With maxDuration=2h the breach would be at 01:00 UTC on
	// March 11, so we're NOT yet breached.
	fixedNow := time.Date(2026, 3, 11, 0, 30, 0, 0, time.UTC)
	d.NowFunc = func() time.Time { return fixedNow }

	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "t1-pipeline"},
		Schedule: types.ScheduleConfig{
			Trigger: &types.TriggerCondition{
				Key:   "upstream-complete",
				Check: types.CheckEquals,
				Field: "status",
				Value: "ready",
			},
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
		},
		SLA: &types.SLAConfig{
			MaxDuration: "2h",
		},
		Validation: types.ValidationConfig{Trigger: "ALL"},
		Job:        types.JobConfig{Type: "command", Config: map[string]interface{}{"command": "echo hello"}},
	}
	seedConfig(mock, cfg)

	// Seed the arrival key under YESTERDAY's date (T-1).
	yesterday := fixedNow.AddDate(0, 0, -1).Format("2006-01-02")
	seedSensor(mock, "t1-pipeline", "first-sensor-arrival#"+yesterday, map[string]interface{}{
		"arrivedAt": "2026-03-10T23:00:00Z",
	})

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	ebMock.mu.Lock()
	defer ebMock.mu.Unlock()
	for _, ev := range ebMock.events {
		assert.NotEqual(t, string(types.EventRelativeSLABreach), *ev.Entries[0].DetailType,
			"should not publish RELATIVE_SLA_BREACH before maxDuration is exceeded")
	}
}

// TestWatchdog_DryRun_SkipsAllSchedulingAndAlerts verifies that dry-run pipelines
// are completely invisible to the watchdog: no SLA schedules created, no
// SCHEDULE_MISSED events, no trigger deadline closures, and no relative SLA breaches.
func TestWatchdog_DryRun_SkipsAllSchedulingAndAlerts(t *testing.T) {
	mock := newMockDDB()
	d, _, ebMock := testDeps(mock)
	schedMock := &mockScheduler{}
	d.Scheduler = schedMock
	d.SLAMonitorARN = "arn:aws:lambda:us-east-1:123:function:sla-monitor"
	d.SchedulerRoleARN = "arn:aws:iam::123:role/scheduler-role"
	d.SchedulerGroupName = "interlock-sla"

	fixedTime := time.Date(2026, 3, 13, 20, 0, 0, 0, time.UTC)
	d.NowFunc = func() time.Time { return fixedTime }
	d.StartedAt = fixedTime.Add(-1 * time.Hour)

	// Configure a dry-run pipeline with cron schedule, SLA, trigger deadline,
	// and inclusion dates — all features that the watchdog checks.
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "dryrun-weather"},
		DryRun:   true,
		Schedule: types.ScheduleConfig{
			Cron:       "0 * * * *",
			Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"},
			Trigger: &types.TriggerCondition{
				Key:      "weather-data",
				Check:    "field_equals",
				Field:    "complete",
				Value:    "true",
				Deadline: ":45",
			},
		},
		SLA: &types.SLAConfig{
			Deadline:         ":30",
			ExpectedDuration: "15m",
			MaxDuration:      "2h",
		},
		Validation: types.ValidationConfig{
			Trigger: "ALL",
			Rules: []types.ValidationRule{
				{Key: "weather-data", Check: "field_equals", Field: "complete", Value: "true"},
			},
		},
		Job: types.JobConfig{Type: "glue", Config: map[string]interface{}{"jobName": "test"}},
	}
	seedConfig(mock, cfg)

	// Seed a first-sensor-arrival key to trigger the relative SLA path.
	seedSensor(mock, "dryrun-weather", "first-sensor-arrival#2026-03-13", map[string]interface{}{
		"arrivedAt": "2026-03-13T17:00:00Z",
	})

	// Seed the trigger sensor so reconciliation would fire if not guarded.
	seedSensor(mock, "dryrun-weather", "weather-data", map[string]interface{}{
		"complete": "true",
	})

	err := lambda.HandleWatchdog(context.Background(), d)
	require.NoError(t, err)

	// No EventBridge Scheduler entries for SLA.
	schedMock.mu.Lock()
	assert.Empty(t, schedMock.created, "dry-run pipeline must not create SLA schedules")
	schedMock.mu.Unlock()

	// No watchdog event types should fire for a dry-run pipeline.
	evtTypes := gatherEventDetailTypes(ebMock)
	prohibitedEvents := []string{
		string(types.EventScheduleMissed),
		string(types.EventIrregularScheduleMissed),
		string(types.EventSensorDeadlineExpired),
		string(types.EventRelativeSLABreach),
		string(types.EventRelativeSLAWarning),
		string(types.EventTriggerRecovered),
		string(types.EventPostRunSensorMissing),
		string(types.EventSFNTimeout),
	}
	for _, prohibited := range prohibitedEvents {
		assert.NotContains(t, evtTypes, prohibited,
			"dry-run pipeline must not produce %s events", prohibited)
	}
}

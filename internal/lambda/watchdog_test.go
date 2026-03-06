package lambda_test

import (
	"context"
	"fmt"
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
		if *ev.Entries[0].DetailType == string(types.EventTriggerRecovered) {
			recoveredCount++
		}
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
		if *ev.Entries[0].DetailType == string(types.EventTriggerRecovered) {
			recoveredCount++
		}
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
		if *ev.Entries[0].DetailType == string(types.EventTriggerRecovered) {
			recoveredCount++
		}
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
		if *ev.Entries[0].DetailType == string(types.EventScheduleMissed) {
			missedCount++
		}
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

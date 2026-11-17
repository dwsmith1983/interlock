package watchdog

import (
	"context"
	"maps"
	"slices"
	"testing"
	"time"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sfn"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// countingSFN is a lambda.SFNAPI double that counts StartExecution calls.
type countingSFN struct{ calls int }

func (c *countingSFN) StartExecution(context.Context, *sfn.StartExecutionInput, ...func(*sfn.Options)) (*sfn.StartExecutionOutput, error) {
	c.calls++
	return &sfn.StartExecutionOutput{}, nil
}

// TestReconcile_Cutoff: the reconciler starts nothing for a ready sensor
// whose date is past its 14:00 UTC cutoff; with no trigger row it closes the
// date instead. Each row runs two passes, and the second adds nothing.
func TestReconcile_Cutoff(t *testing.T) {
	at := func(day, hour, minute int) time.Time { return time.Date(2026, 3, day, hour, minute, 0, 0, time.UTC) }
	tests := []struct {
		name       string
		date       string
		now        time.Time
		exclude    bool   // exclude the sensor's date (not today's)
		jobRow     string // event of a job row for slaTestDate
		wantSFN    int
		wantStatus string
		wantEvent  types.EventDetailType
	}{
		{name: "before the cutoff", date: slaTestDate, now: at(10, 13, 0), wantSFN: 1,
			wantStatus: types.TriggerStatusRunning, wantEvent: types.EventTriggerRecovered},
		{name: "before the cutoff, job ran", date: slaTestDate, now: at(10, 13, 0), jobRow: types.JobEventSuccess},
		{name: "past the cutoff", date: slaTestDate, now: at(10, 14, 30),
			wantStatus: types.TriggerStatusFailedFinal, wantEvent: types.EventSensorDeadlineExpired},
		{name: "past the cutoff, excluded date", date: "2026-03-09", now: at(10, 14, 30), exclude: true},
		{name: "past the cutoff, exhausted launch", date: slaTestDate, now: at(10, 14, 30), jobRow: types.JobEventInfraTriggerExhausted,
			wantStatus: types.TriggerStatusFailedFinal, wantEvent: types.EventSensorDeadlineExpired},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, mem, rec := slaDeps(t, "", tt.jobRow, func(c *types.PipelineConfig) {
				c.SLA, c.Schedule.Trigger.Cutoff = nil, "14:00"
				if tt.exclude {
					c.Schedule.Exclude = &types.ExclusionConfig{Dates: []string{tt.date}}
				}
			})
			sfnFake := &countingSFN{}
			d.SFNClient, d.NowFunc = sfnFake, func() time.Time { return tt.now }
			pk := types.PipelinePK("p")
			mem.Put("control", map[string]ddbtypes.AttributeValue{
				"PK": &ddbtypes.AttributeValueMemberS{Value: pk}, "SK": &ddbtypes.AttributeValueMemberS{Value: types.SensorSK("upstream")},
				"data": &ddbtypes.AttributeValueMemberM{Value: map[string]ddbtypes.AttributeValue{"date": &ddbtypes.AttributeValueMemberS{Value: tt.date}}},
			})

			for pass := 0; pass < 2; pass++ {
				if err := reconcileSensorTriggers(context.Background(), d); err != nil {
					t.Fatalf("pass %d: %v", pass, err)
				}
			}

			var wantEvents []string
			if tt.wantEvent != "" {
				wantEvents = []string{string(tt.wantEvent)}
			}
			if sfnFake.calls != tt.wantSFN || !slices.Equal(rec.events, wantEvents) {
				t.Fatalf("StartExecution calls %d, events %v; want %d and %v", sfnFake.calls, rec.events, tt.wantSFN, wantEvents)
			}
			if len(rec.details) == 1 && (rec.details[0].Detail["source"] != "reconciliation" || rec.details[0].Date != tt.date) {
				t.Errorf("event %+v, want source reconciliation for %s", rec.details[0], tt.date)
			}
			var status string
			if row := mem.Get("control", pk, types.TriggerSK("stream", tt.date)); row != nil {
				status = row["status"].(*ddbtypes.AttributeValueMemberS).Value
			}
			if status != tt.wantStatus {
				t.Errorf("trigger status = %q, want %q", status, tt.wantStatus)
			}
		})
	}
}

// TestReconcile_RestartsAfterRelease: a date whose newest job row is
// evaluation-released was not ready within its evaluation window and had its
// lock released. Before the cutoff the reconciler starts it again and reports
// JOB_TRIGGERED, not TRIGGER_RECOVERED; past the cutoff it closes the date.
// A cron pipeline is reconciled the same way (cron is only its start-by
// time), under its own schedule ID, and one never started is recovered.
func TestReconcile_RestartsAfterRelease(t *testing.T) {
	at := func(hour, minute int) time.Time { return time.Date(2026, 3, 10, hour, minute, 0, 0, time.UTC) }
	triggered, recovered := string(types.EventJobTriggered), string(types.EventTriggerRecovered)
	tests := []struct {
		name       string
		now        time.Time
		cron       string // schedule.cron of the pipeline
		notStarted bool   // no evaluation-released job row
		olderRow   string // a job row older than the evaluation-released one
		wantSFN    int
		wantStatus string
		wantEvents []string
	}{
		{name: "released, before the cutoff", now: at(13, 0), wantSFN: 1, wantStatus: types.TriggerStatusRunning, wantEvents: []string{triggered}},
		{name: "released, past the cutoff", now: at(14, 30), wantStatus: types.TriggerStatusFailedFinal,
			wantEvents: []string{string(types.EventSensorDeadlineExpired)}},
		{name: "infra-trigger-exhausted then released", now: at(13, 0), olderRow: types.JobEventInfraTriggerExhausted,
			wantSFN: 1, wantStatus: types.TriggerStatusRunning, wantEvents: []string{triggered}},
		{name: "released, cron pipeline", now: at(13, 0), cron: "0 8 * * *",
			wantSFN: 1, wantStatus: types.TriggerStatusRunning, wantEvents: []string{triggered}},
		{name: "cron pipeline, never started", now: at(13, 0), cron: "0 8 * * *", notStarted: true,
			wantSFN: 1, wantStatus: types.TriggerStatusRunning, wantEvents: []string{recovered}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, mem, rec := slaDeps(t, "", "", func(c *types.PipelineConfig) {
				c.SLA, c.Schedule.Trigger.Cutoff, c.Schedule.Cron = nil, "14:00", tt.cron
			})
			sfnFake := &countingSFN{}
			d.SFNClient, d.NowFunc = sfnFake, func() time.Time { return tt.now }
			pk := types.PipelinePK("p")
			schedule := lambda.ResolveScheduleID(&types.PipelineConfig{Schedule: types.ScheduleConfig{Cron: tt.cron}})
			if tt.olderRow != "" {
				mem.Put("joblog", sItem("PK", pk, "SK", types.JobSK(schedule, slaTestDate, "1773129500000"), "event", tt.olderRow))
			}
			if !tt.notStarted {
				mem.Put("joblog", sItem("PK", pk, "SK", types.JobSK(schedule, slaTestDate, "1773129600000"), "event", types.JobEventEvaluationReleased))
			}
			mem.Put("control", map[string]ddbtypes.AttributeValue{
				"PK": &ddbtypes.AttributeValueMemberS{Value: pk}, "SK": &ddbtypes.AttributeValueMemberS{Value: types.SensorSK("upstream")},
				"data": &ddbtypes.AttributeValueMemberM{Value: map[string]ddbtypes.AttributeValue{"date": &ddbtypes.AttributeValueMemberS{Value: slaTestDate}}},
			})

			for pass := 0; pass < 2; pass++ {
				if err := reconcileSensorTriggers(context.Background(), d); err != nil {
					t.Fatalf("pass %d: %v", pass, err)
				}
			}

			if sfnFake.calls != tt.wantSFN || !slices.Equal(rec.events, tt.wantEvents) {
				t.Fatalf("StartExecution calls %d, events %v; want %d and %v", sfnFake.calls, rec.events, tt.wantSFN, tt.wantEvents)
			}
			wantDetail := map[string]interface{}{"source": "reconciliation", "actionHint": "evaluation restarted after its window closed"}
			wantMsg := "evaluation restarted for p/" + schedule + "/" + slaTestDate
			if rec.events[0] == recovered {
				wantDetail["actionHint"], wantMsg = "watchdog recovered missed sensor trigger", "trigger recovered for p/"+schedule+"/"+slaTestDate
			}
			if e := rec.details[0]; tt.wantSFN == 1 && (!maps.Equal(e.Detail, wantDetail) || e.Message != wantMsg) {
				t.Errorf("%s %q with %v, want %q with %v", rec.events[0], e.Message, e.Detail, wantMsg, wantDetail)
			}
			if row := mem.Get("control", pk, types.TriggerSK(schedule, slaTestDate)); row == nil || row["status"].(*ddbtypes.AttributeValueMemberS).Value != tt.wantStatus {
				t.Errorf("trigger row = %v, want status %q", row, tt.wantStatus)
			}
		})
	}
}

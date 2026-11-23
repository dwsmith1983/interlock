package watchdog

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sfn"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// countingSFN is a lambda.SFNAPI double that counts StartExecution calls,
// keeps each one's input and answers each with err.
type countingSFN struct {
	calls  int
	err    error
	inputs []string
}

func (c *countingSFN) StartExecution(_ context.Context, in *sfn.StartExecutionInput, _ ...func(*sfn.Options)) (*sfn.StartExecutionOutput, error) {
	c.calls++
	c.inputs = append(c.inputs, aws.ToString(in.Input))
	if c.err != nil {
		return nil, c.err
	}
	return &sfn.StartExecutionOutput{}, nil
}

// runningTriggerScan serves the Scan of store.ScanRunningTriggers from mem:
// the control rows whose SK begins with the :prefix value and whose status
// is the :running value. Any other Scan goes to fake's Scan as before.
func runningTriggerScan(fake *storetest.FakeDynamo, mem *storetest.Mem) {
	next := fake.ScanFn
	fake.ScanFn = func(ctx context.Context, in *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
		if aws.ToString(in.FilterExpression) != "begins_with(SK, :prefix) AND #status = :running" {
			return next(ctx, in)
		}
		value := func(v ddbtypes.AttributeValue) string {
			s, _ := v.(*ddbtypes.AttributeValueMemberS)
			if s == nil {
				return ""
			}
			return s.Value
		}
		prefix, running := value(in.ExpressionAttributeValues[":prefix"]), value(in.ExpressionAttributeValues[":running"])
		out := &dynamodb.ScanOutput{}
		for _, item := range mem.Items(aws.ToString(in.TableName)) {
			if strings.HasPrefix(value(item["SK"]), prefix) && value(item[in.ExpressionAttributeNames["#status"]]) == running {
				out.Items = append(out.Items, item)
			}
		}
		out.Count = int32(len(out.Items))
		return out, nil
	}
}

// TestDetectStaleTriggers: a RUNNING trigger row whose staleAt is before now
// gets one SFN_TIMEOUT and turns FAILED_FINAL without staleAt or ttl, also
// when its pipeline is now dry-run. A row with only a ttl is never stale. The
// clock is 09:00
// on slaTestDate (1773133200) and each row is on TriggerSK("stream",
// slaTestDate) of pipeline p unless a case says otherwise.
func TestDetectStaleTriggers(t *testing.T) {
	const now = 1773133200
	epoch := func(e int64) string { return strconv.FormatInt(e, 10) }
	tests := []struct {
		name    string
		pk, sk  string // "" means PipelinePK("p") and TriggerSK("stream", slaTestDate)
		status  string // "" means RUNNING
		staleAt string // raw N value; "" means no staleAt attribute
		ttl     string // raw N value; "" means no ttl attribute
		tweak   func(*types.PipelineConfig)
		stale   bool
	}{
		{name: "expired", staleAt: epoch(1773129600), stale: true},
		{name: "future", staleAt: epoch(now + 12*3600)},
		{name: "no staleAt"},
		{name: "staleAt equals now", staleAt: epoch(now)},
		{name: "bad PK", pk: "BADPREFIX#p", staleAt: epoch(1773129600)},
		{name: "bad SK", sk: "TRIGGER#nodatepart", staleAt: epoch(1773129600)},
		{name: "non-trigger SK", sk: types.SensorSK("upstream"), staleAt: epoch(1773129600)},
		{name: "dry run", staleAt: epoch(1773129600), tweak: dryRunHTTP, stale: true},
		{name: "expired COMPLETED", status: types.TriggerStatusCompleted, staleAt: epoch(1773129600)},
		{name: "ttl only, expired", ttl: epoch(1773129600)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var tweaks []func(*types.PipelineConfig)
			if tt.tweak != nil {
				tweaks = append(tweaks, tt.tweak)
			}
			d, mem, rec := slaDeps(t, "", "", tweaks...)
			runningTriggerScan(d.Store.Client.(*storetest.FakeDynamo), mem)
			d.NowFunc = func() time.Time { return time.Unix(now, 0).UTC() }
			pk, sk, status := cmp.Or(tt.pk, types.PipelinePK("p")), cmp.Or(tt.sk, types.TriggerSK("stream", slaTestDate)), cmp.Or(tt.status, types.TriggerStatusRunning)
			row := sItem("PK", pk, "SK", sk, "status", status)
			if tt.staleAt != "" {
				row["staleAt"] = &ddbtypes.AttributeValueMemberN{Value: tt.staleAt}
			}
			if tt.ttl != "" {
				row["ttl"] = &ddbtypes.AttributeValueMemberN{Value: tt.ttl}
			}
			mem.Put("control", row)

			if err := detectStaleTriggers(context.Background(), d); err != nil {
				t.Fatalf("detectStaleTriggers: %v", err)
			}

			got := mem.Get("control", pk, sk)
			if !tt.stale {
				if len(rec.events) != 0 || !maps.Equal(got, row) {
					t.Fatalf("events %v, row %v; want none and the row unchanged", rec.events, got)
				}
				return
			}
			want := map[string]interface{}{"source": "watchdog", "actionHint": "step function passed its stale deadline: check SFN execution history",
				"staleAt": "2026-03-10T08:00:00Z"}
			wantMsg := "step function timed out for p/stream/" + slaTestDate
			if len(rec.events) != 1 || rec.events[0] != string(types.EventSFNTimeout) || rec.details[0].Date != slaTestDate ||
				rec.details[0].Message != wantMsg || !maps.Equal(rec.details[0].Detail, want) {
				t.Fatalf("events %v with %+v, want one SFN_TIMEOUT for %s, %q with %v", rec.events, rec.details, slaTestDate, wantMsg, want)
			}
			_, hasStaleAt := got["staleAt"]
			if _, hasTTL := got["ttl"]; got["status"].(*ddbtypes.AttributeValueMemberS).Value != types.TriggerStatusFailedFinal || hasStaleAt || hasTTL {
				t.Errorf("trigger row = %v, want FAILED_FINAL without staleAt or ttl", got)
			}
		})
	}
}

// TestDetectStaleTriggers_PublishFailureRetries: a failed publish leaves the
// stale row RUNNING with its staleAt, so the next pass alerts and closes it.
func TestDetectStaleTriggers_PublishFailureRetries(t *testing.T) {
	d, mem, rec := slaDeps(t, "", "")
	runningTriggerScan(d.Store.Client.(*storetest.FakeDynamo), mem)
	d.NowFunc = func() time.Time { return time.Unix(1773133200, 0).UTC() }
	pk, sk := types.PipelinePK("p"), types.TriggerSK("stream", slaTestDate)
	row := sItem("PK", pk, "SK", sk, "status", types.TriggerStatusRunning)
	row["staleAt"] = &ddbtypes.AttributeValueMemberN{Value: "1773129600"}
	mem.Put("control", row)
	rec.failEvents = 1

	if err := detectStaleTriggers(context.Background(), d); err != nil {
		t.Fatalf("pass 1: %v", err)
	}
	if got := mem.Get("control", pk, sk); len(rec.events) != 0 || !maps.Equal(got, row) {
		t.Fatalf("pass 1: events %v, row %v; want none and the row unchanged", rec.events, got)
	}

	if err := detectStaleTriggers(context.Background(), d); err != nil {
		t.Fatalf("pass 2: %v", err)
	}
	got := mem.Get("control", pk, sk)
	_, hasStaleAt := got["staleAt"]
	if !slices.Equal(rec.events, []string{string(types.EventSFNTimeout)}) ||
		got["status"].(*ddbtypes.AttributeValueMemberS).Value != types.TriggerStatusFailedFinal || hasStaleAt {
		t.Fatalf("pass 2: events %v, row %v; want one SFN_TIMEOUT and FAILED_FINAL without staleAt", rec.events, got)
	}
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
		{name: "before the cutoff, job failed", date: slaTestDate, now: at(10, 13, 0), jobRow: types.JobEventFail},
		{name: "before the cutoff, job timed out", date: slaTestDate, now: at(10, 13, 0), jobRow: types.JobEventTimeout},
		{name: "before the cutoff, launch failed", date: slaTestDate, now: at(10, 13, 0), jobRow: types.JobEventInfraTriggerFailure, wantSFN: 1,
			wantStatus: types.TriggerStatusRunning, wantEvent: types.EventTriggerRecovered},
		{name: "past the cutoff", date: slaTestDate, now: at(10, 14, 30),
			wantStatus: types.TriggerStatusFailedFinal, wantEvent: types.EventSensorDeadlineExpired},
		{name: "past the cutoff, excluded date", date: "2026-03-09", now: at(10, 14, 30), exclude: true},
		{name: "past the cutoff, exhausted job row, no trigger row", date: slaTestDate, now: at(10, 14, 30), jobRow: types.JobEventInfraTriggerExhausted,
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

// TestReconcile_Gating: what keeps the reconciler from starting a run for a
// sensor, and the per-hour dates it starts. No SLA and no cutoff; the
// trigger is upstream exists unless a case says otherwise. A launch that ran
// out of retries ends FAILED_FINAL, which keeps the date closed with or
// without its job rows.
func TestReconcile_Gating(t *testing.T) {
	daily := map[string]map[string]interface{}{"upstream": {"date": slaTestDate}}
	hourly := map[string]map[string]interface{}{
		"upstream#2026-03-10T10": {"date": slaTestDate, "hour": "10"},
		"upstream#2026-03-10T11": {"date": slaTestDate, "hour": "11"},
	}
	tests := []struct {
		name      string
		sensors   map[string]map[string]interface{}
		running   string // date of a RUNNING trigger row present before the pass
		trigger   string // status of a trigger row for slaTestDate
		jobRow    string // event of a job row for slaTestDate
		tweak     func(*types.PipelineConfig)
		sfnErr    error
		wantSFN   int
		wantDates []string // dates with a TRIGGER_RECOVERED and a trigger row
	}{
		{name: "condition not met", sensors: map[string]map[string]interface{}{"upstream": {"status": "pending", "date": slaTestDate}},
			tweak: func(c *types.PipelineConfig) {
				c.Schedule.Trigger = &types.TriggerCondition{Key: "upstream", Check: types.CheckEquals, Field: "status", Value: "ready"}
			}},
		{name: "today excluded", sensors: map[string]map[string]interface{}{"upstream": {"date": "2026-03-09"}},
			tweak: func(c *types.PipelineConfig) {
				c.Schedule.Exclude = &types.ExclusionConfig{Dates: []string{slaTestDate}}
			}},
		{name: "per-hour both", sensors: hourly, wantSFN: 2, wantDates: []string{"2026-03-10T10", "2026-03-10T11"}},
		{name: "per-hour partial", sensors: hourly, running: "2026-03-10T10", wantSFN: 1, wantDates: []string{"2026-03-10T11"}},
		{name: "nil trigger", sensors: daily, tweak: func(c *types.PipelineConfig) { c.Schedule.Trigger = nil }},
		{name: "SFN error", sensors: daily, sfnErr: errors.New("execution limit exceeded"), wantSFN: 1},
		{name: "dry run", sensors: daily, tweak: dryRunHTTP},
		{name: "exhausted launch", sensors: daily, trigger: types.TriggerStatusFailedFinal, jobRow: types.JobEventInfraTriggerExhausted},
		{name: "FAILED_FINAL row, no job row", sensors: daily, trigger: types.TriggerStatusFailedFinal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, mem, rec := slaDeps(t, tt.trigger, tt.jobRow, func(c *types.PipelineConfig) {
				c.SLA = nil
				if tt.tweak != nil {
					tt.tweak(c)
				}
			})
			sfnFake := &countingSFN{err: tt.sfnErr}
			d.SFNClient = sfnFake
			ctx, pk := context.Background(), types.PipelinePK("p")
			for key, data := range tt.sensors {
				if err := d.Store.WriteSensor(ctx, "p", key, data); err != nil {
					t.Fatalf("write sensor %s: %v", key, err)
				}
			}
			wantRows := slices.Clone(tt.wantDates)
			if tt.trigger != "" {
				wantRows = append(wantRows, slaTestDate)
			}
			if tt.running != "" {
				mem.Put("control", sItem("PK", pk, "SK", types.TriggerSK("stream", tt.running), "status", types.TriggerStatusRunning))
				wantRows = append(wantRows, tt.running)
			}

			if err := reconcileSensorTriggers(ctx, d); err != nil {
				t.Fatalf("reconcileSensorTriggers: %v", err)
			}

			var gotDates []string
			for i, evt := range rec.details {
				if rec.events[i] != string(types.EventTriggerRecovered) {
					t.Errorf("event %d = %s, want TRIGGER_RECOVERED", i, rec.events[i])
				}
				gotDates = append(gotDates, evt.Date)
			}
			slices.Sort(gotDates)
			if sfnFake.calls != tt.wantSFN || !slices.Equal(gotDates, tt.wantDates) {
				t.Fatalf("StartExecution calls %d, recovered dates %v; want %d and %v", sfnFake.calls, gotDates, tt.wantSFN, tt.wantDates)
			}
			var gotRows []string
			for _, item := range mem.Items("control") {
				if sk := item["SK"].(*ddbtypes.AttributeValueMemberS).Value; strings.HasPrefix(sk, "TRIGGER#") {
					gotRows = append(gotRows, strings.TrimPrefix(sk, "TRIGGER#stream#"))
				}
			}
			slices.Sort(wantRows)
			if !slices.Equal(gotRows, wantRows) {
				t.Errorf("trigger rows for %v, want %v", gotRows, wantRows)
			}
		})
	}
}

// TestReconcile_RecordsFirstArrival: a run the reconciler starts records the
// date's first sensor arrival at the pass's clock, the start of its relative
// SLA, and passes it to the execution as sensorArrivalAt. An arrival already
// recorded is kept, and an hourly date records under its hourly key. A failed
// write is logged and the run starts without sensorArrivalAt. When another
// start holds the trigger lock, nothing is written.
func TestReconcile_RecordsFirstArrival(t *testing.T) {
	const clock = "2026-03-10T09:00:00Z" // slaDeps' clock
	tests := []struct {
		name, seeded string
		hour         string // hour field of the trigger sensor
		lockHeld     bool
		writeFails   bool     // the arrival write fails
		wantArrival  string   // empty means no arrival row
		wantInputs   []string // sensorArrivalAt of each started execution
	}{
		{name: "recovered", wantArrival: clock, wantInputs: []string{clock}},
		{name: "kept", seeded: "2026-03-10T08:00:00Z", wantArrival: "2026-03-10T08:00:00Z", wantInputs: []string{"2026-03-10T08:00:00Z"}},
		{name: "hourly", hour: "07", wantArrival: clock, wantInputs: []string{clock}},
		{name: "write fails", writeFails: true, wantInputs: []string{""}},
		{name: "lock held", lockHeld: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, _, _ := slaDeps(t, "", "")
			sfnFake := &countingSFN{}
			var logs bytes.Buffer
			d.SFNClient, d.Logger = sfnFake, slog.New(slog.NewTextHandler(&logs, nil))
			date, sensor := slaTestDate, map[string]interface{}{"date": slaTestDate}
			if tt.hour != "" {
				date, sensor["hour"] = slaTestDate+"T"+tt.hour, tt.hour
			}
			ctx, arrivalKey := context.Background(), "first-sensor-arrival#"+date
			if err := d.Store.WriteSensor(ctx, "p", "upstream", sensor); err != nil {
				t.Fatalf("write sensor: %v", err)
			}
			if tt.seeded != "" {
				if err := d.Store.WriteSensor(ctx, "p", arrivalKey, map[string]interface{}{"arrivedAt": tt.seeded}); err != nil {
					t.Fatalf("write arrival: %v", err)
				}
			}
			// With lockHeld, the stream-router takes the lock between the
			// trigger check and the reconciler's lock attempt.
			fake := d.Store.Client.(*storetest.FakeDynamo)
			next := fake.PutItemFn
			fake.PutItemFn = func(ctx context.Context, in *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
				sk, _ := in.Item["SK"].(*ddbtypes.AttributeValueMemberS)
				switch {
				case sk == nil:
				case tt.lockHeld && strings.HasPrefix(sk.Value, "TRIGGER#"):
					return nil, &ddbtypes.ConditionalCheckFailedException{Message: aws.String("lock held")}
				case tt.writeFails && sk.Value == types.SensorSK(arrivalKey):
					return nil, errors.New("put item failed")
				}
				return next(ctx, in)
			}

			if err := reconcileSensorTriggers(ctx, d); err != nil {
				t.Fatalf("reconcileSensorTriggers: %v", err)
			}

			got, err := d.Store.GetSensorData(ctx, "p", arrivalKey)
			if err != nil {
				t.Fatalf("read arrival: %v", err)
			}
			if gotArrival, _ := got["arrivedAt"].(string); gotArrival != tt.wantArrival {
				t.Errorf("arrivedAt under %s = %q, want %q", arrivalKey, gotArrival, tt.wantArrival)
			}
			var gotInputs []string
			for _, in := range sfnFake.inputs {
				var input struct {
					SensorArrivalAt string `json:"sensorArrivalAt"`
				}
				if err := json.Unmarshal([]byte(in), &input); err != nil {
					t.Fatalf("decode execution input %q: %v", in, err)
				}
				gotInputs = append(gotInputs, input.SensorArrivalAt)
			}
			if !slices.Equal(gotInputs, tt.wantInputs) {
				t.Errorf("execution sensorArrivalAt = %q, want %q", gotInputs, tt.wantInputs)
			}
			warned := strings.Contains(logs.String(), `level=WARN msg="failed to write first-sensor-arrival"`)
			if warned != tt.writeFails {
				t.Errorf("arrival write warning logged = %v, want %v; logs:\n%s", warned, tt.writeFails, logs.String())
			}
		})
	}
}

// TestReconcile_RecoveredRunBreachesRelativeSLA: a run the reconciler starts
// at 09:00 with a 1h maxDuration that has not finished by 10:01 gets one
// RELATIVE_SLA_BREACH for its date.
func TestReconcile_RecoveredRunBreachesRelativeSLA(t *testing.T) {
	d, _, rec := slaDeps(t, "", "")
	d.SFNClient = &countingSFN{}
	now := time.Date(2026, 3, 10, 9, 0, 0, 0, time.UTC)
	d.NowFunc = func() time.Time { return now }
	ctx := context.Background()
	if err := d.Store.WriteSensor(ctx, "p", "upstream", map[string]interface{}{"date": slaTestDate}); err != nil {
		t.Fatalf("write sensor: %v", err)
	}

	if err := reconcileSensorTriggers(ctx, d); err != nil {
		t.Fatalf("reconcileSensorTriggers: %v", err)
	}
	now = now.Add(61 * time.Minute)
	if err := detectRelativeSLABreaches(ctx, d); err != nil {
		t.Fatalf("detectRelativeSLABreaches: %v", err)
	}

	var breaches []string
	for i, evt := range rec.events {
		if evt == string(types.EventRelativeSLABreach) {
			breaches = append(breaches, rec.details[i].Date)
		}
	}
	if !slices.Equal(breaches, []string{slaTestDate}) {
		t.Errorf("RELATIVE_SLA_BREACH dates = %v (events %v), want [%s]", breaches, rec.events, slaTestDate)
	}
}

package stream

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/store"
	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestParseRerunRequestSK: the date of a RERUN_REQUEST# row goes into the
// execution name, so only a date the system writes itself passes.
func TestParseRerunRequestSK(t *testing.T) {
	for _, tt := range []struct{ schedule, date string }{
		{"stream", "2026-03-07"},
		{"cron", "2026-03-07T23"},
	} {
		schedule, date, err := parseRerunRequestSK(types.RerunRequestSK(tt.schedule, tt.date))
		if err != nil || schedule != tt.schedule || date != tt.date {
			t.Errorf("parse(%q, %q) = %q, %q, %v", tt.schedule, tt.date, schedule, date, err)
		}
	}
	for _, sk := range []string{
		"RERUN_REQUEST#stream",                   // no date
		"RERUN_REQUEST#stream#2026-3-7",          // date not YYYY-MM-DD
		"RERUN_REQUEST#stream#2026-03-07T24",     // hour out of range
		"RERUN_REQUEST#stream#2026-03-07 manual", // trailing text
		"RERUN_REQUEST#stream#2026-03-07#extra",  // extra segment
		"RERUN_REQUEST#stream#2026-03-07T10#x",   // hourly date with extra segment
	} {
		if s, d, err := parseRerunRequestSK(sk); err == nil {
			t.Errorf("parse(%q) = %q, %q, want an error", sk, s, d)
		}
	}
}

// TestHandleRerunRequest_SkipsBadRows: a RERUN_REQUEST# row with a bad SK or
// a schedule the pipeline does not use is skipped with a warning. Returning an
// error would retry the row, which can never succeed, and stall the shard.
func TestHandleRerunRequest_SkipsBadRows(t *testing.T) {
	for _, sk := range []string{
		"RERUN_REQUEST#stream",                     // no date
		"RERUN_REQUEST#stream#2026-03-07T24",       // bad date
		types.RerunRequestSK("cron", testDate),     // the pipeline has no cron
		types.RerunRequestSK("", testDate),         // empty schedule
		types.RerunRequestSK("stream/x", testDate), // forbidden character
	} {
		t.Run(sk, func(t *testing.T) {
			fake, mem := storetest.NewMem()
			mem.Put("control", configScanItem(t))
			s, sfnFake, logs := storetest.NewStore(fake), &countingSFN{}, &bytes.Buffer{}
			d := &lambda.Deps{Store: s, ConfigCache: store.NewConfigCache(s, 5*time.Minute), SFNClient: sfnFake,
				NowFunc: testNow, Logger: slog.New(slog.NewTextHandler(logs, nil))}
			record := events.DynamoDBEventRecord{EventName: "INSERT", Change: events.DynamoDBStreamRecord{
				NewImage: map[string]events.DynamoDBAttributeValue{"reason": events.NewStringAttribute("manual")},
			}}

			if err := handleRerunRequest(context.Background(), d, types.PipelinePK("p"), sk, record); err != nil {
				t.Fatalf("handleRerunRequest = %v, want nil", err)
			}
			if sfnFake.calls != 0 {
				t.Errorf("StartExecution called %d times, want 0", sfnFake.calls)
			}
			if !strings.Contains(logs.String(), "level=WARN") {
				t.Errorf("logged %q, want a warning", logs.String())
			}
		})
	}
}

// TestRerunRequest_WatchWindow: an automatic (late-data or data-drift) rerun
// request for a COMPLETED date is re-checked against the post-run watch
// window (6h from completedAt) when it is handled; outside it, it is skipped
// with no start or row and publishes only MANUAL_RERUN_REQUIRED, blocked by
// the window as late data after it is (nothing without a completedAt or for
// an excluded date). A manual request is never checked.
func TestRerunRequest_WatchWindow(t *testing.T) {
	const date = "2026-03-10"
	tests := []struct {
		name        string
		status      string // trigger row; empty means COMPLETED
		completedAt string
		reason      string
		now         string
		failLookup  bool
		excluded    bool // the date is calendar-excluded
		spent       bool // the rerun budget is spent
		wantStart   bool
	}{
		{name: "late-data inside (guard)", completedAt: "2026-03-10T12:00:00Z", reason: "late-data", now: "2026-03-10T17:59:59Z", wantStart: true},
		{name: "late-data after", completedAt: "2026-03-10T12:00:00Z", reason: "late-data", now: "2026-03-10T18:00:00Z"},
		{name: "late-data after, excluded date", completedAt: "2026-03-10T12:00:00Z", reason: "late-data", now: "2026-03-10T18:00:00Z", excluded: true},
		{name: "late-data after, budget spent", completedAt: "2026-03-10T12:00:00Z", reason: "late-data", now: "2026-03-10T18:00:00Z", spent: true},
		{name: "data-drift after", completedAt: "2026-03-10T12:00:00Z", reason: "data-drift", now: "2026-03-10T19:00:00Z"},
		{name: "manual after (guard)", completedAt: "2026-03-10T12:00:00Z", reason: "manual", now: "2026-03-12T09:00:00Z", wantStart: true},
		{name: "late-data, no completedAt", reason: "late-data", now: "2026-03-10T13:00:00Z"},
		{name: "late-data, row FAILED_FINAL (guard)", status: types.TriggerStatusFailedFinal, reason: "late-data", now: "2026-03-10T13:00:00Z", wantStart: true},
		{name: "trigger lookup fails", completedAt: "2026-03-10T12:00:00Z", reason: "late-data", now: "2026-03-10T13:00:00Z", failLookup: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := types.PipelineConfig{
				Pipeline: types.PipelineIdentity{ID: "p"},
				Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists, Cutoff: "23:59", CutoffDayOffset: 7}},
				Job:      types.JobConfig{Type: types.TriggerHTTP, Config: httpJobConfig},
				PostRun:  &types.PostRunConfig{WatchWindow: "6h"},
			}
			if tt.excluded {
				cfg.Schedule.Exclude = &types.ExclusionConfig{Dates: []string{date}}
			}
			d, mem, sfnFake, eb := jobWorld(t, cfg)
			row := completedRow(date, tt.completedAt)
			if tt.status != "" {
				row["status"] = &ddbtypes.AttributeValueMemberS{Value: tt.status}
			}
			mem.Put("control", row)
			mem.Put("joblog", map[string]ddbtypes.AttributeValue{
				"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("p")},
				"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("stream", date, "1773129600000")},
				"event": &ddbtypes.AttributeValueMemberS{Value: types.JobEventSuccess}})
			// Mem has no FilterExpression, so rerun-table queries (the budget
			// counts) answer zero, or one when the budget is spent.
			fake := d.Store.Client.(*storetest.FakeDynamo)
			query := fake.QueryFn
			fake.QueryFn = func(ctx context.Context, in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
				if *in.TableName == "rerun" {
					if tt.spent {
						return &dynamodb.QueryOutput{Count: 1}, nil
					}
					return &dynamodb.QueryOutput{}, nil
				}
				return query(ctx, in)
			}
			if tt.failLookup {
				get := fake.GetItemFn
				fake.GetItemFn = func(ctx context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
					if sk := in.Key["SK"].(*ddbtypes.AttributeValueMemberS).Value; strings.HasPrefix(sk, "TRIGGER#") {
						return nil, errors.New("get failed")
					}
					return get(ctx, in)
				}
			}
			now := mustTime(t, tt.now)
			d.NowFunc = func() time.Time { return now }
			record := events.DynamoDBEventRecord{EventName: "INSERT", Change: events.DynamoDBStreamRecord{
				SequenceNumber: "1",
				Keys: map[string]events.DynamoDBAttributeValue{
					"PK": events.NewStringAttribute(types.PipelinePK("p")),
					"SK": events.NewStringAttribute(types.RerunRequestSK("stream", date)),
				},
				NewImage: map[string]events.DynamoDBAttributeValue{"reason": events.NewStringAttribute(tt.reason)},
			}}

			err := handleRecord(context.Background(), d, record)
			if (err != nil) != tt.failLookup {
				t.Fatalf("handleRecord = %v, want an error: %v", err, tt.failLookup)
			}

			var gotEvents []string
			for _, e := range eb.events {
				gotEvents = append(gotEvents, e.Type)
			}
			wantStarts, wantEvents, wantJobs, wantStatus := 0, []string(nil), []string{types.JobEventSuccess}, cmp.Or(tt.status, types.TriggerStatusCompleted)
			if tt.wantStart {
				wantStarts, wantEvents = 1, []string{string(types.EventRerunAccepted)}
				wantJobs, wantStatus = append(wantJobs, types.JobEventRerunAccepted), types.TriggerStatusRunning
			}
			if !tt.wantStart && tt.completedAt != "" && !tt.failLookup && !tt.excluded {
				wantEvents = []string{string(types.EventManualRerunRequired)}
			}
			if sfnFake.calls != wantStarts || !slices.Equal(gotEvents, wantEvents) {
				t.Errorf("StartExecution calls %d, events %v; want %d and %v", sfnFake.calls, gotEvents, wantStarts, wantEvents)
			}
			for _, e := range eb.events {
				if e.Type == evManual && e.Detail["blockedBy"] != "postRun.watchWindow" {
					t.Errorf("MANUAL_RERUN_REQUIRED blockedBy = %v, want postRun.watchWindow", e.Detail["blockedBy"])
				}
			}
			if got := jobEvents(mem); !slices.Equal(got, wantJobs) {
				t.Errorf("job rows = %v, want %v", got, wantJobs)
			}
			if got, want := len(mem.Items("rerun")), wantStarts; got != want {
				t.Errorf("rerun rows = %d, want %d", got, want)
			}
			status := mem.Get("control", types.PipelinePK("p"), types.TriggerSK("stream", date))["status"].(*ddbtypes.AttributeValueMemberS).Value
			if status != wantStatus {
				t.Errorf("trigger status = %q, want %q", status, wantStatus)
			}
		})
	}
}

// rerunCountQuery answers the rerun-table budget counts (the queries with a
// FilterExpression, which Mem does not support) from mem's rerun rows: those
// under :pk whose SK begins with :prefix and whose reason is one of :s0..:sN.
// Every other query goes to Mem.
func rerunCountQuery(fake *storetest.FakeDynamo, mem *storetest.Mem) {
	query := fake.QueryFn
	fake.QueryFn = func(ctx context.Context, in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		if *in.TableName != "rerun" || in.FilterExpression == nil {
			return query(ctx, in)
		}
		values := in.ExpressionAttributeValues
		var reasons []string
		for i := 0; values[":s"+strconv.Itoa(i)] != nil; i++ {
			reasons = append(reasons, strAttr(values[":s"+strconv.Itoa(i)]))
		}
		var n int32
		for _, row := range mem.Items("rerun") {
			if strAttr(row["PK"]) == strAttr(values[":pk"]) && strings.HasPrefix(strAttr(row["SK"]), strAttr(values[":prefix"])) &&
				slices.Contains(reasons, strAttr(row["reason"])) {
				n++
			}
		}
		return &dynamodb.QueryOutput{Count: n}, nil
	}
}

// strAttr is a string attribute's value, or "" for anything else.
func strAttr(v ddbtypes.AttributeValue) string {
	if s, ok := v.(*ddbtypes.AttributeValueMemberS); ok {
		return s.Value
	}
	return ""
}

// TestRerunRequest_Outcomes: what a RERUN_REQUEST does for a date whose run
// finished. Unless a row says otherwise the trigger row is COMPLETED at 12:00,
// now is 13:00 (inside the 6h watch window), the last job row (at 08:00) is a
// success and the "upstream" sensor was updated after it (09:00). An accepted
// request starts one run, writes a rerun row and a rerun-accepted job row,
// deletes the date's post-run baseline and sets the trigger RUNNING; a
// rejected one writes no job row and changes nothing else. Calendar exclusion
// is checked on the request's date, not the wall-clock date.
func TestRerunRequest_Outcomes(t *testing.T) {
	const (
		date      = "2026-03-10"
		noTrigger = "none" // the date has no trigger row
		noJob     = "none" // the date has no job row
		fresh     = "1773133200000"
		stale     = "1773100800000"
	)
	accepted, rejected := []string{string(types.EventRerunAccepted)}, []string{string(types.EventRerunRejected)}
	success, fail := types.JobEventSuccess, types.JobEventFail
	running, completed := types.TriggerStatusRunning, types.TriggerStatusCompleted
	budgets := func(drift, manual int) func(*types.PipelineConfig) {
		return func(c *types.PipelineConfig) {
			c.Job.MaxDriftReruns = &drift
			c.Job.MaxManualReruns = &manual
		}
	}
	tests := []struct {
		name, reason, trigger, job string
		updatedAt                  any
		noSensor                   bool
		reruns                     []string
		tweak                      func(*types.PipelineConfig)
		sfnFail                    int
		wantErr                    bool
		wantStarts                 int
		wantEvents, wantJobs       []string
		wantReruns                 int
		wantStatus                 string // trigger row at the end; empty means none
		wantMsg                    string // the first event's message, if set
		now                        string // empty means 2026-03-10T13:00:00Z
	}{
		{name: "success, fresh sensor", reason: "manual", updatedAt: fresh,
			wantStarts: 1, wantEvents: accepted, wantJobs: []string{success, types.JobEventRerunAccepted}, wantReruns: 1, wantStatus: running},
		{name: "success, stale sensor", reason: "manual", updatedAt: stale,
			wantEvents: rejected, wantJobs: []string{success}, wantStatus: completed,
			wantMsg: "rerun rejected for p: previous run succeeded and no sensor data has changed"},
		{name: "success, fresh RFC3339 sensor", reason: "manual", updatedAt: "2026-03-10T09:00:00Z",
			wantStarts: 1, wantEvents: accepted, wantJobs: []string{success, types.JobEventRerunAccepted}, wantReruns: 1, wantStatus: running},
		{name: "success, stale RFC3339 sensor", reason: "manual", updatedAt: "2026-03-10T07:00:00Z",
			wantEvents: rejected, wantJobs: []string{success}, wantStatus: completed,
			wantMsg: "rerun rejected for p: previous run succeeded and no sensor data has changed"},
		{name: "fail, stale sensor", reason: "manual", trigger: types.TriggerStatusFailedFinal, job: fail, updatedAt: stale,
			wantStarts: 1, wantEvents: accepted, wantJobs: []string{fail, types.JobEventRerunAccepted}, wantReruns: 1, wantStatus: running},
		{name: "first drift", reason: "data-drift", tweak: budgets(1, 1),
			wantStarts: 1, wantEvents: accepted, wantJobs: []string{success, types.JobEventRerunAccepted}, wantReruns: 1, wantStatus: running},
		{name: "second drift", reason: "data-drift", reruns: []string{"data-drift"}, tweak: budgets(1, 1),
			wantEvents: rejected, wantJobs: []string{success}, wantReruns: 1, wantStatus: completed},
		{name: "late-data vs drift row", reason: "late-data", reruns: []string{"data-drift"}, tweak: budgets(1, 1),
			wantEvents: rejected, wantJobs: []string{success}, wantReruns: 1, wantStatus: completed},
		{name: "manual after drift spent", reason: "manual", reruns: []string{"data-drift"}, tweak: budgets(1, 2),
			wantStarts: 1, wantEvents: accepted, wantJobs: []string{success, types.JobEventRerunAccepted}, wantReruns: 2, wantStatus: running},
		{name: "second manual", reason: "manual", reruns: []string{"data-drift", "manual"}, tweak: budgets(1, 2),
			wantStarts: 1, wantEvents: accepted, wantJobs: []string{success, types.JobEventRerunAccepted}, wantReruns: 3, wantStatus: running},
		{name: "third manual", reason: "manual", reruns: []string{"data-drift", "manual", "manual"}, tweak: budgets(1, 2),
			wantEvents: rejected, wantJobs: []string{success}, wantReruns: 3, wantStatus: completed},
		{name: "no trigger row", reason: "manual", trigger: noTrigger, job: fail,
			wantEvents: []string{string(types.EventInfraFailure)}, wantJobs: []string{fail}},
		{name: "fail, no sensor", reason: "manual", job: fail, noSensor: true,
			wantStarts: 1, wantEvents: accepted, wantJobs: []string{fail, types.JobEventRerunAccepted}, wantReruns: 1, wantStatus: running},
		{name: "infra-trigger-exhausted", reason: "manual", trigger: types.TriggerStatusFailedFinal, job: types.JobEventInfraTriggerExhausted, updatedAt: stale, wantStarts: 1, wantEvents: accepted,
			wantJobs: []string{types.JobEventInfraTriggerExhausted, types.JobEventRerunAccepted}, wantReruns: 1, wantStatus: running},
		{name: "timeout", reason: "manual", job: types.JobEventTimeout, updatedAt: stale, wantStarts: 1, wantEvents: accepted,
			wantJobs: []string{types.JobEventTimeout, types.JobEventRerunAccepted}, wantReruns: 1, wantStatus: running},
		{name: "no job row", reason: "manual", job: noJob, updatedAt: stale,
			wantStarts: 1, wantEvents: accepted, wantJobs: []string{types.JobEventRerunAccepted}, wantReruns: 1, wantStatus: running},
		{name: "unknown job event", reason: "manual", job: "some-future-event", updatedAt: stale, wantStarts: 1, wantEvents: accepted,
			wantJobs: []string{"some-future-event", types.JobEventRerunAccepted}, wantReruns: 1, wantStatus: running},
		{name: "no config", reason: "manual", tweak: func(c *types.PipelineConfig) { c.Pipeline.ID = "other" },
			wantJobs: []string{success}, wantStatus: completed},
		{name: "start fails", reason: "manual", job: fail, sfnFail: 1, wantErr: true,
			wantJobs: []string{fail}, wantStatus: completed},
		{name: "data-drift vs late-data row", reason: "data-drift", reruns: []string{"late-data"}, tweak: budgets(1, 1),
			wantEvents: rejected, wantJobs: []string{success}, wantReruns: 1, wantStatus: completed,
			wantMsg: "rerun rejected for p: drift rerun limit exceeded"},
		{name: "manual spent", reason: "manual", reruns: []string{"manual"}, tweak: budgets(1, 1),
			wantEvents: rejected, wantJobs: []string{success}, wantReruns: 1, wantStatus: completed,
			wantMsg: "rerun rejected for p: manual rerun limit exceeded"},
		{name: "excluded date", reason: "manual", tweak: func(c *types.PipelineConfig) { c.Schedule.Exclude = &types.ExclusionConfig{Dates: []string{date}} },
			wantEvents: []string{string(types.EventPipelineExcluded)}, wantJobs: []string{success}, wantStatus: completed},
		{name: "wall clock excluded, request date not (guard)", reason: "manual", now: "2026-03-11T13:00:00Z",
			tweak: func(c *types.PipelineConfig) {
				c.Schedule.Exclude = &types.ExclusionConfig{Dates: []string{"2026-03-11"}}
			},
			wantStarts: 1, wantEvents: accepted, wantJobs: []string{success, types.JobEventRerunAccepted}, wantReruns: 1, wantStatus: running},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := types.PipelineConfig{
				Pipeline: types.PipelineIdentity{ID: "p"},
				Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists, Cutoff: "23:59", CutoffDayOffset: 7}},
				Job:      types.JobConfig{Type: types.TriggerHTTP, Config: httpJobConfig},
				PostRun:  &types.PostRunConfig{WatchWindow: "6h"},
			}
			if tt.tweak != nil {
				tt.tweak(&cfg)
			}
			d, mem, sfnFake, eb := jobWorld(t, cfg)
			pk := &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("p")}
			if tt.trigger != noTrigger {
				row := completedRow(date, "2026-03-10T12:00:00Z")
				row["status"] = &ddbtypes.AttributeValueMemberS{Value: cmp.Or(tt.trigger, completed)}
				mem.Put("control", row)
			}
			if tt.job != noJob {
				mem.Put("joblog", map[string]ddbtypes.AttributeValue{"PK": pk,
					"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("stream", date, "1773129600000")},
					"event": &ddbtypes.AttributeValueMemberS{Value: cmp.Or(tt.job, success)}})
			}
			for i, reason := range tt.reruns {
				mem.Put("rerun", map[string]ddbtypes.AttributeValue{"PK": pk,
					"SK":     &ddbtypes.AttributeValueMemberS{Value: types.RerunSK("stream", date, i)},
					"reason": &ddbtypes.AttributeValueMemberS{Value: reason}})
			}
			ctx := context.Background()
			updatedAt := tt.updatedAt
			if updatedAt == nil {
				updatedAt = fresh
			}
			if !tt.noSensor {
				if err := d.Store.WriteSensor(ctx, "p", "upstream", map[string]interface{}{"status": "ready", "updatedAt": updatedAt}); err != nil {
					t.Fatal(err)
				}
			}
			baselineKey := "postrun-baseline#" + date
			if err := d.Store.WriteSensor(ctx, "p", baselineKey, map[string]interface{}{"upstream": map[string]interface{}{"count": 1}}); err != nil {
				t.Fatal(err)
			}
			rerunCountQuery(d.Store.Client.(*storetest.FakeDynamo), mem)
			sfnFake.fail = tt.sfnFail
			now := mustTime(t, cmp.Or(tt.now, "2026-03-10T13:00:00Z"))
			d.NowFunc = func() time.Time { return now }
			record := events.DynamoDBEventRecord{EventName: "INSERT", Change: events.DynamoDBStreamRecord{
				SequenceNumber: "1",
				Keys: map[string]events.DynamoDBAttributeValue{
					"PK": events.NewStringAttribute(types.PipelinePK("p")),
					"SK": events.NewStringAttribute(types.RerunRequestSK("stream", date)),
				},
				NewImage: map[string]events.DynamoDBAttributeValue{"reason": events.NewStringAttribute(tt.reason)},
			}}

			if err := handleRecord(ctx, d, record); (err != nil) != tt.wantErr {
				t.Fatalf("handleRecord error = %v, want an error: %v", err, tt.wantErr)
			}

			var gotEvents []string
			for _, e := range eb.events {
				gotEvents = append(gotEvents, e.Type)
			}
			if sfnFake.calls != tt.wantStarts || !slices.Equal(gotEvents, tt.wantEvents) {
				t.Errorf("StartExecution calls %d, events %v; want %d and %v", sfnFake.calls, gotEvents, tt.wantStarts, tt.wantEvents)
			}
			if tt.wantMsg != "" && len(eb.events) > 0 && eb.events[0].Message != tt.wantMsg {
				t.Errorf("message = %q, want %q", eb.events[0].Message, tt.wantMsg)
			}
			baseline, err := d.Store.GetSensorData(ctx, "p", baselineKey)
			if err != nil {
				t.Fatal(err)
			}
			if gone, want := baseline == nil, slices.Equal(tt.wantEvents, accepted); gone != want {
				t.Errorf("post-run baseline deleted = %v, want %v", gone, want)
			}
			if got := jobEvents(mem); !slices.Equal(got, tt.wantJobs) {
				t.Errorf("job rows = %v, want %v", got, tt.wantJobs)
			}
			if got := len(mem.Items("rerun")); got != tt.wantReruns {
				t.Errorf("rerun rows = %d, want %d", got, tt.wantReruns)
			}
			var status string
			if row := mem.Get("control", pk.Value, types.TriggerSK("stream", date)); row != nil {
				status = row["status"].(*ddbtypes.AttributeValueMemberS).Value
			}
			if status != tt.wantStatus {
				t.Errorf("trigger status = %q, want %q", status, tt.wantStatus)
			}
		})
	}
}

// TestCheckSensorFreshness: the rerun circuit breaker lets a rerun through
// when any sensor was updated after the job row's timestamp (epoch
// milliseconds, the last SK segment). updatedAt may be RFC3339, or a number or
// numeric string in seconds or milliseconds; one it cannot read counts as
// absent and is logged. No sensors, no readable updatedAt at all, or a job SK
// it cannot read also let it through.
func TestCheckSensorFreshness(t *testing.T) {
	const jobSK = "JOB#stream#2026-03-01#"
	const rfcJob = jobSK + "1773133200000" // 2026-03-10T09:00:00Z
	tests := []struct {
		name, sk string
		sensors  []any // one updatedAt per sensor; a nil entry is a sensor without updatedAt
		want     bool
		wantWarn string // the sensor key logged as unreadable; empty means no warning
		anyWarn  bool   // map order decides whether the unreadable sensor is read before the fresh one, so a warning is optional
	}{
		{name: "no sensors", sk: jobSK + "1000000", want: true},
		{name: "no updatedAt", sk: jobSK + "1000000", sensors: []any{nil}, want: true},
		{name: "fresh float", sk: jobSK + "1000000", sensors: []any{2000000.0}, want: true},
		{name: "stale float", sk: jobSK + "2000000000000", sensors: []any{1000000000000.0}, want: false},
		{name: "fresh string", sk: jobSK + "1000000", sensors: []any{"2000000"}, want: true},
		{name: "epoch seconds", sk: jobSK + "2000000000000", sensors: []any{2000000001.0}, want: true},
		{name: "equal", sk: jobSK + "2000000000000", sensors: []any{2000000000000.0}, want: false},
		{name: "short SK", sk: "JOB#stream#2026-03-01", sensors: []any{1.0}, want: true},
		{name: "short SK ending in a number", sk: "JOB#stream#2000000000000", sensors: []any{1.0}, want: true},
		{name: "non-numeric timestamp", sk: jobSK + "not-a-number", sensors: []any{1.0}, want: true},
		{name: "RFC3339 after the job", sk: rfcJob, sensors: []any{"2026-03-10T09:00:01Z"}, want: true},
		{name: "RFC3339 before the job", sk: rfcJob, sensors: []any{"2026-03-10T08:59:59Z"}, want: false},
		{name: "unreadable alone", sk: rfcJob, sensors: []any{"yesterday"}, want: true, wantWarn: "s0"},
		{name: "unreadable and stale", sk: rfcJob, sensors: []any{"yesterday", "2026-03-10T08:00:00Z"}, want: false, wantWarn: "s0"},
		{name: "unreadable and fresh", sk: rfcJob, sensors: []any{"yesterday", "2026-03-10T10:00:00Z"}, want: true, wantWarn: "s0", anyWarn: true},
		{name: "stale epoch and fresh RFC3339", sk: rfcJob, sensors: []any{1773129600000.0, "2026-03-10T10:00:00Z"}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake, _ := storetest.NewMem()
			logs := &bytes.Buffer{}
			d := &lambda.Deps{Store: storetest.NewStore(fake), Logger: slog.New(slog.NewTextHandler(logs, nil))}
			ctx := context.Background()
			for i, updatedAt := range tt.sensors {
				data := map[string]interface{}{"status": "ready"}
				if updatedAt != nil {
					data["updatedAt"] = updatedAt
				}
				if err := d.Store.WriteSensor(ctx, "p", "s"+strconv.Itoa(i), data); err != nil {
					t.Fatal(err)
				}
			}

			got, err := checkSensorFreshness(ctx, d, "p", tt.sk)
			if err != nil || got != tt.want {
				t.Errorf("checkSensorFreshness(%q) = %v, %v; want %v, nil", tt.sk, got, err, tt.want)
			}
			warned := strings.Contains(logs.String(), "level=WARN")
			switch {
			case tt.anyWarn:
				if warned && !strings.Contains(logs.String(), "sensor="+tt.wantWarn) {
					t.Errorf("warning does not name sensor %s:\n%s", tt.wantWarn, logs)
				}
			case tt.wantWarn != "" && (!warned || !strings.Contains(logs.String(), "sensor="+tt.wantWarn)):
				t.Errorf("no warning naming sensor %s in logs:\n%s", tt.wantWarn, logs)
			case tt.wantWarn == "" && warned:
				t.Errorf("unexpected warning:\n%s", logs)
			}
		})
	}
}

// rerunWorld is a stream-router for pipeline p whose date 2026-03-10 has a
// COMPLETED trigger row (completedAt 12:00), a success job row at 08:00 and
// an "upstream" sensor updated at updatedAt. The router and the store read
// *now.
func rerunWorld(t *testing.T, updatedAt string, now *time.Time) (*lambda.Deps, *storetest.Mem, *countingSFN, *recordingEventBridge) {
	t.Helper()
	const date = "2026-03-10"
	d, mem, sfnFake, eb := jobWorld(t, types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists, Cutoff: "23:59", CutoffDayOffset: 7}},
		Job:      types.JobConfig{Type: types.TriggerHTTP, Config: httpJobConfig},
		PostRun:  &types.PostRunConfig{WatchWindow: "6h"},
	})
	mem.Put("control", completedRow(date, "2026-03-10T12:00:00Z"))
	mem.Put("joblog", map[string]ddbtypes.AttributeValue{
		"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("p")},
		"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("stream", date, "1773129600000")},
		"event": &ddbtypes.AttributeValueMemberS{Value: types.JobEventSuccess}})
	if err := d.Store.WriteSensor(context.Background(), "p", "upstream", map[string]interface{}{"status": "ready", "updatedAt": updatedAt}); err != nil {
		t.Fatal(err)
	}
	rerunCountQuery(d.Store.Client.(*storetest.FakeDynamo), mem)
	d.NowFunc = func() time.Time { return *now }
	d.Store.Now = func() time.Time { return *now }
	return d, mem, sfnFake, eb
}

// rerunRequestRecord is the stream record of a RERUN_REQUEST# row for p on
// 2026-03-10.
func rerunRequestRecord(reason, requestedAt string) events.DynamoDBEventRecord {
	return events.DynamoDBEventRecord{EventName: "INSERT", Change: events.DynamoDBStreamRecord{
		SequenceNumber: "1",
		Keys: map[string]events.DynamoDBAttributeValue{
			"PK": events.NewStringAttribute(types.PipelinePK("p")),
			"SK": events.NewStringAttribute(types.RerunRequestSK("stream", "2026-03-10")),
		},
		NewImage: map[string]events.DynamoDBAttributeValue{
			"reason":      events.NewStringAttribute(reason),
			"requestedAt": events.NewStringAttribute(requestedAt),
		},
	}}
}

// TestRerunRequest_MaxAutoRerunAge: with job.maxAutoRerunAge 24h and a 200h
// watch window unless a row says otherwise, an automatic request handled 25h
// after completedAt starts nothing, publishes only MANUAL_RERUN_REQUIRED and
// logs that the age ended it. A manual request at that time still starts, and
// so does an automatic one inside the age. With a 6h window the log names the
// window, and a COMPLETED row with no completedAt is logged as having no
// completion time and publishes nothing.
func TestRerunRequest_MaxAutoRerunAge(t *testing.T) {
	const date = "2026-03-10"
	tests := []struct {
		name, reason, now string
		window            string // empty means 200h
		noCompletedAt     bool
		wantEndedBy       string // the logged endedBy; empty means the request starts
	}{
		{name: "late-data past the age", reason: "late-data", now: "2026-03-11T13:00:00Z", wantEndedBy: "job.maxAutoRerunAge"},
		{name: "data-drift past the age", reason: "data-drift", now: "2026-03-11T13:00:00Z", wantEndedBy: "job.maxAutoRerunAge"},
		{name: "manual past the age (guard)", reason: "manual", now: "2026-03-11T13:00:00Z"},
		{name: "late-data inside the age (guard)", reason: "late-data", now: "2026-03-11T11:59:59Z"},
		{name: "late-data past a shorter window", reason: "late-data", now: "2026-03-10T19:00:00Z", window: "6h", wantEndedBy: "postRun.watchWindow"},
		{name: "late-data, no completedAt", reason: "late-data", now: "2026-03-10T13:00:00Z", noCompletedAt: true, wantEndedBy: "no-completion-time"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, mem, sfnFake, eb := jobWorld(t, types.PipelineConfig{
				Pipeline: types.PipelineIdentity{ID: "p"},
				Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists, Cutoff: "23:59", CutoffDayOffset: 7}},
				Job:      types.JobConfig{Type: types.TriggerHTTP, Config: httpJobConfig, MaxAutoRerunAge: "24h"},
				PostRun:  &types.PostRunConfig{WatchWindow: cmp.Or(tt.window, "200h")},
			})
			completedAt := "2026-03-10T12:00:00Z"
			if tt.noCompletedAt {
				completedAt = ""
			}
			mem.Put("control", completedRow(date, completedAt))
			mem.Put("joblog", map[string]ddbtypes.AttributeValue{
				"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("p")},
				"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("stream", date, "1773129600000")},
				"event": &ddbtypes.AttributeValueMemberS{Value: types.JobEventSuccess}})
			rerunCountQuery(d.Store.Client.(*storetest.FakeDynamo), mem)
			logs := &bytes.Buffer{}
			d.Logger = slog.New(slog.NewTextHandler(logs, nil))
			now := mustTime(t, tt.now)
			d.NowFunc = func() time.Time { return now }
			d.Store.Now = func() time.Time { return now }

			if err := handleRecord(context.Background(), d, rerunRequestRecord(tt.reason, tt.now)); err != nil {
				t.Fatalf("handleRecord: %v", err)
			}

			var gotEvents []string
			for _, e := range eb.events {
				gotEvents = append(gotEvents, e.Type)
			}
			wantStarts, wantEvents, wantStatus := 0, []string(nil), types.TriggerStatusCompleted
			if tt.wantEndedBy == "" {
				wantStarts, wantEvents, wantStatus = 1, []string{string(types.EventRerunAccepted)}, types.TriggerStatusRunning
			}
			if tt.wantEndedBy != "" && !tt.noCompletedAt {
				wantEvents = []string{string(types.EventManualRerunRequired)}
			}
			if sfnFake.calls != wantStarts || !slices.Equal(gotEvents, wantEvents) {
				t.Errorf("StartExecution calls %d, events %v; want %d and %v", sfnFake.calls, gotEvents, wantStarts, wantEvents)
			}
			if got := len(mem.Items("rerun")); got != wantStarts {
				t.Errorf("rerun rows = %d, want %d", got, wantStarts)
			}
			status := mem.Get("control", types.PipelinePK("p"), types.TriggerSK("stream", date))["status"].(*ddbtypes.AttributeValueMemberS).Value
			if status != wantStatus {
				t.Errorf("trigger status = %q, want %q", status, wantStatus)
			}
			if tt.wantEndedBy == "" && strings.Contains(logs.String(), "endedBy=") {
				t.Errorf("logged %q, want no endedBy line", logs.String())
			}
			if want := "endedBy=" + tt.wantEndedBy; tt.wantEndedBy != "" && !slices.Contains(strings.Fields(logs.String()), want) {
				t.Errorf("logged %q, want %q", logs.String(), want)
			}
		})
	}
}

// TestRerunRequest_RepeatedAfterBreakerRejection: the circuit breaker judges
// by the last run, not by a rerun-rejected row an earlier version stored
// above it, so a request is refused, asking again is refused again, and
// neither rejection adds a job row.
func TestRerunRequest_RepeatedAfterBreakerRejection(t *testing.T) {
	now := mustTime(t, "2026-03-10T13:00:00Z")
	d, mem, sfnFake, eb := rerunWorld(t, "1773100800000", &now)
	mem.Put("joblog", map[string]ddbtypes.AttributeValue{
		"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("p")},
		"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("stream", "2026-03-10", "1773129600001")},
		"event": &ddbtypes.AttributeValueMemberS{Value: types.JobEventRerunRejected}})
	ctx := context.Background()

	if err := handleRecord(ctx, d, rerunRequestRecord("manual", "2026-03-10T13:00:00Z")); err != nil {
		t.Fatalf("first request: %v", err)
	}
	now = now.Add(5 * time.Minute)
	if err := handleRecord(ctx, d, rerunRequestRecord("manual", "2026-03-10T13:05:00Z")); err != nil {
		t.Fatalf("second request: %v", err)
	}

	const wantMsg = "rerun rejected for p: previous run succeeded and no sensor data has changed"
	if len(eb.events) != 2 {
		t.Fatalf("events = %+v, want two %s", eb.events, types.EventRerunRejected)
	}
	for i, e := range eb.events {
		if e.Type != string(types.EventRerunRejected) || e.Message != wantMsg {
			t.Errorf("event %d = %s %q, want %s %q", i, e.Type, e.Message, types.EventRerunRejected, wantMsg)
		}
	}
	if sfnFake.calls != 0 || len(mem.Items("rerun")) != 0 {
		t.Errorf("StartExecution calls %d, rerun rows %d; want 0 and 0", sfnFake.calls, len(mem.Items("rerun")))
	}
	wantJobs := []string{types.JobEventSuccess, types.JobEventRerunRejected}
	if got := jobEvents(mem); !slices.Equal(got, wantJobs) {
		t.Errorf("job rows = %v, want %v", got, wantJobs)
	}
	status := mem.Get("control", types.PipelinePK("p"), types.TriggerSK("stream", "2026-03-10"))["status"].(*ddbtypes.AttributeValueMemberS).Value
	if status != types.TriggerStatusCompleted {
		t.Errorf("trigger status = %q, want COMPLETED", status)
	}
}

// TestRerunRequest_AutomaticSkipsBreaker: a late-data or data-drift request
// is not sent through the circuit breaker (the stream-router already saw the
// change it reruns for), so it starts a rerun after a success even though no
// sensor is newer than the run. A manual request is still checked.
func TestRerunRequest_AutomaticSkipsBreaker(t *testing.T) {
	tests := []struct {
		reason    string
		wantStart bool
	}{
		{reason: "late-data", wantStart: true},
		{reason: "data-drift", wantStart: true},
		{reason: "manual"}, // guard
	}
	for _, tt := range tests {
		t.Run(tt.reason, func(t *testing.T) {
			now := mustTime(t, "2026-03-10T13:00:00Z")
			d, mem, sfnFake, eb := rerunWorld(t, "2026-03-10T07:00:00Z", &now)

			if err := handleRecord(context.Background(), d, rerunRequestRecord(tt.reason, "2026-03-10T13:00:00Z")); err != nil {
				t.Fatalf("handleRecord: %v", err)
			}

			var gotEvents []string
			for _, e := range eb.events {
				gotEvents = append(gotEvents, e.Type)
			}
			wantStarts, wantEvents, wantReruns := 0, []string{string(types.EventRerunRejected)}, 0
			wantJobs := []string{types.JobEventSuccess}
			if tt.wantStart {
				wantStarts, wantEvents, wantReruns = 1, []string{string(types.EventRerunAccepted)}, 1
				wantJobs = []string{types.JobEventSuccess, types.JobEventRerunAccepted}
			}
			if sfnFake.calls != wantStarts || !slices.Equal(gotEvents, wantEvents) {
				t.Errorf("StartExecution calls %d, events %v; want %d and %v", sfnFake.calls, gotEvents, wantStarts, wantEvents)
			}
			if got := len(mem.Items("rerun")); got != wantReruns {
				t.Errorf("rerun rows = %d, want %d", got, wantReruns)
			}
			if got := jobEvents(mem); !slices.Equal(got, wantJobs) {
				t.Errorf("job rows = %v, want %v", got, wantJobs)
			}
		})
	}
}

// TestRerunRequest_RunningDateIsRefused: a request of any reason for a date
// whose run is still RUNNING is rejected with RERUN_REJECTED and leaves the
// run alone: no start, no rerun row, the post-run baseline kept and the
// trigger row's status, ttl and startedAt unchanged. The calendar and the
// budget are checked first, so an excluded date or a spent budget gives
// those verdicts instead; the circuit breaker comes after. A rejection is
// logged with the request's reason. Unless a row says otherwise the date's
// last job row is a fail and the sensor is newer.
func TestRerunRequest_RunningDateIsRefused(t *testing.T) {
	const date = "2026-03-10"
	const inProgress = "rerun rejected for p: a run is in progress for 2026-03-10"
	rejected := string(types.EventRerunRejected)
	tests := []struct {
		name, reason string
		job          string // the last job row's event; empty means fail
		updatedAt    string // the sensor's updatedAt; empty means 09:00, after the job
		reruns       []string
		excluded     bool
		wantEvent    string
		wantMsg      string
		wantWhy      string // the event detail's why
	}{
		{name: "manual", reason: "manual", wantEvent: rejected, wantMsg: inProgress, wantWhy: runInProgress(date)},
		{name: "late-data", reason: "late-data", wantEvent: rejected, wantMsg: inProgress, wantWhy: runInProgress(date)},
		{name: "data-drift", reason: "data-drift", wantEvent: rejected, wantMsg: inProgress, wantWhy: runInProgress(date)},
		{name: "breaker would reject", reason: "manual", job: types.JobEventSuccess, updatedAt: "1773100800000",
			wantEvent: rejected, wantMsg: inProgress, wantWhy: runInProgress(date)},
		{name: "budget spent", reason: "manual", reruns: []string{"manual"}, wantEvent: rejected,
			wantMsg: "rerun rejected for p: manual rerun limit exceeded", wantWhy: "manual rerun limit exceeded"},
		{name: "excluded date", reason: "manual", excluded: true, wantEvent: string(types.EventPipelineExcluded),
			wantMsg: "rerun blocked for p: execution date 2026-03-10 excluded by calendar", wantWhy: "excluded by calendar"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := types.PipelineConfig{
				Pipeline: types.PipelineIdentity{ID: "p"},
				Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists, Cutoff: "23:59", CutoffDayOffset: 7}},
				Job:      types.JobConfig{Type: types.TriggerHTTP, Config: httpJobConfig},
				PostRun:  &types.PostRunConfig{WatchWindow: "6h"},
			}
			if tt.excluded {
				cfg.Schedule.Exclude = &types.ExclusionConfig{Dates: []string{date}}
			}
			d, mem, sfnFake, eb := jobWorld(t, cfg)
			pk := &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("p")}
			mem.Put("control", map[string]ddbtypes.AttributeValue{"PK": pk,
				"SK":        &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("stream", date)},
				"status":    &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusRunning},
				"staleAt":   &ddbtypes.AttributeValueMemberN{Value: "1773160200"},
				"startedAt": &ddbtypes.AttributeValueMemberS{Value: "2026-03-10T12:30:00Z"}})
			mem.Put("joblog", map[string]ddbtypes.AttributeValue{"PK": pk,
				"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("stream", date, "1773129600000")},
				"event": &ddbtypes.AttributeValueMemberS{Value: cmp.Or(tt.job, types.JobEventFail)}})
			for i, reason := range tt.reruns {
				mem.Put("rerun", map[string]ddbtypes.AttributeValue{"PK": pk,
					"SK":     &ddbtypes.AttributeValueMemberS{Value: types.RerunSK("stream", date, i)},
					"reason": &ddbtypes.AttributeValueMemberS{Value: reason}})
			}
			ctx := context.Background()
			if err := d.Store.WriteSensor(ctx, "p", "upstream", map[string]interface{}{"status": "ready", "updatedAt": cmp.Or(tt.updatedAt, "1773133200000")}); err != nil {
				t.Fatal(err)
			}
			baselineKey := "postrun-baseline#" + date
			if err := d.Store.WriteSensor(ctx, "p", baselineKey, map[string]interface{}{"upstream": map[string]interface{}{"count": 1}}); err != nil {
				t.Fatal(err)
			}
			rerunCountQuery(d.Store.Client.(*storetest.FakeDynamo), mem)
			now := mustTime(t, "2026-03-10T13:00:00Z")
			d.NowFunc = func() time.Time { return now }
			d.Store.Now = func() time.Time { return now }
			logs := &bytes.Buffer{}
			d.Logger = slog.New(slog.NewTextHandler(logs, nil))

			if err := handleRecord(ctx, d, rerunRequestRecord(tt.reason, "2026-03-10T13:00:00Z")); err != nil {
				t.Fatalf("handleRecord: %v", err)
			}

			if len(eb.events) != 1 || eb.events[0].Type != tt.wantEvent || eb.events[0].Message != tt.wantMsg {
				t.Fatalf("events = %+v, want one %s %q", eb.events, tt.wantEvent, tt.wantMsg)
			}
			if want := map[string]interface{}{"reason": tt.reason, "why": tt.wantWhy}; !maps.Equal(eb.events[0].Detail, want) {
				t.Errorf("event detail = %v, want %v", eb.events[0].Detail, want)
			}
			if tt.wantEvent == rejected {
				why := strconv.Quote(strings.TrimPrefix(tt.wantMsg, "rerun rejected for p: "))
				if !slices.ContainsFunc(strings.Split(logs.String(), "\n"), func(line string) bool {
					return strings.Contains(line, `msg="rerun request rejected"`) &&
						strings.Contains(line, " reason="+tt.reason+" ") && strings.Contains(line, " why="+why)
				}) {
					t.Errorf("no rejection log with reason=%s why=%s:\n%s", tt.reason, why, logs)
				}
			}
			if sfnFake.calls != 0 || len(mem.Items("rerun")) != len(tt.reruns) {
				t.Errorf("StartExecution calls %d, rerun rows %d; want 0 and %d", sfnFake.calls, len(mem.Items("rerun")), len(tt.reruns))
			}
			wantJobs := []string{cmp.Or(tt.job, types.JobEventFail)}
			if got := jobEvents(mem); !slices.Equal(got, wantJobs) {
				t.Errorf("job rows = %v, want %v", got, wantJobs)
			}
			if baseline, err := d.Store.GetSensorData(ctx, "p", baselineKey); err != nil || baseline == nil {
				t.Errorf("post-run baseline = %v, %v; want it kept", baseline, err)
			}
			row := mem.Get("control", pk.Value, types.TriggerSK("stream", date))
			got := [3]string{strAttr(row["status"]), numAttr(row["staleAt"]), strAttr(row["startedAt"])}
			if want := [3]string{types.TriggerStatusRunning, "1773160200", "2026-03-10T12:30:00Z"}; got != want {
				t.Errorf("trigger status/staleAt/startedAt = %v, want %v", got, want)
			}
			if _, hasTTL := row["ttl"]; hasTTL {
				t.Errorf("trigger row has ttl %v, want none", row["ttl"])
			}
		})
	}
}

// numAttr is a number attribute's value, or "" for anything else.
func numAttr(v ddbtypes.AttributeValue) string {
	if n, ok := v.(*ddbtypes.AttributeValueMemberN); ok {
		return n.Value
	}
	return ""
}

// TestRerunRequest_LostResetRace: the date read COMPLETED, but a run took the
// row before the lock reset, so the reset's condition fails. That is a run in
// progress, not an infrastructure failure: the request is rejected and
// nothing starts.
func TestRerunRequest_LostResetRace(t *testing.T) {
	now := mustTime(t, "2026-03-10T13:00:00Z")
	d, mem, sfnFake, eb := rerunWorld(t, "2026-03-10T09:00:00Z", &now)
	fake := d.Store.Client.(*storetest.FakeDynamo)
	update := fake.UpdateItemFn
	fake.UpdateItemFn = func(ctx context.Context, in *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
		if sk := strAttr(in.Key["SK"]); strings.HasPrefix(sk, "TRIGGER#") {
			return nil, &ddbtypes.ConditionalCheckFailedException{}
		}
		return update(ctx, in)
	}

	if err := handleRecord(context.Background(), d, rerunRequestRecord("manual", "2026-03-10T13:00:00Z")); err != nil {
		t.Fatalf("handleRecord: %v", err)
	}

	const wantMsg = "rerun rejected for p: a run is in progress for 2026-03-10"
	if len(eb.events) != 1 || eb.events[0].Type != string(types.EventRerunRejected) || eb.events[0].Message != wantMsg {
		t.Errorf("events = %+v, want one %s %q", eb.events, types.EventRerunRejected, wantMsg)
	}
	if sfnFake.calls != 0 || len(mem.Items("rerun")) != 0 {
		t.Errorf("StartExecution calls %d, rerun rows %d; want 0 and 0", sfnFake.calls, len(mem.Items("rerun")))
	}
	wantJobs := []string{types.JobEventSuccess}
	if got := jobEvents(mem); !slices.Equal(got, wantJobs) {
		t.Errorf("job rows = %v, want %v", got, wantJobs)
	}
}

// failedStartWorld is rerunWorld at 13:00 with a post-run baseline for
// 2026-03-10, the date's trigger row set to status and an SFN double whose
// next start fails with startErr (a plain error when nil).
func failedStartWorld(t *testing.T, status string, startErr error) (*lambda.Deps, *storetest.Mem, *countingSFN, *recordingEventBridge, *time.Time) {
	t.Helper()
	now := mustTime(t, "2026-03-10T13:00:00Z")
	d, mem, sfnFake, eb := rerunWorld(t, "2026-03-10T09:00:00Z", &now)
	row := completedRow("2026-03-10", "2026-03-10T12:00:00Z")
	row["status"] = &ddbtypes.AttributeValueMemberS{Value: status}
	mem.Put("control", row)
	if err := d.Store.WriteSensor(context.Background(), "p", "postrun-baseline#2026-03-10", map[string]interface{}{"upstream": map[string]interface{}{"count": 1}}); err != nil {
		t.Fatal(err)
	}
	sfnFake.fail, sfnFake.err = 1, startErr
	return d, mem, sfnFake, eb, &now
}

// triggerRow is p's trigger row for 2026-03-10.
func triggerRow(mem *storetest.Mem) map[string]ddbtypes.AttributeValue {
	return mem.Get("control", types.PipelinePK("p"), types.TriggerSK("stream", "2026-03-10"))
}

// baselineKept reports whether the date's post-run baseline is still there.
func baselineKept(t *testing.T, d *lambda.Deps) bool {
	t.Helper()
	baseline, err := d.Store.GetSensorData(context.Background(), "p", "postrun-baseline#2026-03-10")
	if err != nil {
		t.Fatal(err)
	}
	return baseline != nil
}

// TestRerunRequest_FailedStartThenRedelivery: a late-data rerun (the default
// drift budget of 1) whose execution cannot start returns an error and leaves
// the COMPLETED date as it was: completedAt kept, no staleAt, no rerun row, no
// event, no new job row and the baseline kept. The stream redelivers the same
// record, which then starts the rerun under a name built from the record's
// creation time, not the clock.
func TestRerunRequest_FailedStartThenRedelivery(t *testing.T) {
	d, mem, sfnFake, eb, now := failedStartWorld(t, types.TriggerStatusCompleted, nil)
	created := mustTime(t, "2026-03-10T12:59:30Z")
	record := rerunRequestRecord("late-data", "2026-03-10T12:59:30Z")
	record.Change.ApproximateCreationDateTime = events.SecondsEpochTime{Time: created}
	ctx := context.Background()

	if err := handleRecord(ctx, d, record); err == nil {
		t.Fatal("first delivery: handleRecord = nil, want the start error")
	}
	row := triggerRow(mem)
	if got := [2]string{strAttr(row["status"]), strAttr(row["completedAt"])}; got != [2]string{types.TriggerStatusCompleted, "2026-03-10T12:00:00Z"} {
		t.Errorf("after the failed start: trigger status/completedAt = %v, want COMPLETED and 12:00", got)
	}
	if _, ok := row["staleAt"]; ok {
		t.Errorf("after the failed start: trigger staleAt = %v, want none", row["staleAt"])
	}
	if got := len(mem.Items("rerun")); got != 0 || len(eb.events) != 0 || sfnFake.calls != 0 {
		t.Errorf("after the failed start: rerun rows %d, events %+v, starts %d; want none", got, eb.events, sfnFake.calls)
	}
	if got := jobEvents(mem); !slices.Equal(got, []string{types.JobEventSuccess}) {
		t.Errorf("after the failed start: job rows = %v, want [success]", got)
	}
	if !baselineKept(t, d) {
		t.Error("after the failed start: post-run baseline deleted, want it kept")
	}

	*now = now.Add(time.Minute)
	if err := handleRecord(ctx, d, record); err != nil {
		t.Fatalf("redelivery: handleRecord = %v", err)
	}
	if got := strAttr(triggerRow(mem)["status"]); got != types.TriggerStatusRunning || sfnFake.calls != 1 {
		t.Errorf("after the redelivery: trigger status %q, starts %d; want RUNNING and 1", got, sfnFake.calls)
	}
	reruns := mem.Items("rerun")
	if len(reruns) != 1 || strAttr(reruns[0]["SK"]) != types.RerunSK("stream", "2026-03-10", 0) {
		t.Errorf("after the redelivery: rerun rows = %v, want one at attempt 0", reruns)
	}
	if got, want := jobEvents(mem), []string{types.JobEventSuccess, types.JobEventRerunAccepted}; !slices.Equal(got, want) {
		t.Errorf("after the redelivery: job rows = %v, want %v", got, want)
	}
	if len(eb.events) != 1 || eb.events[0].Type != string(types.EventRerunAccepted) {
		t.Errorf("after the redelivery: events = %+v, want one %s", eb.events, types.EventRerunAccepted)
	}
	if baselineKept(t, d) {
		t.Error("after the redelivery: post-run baseline kept, want it deleted")
	}
	want := fmt.Sprintf("%s-%s-%s-rerun%d-%d", "p", "stream", "2026-03-10", 0, created.Unix())
	if !slices.Equal(sfnFake.names, []string{want}) {
		t.Errorf("execution names = %v, want [%s]", sfnFake.names, want)
	}
}

// TestRerunRequest_FailedStartKeepsFailedFinal: a FAILED_FINAL date whose
// rerun cannot start is FAILED_FINAL again, with no staleAt.
func TestRerunRequest_FailedStartKeepsFailedFinal(t *testing.T) {
	d, mem, sfnFake, _, _ := failedStartWorld(t, types.TriggerStatusFailedFinal, nil)

	if err := handleRecord(context.Background(), d, rerunRequestRecord("manual", "2026-03-10T13:00:00Z")); err == nil {
		t.Fatal("handleRecord = nil, want the start error")
	}
	row := triggerRow(mem)
	if got := strAttr(row["status"]); got != types.TriggerStatusFailedFinal || row["staleAt"] != nil {
		t.Errorf("trigger status %q, staleAt %v; want FAILED_FINAL and none", got, row["staleAt"])
	}
	if got := len(mem.Items("rerun")); got != 0 || sfnFake.calls != 0 {
		t.Errorf("rerun rows %d, starts %d; want 0 and 0", got, sfnFake.calls)
	}
}

// TestRerunRequest_RerunWriteFails: when the rerun row cannot be written the
// request fails before any start and the trigger row is restored, not
// deleted.
func TestRerunRequest_RerunWriteFails(t *testing.T) {
	d, mem, sfnFake, eb, _ := failedStartWorld(t, types.TriggerStatusCompleted, nil)
	sfnFake.fail = 0
	fake := d.Store.Client.(*storetest.FakeDynamo)
	put := fake.PutItemFn
	fake.PutItemFn = func(ctx context.Context, in *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
		if *in.TableName == "rerun" {
			return nil, errors.New("put rerun failed")
		}
		return put(ctx, in)
	}

	if err := handleRecord(context.Background(), d, rerunRequestRecord("manual", "2026-03-10T13:00:00Z")); err == nil {
		t.Fatal("handleRecord = nil, want the rerun write error")
	}
	row := triggerRow(mem)
	if row == nil || strAttr(row["status"]) != types.TriggerStatusCompleted {
		t.Errorf("trigger row = %v, want it restored to COMPLETED", row)
	}
	if sfnFake.calls != 0 || len(eb.events) != 0 || !baselineKept(t, d) {
		t.Errorf("starts %d, events %+v, baseline kept %v; want 0, none and true", sfnFake.calls, eb.events, baselineKept(t, d))
	}
}

// TestRerunRequest_ExecutionAlreadyExistsCountsAsStarted: a start answered
// with ExecutionAlreadyExists means the rerun's execution exists, so the
// request succeeds: the date stays RUNNING, the rerun row is kept and the
// rerun is accepted once.
func TestRerunRequest_ExecutionAlreadyExistsCountsAsStarted(t *testing.T) {
	d, mem, _, eb, _ := failedStartWorld(t, types.TriggerStatusCompleted, &sfntypes.ExecutionAlreadyExists{Message: aws.String("exists")})

	if err := handleRecord(context.Background(), d, rerunRequestRecord("manual", "2026-03-10T13:00:00Z")); err != nil {
		t.Fatalf("handleRecord = %v, want nil", err)
	}
	if got := strAttr(triggerRow(mem)["status"]); got != types.TriggerStatusRunning {
		t.Errorf("trigger status = %q, want RUNNING", got)
	}
	if got := len(mem.Items("rerun")); got != 1 {
		t.Errorf("rerun rows = %d, want 1", got)
	}
	if len(eb.events) != 1 || eb.events[0].Type != string(types.EventRerunAccepted) {
		t.Errorf("events = %+v, want one %s", eb.events, types.EventRerunAccepted)
	}
	if got, want := jobEvents(mem), []string{types.JobEventSuccess, types.JobEventRerunAccepted}; !slices.Equal(got, want) {
		t.Errorf("job rows = %v, want %v", got, want)
	}
}

// TestRerunRequest_UndoFails: when the status restore after a failed start
// fails too, the start error is still returned and the restore failure is
// logged at Error.
func TestRerunRequest_UndoFails(t *testing.T) {
	d, mem, _, _, _ := failedStartWorld(t, types.TriggerStatusCompleted, nil)
	fake := d.Store.Client.(*storetest.FakeDynamo)
	update := fake.UpdateItemFn
	fake.UpdateItemFn = func(ctx context.Context, in *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
		if strings.HasPrefix(strAttr(in.Key["SK"]), "TRIGGER#") && in.ExpressionAttributeValues[":s"] != nil {
			return nil, errors.New("restore failed")
		}
		return update(ctx, in)
	}
	logs := &bytes.Buffer{}
	d.Logger = slog.New(slog.NewTextHandler(logs, nil))

	err := handleRecord(context.Background(), d, rerunRequestRecord("manual", "2026-03-10T13:00:00Z"))
	if err == nil || !strings.Contains(err.Error(), "start execution failed") {
		t.Fatalf("handleRecord = %v, want the start error", err)
	}
	if !strings.Contains(logs.String(), "level=ERROR") || !strings.Contains(logs.String(), "restore failed") {
		t.Errorf("no Error log of the failed restore:\n%s", logs)
	}
	if got := len(mem.Items("rerun")); got != 0 {
		t.Errorf("rerun rows = %d, want 0 (deleted before the restore)", got)
	}
}

// TestRerunRequest_UndoDeleteFails: when the rerun row of a rerun that did
// not start cannot be deleted, the status is still restored, so a COMPLETED
// date stays COMPLETED (no ttl) rather than waiting for the watchdog. The
// row stays and counts against the budget. The start error is returned and
// the failed delete logged at Error.
func TestRerunRequest_UndoDeleteFails(t *testing.T) {
	d, mem, _, _, _ := failedStartWorld(t, types.TriggerStatusCompleted, nil)
	fake := d.Store.Client.(*storetest.FakeDynamo)
	del := fake.DeleteItemFn
	fake.DeleteItemFn = func(ctx context.Context, in *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
		if *in.TableName == "rerun" {
			return nil, errors.New("delete rerun failed")
		}
		return del(ctx, in)
	}
	logs := &bytes.Buffer{}
	d.Logger = slog.New(slog.NewTextHandler(logs, nil))

	err := handleRecord(context.Background(), d, rerunRequestRecord("manual", "2026-03-10T13:00:00Z"))
	if err == nil || !strings.Contains(err.Error(), "start execution failed") {
		t.Fatalf("handleRecord = %v, want the start error", err)
	}
	row := triggerRow(mem)
	if got := strAttr(row["status"]); got != types.TriggerStatusCompleted || row["staleAt"] != nil {
		t.Errorf("trigger status %q, staleAt %v; want COMPLETED and none", got, row["staleAt"])
	}
	if got := len(mem.Items("rerun")); got != 1 {
		t.Errorf("rerun rows = %d, want 1 (the delete failed)", got)
	}
	if !strings.Contains(logs.String(), "level=ERROR") || !strings.Contains(logs.String(), "delete rerun failed") {
		t.Errorf("no Error log of the failed delete:\n%s", logs)
	}
}

// cancellingSFN fails every start and cancels the request context first, as
// a Lambda timeout during the call would.
type cancellingSFN struct{ cancel context.CancelFunc }

func (c cancellingSFN) StartExecution(context.Context, *sfn.StartExecutionInput, ...func(*sfn.Options)) (*sfn.StartExecutionOutput, error) {
	c.cancel()
	return nil, errors.New("start execution failed")
}

// TestRerunRequest_UndoSurvivesCancelledContext: the undo runs on a context
// of its own, so a request context cancelled during the start still lets it
// delete the rerun row and restore the date.
func TestRerunRequest_UndoSurvivesCancelledContext(t *testing.T) {
	d, mem, _, _, _ := failedStartWorld(t, types.TriggerStatusCompleted, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d.SFNClient = cancellingSFN{cancel: cancel}
	fake := d.Store.Client.(*storetest.FakeDynamo)
	update, del := fake.UpdateItemFn, fake.DeleteItemFn
	fake.UpdateItemFn = func(ctx context.Context, in *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return update(ctx, in)
	}
	fake.DeleteItemFn = func(ctx context.Context, in *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return del(ctx, in)
	}

	if err := handleRecord(ctx, d, rerunRequestRecord("manual", "2026-03-10T13:00:00Z")); err == nil {
		t.Fatal("handleRecord = nil, want the start error")
	}
	if got := strAttr(triggerRow(mem)["status"]); got != types.TriggerStatusCompleted {
		t.Errorf("trigger status = %q, want COMPLETED", got)
	}
	if got := len(mem.Items("rerun")); got != 0 {
		t.Errorf("rerun rows = %d, want 0", got)
	}
}

package stream

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

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
// with no start, event or row. A manual request is never checked.
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
				Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists}},
				Job:      types.JobConfig{Type: types.TriggerHTTP},
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
			if sfnFake.calls != wantStarts || !slices.Equal(gotEvents, wantEvents) {
				t.Errorf("StartExecution calls %d, events %v; want %d and %v", sfnFake.calls, gotEvents, wantStarts, wantEvents)
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
// request starts one run, writes a rerun row and a rerun-accepted job row, and
// sets the trigger RUNNING; a rejected one writes a rerun-rejected job row and
// changes nothing else.
func TestRerunRequest_Outcomes(t *testing.T) {
	const (
		date      = "2026-03-10"
		noTrigger = "none" // the date has no trigger row
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
		reruns                     []string
		tweak                      func(*types.PipelineConfig)
		wantStarts                 int
		wantEvents, wantJobs       []string
		wantReruns                 int
		wantStatus                 string // trigger row at the end; empty means none
	}{
		{name: "success, fresh sensor", reason: "manual", updatedAt: fresh,
			wantStarts: 1, wantEvents: accepted, wantJobs: []string{success, types.JobEventRerunAccepted}, wantReruns: 1, wantStatus: running},
		{name: "success, stale sensor", reason: "manual", updatedAt: stale,
			wantEvents: rejected, wantJobs: []string{success, types.JobEventRerunRejected}, wantStatus: completed},
		{name: "fail, stale sensor", reason: "manual", trigger: types.TriggerStatusFailedFinal, job: fail, updatedAt: stale,
			wantStarts: 1, wantEvents: accepted, wantJobs: []string{fail, types.JobEventRerunAccepted}, wantReruns: 1, wantStatus: running},
		{name: "first drift", reason: "data-drift", tweak: budgets(1, 1),
			wantStarts: 1, wantEvents: accepted, wantJobs: []string{success, types.JobEventRerunAccepted}, wantReruns: 1, wantStatus: running},
		{name: "second drift", reason: "data-drift", reruns: []string{"data-drift"}, tweak: budgets(1, 1),
			wantEvents: rejected, wantJobs: []string{success, types.JobEventRerunRejected}, wantReruns: 1, wantStatus: completed},
		{name: "late-data vs drift row", reason: "late-data", reruns: []string{"data-drift"}, tweak: budgets(1, 1),
			wantEvents: rejected, wantJobs: []string{success, types.JobEventRerunRejected}, wantReruns: 1, wantStatus: completed},
		{name: "manual after drift spent", reason: "manual", reruns: []string{"data-drift"}, tweak: budgets(1, 2),
			wantStarts: 1, wantEvents: accepted, wantJobs: []string{success, types.JobEventRerunAccepted}, wantReruns: 2, wantStatus: running},
		{name: "second manual", reason: "manual", reruns: []string{"data-drift", "manual"}, tweak: budgets(1, 2),
			wantStarts: 1, wantEvents: accepted, wantJobs: []string{success, types.JobEventRerunAccepted}, wantReruns: 3, wantStatus: running},
		{name: "third manual", reason: "manual", reruns: []string{"data-drift", "manual", "manual"}, tweak: budgets(1, 2),
			wantEvents: rejected, wantJobs: []string{success, types.JobEventRerunRejected}, wantReruns: 3, wantStatus: completed},
		{name: "no trigger row", reason: "manual", trigger: noTrigger, job: fail,
			wantEvents: []string{string(types.EventInfraFailure)}, wantJobs: []string{fail}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := types.PipelineConfig{
				Pipeline: types.PipelineIdentity{ID: "p"},
				Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists}},
				Job:      types.JobConfig{Type: types.TriggerHTTP},
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
			mem.Put("joblog", map[string]ddbtypes.AttributeValue{"PK": pk,
				"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("stream", date, "1773129600000")},
				"event": &ddbtypes.AttributeValueMemberS{Value: cmp.Or(tt.job, success)}})
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
			if err := d.Store.WriteSensor(ctx, "p", "upstream", map[string]interface{}{"status": "ready", "updatedAt": updatedAt}); err != nil {
				t.Fatal(err)
			}
			rerunCountQuery(d.Store.Client.(*storetest.FakeDynamo), mem)
			now := mustTime(t, "2026-03-10T13:00:00Z")
			d.NowFunc = func() time.Time { return now }
			record := events.DynamoDBEventRecord{EventName: "INSERT", Change: events.DynamoDBStreamRecord{
				SequenceNumber: "1",
				Keys: map[string]events.DynamoDBAttributeValue{
					"PK": events.NewStringAttribute(types.PipelinePK("p")),
					"SK": events.NewStringAttribute(types.RerunRequestSK("stream", date)),
				},
				NewImage: map[string]events.DynamoDBAttributeValue{"reason": events.NewStringAttribute(tt.reason)},
			}}

			if err := handleRecord(ctx, d, record); err != nil {
				t.Fatalf("handleRecord: %v", err)
			}

			var gotEvents []string
			for _, e := range eb.events {
				gotEvents = append(gotEvents, e.Type)
			}
			if sfnFake.calls != tt.wantStarts || !slices.Equal(gotEvents, tt.wantEvents) {
				t.Errorf("StartExecution calls %d, events %v; want %d and %v", sfnFake.calls, gotEvents, tt.wantStarts, tt.wantEvents)
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

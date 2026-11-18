package stream

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"log/slog"
	"slices"
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

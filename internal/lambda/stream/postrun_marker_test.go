package stream

import (
	"cmp"
	"context"
	"errors"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/dwsmith1983/interlock/pkg/validation"
)

// markerWorld is pipeline p with a post-run rule on row-count (DriftField
// count, threshold 0, watch window 6h), its runDate run COMPLETED at 12:00
// on 2026-03-10 (status overrides it when set), and the clock at now.
func markerWorld(t *testing.T, runDate, status, now string, dryRun bool) (*lambda.Deps, *storetest.Mem, *recordingEventBridge) {
	t.Helper()
	threshold := 0.0
	d, mem, _, eb := jobWorld(t, types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists, Cutoff: "23:59", CutoffDayOffset: 7}},
		Job:      types.JobConfig{Type: types.TriggerHTTP, Config: httpJobConfig},
		PostRun: &types.PostRunConfig{Rules: []types.ValidationRule{{Key: "row-count", Check: types.CheckExists}},
			DriftField: "count", DriftThreshold: &threshold, WatchWindow: "6h"},
		DryRun: dryRun,
	})
	row := completedRow(runDate, "2026-03-10T12:00:00Z")
	if status != "" {
		row["status"] = &ddbtypes.AttributeValueMemberS{Value: status}
	}
	mem.Put("control", row)
	mem.Put("joblog", jobRow(runDate, types.JobEventSuccess))
	if dryRun {
		seedDryRunMarker(mem, runDate, "2026-03-10T11:00:00Z")
	}
	clock := mustTime(t, now)
	d.NowFunc = func() time.Time { return clock }
	d.Store.Now = func() time.Time { return clock }
	return d, mem, eb
}

// TestPostRunSensor_ArrivalMarker: a post-run rule's sensor written for a
// COMPLETED date records its arrival as SENSOR#postrun-check#<date> with
// {"arrived":"true"} and the own-row ttl, inside the watch window (no drift,
// drift, the first write that seeds the baseline) and after it, before any
// rerun request. The date is the one the write resolves to (the current UTC
// date without a date field, so a write for yesterday's completed row needs
// one), and a period key (dashed, compact or hourly) must name it; a key
// naming another period records nothing and the rest of the record goes on. A marker already there
// (alerted) is left as it was. A RUNNING or FAILED_FINAL date, a dry-run
// pipeline, the trigger sensor and a sensor no rule names record nothing.
func TestPostRunSensor_ArrivalMarker(t *testing.T) {
	arrived := map[string]interface{}{"arrived": "true"}
	tests := []struct {
		name       string
		key        string // the sensor written; empty means row-count
		hour       string // the write's hour; empty means a daily write
		noDate     bool   // the write has no date field
		runDate    string // the completed row's date; empty means 2026-03-10 (with hour)
		status     string // the trigger row; empty means COMPLETED
		now        string // empty means 13:00, inside the window
		count      string // empty means 100, the baseline's count
		noBaseline bool
		alerted    bool // an alerted marker is already there
		dryRun     bool
		want       map[string]interface{} // the marker's data; nil means none
		wantRerun  bool
		wantEvent  string // an event the record must still publish
	}{
		{name: "inside, no drift", want: arrived},
		{name: "inside, drift", count: "120", want: arrived, wantRerun: true},
		{name: "inside, first write seeds the baseline", noBaseline: true, want: arrived},
		{name: "after the window", now: "2026-03-10T19:00:00Z", count: "120", want: arrived},
		{name: "hourly date", hour: "05", want: arrived},
		{name: "alerted marker kept", alerted: true, want: map[string]interface{}{"alerted": "true"}},
		{name: "RUNNING", status: types.TriggerStatusRunning},
		{name: "dry run", dryRun: true, count: "120"},
		{name: "not a rule sensor (guard)", key: "other"},
		{name: "dashed period key", key: "row-count#2026-03-10", want: arrived},
		{name: "compact period key", key: "row-count#20260310", want: arrived},
		{name: "hourly period key", key: "row-count#2026-03-10T05", hour: "05", want: arrived},
		{name: "other day's key, no date field", key: "row-count#2026-03-09", noDate: true, wantEvent: string(types.EventPostRunFailed)},
		{name: "hourly key, no hour field", key: "row-count#2026-03-10T05", wantEvent: string(types.EventPostRunFailed)},
		{name: "trigger sensor (guard)", key: "upstream", wantRerun: true},
		{name: "yesterday's row, rule key, no date field", runDate: "2026-03-09", noDate: true},
		{name: "FAILED_FINAL (guard)", status: types.TriggerStatusFailedFinal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const date = "2026-03-10"
			runDate := cmp.Or(tt.runDate, date)
			if tt.hour != "" {
				runDate += "T" + tt.hour
			}
			d, mem, eb := markerWorld(t, runDate, tt.status, cmp.Or(tt.now, "2026-03-10T13:00:00Z"), tt.dryRun)
			ctx := context.Background()
			if !tt.noBaseline {
				if err := d.Store.WriteSensor(ctx, "p", "postrun-baseline#"+runDate, map[string]interface{}{
					"row-count": map[string]interface{}{"count": 100},
				}); err != nil {
					t.Fatal(err)
				}
			}
			markerSK := types.SensorSK("postrun-check#" + runDate)
			alertedRow := map[string]ddbtypes.AttributeValue{
				"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("p")}, "SK": &ddbtypes.AttributeValueMemberS{Value: markerSK},
				"data": &ddbtypes.AttributeValueMemberM{Value: map[string]ddbtypes.AttributeValue{"alerted": &ddbtypes.AttributeValueMemberS{Value: "true"}}},
				"ttl":  &ddbtypes.AttributeValueMemberN{Value: "1"},
			}
			if tt.alerted {
				mem.Put("control", alertedRow)
			}
			// The SK of each row put, in order.
			var puts []string
			fake := d.Store.Client.(*storetest.FakeDynamo)
			put := fake.PutItemFn
			fake.PutItemFn = func(ctx context.Context, in *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
				puts = append(puts, strAttr(in.Item["SK"]))
				return put(ctx, in)
			}
			data := map[string]events.DynamoDBAttributeValue{
				"date": events.NewStringAttribute(date), "count": events.NewNumberAttribute(cmp.Or(tt.count, "100"))}
			if tt.hour != "" {
				data["hour"] = events.NewStringAttribute(tt.hour)
			}
			if tt.noDate {
				delete(data, "date")
			}

			if err := handleRecord(ctx, d, sensorRecordWith(cmp.Or(tt.key, "row-count"), data)); err != nil {
				t.Fatalf("handleRecord: %v", err)
			}

			if tt.wantEvent != "" && !slices.Contains(eventTypes(eb.events), tt.wantEvent) {
				t.Errorf("events = %v, want %s among them", eventTypes(eb.events), tt.wantEvent)
			}
			got, err := d.Store.GetSensorData(ctx, "p", "postrun-check#"+runDate)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("postrun-check marker = %v, want %v", got, tt.want)
			}
			row := mem.Get("control", types.PipelinePK("p"), markerSK)
			if tt.alerted {
				if !reflect.DeepEqual(row, alertedRow) {
					t.Errorf("alerted marker = %v, want it unchanged: %v", row, alertedRow)
				}
			} else if tt.want != nil {
				wantTTL := strconv.FormatInt(d.NowFunc().Add(validation.MaxPostRunWatchWindow+24*time.Hour).Unix(), 10)
				if ttl, ok := row["ttl"].(*ddbtypes.AttributeValueMemberN); !ok || ttl.Value != wantTTL {
					t.Errorf("marker ttl = %v, want %s", row["ttl"], wantTTL)
				}
			}
			if tt.want == nil && slices.ContainsFunc(puts, func(sk string) bool { return strings.HasPrefix(sk, types.SensorSK("postrun-check#")) }) {
				t.Errorf("rows put %v; want no postrun-check marker for any date", puts)
			}
			rerunAt := slices.Index(puts, types.RerunRequestSK("stream", runDate))
			if (rerunAt >= 0) != tt.wantRerun {
				t.Fatalf("rows put %v; want a rerun request %v", puts, tt.wantRerun)
			}
			if markerAt := slices.Index(puts, markerSK); tt.want != nil && rerunAt >= 0 && (markerAt < 0 || markerAt > rerunAt) {
				t.Errorf("rows put %v; want the marker before the rerun request", puts)
			}
		})
	}
}

// TestPostRunSensor_ArrivalMarkerWriteFails: a failed marker write fails the
// record, so its redelivery retries the marker, before anything else: no rule
// is evaluated, no rerun is requested and nothing is published, with a
// write that would pass the rules (100) or drift from the baseline (120).
// The error names the pipeline and date.
func TestPostRunSensor_ArrivalMarkerWriteFails(t *testing.T) {
	for _, count := range []string{"100", "120"} {
		t.Run(count, func(t *testing.T) {
			const date = "2026-03-10"
			d, mem, eb := markerWorld(t, date, "", "2026-03-10T13:00:00Z", false)
			ctx := context.Background()
			if err := d.Store.WriteSensor(ctx, "p", "postrun-baseline#"+date, map[string]interface{}{
				"row-count": map[string]interface{}{"count": 100},
			}); err != nil {
				t.Fatal(err)
			}
			fake := d.Store.Client.(*storetest.FakeDynamo)
			put, query := fake.PutItemFn, fake.QueryFn
			fake.PutItemFn = func(ctx context.Context, in *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
				if strAttr(in.Item["SK"]) == types.SensorSK("postrun-check#"+date) {
					return nil, errors.New("put marker failed")
				}
				return put(ctx, in)
			}
			var controlQueries int
			fake.QueryFn = func(ctx context.Context, in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
				if *in.TableName == "control" {
					controlQueries++
				}
				return query(ctx, in)
			}

			err := handleRecord(ctx, d, postRunRecord("count", count))
			if err == nil || !strings.Contains(err.Error(), `record post-run arrival for "p" on `+date) {
				t.Fatalf("handleRecord = %v, want the marker write error naming p and %s", err, date)
			}
			req := mem.Get("control", types.PipelinePK("p"), types.RerunRequestSK("stream", date))
			if len(eb.events) != 0 || req != nil || controlQueries != 0 {
				t.Errorf("events %v, rerun request %v, control queries %d; want none, none and 0", eventTypes(eb.events), req, controlQueries)
			}
		})
	}
}

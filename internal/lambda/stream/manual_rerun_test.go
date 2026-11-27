package stream

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

const manualDate = "2026-03-10"

// manualRerunNow is 25h after the run of manualRerunWorld completed.
var manualRerunNow = time.Date(2026, 3, 11, 13, 0, 0, 0, time.UTC)

// manualRerunWorld is a stream-router for pipeline p (http job with job.config
// conf, 200h watch window, job.maxAutoRerunAge 24h) whose 2026-03-10 run
// run-1 started at 11:00 and completed at 12:00 with outcome. The clock reads
// manualRerunNow. Each opt changes the config before it is stored.
func manualRerunWorld(t *testing.T, conf map[string]interface{}, outcome string, opts ...func(*types.PipelineConfig)) (*lambda.Deps, *storetest.Mem, *recordingEventBridge) {
	t.Helper()
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists, Cutoff: "23:59", CutoffDayOffset: 7}},
		Job:      types.JobConfig{Type: types.TriggerHTTP, Config: conf, MaxAutoRerunAge: "24h"},
		PostRun:  &types.PostRunConfig{WatchWindow: "200h"},
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	d, mem, _, eb := jobWorld(t, cfg)
	row := completedRow(manualDate, "2026-03-10T12:00:00Z")
	row["startedAt"] = &ddbtypes.AttributeValueMemberS{Value: "2026-03-10T11:00:00Z"}
	mem.Put("control", row)
	mem.Put("joblog", map[string]ddbtypes.AttributeValue{
		"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("p")},
		"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("stream", manualDate, "1773144000000")},
		"event": &ddbtypes.AttributeValueMemberS{Value: outcome},
		"runId": &ddbtypes.AttributeValueMemberS{Value: "run-1"}})
	d.NowFunc = func() time.Time { return manualRerunNow }
	d.Store.Now = func() time.Time { return manualRerunNow }
	return d, mem, eb
}

// lateRecord is a write of the trigger sensor holding data (the date when
// nil), created 30s before manualRerunNow.
func lateRecord(data map[string]events.DynamoDBAttributeValue) events.DynamoDBEventRecord {
	if data == nil {
		data = map[string]events.DynamoDBAttributeValue{"date": events.NewStringAttribute(manualDate)}
	}
	r := sensorRecordWith("upstream", data)
	r.Change.ApproximateCreationDateTime = events.SecondsEpochTime{Time: manualRerunNow.Add(-30 * time.Second)}
	return r
}

func handleRecords(t *testing.T, d *lambda.Deps, records ...events.DynamoDBEventRecord) {
	t.Helper()
	for _, r := range records {
		if err := handleRecord(context.Background(), d, r); err != nil {
			t.Fatalf("handleRecord: %v", err)
		}
	}
}

func publishedTypes(eb *recordingEventBridge) []string {
	var got []string
	for _, e := range eb.events {
		got = append(got, e.Type)
	}
	return got
}

// markerCompletedAt is the completedAt of the date's manual-rerun alert
// marker for reason, "" when there is none.
func markerCompletedAt(mem *storetest.Mem, reason string) string {
	row := mem.Get("control", types.PipelinePK("p"), types.SensorSK("manual-rerun-alert-"+reason+"#"+manualDate))
	data, ok := row["data"].(*ddbtypes.AttributeValueMemberM)
	if !ok {
		return ""
	}
	return strAttr(data.Value["completedAt"])
}

var evManual, evStill, evLate = string(types.EventManualRerunRequired), string(types.EventManualRerunStillRequired), string(types.EventLateDataArrival)

// TestManualRerunRequired_LateData: late data 25h after completion, past the
// 24h job.maxAutoRerunAge, publishes MANUAL_RERUN_REQUIRED with what a person
// needs to rerun by hand, writes the alert marker and writes no request. The
// detail is compared whole, so the http job's headers, body, query and
// fragment cannot reach it.
func TestManualRerunRequired_LateData(t *testing.T) {
	conf := map[string]interface{}{"url": "https://example.com/job?token=abc#frag", "method": "POST",
		"headers": map[string]interface{}{"Authorization": "Bearer s3cret"}, "body": `{"password":"hunter2"}`}
	d, mem, eb := manualRerunWorld(t, conf, types.JobEventSuccess)
	handleRecords(t, d, lateRecord(nil))

	if got := publishedTypes(eb); !slices.Equal(got, []string{evLate, evManual}) {
		t.Fatalf("events = %v, want [%s %s]", got, evLate, evManual)
	}
	want := map[string]interface{}{
		"reason": "late-data", "blockedBy": "job.maxAutoRerunAge", "source": "sensor",
		"jobType": "http", "job": map[string]interface{}{"url": "https://example.com/job", "method": "POST"}, "lastRunId": "run-1",
		"startedAt": "2026-03-10T11:00:00Z", "completedAt": "2026-03-10T12:00:00Z", "finishedAgo": "25h0m0s",
		"maxAutoRerunAge": "24h0m0s", "watchWindow": "200h0m0s",
		"sensorKey": "upstream", "sensorData": map[string]interface{}{"date": manualDate}, "arrivedAt": "2026-03-11T12:59:30Z",
		"rerunRequest": map[string]interface{}{"table": "control", "PK": "PIPELINE#p", "SK": "RERUN_REQUEST#stream#2026-03-10", "reason": "manual"},
		"actionHint":   manualRerunHint,
	}
	if got := eb.events[1].Detail; !reflect.DeepEqual(got, want) {
		t.Errorf("detail = %v\nwant %v", got, want)
	}
	if got := markerCompletedAt(mem, "late-data"); got != "2026-03-10T12:00:00Z" {
		t.Errorf("marker completedAt = %q, want the run's", got)
	}
	if mem.Get("control", types.PipelinePK("p"), types.RerunRequestSK("stream", manualDate)) != nil {
		t.Error("a rerun request was written")
	}
}

// TestManualRerunRequired_RequestRecheck: an automatic request handled past
// the age publishes MANUAL_RERUN_REQUIRED from the request, with its time and
// no sensor fields, and marks only its own reason; a second request for the
// same completion publishes MANUAL_RERUN_STILL_REQUIRED, and so does a
// late-data request after a late sensor write was alerted.
func TestManualRerunRequired_RequestRecheck(t *testing.T) {
	for _, tt := range []struct{ reason, other string }{{"late-data", "data-drift"}, {"data-drift", "late-data"}} {
		t.Run(tt.reason, func(t *testing.T) {
			d, mem, eb := manualRerunWorld(t, httpJobConfig, types.JobEventSuccess)
			handleRecords(t, d, rerunRequestRecord(tt.reason, "2026-03-11T12:30:00Z"), rerunRequestRecord(tt.reason, "2026-03-11T12:40:00Z"))

			if got := publishedTypes(eb); !slices.Equal(got, []string{evManual, evStill}) {
				t.Fatalf("events = %v, want [%s %s]", got, evManual, evStill)
			}
			got := eb.events[0].Detail
			if got["source"] != "rerun-request" || got["reason"] != tt.reason || got["requestedAt"] != "2026-03-11T12:30:00Z" ||
				got["blockedBy"] != "job.maxAutoRerunAge" {
				t.Errorf("detail = %v, want source rerun-request, reason %s, the request's time and the age", got, tt.reason)
			}
			for _, k := range []string{"sensorKey", "sensorData", "arrivedAt"} {
				if _, ok := got[k]; ok {
					t.Errorf("detail has %s: %v", k, got)
				}
			}
			if markerCompletedAt(mem, tt.reason) == "" || markerCompletedAt(mem, tt.other) != "" {
				t.Errorf("markers: %s %q, %s %q; want only %s", tt.reason, markerCompletedAt(mem, tt.reason),
					tt.other, markerCompletedAt(mem, tt.other), tt.reason)
			}
		})
	}
	t.Run("late-data request after a sensor alert", func(t *testing.T) {
		d, _, eb := manualRerunWorld(t, httpJobConfig, types.JobEventSuccess)
		handleRecords(t, d, lateRecord(nil), rerunRequestRecord("late-data", "2026-03-11T12:30:00Z"))
		if got := publishedTypes(eb); !slices.Equal(got, []string{evLate, evManual, evStill}) {
			t.Errorf("events = %v, want [%s %s %s]", got, evLate, evManual, evStill)
		}
	})
	t.Run("outcome read fails", func(t *testing.T) {
		d, _, eb := manualRerunWorld(t, httpJobConfig, types.JobEventSuccess)
		fake := d.Store.Client.(*storetest.FakeDynamo)
		query := fake.QueryFn
		fake.QueryFn = func(ctx context.Context, in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
			if *in.TableName == "joblog" {
				return nil, errors.New("query failed")
			}
			return query(ctx, in)
		}
		if err := handleRecord(context.Background(), d, rerunRequestRecord("late-data", "2026-03-11T12:30:00Z")); err == nil {
			t.Error("handleRecord = nil, want the read error so the stream retries")
		}
		if got := publishedTypes(eb); len(got) != 0 {
			t.Errorf("events = %v, want none", got)
		}
	})
}

// TestManualRerunRequired_AlertsOncePerCompletion: a second arrival for the
// same completion publishes MANUAL_RERUN_STILL_REQUIRED; once a later run
// completes, the next arrival alerts again.
func TestManualRerunRequired_AlertsOncePerCompletion(t *testing.T) {
	d, mem, eb := manualRerunWorld(t, httpJobConfig, types.JobEventSuccess)
	handleRecords(t, d, lateRecord(nil), lateRecord(nil))
	mem.Put("control", completedRow(manualDate, "2026-03-10T12:30:00Z"))
	handleRecords(t, d, lateRecord(nil))

	if got, want := publishedTypes(eb), []string{evLate, evManual, evLate, evStill, evLate, evManual}; !slices.Equal(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	if got := eb.events[3].Detail["completedAt"]; got != "2026-03-10T12:00:00Z" {
		t.Errorf("still-required completedAt = %v, want the first run's", got)
	}
	if got := markerCompletedAt(mem, "late-data"); got != "2026-03-10T12:30:00Z" {
		t.Errorf("marker completedAt = %q, want the later run's", got)
	}
}

// TestManualRerunRequired_Skips: at either site (a late sensor write, or an
// automatic late-data request), a date whose latest outcome failed, or with
// no completedAt, gets no manual-rerun event and no marker, and so does a
// calendar-excluded execution date, where a manual rerun would be refused; a
// publish that fails writes no marker, so the next arrival alerts (at the
// sensor, fail 2 fails LATE_DATA_ARRIVAL and MANUAL_RERUN_REQUIRED). A marker
// that cannot be read alerts as if absent.
func TestManualRerunRequired_Skips(t *testing.T) {
	for _, tt := range []struct {
		name, outcome  string
		request        bool // an automatic late-data request, not a sensor write
		noCompletedAt  bool
		excluded       bool // the execution date is calendar-excluded
		fail           int
		failMarkerRead bool
		wantFirst      []string // events of the first arrival; a marker exactly when it has REQUIRED
		wantSecond     []string // events of the second arrival
	}{
		{name: "not success", outcome: types.JobEventFail},
		{name: "no completion time", outcome: types.JobEventSuccess, noCompletedAt: true},
		{name: "publish fails", outcome: types.JobEventSuccess, fail: 2, wantSecond: []string{evLate, evManual}},
		{name: "excluded date", outcome: types.JobEventSuccess, excluded: true, wantFirst: []string{evLate}, wantSecond: []string{evLate}},
		{name: "request, not success", request: true, outcome: types.JobEventFail},
		{name: "request, no completion time", request: true, outcome: types.JobEventSuccess, noCompletedAt: true},
		{name: "request, publish fails", request: true, outcome: types.JobEventSuccess, fail: 1, wantSecond: []string{evManual}},
		{name: "request, marker read fails", request: true, outcome: types.JobEventSuccess, failMarkerRead: true,
			wantFirst: []string{evManual}, wantSecond: []string{evManual}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			d, mem, eb := manualRerunWorld(t, httpJobConfig, tt.outcome, func(c *types.PipelineConfig) {
				if tt.excluded {
					c.Schedule.Exclude = &types.ExclusionConfig{Dates: []string{manualDate}}
				}
			})
			if tt.noCompletedAt {
				mem.Put("control", completedRow(manualDate, ""))
			}
			if tt.failMarkerRead {
				fake := d.Store.Client.(*storetest.FakeDynamo)
				get := fake.GetItemFn
				fake.GetItemFn = func(ctx context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
					if in.Key["SK"].(*ddbtypes.AttributeValueMemberS).Value == types.SensorSK("manual-rerun-alert-late-data#"+manualDate) {
						return nil, errors.New("get failed")
					}
					return get(ctx, in)
				}
			}
			record := lateRecord(nil)
			if tt.request {
				record = rerunRequestRecord("late-data", "2026-03-11T12:30:00Z")
			}
			eb.fail = tt.fail
			handleRecords(t, d, record)
			if got, marker := publishedTypes(eb), markerCompletedAt(mem, "late-data"); !slices.Equal(got, tt.wantFirst) || (marker != "") != slices.Contains(tt.wantFirst, evManual) {
				t.Fatalf("first arrival: events %v, marker %q; want %v", got, marker, tt.wantFirst)
			}
			handleRecords(t, d, record)
			if got := publishedTypes(eb)[len(tt.wantFirst):]; !slices.Equal(got, tt.wantSecond) {
				t.Errorf("second arrival: events %v, want %v", got, tt.wantSecond)
			}
		})
	}
}

// TestManualRerunRequired_LargeSensorData: sensor data over 32 KB is left out
// of the event and flagged.
func TestManualRerunRequired_LargeSensorData(t *testing.T) {
	d, _, eb := manualRerunWorld(t, httpJobConfig, types.JobEventSuccess)
	handleRecords(t, d, lateRecord(map[string]events.DynamoDBAttributeValue{
		"date": events.NewStringAttribute(manualDate), "blob": events.NewStringAttribute(strings.Repeat("x", 32*1024))}))

	if got := publishedTypes(eb); !slices.Equal(got, []string{evLate, evManual}) {
		t.Fatalf("events = %v", got)
	}
	got := eb.events[1].Detail
	if _, ok := got["sensorData"]; ok || got["sensorDataOmitted"] != true {
		t.Errorf("sensorData present %v, sensorDataOmitted %v; want absent and true", ok, got["sensorDataOmitted"])
	}
}

// TestJobIdentifiers: each job type gives only the fields that name the job,
// with URLs stripped of user info, query and fragment. Every registered job
// type needs a row.
func TestJobIdentifiers(t *testing.T) {
	type m = map[string]interface{}
	secrets := m{"X-Token": "s3cret"}
	tests := map[types.TriggerType]struct {
		conf m
		want map[string]string
	}{
		types.TriggerHTTP: {m{"url": "https://u:pw@example.com/run?k=v#f", "method": "POST", "headers": secrets, "body": "b"},
			map[string]string{"url": "https://example.com/run", "method": "POST"}},
		types.TriggerAirflow: {m{"url": "https://airflow.example.com", "dagId": "daily", "headers": secrets, "body": "b"},
			map[string]string{"url": "https://airflow.example.com", "dagId": "daily"}},
		types.TriggerGlue: {m{"jobName": "j", "arguments": secrets}, map[string]string{"jobName": "j"}},
		types.TriggerEMR: {m{"clusterId": "j-1", "stepName": "s", "command": "c", "arguments": secrets},
			map[string]string{"clusterId": "j-1", "stepName": "s"}},
		types.TriggerEMRServerless: {m{"applicationId": "a", "jobName": "j", "executionRoleArn": "r", "entryPoint": "e",
			"entryPointArguments": []string{"x"}, "sparkSubmitParameters": "--conf"}, map[string]string{"applicationId": "a", "jobName": "j"}},
		types.TriggerDatabricks: {m{"workspaceUrl": "https://db.example.com", "jobId": "1", "headers": secrets, "arguments": secrets},
			map[string]string{"workspaceUrl": "https://db.example.com", "jobId": "1"}},
		types.TriggerStepFunction: {m{"stateMachineArn": "arn:sm", "arguments": secrets}, map[string]string{"stateMachineArn": "arn:sm"}},
		types.TriggerLambda:       {m{"functionName": "f", "payload": "p"}, map[string]string{"functionName": "f"}},
	}
	for typ := range lambda.TriggerUnmarshalers {
		if _, ok := tests[typ]; !ok {
			t.Errorf("job type %q has no row", typ)
		}
	}
	for typ, tt := range tests {
		t.Run(string(typ), func(t *testing.T) {
			got := jobIdentifiers(&types.PipelineConfig{Job: types.JobConfig{Type: typ, Config: tt.conf}})
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("jobIdentifiers = %v, want %v", got, tt.want)
			}
		})
	}
}

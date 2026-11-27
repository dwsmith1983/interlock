package stream

import (
	"context"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
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
// manualRerunNow.
func manualRerunWorld(t *testing.T, conf map[string]interface{}, outcome string) (*lambda.Deps, *storetest.Mem, *recordingEventBridge) {
	t.Helper()
	d, mem, _, eb := jobWorld(t, types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists, Cutoff: "23:59", CutoffDayOffset: 7}},
		Job:      types.JobConfig{Type: types.TriggerHTTP, Config: conf, MaxAutoRerunAge: "24h"},
		PostRun:  &types.PostRunConfig{WatchWindow: "200h"},
	})
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

// markerCompletedAt is the completedAt of the date's late-data manual-rerun
// alert marker, "" when there is none.
func markerCompletedAt(mem *storetest.Mem) string {
	row := mem.Get("control", types.PipelinePK("p"), types.SensorSK("manual-rerun-alert-late-data#"+manualDate))
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
	if got := markerCompletedAt(mem); got != "2026-03-10T12:00:00Z" {
		t.Errorf("marker completedAt = %q, want the run's", got)
	}
	if mem.Get("control", types.PipelinePK("p"), types.RerunRequestSK("stream", manualDate)) != nil {
		t.Error("a rerun request was written")
	}
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
	if got := markerCompletedAt(mem); got != "2026-03-10T12:30:00Z" {
		t.Errorf("marker completedAt = %q, want the later run's", got)
	}
}

// TestManualRerunRequired_Skips: a date whose latest outcome failed, or with
// no completedAt, gets no manual-rerun event and no marker; a publish that
// fails (fail 2 fails LATE_DATA_ARRIVAL and MANUAL_RERUN_REQUIRED) writes no
// marker, so the next arrival alerts.
func TestManualRerunRequired_Skips(t *testing.T) {
	for _, tt := range []struct {
		name, outcome string
		noCompletedAt bool
		fail          int
		wantSecond    []string // events of the second arrival
	}{
		{name: "not success", outcome: types.JobEventFail},
		{name: "no completion time", outcome: types.JobEventSuccess, noCompletedAt: true},
		{name: "publish fails", outcome: types.JobEventSuccess, fail: 2, wantSecond: []string{evLate, evManual}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			d, mem, eb := manualRerunWorld(t, httpJobConfig, tt.outcome)
			if tt.noCompletedAt {
				mem.Put("control", completedRow(manualDate, ""))
			}
			eb.fail = tt.fail
			handleRecords(t, d, lateRecord(nil))
			if got := publishedTypes(eb); len(got) != 0 || markerCompletedAt(mem) != "" {
				t.Fatalf("first arrival: events %v, marker %q; want none", got, markerCompletedAt(mem))
			}
			handleRecords(t, d, lateRecord(nil))
			if got := publishedTypes(eb); !slices.Equal(got, tt.wantSecond) {
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

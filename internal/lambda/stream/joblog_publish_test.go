package stream

import (
	"context"
	"maps"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/aws/aws-lambda-go/events"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestStreamJobRows_PublishGates: JOB_FAILED and JOB_COMPLETED come only from
// a new row (INSERT) with a valid date, for a configured pipeline that has a
// trigger row for that schedule and date. Anything else is logged and dropped.
func TestStreamJobRows_PublishGates(t *testing.T) {
	failed := map[string]interface{}{"event": types.JobEventFail, "error": "boom", "runId": "run-a", "category": string(types.FailurePermanent)}
	tests := []struct {
		name, eventName, date, event, pipeline string
		noTrigger, dryRun, noEvent, wantErr    bool
		ebFail                                 int
		wantType                               types.EventDetailType
		wantDetail                             map[string]interface{}
	}{
		{name: "fail row", eventName: "INSERT", date: testDate, event: types.JobEventFail, wantType: types.EventJobFailed, wantDetail: failed},
		{name: "timeout row", eventName: "INSERT", date: testDate, event: types.JobEventTimeout, wantType: types.EventJobFailed,
			wantDetail: map[string]interface{}{"event": types.JobEventTimeout, "error": "boom", "runId": "run-a", "category": string(types.FailurePermanent)}},
		{name: "success row", eventName: "INSERT", date: testDate, event: types.JobEventSuccess, wantType: types.EventJobCompleted},
		{name: "hourly date", eventName: "INSERT", date: testDate + "T05", event: types.JobEventFail, wantType: types.EventJobFailed, wantDetail: failed},
		{name: "MODIFY record", eventName: "MODIFY", date: testDate, event: types.JobEventFail},
		{name: "MODIFY success", eventName: "MODIFY", date: testDate, event: types.JobEventSuccess},
		{name: "junk date", eventName: "INSERT", date: "2026-13-45", event: types.JobEventFail},
		{name: "no config", eventName: "INSERT", date: testDate, event: types.JobEventFail, pipeline: "other"},
		{name: "no trigger row", eventName: "INSERT", date: testDate, event: types.JobEventSuccess, noTrigger: true},
		{name: "fail row without a trigger row", eventName: "INSERT", date: testDate, event: types.JobEventFail, noTrigger: true},
		{name: "dry run without its marker", eventName: "INSERT", date: testDate, event: types.JobEventFail, dryRun: true},
		{name: "infra-trigger-exhausted row", eventName: "INSERT", date: testDate, event: types.JobEventInfraTriggerExhausted},
		{name: "rerun-accepted row", eventName: "INSERT", date: testDate, event: types.JobEventRerunAccepted},
		{name: "no event attribute", eventName: "INSERT", date: testDate, event: types.JobEventFail, noEvent: true},
		{name: "publish fails", eventName: "INSERT", date: testDate, event: types.JobEventSuccess, ebFail: 1, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := types.PipelineConfig{Pipeline: types.PipelineIdentity{ID: cmpOr(tt.pipeline, "p")}, Job: types.JobConfig{Type: types.TriggerHTTP}, DryRun: tt.dryRun,
				Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists}}}
			d, mem, _, eb := jobWorld(t, cfg)
			if !tt.noTrigger {
				mem.Put("control", map[string]ddbtypes.AttributeValue{
					"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("p")},
					"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("stream", tt.date)},
					"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusRunning},
				})
			}
			record := jobRecord(tt.eventName, tt.date, tt.event, types.FailurePermanent)
			if tt.noEvent {
				delete(record.Change.NewImage, "event")
			}
			eb.fail = tt.ebFail
			if err := handleRecord(context.Background(), d, record); (err != nil) != tt.wantErr {
				t.Fatalf("handleRecord error = %v, want an error: %v", err, tt.wantErr)
			}
			if tt.wantType == "" {
				if len(eb.events) != 0 {
					t.Errorf("events = %+v, want none", eb.events)
				}
				return
			}
			if len(eb.events) != 1 || eb.events[0].Type != string(tt.wantType) || !maps.Equal(eb.events[0].Detail, tt.wantDetail) {
				t.Errorf("events = %+v, want one %s with detail %v", eb.events, tt.wantType, tt.wantDetail)
			}
		})
	}
}

func cmpOr(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// TestStreamJobFail_TruncatesError: a row's error is cut to 2 KB on a UTF-8
// boundary before it reaches the message or the detail.
func TestStreamJobFail_TruncatesError(t *testing.T) {
	d, mem, _, eb := jobWorld(t, types.PipelineConfig{Pipeline: types.PipelineIdentity{ID: "p"}, Job: types.JobConfig{Type: types.TriggerHTTP}})
	mem.Put("control", map[string]ddbtypes.AttributeValue{
		"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("p")}, "SK": &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("stream", testDate)},
	})
	record := jobRecord("INSERT", testDate, types.JobEventFail, "")
	long := strings.Repeat("a", 2047) + strings.Repeat("é", 1000) // byte 2048 falls inside an é
	record.Change.NewImage["error"] = events.NewStringAttribute(long)

	if err := handleRecord(context.Background(), d, record); err != nil {
		t.Fatalf("handleRecord: %v", err)
	}
	if len(eb.events) != 1 {
		t.Fatalf("events = %+v, want one", eb.events)
	}
	got, _ := eb.events[0].Detail["error"].(string)
	if got != strings.Repeat("a", 2047) || !utf8.ValidString(got) {
		t.Errorf("detail error = %d bytes (valid UTF-8 %v), want the 2047 bytes before the split rune", len(got), utf8.ValidString(got))
	}
	if msg := eb.events[0].Message; !strings.HasSuffix(msg, ": "+got) || len(msg) > 2048+64 {
		t.Errorf("message = %d bytes ending %q, want the cut error at its end", len(msg), msg[max(0, len(msg)-20):])
	}
}

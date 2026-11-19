package sla_test

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/lambda/sla"
	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestSLAFireAlert_RetryKeepsAlerts pins that a retry does not silence the
// SLA: while CompleteTrigger's retry runs, the trigger row stays RUNNING and
// the newest job row is the previous attempt's fail, and the warning and
// breach still publish. A finished trigger row still suppresses them, and a
// warning due at or after the breach time is dropped.
func TestSLAFireAlert_RetryKeepsAlerts(t *testing.T) {
	const date = "2026-03-10"
	now := time.Date(2026, 3, 11, 9, 30, 0, 0, time.UTC)
	tests := []struct {
		name        string
		trigger     string // "" means no trigger row
		jobRow      string // "" means no job rows
		alertType   string
		breachAt    string // "" means 2026-03-11T10:00:00Z
		wantPublish int
	}{
		{name: "RetryRunningWarning", trigger: types.TriggerStatusRunning, jobRow: types.JobEventFail, alertType: "SLA_WARNING", wantPublish: 1},
		{name: "RetryRunningBreach", trigger: types.TriggerStatusRunning, jobRow: types.JobEventFail, alertType: "SLA_BREACH", wantPublish: 1},
		{name: "RetryAfterTimeoutBreach", trigger: types.TriggerStatusRunning, jobRow: types.JobEventTimeout, alertType: "SLA_BREACH", wantPublish: 1},
		{name: "RunningSuccessSuppresses", trigger: types.TriggerStatusRunning, jobRow: types.JobEventSuccess, alertType: "SLA_BREACH", wantPublish: 0},
		{name: "RunningValidationExhaustedSuppresses", trigger: types.TriggerStatusRunning, jobRow: types.JobEventValidationExhausted, alertType: "SLA_BREACH", wantPublish: 0},
		{name: "CompletedSuppresses", trigger: types.TriggerStatusCompleted, jobRow: types.JobEventSuccess, alertType: "SLA_BREACH", wantPublish: 0},
		{name: "FailedFinalSuppresses", trigger: types.TriggerStatusFailedFinal, jobRow: types.JobEventFail, alertType: "SLA_BREACH", wantPublish: 0},
		{name: "NoTriggerTerminalRowSuppresses", jobRow: types.JobEventInfraTriggerExhausted, alertType: "SLA_BREACH", wantPublish: 0},
		{name: "NotStartedPublishes", alertType: "SLA_BREACH", wantPublish: 1},
		{name: "ReleasedEvaluationPublishes", jobRow: types.JobEventEvaluationReleased, alertType: "SLA_BREACH", wantPublish: 1},
		{name: "WarningAtBreachSuppressed", alertType: "SLA_WARNING", breachAt: "2026-03-11T09:30:00Z", wantPublish: 0},
		{name: "WarningPastBreachSuppressed", alertType: "SLA_WARNING", breachAt: "2026-03-11T09:00:00Z", wantPublish: 0},
		{name: "BreachPastBreachPublishes", alertType: "SLA_BREACH", breachAt: "2026-03-11T09:00:00Z", wantPublish: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake, mem := storetest.NewMem()
			if tt.trigger != "" {
				mem.Put("control", map[string]ddbtypes.AttributeValue{
					"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("p")},
					"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("stream", date)},
					"status": &ddbtypes.AttributeValueMemberS{Value: tt.trigger},
				})
			}
			if tt.jobRow != "" {
				mem.Put("joblog", map[string]ddbtypes.AttributeValue{
					"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("p")},
					"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("stream", date, "1773133200000")},
					"event": &ddbtypes.AttributeValueMemberS{Value: tt.jobRow},
				})
			}
			d := testDeps(fake, now)
			eb := &countingEventBridge{}
			d.EventBridge, d.EventBusName = eb, "bus"
			breachAt := tt.breachAt
			if breachAt == "" {
				breachAt = "2026-03-11T10:00:00Z"
			}

			out, err := sla.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
				Mode: "fire-alert", PipelineID: "p", ScheduleID: "stream", Date: date,
				AlertType: tt.alertType, BreachAt: breachAt,
			})

			if err != nil {
				t.Fatalf("fire-alert: %v", err)
			}
			if out.AlertType != tt.alertType {
				t.Errorf("AlertType = %q, want %q", out.AlertType, tt.alertType)
			}
			if out.FiredAt != "2026-03-11T09:30:00Z" {
				t.Errorf("FiredAt = %q, want 2026-03-11T09:30:00Z", out.FiredAt)
			}
			if eb.calls != tt.wantPublish {
				t.Fatalf("published %d events, want %d", eb.calls, tt.wantPublish)
			}
		})
	}
}

// TestSLAFireAlert_Publish pins what fire-alert sends for each alert type on
// a pipeline with no trigger or job rows, and how it fails.
func TestSLAFireAlert_Publish(t *testing.T) {
	now := time.Date(2026, 3, 11, 9, 30, 0, 0, time.UTC)
	tests := []struct {
		name      string
		alertType string
		breachAt  string
		noEB      bool
		ebErr     error
		wantErr   string
		wantTypes []string
	}{
		{name: "warning without breach time", alertType: "SLA_WARNING", wantTypes: []string{"SLA_WARNING"}},
		{name: "breach", alertType: "SLA_BREACH", wantTypes: []string{"SLA_BREACH"}},
		{name: "relative warning", alertType: "RELATIVE_SLA_WARNING", breachAt: "2099-01-01T00:00:00Z", wantTypes: []string{"RELATIVE_SLA_WARNING"}},
		{name: "relative breach", alertType: "RELATIVE_SLA_BREACH", wantTypes: []string{"RELATIVE_SLA_BREACH"}},
		{name: "no EventBridge", alertType: "SLA_BREACH", noEB: true},
		{
			name: "publish error", alertType: "SLA_BREACH", ebErr: errors.New("EventBridge unavailable"),
			wantErr: "publish SLA event: publish SLA_BREACH event: EventBridge unavailable",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake, _ := storetest.NewMem()
			d := testDeps(fake, now)
			eb := &countingEventBridge{err: tt.ebErr}
			if !tt.noEB {
				d.EventBridge, d.EventBusName = eb, "bus"
			}

			out, err := sla.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
				Mode: "fire-alert", PipelineID: "p", ScheduleID: "stream", Date: "2026-03-10",
				AlertType: tt.alertType, BreachAt: tt.breachAt,
			})

			if tt.wantErr != "" {
				if err == nil || err.Error() != tt.wantErr {
					t.Fatalf("error = %v, want %q", err, tt.wantErr)
				}
				if out != (lambda.SLAMonitorOutput{}) {
					t.Errorf("output = %+v, want zero", out)
				}
				return
			}
			if err != nil {
				t.Fatalf("fire-alert: %v", err)
			}
			want := lambda.SLAMonitorOutput{AlertType: tt.alertType, FiredAt: "2026-03-11T09:30:00Z"}
			if out != want {
				t.Errorf("output = %+v, want %+v", out, want)
			}
			if !slices.Equal(eb.detailTypes, tt.wantTypes) {
				t.Errorf("published %v, want %v", eb.detailTypes, tt.wantTypes)
			}
		})
	}
}

// TestSLAFireAlert_EventDetail pins the event fire-alert publishes: its
// envelope and the whole detail map alert consumers read.
func TestSLAFireAlert_EventDetail(t *testing.T) {
	const (
		date        = "2026-03-10"
		b           = "2026-03-11T10:00:00Z"
		running     = types.TriggerStatusRunning
		checkSensor = "pipeline not started \u2014 check sensor data"
		investigate = "pipeline not started \u2014 investigate trigger"
		mayComplete = "pipeline running \u2014 may complete before breach"
	)
	now := time.Date(2026, 3, 11, 9, 30, 0, 0, time.UTC)
	tests := []struct {
		name       string
		trigger    string // "" means no trigger row
		alertType  string
		breachAt   string
		deadline   string
		wantDetail map[string]interface{}
	}{
		{name: "not started warning", alertType: "SLA_WARNING", breachAt: b, wantDetail: map[string]interface{}{"status": "not started", "source": "schedule", "actionHint": checkSensor, "breachAt": b}},
		{name: "running warning", trigger: running, alertType: "SLA_WARNING", breachAt: b, wantDetail: map[string]interface{}{"status": running, "source": "schedule", "actionHint": mayComplete, "breachAt": b}},
		{name: "running breach", trigger: running, alertType: "SLA_BREACH", wantDetail: map[string]interface{}{"status": running, "source": "schedule", "actionHint": mayComplete}},
		{name: "not started breach", alertType: "SLA_BREACH", wantDetail: map[string]interface{}{"status": "not started", "source": "schedule", "actionHint": investigate}},
		{name: "not started breach with deadline", alertType: "SLA_BREACH", deadline: "14:00", wantDetail: map[string]interface{}{"status": "not started", "source": "schedule", "actionHint": investigate, "deadline": "14:00"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake, mem := storetest.NewMem()
			if tt.trigger != "" {
				mem.Put("control", map[string]ddbtypes.AttributeValue{
					"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("p")},
					"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("stream", date)},
					"status": &ddbtypes.AttributeValueMemberS{Value: tt.trigger},
				})
			}
			d := testDeps(fake, now)
			eb := &countingEventBridge{}
			d.EventBridge, d.EventBusName = eb, "bus"

			_, err := sla.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
				Mode: "fire-alert", PipelineID: "p", ScheduleID: "stream", Date: date,
				AlertType: tt.alertType, BreachAt: tt.breachAt, Deadline: tt.deadline,
			})
			if err != nil {
				t.Fatalf("fire-alert: %v", err)
			}
			if len(eb.details) != 1 {
				t.Fatalf("published %d events, want 1", len(eb.details))
			}
			evt := eb.details[0]
			if evt.PipelineID != "p" || evt.ScheduleID != "stream" || evt.Date != date {
				t.Errorf("event key = %s/%s/%s, want p/stream/%s", evt.PipelineID, evt.ScheduleID, evt.Date, date)
			}
			if want := "pipeline p: " + tt.alertType; evt.Message != want {
				t.Errorf("Message = %q, want %q", evt.Message, want)
			}
			if !evt.Timestamp.Equal(now) {
				t.Errorf("Timestamp = %v, want %v", evt.Timestamp, now)
			}
			if !reflect.DeepEqual(evt.Detail, tt.wantDetail) {
				t.Errorf("Detail = %v, want %v", evt.Detail, tt.wantDetail)
			}
		})
	}
}

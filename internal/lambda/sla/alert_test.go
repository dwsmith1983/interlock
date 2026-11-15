package sla_test

import (
	"context"
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
// breach still publish. A finished trigger row still suppresses them.
func TestSLAFireAlert_RetryKeepsAlerts(t *testing.T) {
	const date = "2026-03-10"
	now := time.Date(2026, 3, 11, 9, 30, 0, 0, time.UTC)
	tests := []struct {
		name        string
		trigger     string // "" means no trigger row
		jobRow      string // "" means no job rows
		alertType   string
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

			out, err := sla.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{
				Mode: "fire-alert", PipelineID: "p", ScheduleID: "stream", Date: date,
				AlertType: tt.alertType, BreachAt: "2026-03-11T10:00:00Z",
			})

			if err != nil {
				t.Fatalf("fire-alert: %v", err)
			}
			if out.AlertType != tt.alertType {
				t.Errorf("AlertType = %q, want %q", out.AlertType, tt.alertType)
			}
			if eb.calls != tt.wantPublish {
				t.Fatalf("published %d events, want %d", eb.calls, tt.wantPublish)
			}
		})
	}
}

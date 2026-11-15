package lambda_test

import (
	"context"
	"io"
	"log/slog"
	"testing"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestRunFinished covers the rule the SLA paths share: a trigger row's status
// decides, and the job log decides only when there is no trigger row. During
// a retry the row is RUNNING while the newest job row is the previous
// attempt's fail, and the run is not finished.
func TestRunFinished(t *testing.T) {
	const date = "2026-03-01"
	tests := []struct {
		name    string
		trigger string // "" means no trigger row
		jobRow  string // "" means no job rows
		want    bool
	}{
		{name: "RunningWithPreviousAttemptFail", trigger: types.TriggerStatusRunning, jobRow: types.JobEventFail, want: false},
		{name: "RunningWithPreviousAttemptTimeout", trigger: types.TriggerStatusRunning, jobRow: types.JobEventTimeout, want: false},
		{name: "RunningWithSuccess", trigger: types.TriggerStatusRunning, jobRow: types.JobEventSuccess, want: true},
		{name: "RunningWithValidationExhausted", trigger: types.TriggerStatusRunning, jobRow: types.JobEventValidationExhausted, want: true},
		{name: "RunningWithInfraTriggerExhausted", trigger: types.TriggerStatusRunning, jobRow: types.JobEventInfraTriggerExhausted, want: true},
		{name: "RunningWithNonTerminalRow", trigger: types.TriggerStatusRunning, jobRow: types.JobEventInfraTriggerFailure, want: false},
		{name: "RunningNoRows", trigger: types.TriggerStatusRunning, want: false},
		{name: "Completed", trigger: types.TriggerStatusCompleted, want: true},
		{name: "FailedFinal", trigger: types.TriggerStatusFailedFinal, jobRow: types.JobEventFail, want: true},
		{name: "NoTriggerTerminalRow", jobRow: types.JobEventInfraTriggerExhausted, want: true},
		{name: "NoTriggerNoRows", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake, mem := storetest.NewMem()
			if tt.jobRow != "" {
				mem.Put("joblog", map[string]ddbtypes.AttributeValue{
					"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("p")},
					"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("s", date, "1772366400000")},
					"event": &ddbtypes.AttributeValueMemberS{Value: tt.jobRow},
				})
			}
			var tr *types.ControlRecord
			if tt.trigger != "" {
				tr = &types.ControlRecord{PK: types.PipelinePK("p"), SK: types.TriggerSK("s", date), Status: tt.trigger}
			}
			d := &lambda.Deps{Store: storetest.NewStore(fake), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}

			finished, reason := lambda.RunFinished(context.Background(), d, tr, "p", "s", date)

			if finished != tt.want {
				t.Fatalf("RunFinished = %v (%q), want %v", finished, reason, tt.want)
			}
			if finished == (reason == "") {
				t.Fatalf("RunFinished = %v with reason %q, want a reason exactly when finished", finished, reason)
			}
		})
	}
}

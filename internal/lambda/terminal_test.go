package lambda_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestRunFinished covers the rule the SLA paths share: a trigger row's status
// decides, whatever the job log says (the newest job row of a RUNNING date is
// often an earlier run's), and the job log decides only when there is no
// trigger row.
func TestRunFinished(t *testing.T) {
	const date = "2026-03-01"
	tests := []struct {
		name    string
		trigger string // "" means no trigger row
		jobRow  string // "" means no job rows
		newer   string // a job row newer than jobRow
		want    bool
	}{
		{name: "RunningWithPreviousAttemptFail", trigger: types.TriggerStatusRunning, jobRow: types.JobEventFail, want: false},
		{name: "RunningWithPreviousAttemptTimeout", trigger: types.TriggerStatusRunning, jobRow: types.JobEventTimeout, want: false},
		{name: "RunningWithSuccess", trigger: types.TriggerStatusRunning, jobRow: types.JobEventSuccess, want: false},
		{name: "RunningWithValidationExhausted", trigger: types.TriggerStatusRunning, jobRow: types.JobEventValidationExhausted, want: false},
		{name: "RunningWithInfraTriggerExhausted", trigger: types.TriggerStatusRunning, jobRow: types.JobEventInfraTriggerExhausted, want: false},
		{name: "RunningWithNonTerminalRow", trigger: types.TriggerStatusRunning, jobRow: types.JobEventInfraTriggerFailure, want: false},
		{name: "RunningNoRows", trigger: types.TriggerStatusRunning, want: false},
		{name: "Completed", trigger: types.TriggerStatusCompleted, want: true},
		{name: "FailedFinal", trigger: types.TriggerStatusFailedFinal, jobRow: types.JobEventFail, want: true},
		{name: "NoTriggerTerminalRow", jobRow: types.JobEventInfraTriggerExhausted, want: true},
		{name: "NoTriggerNoRows", want: false},
		{name: "NoTriggerEvaluationReleased", jobRow: types.JobEventEvaluationReleased, want: false},
		{name: "NoTriggerSuccess", jobRow: types.JobEventSuccess, want: true},
		{name: "NoTriggerSuccessThenRerunRejected", jobRow: types.JobEventSuccess, newer: types.JobEventRerunRejected, want: true},
		{name: "NoTriggerFail", jobRow: types.JobEventFail, want: true},
		{name: "NoTriggerTimeout", jobRow: types.JobEventTimeout, want: true},
		{name: "NoTriggerValidationExhausted", jobRow: types.JobEventValidationExhausted, want: true},
		{name: "NoTriggerInfraTriggerFailure", jobRow: types.JobEventInfraTriggerFailure, want: false},
		{name: "NoTriggerRerunAccepted", jobRow: types.JobEventRerunAccepted, want: false},
		{name: "RunningWithEvaluationReleased", trigger: types.TriggerStatusRunning, jobRow: types.JobEventEvaluationReleased, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake, mem := storetest.NewMem()
			for i, event := range []string{tt.jobRow, tt.newer} {
				if event == "" {
					continue
				}
				mem.Put("joblog", map[string]ddbtypes.AttributeValue{
					"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("p")},
					"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("s", date, fmt.Sprintf("%d", 1772366400000+i))},
					"event": &ddbtypes.AttributeValueMemberS{Value: event},
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

// TestEvaluationReleased: only a newest outcome row that is evaluation-released
// means the date's evaluation was released; a rerun rejected after it does not
// hide it, and a failed lookup is not a release.
func TestEvaluationReleased(t *testing.T) {
	const date = "2026-03-01"
	tests := []struct {
		name    string
		jobRows []string // oldest first
		failQ   bool
		want    bool
	}{
		{name: "NoRows", want: false},
		{name: "LatestReleased", jobRows: []string{types.JobEventInfraTriggerExhausted, types.JobEventEvaluationReleased}, want: true},
		{name: "ReleasedThenRerunRejected", jobRows: []string{types.JobEventEvaluationReleased, types.JobEventRerunRejected}, want: true},
		{name: "ReleasedThenSuccess", jobRows: []string{types.JobEventEvaluationReleased, types.JobEventSuccess}, want: false},
		{name: "LookupFails", jobRows: []string{types.JobEventEvaluationReleased}, failQ: true, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake, mem := storetest.NewMem()
			for i, event := range tt.jobRows {
				mem.Put("joblog", map[string]ddbtypes.AttributeValue{
					"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("p")},
					"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("s", date, fmt.Sprintf("%d", 1772366400000+i))},
					"event": &ddbtypes.AttributeValueMemberS{Value: event},
				})
			}
			if tt.failQ {
				fake.QueryFn = func(context.Context, *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
					return nil, errors.New("dynamodb: throttled")
				}
			}
			d := &lambda.Deps{Store: storetest.NewStore(fake), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}

			if got := lambda.EvaluationReleased(context.Background(), d, "p", "s", date); got != tt.want {
				t.Fatalf("EvaluationReleased = %v, want %v", got, tt.want)
			}
		})
	}
}

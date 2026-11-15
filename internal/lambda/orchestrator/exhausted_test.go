package orchestrator_test

import (
	"context"
	"testing"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/lambda/orchestrator"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestJobPollExhausted_TimeoutRowCarriesRunID: the closed poll window is a
// timeout of this run, which CompleteTrigger may retry; the stream-router
// publishes JOB_FAILED from the row, so this handler publishes nothing.
func TestJobPollExhausted_TimeoutRowCarriesRunID(t *testing.T) {
	d, mem, rec := memDeps(t, failCfg(1))
	if _, err := orchestrator.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode: "job-poll-exhausted", PipelineID: "p", ScheduleID: "s", Date: ctDate, RunID: "run-1",
	}); err != nil {
		t.Fatalf("job-poll-exhausted: %v", err)
	}
	rows := mem.Items("joblog")
	if len(rows) != 1 {
		t.Fatalf("job log rows = %v, want one", rows)
	}
	if got := [3]string{strAttr(t, rows[0], "event"), strAttr(t, rows[0], "runId"), strAttr(t, rows[0], "error")}; got != [3]string{types.JobEventTimeout, "run-1", "job poll window exhausted"} {
		t.Errorf("row event/runId/error = %q", got)
	}
	if len(rec.events) != 0 {
		t.Errorf("events = %+v, want none", rec.events)
	}
}

package orchestrator_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/lambda/orchestrator"
	"github.com/dwsmith1983/interlock/internal/trigger"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestHandleTrigger_MarksStarted: the trigger row is marked started at the
// clock's time before Execute runs (a lambda job runs inside it), whatever
// Execute returns. A missing row is a warning, and the trigger still returns.
func TestHandleTrigger_MarksStarted(t *testing.T) {
	httpJob := types.JobConfig{Type: types.TriggerHTTP, Config: map[string]interface{}{"url": "https://example.com/run"}}
	lambdaJob := types.JobConfig{Type: types.TriggerLambda, Config: map[string]interface{}{"functionName": "f"}}
	const started = "2026-03-01T12:00:00Z" // memDeps' clock
	tests := []struct {
		name           string
		job            types.JobConfig
		execErr        error
		noRow, wantErr bool
	}{
		{name: "http success", job: httpJob},
		{name: "lambda success", job: lambdaJob},
		{name: "lambda job failure", job: lambdaJob, execErr: &trigger.JobFailedError{Message: "boom"}},
		{name: "infra error still marks the launch", job: httpJob, execErr: errors.New("connection refused"), wantErr: true},
		{name: "missing row", job: httpJob, noRow: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			d, _, _ := memDeps(t, types.PipelineConfig{Pipeline: types.PipelineIdentity{ID: "p"}, Job: tt.job})
			var logs bytes.Buffer
			d.Logger = slog.New(slog.NewTextHandler(&logs, nil))
			var seen string // startedAt as Execute sees it
			d.TriggerRunner = &fakeExecutor{err: tt.execErr, hook: func() {
				if rec, err := d.Store.GetTrigger(ctx, "p", "s", ctDate); err == nil && rec != nil {
					seen = rec.StartedAt
				}
			}}
			if !tt.noRow {
				if ok, err := d.Store.AcquireTriggerLock(ctx, "p", "s", ctDate, time.Hour); !ok || err != nil {
					t.Fatalf("AcquireTriggerLock = %v, %v", ok, err)
				}
			}

			out, err := orchestrator.HandleOrchestrator(ctx, d, lambda.OrchestratorInput{
				Mode: "trigger", PipelineID: "p", ScheduleID: "s", Date: ctDate, ExecutionName: ctExec,
			})
			if (err != nil) != tt.wantErr || (!tt.wantErr && (out.Mode != "trigger" || out.RunID != ctExec)) {
				t.Fatalf("HandleOrchestrator = %+v, %v; want an error %v, else the output for run %s", out, err, tt.wantErr, ctExec)
			}

			rec, err := d.Store.GetTrigger(ctx, "p", "s", ctDate)
			if err != nil {
				t.Fatalf("GetTrigger: %v", err)
			}
			if !tt.noRow && (seen != started || rec == nil || rec.StartedAt != started) {
				t.Errorf("startedAt at Execute %q, trigger row %+v; want startedAt %q", seen, rec, started)
			}
			wantLog := "pipeline=p schedule=s date=" + ctDate + " runId=" + ctExec
			if log := logs.String(); tt.noRow && (rec != nil || !strings.Contains(log, "level=WARN") || !strings.Contains(log, wantLog)) {
				t.Errorf("trigger row = %+v, log = %q; want no row and a warning with %q", rec, log, wantLog)
			}
		})
	}
}

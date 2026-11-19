package sla_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/lambda/sla"
)

// TestHandleSLAMonitor_UnknownModes pins that the SLA monitor accepts only
// fire-alert and cancel: the watchdog creates the SLA schedules itself.
func TestHandleSLAMonitor_UnknownModes(t *testing.T) {
	d := &lambda.Deps{
		Logger:  slog.Default(),
		NowFunc: func() time.Time { return time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC) },
	}
	for _, mode := range []string{"explode", "schedule", "reconcile", "calculate"} {
		t.Run(mode, func(t *testing.T) {
			out, err := sla.HandleSLAMonitor(context.Background(), d, lambda.SLAMonitorInput{Mode: mode, PipelineID: "p"})

			want := `unknown SLA monitor mode: "` + mode + `"`
			if err == nil || err.Error() != want {
				t.Fatalf("error = %v, want %q", err, want)
			}
			if out != (lambda.SLAMonitorOutput{}) {
				t.Errorf("output = %+v, want zero", out)
			}
		})
	}
}

package testutil

import (
	"testing"
	"time"

	"github.com/interlock-systems/interlock/pkg/types"
)

// WaitFor polls check every 10ms until it returns true or timeout is reached.
func WaitFor(t *testing.T, timeout time.Duration, check func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for condition: %s", msg)
}

// WaitForRun polls until a run exists for the pipeline, returning the first run.
func WaitForRun(t *testing.T, prov *MockProvider, pipelineID string, timeout time.Duration) types.RunState {
	t.Helper()
	var run types.RunState
	WaitFor(t, timeout, func() bool {
		runs, err := prov.ListRuns(nil, pipelineID, 1)
		if err != nil || len(runs) == 0 {
			return false
		}
		run = runs[0]
		return true
	}, "run created for "+pipelineID)
	return run
}

// WaitForRunStatus polls until a run with the given status exists for the pipeline.
func WaitForRunStatus(t *testing.T, prov *MockProvider, pipelineID string, status types.RunStatus, timeout time.Duration) types.RunState {
	t.Helper()
	var run types.RunState
	WaitFor(t, timeout, func() bool {
		runs, err := prov.ListRuns(nil, pipelineID, 1)
		if err != nil || len(runs) == 0 {
			return false
		}
		run = runs[0]
		return run.Status == status
	}, "run with status "+string(status)+" for "+pipelineID)
	return run
}

// WaitForEvent polls until an event of the given kind exists for the pipeline.
func WaitForEvent(t *testing.T, prov *MockProvider, pipelineID string, kind types.EventKind, timeout time.Duration) {
	t.Helper()
	WaitFor(t, timeout, func() bool {
		events := prov.Events()
		for _, e := range events {
			if e.PipelineID == pipelineID && e.Kind == kind {
				return true
			}
		}
		return false
	}, "event "+string(kind)+" for "+pipelineID)
}

// WaitForPollCount polls until the provider's ListPipelines has been called
// at least n times, indicating the watcher has completed that many poll cycles.
func WaitForPollCount(t *testing.T, prov *MockProvider, n int64, timeout time.Duration) {
	t.Helper()
	WaitFor(t, timeout, func() bool {
		return prov.PollCount() >= n
	}, "watcher poll count >= target")
}

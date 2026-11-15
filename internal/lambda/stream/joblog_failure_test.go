package stream

import (
	"testing"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestStreamJobFail_NoRestart: CompleteTrigger owns retries, so a fail or
// timeout row starts nothing and writes nothing.
func TestStreamJobFail_NoRestart(t *testing.T) {
	runJobFailCases(t, []jobFailCase{
		{name: "fail", event: types.JobEventFail, maxRetries: 2},
		{name: "timeout", event: types.JobEventTimeout, maxRetries: 2},
	})
}

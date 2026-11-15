package deploy_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/lambda/orchestrator"
)

// TestASL_IsRetryingMatchesCompleteTrigger ties the IsRetrying literal to the
// Go constant: the retrying output must resolve the Choice's Variable and
// match its StringEquals, or a retried execution would also cancel the SLA.
func TestASL_IsRetryingMatchesCompleteTrigger(t *testing.T) {
	var choice struct {
		Choices []struct{ Variable, StringEquals, Next string }
	}
	require.NoError(t, json.Unmarshal(loadASL(t).States["IsRetrying"], &choice))
	require.Len(t, choice.Choices, 1)
	rule := choice.Choices[0]

	out := lambda.OrchestratorOutput{Mode: "complete-trigger", Status: orchestrator.StatusRetrying, NextExecution: "x-r1"}
	doc := map[string]interface{}{"completeTriggerResult": toDoc(t, out)}
	require.True(t, resolvePath(doc, rule.Variable), "%s must resolve against the retrying output", rule.Variable)
	require.Equal(t, orchestrator.StatusRetrying, rule.StringEquals)
	require.Equal(t, "Done", rule.Next)
}

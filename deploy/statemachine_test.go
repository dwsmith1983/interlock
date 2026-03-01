package deploy_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- ASL structural types ---

type aslDefinition struct {
	Comment string                     `json:"Comment"`
	StartAt string                     `json:"StartAt"`
	States  map[string]json.RawMessage `json:"States"`
}

type stateBase struct {
	Type    string `json:"Type"`
	Next    string `json:"Next,omitempty"`
	End     bool   `json:"End,omitempty"`
	Default string `json:"Default,omitempty"`
}

type choiceState struct {
	Type    string `json:"Type"`
	Choices []struct {
		Next string `json:"Next"`
	} `json:"Choices"`
	Default string `json:"Default,omitempty"`
}

type catchEntry struct {
	Next string `json:"Next"`
}

type retryEntry struct {
	ErrorEquals []string `json:"ErrorEquals"`
	MaxAttempts int      `json:"MaxAttempts"`
}

type taskState struct {
	Type  string       `json:"Type"`
	Next  string       `json:"Next,omitempty"`
	End   bool         `json:"End,omitempty"`
	Retry []retryEntry `json:"Retry,omitempty"`
	Catch []catchEntry `json:"Catch,omitempty"`
}

type branch struct {
	StartAt string                     `json:"StartAt"`
	States  map[string]json.RawMessage `json:"States"`
}

type parallelState struct {
	Type     string       `json:"Type"`
	Branches []branch     `json:"Branches"`
	Next     string       `json:"Next,omitempty"`
	Catch    []catchEntry `json:"Catch,omitempty"`
}

// --- helpers ---

func loadASL(t *testing.T) aslDefinition {
	t.Helper()
	data, err := os.ReadFile("statemachine.asl.json")
	require.NoError(t, err, "reading ASL file")

	var asl aslDefinition
	require.NoError(t, json.Unmarshal(data, &asl), "parsing ASL JSON")
	return asl
}

func loadParallel(t *testing.T) parallelState {
	t.Helper()
	asl := loadASL(t)
	raw, ok := asl.States["Parallel"]
	require.True(t, ok, "Parallel state must exist")

	var ps parallelState
	require.NoError(t, json.Unmarshal(raw, &ps))
	require.Equal(t, "Parallel", ps.Type)
	return ps
}

// allBranchTransitionTargets collects every state name referenced as Next,
// Default, or Catch target within a single branch.
func allBranchTransitionTargets(t *testing.T, states map[string]json.RawMessage) map[string]bool {
	t.Helper()
	targets := make(map[string]bool)

	for _, raw := range states {
		var ts taskState
		if err := json.Unmarshal(raw, &ts); err == nil {
			if ts.Next != "" {
				targets[ts.Next] = true
			}
			for _, c := range ts.Catch {
				if c.Next != "" {
					targets[c.Next] = true
				}
			}
		}

		var cs choiceState
		if err := json.Unmarshal(raw, &cs); err == nil && cs.Type == "Choice" {
			if cs.Default != "" {
				targets[cs.Default] = true
			}
			for _, choice := range cs.Choices {
				if choice.Next != "" {
					targets[choice.Next] = true
				}
			}
		}
	}

	return targets
}

// --- tests ---

func TestASL_ValidJSON(t *testing.T) {
	data, err := os.ReadFile("statemachine.asl.json")
	require.NoError(t, err)

	var raw map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &raw), "ASL file must be valid JSON")
}

func TestASL_HasRequiredFields(t *testing.T) {
	asl := loadASL(t)
	assert.NotEmpty(t, asl.Comment)
	assert.NotEmpty(t, asl.StartAt)
	assert.NotEmpty(t, asl.States)
}

func TestASL_StartAtExists(t *testing.T) {
	asl := loadASL(t)
	_, ok := asl.States[asl.StartAt]
	assert.True(t, ok, "StartAt %q must reference an existing state", asl.StartAt)
}

func TestASL_TopLevelStatesExist(t *testing.T) {
	asl := loadASL(t)
	for _, name := range []string{"Parallel", "Reconcile", "InfraFailure", "End"} {
		_, ok := asl.States[name]
		assert.True(t, ok, "expected top-level state %q not found", name)
	}
}

func TestASL_ParallelHasTwoBranches(t *testing.T) {
	ps := loadParallel(t)
	assert.Len(t, ps.Branches, 2, "Parallel state must have exactly 2 branches")
}

func TestASL_ParallelHasCatch(t *testing.T) {
	ps := loadParallel(t)
	require.NotEmpty(t, ps.Catch, "Parallel state must have a Catch block")
	assert.Equal(t, "InfraFailure", ps.Catch[0].Next)
}

func TestASL_EvalBranchStatesExist(t *testing.T) {
	ps := loadParallel(t)
	require.Len(t, ps.Branches, 2)
	evalBranch := ps.Branches[0]

	expected := []string{
		"InitEvalLoop",
		"Evaluate",
		"IsReady",
		"WaitInterval",
		"IncrementElapsed",
		"CheckWindowExhausted",
		"ValidationExhausted",
		"Trigger",
		"WaitForJob",
		"CheckJob",
		"IsJobDone",
		"EvalDone",
		"EvalBranchFailed",
	}
	for _, name := range expected {
		_, ok := evalBranch.States[name]
		assert.True(t, ok, "eval branch: expected state %q not found", name)
	}
}

func TestASL_SLABranchStatesExist(t *testing.T) {
	ps := loadParallel(t)
	require.Len(t, ps.Branches, 2)
	slaBranch := ps.Branches[1]

	expected := []string{
		"CheckSLAConfig",
		"SLASkipped",
		"CalcDeadlines",
		"WaitForWarning",
		"FireSLAWarning",
		"WaitForBreach",
		"FireSLABreach",
		"SLADone",
		"SLABranchFailed",
	}
	for _, name := range expected {
		_, ok := slaBranch.States[name]
		assert.True(t, ok, "SLA branch: expected state %q not found", name)
	}
}

func TestASL_EvalBranchStartAt(t *testing.T) {
	ps := loadParallel(t)
	evalBranch := ps.Branches[0]
	assert.Equal(t, "InitEvalLoop", evalBranch.StartAt)
	_, ok := evalBranch.States[evalBranch.StartAt]
	assert.True(t, ok, "eval branch StartAt %q must exist", evalBranch.StartAt)
}

func TestASL_SLABranchStartAt(t *testing.T) {
	ps := loadParallel(t)
	slaBranch := ps.Branches[1]
	assert.Equal(t, "CheckSLAConfig", slaBranch.StartAt)
	_, ok := slaBranch.States[slaBranch.StartAt]
	assert.True(t, ok, "SLA branch StartAt %q must exist", slaBranch.StartAt)
}

func TestASL_AllStatesHaveValidType(t *testing.T) {
	validTypes := map[string]bool{
		"Task":     true,
		"Choice":   true,
		"Wait":     true,
		"Pass":     true,
		"Succeed":  true,
		"Fail":     true,
		"Map":      true,
		"Parallel": true,
	}

	asl := loadASL(t)
	// Top-level states
	for name, raw := range asl.States {
		var base stateBase
		require.NoError(t, json.Unmarshal(raw, &base), "parsing state %q", name)
		assert.True(t, validTypes[base.Type], "top-level state %q has invalid type %q", name, base.Type)
	}

	// Branch states
	ps := loadParallel(t)
	for bi, br := range ps.Branches {
		for name, raw := range br.States {
			var base stateBase
			require.NoError(t, json.Unmarshal(raw, &base), "parsing branch %d state %q", bi, name)
			assert.True(t, validTypes[base.Type], "branch %d state %q has invalid type %q", bi, name, base.Type)
		}
	}
}

func TestASL_TerminalStatesHaveNoNext(t *testing.T) {
	asl := loadASL(t)
	for name, raw := range asl.States {
		var base stateBase
		require.NoError(t, json.Unmarshal(raw, &base))
		if base.Type == "Succeed" || base.Type == "Fail" {
			assert.Empty(t, base.Next, "terminal state %q should not have Next", name)
		}
	}

	ps := loadParallel(t)
	for _, br := range ps.Branches {
		for name, raw := range br.States {
			var base stateBase
			require.NoError(t, json.Unmarshal(raw, &base))
			if base.Type == "Succeed" || base.Type == "Fail" {
				assert.Empty(t, base.Next, "terminal branch state %q should not have Next", name)
			}
		}
	}
}

func TestASL_ChoiceStatesHaveDefault(t *testing.T) {
	ps := loadParallel(t)
	for _, br := range ps.Branches {
		for name, raw := range br.States {
			var cs choiceState
			if err := json.Unmarshal(raw, &cs); err == nil && cs.Type == "Choice" {
				assert.NotEmpty(t, cs.Default, "Choice state %q should have a Default", name)
				assert.NotEmpty(t, cs.Choices, "Choice state %q should have at least one choice", name)
			}
		}
	}
}

func TestASL_AllTaskStatesHaveRetry(t *testing.T) {
	ps := loadParallel(t)
	for bi, br := range ps.Branches {
		for name, raw := range br.States {
			var ts taskState
			if err := json.Unmarshal(raw, &ts); err != nil || ts.Type != "Task" {
				continue
			}
			assert.NotEmpty(t, ts.Retry, "branch %d Task state %q must have Retry", bi, name)

			// Verify the retry contains the required Lambda error types.
			require.NotEmpty(t, ts.Retry)
			errs := ts.Retry[0].ErrorEquals
			assert.Contains(t, errs, "Lambda.ServiceException")
			assert.Contains(t, errs, "Lambda.AWSLambdaException")
			assert.Contains(t, errs, "Lambda.TooManyRequestsException")
			assert.Contains(t, errs, "States.TaskFailed")
		}
	}
}

func TestASL_AllTaskStatesHaveCatch(t *testing.T) {
	ps := loadParallel(t)
	for bi, br := range ps.Branches {
		for name, raw := range br.States {
			var ts taskState
			if err := json.Unmarshal(raw, &ts); err != nil || ts.Type != "Task" {
				continue
			}
			assert.NotEmpty(t, ts.Catch, "branch %d Task state %q must have Catch", bi, name)
		}
	}
}

func TestASL_BranchTransitionsReferenceExistingStates(t *testing.T) {
	ps := loadParallel(t)
	for bi, br := range ps.Branches {
		targets := allBranchTransitionTargets(t, br.States)
		for target := range targets {
			_, ok := br.States[target]
			assert.True(t, ok, "branch %d: transition target %q does not exist", bi, target)
		}
	}
}

func TestASL_NoBranchOrphanStates(t *testing.T) {
	ps := loadParallel(t)
	for bi, br := range ps.Branches {
		targets := allBranchTransitionTargets(t, br.States)
		targets[br.StartAt] = true

		for name := range br.States {
			assert.True(t, targets[name], "branch %d state %q is never referenced (orphan)", bi, name)
		}
	}
}

func TestASL_TopLevelTransitions(t *testing.T) {
	asl := loadASL(t)

	// Parallel -> Reconcile
	var ps stateBase
	require.NoError(t, json.Unmarshal(asl.States["Parallel"], &ps))
	assert.Equal(t, "Reconcile", ps.Next)

	// Reconcile -> End
	var rec stateBase
	require.NoError(t, json.Unmarshal(asl.States["Reconcile"], &rec))
	assert.Equal(t, "End", rec.Next)

	// End is Succeed
	var end stateBase
	require.NoError(t, json.Unmarshal(asl.States["End"], &end))
	assert.Equal(t, "Succeed", end.Type)

	// InfraFailure is Fail
	var inf stateBase
	require.NoError(t, json.Unmarshal(asl.States["InfraFailure"], &inf))
	assert.Equal(t, "Fail", inf.Type)
}

func TestASL_EvalBranchFailStateExists(t *testing.T) {
	ps := loadParallel(t)
	evalBranch := ps.Branches[0]

	raw, ok := evalBranch.States["EvalBranchFailed"]
	require.True(t, ok)

	var base stateBase
	require.NoError(t, json.Unmarshal(raw, &base))
	assert.Equal(t, "Fail", base.Type)
}

func TestASL_SLABranchFailStateExists(t *testing.T) {
	ps := loadParallel(t)
	slaBranch := ps.Branches[1]

	raw, ok := slaBranch.States["SLABranchFailed"]
	require.True(t, ok)

	var base stateBase
	require.NoError(t, json.Unmarshal(raw, &base))
	assert.Equal(t, "Fail", base.Type)
}

func TestASL_SLABranchSkipsWhenNoConfig(t *testing.T) {
	ps := loadParallel(t)
	slaBranch := ps.Branches[1]

	raw := slaBranch.States["CheckSLAConfig"]
	var cs choiceState
	require.NoError(t, json.Unmarshal(raw, &cs))
	assert.Equal(t, "SLASkipped", cs.Default, "SLA branch should skip to SLASkipped when no SLA config")
}

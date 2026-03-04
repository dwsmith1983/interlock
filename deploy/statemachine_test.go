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

// --- helpers ---

func loadASL(t *testing.T) aslDefinition {
	t.Helper()
	data, err := os.ReadFile("statemachine.asl.json")
	require.NoError(t, err, "reading ASL file")

	var asl aslDefinition
	require.NoError(t, json.Unmarshal(data, &asl), "parsing ASL JSON")
	return asl
}

// allTransitionTargets collects every state name referenced as Next,
// Default, or Catch target within the top-level states.
func allTransitionTargets(t *testing.T, states map[string]json.RawMessage) map[string]bool {
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
	expected := []string{
		"InitEvalLoop",
		"Evaluate",
		"IsReady",
		"WaitInterval",
		"IncrementElapsed",
		"CheckWindowExhausted",
		"ValidationExhausted",
		"Trigger",
		"CheckSLAConfig",
		"ScheduleSLAAlerts",
		"HasTriggerResult",
		"WaitForJob",
		"CheckJob",
		"IsJobDone",
		"CheckCancelSLA",
		"TriggerRetryExhausted",
		"CancelSLASchedules",
		"InfraFailure",
		"Done",
	}
	for _, name := range expected {
		_, ok := asl.States[name]
		assert.True(t, ok, "expected state %q not found", name)
	}
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
	for name, raw := range asl.States {
		var base stateBase
		require.NoError(t, json.Unmarshal(raw, &base), "parsing state %q", name)
		assert.True(t, validTypes[base.Type], "state %q has invalid type %q", name, base.Type)
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
}

func TestASL_ChoiceStatesHaveDefault(t *testing.T) {
	asl := loadASL(t)
	for name, raw := range asl.States {
		var cs choiceState
		if err := json.Unmarshal(raw, &cs); err == nil && cs.Type == "Choice" {
			assert.NotEmpty(t, cs.Default, "Choice state %q should have a Default", name)
			assert.NotEmpty(t, cs.Choices, "Choice state %q should have at least one choice", name)
		}
	}
}

func TestASL_AllTaskStatesHaveRetry(t *testing.T) {
	asl := loadASL(t)
	for name, raw := range asl.States {
		var ts taskState
		if err := json.Unmarshal(raw, &ts); err != nil || ts.Type != "Task" {
			continue
		}
		assert.NotEmpty(t, ts.Retry, "Task state %q must have Retry", name)

		require.NotEmpty(t, ts.Retry)
		errs := ts.Retry[0].ErrorEquals
		assert.Contains(t, errs, "Lambda.ServiceException")
		assert.Contains(t, errs, "Lambda.AWSLambdaException")
		assert.Contains(t, errs, "Lambda.TooManyRequestsException")
		assert.Contains(t, errs, "States.TaskFailed")
	}
}

func TestASL_AllTaskStatesHaveCatch(t *testing.T) {
	asl := loadASL(t)
	for name, raw := range asl.States {
		var ts taskState
		if err := json.Unmarshal(raw, &ts); err != nil || ts.Type != "Task" {
			continue
		}
		assert.NotEmpty(t, ts.Catch, "Task state %q must have Catch", name)
	}
}

func TestASL_TransitionsReferenceExistingStates(t *testing.T) {
	asl := loadASL(t)
	targets := allTransitionTargets(t, asl.States)
	for target := range targets {
		_, ok := asl.States[target]
		assert.True(t, ok, "transition target %q does not exist", target)
	}
}

func TestASL_NoOrphanStates(t *testing.T) {
	asl := loadASL(t)
	targets := allTransitionTargets(t, asl.States)
	targets[asl.StartAt] = true

	for name := range asl.States {
		assert.True(t, targets[name], "state %q is never referenced (orphan)", name)
	}
}

func TestASL_EvalLoopFlow(t *testing.T) {
	asl := loadASL(t)

	// InitEvalLoop → Evaluate
	var init stateBase
	require.NoError(t, json.Unmarshal(asl.States["InitEvalLoop"], &init))
	assert.Equal(t, "Evaluate", init.Next)

	// Evaluate → IsReady
	var eval taskState
	require.NoError(t, json.Unmarshal(asl.States["Evaluate"], &eval))
	assert.Equal(t, "IsReady", eval.Next)

	// IsReady: passed → Trigger, default → WaitInterval
	var ready choiceState
	require.NoError(t, json.Unmarshal(asl.States["IsReady"], &ready))
	assert.Equal(t, "WaitInterval", ready.Default)
	require.NotEmpty(t, ready.Choices)
	assert.Equal(t, "Trigger", ready.Choices[0].Next)
}

func TestASL_TriggerToSLAFlow(t *testing.T) {
	asl := loadASL(t)

	// Trigger → CheckSLAConfig
	var trigger taskState
	require.NoError(t, json.Unmarshal(asl.States["Trigger"], &trigger))
	assert.Equal(t, "CheckSLAConfig", trigger.Next)

	// CheckSLAConfig: SLA → ScheduleSLAAlerts, default → HasTriggerResult
	var slaCheck choiceState
	require.NoError(t, json.Unmarshal(asl.States["CheckSLAConfig"], &slaCheck))
	assert.Equal(t, "HasTriggerResult", slaCheck.Default)
	require.NotEmpty(t, slaCheck.Choices)
	assert.Equal(t, "ScheduleSLAAlerts", slaCheck.Choices[0].Next)

	// ScheduleSLAAlerts → HasTriggerResult
	var sched taskState
	require.NoError(t, json.Unmarshal(asl.States["ScheduleSLAAlerts"], &sched))
	assert.Equal(t, "HasTriggerResult", sched.Next)
}

func TestASL_HasTriggerResultRouting(t *testing.T) {
	asl := loadASL(t)

	var hasTrigger choiceState
	require.NoError(t, json.Unmarshal(asl.States["HasTriggerResult"], &hasTrigger))
	assert.Equal(t, "Done", hasTrigger.Default, "no trigger result should go to Done")
	require.NotEmpty(t, hasTrigger.Choices)
	assert.Equal(t, "WaitForJob", hasTrigger.Choices[0].Next, "with trigger result should poll job")
}

func TestASL_JobPollingFlow(t *testing.T) {
	asl := loadASL(t)

	// WaitForJob → CheckJob
	var wait stateBase
	require.NoError(t, json.Unmarshal(asl.States["WaitForJob"], &wait))
	assert.Equal(t, "CheckJob", wait.Next)

	// CheckJob → IsJobDone
	var check taskState
	require.NoError(t, json.Unmarshal(asl.States["CheckJob"], &check))
	assert.Equal(t, "IsJobDone", check.Next)

	// IsJobDone: terminal events → CheckCancelSLA, default → WaitForJob
	var done choiceState
	require.NoError(t, json.Unmarshal(asl.States["IsJobDone"], &done))
	assert.Equal(t, "WaitForJob", done.Default)
	// Check that terminal events route to CheckCancelSLA
	foundCancel := false
	for _, c := range done.Choices {
		if c.Next == "CheckCancelSLA" {
			foundCancel = true
		}
	}
	assert.True(t, foundCancel, "IsJobDone should route terminal events to CheckCancelSLA")
}

func TestASL_CancelSLAFlow(t *testing.T) {
	asl := loadASL(t)

	// CheckCancelSLA: SLA → CancelSLASchedules, default → Done
	var cancelCheck choiceState
	require.NoError(t, json.Unmarshal(asl.States["CheckCancelSLA"], &cancelCheck))
	assert.Equal(t, "Done", cancelCheck.Default)
	require.NotEmpty(t, cancelCheck.Choices)
	assert.Equal(t, "CancelSLASchedules", cancelCheck.Choices[0].Next)

	// CancelSLASchedules → Done
	var cancel taskState
	require.NoError(t, json.Unmarshal(asl.States["CancelSLASchedules"], &cancel))
	assert.Equal(t, "Done", cancel.Next)
}

func TestASL_ValidationExhaustedToSLA(t *testing.T) {
	asl := loadASL(t)

	// ValidationExhausted → CheckSLAConfig
	var exhausted taskState
	require.NoError(t, json.Unmarshal(asl.States["ValidationExhausted"], &exhausted))
	assert.Equal(t, "CheckSLAConfig", exhausted.Next)
}

func TestASL_TerminalStates(t *testing.T) {
	asl := loadASL(t)

	// Done is Succeed
	var done stateBase
	require.NoError(t, json.Unmarshal(asl.States["Done"], &done))
	assert.Equal(t, "Succeed", done.Type)

	// InfraFailure is Fail
	var inf stateBase
	require.NoError(t, json.Unmarshal(asl.States["InfraFailure"], &inf))
	assert.Equal(t, "Fail", inf.Type)
}

func TestASL_ScheduleSLACatchDoesNotBlock(t *testing.T) {
	asl := loadASL(t)

	// ScheduleSLAAlerts Catch should route to HasTriggerResult (not InfraFailure)
	// SLA scheduling failure should not kill the pipeline
	var sched taskState
	require.NoError(t, json.Unmarshal(asl.States["ScheduleSLAAlerts"], &sched))
	require.NotEmpty(t, sched.Catch)
	assert.Equal(t, "HasTriggerResult", sched.Catch[0].Next,
		"ScheduleSLAAlerts catch should continue to HasTriggerResult, not block pipeline")
}

func TestASL_TriggerRetryExhaustedFlow(t *testing.T) {
	asl := loadASL(t)

	// Trigger Catch routes to TriggerRetryExhausted (not CheckCancelSLA directly).
	var trigger taskState
	require.NoError(t, json.Unmarshal(asl.States["Trigger"], &trigger))
	require.NotEmpty(t, trigger.Catch)
	assert.Equal(t, "TriggerRetryExhausted", trigger.Catch[0].Next,
		"Trigger catch should route to TriggerRetryExhausted")

	// TriggerRetryExhausted → CheckCancelSLA.
	var exhausted taskState
	require.NoError(t, json.Unmarshal(asl.States["TriggerRetryExhausted"], &exhausted))
	assert.Equal(t, "CheckCancelSLA", exhausted.Next,
		"TriggerRetryExhausted should proceed to CheckCancelSLA")

	// TriggerRetryExhausted Catch falls through to CheckCancelSLA (best-effort).
	require.NotEmpty(t, exhausted.Catch)
	assert.Equal(t, "CheckCancelSLA", exhausted.Catch[0].Next,
		"TriggerRetryExhausted catch should fall through to CheckCancelSLA")
}

func TestASL_CancelSLACatchDoesNotBlock(t *testing.T) {
	asl := loadASL(t)

	// CancelSLASchedules Catch should route to Done (not InfraFailure)
	var cancel taskState
	require.NoError(t, json.Unmarshal(asl.States["CancelSLASchedules"], &cancel))
	require.NotEmpty(t, cancel.Catch)
	assert.Equal(t, "Done", cancel.Catch[0].Next,
		"CancelSLASchedules catch should continue to Done, not block pipeline")
}

package deploy_test

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- ASL structural types ---

type aslDefinition struct {
	Comment        string                     `json:"Comment"`
	StartAt        string                     `json:"StartAt"`
	TimeoutSeconds int                        `json:"TimeoutSeconds"`
	States         map[string]json.RawMessage `json:"States"`
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

// renderASLTemplate replaces Terraform template variables with default values
// so the raw ASL template can be parsed as valid JSON.
// NOTE: numeric defaults must stay in sync with deploy/terraform/variables.tf.
func renderASLTemplate(data []byte) []byte {
	s := string(data)
	s = strings.ReplaceAll(s, "${sfn_timeout_seconds}", "14400")
	s = strings.ReplaceAll(s, "${trigger_max_attempts}", "3")
	s = strings.ReplaceAll(s, "${orchestrator_arn}", "arn:aws:lambda:us-east-1:123456789012:function:orchestrator")
	s = strings.ReplaceAll(s, "${sla_monitor_arn}", "arn:aws:lambda:us-east-1:123456789012:function:sla-monitor")
	return []byte(s)
}

func loadASL(t *testing.T) aslDefinition {
	t.Helper()
	data, err := os.ReadFile("statemachine.asl.json")
	require.NoError(t, err, "reading ASL file")

	rendered := renderASLTemplate(data)
	var asl aslDefinition
	require.NoError(t, json.Unmarshal(rendered, &asl), "parsing ASL JSON")
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

	rendered := renderASLTemplate(data)
	var raw map[string]interface{}
	require.NoError(t, json.Unmarshal(rendered, &raw), "ASL template must produce valid JSON")
}

func TestASL_HasGlobalTimeout(t *testing.T) {
	asl := loadASL(t)
	assert.Greater(t, asl.TimeoutSeconds, 0, "ASL must have a positive global TimeoutSeconds")
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
		"HasTriggerResult",
		"InitJobPollLoop",
		"WaitForJob",
		"CheckJob",
		"IsJobDone",
		"IncrementJobPollElapsed",
		"CheckJobPollExhausted",
		"JobPollExhausted",
		"InjectTimeoutEvent",
		"CheckHasPostRun",
		"InitPostRunLoop",
		"PostRunEvaluate",
		"IsPostRunDone",
		"WaitForPostRun",
		"IncrementPostRunElapsed",
		"CompleteTrigger",
		"CheckCancelSLA",
		"TriggerRetryExhausted",
		"FailValidationExhausted",
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

func TestASL_TriggerToHasTriggerResult(t *testing.T) {
	asl := loadASL(t)

	// Trigger → HasTriggerResult (proactive SLA: scheduling moved to watchdog)
	var trigger taskState
	require.NoError(t, json.Unmarshal(asl.States["Trigger"], &trigger))
	assert.Equal(t, "HasTriggerResult", trigger.Next)
}

func TestASL_HasTriggerResultRouting(t *testing.T) {
	asl := loadASL(t)

	var hasTrigger choiceState
	require.NoError(t, json.Unmarshal(asl.States["HasTriggerResult"], &hasTrigger))
	assert.Equal(t, "FailValidationExhausted", hasTrigger.Default, "no trigger result should fail as validation exhausted")
	require.NotEmpty(t, hasTrigger.Choices)
	assert.Equal(t, "InitJobPollLoop", hasTrigger.Choices[0].Next, "with trigger result should init job poll loop")
}

func TestASL_JobPollingFlow(t *testing.T) {
	asl := loadASL(t)

	// InitJobPollLoop → WaitForJob
	var initLoop stateBase
	require.NoError(t, json.Unmarshal(asl.States["InitJobPollLoop"], &initLoop))
	assert.Equal(t, "WaitForJob", initLoop.Next)

	// WaitForJob → CheckJob
	var wait stateBase
	require.NoError(t, json.Unmarshal(asl.States["WaitForJob"], &wait))
	assert.Equal(t, "CheckJob", wait.Next)

	// CheckJob → IsJobDone
	var check taskState
	require.NoError(t, json.Unmarshal(asl.States["CheckJob"], &check))
	assert.Equal(t, "IsJobDone", check.Next)

	// IsJobDone: success → CheckHasPostRun, fail/timeout → CompleteTrigger, default → IncrementJobPollElapsed
	var done choiceState
	require.NoError(t, json.Unmarshal(asl.States["IsJobDone"], &done))
	assert.Equal(t, "IncrementJobPollElapsed", done.Default)
	// Check that success routes to CheckHasPostRun
	foundPostRunRoute := false
	foundComplete := false
	for _, c := range done.Choices {
		if c.Next == "CheckHasPostRun" {
			foundPostRunRoute = true
		}
		if c.Next == "CompleteTrigger" {
			foundComplete = true
		}
	}
	assert.True(t, foundPostRunRoute, "IsJobDone should route success to CheckHasPostRun")
	assert.True(t, foundComplete, "IsJobDone should route fail/timeout to CompleteTrigger")

	// CheckHasPostRun: hasPostRun=true → InitPostRunLoop, default → CompleteTrigger
	var postRunCheck choiceState
	require.NoError(t, json.Unmarshal(asl.States["CheckHasPostRun"], &postRunCheck))
	assert.Equal(t, "CompleteTrigger", postRunCheck.Default)
	require.NotEmpty(t, postRunCheck.Choices)
	assert.Equal(t, "InitPostRunLoop", postRunCheck.Choices[0].Next)

	// CompleteTrigger → CheckCancelSLA
	var complete taskState
	require.NoError(t, json.Unmarshal(asl.States["CompleteTrigger"], &complete))
	assert.Equal(t, "CheckCancelSLA", complete.Next)
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

	// ValidationExhausted → CheckCancelSLA (proactive SLA: scheduling moved to watchdog)
	var exhausted taskState
	require.NoError(t, json.Unmarshal(asl.States["ValidationExhausted"], &exhausted))
	assert.Equal(t, "CheckCancelSLA", exhausted.Next)
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

func TestASL_JobPollExhaustionFlow(t *testing.T) {
	asl := loadASL(t)

	// IncrementJobPollElapsed → CheckJobPollExhausted
	var inc stateBase
	require.NoError(t, json.Unmarshal(asl.States["IncrementJobPollElapsed"], &inc))
	assert.Equal(t, "CheckJobPollExhausted", inc.Next)

	// CheckJobPollExhausted: exhausted → JobPollExhausted, default → WaitForJob
	var check choiceState
	require.NoError(t, json.Unmarshal(asl.States["CheckJobPollExhausted"], &check))
	assert.Equal(t, "WaitForJob", check.Default)
	require.NotEmpty(t, check.Choices)
	assert.Equal(t, "JobPollExhausted", check.Choices[0].Next)

	// JobPollExhausted → InjectTimeoutEvent
	var exhausted taskState
	require.NoError(t, json.Unmarshal(asl.States["JobPollExhausted"], &exhausted))
	assert.Equal(t, "InjectTimeoutEvent", exhausted.Next)

	// InjectTimeoutEvent → CompleteTrigger
	var inject stateBase
	require.NoError(t, json.Unmarshal(asl.States["InjectTimeoutEvent"], &inject))
	assert.Equal(t, "CompleteTrigger", inject.Next)

	// JobPollExhausted Catch falls through to InjectTimeoutEvent (best-effort)
	require.NotEmpty(t, exhausted.Catch)
	assert.Equal(t, "InjectTimeoutEvent", exhausted.Catch[0].Next)
}

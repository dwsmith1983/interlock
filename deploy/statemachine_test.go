package deploy

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

type taskState struct {
	Type  string       `json:"Type"`
	Next  string       `json:"Next,omitempty"`
	End   bool         `json:"End,omitempty"`
	Catch []catchEntry `json:"Catch,omitempty"`
}

type mapState struct {
	Type          string `json:"Type"`
	Next          string `json:"Next,omitempty"`
	ItemProcessor struct {
		StartAt string                     `json:"StartAt"`
		States  map[string]json.RawMessage `json:"States"`
	} `json:"ItemProcessor"`
}

func loadASL(t *testing.T) aslDefinition {
	t.Helper()
	data, err := os.ReadFile("statemachine.asl.json")
	require.NoError(t, err, "reading ASL file")

	var asl aslDefinition
	require.NoError(t, json.Unmarshal(data, &asl), "parsing ASL JSON")
	return asl
}

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

func TestASL_ExpectedStatesExist(t *testing.T) {
	asl := loadASL(t)

	expectedStates := []string{
		"CheckExclusion",
		"IsExcluded",
		"AcquireLock",
		"IsLockAcquired",
		"CheckRunLog",
		"ShouldProceedAfterRunLog",
		"ResolvePipeline",
		"IsResolved",
		"EvaluateTraits",
		"CheckEvaluationSLA",
		"CheckReadiness",
		"IsReady",
		"LogNotReady",
		"TriggerPipeline",
		"IsTriggerSuccessful",
		"LogTriggerFailed",
		"InitPollCounter",
		"WaitForRun",
		"PollRunStatus",
		"EvaluatePollResult",
		"IncrementPollCount",
		"CheckPollLimit",
		"CheckCompletionSLA",
		"LogCompleted",
		"LogRunFailed",
		"LogTimeout",
		"ReleaseLock",
		"End",
	}

	for _, name := range expectedStates {
		_, ok := asl.States[name]
		assert.True(t, ok, "expected state %q not found", name)
	}
}

// allTransitionTargets collects every state name that is referenced as a Next, Default, or Catch target.
func allTransitionTargets(t *testing.T, asl aslDefinition) map[string]bool {
	t.Helper()
	targets := make(map[string]bool)

	for _, raw := range asl.States {
		// Try as task state (has Next, Catch)
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

		// Try as choice state (has Choices, Default)
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

		// Try as map state (has ItemProcessor with inner states)
		var ms mapState
		if err := json.Unmarshal(raw, &ms); err == nil && ms.Type == "Map" {
			if ms.Next != "" {
				targets[ms.Next] = true
			}
		}
	}

	return targets
}

func TestASL_AllTransitionsReferenceExistingStates(t *testing.T) {
	asl := loadASL(t)
	targets := allTransitionTargets(t, asl)

	for target := range targets {
		_, ok := asl.States[target]
		assert.True(t, ok, "transition target %q does not reference an existing state", target)
	}
}

func TestASL_NoOrphanStates(t *testing.T) {
	asl := loadASL(t)
	targets := allTransitionTargets(t, asl)

	// The start state is always reachable
	targets[asl.StartAt] = true

	for name := range asl.States {
		assert.True(t, targets[name], "state %q is never referenced (orphan)", name)
	}
}

func TestASL_AllStatesHaveValidType(t *testing.T) {
	asl := loadASL(t)
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

func TestASL_MapStateHasItemProcessor(t *testing.T) {
	asl := loadASL(t)

	for name, raw := range asl.States {
		var ms mapState
		if err := json.Unmarshal(raw, &ms); err == nil && ms.Type == "Map" {
			assert.NotEmpty(t, ms.ItemProcessor.StartAt, "Map state %q must have ItemProcessor.StartAt", name)
			assert.NotEmpty(t, ms.ItemProcessor.States, "Map state %q must have ItemProcessor.States", name)

			// Verify inner StartAt exists
			_, ok := ms.ItemProcessor.States[ms.ItemProcessor.StartAt]
			assert.True(t, ok, "Map state %q ItemProcessor.StartAt %q must reference an inner state", name, ms.ItemProcessor.StartAt)
		}
	}
}

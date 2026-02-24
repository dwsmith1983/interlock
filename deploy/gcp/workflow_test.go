package gcp

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// loadWorkflowSteps parses the workflow YAML and returns the main steps as a
// list of maps (stepName → stepBody).
func loadWorkflowSteps(t *testing.T) []map[string]interface{} {
	t.Helper()
	data, err := os.ReadFile("workflow.yaml")
	require.NoError(t, err, "reading workflow YAML")

	var raw map[string]interface{}
	require.NoError(t, yaml.Unmarshal(data, &raw), "parsing workflow YAML")

	main, ok := raw["main"].(map[string]interface{})
	require.True(t, ok, "main must be a map")

	stepsRaw, ok := main["steps"].([]interface{})
	require.True(t, ok, "main.steps must be a list")

	var steps []map[string]interface{}
	for _, s := range stepsRaw {
		step, ok := s.(map[string]interface{})
		require.True(t, ok, "each step must be a map")
		steps = append(steps, step)
	}
	return steps
}

func loadWorkflowRaw(t *testing.T) map[string]interface{} {
	t.Helper()
	data, err := os.ReadFile("workflow.yaml")
	require.NoError(t, err, "reading workflow YAML")

	var raw map[string]interface{}
	require.NoError(t, yaml.Unmarshal(data, &raw), "parsing workflow YAML")
	return raw
}

func stepNames(steps []map[string]interface{}) []string {
	var names []string
	for _, step := range steps {
		for name := range step {
			names = append(names, name)
		}
	}
	return names
}

func stepByName(steps []map[string]interface{}, name string) (map[string]interface{}, bool) {
	for _, step := range steps {
		if body, ok := step[name]; ok {
			bodyMap, ok := body.(map[string]interface{})
			if ok {
				return bodyMap, true
			}
		}
	}
	return nil, false
}

func TestWorkflow_ValidYAML(t *testing.T) {
	data, err := os.ReadFile("workflow.yaml")
	require.NoError(t, err)

	var raw interface{}
	require.NoError(t, yaml.Unmarshal(data, &raw), "workflow file must be valid YAML")
}

func TestWorkflow_HasMainWorkflow(t *testing.T) {
	raw := loadWorkflowRaw(t)

	main, ok := raw["main"].(map[string]interface{})
	require.True(t, ok, "main must be a map")

	params, ok := main["params"].([]interface{})
	require.True(t, ok, "main.params must be a list")
	assert.Contains(t, params, "input", "main workflow should accept input param")

	steps := loadWorkflowSteps(t)
	assert.NotEmpty(t, steps, "main workflow must have steps")
}

func TestWorkflow_HasRetryPolicy(t *testing.T) {
	raw := loadWorkflowRaw(t)
	_, ok := raw["retryPolicy"]
	assert.True(t, ok, "workflow should define a retryPolicy")
}

func TestWorkflow_ExpectedStepsExist(t *testing.T) {
	steps := loadWorkflowSteps(t)
	names := stepNames(steps)

	expectedSteps := []string{
		"initDefaults",
		"checkExclusion",
		"isExcluded",
		"acquireLock",
		"isLockAcquired",
		"checkRunLog",
		"shouldProceedAfterRunLog",
		"resolvePipeline",
		"isResolved",
		"evaluateTraits",
		"checkEvaluationSLA",
		"checkValidationTimeout",
		"isValidationTimedOut",
		"logValidationTimeout",
		"checkReadiness",
		"isReady",
		"logNotReady",
		"triggerPipeline",
		"isTriggerSuccessful",
		"logTriggerFailed",
		"initPollCounter",
		"waitForRun",
		"pollRunStatus",
		"evaluatePollResult",
		"incrementPollCount",
		"checkPollLimit",
		"checkCompletionSLA",
		"logCompleted",
		"notifyDownstream",
		"shouldMonitor",
		"initMonitoringCounter",
		"monitoringWait",
		"monitoringEvaluate",
		"monitoringCheckDrift",
		"isDrifted",
		"handleLateArrival",
		"incrementMonitoringCount",
		"checkMonitoringExpired",
		"isMonitoringExpired",
		"logRunFailed",
		"shouldRetry",
		"retryBackoffWait",
		"releaseLockForRetry",
		"logTimeout",
		"alertError",
		"releaseLock",
		"theEnd",
	}

	for _, expected := range expectedSteps {
		assert.Contains(t, names, expected, "expected step %q not found", expected)
	}
}

func TestWorkflow_StepCount(t *testing.T) {
	steps := loadWorkflowSteps(t)
	names := stepNames(steps)
	// 47 steps matching the ASL state count
	assert.GreaterOrEqual(t, len(names), 47, "workflow should have at least 47 steps")
}

func TestWorkflow_AllNextTargetsExist(t *testing.T) {
	steps := loadWorkflowSteps(t)
	nameSet := make(map[string]bool)
	for _, step := range steps {
		for name := range step {
			nameSet[name] = true
		}
	}

	for _, step := range steps {
		for stepName, body := range step {
			bodyMap, ok := body.(map[string]interface{})
			if !ok {
				continue
			}

			// Check direct "next" field
			if next, ok := bodyMap["next"].(string); ok {
				assert.True(t, nameSet[next], "step %q references non-existent target %q via next", stepName, next)
			}

			// Check switch conditions
			if sw, ok := bodyMap["switch"].([]interface{}); ok {
				for _, cond := range sw {
					condMap, ok := cond.(map[string]interface{})
					if !ok {
						continue
					}
					if next, ok := condMap["next"].(string); ok {
						assert.True(t, nameSet[next], "step %q switch references non-existent target %q", stepName, next)
					}
				}
			}

			// Check try/except blocks for "next" in sub-steps
			checkSubStepNextTargets(t, stepName, bodyMap, nameSet)
		}
	}
}

// checkSubStepNextTargets looks for next targets inside try/except sub-steps.
func checkSubStepNextTargets(t *testing.T, parentName string, body map[string]interface{}, validNames map[string]bool) {
	t.Helper()

	if except, ok := body["except"].(map[string]interface{}); ok {
		if innerSteps, ok := except["steps"].([]interface{}); ok {
			for _, step := range innerSteps {
				stepMap, ok := step.(map[string]interface{})
				if !ok {
					continue
				}
				for innerName, innerBody := range stepMap {
					innerMap, ok := innerBody.(map[string]interface{})
					if !ok {
						continue
					}
					if next, ok := innerMap["next"].(string); ok {
						assert.True(t, validNames[next],
							"step %q except sub-step %q references non-existent target %q",
							parentName, innerName, next)
					}
				}
			}
		}
	}
}

func TestWorkflow_SwitchStepsHaveConditions(t *testing.T) {
	steps := loadWorkflowSteps(t)

	switchSteps := []string{
		"isExcluded",
		"isLockAcquired",
		"shouldProceedAfterRunLog",
		"isResolved",
		"isValidationTimedOut",
		"isReady",
		"isTriggerSuccessful",
		"evaluatePollResult",
		"checkPollLimit",
		"shouldMonitor",
		"isDrifted",
		"isMonitoringExpired",
		"shouldRetry",
	}

	for _, name := range switchSteps {
		bodyMap, ok := stepByName(steps, name)
		require.True(t, ok, "step %q not found", name)

		sw, ok := bodyMap["switch"]
		require.True(t, ok, "step %q should have a switch field", name)

		swSlice, ok := sw.([]interface{})
		require.True(t, ok, "step %q switch should be a list", name)
		assert.NotEmpty(t, swSlice, "step %q switch should have at least one condition", name)
	}
}

func TestWorkflow_TerminalStepReturns(t *testing.T) {
	steps := loadWorkflowSteps(t)

	bodyMap, ok := stepByName(steps, "theEnd")
	require.True(t, ok, "theEnd step must exist")
	assert.Contains(t, bodyMap, "return", "theEnd step should have a return")
}

func TestWorkflow_FirstStepIsInit(t *testing.T) {
	steps := loadWorkflowSteps(t)
	require.NotEmpty(t, steps)

	firstStep := steps[0]
	for name := range firstStep {
		assert.Equal(t, "initDefaults", name, "first step should be initDefaults")
	}
}

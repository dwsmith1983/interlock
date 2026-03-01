package v2_test

import (
	"encoding/json"
	"testing"

	v2 "github.com/dwsmith1983/interlock/internal/lambda/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOrchestratorInput_JSON(t *testing.T) {
	input := v2.OrchestratorInput{
		Mode:       "evaluate",
		PipelineID: "gold-revenue",
		ScheduleID: "cron",
		Date:       "2026-03-01",
	}
	data, err := json.Marshal(input)
	require.NoError(t, err)

	var decoded v2.OrchestratorInput
	require.NoError(t, json.Unmarshal(data, &decoded))
	assert.Equal(t, input, decoded)
}

func TestOrchestratorOutput_JSON(t *testing.T) {
	output := v2.OrchestratorOutput{
		Mode:   "evaluate",
		Passed: true,
		RunID:  "run-123",
	}
	data, err := json.Marshal(output)
	require.NoError(t, err)

	var decoded v2.OrchestratorOutput
	require.NoError(t, json.Unmarshal(data, &decoded))
	assert.Equal(t, output.Mode, decoded.Mode)
	assert.Equal(t, output.Passed, decoded.Passed)
	assert.Equal(t, output.RunID, decoded.RunID)
}

func TestSLAMonitorInput_JSON(t *testing.T) {
	input := v2.SLAMonitorInput{
		Mode:             "calculate",
		PipelineID:       "gold-revenue",
		Deadline:         "08:00",
		ExpectedDuration: "30m",
		Timezone:         "America/New_York",
		Critical:         true,
	}
	data, err := json.Marshal(input)
	require.NoError(t, err)

	var decoded v2.SLAMonitorInput
	require.NoError(t, json.Unmarshal(data, &decoded))
	assert.Equal(t, input, decoded)
}

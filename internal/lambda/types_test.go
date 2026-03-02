package lambda_test

import (
	"encoding/json"
	"testing"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOrchestratorInput_JSON(t *testing.T) {
	input := lambda.OrchestratorInput{
		Mode:       "evaluate",
		PipelineID: "gold-revenue",
		ScheduleID: "cron",
		Date:       "2026-03-01",
	}
	data, err := json.Marshal(input)
	require.NoError(t, err)

	var decoded lambda.OrchestratorInput
	require.NoError(t, json.Unmarshal(data, &decoded))
	assert.Equal(t, input, decoded)
}

func TestOrchestratorOutput_JSON(t *testing.T) {
	output := lambda.OrchestratorOutput{
		Mode:   "evaluate",
		Status: "passed",
		RunID:  "run-123",
	}
	data, err := json.Marshal(output)
	require.NoError(t, err)

	var decoded lambda.OrchestratorOutput
	require.NoError(t, json.Unmarshal(data, &decoded))
	assert.Equal(t, output.Mode, decoded.Mode)
	assert.Equal(t, output.Status, decoded.Status)
	assert.Equal(t, output.RunID, decoded.RunID)
}

func TestSLAMonitorInput_JSON(t *testing.T) {
	input := lambda.SLAMonitorInput{
		Mode:             "calculate",
		PipelineID:       "gold-revenue",
		Deadline:         "08:00",
		ExpectedDuration: "30m",
		Timezone:         "America/New_York",
		Critical:         true,
	}
	data, err := json.Marshal(input)
	require.NoError(t, err)

	var decoded lambda.SLAMonitorInput
	require.NoError(t, json.Unmarshal(data, &decoded))
	assert.Equal(t, input, decoded)
}

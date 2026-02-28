package schedule

import (
	"testing"
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckEvaluationSLA_NoSLA(t *testing.T) {
	pipeline := types.PipelineConfig{Name: "pipe-a"}
	result, err := CheckEvaluationSLA(pipeline, time.Now(), time.Time{})
	require.NoError(t, err)
	assert.False(t, result.Breached)
	assert.False(t, result.AtRisk)
	assert.True(t, result.Deadline.IsZero())
}

func TestCheckEvaluationSLA_Breached(t *testing.T) {
	now := time.Date(2026, 2, 28, 12, 0, 0, 0, time.UTC)
	pipeline := types.PipelineConfig{
		Name: "pipe-a",
		SLA:  &types.SLAConfig{EvaluationDeadline: "10:00"},
	}

	result, err := CheckEvaluationSLA(pipeline, now, time.Time{})
	require.NoError(t, err)
	assert.True(t, result.Breached)
	assert.False(t, result.AtRisk)
}

func TestCheckEvaluationSLA_NotBreached(t *testing.T) {
	now := time.Date(2026, 2, 28, 8, 0, 0, 0, time.UTC)
	pipeline := types.PipelineConfig{
		Name: "pipe-a",
		SLA:  &types.SLAConfig{EvaluationDeadline: "10:00"},
	}

	result, err := CheckEvaluationSLA(pipeline, now, time.Time{})
	require.NoError(t, err)
	assert.False(t, result.Breached)
}

func TestCheckEvaluationSLA_AtRisk(t *testing.T) {
	now := time.Date(2026, 2, 28, 9, 57, 0, 0, time.UTC)
	pipeline := types.PipelineConfig{
		Name: "pipe-a",
		SLA: &types.SLAConfig{
			EvaluationDeadline: "10:00",
			AtRiskLeadTime:     "5m",
		},
	}

	result, err := CheckEvaluationSLA(pipeline, now, time.Time{})
	require.NoError(t, err)
	assert.False(t, result.Breached)
	assert.True(t, result.AtRisk)
}

func TestCheckEvaluationSLA_InvalidDeadline(t *testing.T) {
	now := time.Date(2026, 2, 28, 10, 0, 0, 0, time.UTC)
	pipeline := types.PipelineConfig{
		Name: "pipe-a",
		SLA:  &types.SLAConfig{EvaluationDeadline: "not-valid"},
	}

	_, err := CheckEvaluationSLA(pipeline, now, time.Time{})
	assert.Error(t, err)
}

func TestCheckCompletionSLA_NoDeadline(t *testing.T) {
	pipeline := types.PipelineConfig{Name: "pipe-a"}
	result, err := CheckCompletionSLA(pipeline, "daily", time.Now(), time.Time{})
	require.NoError(t, err)
	assert.False(t, result.Breached)
	assert.True(t, result.Deadline.IsZero())
}

func TestCheckCompletionSLA_Breached(t *testing.T) {
	now := time.Date(2026, 2, 28, 12, 0, 0, 0, time.UTC)
	pipeline := types.PipelineConfig{
		Name: "pipe-a",
		SLA:  &types.SLAConfig{CompletionDeadline: "10:00"},
	}

	result, err := CheckCompletionSLA(pipeline, "daily", now, time.Time{})
	require.NoError(t, err)
	assert.True(t, result.Breached)
}

func TestCheckCompletionSLA_AtRisk(t *testing.T) {
	now := time.Date(2026, 2, 28, 9, 57, 0, 0, time.UTC)
	pipeline := types.PipelineConfig{
		Name: "pipe-a",
		SLA: &types.SLAConfig{
			CompletionDeadline: "10:00",
			AtRiskLeadTime:     "5m",
		},
	}

	result, err := CheckCompletionSLA(pipeline, "daily", now, time.Time{})
	require.NoError(t, err)
	assert.False(t, result.Breached)
	assert.True(t, result.AtRisk)
}

func TestCheckValidationTimeout_NoSLA(t *testing.T) {
	pipeline := types.PipelineConfig{Name: "pipe-a"}
	timedOut, err := CheckValidationTimeout(pipeline, time.Now(), time.Time{})
	require.NoError(t, err)
	assert.False(t, timedOut)
}

func TestCheckValidationTimeout_Exceeded(t *testing.T) {
	now := time.Date(2026, 2, 28, 12, 0, 0, 0, time.UTC)
	pipeline := types.PipelineConfig{
		Name: "pipe-a",
		SLA:  &types.SLAConfig{ValidationTimeout: "10:00"},
	}

	timedOut, err := CheckValidationTimeout(pipeline, now, time.Time{})
	require.NoError(t, err)
	assert.True(t, timedOut)
}

func TestCheckValidationTimeout_NotExceeded(t *testing.T) {
	now := time.Date(2026, 2, 28, 8, 0, 0, 0, time.UTC)
	pipeline := types.PipelineConfig{
		Name: "pipe-a",
		SLA:  &types.SLAConfig{ValidationTimeout: "10:00"},
	}

	timedOut, err := CheckValidationTimeout(pipeline, now, time.Time{})
	require.NoError(t, err)
	assert.False(t, timedOut)
}

func TestCheckValidationTimeout_InvalidFormat(t *testing.T) {
	now := time.Date(2026, 2, 28, 10, 0, 0, 0, time.UTC)
	pipeline := types.PipelineConfig{
		Name: "pipe-a",
		SLA:  &types.SLAConfig{ValidationTimeout: "not-valid"},
	}

	_, err := CheckValidationTimeout(pipeline, now, time.Time{})
	assert.Error(t, err)
}

func TestCheckCompletionSLA_ScheduleLevelDeadline(t *testing.T) {
	now := time.Date(2026, 2, 28, 12, 0, 0, 0, time.UTC)
	pipeline := types.PipelineConfig{
		Name: "pipe-a",
		Schedules: []types.ScheduleConfig{
			{Name: "hourly", Deadline: "10:00"},
		},
	}

	result, err := CheckCompletionSLA(pipeline, "hourly", now, time.Time{})
	require.NoError(t, err)
	assert.True(t, result.Breached)
}

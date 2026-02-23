package evaluator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dwsmith1983/interlock/internal/testutil"
	"github.com/dwsmith1983/interlock/pkg/types"
)

func TestUpstreamJobLogHandler_Pass(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	// Set up upstream as completed
	now := time.Now()
	require.NoError(t, prov.PutRunLog(ctx, types.RunLogEntry{
		PipelineID:  "silver-pipeline",
		Date:        "2026-02-23",
		ScheduleID:  "h14",
		Status:      types.RunCompleted,
		RunID:       "run-123",
		StartedAt:   now,
		CompletedAt: &now,
		UpdatedAt:   now,
	}))

	handler := NewUpstreamJobLogHandler(prov)
	out, err := handler(ctx, types.EvaluatorInput{
		PipelineID: "gold-pipeline",
		TraitType:  "upstream",
		Config: map[string]interface{}{
			"upstreamPipeline": "silver-pipeline",
			"upstreamSchedule": "h14",
			"date":             "2026-02-23",
		},
	})

	require.NoError(t, err)
	assert.Equal(t, types.TraitPass, out.Status)
	assert.Contains(t, out.Reason, "completed")
}

func TestUpstreamJobLogHandler_Fail_NotCompleted(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	// Upstream is running, not completed
	require.NoError(t, prov.PutRunLog(ctx, types.RunLogEntry{
		PipelineID: "silver-pipeline",
		Date:       "2026-02-23",
		ScheduleID: "daily",
		Status:     types.RunRunning,
		RunID:      "run-456",
		StartedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}))

	handler := NewUpstreamJobLogHandler(prov)
	out, err := handler(ctx, types.EvaluatorInput{
		PipelineID: "gold-pipeline",
		TraitType:  "upstream",
		Config: map[string]interface{}{
			"upstreamPipeline": "silver-pipeline",
			"date":             "2026-02-23",
		},
	})

	require.NoError(t, err)
	assert.Equal(t, types.TraitFail, out.Status)
	assert.Contains(t, out.Reason, "RUNNING")
}

func TestUpstreamJobLogHandler_Fail_NoRunLog(t *testing.T) {
	prov := testutil.NewMockProvider()
	handler := NewUpstreamJobLogHandler(prov)

	out, err := handler(context.Background(), types.EvaluatorInput{
		PipelineID: "gold-pipeline",
		TraitType:  "upstream",
		Config: map[string]interface{}{
			"upstreamPipeline": "silver-pipeline",
			"date":             "2026-02-23",
		},
	})

	require.NoError(t, err)
	assert.Equal(t, types.TraitFail, out.Status)
	assert.Contains(t, out.Reason, "no run log")
}

func TestUpstreamJobLogHandler_MissingConfig(t *testing.T) {
	prov := testutil.NewMockProvider()
	handler := NewUpstreamJobLogHandler(prov)

	// Missing upstreamPipeline
	out, err := handler(context.Background(), types.EvaluatorInput{
		Config: map[string]interface{}{"date": "2026-02-23"},
	})
	require.NoError(t, err)
	assert.Equal(t, types.TraitFail, out.Status)
	assert.Contains(t, out.Reason, "upstreamPipeline")

	// Missing date
	out, err = handler(context.Background(), types.EvaluatorInput{
		Config: map[string]interface{}{"upstreamPipeline": "silver"},
	})
	require.NoError(t, err)
	assert.Equal(t, types.TraitFail, out.Status)
	assert.Contains(t, out.Reason, "date")
}

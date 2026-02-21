package providertest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/interlock-systems/interlock/internal/provider"
	"github.com/interlock-systems/interlock/pkg/types"
)

// TestPipelineCRUD verifies register, get, list, delete operations.
func TestPipelineCRUD(t *testing.T, prov provider.Provider) {
	ctx := context.Background()

	pipeline := types.PipelineConfig{
		Name:      "ct-pipeline",
		Archetype: "batch-ingestion",
		Tier:      2,
	}

	// Register
	err := prov.RegisterPipeline(ctx, pipeline)
	require.NoError(t, err)

	// Get
	got, err := prov.GetPipeline(ctx, "ct-pipeline")
	require.NoError(t, err)
	assert.Equal(t, "ct-pipeline", got.Name)
	assert.Equal(t, "batch-ingestion", got.Archetype)
	assert.Equal(t, 2, got.Tier)

	// List
	list, err := prov.ListPipelines(ctx)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(list), 1)

	// Register second
	err = prov.RegisterPipeline(ctx, types.PipelineConfig{Name: "ct-pipeline-2", Archetype: "streaming"})
	require.NoError(t, err)
	list, err = prov.ListPipelines(ctx)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(list), 2)

	// Delete
	err = prov.DeletePipeline(ctx, "ct-pipeline")
	require.NoError(t, err)
	_, err = prov.GetPipeline(ctx, "ct-pipeline")
	assert.Error(t, err)
}

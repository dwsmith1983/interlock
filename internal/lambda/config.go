package lambda

import (
	"context"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// GetValidatedConfig returns the pipeline's config from the config cache,
// which holds only configs that parsed and passed validation (the cache logs
// each rejected row). Returns nil without an error for a missing or
// rejected pipeline, signalling the caller to skip it.
func GetValidatedConfig(ctx context.Context, d *Deps, pipelineID string) (*types.PipelineConfig, error) {
	return d.ConfigCache.Get(ctx, pipelineID)
}

package lambda

import (
	"context"

	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/dwsmith1983/interlock/pkg/validation"
)

// GetValidatedConfig loads a pipeline config and validates its retry/timeout
// fields. Returns nil (with a warning log) if validation fails, signalling the
// caller to skip processing for this pipeline.
func GetValidatedConfig(ctx context.Context, d *Deps, pipelineID string) (*types.PipelineConfig, error) {
	cfg, err := d.ConfigCache.Get(ctx, pipelineID)
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, nil
	}
	if errs := validation.ValidatePipelineConfig(cfg); len(errs) > 0 {
		d.Logger.Warn("invalid pipeline config, skipping",
			"pipelineId", pipelineID,
			"errors", errs,
		)
		return nil, nil
	}
	return cfg, nil
}

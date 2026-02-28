// Package cascade provides downstream pipeline notification logic.
//
// When a pipeline completes, its downstream dependents need to be notified
// via cascade markers so they can be re-evaluated. This package extracts
// that domain logic from the orchestrator Lambda handler.
package cascade

import (
	"context"
	"log/slog"
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// Provider is the minimal storage interface needed for cascade operations.
type Provider interface {
	ListDependents(ctx context.Context, upstreamID string) ([]string, error)
	ListPipelines(ctx context.Context) ([]types.PipelineConfig, error)
	GetPipeline(ctx context.Context, id string) (*types.PipelineConfig, error)
	WriteCascadeMarker(ctx context.Context, pipelineID, scheduleID, date, source string) error
}

// ScanDependents performs a full pipeline scan to find downstream dependents
// of the given upstream pipeline. This is the fallback path when the
// dependency index (ListDependents) returns empty results during bootstrap.
func ScanDependents(ctx context.Context, prov Provider, upstreamID string) ([]string, error) {
	pipelines, err := prov.ListPipelines(ctx)
	if err != nil {
		return nil, err
	}
	var dependents []string
	for _, p := range pipelines {
		if p.Name == upstreamID {
			continue
		}
		for _, tc := range p.Traits {
			up, _ := tc.Config["upstreamPipeline"].(string)
			if up == upstreamID {
				dependents = append(dependents, p.Name)
				break
			}
		}
	}
	return dependents, nil
}

// NotifyDownstream finds downstream dependents of a pipeline and writes
// cascade markers to trigger their re-evaluation. It first tries the O(1)
// dependency index, falling back to a full pipeline scan if the index is
// empty (bootstrap period).
//
// Returns the list of notified entries as "pipelineID/scheduleID" strings.
func NotifyDownstream(ctx context.Context, prov Provider, pipelineID, scheduleID, date string, logger *slog.Logger) ([]string, error) {
	// Use dependency index for O(1) downstream lookup.
	dependents, err := prov.ListDependents(ctx, pipelineID)
	if err != nil {
		logger.Warn("ListDependents failed, falling back to scan", "pipeline", pipelineID, "error", err)
	}

	// Fallback to full scan if dependency index returns empty (bootstrap period).
	if len(dependents) == 0 {
		dependents, err = ScanDependents(ctx, prov, pipelineID)
		if err != nil {
			return nil, err
		}
	}

	// Resolve cascade backpressure delay.
	var cascadeDelay time.Duration
	pipeline, err := prov.GetPipeline(ctx, pipelineID)
	if err == nil && pipeline != nil && pipeline.Cascade != nil && pipeline.Cascade.DelayBetween != "" {
		if d, parseErr := time.ParseDuration(pipeline.Cascade.DelayBetween); parseErr == nil {
			cascadeDelay = d
		}
	}

	var notified []string
	for i, downstream := range dependents {
		// Apply backpressure delay between cascade writes.
		if cascadeDelay > 0 && i > 0 {
			time.Sleep(cascadeDelay)
		}
		if writeErr := prov.WriteCascadeMarker(ctx, downstream, scheduleID, date, pipelineID); writeErr != nil {
			logger.Error("failed to write cascade marker",
				"downstream", downstream, "schedule", scheduleID, "error", writeErr)
		} else {
			notified = append(notified, downstream+"/"+scheduleID)
		}
	}

	return notified, nil
}

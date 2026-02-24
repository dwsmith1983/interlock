package firestore

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/firestore"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// RegisterPipeline stores a pipeline configuration.
func (p *FirestoreProvider) RegisterPipeline(ctx context.Context, config types.PipelineConfig) error {
	data, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("marshaling pipeline: %w", err)
	}

	id := docID(pipelinePK(config.Name), configSK())
	_, err = p.coll().Doc(id).Set(ctx, map[string]interface{}{
		"data":   string(data),
		"gsi1pk": prefixType + "pipeline",
		"gsi1sk": pipelinePK(config.Name),
	})
	return err
}

// GetPipeline retrieves a pipeline configuration.
func (p *FirestoreProvider) GetPipeline(ctx context.Context, id string) (*types.PipelineConfig, error) {
	docRef := p.coll().Doc(docID(pipelinePK(id), configSK()))
	snap, err := docRef.Get(ctx)
	if err != nil {
		if isNotFound(err) {
			return nil, fmt.Errorf("pipeline %q not found", id)
		}
		return nil, err
	}

	dataStr, err := snapString(snap, "data")
	if err != nil {
		return nil, err
	}
	var config types.PipelineConfig
	if err := json.Unmarshal([]byte(dataStr), &config); err != nil {
		return nil, err
	}
	return &config, nil
}

// ListPipelines returns all registered pipelines via GSI1-equivalent query.
func (p *FirestoreProvider) ListPipelines(ctx context.Context) ([]types.PipelineConfig, error) {
	iter := p.coll().Where("gsi1pk", "==", prefixType+"pipeline").Documents(ctx)
	defer iter.Stop()

	var pipelines []types.PipelineConfig
	for {
		snap, err := iter.Next()
		if err != nil {
			break
		}
		dataStr, err := snapString(snap, "data")
		if err != nil {
			p.logger.Warn("skipping corrupt pipeline entry", "error", err)
			continue
		}
		var config types.PipelineConfig
		if err := json.Unmarshal([]byte(dataStr), &config); err != nil {
			p.logger.Warn("skipping corrupt pipeline data", "error", err)
			continue
		}
		pipelines = append(pipelines, config)
	}
	return pipelines, nil
}

// DeletePipeline removes a pipeline configuration.
func (p *FirestoreProvider) DeletePipeline(ctx context.Context, id string) error {
	_, err := p.coll().Doc(docID(pipelinePK(id), configSK())).Delete(ctx)
	return err
}

// snapString extracts a string field from a Firestore document snapshot.
func snapString(snap *firestore.DocumentSnapshot, key string) (string, error) {
	raw, err := snap.DataAt(key)
	if err != nil {
		return "", fmt.Errorf("missing field %q: %w", key, err)
	}
	s, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("field %q is not a string", key)
	}
	return s, nil
}

// snapInt64 extracts an int64 field (TTL) from a Firestore document snapshot.
func snapInt64(snap *firestore.DocumentSnapshot, key string) (int64, error) {
	raw, err := snap.DataAt(key)
	if err != nil {
		return 0, nil // missing is OK — no TTL
	}
	switch v := raw.(type) {
	case int64:
		return v, nil
	case float64:
		return int64(v), nil
	default:
		return 0, fmt.Errorf("field %q is not numeric", key)
	}
}

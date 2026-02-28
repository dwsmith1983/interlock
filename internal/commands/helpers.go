// Package commands implements the CLI subcommands for the interlock binary.
package commands

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/dwsmith1983/interlock/internal/provider"
	ddbprov "github.com/dwsmith1983/interlock/internal/provider/dynamodb"
	"github.com/dwsmith1983/interlock/internal/provider/redis"
	"github.com/dwsmith1983/interlock/pkg/types"
	"gopkg.in/yaml.v3"
)

// newProvider creates the configured storage provider.
func newProvider(cfg *types.ProjectConfig) (provider.Provider, error) {
	switch cfg.Provider {
	case "redis":
		rc, ok := cfg.Redis.(*redis.Config)
		if !ok || rc == nil {
			return nil, fmt.Errorf("redis config is required when provider is redis")
		}
		return redis.New(rc), nil
	case "dynamodb":
		dc, ok := cfg.DynamoDB.(*ddbprov.Config)
		if !ok || dc == nil {
			return nil, fmt.Errorf("dynamodb config is required when provider is dynamodb")
		}
		return ddbprov.New(dc)
	default:
		return nil, fmt.Errorf("unsupported provider: %s", cfg.Provider)
	}
}

// loadPipelineDir loads all pipeline YAML files from a directory.
func loadPipelineDir(dir string) ([]types.PipelineConfig, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var pipelines []types.PipelineConfig
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".yaml") && !strings.HasSuffix(name, ".yml") {
			continue
		}

		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, fmt.Errorf("reading %s: %w", name, err)
		}

		var p types.PipelineConfig
		if err := yaml.Unmarshal(data, &p); err != nil {
			return nil, fmt.Errorf("parsing %s: %w", name, err)
		}

		if p.Name == "" {
			continue
		}
		pipelines = append(pipelines, p)
	}

	return pipelines, nil
}

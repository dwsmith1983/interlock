// Package commands implements the CLI subcommands for the interlock binary.
package commands

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/dwsmith1983/interlock/internal/provider"
	ddbprov "github.com/dwsmith1983/interlock/internal/provider/dynamodb"
	fsprov "github.com/dwsmith1983/interlock/internal/provider/firestore"
	"github.com/dwsmith1983/interlock/internal/provider/redis"
	"github.com/dwsmith1983/interlock/pkg/types"
	"gopkg.in/yaml.v3"
)

// newProvider creates the configured storage provider.
func newProvider(cfg *types.ProjectConfig) (provider.Provider, error) {
	switch cfg.Provider {
	case "redis":
		return redis.New(cfg.Redis), nil
	case "dynamodb":
		return ddbprov.New(cfg.DynamoDB)
	case "firestore":
		return fsprov.New(cfg.Firestore)
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

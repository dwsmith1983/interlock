// Package config handles loading and validation of interlock.yaml project configuration.
package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/dwsmith1983/interlock/pkg/types"
	"gopkg.in/yaml.v3"
)

// Load reads and parses interlock.yaml from the given directory.
func Load(dir string) (*types.ProjectConfig, error) {
	path := filepath.Join(dir, "interlock.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}

	var cfg types.ProjectConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	if err := validate(&cfg); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	return &cfg, nil
}

func validate(cfg *types.ProjectConfig) error {
	if cfg.Provider == "" {
		return fmt.Errorf("provider is required")
	}
	if cfg.Provider == "redis" && cfg.Redis == nil {
		return fmt.Errorf("redis config is required when provider is redis")
	}
	if cfg.Redis != nil && cfg.Redis.Addr == "" {
		return fmt.Errorf("redis.addr is required")
	}
	if cfg.Provider == "dynamodb" && cfg.DynamoDB == nil {
		return fmt.Errorf("dynamodb config is required when provider is dynamodb")
	}
	if cfg.DynamoDB != nil && cfg.DynamoDB.TableName == "" {
		return fmt.Errorf("dynamodb.tableName is required")
	}
	if len(cfg.ArchetypeDirs) == 0 {
		return fmt.Errorf("at least one archetypeDir is required")
	}
	if len(cfg.EvaluatorDirs) == 0 {
		return fmt.Errorf("at least one evaluatorDir is required")
	}
	if len(cfg.PipelineDirs) == 0 {
		return fmt.Errorf("at least one pipelineDir is required")
	}
	return nil
}

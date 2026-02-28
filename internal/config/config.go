// Package config handles loading and validation of interlock.yaml project configuration.
package config

import (
	"fmt"
	"os"
	"path/filepath"

	ddbprov "github.com/dwsmith1983/interlock/internal/provider/dynamodb"
	"github.com/dwsmith1983/interlock/internal/provider/redis"
	"github.com/dwsmith1983/interlock/pkg/types"
	"gopkg.in/yaml.v3"
)

// providerConfigs is a helper struct used for a second YAML unmarshal pass
// to decode provider-specific config sections into their concrete types.
type providerConfigs struct {
	Redis    *redis.Config   `yaml:"redis,omitempty"`
	DynamoDB *ddbprov.Config `yaml:"dynamodb,omitempty"`
}

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

	// Second pass: decode provider-specific sections into concrete types.
	var raw providerConfigs
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parsing provider config: %w", err)
	}
	if raw.Redis != nil {
		cfg.Redis = raw.Redis
	}
	if raw.DynamoDB != nil {
		cfg.DynamoDB = raw.DynamoDB
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
	if cfg.Provider == "redis" {
		rc, _ := cfg.Redis.(*redis.Config)
		if rc == nil {
			return fmt.Errorf("redis config is required when provider is redis")
		}
		if rc.Addr == "" {
			return fmt.Errorf("redis.addr is required")
		}
	}
	if cfg.Provider == "dynamodb" {
		dc, _ := cfg.DynamoDB.(*ddbprov.Config)
		if dc == nil {
			return fmt.Errorf("dynamodb config is required when provider is dynamodb")
		}
		if dc.TableName == "" {
			return fmt.Errorf("dynamodb.tableName is required")
		}
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

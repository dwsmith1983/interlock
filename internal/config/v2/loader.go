// Package config provides pipeline YAML configuration loading for Interlock v2.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	v2 "github.com/dwsmith1983/interlock/pkg/types/v2"
	"gopkg.in/yaml.v3"
)

// LoadPipelines loads all pipeline YAML files from a directory.
func LoadPipelines(dir string) ([]v2.PipelineConfig, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading pipelines dir %s: %w", dir, err)
	}

	var pipelines []v2.PipelineConfig
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".yaml") && !strings.HasSuffix(name, ".yml") {
			continue
		}
		path := filepath.Join(dir, name)
		cfg, err := LoadPipeline(path)
		if err != nil {
			return nil, fmt.Errorf("loading %s: %w", path, err)
		}
		pipelines = append(pipelines, cfg)
	}
	return pipelines, nil
}

// LoadPipeline loads a single pipeline YAML file.
func LoadPipeline(path string) (v2.PipelineConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return v2.PipelineConfig{}, fmt.Errorf("reading file: %w", err)
	}

	var cfg v2.PipelineConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return v2.PipelineConfig{}, fmt.Errorf("parsing YAML: %w", err)
	}

	if cfg.Pipeline.ID == "" {
		return v2.PipelineConfig{}, fmt.Errorf("pipeline ID is required in %s", path)
	}

	return cfg, nil
}

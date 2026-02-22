// Package archetype handles loading, validating, and resolving STAMP archetypes.
package archetype

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/dwsmith1983/interlock/pkg/types"
	"gopkg.in/yaml.v3"
)

// Registry manages archetype definitions loaded from YAML files.
type Registry struct {
	archetypes map[string]*types.Archetype
}

// NewRegistry creates a new empty archetype registry.
func NewRegistry() *Registry {
	return &Registry{
		archetypes: make(map[string]*types.Archetype),
	}
}

// LoadDir loads all YAML archetype files from a directory.
func (r *Registry) LoadDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("reading archetype dir %s: %w", dir, err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".yaml") && !strings.HasSuffix(name, ".yml") {
			continue
		}
		path := filepath.Join(dir, name)
		if err := r.LoadFile(path); err != nil {
			return fmt.Errorf("loading archetype %s: %w", path, err)
		}
	}
	return nil
}

// LoadFile loads a single archetype YAML file.
func (r *Registry) LoadFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading file: %w", err)
	}

	var arch types.Archetype
	if err := yaml.Unmarshal(data, &arch); err != nil {
		return fmt.Errorf("parsing YAML: %w", err)
	}

	if err := ValidateArchetype(&arch); err != nil {
		return fmt.Errorf("validating archetype %q: %w", arch.Name, err)
	}

	r.archetypes[arch.Name] = &arch
	return nil
}

// Get returns an archetype by name.
func (r *Registry) Get(name string) (*types.Archetype, error) {
	arch, ok := r.archetypes[name]
	if !ok {
		return nil, fmt.Errorf("archetype %q not found", name)
	}
	return arch, nil
}

// List returns all registered archetypes.
func (r *Registry) List() []*types.Archetype {
	result := make([]*types.Archetype, 0, len(r.archetypes))
	for _, arch := range r.archetypes {
		result = append(result, arch)
	}
	return result
}

// Register adds an archetype directly to the registry.
func (r *Registry) Register(arch *types.Archetype) error {
	if err := ValidateArchetype(arch); err != nil {
		return err
	}
	r.archetypes[arch.Name] = arch
	return nil
}

// ResolveTraits merges archetype trait definitions with pipeline overrides.
// Pipeline-level config overrides archetype defaults for evaluator, config, TTL, and timeout.
func ResolveTraits(arch *types.Archetype, pipeline *types.PipelineConfig) []ResolvedTrait {
	var resolved []ResolvedTrait

	for _, traitDef := range arch.RequiredTraits {
		rt := ResolvedTrait{
			Type:        traitDef.Type,
			Description: traitDef.Description,
			Required:    true,
			Config:      copyMap(traitDef.DefaultConfig),
			TTL:         traitDef.DefaultTTL,
			Timeout:     traitDef.DefaultTimeout,
		}

		if pipeline != nil && pipeline.Traits != nil {
			if override, ok := pipeline.Traits[traitDef.Type]; ok {
				applyOverrides(&rt, override)
			}
		}

		resolved = append(resolved, rt)
	}

	for _, traitDef := range arch.OptionalTraits {
		if pipeline == nil || pipeline.Traits == nil {
			continue
		}
		if _, ok := pipeline.Traits[traitDef.Type]; !ok {
			continue
		}

		rt := ResolvedTrait{
			Type:        traitDef.Type,
			Description: traitDef.Description,
			Required:    false,
			Config:      copyMap(traitDef.DefaultConfig),
			TTL:         traitDef.DefaultTTL,
			Timeout:     traitDef.DefaultTimeout,
		}

		applyOverrides(&rt, pipeline.Traits[traitDef.Type])
		resolved = append(resolved, rt)
	}

	return resolved
}

// applyOverrides merges pipeline-level trait config overrides onto a resolved trait.
func applyOverrides(rt *ResolvedTrait, override types.TraitConfig) {
	if override.Evaluator != "" {
		rt.Evaluator = override.Evaluator
	}
	if override.Config != nil {
		for k, v := range override.Config {
			rt.Config[k] = v
		}
	}
	if override.TTL > 0 {
		rt.TTL = override.TTL
	}
	if override.Timeout > 0 {
		rt.Timeout = override.Timeout
	}
}

// ResolvedTrait is a fully-resolved trait with archetype defaults and pipeline overrides merged.
type ResolvedTrait struct {
	Type        string
	Description string
	Required    bool
	Evaluator   string
	Config      map[string]interface{}
	TTL         int // seconds
	Timeout     int // seconds
}

func copyMap(m map[string]interface{}) map[string]interface{} {
	if m == nil {
		return make(map[string]interface{})
	}
	result := make(map[string]interface{}, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}

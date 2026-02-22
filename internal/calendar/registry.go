// Package calendar handles loading and resolving calendar exclusion definitions.
package calendar

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/dwsmith1983/interlock/pkg/types"
	"gopkg.in/yaml.v3"
)

// Registry manages calendar definitions loaded from YAML files.
type Registry struct {
	calendars map[string]*types.Calendar
}

// NewRegistry creates a new empty calendar registry.
func NewRegistry() *Registry {
	return &Registry{
		calendars: make(map[string]*types.Calendar),
	}
}

// LoadDir loads all YAML calendar files from a directory.
func (r *Registry) LoadDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("reading calendar dir %s: %w", dir, err)
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
			return fmt.Errorf("loading calendar %s: %w", path, err)
		}
	}
	return nil
}

// LoadFile loads a single calendar YAML file.
func (r *Registry) LoadFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading file: %w", err)
	}

	var cal types.Calendar
	if err := yaml.Unmarshal(data, &cal); err != nil {
		return fmt.Errorf("parsing YAML: %w", err)
	}

	if cal.Name == "" {
		return fmt.Errorf("calendar in %s has no name", path)
	}

	r.calendars[cal.Name] = &cal
	return nil
}

// Get returns a calendar by name, or nil if not found.
func (r *Registry) Get(name string) *types.Calendar {
	return r.calendars[name]
}

// Register adds a calendar directly to the registry (useful for testing).
func (r *Registry) Register(cal *types.Calendar) error {
	if cal.Name == "" {
		return fmt.Errorf("calendar has no name")
	}
	r.calendars[cal.Name] = cal
	return nil
}

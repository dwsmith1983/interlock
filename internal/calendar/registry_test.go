package calendar

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegistry_LoadDir(t *testing.T) {
	dir := t.TempDir()

	// Write two calendar files
	require.NoError(t, os.WriteFile(filepath.Join(dir, "us-business.yaml"), []byte(`
name: us-business
days: ["saturday", "sunday"]
dates:
  - "2025-12-25"
  - "2025-01-01"
`), 0o644))

	require.NoError(t, os.WriteFile(filepath.Join(dir, "uk-business.yml"), []byte(`
name: uk-business
days: ["saturday", "sunday"]
dates:
  - "2025-12-25"
  - "2025-12-26"
`), 0o644))

	// Write a non-YAML file that should be ignored
	require.NoError(t, os.WriteFile(filepath.Join(dir, "README.md"), []byte("ignored"), 0o644))

	reg := NewRegistry()
	require.NoError(t, reg.LoadDir(dir))

	us := reg.Get("us-business")
	require.NotNil(t, us)
	assert.Equal(t, "us-business", us.Name)
	assert.Equal(t, []string{"saturday", "sunday"}, us.Days)
	assert.Contains(t, us.Dates, "2025-12-25")
	assert.Contains(t, us.Dates, "2025-01-01")

	uk := reg.Get("uk-business")
	require.NotNil(t, uk)
	assert.Equal(t, "uk-business", uk.Name)
	assert.Contains(t, uk.Dates, "2025-12-26")
}

func TestRegistry_Get_NotFound(t *testing.T) {
	reg := NewRegistry()
	assert.Nil(t, reg.Get("nonexistent"))
}

func TestRegistry_LoadFile_NoName(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`days: ["saturday"]`), 0o644))

	reg := NewRegistry()
	err := reg.LoadFile(path)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no name")
}

func TestRegistry_LoadDir_MissingDir(t *testing.T) {
	reg := NewRegistry()
	err := reg.LoadDir("/nonexistent/path")
	assert.Error(t, err)
}

func TestRegistry_Register_Success(t *testing.T) {
	reg := NewRegistry()
	cal := &types.Calendar{Name: "test-cal", Dates: []string{"2025-12-25"}}
	require.NoError(t, reg.Register(cal))

	retrieved := reg.Get("test-cal")
	require.NotNil(t, retrieved)
	assert.Equal(t, "test-cal", retrieved.Name)
	assert.Contains(t, retrieved.Dates, "2025-12-25")
}

func TestRegistry_Register_NoName(t *testing.T) {
	reg := NewRegistry()
	err := reg.Register(&types.Calendar{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no name")
}

func TestRegistry_LoadFile_InvalidYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	require.NoError(t, os.WriteFile(path, []byte("{{invalid yaml"), 0o644))

	reg := NewRegistry()
	err := reg.LoadFile(path)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parsing YAML")
}

func TestRegistry_LoadFile_FileNotFound(t *testing.T) {
	reg := NewRegistry()
	err := reg.LoadFile("/nonexistent/calendar.yaml")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "reading file")
}

func TestRegistry_LoadDir_IgnoresSubdirs(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "subdir"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "subdir", "hidden.yaml"), []byte(`
name: hidden
dates: ["2025-01-01"]
`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "main.yaml"), []byte(`
name: main-cal
dates: ["2025-12-25"]
`), 0o644))

	reg := NewRegistry()
	require.NoError(t, reg.LoadDir(dir))
	assert.NotNil(t, reg.Get("main-cal"), "top-level calendar should be loaded")
	assert.Nil(t, reg.Get("hidden"), "subdirectory calendar should NOT be loaded")
}

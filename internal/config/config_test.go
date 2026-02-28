package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dwsmith1983/interlock/internal/provider/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad(t *testing.T) {
	dir := t.TempDir()
	content := `provider: redis
redis:
  addr: localhost:6379
  keyPrefix: "interlock:"
server:
  addr: ":3000"
archetypeDirs:
  - ./archetypes
evaluatorDirs:
  - ./evaluators
pipelineDirs:
  - ./pipelines
alerts:
  - type: console
`
	err := os.WriteFile(filepath.Join(dir, "interlock.yaml"), []byte(content), 0o644)
	require.NoError(t, err)

	cfg, err := Load(dir)
	require.NoError(t, err)
	assert.Equal(t, "redis", cfg.Provider)
	rc, ok := cfg.Redis.(*redis.Config)
	require.True(t, ok, "Redis config should be *redis.Config")
	assert.Equal(t, "localhost:6379", rc.Addr)
	assert.Equal(t, "interlock:", rc.KeyPrefix)
	assert.Equal(t, ":3000", cfg.Server.Addr)
	assert.Len(t, cfg.ArchetypeDirs, 1)
	assert.Len(t, cfg.Alerts, 1)
}

func TestLoadMissingFile(t *testing.T) {
	_, err := Load("/nonexistent")
	assert.Error(t, err)
}

func TestLoadInvalidYAML(t *testing.T) {
	dir := t.TempDir()
	err := os.WriteFile(filepath.Join(dir, "interlock.yaml"), []byte("invalid: [yaml"), 0o644)
	require.NoError(t, err)

	_, err = Load(dir)
	assert.Error(t, err)
}

func TestValidation_MissingProvider(t *testing.T) {
	dir := t.TempDir()
	content := `archetypeDirs: [./a]
evaluatorDirs: [./e]
pipelineDirs: [./p]
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "interlock.yaml"), []byte(content), 0o644))

	_, err := Load(dir)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "provider is required")
}

func TestValidation_MissingRedisConfig(t *testing.T) {
	dir := t.TempDir()
	content := `provider: redis
archetypeDirs: [./a]
evaluatorDirs: [./e]
pipelineDirs: [./p]
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "interlock.yaml"), []byte(content), 0o644))

	_, err := Load(dir)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "redis config is required")
}

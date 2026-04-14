package config_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/dwsmith1983/interlock/internal/config"
)

func TestLoadConfig_ValidationRejectsInvalidValues(t *testing.T) {
	t.Skip("not yet implemented")
	os.Setenv("STAGE_TIMEOUT", "50ms") // min is 100ms
	defer os.Clearenv()

	_, err := config.Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "min=100ms")
}

func TestLoadConfig_AppliesDefaults(t *testing.T) {
	t.Skip("not yet implemented")
	cfg, err := config.Load()
	require.NoError(t, err)
	assert.NotZero(t, cfg.Timeouts.StageDefault)
}

func TestLoadConfig_EnvOverridesWork(t *testing.T) {
	t.Skip("not yet implemented")
	os.Setenv("WORKERS_PER_STAGE", "50")
	defer os.Clearenv()

	cfg, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, 50, cfg.Workers.PerStage)
}

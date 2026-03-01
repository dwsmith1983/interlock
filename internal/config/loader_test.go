package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dwsmith1983/interlock/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const validYAML = `
pipeline:
  id: pipe-a
  owner: team-a
schedule:
  cron: "0 7 * * *"
  evaluation:
    window: 1h
    interval: 5m
validation:
  trigger: "ALL"
  rules:
    - key: SENSOR#complete
      check: exists
job:
  type: glue
  config:
    jobName: pipe-a-etl
`

func TestLoadPipelines(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pipe-a.yaml"), []byte(validYAML), 0o644))

	pipelines, err := config.LoadPipelines(dir)
	require.NoError(t, err)
	require.Len(t, pipelines, 1)
	assert.Equal(t, "pipe-a", pipelines[0].Pipeline.ID)
	assert.Equal(t, "team-a", pipelines[0].Pipeline.Owner)
	assert.Len(t, pipelines[0].Validation.Rules, 1)
}

func TestLoadPipelines_EmptyDir(t *testing.T) {
	dir := t.TempDir()
	pipelines, err := config.LoadPipelines(dir)
	require.NoError(t, err)
	assert.Empty(t, pipelines)
}

func TestLoadPipelines_InvalidYAML(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "bad.yaml"), []byte("{{invalid"), 0o644))
	_, err := config.LoadPipelines(dir)
	assert.Error(t, err)
}

func TestLoadPipelines_MissingPipelineID(t *testing.T) {
	dir := t.TempDir()
	yaml1 := `
pipeline:
  owner: team-a
schedule:
  evaluation:
    window: 1h
    interval: 5m
validation:
  trigger: "ALL"
  rules: []
job:
  type: glue
  config:
    jobName: test
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "bad.yaml"), []byte(yaml1), 0o644))
	_, err := config.LoadPipelines(dir)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pipeline ID")
}

func TestLoadPipelines_MultipleFiles(t *testing.T) {
	dir := t.TempDir()
	yamlA := `
pipeline:
  id: pipe-a
  owner: team-a
schedule:
  cron: "0 7 * * *"
  evaluation:
    window: 1h
    interval: 5m
validation:
  trigger: "ALL"
  rules:
    - key: SENSOR#a-complete
      check: exists
job:
  type: glue
  config:
    jobName: pipe-a-etl
`
	yamlB := `
pipeline:
  id: pipe-b
  owner: team-b
schedule:
  cron: "0 9 * * *"
  evaluation:
    window: 2h
    interval: 10m
validation:
  trigger: "ANY"
  rules:
    - key: SENSOR#b-ready
      check: exists
    - key: SENSOR#b-complete
      check: equals
      field: status
      value: done
job:
  type: http
  config:
    url: https://example.com/trigger
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pipe-a.yaml"), []byte(yamlA), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pipe-b.yaml"), []byte(yamlB), 0o644))

	pipelines, err := config.LoadPipelines(dir)
	require.NoError(t, err)
	require.Len(t, pipelines, 2)

	// Files are read via os.ReadDir which returns entries sorted by name.
	assert.Equal(t, "pipe-a", pipelines[0].Pipeline.ID)
	assert.Equal(t, "pipe-b", pipelines[1].Pipeline.ID)
	assert.Len(t, pipelines[1].Validation.Rules, 2)
}

func TestLoadPipelines_IgnoresNonYAMLFiles(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pipe-a.yaml"), []byte(validYAML), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "README.md"), []byte("# docs"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("some notes"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "data.json"), []byte("{}"), 0o644))

	pipelines, err := config.LoadPipelines(dir)
	require.NoError(t, err)
	require.Len(t, pipelines, 1)
	assert.Equal(t, "pipe-a", pipelines[0].Pipeline.ID)
}

func TestLoadPipelines_YMLExtension(t *testing.T) {
	dir := t.TempDir()
	yml := `
pipeline:
  id: pipe-yml
  owner: team-yml
schedule:
  evaluation:
    window: 30m
    interval: 5m
validation:
  trigger: "ALL"
  rules:
    - key: SENSOR#ready
      check: exists
job:
  type: command
  config:
    command: echo hello
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pipe.yml"), []byte(yml), 0o644))

	pipelines, err := config.LoadPipelines(dir)
	require.NoError(t, err)
	require.Len(t, pipelines, 1)
	assert.Equal(t, "pipe-yml", pipelines[0].Pipeline.ID)
}

func TestLoadPipeline_SingleFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "single.yaml")
	require.NoError(t, os.WriteFile(path, []byte(validYAML), 0o644))

	cfg, err := config.LoadPipeline(path)
	require.NoError(t, err)
	assert.Equal(t, "pipe-a", cfg.Pipeline.ID)
	assert.Equal(t, "team-a", cfg.Pipeline.Owner)
	assert.Equal(t, "0 7 * * *", cfg.Schedule.Cron)
	assert.Equal(t, "1h", cfg.Schedule.Evaluation.Window)
	assert.Equal(t, "5m", cfg.Schedule.Evaluation.Interval)
	assert.Equal(t, "ALL", cfg.Validation.Trigger)
	assert.Len(t, cfg.Validation.Rules, 1)
	assert.Equal(t, "glue", string(cfg.Job.Type))
}

func TestLoadPipeline_FileNotFound(t *testing.T) {
	_, err := config.LoadPipeline("/nonexistent/path/pipeline.yaml")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "reading file")
}

func TestLoadPipelines_SkipsDirectories(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pipe-a.yaml"), []byte(validYAML), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "subdir.yaml"), 0o755))

	pipelines, err := config.LoadPipelines(dir)
	require.NoError(t, err)
	require.Len(t, pipelines, 1)
	assert.Equal(t, "pipe-a", pipelines[0].Pipeline.ID)
}

func TestLoadPipelines_NonexistentDir(t *testing.T) {
	_, err := config.LoadPipelines("/nonexistent/dir")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "reading pipelines dir")
}

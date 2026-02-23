package lambda

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// clearInitEnv unsets all env vars read by Init so each subtest starts clean.
func clearInitEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"TABLE_NAME", "AWS_REGION",
		"SNS_TOPIC_ARN", "S3_ALERT_BUCKET", "S3_ALERT_PREFIX",
		"EVALUATOR_BASE_URL", "ARCHETYPE_DIR",
		"READINESS_TTL", "RETENTION_TTL",
	} {
		t.Setenv(key, "")
	}
}

func TestInit_MissingTableName(t *testing.T) {
	clearInitEnv(t)
	t.Setenv("AWS_REGION", "us-east-1")

	_, err := Init(t.Context())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "TABLE_NAME")
}

func TestInit_MissingRegion(t *testing.T) {
	clearInitEnv(t)
	t.Setenv("TABLE_NAME", "interlock-test")

	_, err := Init(t.Context())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "AWS_REGION")
}

func TestInit_BothRequired_Missing(t *testing.T) {
	clearInitEnv(t)

	_, err := Init(t.Context())
	require.Error(t, err)
	// TABLE_NAME is checked first
	assert.Contains(t, err.Error(), "TABLE_NAME")
}

func TestInit_Success_MinimalEnv(t *testing.T) {
	clearInitEnv(t)
	t.Setenv("TABLE_NAME", "test-table")
	t.Setenv("AWS_REGION", "us-west-2")
	// Point archetype dir to a nonexistent path so the stat check skips loading
	t.Setenv("ARCHETYPE_DIR", filepath.Join(t.TempDir(), "no-such-dir"))

	deps, err := Init(t.Context())
	require.NoError(t, err)
	require.NotNil(t, deps)

	assert.NotNil(t, deps.Provider, "Provider should be initialized")
	assert.NotNil(t, deps.Engine, "Engine should be initialized")
	assert.NotNil(t, deps.Runner, "Runner should be initialized")
	assert.NotNil(t, deps.ArchetypeReg, "ArchetypeReg should be initialized")
	assert.NotNil(t, deps.AlertFn, "AlertFn should be initialized")
	assert.NotNil(t, deps.Logger, "Logger should be initialized")
}

func TestInit_Success_WithSNSTopicARN(t *testing.T) {
	clearInitEnv(t)
	t.Setenv("TABLE_NAME", "test-table")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:123456789012:test-topic")
	t.Setenv("ARCHETYPE_DIR", filepath.Join(t.TempDir(), "no-such-dir"))

	deps, err := Init(t.Context())
	require.NoError(t, err)
	require.NotNil(t, deps)
	assert.NotNil(t, deps.AlertFn)
}

func TestInit_Success_WithS3AlertBucket(t *testing.T) {
	clearInitEnv(t)
	t.Setenv("TABLE_NAME", "test-table")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("S3_ALERT_BUCKET", "my-alert-bucket")
	t.Setenv("ARCHETYPE_DIR", filepath.Join(t.TempDir(), "no-such-dir"))

	deps, err := Init(t.Context())
	require.NoError(t, err)
	require.NotNil(t, deps)
	assert.NotNil(t, deps.AlertFn)
}

func TestInit_Success_WithS3AlertBucketAndPrefix(t *testing.T) {
	clearInitEnv(t)
	t.Setenv("TABLE_NAME", "test-table")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("S3_ALERT_BUCKET", "my-alert-bucket")
	t.Setenv("S3_ALERT_PREFIX", "custom-prefix")
	t.Setenv("ARCHETYPE_DIR", filepath.Join(t.TempDir(), "no-such-dir"))

	deps, err := Init(t.Context())
	require.NoError(t, err)
	require.NotNil(t, deps)
}

func TestInit_Success_WithEvaluatorBaseURL(t *testing.T) {
	clearInitEnv(t)
	t.Setenv("TABLE_NAME", "test-table")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("EVALUATOR_BASE_URL", "https://evaluator.example.com")
	t.Setenv("ARCHETYPE_DIR", filepath.Join(t.TempDir(), "no-such-dir"))

	deps, err := Init(t.Context())
	require.NoError(t, err)
	require.NotNil(t, deps)
}

func TestInit_Success_AllOptionalEnvVars(t *testing.T) {
	clearInitEnv(t)
	t.Setenv("TABLE_NAME", "test-table")
	t.Setenv("AWS_REGION", "eu-west-1")
	t.Setenv("SNS_TOPIC_ARN", "arn:aws:sns:eu-west-1:123456789012:alerts")
	t.Setenv("S3_ALERT_BUCKET", "alerts-bucket")
	t.Setenv("S3_ALERT_PREFIX", "interlock-alerts")
	t.Setenv("EVALUATOR_BASE_URL", "https://eval.example.com")
	t.Setenv("READINESS_TTL", "10m")
	t.Setenv("RETENTION_TTL", "720h")
	t.Setenv("ARCHETYPE_DIR", filepath.Join(t.TempDir(), "no-such-dir"))

	deps, err := Init(t.Context())
	require.NoError(t, err)
	require.NotNil(t, deps)

	assert.NotNil(t, deps.Provider)
	assert.NotNil(t, deps.Engine)
	assert.NotNil(t, deps.Runner)
	assert.NotNil(t, deps.ArchetypeReg)
	assert.NotNil(t, deps.AlertFn)
	assert.NotNil(t, deps.Logger)
}

func TestInit_ArchetypeDir_ValidYAML(t *testing.T) {
	clearInitEnv(t)
	t.Setenv("TABLE_NAME", "test-table")
	t.Setenv("AWS_REGION", "us-east-1")

	// Create a temp dir with a valid archetype YAML file
	dir := t.TempDir()
	yamlContent := `name: test-archetype
requiredTraits:
  - type: freshness
    description: "Data is fresh"
    defaultTtl: 120
    defaultTimeout: 10
readinessRule:
  type: all-required-pass
`
	err := os.WriteFile(filepath.Join(dir, "test.yaml"), []byte(yamlContent), 0o644)
	require.NoError(t, err)

	t.Setenv("ARCHETYPE_DIR", dir)

	deps, err := Init(t.Context())
	require.NoError(t, err)
	require.NotNil(t, deps)
	require.NotNil(t, deps.ArchetypeReg)

	// Verify the archetype was actually loaded
	arch, archErr := deps.ArchetypeReg.Get("test-archetype")
	assert.NoError(t, archErr)
	assert.NotNil(t, arch)
	assert.Equal(t, "test-archetype", arch.Name)
}

func TestInit_ArchetypeDir_MultipleFiles(t *testing.T) {
	clearInitEnv(t)
	t.Setenv("TABLE_NAME", "test-table")
	t.Setenv("AWS_REGION", "us-east-1")

	dir := t.TempDir()
	yaml1 := `name: archetype-one
requiredTraits:
  - type: freshness
    description: "Data is fresh"
readinessRule:
  type: all-required-pass
`
	yaml2 := `name: archetype-two
requiredTraits:
  - type: availability
    description: "Resource available"
readinessRule:
  type: all-required-pass
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "one.yaml"), []byte(yaml1), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "two.yml"), []byte(yaml2), 0o644))

	t.Setenv("ARCHETYPE_DIR", dir)

	deps, err := Init(t.Context())
	require.NoError(t, err)

	a1, err := deps.ArchetypeReg.Get("archetype-one")
	assert.NoError(t, err)
	assert.NotNil(t, a1)

	a2, err := deps.ArchetypeReg.Get("archetype-two")
	assert.NoError(t, err)
	assert.NotNil(t, a2)
}

func TestInit_ArchetypeDir_InvalidYAML(t *testing.T) {
	clearInitEnv(t)
	t.Setenv("TABLE_NAME", "test-table")
	t.Setenv("AWS_REGION", "us-east-1")

	dir := t.TempDir()
	// Write invalid YAML: valid YAML but invalid archetype (missing name + required traits)
	err := os.WriteFile(filepath.Join(dir, "bad.yaml"), []byte("invalid: true\n"), 0o644)
	require.NoError(t, err)

	t.Setenv("ARCHETYPE_DIR", dir)

	_, err = Init(t.Context())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "loading archetypes")
}

func TestInit_ArchetypeDir_NonexistentPath(t *testing.T) {
	clearInitEnv(t)
	t.Setenv("TABLE_NAME", "test-table")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("ARCHETYPE_DIR", "/tmp/absolutely-does-not-exist-interlock-test")

	deps, err := Init(t.Context())
	require.NoError(t, err, "nonexistent archetype dir should not cause an error")
	require.NotNil(t, deps)
	assert.NotNil(t, deps.ArchetypeReg, "registry should still be initialized (empty)")
}

func TestInit_ArchetypeDir_DefaultPath(t *testing.T) {
	clearInitEnv(t)
	t.Setenv("TABLE_NAME", "test-table")
	t.Setenv("AWS_REGION", "us-east-1")
	// Do not set ARCHETYPE_DIR — defaults to /var/task/archetypes which should not exist locally

	deps, err := Init(t.Context())
	require.NoError(t, err)
	require.NotNil(t, deps)
	assert.NotNil(t, deps.ArchetypeReg)
}

func TestInit_ArchetypeDir_EmptyDir(t *testing.T) {
	clearInitEnv(t)
	t.Setenv("TABLE_NAME", "test-table")
	t.Setenv("AWS_REGION", "us-east-1")

	// Empty directory — no YAML files to load
	dir := t.TempDir()
	t.Setenv("ARCHETYPE_DIR", dir)

	deps, err := Init(t.Context())
	require.NoError(t, err)
	require.NotNil(t, deps)
	// Registry exists but is empty
	assert.Empty(t, deps.ArchetypeReg.List())
}

func TestInit_ArchetypeDir_IsFile(t *testing.T) {
	clearInitEnv(t)
	t.Setenv("TABLE_NAME", "test-table")
	t.Setenv("AWS_REGION", "us-east-1")

	// Point ARCHETYPE_DIR at a file, not a directory — os.Stat succeeds but IsDir() is false
	f := filepath.Join(t.TempDir(), "not-a-dir.txt")
	require.NoError(t, os.WriteFile(f, []byte("hello"), 0o644))
	t.Setenv("ARCHETYPE_DIR", f)

	deps, err := Init(t.Context())
	require.NoError(t, err, "file instead of dir should be skipped gracefully")
	require.NotNil(t, deps)
	assert.NotNil(t, deps.ArchetypeReg)
}

func TestInit_CustomTTLValues(t *testing.T) {
	clearInitEnv(t)
	t.Setenv("TABLE_NAME", "test-table")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("READINESS_TTL", "30m")
	t.Setenv("RETENTION_TTL", "336h")
	t.Setenv("ARCHETYPE_DIR", filepath.Join(t.TempDir(), "no-such-dir"))

	deps, err := Init(t.Context())
	require.NoError(t, err)
	require.NotNil(t, deps)
	// Provider was created successfully with custom TTLs
	assert.NotNil(t, deps.Provider)
}

func TestEnvOrDefault(t *testing.T) {
	t.Run("env set", func(t *testing.T) {
		t.Setenv("TEST_KEY_INIT", "custom")
		assert.Equal(t, "custom", envOrDefault("TEST_KEY_INIT", "fallback"))
	})

	t.Run("env empty", func(t *testing.T) {
		t.Setenv("TEST_KEY_INIT", "")
		assert.Equal(t, "fallback", envOrDefault("TEST_KEY_INIT", "fallback"))
	})

	t.Run("env unset", func(t *testing.T) {
		// Use a key that is very unlikely to be set
		assert.Equal(t, "default-val", envOrDefault("INTERLOCK_NONEXISTENT_KEY_12345", "default-val"))
	})
}

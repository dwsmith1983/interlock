package lambda

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInit_MissingTableName(t *testing.T) {
	t.Setenv("TABLE_NAME", "")
	t.Setenv("AWS_REGION", "us-east-1")

	_, err := Init(t.Context())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "TABLE_NAME")
}

func TestInit_MissingRegion(t *testing.T) {
	t.Setenv("TABLE_NAME", "interlock-test")
	t.Setenv("AWS_REGION", "")

	_, err := Init(t.Context())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "AWS_REGION")
}

func TestEnvOrDefault(t *testing.T) {
	t.Setenv("TEST_KEY", "custom")
	assert.Equal(t, "custom", envOrDefault("TEST_KEY", "fallback"))

	t.Setenv("TEST_KEY", "")
	assert.Equal(t, "fallback", envOrDefault("TEST_KEY", "fallback"))
}

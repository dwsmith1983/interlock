package trigger

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRunCheckState_Constants(t *testing.T) {
	assert.Equal(t, RunCheckState("running"), RunCheckRunning)
	assert.Equal(t, RunCheckState("succeeded"), RunCheckSucceeded)
	assert.Equal(t, RunCheckState("failed"), RunCheckFailed)
}

func TestStatusResult_ConstructAndCompare(t *testing.T) {
	result := StatusResult{
		State:   RunCheckSucceeded,
		Message: "job completed",
	}
	assert.Equal(t, RunCheckSucceeded, result.State)
	assert.Equal(t, "job completed", result.Message)
	assert.Empty(t, result.FailureCategory)
}

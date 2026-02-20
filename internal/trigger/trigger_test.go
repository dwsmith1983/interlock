package trigger

import (
	"fmt"
	"testing"

	"github.com/interlock-systems/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestClassifyFailure_Timeout(t *testing.T) {
	err := fmt.Errorf("context deadline exceeded")
	assert.Equal(t, types.FailureTimeout, ClassifyFailure(err))
}

func TestClassifyFailure_HTTP4xx(t *testing.T) {
	err := fmt.Errorf("trigger returned status 400: bad request")
	assert.Equal(t, types.FailurePermanent, ClassifyFailure(err))

	err = fmt.Errorf("trigger returned status 404: not found")
	assert.Equal(t, types.FailurePermanent, ClassifyFailure(err))
}

func TestClassifyFailure_HTTP5xx(t *testing.T) {
	err := fmt.Errorf("trigger returned status 500: internal error")
	assert.Equal(t, types.FailureTransient, ClassifyFailure(err))
}

func TestClassifyFailure_NetworkError(t *testing.T) {
	err := fmt.Errorf("connection refused")
	assert.Equal(t, types.FailureTransient, ClassifyFailure(err))
}

func TestClassifyFailure_Nil(t *testing.T) {
	assert.Equal(t, types.FailureCategory(""), ClassifyFailure(nil))
}

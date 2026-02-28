package trigger

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecuteCommand_Success(t *testing.T) {
	cfg := &types.TriggerConfig{
		Type:    types.TriggerCommand,
		Command: &types.CommandTriggerConfig{Command: "echo ok"},
	}
	_, err := Execute(context.Background(), cfg)
	require.NoError(t, err)
}

func TestExecuteCommand_Failure(t *testing.T) {
	cfg := &types.TriggerConfig{
		Type:    types.TriggerCommand,
		Command: &types.CommandTriggerConfig{Command: "false"},
	}
	_, err := Execute(context.Background(), cfg)
	assert.Error(t, err)
}

func TestExecuteHTTP_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := &types.TriggerConfig{
		Type: types.TriggerHTTP,
		HTTP: &types.HTTPTriggerConfig{Method: "POST", URL: srv.URL},
	}
	_, err := Execute(context.Background(), cfg)
	require.NoError(t, err)
}

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

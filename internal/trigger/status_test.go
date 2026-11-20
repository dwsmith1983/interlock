package trigger

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// TestCheckAirflowStatus_SendsExpandedHeaders: the runner's airflow poll goes
// through the runner's client and sends the configured headers with their
// INTERLOCK_ variables expanded.
func TestCheckAirflowStatus_SendsExpandedHeaders(t *testing.T) {
	t.Setenv("INTERLOCK_TOKEN", "tok-123")
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"state": "success"})
	}))
	t.Cleanup(srv.Close)

	r := NewRunner(WithHTTPClient(srv.Client()))
	res, err := r.CheckStatus(context.Background(), types.TriggerAirflow, map[string]interface{}{
		"airflow_url": srv.URL, "airflow_dag_id": "etl", "airflow_dag_run_id": "r1",
	}, map[string]string{"Authorization": "Bearer ${INTERLOCK_TOKEN}"})

	require.NoError(t, err)
	assert.Equal(t, RunCheckSucceeded, res.State)
	assert.Equal(t, "Bearer tok-123", gotAuth)
}

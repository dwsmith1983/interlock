package evaluator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPRunner_Success(t *testing.T) {
	expected := types.EvaluatorOutput{
		Status: types.TraitPass,
		Value:  map[string]interface{}{"lag": float64(10)},
		Reason: "within threshold",
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		var input types.EvaluatorInput
		require.NoError(t, json.NewDecoder(r.Body).Decode(&input))
		assert.Equal(t, "test-pipe", input.PipelineID)
		assert.Equal(t, "freshness", input.TraitType)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(expected)
	}))
	defer srv.Close()

	runner := NewHTTPRunner("")
	input := types.EvaluatorInput{
		PipelineID: "test-pipe",
		TraitType:  "freshness",
		Config:     map[string]interface{}{"maxLag": 60},
	}

	result, err := runner.Run(context.Background(), srv.URL, input, 5*time.Second)
	require.NoError(t, err)
	assert.Equal(t, types.TraitPass, result.Status)
	assert.Equal(t, "within threshold", result.Reason)
}

func TestHTTPRunner_BaseURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/evaluate/freshness", r.URL.Path)
		json.NewEncoder(w).Encode(types.EvaluatorOutput{Status: types.TraitPass})
	}))
	defer srv.Close()

	runner := NewHTTPRunner(srv.URL)
	result, err := runner.Run(context.Background(), "evaluate/freshness", types.EvaluatorInput{}, 5*time.Second)
	require.NoError(t, err)
	assert.Equal(t, types.TraitPass, result.Status)
}

func TestHTTPRunner_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))
	defer srv.Close()

	runner := NewHTTPRunner("")
	result, err := runner.Run(context.Background(), srv.URL, types.EvaluatorInput{}, 5*time.Second)
	require.NoError(t, err)
	assert.Equal(t, types.TraitFail, result.Status)
	assert.Contains(t, result.Reason, "EVALUATOR_HTTP_ERROR")
	assert.Equal(t, types.FailureTransient, result.FailureCategory)
}

func TestHTTPRunner_ClientError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer srv.Close()

	runner := NewHTTPRunner("")
	result, err := runner.Run(context.Background(), srv.URL, types.EvaluatorInput{}, 5*time.Second)
	require.NoError(t, err)
	assert.Equal(t, types.TraitFail, result.Status)
	assert.Equal(t, types.FailurePermanent, result.FailureCategory)
}

func TestHTTPRunner_InvalidJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("not json"))
	}))
	defer srv.Close()

	runner := NewHTTPRunner("")
	result, err := runner.Run(context.Background(), srv.URL, types.EvaluatorInput{}, 5*time.Second)
	require.NoError(t, err)
	assert.Equal(t, types.TraitFail, result.Status)
	assert.Contains(t, result.Reason, "EVALUATOR_OUTPUT_INVALID")
	assert.Equal(t, types.FailurePermanent, result.FailureCategory)
}

func TestHTTPRunner_NoBaseURL(t *testing.T) {
	runner := NewHTTPRunner("")
	_, err := runner.Run(context.Background(), "relative/path", types.EvaluatorInput{}, 5*time.Second)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not a URL and no base URL")
}

func TestHTTPRunner_Timeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		json.NewEncoder(w).Encode(types.EvaluatorOutput{Status: types.TraitPass})
	}))
	defer srv.Close()

	runner := NewHTTPRunner("")
	result, err := runner.Run(context.Background(), srv.URL, types.EvaluatorInput{}, 50*time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, types.TraitFail, result.Status)
	assert.Equal(t, "EVALUATOR_TIMEOUT", result.Reason)
	assert.Equal(t, types.FailureTimeout, result.FailureCategory)
}

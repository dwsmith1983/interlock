package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dwsmith1983/interlock/internal/engine"
	"github.com/dwsmith1983/interlock/internal/evaluator"
	intlambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/testutil"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupDeps(t *testing.T, evaluatorURL string) *intlambda.Deps {
	t.Helper()
	prov := testutil.NewMockProvider()
	runner := evaluator.NewHTTPRunner(evaluatorURL)
	eng := engine.New(prov, nil, runner, nil)
	return &intlambda.Deps{
		Provider: prov,
		Engine:   eng,
		Logger:   slog.Default(),
	}
}

func TestHandleEvaluate_Pass(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(types.EvaluatorOutput{
			Status: types.TraitPass,
			Value:  map[string]interface{}{"lag": float64(5)},
			Reason: "within threshold",
		})
	}))
	defer srv.Close()

	d := setupDeps(t, srv.URL)

	resp, err := handleEvaluate(context.Background(), d, intlambda.EvaluatorRequest{
		PipelineID: "test-pipe",
		TraitType:  "freshness",
		Evaluator:  srv.URL + "/evaluate",
		Config:     map[string]interface{}{"maxLag": 60},
		Timeout:    5,
		TTL:        120,
		Required:   true,
	})
	require.NoError(t, err)
	assert.Equal(t, types.TraitPass, resp.Status)
	assert.Equal(t, "freshness", resp.TraitType)
	assert.Equal(t, "within threshold", resp.Reason)
	assert.True(t, resp.Required)
}

func TestHandleEvaluate_Fail(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(types.EvaluatorOutput{
			Status: types.TraitFail,
			Reason: "lag exceeded threshold",
		})
	}))
	defer srv.Close()

	d := setupDeps(t, srv.URL)

	resp, err := handleEvaluate(context.Background(), d, intlambda.EvaluatorRequest{
		PipelineID: "test-pipe",
		TraitType:  "freshness",
		Evaluator:  srv.URL + "/evaluate",
		Timeout:    5,
		Required:   true,
	})
	require.NoError(t, err)
	assert.Equal(t, types.TraitFail, resp.Status)
	assert.Equal(t, "lag exceeded threshold", resp.Reason)
}

func TestHandleEvaluate_NoEvaluator(t *testing.T) {
	d := setupDeps(t, "")

	resp, err := handleEvaluate(context.Background(), d, intlambda.EvaluatorRequest{
		PipelineID: "test-pipe",
		TraitType:  "freshness",
		Evaluator:  "",
		Required:   true,
	})
	require.NoError(t, err)
	assert.Equal(t, types.TraitError, resp.Status)
	assert.Equal(t, "no evaluator configured", resp.Reason)
}

func TestHandleEvaluate_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))
	defer srv.Close()

	d := setupDeps(t, srv.URL)

	resp, err := handleEvaluate(context.Background(), d, intlambda.EvaluatorRequest{
		PipelineID: "test-pipe",
		TraitType:  "freshness",
		Evaluator:  srv.URL + "/evaluate",
		Timeout:    5,
		Required:   true,
	})
	require.NoError(t, err)
	assert.Equal(t, types.TraitError, resp.Status)
	assert.Contains(t, resp.Reason, "EVALUATOR_HTTP_ERROR")
}

func TestHandleEvaluate_FullHTTPURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var input types.EvaluatorInput
		_ = json.NewDecoder(r.Body).Decode(&input)

		assert.Equal(t, "pipeline-a", input.PipelineID)
		assert.Equal(t, "record-count", input.TraitType)
		assert.Equal(t, float64(100), input.Config["minRecords"])

		_ = json.NewEncoder(w).Encode(types.EvaluatorOutput{
			Status: types.TraitPass,
			Value:  map[string]interface{}{"count": float64(250)},
			Reason: "record count sufficient",
		})
	}))
	defer srv.Close()

	d := setupDeps(t, srv.URL)

	resp, err := handleEvaluate(context.Background(), d, intlambda.EvaluatorRequest{
		PipelineID: "pipeline-a",
		TraitType:  "record-count",
		Evaluator:  srv.URL + "/check-records",
		Config:     map[string]interface{}{"minRecords": float64(100)},
		Timeout:    10,
		TTL:        300,
		Required:   true,
	})
	require.NoError(t, err)
	assert.Equal(t, types.TraitPass, resp.Status)
	assert.Equal(t, "record-count", resp.TraitType)
	assert.Equal(t, "record count sufficient", resp.Reason)
	assert.True(t, resp.Required)
	assert.NotNil(t, resp.Value)
}

func TestHandleEvaluate_Timeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Sleep longer than the timeout to trigger deadline exceeded
		time.Sleep(3 * time.Second)
		_ = json.NewEncoder(w).Encode(types.EvaluatorOutput{
			Status: types.TraitPass,
		})
	}))
	defer srv.Close()

	d := setupDeps(t, srv.URL)

	resp, err := handleEvaluate(context.Background(), d, intlambda.EvaluatorRequest{
		PipelineID: "test-pipe",
		TraitType:  "slow-trait",
		Evaluator:  srv.URL + "/slow",
		Timeout:    1, // 1 second timeout — will be exceeded
		Required:   true,
	})
	require.NoError(t, err)
	// The HTTPRunner returns EVALUATOR_TIMEOUT on deadline exceeded
	assert.Equal(t, types.TraitError, resp.Status)
	assert.Contains(t, resp.Reason, "EVALUATOR_TIMEOUT")
}

func TestHandleEvaluate_ConfigParsing(t *testing.T) {
	var receivedConfig map[string]interface{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var input types.EvaluatorInput
		_ = json.NewDecoder(r.Body).Decode(&input)
		receivedConfig = input.Config

		_ = json.NewEncoder(w).Encode(types.EvaluatorOutput{
			Status: types.TraitPass,
			Reason: "config verified",
		})
	}))
	defer srv.Close()

	d := setupDeps(t, srv.URL)

	config := map[string]interface{}{
		"maxLag":     float64(60),
		"source":     "s3://my-bucket/data",
		"thresholds": map[string]interface{}{"warn": float64(30), "crit": float64(60)},
	}

	resp, err := handleEvaluate(context.Background(), d, intlambda.EvaluatorRequest{
		PipelineID: "test-pipe",
		TraitType:  "freshness",
		Evaluator:  srv.URL + "/evaluate",
		Config:     config,
		Timeout:    5,
		Required:   false,
	})
	require.NoError(t, err)
	assert.Equal(t, types.TraitPass, resp.Status)
	assert.False(t, resp.Required)

	// Verify config was passed through correctly
	assert.Equal(t, float64(60), receivedConfig["maxLag"])
	assert.Equal(t, "s3://my-bucket/data", receivedConfig["source"])
}

func TestHandleEvaluate_InvalidEvaluatorPath(t *testing.T) {
	// HTTPRunner with empty baseURL rejects non-URL paths
	d := setupDeps(t, "")

	resp, err := handleEvaluate(context.Background(), d, intlambda.EvaluatorRequest{
		PipelineID: "test-pipe",
		TraitType:  "freshness",
		Evaluator:  "not-a-url",
		Timeout:    5,
		Required:   true,
	})
	require.NoError(t, err)
	// EvaluateTrait returns the error as FAIL status
	assert.Equal(t, types.TraitError, resp.Status)
	assert.Contains(t, resp.Reason, "not a URL")
}

func TestHandleEvaluate_OptionalTraitFail(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(types.EvaluatorOutput{
			Status: types.TraitFail,
			Reason: "optional check failed",
		})
	}))
	defer srv.Close()

	d := setupDeps(t, srv.URL)

	resp, err := handleEvaluate(context.Background(), d, intlambda.EvaluatorRequest{
		PipelineID: "test-pipe",
		TraitType:  "schema-check",
		Evaluator:  srv.URL + "/evaluate",
		Timeout:    5,
		Required:   false,
	})
	require.NoError(t, err)
	assert.Equal(t, types.TraitFail, resp.Status)
	assert.Equal(t, "optional check failed", resp.Reason)
	assert.False(t, resp.Required)
}

func TestHandleEvaluate_InvalidJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("not valid json"))
	}))
	defer srv.Close()

	d := setupDeps(t, srv.URL)

	resp, err := handleEvaluate(context.Background(), d, intlambda.EvaluatorRequest{
		PipelineID: "test-pipe",
		TraitType:  "freshness",
		Evaluator:  srv.URL + "/evaluate",
		Timeout:    5,
		Required:   true,
	})
	require.NoError(t, err)
	assert.Equal(t, types.TraitError, resp.Status)
	assert.Contains(t, resp.Reason, "EVALUATOR_OUTPUT_INVALID")
}

func TestHandleEvaluate_TTLSet(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(types.EvaluatorOutput{
			Status: types.TraitPass,
			Value:  map[string]interface{}{"rows": float64(1000)},
			Reason: "all good",
		})
	}))
	defer srv.Close()

	d := setupDeps(t, srv.URL)

	resp, err := handleEvaluate(context.Background(), d, intlambda.EvaluatorRequest{
		PipelineID: "test-pipe",
		TraitType:  "record-count",
		Evaluator:  srv.URL + "/evaluate",
		Config:     map[string]interface{}{"minRecords": float64(500)},
		Timeout:    5,
		TTL:        600,
		Required:   true,
	})
	require.NoError(t, err)
	assert.Equal(t, types.TraitPass, resp.Status)
	assert.Equal(t, "record-count", resp.TraitType)
	assert.Equal(t, "all good", resp.Reason)
}

func TestHandleEvaluate_NoTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(types.EvaluatorOutput{
			Status: types.TraitPass,
			Reason: "ok",
		})
	}))
	defer srv.Close()

	d := setupDeps(t, srv.URL)

	// Timeout=0 means engine uses its default timeout (30s)
	resp, err := handleEvaluate(context.Background(), d, intlambda.EvaluatorRequest{
		PipelineID: "test-pipe",
		TraitType:  "freshness",
		Evaluator:  srv.URL + "/evaluate",
		Timeout:    0,
		Required:   true,
	})
	require.NoError(t, err)
	assert.Equal(t, types.TraitPass, resp.Status)
}

func TestHandleEvaluate_ClientError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer srv.Close()

	d := setupDeps(t, srv.URL)

	resp, err := handleEvaluate(context.Background(), d, intlambda.EvaluatorRequest{
		PipelineID: "test-pipe",
		TraitType:  "freshness",
		Evaluator:  srv.URL + "/evaluate",
		Timeout:    5,
		Required:   true,
	})
	require.NoError(t, err)
	assert.Equal(t, types.TraitError, resp.Status)
	assert.Contains(t, resp.Reason, "EVALUATOR_HTTP_ERROR")
}

// TestHandler_InitError exercises the top-level handler function.
// getDeps() calls intlambda.Init() which requires TABLE_NAME/AWS_REGION env vars.
// Without those vars Init fails, so handler() returns the init error.
func TestHandler_InitError(t *testing.T) {
	t.Setenv("TABLE_NAME", "")
	t.Setenv("AWS_REGION", "")

	_, err := handler(context.Background(), intlambda.EvaluatorRequest{
		PipelineID: "test-pipe",
		TraitType:  "freshness",
		Evaluator:  "http://example.com/evaluate",
		Required:   true,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TABLE_NAME")
}

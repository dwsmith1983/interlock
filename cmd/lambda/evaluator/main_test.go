package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/interlock-systems/interlock/internal/engine"
	"github.com/interlock-systems/interlock/internal/evaluator"
	intlambda "github.com/interlock-systems/interlock/internal/lambda"
	"github.com/interlock-systems/interlock/internal/testutil"
	"github.com/interlock-systems/interlock/pkg/types"
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
		json.NewEncoder(w).Encode(types.EvaluatorOutput{
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
		json.NewEncoder(w).Encode(types.EvaluatorOutput{
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
	assert.Equal(t, types.TraitFail, resp.Status)
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
	assert.Equal(t, types.TraitFail, resp.Status)
	assert.Contains(t, resp.Reason, "EVALUATOR_HTTP_ERROR")
}

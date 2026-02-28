package trigger

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// ExecuteDatabricks submits a Databricks job run via the REST API.
func ExecuteDatabricks(ctx context.Context, cfg *types.DatabricksTriggerConfig, httpClient *http.Client) (map[string]interface{}, error) {
	if cfg.WorkspaceURL == "" {
		return nil, fmt.Errorf("databricks trigger: workspaceUrl is required")
	}
	if cfg.JobID == "" {
		return nil, fmt.Errorf("databricks trigger: jobId is required")
	}

	url := strings.TrimRight(cfg.WorkspaceURL, "/") + "/api/2.1/jobs/runs/submit"

	payload := map[string]interface{}{
		"run_name": cfg.JobID,
	}
	if len(cfg.Arguments) > 0 {
		payload["notebook_params"] = cfg.Arguments
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("databricks trigger: marshaling payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("databricks trigger: creating request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	for k, v := range cfg.Headers {
		// os.ExpandEnv is intentional: operators store ${VAR} references in
		// pipeline configs, resolved at runtime from the execution environment.
		req.Header.Set(k, os.ExpandEnv(v))
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("databricks trigger: request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("databricks trigger: reading response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("databricks trigger: returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("databricks trigger: parsing response: %w", err)
	}

	runID, _ := result["run_id"].(float64)
	if runID == 0 {
		return nil, fmt.Errorf("databricks trigger: response missing run_id")
	}

	meta := map[string]interface{}{
		"databricks_workspace_url": cfg.WorkspaceURL,
		"databricks_run_id":        fmt.Sprintf("%.0f", runID),
	}
	return meta, nil
}

// checkDatabricksStatus checks the status of a Databricks run via the REST API.
func (r *Runner) checkDatabricksStatus(ctx context.Context, metadata map[string]interface{}, headers map[string]string) (StatusResult, error) {
	workspaceURL, _ := metadata["databricks_workspace_url"].(string)
	runID, _ := metadata["databricks_run_id"].(string)
	if workspaceURL == "" || runID == "" {
		return StatusResult{State: RunCheckRunning, Message: "missing databricks metadata"}, nil
	}

	url := strings.TrimRight(workspaceURL, "/") + "/api/2.1/jobs/runs/get?run_id=" + runID

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return StatusResult{}, fmt.Errorf("databricks status: creating request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		// os.ExpandEnv is intentional — see ExecuteDatabricks comment.
		req.Header.Set(k, os.ExpandEnv(v))
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return StatusResult{}, fmt.Errorf("databricks status: request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return StatusResult{}, fmt.Errorf("databricks status: reading response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return StatusResult{}, fmt.Errorf("databricks status: returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return StatusResult{}, fmt.Errorf("databricks status: parsing response: %w", err)
	}

	stateObj, _ := result["state"].(map[string]interface{})
	lifeCycleState, _ := stateObj["life_cycle_state"].(string)
	resultState, _ := stateObj["result_state"].(string)

	msg := lifeCycleState
	if resultState != "" {
		msg = lifeCycleState + "/" + resultState
	}

	if lifeCycleState == "TERMINATED" {
		if resultState == "SUCCESS" {
			return StatusResult{State: RunCheckSucceeded, Message: msg}, nil
		}
		return StatusResult{State: RunCheckFailed, Message: msg, FailureCategory: types.FailureTransient}, nil
	}

	if lifeCycleState == "SKIPPED" {
		return StatusResult{State: RunCheckFailed, Message: msg, FailureCategory: types.FailurePermanent}, nil
	}

	if lifeCycleState == "INTERNAL_ERROR" {
		return StatusResult{State: RunCheckFailed, Message: msg, FailureCategory: types.FailureTransient}, nil
	}

	return StatusResult{State: RunCheckRunning, Message: msg}, nil
}

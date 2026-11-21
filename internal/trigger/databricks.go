package trigger

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	neturl "net/url"
	"os"
	"strconv"
	"strings"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// runNowRequest is the body of a Jobs API 2.1 run-now call.
type runNowRequest struct {
	JobID            int64             `json:"job_id"`
	IdempotencyToken string            `json:"idempotency_token"`
	JobParameters    map[string]string `json:"job_parameters,omitempty"`
}

// ExecuteDatabricks starts a run of an existing Databricks job with run-now,
// passing the arguments as job parameters. The idempotency token is the hex
// SHA-256 of the run ID (64 characters, the token's limit, while a run ID can
// be longer): a retried start of the same run reuses the token, so Databricks
// returns the run it already started, and a new run starts a new one.
func ExecuteDatabricks(ctx context.Context, cfg *types.DatabricksTriggerConfig, httpClient *http.Client) (map[string]interface{}, error) {
	if cfg.WorkspaceURL == "" {
		return nil, fmt.Errorf("databricks trigger: workspaceUrl is required")
	}
	if cfg.JobID == "" {
		return nil, fmt.Errorf("databricks trigger: jobId is required")
	}

	// The ID stays a string in config: a float64 JSON number would lose
	// precision above 2^53.
	jobID, err := strconv.ParseInt(cfg.JobID, 10, 64)
	if err != nil || jobID <= 0 {
		return nil, fmt.Errorf("databricks trigger: jobId must be a positive integer job ID, got %q", cfg.JobID)
	}
	if cfg.Run.RunID == "" {
		return nil, fmt.Errorf("databricks trigger: run ID is required for the idempotency token")
	}

	url := strings.TrimRight(cfg.WorkspaceURL, "/") + "/api/2.1/jobs/run-now"

	token := sha256.Sum256([]byte(cfg.Run.RunID))
	body, err := json.Marshal(runNowRequest{
		JobID:            jobID,
		IdempotencyToken: hex.EncodeToString(token[:]),
		JobParameters:    cfg.Arguments,
	})
	if err != nil {
		return nil, fmt.Errorf("databricks trigger: marshaling payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("databricks trigger: creating request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	for k, v := range cfg.Headers {
		req.Header.Set(k, os.Expand(v, safeEnvLookup))
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("databricks trigger: request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := readBounded(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("databricks trigger: reading response: %w", err)
	}

	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("databricks trigger: %s", failedStatus(resp, respBody, secretValues(cfg.Headers)))
	}

	// Errors are fixed text: the decoder's message quotes the literal, which
	// can run to the size of the response.
	var result struct {
		RunID json.RawMessage `json:"run_id"`
	}
	if json.Unmarshal(respBody, &result) != nil {
		return nil, fmt.Errorf("databricks trigger: response is not a JSON object")
	}
	if result.RunID == nil || string(result.RunID) == "null" {
		return nil, fmt.Errorf("databricks trigger: response missing run_id")
	}
	var runID int64
	if json.Unmarshal(result.RunID, &runID) != nil || runID <= 0 {
		return nil, fmt.Errorf("databricks trigger: response run_id is not a positive integer")
	}

	meta := map[string]interface{}{
		"databricks_workspace_url": cfg.WorkspaceURL,
		"databricks_run_id":        strconv.FormatInt(runID, 10),
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

	url := strings.TrimRight(workspaceURL, "/") + "/api/2.1/jobs/runs/get?" + neturl.Values{"run_id": {runID}}.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return StatusResult{}, fmt.Errorf("databricks status: creating request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, os.Expand(v, safeEnvLookup))
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return StatusResult{}, fmt.Errorf("databricks status: request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := readBounded(resp.Body)
	if err != nil {
		return StatusResult{}, fmt.Errorf("databricks status: reading response: %w", err)
	}

	if resp.StatusCode >= 300 {
		return StatusResult{}, fmt.Errorf("databricks status: %s", failedStatus(resp, respBody, secretValues(headers)))
	}

	var result map[string]interface{}
	if json.Unmarshal(respBody, &result) != nil {
		// Fixed text: the decoder's message can quote the response.
		return StatusResult{}, fmt.Errorf("databricks status: response is not a JSON object")
	}

	stateObj, _ := result["state"].(map[string]interface{})
	lifeCycleState, _ := stateObj["life_cycle_state"].(string)
	resultState, _ := stateObj["result_state"].(string)
	stateMessage, _ := stateObj["state_message"].(string)

	msg := lifeCycleState
	if resultState != "" {
		msg = lifeCycleState + "/" + resultState
	}
	if stateMessage != "" {
		msg += ": " + stateMessage
	}
	// The message reaches the job log, so it is cleaned like an error body.
	msg = sanitizeBody([]byte(msg), secretValues(headers), maxErrorBodyBytes)

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

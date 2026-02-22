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
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// ExecuteAirflow triggers an Airflow DAG run and returns metadata with the dag_run_id.
func ExecuteAirflow(ctx context.Context, cfg *types.TriggerConfig) (map[string]interface{}, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("airflow trigger: url is required")
	}
	if cfg.DagID == "" {
		return nil, fmt.Errorf("airflow trigger: dagID is required")
	}

	url := strings.TrimRight(cfg.URL, "/") + "/api/v1/dags/" + cfg.DagID + "/dagRuns"

	payload := map[string]interface{}{}
	if cfg.Body != "" {
		var conf interface{}
		if err := json.Unmarshal([]byte(os.ExpandEnv(cfg.Body)), &conf); err != nil {
			return nil, fmt.Errorf("airflow trigger: invalid body JSON: %w", err)
		}
		payload["conf"] = conf
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("airflow trigger: marshaling payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("airflow trigger: creating request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	for k, v := range cfg.Headers {
		req.Header.Set(k, os.ExpandEnv(v))
	}

	client := defaultHTTPClient
	if cfg.Timeout > 0 {
		timeout := time.Duration(cfg.Timeout) * time.Second
		if timeout != defaultTriggerTimeout {
			client = &http.Client{Timeout: timeout}
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("airflow trigger: request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("airflow trigger: reading response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("airflow trigger: returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("airflow trigger: parsing response: %w", err)
	}

	dagRunID, _ := result["dag_run_id"].(string)
	if dagRunID == "" {
		return nil, fmt.Errorf("airflow trigger: response missing dag_run_id")
	}

	meta := map[string]interface{}{
		"airflow_dag_run_id": dagRunID,
		"airflow_dag_id":     cfg.DagID,
		"airflow_url":        cfg.URL,
	}
	return meta, nil
}

// CheckAirflowStatus polls the status of an Airflow DAG run.
// Returns the state string: "queued", "running", "success", "failed".
func CheckAirflowStatus(ctx context.Context, airflowURL, dagID, dagRunID string, headers map[string]string) (string, error) {
	url := strings.TrimRight(airflowURL, "/") + "/api/v1/dags/" + dagID + "/dagRuns/" + dagRunID

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return "", fmt.Errorf("airflow status: creating request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, os.ExpandEnv(v))
	}

	resp, err := defaultHTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("airflow status: request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("airflow status: reading response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("airflow status: returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return "", fmt.Errorf("airflow status: parsing response: %w", err)
	}

	state, _ := result["state"].(string)
	if state == "" {
		return "", fmt.Errorf("airflow status: response missing state field")
	}

	return state, nil
}

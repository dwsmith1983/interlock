package trigger

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// ExecuteAirflow triggers an Airflow DAG run and returns metadata with the dag_run_id.
func ExecuteAirflow(ctx context.Context, cfg *types.AirflowTriggerConfig) (map[string]interface{}, error) {
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
		// The decoder error is not wrapped: it can quote a character of an
		// expanded INTERLOCK_ value, such as a token.
		if json.Unmarshal([]byte(expandRunBody(cfg.Body, cfg.BodyGenerated, cfg.Run)), &conf) != nil {
			return nil, errors.New("airflow trigger: body is not valid JSON after variable expansion")
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
	// Env only: CheckAirflowStatus re-sends these headers with no run identity.
	for k, v := range cfg.Headers {
		req.Header.Set(k, os.Expand(v, safeEnvLookup))
	}

	resp, err := resolveHTTPClient(cfg.Timeout).Do(req)
	if err != nil {
		return nil, fmt.Errorf("airflow trigger: request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("airflow trigger: reading response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("airflow trigger: returned status %d: %s", resp.StatusCode, sanitizeBody(respBody, secretValues(cfg.Headers, cfg.Body), maxErrorBodyBytes))
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
		req.Header.Set(k, os.Expand(v, safeEnvLookup))
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
		return "", fmt.Errorf("airflow status: returned status %d: %s", resp.StatusCode, sanitizeBody(respBody, secretValues(headers), maxErrorBodyBytes))
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

package trigger

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// BigQueryAPI is the subset of the GCP BigQuery client used by the trigger package.
type BigQueryAPI interface {
	InsertJob(ctx context.Context, projectID, query, datasetID string) (string, error)
	GetJob(ctx context.Context, projectID, jobID string) (BigQueryJobStatus, error)
}

// BigQueryJobStatus is the normalized result of a BigQuery job status check.
type BigQueryJobStatus struct {
	State string // "Done", "Running", "Pending"
	Error string // non-empty if job failed
}

// bigqueryClientWrapper wraps the real BigQuery client.
type bigqueryClientWrapper struct {
	client *bigquery.Client
}

func (w *bigqueryClientWrapper) InsertJob(ctx context.Context, _, query, datasetID string) (string, error) {
	q := w.client.Query(query)
	if datasetID != "" {
		q.DefaultDatasetID = datasetID
	}
	job, err := q.Run(ctx)
	if err != nil {
		return "", err
	}
	return job.ID(), nil
}

func (w *bigqueryClientWrapper) GetJob(ctx context.Context, _, jobID string) (BigQueryJobStatus, error) {
	job, err := w.client.JobFromID(ctx, jobID)
	if err != nil {
		return BigQueryJobStatus{}, err
	}
	status, err := job.Status(ctx)
	if err != nil {
		return BigQueryJobStatus{}, err
	}
	result := BigQueryJobStatus{
		State: bqStateToString(status.State),
	}
	if status.Err() != nil {
		result.Error = status.Err().Error()
	}
	return result, nil
}

// bqStateToString converts a BigQuery State int to a human-readable string.
func bqStateToString(state bigquery.State) string {
	switch state {
	case bigquery.Done:
		return "Done"
	case bigquery.Running:
		return "Running"
	case bigquery.Pending:
		return "Pending"
	default:
		return fmt.Sprintf("Unknown(%d)", int(state))
	}
}

// ExecuteBigQuery submits a BigQuery query job.
func ExecuteBigQuery(ctx context.Context, cfg *types.TriggerConfig, client BigQueryAPI) (map[string]interface{}, error) {
	if cfg.ProjectID == "" {
		return nil, fmt.Errorf("bigquery trigger: projectId is required")
	}
	if cfg.Query == "" {
		return nil, fmt.Errorf("bigquery trigger: query is required")
	}

	jobID, err := client.InsertJob(ctx, cfg.ProjectID, cfg.Query, cfg.DatasetID)
	if err != nil {
		return nil, fmt.Errorf("bigquery trigger: InsertJob failed: %w", err)
	}

	meta := map[string]interface{}{
		"bigquery_project_id": cfg.ProjectID,
		"bigquery_job_id":     jobID,
	}
	return meta, nil
}

// checkBigQueryStatus checks the status of a BigQuery job.
func (r *Runner) checkBigQueryStatus(ctx context.Context, metadata map[string]interface{}) (StatusResult, error) {
	projectID, _ := metadata["bigquery_project_id"].(string)
	jobID, _ := metadata["bigquery_job_id"].(string)
	if projectID == "" || jobID == "" {
		return StatusResult{State: RunCheckRunning, Message: "missing bigquery metadata"}, nil
	}

	client, err := r.getBigQueryClient(projectID)
	if err != nil {
		return StatusResult{}, fmt.Errorf("bigquery status: getting client: %w", err)
	}

	status, err := client.GetJob(ctx, projectID, jobID)
	if err != nil {
		return StatusResult{}, fmt.Errorf("bigquery status: GetJob failed: %w", err)
	}

	if status.State == "Done" {
		if status.Error != "" {
			return StatusResult{State: RunCheckFailed, Message: status.Error}, nil
		}
		return StatusResult{State: RunCheckSucceeded, Message: "DONE"}, nil
	}
	return StatusResult{State: RunCheckRunning, Message: status.State}, nil
}

func (r *Runner) getBigQueryClient(projectID string) (BigQueryAPI, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.bigqueryClient != nil {
		return r.bigqueryClient, nil
	}
	client, err := bigquery.NewClient(context.Background(), projectID)
	if err != nil {
		return nil, fmt.Errorf("creating BigQuery client: %w", err)
	}
	r.bigqueryClient = &bigqueryClientWrapper{client: client}
	return r.bigqueryClient, nil
}

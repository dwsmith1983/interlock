package trigger

import (
	"context"
	"testing"

	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockBigQueryClient struct {
	insertJobID  string
	insertJobErr error
	getJobStatus BigQueryJobStatus
	getJobErr    error
}

func (m *mockBigQueryClient) InsertJob(_ context.Context, _, _, _ string) (string, error) {
	return m.insertJobID, m.insertJobErr
}

func (m *mockBigQueryClient) GetJob(_ context.Context, _, _ string) (BigQueryJobStatus, error) {
	return m.getJobStatus, m.getJobErr
}

func TestExecuteBigQuery_Success(t *testing.T) {
	client := &mockBigQueryClient{
		insertJobID: "bq-job-123",
	}

	cfg := &types.TriggerConfig{
		Type:      types.TriggerBigQuery,
		ProjectID: "my-project",
		Query:     "SELECT 1",
		DatasetID: "my_dataset",
	}

	meta, err := ExecuteBigQuery(context.Background(), cfg, client)
	require.NoError(t, err)
	assert.Equal(t, "my-project", meta["bigquery_project_id"])
	assert.Equal(t, "bq-job-123", meta["bigquery_job_id"])
}

func TestExecuteBigQuery_MissingProjectID(t *testing.T) {
	client := &mockBigQueryClient{}
	cfg := &types.TriggerConfig{Type: types.TriggerBigQuery, Query: "SELECT 1"}
	_, err := ExecuteBigQuery(context.Background(), cfg, client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "projectId is required")
}

func TestExecuteBigQuery_MissingQuery(t *testing.T) {
	client := &mockBigQueryClient{}
	cfg := &types.TriggerConfig{Type: types.TriggerBigQuery, ProjectID: "p"}
	_, err := ExecuteBigQuery(context.Background(), cfg, client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "query is required")
}

func TestExecuteBigQuery_APIError(t *testing.T) {
	client := &mockBigQueryClient{insertJobErr: assert.AnError}
	cfg := &types.TriggerConfig{
		Type:      types.TriggerBigQuery,
		ProjectID: "p",
		Query:     "SELECT 1",
	}
	_, err := ExecuteBigQuery(context.Background(), cfg, client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "InsertJob failed")
}

func TestCheckBigQueryStatus(t *testing.T) {
	tests := []struct {
		name     string
		status   BigQueryJobStatus
		expected RunCheckState
	}{
		{"done_success", BigQueryJobStatus{State: "Done"}, RunCheckSucceeded},
		{"done_error", BigQueryJobStatus{State: "Done", Error: "query error"}, RunCheckFailed},
		{"running", BigQueryJobStatus{State: "Running"}, RunCheckRunning},
		{"pending", BigQueryJobStatus{State: "Pending"}, RunCheckRunning},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &mockBigQueryClient{getJobStatus: tt.status}

			r := NewRunner(WithBigQueryClient(client))
			result, err := r.checkBigQueryStatus(context.Background(), map[string]interface{}{
				"bigquery_project_id": "p",
				"bigquery_job_id":     "j-1",
			})
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result.State)
		})
	}
}

func TestCheckBigQueryStatus_MissingMetadata(t *testing.T) {
	r := NewRunner()
	result, err := r.checkBigQueryStatus(context.Background(), map[string]interface{}{})
	require.NoError(t, err)
	assert.Equal(t, RunCheckRunning, result.State)
}

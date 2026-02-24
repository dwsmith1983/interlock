package trigger

import (
	"context"
	"testing"

	dataprocpb "cloud.google.com/go/dataproc/v2/apiv1/dataprocpb"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockDataprocSLClient struct {
	createBatchID  string
	createBatchErr error
	getBatchOut    *dataprocpb.Batch
	getBatchErr    error
}

func (m *mockDataprocSLClient) CreateBatch(_ context.Context, req *dataprocpb.CreateBatchRequest, _ ...interface{}) (string, error) {
	if m.createBatchErr != nil {
		return "", m.createBatchErr
	}
	return m.createBatchID, nil
}

func (m *mockDataprocSLClient) GetBatch(_ context.Context, _ *dataprocpb.GetBatchRequest, _ ...interface{}) (*dataprocpb.Batch, error) {
	return m.getBatchOut, m.getBatchErr
}

func TestExecuteDataprocServerless_Success(t *testing.T) {
	client := &mockDataprocSLClient{
		createBatchID: "my-batch",
	}

	cfg := &types.TriggerConfig{
		Type:      types.TriggerDataprocServerless,
		ProjectID: "my-project",
		Region:    "us-central1",
		JobName:   "my-batch",
	}

	meta, err := ExecuteDataprocServerless(context.Background(), cfg, client)
	require.NoError(t, err)
	assert.Equal(t, "my-project", meta["dataproc_sl_project_id"])
	assert.Equal(t, "us-central1", meta["dataproc_sl_region"])
	assert.Equal(t, "my-batch", meta["dataproc_sl_batch_id"])
}

func TestExecuteDataprocServerless_MissingProjectID(t *testing.T) {
	client := &mockDataprocSLClient{}
	cfg := &types.TriggerConfig{Type: types.TriggerDataprocServerless, Region: "r", JobName: "j"}
	_, err := ExecuteDataprocServerless(context.Background(), cfg, client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "projectId is required")
}

func TestExecuteDataprocServerless_MissingRegion(t *testing.T) {
	client := &mockDataprocSLClient{}
	cfg := &types.TriggerConfig{Type: types.TriggerDataprocServerless, ProjectID: "p", JobName: "j"}
	_, err := ExecuteDataprocServerless(context.Background(), cfg, client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "region is required")
}

func TestExecuteDataprocServerless_APIError(t *testing.T) {
	client := &mockDataprocSLClient{createBatchErr: assert.AnError}
	cfg := &types.TriggerConfig{
		Type:      types.TriggerDataprocServerless,
		ProjectID: "p",
		Region:    "r",
		JobName:   "j",
	}
	_, err := ExecuteDataprocServerless(context.Background(), cfg, client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "CreateBatch failed")
}

func TestCheckDataprocServerlessStatus(t *testing.T) {
	tests := []struct {
		name     string
		state    dataprocpb.Batch_State
		expected RunCheckState
	}{
		{"succeeded", dataprocpb.Batch_SUCCEEDED, RunCheckSucceeded},
		{"failed", dataprocpb.Batch_FAILED, RunCheckFailed},
		{"cancelled", dataprocpb.Batch_CANCELLED, RunCheckFailed},
		{"running", dataprocpb.Batch_RUNNING, RunCheckRunning},
		{"pending", dataprocpb.Batch_PENDING, RunCheckRunning},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &mockDataprocSLClient{
				getBatchOut: &dataprocpb.Batch{State: tt.state},
			}

			r := NewRunner(WithDataprocServerlessClient(client))
			result, err := r.checkDataprocServerlessStatus(context.Background(), map[string]interface{}{
				"dataproc_sl_project_id": "p",
				"dataproc_sl_region":     "r",
				"dataproc_sl_batch_id":   "b-1",
			})
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result.State)
		})
	}
}

func TestCheckDataprocServerlessStatus_MissingMetadata(t *testing.T) {
	r := NewRunner()
	result, err := r.checkDataprocServerlessStatus(context.Background(), map[string]interface{}{})
	require.NoError(t, err)
	assert.Equal(t, RunCheckRunning, result.State)
}

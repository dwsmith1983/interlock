package trigger

import (
	"context"
	"testing"

	dataprocpb "cloud.google.com/go/dataproc/v2/apiv1/dataprocpb"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockDataprocClient struct {
	submitOut *dataprocpb.Job
	submitErr error
	getOut    *dataprocpb.Job
	getErr    error
}

func (m *mockDataprocClient) SubmitJob(_ context.Context, _ *dataprocpb.SubmitJobRequest, _ ...interface{}) (*dataprocpb.Job, error) {
	return m.submitOut, m.submitErr
}

func (m *mockDataprocClient) GetJob(_ context.Context, _ *dataprocpb.GetJobRequest, _ ...interface{}) (*dataprocpb.Job, error) {
	return m.getOut, m.getErr
}

func TestExecuteDataproc_Success(t *testing.T) {
	client := &mockDataprocClient{
		submitOut: &dataprocpb.Job{
			Reference: &dataprocpb.JobReference{JobId: "job-123"},
		},
	}

	cfg := &types.TriggerConfig{
		Type:      types.TriggerDataproc,
		ProjectID: "my-project",
		Region:    "us-central1",
		ClusterID: "my-cluster",
		JobName:   "my-job",
		Command:   "gs://bucket/my-jar.jar",
	}

	meta, err := ExecuteDataproc(context.Background(), cfg, client)
	require.NoError(t, err)
	assert.Equal(t, "my-project", meta["dataproc_project_id"])
	assert.Equal(t, "us-central1", meta["dataproc_region"])
	assert.Equal(t, "job-123", meta["dataproc_job_id"])
}

func TestExecuteDataproc_MissingProjectID(t *testing.T) {
	client := &mockDataprocClient{}
	cfg := &types.TriggerConfig{Type: types.TriggerDataproc, Region: "us-central1", ClusterID: "c", JobName: "j"}
	_, err := ExecuteDataproc(context.Background(), cfg, client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "projectId is required")
}

func TestExecuteDataproc_MissingRegion(t *testing.T) {
	client := &mockDataprocClient{}
	cfg := &types.TriggerConfig{Type: types.TriggerDataproc, ProjectID: "p", ClusterID: "c", JobName: "j"}
	_, err := ExecuteDataproc(context.Background(), cfg, client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "region is required")
}

func TestExecuteDataproc_MissingClusterID(t *testing.T) {
	client := &mockDataprocClient{}
	cfg := &types.TriggerConfig{Type: types.TriggerDataproc, ProjectID: "p", Region: "r", JobName: "j"}
	_, err := ExecuteDataproc(context.Background(), cfg, client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "clusterId is required")
}

func TestExecuteDataproc_APIError(t *testing.T) {
	client := &mockDataprocClient{submitErr: assert.AnError}
	cfg := &types.TriggerConfig{
		Type:      types.TriggerDataproc,
		ProjectID: "p",
		Region:    "r",
		ClusterID: "c",
		JobName:   "j",
	}
	_, err := ExecuteDataproc(context.Background(), cfg, client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SubmitJob failed")
}

func TestCheckDataprocStatus(t *testing.T) {
	tests := []struct {
		name     string
		state    dataprocpb.JobStatus_State
		expected RunCheckState
	}{
		{"done", dataprocpb.JobStatus_DONE, RunCheckSucceeded},
		{"error", dataprocpb.JobStatus_ERROR, RunCheckFailed},
		{"cancelled", dataprocpb.JobStatus_CANCELLED, RunCheckFailed},
		{"running", dataprocpb.JobStatus_RUNNING, RunCheckRunning},
		{"pending", dataprocpb.JobStatus_PENDING, RunCheckRunning},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &mockDataprocClient{
				getOut: &dataprocpb.Job{
					Status: &dataprocpb.JobStatus{State: tt.state},
				},
			}

			r := NewRunner(WithDataprocClient(client))
			result, err := r.checkDataprocStatus(context.Background(), map[string]interface{}{
				"dataproc_project_id": "p",
				"dataproc_region":     "r",
				"dataproc_job_id":     "j-1",
			})
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result.State)
		})
	}
}

func TestCheckDataprocStatus_MissingMetadata(t *testing.T) {
	r := NewRunner()
	result, err := r.checkDataprocStatus(context.Background(), map[string]interface{}{})
	require.NoError(t, err)
	assert.Equal(t, RunCheckRunning, result.State)
}

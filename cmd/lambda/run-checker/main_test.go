package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/emr"
	emrtypes "github.com/aws/aws-sdk-go-v2/service/emr/types"
	"github.com/aws/aws-sdk-go-v2/service/emrserverless"
	emrSLtypes "github.com/aws/aws-sdk-go-v2/service/emrserverless/types"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	intlambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/trigger"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- mock AWS SDK clients for testing via handleRunCheck ---

type stubGlueClient struct {
	getOut *glue.GetJobRunOutput
	getErr error
}

func (s *stubGlueClient) StartJobRun(_ context.Context, _ *glue.StartJobRunInput, _ ...func(*glue.Options)) (*glue.StartJobRunOutput, error) {
	return nil, nil
}

func (s *stubGlueClient) GetJobRun(_ context.Context, _ *glue.GetJobRunInput, _ ...func(*glue.Options)) (*glue.GetJobRunOutput, error) {
	return s.getOut, s.getErr
}

type stubEMRClient struct {
	describeOut *emr.DescribeStepOutput
	describeErr error
}

func (s *stubEMRClient) AddJobFlowSteps(_ context.Context, _ *emr.AddJobFlowStepsInput, _ ...func(*emr.Options)) (*emr.AddJobFlowStepsOutput, error) {
	return nil, nil
}

func (s *stubEMRClient) DescribeStep(_ context.Context, _ *emr.DescribeStepInput, _ ...func(*emr.Options)) (*emr.DescribeStepOutput, error) {
	return s.describeOut, s.describeErr
}

type stubEMRServerlessClient struct {
	getOut *emrserverless.GetJobRunOutput
	getErr error
}

func (s *stubEMRServerlessClient) StartJobRun(_ context.Context, _ *emrserverless.StartJobRunInput, _ ...func(*emrserverless.Options)) (*emrserverless.StartJobRunOutput, error) {
	return nil, nil
}

func (s *stubEMRServerlessClient) GetJobRun(_ context.Context, _ *emrserverless.GetJobRunInput, _ ...func(*emrserverless.Options)) (*emrserverless.GetJobRunOutput, error) {
	return s.getOut, s.getErr
}

type stubSFNClient struct {
	describeOut *sfn.DescribeExecutionOutput
	describeErr error
}

func (s *stubSFNClient) StartExecution(_ context.Context, _ *sfn.StartExecutionInput, _ ...func(*sfn.Options)) (*sfn.StartExecutionOutput, error) {
	return nil, nil
}

func (s *stubSFNClient) DescribeExecution(_ context.Context, _ *sfn.DescribeExecutionInput, _ ...func(*sfn.Options)) (*sfn.DescribeExecutionOutput, error) {
	return s.describeOut, s.describeErr
}

// --- test helpers ---

func testDeps(httpClient *http.Client) *intlambda.Deps {
	opts := []trigger.RunnerOption{}
	if httpClient != nil {
		opts = append(opts, trigger.WithHTTPClient(httpClient))
	}
	return &intlambda.Deps{
		Runner: trigger.NewRunner(opts...),
		Logger: slog.Default(),
	}
}

func testDepsWithRunner(r *trigger.Runner) *intlambda.Deps {
	return &intlambda.Deps{
		Runner: r,
		Logger: slog.Default(),
	}
}

func TestHandleRunCheck_NonPollingType(t *testing.T) {
	d := testDeps(nil)

	req := intlambda.RunCheckRequest{
		PipelineID:  "test-pipe",
		RunID:       "run-1",
		TriggerType: types.TriggerHTTP,
		Metadata:    map[string]interface{}{},
	}

	resp, err := handleRunCheck(context.Background(), d, req)
	require.NoError(t, err)
	assert.Equal(t, trigger.RunCheckRunning, resp.State)
	assert.Equal(t, "non-polling trigger type", resp.Message)
}

func TestHandleRunCheck_CommandType(t *testing.T) {
	d := testDeps(nil)

	resp, err := handleRunCheck(context.Background(), d, intlambda.RunCheckRequest{
		PipelineID:  "test-pipe",
		RunID:       "run-1",
		TriggerType: types.TriggerCommand,
		Metadata:    map[string]interface{}{},
	})
	require.NoError(t, err)
	assert.Equal(t, trigger.RunCheckRunning, resp.State)
}

func TestHandleRunCheck_AirflowSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, `{"dag_run_id": "run_1", "state": "success"}`)
	}))
	defer srv.Close()

	d := testDeps(srv.Client())

	resp, err := handleRunCheck(context.Background(), d, intlambda.RunCheckRequest{
		PipelineID:  "test-pipe",
		RunID:       "run-1",
		TriggerType: types.TriggerAirflow,
		Metadata: map[string]interface{}{
			"airflow_url":        srv.URL,
			"airflow_dag_id":     "my_dag",
			"airflow_dag_run_id": "run_1",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, trigger.RunCheckSucceeded, resp.State)
}

func TestHandleRunCheck_AirflowMissingMetadata(t *testing.T) {
	d := testDeps(nil)

	resp, err := handleRunCheck(context.Background(), d, intlambda.RunCheckRequest{
		PipelineID:  "test-pipe",
		RunID:       "run-1",
		TriggerType: types.TriggerAirflow,
		Metadata:    map[string]interface{}{},
	})
	require.NoError(t, err)
	// Missing metadata falls through to "running" state in the runner
	assert.Equal(t, trigger.RunCheckRunning, resp.State)
}

func TestHandleRunCheck_UnknownType(t *testing.T) {
	d := testDeps(nil)

	resp, err := handleRunCheck(context.Background(), d, intlambda.RunCheckRequest{
		PipelineID:  "test-pipe",
		RunID:       "run-1",
		TriggerType: "unknown",
		Metadata:    map[string]interface{}{},
	})
	require.NoError(t, err)
	assert.Equal(t, trigger.RunCheckFailed, resp.State)
}

func TestHandleRunCheck_GlueStatusMapping(t *testing.T) {
	tests := []struct {
		name     string
		state    gluetypes.JobRunState
		expected trigger.RunCheckState
	}{
		{"succeeded", gluetypes.JobRunStateSucceeded, trigger.RunCheckSucceeded},
		{"failed", gluetypes.JobRunStateFailed, trigger.RunCheckFailed},
		{"timeout", gluetypes.JobRunStateTimeout, trigger.RunCheckFailed},
		{"stopped", gluetypes.JobRunStateStopped, trigger.RunCheckFailed},
		{"error", gluetypes.JobRunStateError, trigger.RunCheckFailed},
		{"running", gluetypes.JobRunStateRunning, trigger.RunCheckRunning},
		{"starting", gluetypes.JobRunStateStarting, trigger.RunCheckRunning},
		{"waiting", gluetypes.JobRunStateWaiting, trigger.RunCheckRunning},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &stubGlueClient{
				getOut: &glue.GetJobRunOutput{
					JobRun: &gluetypes.JobRun{
						JobRunState: tt.state,
					},
				},
			}
			r := trigger.NewRunner(trigger.WithGlueClient(client))
			d := testDepsWithRunner(r)

			resp, err := handleRunCheck(context.Background(), d, intlambda.RunCheckRequest{
				PipelineID:  "glue-pipe",
				RunID:       "run-g1",
				TriggerType: types.TriggerGlue,
				Metadata: map[string]interface{}{
					"glue_job_name":   "etl-job",
					"glue_job_run_id": "jr_abc",
				},
			})
			require.NoError(t, err)
			assert.Equal(t, tt.expected, resp.State)
			assert.Equal(t, string(tt.state), resp.Message)
		})
	}
}

func TestHandleRunCheck_GlueMissingMetadata(t *testing.T) {
	client := &stubGlueClient{}
	r := trigger.NewRunner(trigger.WithGlueClient(client))
	d := testDepsWithRunner(r)

	resp, err := handleRunCheck(context.Background(), d, intlambda.RunCheckRequest{
		PipelineID:  "glue-pipe",
		RunID:       "run-g2",
		TriggerType: types.TriggerGlue,
		Metadata:    map[string]interface{}{},
	})
	require.NoError(t, err)
	assert.Equal(t, trigger.RunCheckRunning, resp.State)
	assert.Contains(t, resp.Message, "missing glue metadata")
}

func TestHandleRunCheck_GlueAPIError(t *testing.T) {
	client := &stubGlueClient{getErr: fmt.Errorf("access denied")}
	r := trigger.NewRunner(trigger.WithGlueClient(client))
	d := testDepsWithRunner(r)

	resp, err := handleRunCheck(context.Background(), d, intlambda.RunCheckRequest{
		PipelineID:  "glue-pipe",
		RunID:       "run-g3",
		TriggerType: types.TriggerGlue,
		Metadata: map[string]interface{}{
			"glue_job_name":   "job",
			"glue_job_run_id": "jr_1",
		},
	})
	require.NoError(t, err)
	// Runner returns an error, which handleRunCheck converts to a failed response
	assert.Equal(t, trigger.RunCheckFailed, resp.State)
	assert.Contains(t, resp.Message, "access denied")
}

func TestHandleRunCheck_EMRStatusMapping(t *testing.T) {
	tests := []struct {
		name     string
		state    emrtypes.StepState
		expected trigger.RunCheckState
	}{
		{"completed", emrtypes.StepStateCompleted, trigger.RunCheckSucceeded},
		{"failed", emrtypes.StepStateFailed, trigger.RunCheckFailed},
		{"cancelled", emrtypes.StepStateCancelled, trigger.RunCheckFailed},
		{"interrupted", emrtypes.StepStateInterrupted, trigger.RunCheckFailed},
		{"running", emrtypes.StepStateRunning, trigger.RunCheckRunning},
		{"pending", emrtypes.StepStatePending, trigger.RunCheckRunning},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &stubEMRClient{
				describeOut: &emr.DescribeStepOutput{
					Step: &emrtypes.Step{
						Status: &emrtypes.StepStatus{
							State: tt.state,
						},
					},
				},
			}
			r := trigger.NewRunner(trigger.WithEMRClient(client))
			d := testDepsWithRunner(r)

			resp, err := handleRunCheck(context.Background(), d, intlambda.RunCheckRequest{
				PipelineID:  "emr-pipe",
				RunID:       "run-e1",
				TriggerType: types.TriggerEMR,
				Metadata: map[string]interface{}{
					"emr_cluster_id": "j-CLUSTER",
					"emr_step_id":    "s-STEP",
				},
			})
			require.NoError(t, err)
			assert.Equal(t, tt.expected, resp.State)
			assert.Equal(t, string(tt.state), resp.Message)
		})
	}
}

func TestHandleRunCheck_EMRMissingMetadata(t *testing.T) {
	client := &stubEMRClient{}
	r := trigger.NewRunner(trigger.WithEMRClient(client))
	d := testDepsWithRunner(r)

	resp, err := handleRunCheck(context.Background(), d, intlambda.RunCheckRequest{
		PipelineID:  "emr-pipe",
		RunID:       "run-e2",
		TriggerType: types.TriggerEMR,
		Metadata:    map[string]interface{}{},
	})
	require.NoError(t, err)
	assert.Equal(t, trigger.RunCheckRunning, resp.State)
	assert.Contains(t, resp.Message, "missing emr metadata")
}

func TestHandleRunCheck_EMRAPIError(t *testing.T) {
	client := &stubEMRClient{describeErr: fmt.Errorf("cluster terminated")}
	r := trigger.NewRunner(trigger.WithEMRClient(client))
	d := testDepsWithRunner(r)

	resp, err := handleRunCheck(context.Background(), d, intlambda.RunCheckRequest{
		PipelineID:  "emr-pipe",
		RunID:       "run-e3",
		TriggerType: types.TriggerEMR,
		Metadata: map[string]interface{}{
			"emr_cluster_id": "j-1",
			"emr_step_id":    "s-1",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, trigger.RunCheckFailed, resp.State)
	assert.Contains(t, resp.Message, "cluster terminated")
}

func TestHandleRunCheck_SFNStatusMapping(t *testing.T) {
	tests := []struct {
		name     string
		status   sfntypes.ExecutionStatus
		expected trigger.RunCheckState
	}{
		{"succeeded", sfntypes.ExecutionStatusSucceeded, trigger.RunCheckSucceeded},
		{"failed", sfntypes.ExecutionStatusFailed, trigger.RunCheckFailed},
		{"timed_out", sfntypes.ExecutionStatusTimedOut, trigger.RunCheckFailed},
		{"aborted", sfntypes.ExecutionStatusAborted, trigger.RunCheckFailed},
		{"running", sfntypes.ExecutionStatusRunning, trigger.RunCheckRunning},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &stubSFNClient{
				describeOut: &sfn.DescribeExecutionOutput{
					Status: tt.status,
				},
			}
			r := trigger.NewRunner(trigger.WithSFNClient(client))
			d := testDepsWithRunner(r)

			resp, err := handleRunCheck(context.Background(), d, intlambda.RunCheckRequest{
				PipelineID:  "sfn-pipe",
				RunID:       "run-s1",
				TriggerType: types.TriggerStepFunction,
				Metadata: map[string]interface{}{
					"sfn_execution_arn": "arn:aws:states:us-east-1:123:execution:sm:exec-1",
				},
			})
			require.NoError(t, err)
			assert.Equal(t, tt.expected, resp.State)
			assert.Equal(t, string(tt.status), resp.Message)
		})
	}
}

func TestHandleRunCheck_SFNMissingMetadata(t *testing.T) {
	client := &stubSFNClient{}
	r := trigger.NewRunner(trigger.WithSFNClient(client))
	d := testDepsWithRunner(r)

	resp, err := handleRunCheck(context.Background(), d, intlambda.RunCheckRequest{
		PipelineID:  "sfn-pipe",
		RunID:       "run-s2",
		TriggerType: types.TriggerStepFunction,
		Metadata:    map[string]interface{}{},
	})
	require.NoError(t, err)
	assert.Equal(t, trigger.RunCheckRunning, resp.State)
	assert.Contains(t, resp.Message, "missing sfn metadata")
}

func TestHandleRunCheck_SFNAPIError(t *testing.T) {
	client := &stubSFNClient{describeErr: fmt.Errorf("execution not found")}
	r := trigger.NewRunner(trigger.WithSFNClient(client))
	d := testDepsWithRunner(r)

	resp, err := handleRunCheck(context.Background(), d, intlambda.RunCheckRequest{
		PipelineID:  "sfn-pipe",
		RunID:       "run-s3",
		TriggerType: types.TriggerStepFunction,
		Metadata: map[string]interface{}{
			"sfn_execution_arn": "arn:exec",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, trigger.RunCheckFailed, resp.State)
	assert.Contains(t, resp.Message, "execution not found")
}

func TestHandleRunCheck_EMRServerlessStatusMapping(t *testing.T) {
	tests := []struct {
		name     string
		state    emrSLtypes.JobRunState
		expected trigger.RunCheckState
	}{
		{"success", emrSLtypes.JobRunStateSuccess, trigger.RunCheckSucceeded},
		{"failed", emrSLtypes.JobRunStateFailed, trigger.RunCheckFailed},
		{"cancelled", emrSLtypes.JobRunStateCancelled, trigger.RunCheckFailed},
		{"running", emrSLtypes.JobRunStateRunning, trigger.RunCheckRunning},
		{"pending", emrSLtypes.JobRunStatePending, trigger.RunCheckRunning},
		{"submitted", emrSLtypes.JobRunStateSubmitted, trigger.RunCheckRunning},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &stubEMRServerlessClient{
				getOut: &emrserverless.GetJobRunOutput{
					JobRun: &emrSLtypes.JobRun{
						State: tt.state,
					},
				},
			}
			r := trigger.NewRunner(trigger.WithEMRServerlessClient(client))
			d := testDepsWithRunner(r)

			resp, err := handleRunCheck(context.Background(), d, intlambda.RunCheckRequest{
				PipelineID:  "emrsl-pipe",
				RunID:       "run-esl1",
				TriggerType: types.TriggerEMRServerless,
				Metadata: map[string]interface{}{
					"emr_sl_application_id": "app-1",
					"emr_sl_job_run_id":     "run-1",
				},
			})
			require.NoError(t, err)
			assert.Equal(t, tt.expected, resp.State)
			assert.Equal(t, string(tt.state), resp.Message)
		})
	}
}

func TestHandleRunCheck_EMRServerlessMissingMetadata(t *testing.T) {
	client := &stubEMRServerlessClient{}
	r := trigger.NewRunner(trigger.WithEMRServerlessClient(client))
	d := testDepsWithRunner(r)

	resp, err := handleRunCheck(context.Background(), d, intlambda.RunCheckRequest{
		PipelineID:  "emrsl-pipe",
		RunID:       "run-esl2",
		TriggerType: types.TriggerEMRServerless,
		Metadata:    map[string]interface{}{},
	})
	require.NoError(t, err)
	assert.Equal(t, trigger.RunCheckRunning, resp.State)
	assert.Contains(t, resp.Message, "missing emr-serverless metadata")
}

func TestHandleRunCheck_DatabricksSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, `{"state":{"life_cycle_state":"TERMINATED","result_state":"SUCCESS"}}`)
	}))
	defer srv.Close()

	d := testDeps(srv.Client())

	resp, err := handleRunCheck(context.Background(), d, intlambda.RunCheckRequest{
		PipelineID:  "db-pipe",
		RunID:       "run-db1",
		TriggerType: types.TriggerDatabricks,
		Metadata: map[string]interface{}{
			"databricks_workspace_url": srv.URL,
			"databricks_run_id":        "12345",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, trigger.RunCheckSucceeded, resp.State)
}

func TestHandleRunCheck_DatabricksRunning(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, `{"state":{"life_cycle_state":"RUNNING"}}`)
	}))
	defer srv.Close()

	d := testDeps(srv.Client())

	resp, err := handleRunCheck(context.Background(), d, intlambda.RunCheckRequest{
		PipelineID:  "db-pipe",
		RunID:       "run-db2",
		TriggerType: types.TriggerDatabricks,
		Metadata: map[string]interface{}{
			"databricks_workspace_url": srv.URL,
			"databricks_run_id":        "12345",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, trigger.RunCheckRunning, resp.State)
}

func TestHandleRunCheck_DatabricksFailed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, `{"state":{"life_cycle_state":"TERMINATED","result_state":"FAILED"}}`)
	}))
	defer srv.Close()

	d := testDeps(srv.Client())

	resp, err := handleRunCheck(context.Background(), d, intlambda.RunCheckRequest{
		PipelineID:  "db-pipe",
		RunID:       "run-db3",
		TriggerType: types.TriggerDatabricks,
		Metadata: map[string]interface{}{
			"databricks_workspace_url": srv.URL,
			"databricks_run_id":        "67890",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, trigger.RunCheckFailed, resp.State)
}

func TestHandleRunCheck_DatabricksMissingMetadata(t *testing.T) {
	d := testDeps(nil)

	resp, err := handleRunCheck(context.Background(), d, intlambda.RunCheckRequest{
		PipelineID:  "db-pipe",
		RunID:       "run-db4",
		TriggerType: types.TriggerDatabricks,
		Metadata:    map[string]interface{}{},
	})
	require.NoError(t, err)
	assert.Equal(t, trigger.RunCheckRunning, resp.State)
	assert.Contains(t, resp.Message, "missing databricks metadata")
}

func TestHandleRunCheck_NilMetadata(t *testing.T) {
	d := testDeps(nil)

	resp, err := handleRunCheck(context.Background(), d, intlambda.RunCheckRequest{
		PipelineID:  "test-pipe",
		RunID:       "run-nil",
		TriggerType: types.TriggerHTTP,
		Metadata:    nil,
	})
	require.NoError(t, err)
	assert.Equal(t, trigger.RunCheckRunning, resp.State)
}

func TestHandleRunCheck_AirflowRunning(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, `{"dag_run_id": "run_1", "state": "running"}`)
	}))
	defer srv.Close()

	d := testDeps(srv.Client())

	resp, err := handleRunCheck(context.Background(), d, intlambda.RunCheckRequest{
		PipelineID:  "test-pipe",
		RunID:       "run-af1",
		TriggerType: types.TriggerAirflow,
		Metadata: map[string]interface{}{
			"airflow_url":        srv.URL,
			"airflow_dag_id":     "my_dag",
			"airflow_dag_run_id": "run_1",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, trigger.RunCheckRunning, resp.State)
}

func TestHandleRunCheck_AirflowFailed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, `{"dag_run_id": "run_1", "state": "failed"}`)
	}))
	defer srv.Close()

	d := testDeps(srv.Client())

	resp, err := handleRunCheck(context.Background(), d, intlambda.RunCheckRequest{
		PipelineID:  "test-pipe",
		RunID:       "run-af2",
		TriggerType: types.TriggerAirflow,
		Metadata: map[string]interface{}{
			"airflow_url":        srv.URL,
			"airflow_dag_id":     "my_dag",
			"airflow_dag_run_id": "run_1",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, trigger.RunCheckFailed, resp.State)
}

// TestHandler_InitError exercises the top-level handler function.
// getDeps() calls intlambda.Init() which requires TABLE_NAME/AWS_REGION env vars.
// Without those vars Init fails, so handler() returns the init error.
// This covers getDeps() and the error path of handler().
func TestHandler_InitError(t *testing.T) {
	// Ensure env vars are unset so Init fails predictably.
	t.Setenv("TABLE_NAME", "")
	t.Setenv("AWS_REGION", "")

	_, err := handler(context.Background(), intlambda.RunCheckRequest{
		PipelineID:  "test-pipe",
		RunID:       "run-init",
		TriggerType: types.TriggerHTTP,
		Metadata:    map[string]interface{}{},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TABLE_NAME")
}

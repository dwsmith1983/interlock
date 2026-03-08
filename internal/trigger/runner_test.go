package trigger

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/emr"
	emrtypes "github.com/aws/aws-sdk-go-v2/service/emr/types"
	"github.com/aws/aws-sdk-go-v2/service/emrserverless"
	emrsltypes "github.com/aws/aws-sdk-go-v2/service/emrserverless/types"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunner_Execute_UnknownType(t *testing.T) {
	r := NewRunner()
	_, err := r.Execute(context.Background(), &types.TriggerConfig{
		Type: "unknown-type",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown trigger type")
}

func TestRunner_Execute_NilConfig(t *testing.T) {
	r := NewRunner()
	_, err := r.Execute(context.Background(), nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no trigger configured")
}

func TestRunner_CheckStatus_NonPollingTypes(t *testing.T) {
	r := NewRunner()

	for _, tt := range []types.TriggerType{types.TriggerCommand, types.TriggerHTTP} {
		result, err := r.CheckStatus(context.Background(), tt, nil, nil)
		require.NoError(t, err)
		assert.Equal(t, RunCheckRunning, result.State, "trigger type %s", tt)
	}
}

func TestRunner_CheckStatus_UnknownType(t *testing.T) {
	r := NewRunner()
	_, err := r.CheckStatus(context.Background(), "bogus", nil, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown trigger type for status check")
}

func TestRunner_CheckStatus_GlueDispatch(t *testing.T) {
	client := &mockGlueClient{
		getOut: &glue.GetJobRunOutput{
			JobRun: &gluetypes.JobRun{
				JobRunState: gluetypes.JobRunStateSucceeded,
			},
		},
	}
	r := NewRunner(WithGlueClient(client))

	result, err := r.CheckStatus(context.Background(), types.TriggerGlue, map[string]interface{}{
		"glue_job_name":   "job",
		"glue_job_run_id": "run",
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, RunCheckSucceeded, result.State)
}

func TestRunner_CheckStatus_EMRDispatch(t *testing.T) {
	client := &mockEMRClient{
		describeOut: &emr.DescribeStepOutput{
			Step: &emrtypes.Step{
				Status: &emrtypes.StepStatus{
					State: emrtypes.StepStateFailed,
				},
			},
		},
	}
	r := NewRunner(WithEMRClient(client))

	result, err := r.CheckStatus(context.Background(), types.TriggerEMR, map[string]interface{}{
		"emr_cluster_id": "j-1",
		"emr_step_id":    "s-1",
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, RunCheckFailed, result.State)
}

func TestRunner_CheckStatus_SFNDispatch(t *testing.T) {
	client := &mockSFNClient{
		describeOut: &sfn.DescribeExecutionOutput{
			Status: sfntypes.ExecutionStatusRunning,
		},
	}
	r := NewRunner(WithSFNClient(client))

	result, err := r.CheckStatus(context.Background(), types.TriggerStepFunction, map[string]interface{}{
		"sfn_execution_arn": "arn:exec",
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, RunCheckRunning, result.State)
}

func TestPackageLevelExecute_NilConfig(t *testing.T) {
	_, err := Execute(context.Background(), nil)
	assert.Error(t, err)
}

func TestPackageLevelCheckStatus_NonPolling(t *testing.T) {
	result, err := CheckStatus(context.Background(), types.TriggerCommand, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, RunCheckRunning, result.State)
}

// --- Runner.Execute dispatch tests ---

func TestRunner_Execute_CommandType(t *testing.T) {
	r := NewRunner()
	_, err := r.Execute(context.Background(), &types.TriggerConfig{
		Type:    types.TriggerCommand,
		Command: &types.CommandTriggerConfig{Command: "echo test"},
	})
	require.NoError(t, err)
}

func TestRunner_Execute_CommandType_NilConfig(t *testing.T) {
	r := NewRunner()
	_, err := r.Execute(context.Background(), &types.TriggerConfig{
		Type: types.TriggerCommand,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "command trigger config is nil")
}

func TestRunner_Execute_HTTPType(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	r := NewRunner()
	_, err := r.Execute(context.Background(), &types.TriggerConfig{
		Type: types.TriggerHTTP,
		HTTP: &types.HTTPTriggerConfig{Method: "POST", URL: srv.URL},
	})
	require.NoError(t, err)
}

func TestRunner_Execute_HTTPType_NilConfig(t *testing.T) {
	r := NewRunner()
	_, err := r.Execute(context.Background(), &types.TriggerConfig{
		Type: types.TriggerHTTP,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "http trigger config is nil")
}

func TestRunner_Execute_AirflowType(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"dag_run_id": "manual__run-1",
		})
	}))
	defer srv.Close()

	r := NewRunner()
	meta, err := r.Execute(context.Background(), &types.TriggerConfig{
		Type: types.TriggerAirflow,
		Airflow: &types.AirflowTriggerConfig{
			URL:   srv.URL,
			DagID: "my_dag",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "manual__run-1", meta["airflow_dag_run_id"])
}

func TestRunner_Execute_AirflowType_NilConfig(t *testing.T) {
	r := NewRunner()
	_, err := r.Execute(context.Background(), &types.TriggerConfig{
		Type: types.TriggerAirflow,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "airflow trigger config is nil")
}

func TestRunner_Execute_GlueType(t *testing.T) {
	runID := "jr_123"
	client := &mockGlueClient{
		startOut: &glue.StartJobRunOutput{JobRunId: &runID},
	}
	r := NewRunner(WithGlueClient(client))
	meta, err := r.Execute(context.Background(), &types.TriggerConfig{
		Type: types.TriggerGlue,
		Glue: &types.GlueTriggerConfig{JobName: "test-job"},
	})
	require.NoError(t, err)
	assert.Equal(t, "jr_123", meta["glue_job_run_id"])
	assert.Equal(t, "test-job", meta["glue_job_name"])
}

func TestRunner_Execute_GlueType_NilConfig(t *testing.T) {
	r := NewRunner()
	_, err := r.Execute(context.Background(), &types.TriggerConfig{
		Type: types.TriggerGlue,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "glue trigger config is nil")
}

func TestRunner_Execute_EMRType(t *testing.T) {
	client := &mockEMRClient{
		addOut: &emr.AddJobFlowStepsOutput{StepIds: []string{"s-abc"}},
	}
	r := NewRunner(WithEMRClient(client))
	meta, err := r.Execute(context.Background(), &types.TriggerConfig{
		Type: types.TriggerEMR,
		EMR: &types.EMRTriggerConfig{
			ClusterID: "j-1",
			StepName:  "my-step",
			Command:   "s3://bucket/step.jar",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "s-abc", meta["emr_step_id"])
	assert.Equal(t, "j-1", meta["emr_cluster_id"])
}

func TestRunner_Execute_EMRType_NilConfig(t *testing.T) {
	r := NewRunner()
	_, err := r.Execute(context.Background(), &types.TriggerConfig{
		Type: types.TriggerEMR,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "emr trigger config is nil")
}

func TestRunner_Execute_EMRServerlessType(t *testing.T) {
	runID := "run-abc"
	client := &mockEMRServerlessClient{
		startOut: &emrserverless.StartJobRunOutput{JobRunId: &runID},
	}
	r := NewRunner(WithEMRServerlessClient(client))
	meta, err := r.Execute(context.Background(), &types.TriggerConfig{
		Type: types.TriggerEMRServerless,
		EMRServerless: &types.EMRServerlessTriggerConfig{
			ApplicationID: "app-1",
			JobName:       "spark-job",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "run-abc", meta["emr_sl_job_run_id"])
	assert.Equal(t, "app-1", meta["emr_sl_application_id"])
}

func TestRunner_Execute_EMRServerlessType_NilConfig(t *testing.T) {
	r := NewRunner()
	_, err := r.Execute(context.Background(), &types.TriggerConfig{
		Type: types.TriggerEMRServerless,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "emr-serverless trigger config is nil")
}

func TestRunner_Execute_SFNType(t *testing.T) {
	execArn := "arn:aws:states:us-east-1:123:execution:my-sfn:run-1"
	client := &mockSFNClient{
		startOut: &sfn.StartExecutionOutput{ExecutionArn: &execArn},
	}
	r := NewRunner(WithSFNClient(client))
	meta, err := r.Execute(context.Background(), &types.TriggerConfig{
		Type: types.TriggerStepFunction,
		StepFunction: &types.StepFunctionTriggerConfig{
			StateMachineARN: "arn:aws:states:us-east-1:123:stateMachine:my-sfn",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, execArn, meta["sfn_execution_arn"])
}

func TestRunner_Execute_SFNType_NilConfig(t *testing.T) {
	r := NewRunner()
	_, err := r.Execute(context.Background(), &types.TriggerConfig{
		Type: types.TriggerStepFunction,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "step-function trigger config is nil")
}

func TestRunner_Execute_DatabricksType(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"run_id": 12345})
	}))
	defer srv.Close()

	r := NewRunner(WithHTTPClient(srv.Client()))
	meta, err := r.Execute(context.Background(), &types.TriggerConfig{
		Type: types.TriggerDatabricks,
		Databricks: &types.DatabricksTriggerConfig{
			WorkspaceURL: srv.URL,
			JobID:        "my-job",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "12345", meta["databricks_run_id"])
}

func TestRunner_Execute_DatabricksType_NilConfig(t *testing.T) {
	r := NewRunner()
	_, err := r.Execute(context.Background(), &types.TriggerConfig{
		Type: types.TriggerDatabricks,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "databricks trigger config is nil")
}

func TestRunner_Execute_LambdaType(t *testing.T) {
	client := &mockLambdaClient{
		invokeOut: &lambda.InvokeOutput{StatusCode: 200, Payload: []byte(`{"ok":true}`)},
	}
	r := NewRunner(WithLambdaClient(client))
	_, err := r.Execute(context.Background(), &types.TriggerConfig{
		Type:   types.TriggerLambda,
		Lambda: &types.LambdaTriggerConfig{FunctionName: "my-func"},
	})
	require.NoError(t, err)
}

func TestRunner_Execute_LambdaType_NilConfig(t *testing.T) {
	r := NewRunner()
	_, err := r.Execute(context.Background(), &types.TriggerConfig{
		Type: types.TriggerLambda,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "lambda trigger config is nil")
}

// --- Runner.checkAirflowStatus tests ---

func TestRunner_CheckAirflowStatus_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"state": "success",
		})
	}))
	defer srv.Close()

	// Temporarily replace defaultHTTPClient to route requests to the test server.
	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	defer func() { defaultHTTPClient = origClient }()

	r := NewRunner()
	result, err := r.checkAirflowStatus(context.Background(), map[string]interface{}{
		"airflow_url":        srv.URL,
		"airflow_dag_id":     "my_dag",
		"airflow_dag_run_id": "run_1",
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, RunCheckSucceeded, result.State)
	assert.Equal(t, "success", result.Message)
}

func TestRunner_CheckAirflowStatus_Failed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"state": "failed",
		})
	}))
	defer srv.Close()

	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	defer func() { defaultHTTPClient = origClient }()

	r := NewRunner()
	result, err := r.checkAirflowStatus(context.Background(), map[string]interface{}{
		"airflow_url":        srv.URL,
		"airflow_dag_id":     "my_dag",
		"airflow_dag_run_id": "run_1",
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, RunCheckFailed, result.State)
	assert.Equal(t, "failed", result.Message)
}

func TestRunner_CheckAirflowStatus_Running(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"state": "running",
		})
	}))
	defer srv.Close()

	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	defer func() { defaultHTTPClient = origClient }()

	r := NewRunner()
	result, err := r.checkAirflowStatus(context.Background(), map[string]interface{}{
		"airflow_url":        srv.URL,
		"airflow_dag_id":     "my_dag",
		"airflow_dag_run_id": "run_1",
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, RunCheckRunning, result.State)
	assert.Equal(t, "running", result.Message)
}

func TestRunner_CheckAirflowStatus_MissingMetadata(t *testing.T) {
	r := NewRunner()

	// Missing all fields
	result, err := r.checkAirflowStatus(context.Background(), map[string]interface{}{}, nil)
	require.NoError(t, err)
	assert.Equal(t, RunCheckRunning, result.State)
	assert.Equal(t, "missing airflow metadata", result.Message)

	// Missing dag_id and dag_run_id
	result, err = r.checkAirflowStatus(context.Background(), map[string]interface{}{
		"airflow_url": "http://example.com",
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, RunCheckRunning, result.State)
	assert.Equal(t, "missing airflow metadata", result.Message)
}

// --- Runner.CheckStatus dispatch tests for remaining types ---

func TestRunner_CheckStatus_EMRServerlessDispatch(t *testing.T) {
	client := &mockEMRServerlessClient{
		getOut: &emrserverless.GetJobRunOutput{
			JobRun: &emrsltypes.JobRun{
				State: emrsltypes.JobRunStateSuccess,
			},
		},
	}
	r := NewRunner(WithEMRServerlessClient(client))

	result, err := r.CheckStatus(context.Background(), types.TriggerEMRServerless, map[string]interface{}{
		"emr_sl_application_id": "app-1",
		"emr_sl_job_run_id":     "run-1",
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, RunCheckSucceeded, result.State)
}

func TestRunner_CheckStatus_DatabricksDispatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"state": map[string]interface{}{
				"life_cycle_state": "TERMINATED",
				"result_state":     "SUCCESS",
			},
		})
	}))
	defer srv.Close()

	r := NewRunner(WithHTTPClient(srv.Client()))
	result, err := r.CheckStatus(context.Background(), types.TriggerDatabricks, map[string]interface{}{
		"databricks_workspace_url": srv.URL,
		"databricks_run_id":        "999",
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, RunCheckSucceeded, result.State)
}

func TestRunner_CheckStatus_AirflowDispatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"state": "success",
		})
	}))
	defer srv.Close()

	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	defer func() { defaultHTTPClient = origClient }()

	r := NewRunner()
	result, err := r.CheckStatus(context.Background(), types.TriggerAirflow, map[string]interface{}{
		"airflow_url":        srv.URL,
		"airflow_dag_id":     "my_dag",
		"airflow_dag_run_id": "run_1",
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, RunCheckSucceeded, result.State)
}

func TestRunner_CheckStatus_LambdaDispatch(t *testing.T) {
	r := NewRunner()
	result, err := r.CheckStatus(context.Background(), types.TriggerLambda, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, RunCheckRunning, result.State)
	assert.Equal(t, "non-polling trigger type", result.Message)
}

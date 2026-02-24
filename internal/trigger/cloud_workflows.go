package trigger

import (
	"context"
	"encoding/json"
	"fmt"

	executions "cloud.google.com/go/workflows/executions/apiv1"
	executionspb "cloud.google.com/go/workflows/executions/apiv1/executionspb"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// CloudWorkflowsAPI is the subset of the GCP Workflows Executions client used by the trigger package.
type CloudWorkflowsAPI interface {
	CreateExecution(ctx context.Context, req *executionspb.CreateExecutionRequest, opts ...interface{}) (*executionspb.Execution, error)
	GetExecution(ctx context.Context, req *executionspb.GetExecutionRequest, opts ...interface{}) (*executionspb.Execution, error)
}

// cloudWorkflowsClientWrapper wraps the real Workflows Executions client.
type cloudWorkflowsClientWrapper struct {
	client *executions.Client
}

func (w *cloudWorkflowsClientWrapper) CreateExecution(ctx context.Context, req *executionspb.CreateExecutionRequest, _ ...interface{}) (*executionspb.Execution, error) {
	return w.client.CreateExecution(ctx, req)
}

func (w *cloudWorkflowsClientWrapper) GetExecution(ctx context.Context, req *executionspb.GetExecutionRequest, _ ...interface{}) (*executionspb.Execution, error) {
	return w.client.GetExecution(ctx, req)
}

// ExecuteCloudWorkflows starts a Cloud Workflows execution.
func ExecuteCloudWorkflows(ctx context.Context, cfg *types.TriggerConfig, client CloudWorkflowsAPI) (map[string]interface{}, error) {
	if cfg.ProjectID == "" {
		return nil, fmt.Errorf("cloud-workflows trigger: projectId is required")
	}
	if cfg.Region == "" {
		return nil, fmt.Errorf("cloud-workflows trigger: region is required")
	}
	if cfg.WorkflowID == "" {
		return nil, fmt.Errorf("cloud-workflows trigger: workflowId is required")
	}

	parent := fmt.Sprintf("projects/%s/locations/%s/workflows/%s", cfg.ProjectID, cfg.Region, cfg.WorkflowID)

	execution := &executionspb.Execution{}
	if len(cfg.Arguments) > 0 {
		b, err := json.Marshal(cfg.Arguments)
		if err != nil {
			return nil, fmt.Errorf("cloud-workflows trigger: marshaling arguments: %w", err)
		}
		execution.Argument = string(b)
	}

	out, err := client.CreateExecution(ctx, &executionspb.CreateExecutionRequest{
		Parent:    parent,
		Execution: execution,
	})
	if err != nil {
		return nil, fmt.Errorf("cloud-workflows trigger: CreateExecution failed: %w", err)
	}

	meta := map[string]interface{}{
		"cloud_workflows_execution_name": out.Name,
	}
	return meta, nil
}

// checkCloudWorkflowsStatus checks the status of a Cloud Workflows execution.
func (r *Runner) checkCloudWorkflowsStatus(ctx context.Context, metadata map[string]interface{}) (StatusResult, error) {
	execName, _ := metadata["cloud_workflows_execution_name"].(string)
	if execName == "" {
		return StatusResult{State: RunCheckRunning, Message: "missing cloud-workflows metadata"}, nil
	}

	client, err := r.getCloudWorkflowsClient()
	if err != nil {
		return StatusResult{}, fmt.Errorf("cloud-workflows status: getting client: %w", err)
	}

	out, err := client.GetExecution(ctx, &executionspb.GetExecutionRequest{
		Name: execName,
	})
	if err != nil {
		return StatusResult{}, fmt.Errorf("cloud-workflows status: GetExecution failed: %w", err)
	}

	state := out.State
	switch state {
	case executionspb.Execution_SUCCEEDED:
		return StatusResult{State: RunCheckSucceeded, Message: state.String()}, nil
	case executionspb.Execution_FAILED, executionspb.Execution_CANCELLED:
		return StatusResult{State: RunCheckFailed, Message: state.String()}, nil
	default:
		return StatusResult{State: RunCheckRunning, Message: state.String()}, nil
	}
}

func (r *Runner) getCloudWorkflowsClient() (CloudWorkflowsAPI, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cloudWorkflowsClient != nil {
		return r.cloudWorkflowsClient, nil
	}
	client, err := executions.NewClient(context.Background())
	if err != nil {
		return nil, fmt.Errorf("creating Cloud Workflows client: %w", err)
	}
	r.cloudWorkflowsClient = &cloudWorkflowsClientWrapper{client: client}
	return r.cloudWorkflowsClient, nil
}

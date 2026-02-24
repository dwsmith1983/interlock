package trigger

import (
	"context"
	"fmt"

	dataproc "cloud.google.com/go/dataproc/v2/apiv1"
	dataprocpb "cloud.google.com/go/dataproc/v2/apiv1/dataprocpb"
	"github.com/dwsmith1983/interlock/pkg/types"
	"google.golang.org/api/option"
)

// DataprocServerlessAPI is the subset of the GCP Dataproc Batches client used by the trigger package.
type DataprocServerlessAPI interface {
	CreateBatch(ctx context.Context, req *dataprocpb.CreateBatchRequest, opts ...interface{}) (string, error)
	GetBatch(ctx context.Context, req *dataprocpb.GetBatchRequest, opts ...interface{}) (*dataprocpb.Batch, error)
}

// dataprocBatchClientWrapper wraps the real BatchController client.
type dataprocBatchClientWrapper struct {
	client *dataproc.BatchControllerClient
}

func (w *dataprocBatchClientWrapper) CreateBatch(ctx context.Context, req *dataprocpb.CreateBatchRequest, _ ...interface{}) (string, error) {
	op, err := w.client.CreateBatch(ctx, req)
	if err != nil {
		return "", err
	}
	// The operation metadata contains the batch name. We return the batch ID from the request.
	_ = op // Long-running operation — we don't wait, just track batch ID.
	return req.BatchId, nil
}

func (w *dataprocBatchClientWrapper) GetBatch(ctx context.Context, req *dataprocpb.GetBatchRequest, _ ...interface{}) (*dataprocpb.Batch, error) {
	return w.client.GetBatch(ctx, req)
}

// ExecuteDataprocServerless creates a serverless batch job.
func ExecuteDataprocServerless(ctx context.Context, cfg *types.TriggerConfig, client DataprocServerlessAPI) (map[string]interface{}, error) {
	if cfg.ProjectID == "" {
		return nil, fmt.Errorf("dataproc-serverless trigger: projectId is required")
	}
	if cfg.Region == "" {
		return nil, fmt.Errorf("dataproc-serverless trigger: region is required")
	}
	if cfg.JobName == "" {
		return nil, fmt.Errorf("dataproc-serverless trigger: jobName is required")
	}

	args := make([]string, 0, len(cfg.Arguments))
	for k, v := range cfg.Arguments {
		args = append(args, k+"="+v)
	}

	batch := &dataprocpb.Batch{
		BatchConfig: &dataprocpb.Batch_SparkBatch{
			SparkBatch: &dataprocpb.SparkBatch{
				Driver: &dataprocpb.SparkBatch_MainJarFileUri{
					MainJarFileUri: cfg.Command,
				},
				Args: args,
			},
		},
	}

	parent := fmt.Sprintf("projects/%s/locations/%s", cfg.ProjectID, cfg.Region)
	batchID, err := client.CreateBatch(ctx, &dataprocpb.CreateBatchRequest{
		Parent:  parent,
		BatchId: cfg.JobName,
		Batch:   batch,
	})
	if err != nil {
		return nil, fmt.Errorf("dataproc-serverless trigger: CreateBatch failed: %w", err)
	}

	meta := map[string]interface{}{
		"dataproc_sl_project_id": cfg.ProjectID,
		"dataproc_sl_region":     cfg.Region,
		"dataproc_sl_batch_id":   batchID,
	}
	return meta, nil
}

// checkDataprocServerlessStatus checks the status of a Dataproc Serverless batch.
func (r *Runner) checkDataprocServerlessStatus(ctx context.Context, metadata map[string]interface{}) (StatusResult, error) {
	projectID, _ := metadata["dataproc_sl_project_id"].(string)
	region, _ := metadata["dataproc_sl_region"].(string)
	batchID, _ := metadata["dataproc_sl_batch_id"].(string)
	if projectID == "" || region == "" || batchID == "" {
		return StatusResult{State: RunCheckRunning, Message: "missing dataproc-serverless metadata"}, nil
	}

	client, err := r.getDataprocServerlessClient(region)
	if err != nil {
		return StatusResult{}, fmt.Errorf("dataproc-serverless status: getting client: %w", err)
	}

	name := fmt.Sprintf("projects/%s/locations/%s/batches/%s", projectID, region, batchID)
	out, err := client.GetBatch(ctx, &dataprocpb.GetBatchRequest{
		Name: name,
	})
	if err != nil {
		return StatusResult{}, fmt.Errorf("dataproc-serverless status: GetBatch failed: %w", err)
	}

	state := out.State
	switch state {
	case dataprocpb.Batch_SUCCEEDED:
		return StatusResult{State: RunCheckSucceeded, Message: state.String()}, nil
	case dataprocpb.Batch_FAILED, dataprocpb.Batch_CANCELLED:
		return StatusResult{State: RunCheckFailed, Message: state.String()}, nil
	default:
		return StatusResult{State: RunCheckRunning, Message: state.String()}, nil
	}
}

func (r *Runner) getDataprocServerlessClient(region string) (DataprocServerlessAPI, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.dataprocSLClient != nil {
		return r.dataprocSLClient, nil
	}
	opts := []option.ClientOption{}
	if region != "" {
		opts = append(opts, option.WithEndpoint(region+"-dataproc.googleapis.com:443"))
	}
	client, err := dataproc.NewBatchControllerClient(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("creating Dataproc Batch client: %w", err)
	}
	r.dataprocSLClient = &dataprocBatchClientWrapper{client: client}
	return r.dataprocSLClient, nil
}

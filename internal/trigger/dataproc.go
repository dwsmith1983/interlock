package trigger

import (
	"context"
	"fmt"

	dataproc "cloud.google.com/go/dataproc/v2/apiv1"
	dataprocpb "cloud.google.com/go/dataproc/v2/apiv1/dataprocpb"
	"github.com/dwsmith1983/interlock/pkg/types"
	"google.golang.org/api/option"
)

// DataprocAPI is the subset of the GCP Dataproc JobController client used by the trigger package.
type DataprocAPI interface {
	SubmitJob(ctx context.Context, req *dataprocpb.SubmitJobRequest, opts ...interface{}) (*dataprocpb.Job, error)
	GetJob(ctx context.Context, req *dataprocpb.GetJobRequest, opts ...interface{}) (*dataprocpb.Job, error)
}

// dataprocClientWrapper wraps the real Dataproc client to satisfy DataprocAPI.
type dataprocClientWrapper struct {
	client *dataproc.JobControllerClient
}

func (w *dataprocClientWrapper) SubmitJob(ctx context.Context, req *dataprocpb.SubmitJobRequest, _ ...interface{}) (*dataprocpb.Job, error) {
	return w.client.SubmitJob(ctx, req)
}

func (w *dataprocClientWrapper) GetJob(ctx context.Context, req *dataprocpb.GetJobRequest, _ ...interface{}) (*dataprocpb.Job, error) {
	return w.client.GetJob(ctx, req)
}

// ExecuteDataproc submits a Spark/Hadoop job to an existing Dataproc cluster.
func ExecuteDataproc(ctx context.Context, cfg *types.TriggerConfig, client DataprocAPI) (map[string]interface{}, error) {
	if cfg.ProjectID == "" {
		return nil, fmt.Errorf("dataproc trigger: projectId is required")
	}
	if cfg.Region == "" {
		return nil, fmt.Errorf("dataproc trigger: region is required")
	}
	if cfg.ClusterID == "" {
		return nil, fmt.Errorf("dataproc trigger: clusterId is required")
	}
	if cfg.JobName == "" {
		return nil, fmt.Errorf("dataproc trigger: jobName is required")
	}

	args := make([]string, 0, len(cfg.Arguments))
	for k, v := range cfg.Arguments {
		args = append(args, k+"="+v)
	}

	job := &dataprocpb.Job{
		Placement: &dataprocpb.JobPlacement{
			ClusterName: cfg.ClusterID,
		},
		Reference: &dataprocpb.JobReference{
			ProjectId: cfg.ProjectID,
		},
		TypeJob: &dataprocpb.Job_SparkJob{
			SparkJob: &dataprocpb.SparkJob{
				Driver: &dataprocpb.SparkJob_MainJarFileUri{
					MainJarFileUri: cfg.Command,
				},
				Args: args,
			},
		},
	}

	out, err := client.SubmitJob(ctx, &dataprocpb.SubmitJobRequest{
		ProjectId: cfg.ProjectID,
		Region:    cfg.Region,
		Job:       job,
	})
	if err != nil {
		return nil, fmt.Errorf("dataproc trigger: SubmitJob failed: %w", err)
	}

	jobID := ""
	if out.Reference != nil {
		jobID = out.Reference.JobId
	}

	meta := map[string]interface{}{
		"dataproc_project_id": cfg.ProjectID,
		"dataproc_region":     cfg.Region,
		"dataproc_job_id":     jobID,
	}
	return meta, nil
}

// checkDataprocStatus checks the status of a Dataproc job.
func (r *Runner) checkDataprocStatus(ctx context.Context, metadata map[string]interface{}) (StatusResult, error) {
	projectID, _ := metadata["dataproc_project_id"].(string)
	region, _ := metadata["dataproc_region"].(string)
	jobID, _ := metadata["dataproc_job_id"].(string)
	if projectID == "" || region == "" || jobID == "" {
		return StatusResult{State: RunCheckRunning, Message: "missing dataproc metadata"}, nil
	}

	client, err := r.getDataprocClient(region)
	if err != nil {
		return StatusResult{}, fmt.Errorf("dataproc status: getting client: %w", err)
	}

	out, err := client.GetJob(ctx, &dataprocpb.GetJobRequest{
		ProjectId: projectID,
		Region:    region,
		JobId:     jobID,
	})
	if err != nil {
		return StatusResult{}, fmt.Errorf("dataproc status: GetJob failed: %w", err)
	}

	state := out.Status.State
	switch state {
	case dataprocpb.JobStatus_DONE:
		return StatusResult{State: RunCheckSucceeded, Message: state.String()}, nil
	case dataprocpb.JobStatus_ERROR, dataprocpb.JobStatus_CANCELLED:
		return StatusResult{State: RunCheckFailed, Message: state.String()}, nil
	default:
		return StatusResult{State: RunCheckRunning, Message: state.String()}, nil
	}
}

func (r *Runner) getDataprocClient(region string) (DataprocAPI, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.dataprocClient != nil {
		return r.dataprocClient, nil
	}
	opts := []option.ClientOption{}
	if region != "" {
		opts = append(opts, option.WithEndpoint(region+"-dataproc.googleapis.com:443"))
	}
	client, err := dataproc.NewJobControllerClient(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("creating Dataproc client: %w", err)
	}
	r.dataprocClient = &dataprocClientWrapper{client: client}
	return r.dataprocClient, nil
}

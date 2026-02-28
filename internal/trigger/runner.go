package trigger

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/emr"
	"github.com/aws/aws-sdk-go-v2/service/emrserverless"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// Runner holds injectable AWS SDK clients and dispatches trigger execution
// and status checking across all supported trigger types.
type Runner struct {
	httpClient *http.Client

	mu sync.Mutex

	glueClient  GlueAPI
	emrClient   EMRAPI
	emrSLClient EMRServerlessAPI
	sfnClient   SFNAPI
}

// RunnerOption configures a Runner.
type RunnerOption func(*Runner)

// WithGlueClient sets a custom Glue client (useful for testing).
func WithGlueClient(c GlueAPI) RunnerOption {
	return func(r *Runner) { r.glueClient = c }
}

// WithEMRClient sets a custom EMR client.
func WithEMRClient(c EMRAPI) RunnerOption {
	return func(r *Runner) { r.emrClient = c }
}

// WithEMRServerlessClient sets a custom EMR Serverless client.
func WithEMRServerlessClient(c EMRServerlessAPI) RunnerOption {
	return func(r *Runner) { r.emrSLClient = c }
}

// WithSFNClient sets a custom Step Functions client.
func WithSFNClient(c SFNAPI) RunnerOption {
	return func(r *Runner) { r.sfnClient = c }
}

// WithHTTPClient sets a custom HTTP client for HTTP-based triggers.
func WithHTTPClient(c *http.Client) RunnerOption {
	return func(r *Runner) { r.httpClient = c }
}

// NewRunner creates a Runner with the given options.
func NewRunner(opts ...RunnerOption) *Runner {
	r := &Runner{
		httpClient: defaultHTTPClient,
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Execute dispatches trigger execution based on the trigger type.
func (r *Runner) Execute(ctx context.Context, cfg *types.TriggerConfig) (map[string]interface{}, error) {
	if cfg == nil {
		return nil, fmt.Errorf("no trigger configured")
	}

	switch cfg.Type {
	case types.TriggerCommand:
		if cfg.Command == nil {
			return nil, fmt.Errorf("command trigger config is nil")
		}
		return nil, ExecuteCommand(ctx, cfg.Command.Command)
	case types.TriggerHTTP:
		if cfg.HTTP == nil {
			return nil, fmt.Errorf("http trigger config is nil")
		}
		return nil, ExecuteHTTP(ctx, cfg.HTTP)
	case types.TriggerAirflow:
		if cfg.Airflow == nil {
			return nil, fmt.Errorf("airflow trigger config is nil")
		}
		return ExecuteAirflow(ctx, cfg.Airflow)
	case types.TriggerGlue:
		if cfg.Glue == nil {
			return nil, fmt.Errorf("glue trigger config is nil")
		}
		client, err := r.getGlueClient("")
		if err != nil {
			return nil, err
		}
		return ExecuteGlue(ctx, cfg.Glue, client)
	case types.TriggerEMR:
		if cfg.EMR == nil {
			return nil, fmt.Errorf("emr trigger config is nil")
		}
		client, err := r.getEMRClient("")
		if err != nil {
			return nil, err
		}
		return ExecuteEMR(ctx, cfg.EMR, client)
	case types.TriggerEMRServerless:
		if cfg.EMRServerless == nil {
			return nil, fmt.Errorf("emr-serverless trigger config is nil")
		}
		client, err := r.getEMRServerlessClient("")
		if err != nil {
			return nil, err
		}
		return ExecuteEMRServerless(ctx, cfg.EMRServerless, client)
	case types.TriggerStepFunction:
		if cfg.StepFunction == nil {
			return nil, fmt.Errorf("step-function trigger config is nil")
		}
		client, err := r.getSFNClient("")
		if err != nil {
			return nil, err
		}
		return ExecuteSFN(ctx, cfg.StepFunction, client)
	case types.TriggerDatabricks:
		if cfg.Databricks == nil {
			return nil, fmt.Errorf("databricks trigger config is nil")
		}
		return ExecuteDatabricks(ctx, cfg.Databricks, r.httpClient)
	default:
		return nil, fmt.Errorf("unknown trigger type: %s", cfg.Type)
	}
}

// CheckStatus checks the status of an active run for the given trigger type.
// It uses run metadata to identify the remote job and returns a normalized StatusResult.
func (r *Runner) CheckStatus(ctx context.Context, triggerType types.TriggerType, metadata map[string]interface{}, headers map[string]string) (StatusResult, error) {
	switch triggerType {
	case types.TriggerAirflow:
		return r.checkAirflowStatus(ctx, metadata, headers)
	case types.TriggerGlue:
		return r.checkGlueStatus(ctx, metadata)
	case types.TriggerEMR:
		return r.checkEMRStatus(ctx, metadata)
	case types.TriggerEMRServerless:
		return r.checkEMRServerlessStatus(ctx, metadata)
	case types.TriggerStepFunction:
		return r.checkSFNStatus(ctx, metadata)
	case types.TriggerDatabricks:
		return r.checkDatabricksStatus(ctx, metadata, headers)
	case types.TriggerCommand, types.TriggerHTTP:
		// Non-polling types — if they're RUNNING they stay that way until callback
		return StatusResult{State: RunCheckRunning, Message: "non-polling trigger type"}, nil
	default:
		return StatusResult{}, fmt.Errorf("unknown trigger type for status check: %s", triggerType)
	}
}

// checkAirflowStatus wraps the existing CheckAirflowStatus function into the Runner pattern.
func (r *Runner) checkAirflowStatus(ctx context.Context, metadata map[string]interface{}, headers map[string]string) (StatusResult, error) {
	airflowURL, _ := metadata["airflow_url"].(string)
	dagID, _ := metadata["airflow_dag_id"].(string)
	dagRunID, _ := metadata["airflow_dag_run_id"].(string)
	if airflowURL == "" || dagID == "" || dagRunID == "" {
		return StatusResult{State: RunCheckRunning, Message: "missing airflow metadata"}, nil
	}

	state, err := CheckAirflowStatus(ctx, airflowURL, dagID, dagRunID, headers)
	if err != nil {
		return StatusResult{}, err
	}

	switch state {
	case "success":
		return StatusResult{State: RunCheckSucceeded, Message: state}, nil
	case "failed":
		return StatusResult{State: RunCheckFailed, Message: state}, nil
	default:
		return StatusResult{State: RunCheckRunning, Message: state}, nil
	}
}

func (r *Runner) getGlueClient(region string) (GlueAPI, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.glueClient != nil {
		return r.glueClient, nil
	}
	opts := []func(*glue.Options){}
	if region != "" {
		opts = append(opts, func(o *glue.Options) { o.Region = region })
	}
	cfg, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}
	r.glueClient = glue.NewFromConfig(cfg, opts...)
	return r.glueClient, nil
}

func (r *Runner) getEMRClient(region string) (EMRAPI, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.emrClient != nil {
		return r.emrClient, nil
	}
	opts := []func(*emr.Options){}
	if region != "" {
		opts = append(opts, func(o *emr.Options) { o.Region = region })
	}
	cfg, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}
	r.emrClient = emr.NewFromConfig(cfg, opts...)
	return r.emrClient, nil
}

func (r *Runner) getEMRServerlessClient(region string) (EMRServerlessAPI, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.emrSLClient != nil {
		return r.emrSLClient, nil
	}
	opts := []func(*emrserverless.Options){}
	if region != "" {
		opts = append(opts, func(o *emrserverless.Options) { o.Region = region })
	}
	cfg, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}
	r.emrSLClient = emrserverless.NewFromConfig(cfg, opts...)
	return r.emrSLClient, nil
}

func (r *Runner) getSFNClient(region string) (SFNAPI, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sfnClient != nil {
		return r.sfnClient, nil
	}
	opts := []func(*sfn.Options){}
	if region != "" {
		opts = append(opts, func(o *sfn.Options) { o.Region = region })
	}
	cfg, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}
	r.sfnClient = sfn.NewFromConfig(cfg, opts...)
	return r.sfnClient, nil
}

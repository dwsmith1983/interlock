package lambda

// runIDMetadataKeys lists the trigger-metadata keys that carry a remote run
// identifier, in priority order. The first six are the keys emitted by the
// executors in internal/trigger; the remaining generic keys are retained for
// backward compatibility with records written by older versions.
var runIDMetadataKeys = []string{
	"glue_job_run_id",
	"emr_step_id",
	"emr_sl_job_run_id",
	"sfn_execution_arn",
	"airflow_dag_run_id",
	"databricks_run_id",
	"runId",
	"jobRunId",
	"executionArn",
	"stepId",
	"dagRunId",
}

// ExtractRunID searches trigger metadata for a recognisable run identifier.
// A nil map is safe: indexing a nil map returns the zero value.
func ExtractRunID(metadata map[string]interface{}) string {
	for _, key := range runIDMetadataKeys {
		v, ok := metadata[key]
		if !ok {
			continue
		}
		if s, ok := v.(string); ok && s != "" {
			return s
		}
	}
	return ""
}

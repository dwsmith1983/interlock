// Package trigger implements pipeline trigger execution and status polling
// for nine trigger types:
//
//   - command: local shell command execution
//   - http: HTTP/webhook trigger with configurable method, headers, and body
//   - airflow: Apache Airflow DAG trigger via REST API
//   - glue: AWS Glue job trigger via SDK
//   - emr: Amazon EMR step submission via SDK
//   - emr-serverless: Amazon EMR Serverless job run via SDK
//   - step-function: AWS Step Functions execution via SDK
//   - databricks: Databricks job run via REST API
//   - lambda: AWS Lambda direct invocation via SDK
//
// The [Runner] struct provides dependency injection for AWS SDK clients
// using functional options ([WithGlueClient], [WithEMRClient], etc.).
// Status polling is handled by [Runner.CheckStatus], which normalizes
// provider-specific states into [RunCheckRunning], [RunCheckSucceeded],
// or [RunCheckFailed].
//
// Errors from trigger execution are wrapped in [TriggerError] with a
// [types.FailureCategory] for retry decisions. [ClassifyFailure] inspects
// any error and returns the appropriate category.
package trigger

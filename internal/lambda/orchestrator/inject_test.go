package orchestrator_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sfn"

	"github.com/dwsmith1983/interlock/internal/lambda/orchestrator"
	"github.com/dwsmith1983/interlock/internal/trigger"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestInjectDateArgs: every job learns the date it runs for, so a rerun or a
// retry of an earlier date processes that date. Map-argument jobs get the
// date keys, body jobs with nothing configured the generated run object.
func TestInjectDateArgs(t *testing.T) {
	run := func(date string) types.RunIdentity {
		return types.RunIdentity{PipelineID: "p", ScheduleID: "s", Date: date, RunID: "exec-1"}
	}
	daily, hourly := "2026-03-03", "2026-03-03T07"
	dailyObj := map[string]string{"par_day": "20260303", "pipelineId": "p", "scheduleId": "s", "date": daily, "runId": "exec-1"}
	hourlyObj := map[string]string{"par_day": "20260303", "par_hour": "07", "pipelineId": "p", "scheduleId": "s",
		"date": hourly, "runId": "exec-1"}

	argTests := []struct {
		name string
		tc   types.TriggerConfig
		date string
		args func(types.TriggerConfig) map[string]string
		want map[string]string
	}{
		{name: "glue daily creates the map", date: daily,
			tc:   types.TriggerConfig{Glue: &types.GlueTriggerConfig{JobName: "j"}},
			args: func(tc types.TriggerConfig) map[string]string { return tc.Glue.Arguments },
			want: map[string]string{"--par_day": "20260303"}},
		{name: "glue hourly overwrites a configured key", date: hourly,
			tc: types.TriggerConfig{Glue: &types.GlueTriggerConfig{
				Arguments: map[string]string{"--par_day": "19700101", "--env": "prod"}}},
			args: func(tc types.TriggerConfig) map[string]string { return tc.Glue.Arguments },
			want: map[string]string{"--par_day": "20260303", "--par_hour": "07", "--env": "prod"}},
		{name: "step-function daily creates the map", date: daily,
			tc:   types.TriggerConfig{StepFunction: &types.StepFunctionTriggerConfig{StateMachineARN: "arn"}},
			args: func(tc types.TriggerConfig) map[string]string { return tc.StepFunction.Arguments },
			want: map[string]string{"par_day": "20260303"}},
		{name: "step-function hourly overwrites a configured key", date: hourly,
			tc: types.TriggerConfig{StepFunction: &types.StepFunctionTriggerConfig{
				Arguments: map[string]string{"par_day": "19700101", "env": "prod"}}},
			args: func(tc types.TriggerConfig) map[string]string { return tc.StepFunction.Arguments },
			want: map[string]string{"par_day": "20260303", "par_hour": "07", "env": "prod"}},
		{name: "emr daily creates the map", date: daily,
			tc:   types.TriggerConfig{EMR: &types.EMRTriggerConfig{ClusterID: "j-1"}},
			args: func(tc types.TriggerConfig) map[string]string { return tc.EMR.Arguments },
			want: map[string]string{"--par_day": "20260303"}},
		{name: "emr hourly overwrites a configured key", date: hourly,
			tc: types.TriggerConfig{EMR: &types.EMRTriggerConfig{
				Arguments: map[string]string{"--par_hour": "99", "--input": "s3://in"}}},
			args: func(tc types.TriggerConfig) map[string]string { return tc.EMR.Arguments },
			want: map[string]string{"--par_day": "20260303", "--par_hour": "07", "--input": "s3://in"}},
		{name: "databricks daily creates the map", date: daily,
			tc:   types.TriggerConfig{Databricks: &types.DatabricksTriggerConfig{WorkspaceURL: "https://dbc", JobID: "1"}},
			args: func(tc types.TriggerConfig) map[string]string { return tc.Databricks.Arguments },
			want: map[string]string{"par_day": "20260303"}},
		{name: "databricks hourly overwrites a configured key", date: hourly,
			tc: types.TriggerConfig{Databricks: &types.DatabricksTriggerConfig{
				Arguments: map[string]string{"par_hour": "99", "env": "prod"}}},
			args: func(tc types.TriggerConfig) map[string]string { return tc.Databricks.Arguments },
			want: map[string]string{"par_day": "20260303", "par_hour": "07", "env": "prod"}},
	}
	for _, tt := range argTests {
		t.Run(tt.name, func(t *testing.T) {
			orchestrator.InjectDateArgs(&tt.tc, tt.date)
			if got := tt.args(tt.tc); !maps.Equal(got, tt.want) {
				t.Errorf("arguments = %v, want %v", got, tt.want)
			}
		})
	}

	// body returns the payload or body and whether it is marked generated.
	lambdaBody := func(tc types.TriggerConfig) (string, bool) { return tc.Lambda.Payload, tc.Lambda.PayloadGenerated }
	airflowBody := func(tc types.TriggerConfig) (string, bool) { return tc.Airflow.Body, tc.Airflow.BodyGenerated }
	httpBody := func(tc types.TriggerConfig) (string, bool) { return tc.HTTP.Body, tc.HTTP.BodyGenerated }
	bodyTests := []struct {
		name       string
		tc         types.TriggerConfig
		date       string
		body       func(types.TriggerConfig) (string, bool)
		wantObj    map[string]string // the generated object; nil when the configured text must stay
		wantConfig string
	}{
		{name: "lambda daily generated", date: daily, body: lambdaBody, wantObj: dailyObj,
			tc: types.TriggerConfig{Lambda: &types.LambdaTriggerConfig{FunctionName: "f", Run: run(daily)}}},
		{name: "lambda hourly generated", date: hourly, body: lambdaBody, wantObj: hourlyObj,
			tc: types.TriggerConfig{Lambda: &types.LambdaTriggerConfig{FunctionName: "f", Run: run(hourly)}}},
		{name: "lambda configured payload unchanged", date: daily, body: lambdaBody,
			wantConfig: `{"d":"${INTERLOCK_DATE}"}`,
			tc: types.TriggerConfig{Lambda: &types.LambdaTriggerConfig{FunctionName: "f", Run: run(daily),
				Payload: `{"d":"${INTERLOCK_DATE}"}`}}},
		{name: "airflow daily generated", date: daily, body: airflowBody, wantObj: dailyObj,
			tc: types.TriggerConfig{Airflow: &types.AirflowTriggerConfig{DagID: "d", Run: run(daily)}}},
		{name: "airflow hourly generated", date: hourly, body: airflowBody, wantObj: hourlyObj,
			tc: types.TriggerConfig{Airflow: &types.AirflowTriggerConfig{DagID: "d", Run: run(hourly)}}},
		{name: "airflow configured body unchanged", date: hourly, body: airflowBody,
			wantConfig: `{"r":"${INTERLOCK_RUN_ID}"}`,
			tc: types.TriggerConfig{Airflow: &types.AirflowTriggerConfig{DagID: "d", Run: run(hourly),
				Body: `{"r":"${INTERLOCK_RUN_ID}"}`}}},
		{name: "http configured body unchanged", date: daily, body: httpBody,
			wantConfig: `{"custom": "payload"}`,
			tc: types.TriggerConfig{HTTP: &types.HTTPTriggerConfig{Method: "POST", URL: "https://example.com/custom",
				Body: `{"custom": "payload"}`}}},
	}
	for _, tt := range bodyTests {
		t.Run(tt.name, func(t *testing.T) {
			orchestrator.InjectDateArgs(&tt.tc, tt.date)
			got, generated := tt.body(tt.tc)
			if tt.wantObj == nil {
				if got != tt.wantConfig || generated {
					t.Errorf("body = %q (generated %v), want %q unchanged and not generated", got, generated, tt.wantConfig)
				}
				return
			}
			var obj map[string]string
			if err := json.Unmarshal([]byte(got), &obj); err != nil || !maps.Equal(obj, tt.wantObj) {
				t.Errorf("generated body = %s (%v), want %v", got, err, tt.wantObj)
			}
			if !generated {
				t.Error("generated body not marked generated: it would go through variable expansion")
			}
		})
	}
}

// TestInjectDateArgs_EMRServerless: the date reaches an emr-serverless job as
// --par_day and --par_hour entry point arguments, each flag followed by its
// value; a configured flag keeps its place and takes the run's value.
func TestInjectDateArgs_EMRServerless(t *testing.T) {
	tests := []struct {
		name string
		date string
		args []string
		want []string
	}{
		{name: "daily without arguments", date: "2026-03-03",
			want: []string{"--par_day", "20260303"}},
		{name: "hourly appends after configured arguments", date: "2026-03-03T07",
			args: []string{"--input", "s3://in"},
			want: []string{"--input", "s3://in", "--par_day", "20260303", "--par_hour", "07"}},
		{name: "configured flag takes the run's value", date: "2026-03-03T07",
			args: []string{"--par_hour", "99", "--input", "s3://in"},
			want: []string{"--par_hour", "07", "--input", "s3://in", "--par_day", "20260303"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configured := slices.Clone(tt.args)
			tc := types.TriggerConfig{EMRServerless: &types.EMRServerlessTriggerConfig{EntryPointArguments: tt.args}}
			orchestrator.InjectDateArgs(&tc, tt.date)
			if got := tc.EMRServerless.EntryPointArguments; !slices.Equal(got, tt.want) {
				t.Errorf("entry point arguments = %q, want %q", got, tt.want)
			}
			if !slices.Equal(tt.args, configured) {
				t.Errorf("configured arguments changed to %q, want %q", tt.args, configured)
			}
		})
	}
}

// TestInjectDateArgs_NoSubConfig: a config with no sub-config for its type is
// left as it is.
func TestInjectDateArgs_NoSubConfig(t *testing.T) {
	tc := types.TriggerConfig{Type: types.TriggerLambda}
	orchestrator.InjectDateArgs(&tc, "2026-03-05")
	if !reflect.DeepEqual(tc, types.TriggerConfig{Type: types.TriggerLambda}) {
		t.Errorf("config = %+v, want it unchanged", tc)
	}
}

// sfnRecorder is a trigger.SFNAPI that keeps the StartExecution input.
type sfnRecorder struct{ in *sfn.StartExecutionInput }

func (r *sfnRecorder) StartExecution(_ context.Context, in *sfn.StartExecutionInput, _ ...func(*sfn.Options)) (*sfn.StartExecutionOutput, error) {
	r.in = in
	return &sfn.StartExecutionOutput{ExecutionArn: aws.String("arn:exec")}, nil
}

func (r *sfnRecorder) DescribeExecution(context.Context, *sfn.DescribeExecutionInput, ...func(*sfn.Options)) (*sfn.DescribeExecutionOutput, error) {
	return &sfn.DescribeExecutionOutput{}, nil
}

// The date keys are top-level keys of the step function's execution input.
func TestInjectDateArgs_StepFunctionInput(t *testing.T) {
	tc := types.TriggerConfig{Type: types.TriggerStepFunction, StepFunction: &types.StepFunctionTriggerConfig{StateMachineARN: "arn:sm"}}
	orchestrator.InjectDateArgs(&tc, "2026-03-03T07")
	client := &sfnRecorder{}
	if _, err := trigger.ExecuteSFN(context.Background(), tc.StepFunction, client); err != nil {
		t.Fatalf("ExecuteSFN: %v", err)
	}
	var input map[string]string
	if client.in.Input == nil || json.Unmarshal([]byte(*client.in.Input), &input) != nil ||
		!maps.Equal(input, map[string]string{"par_day": "20260303", "par_hour": "07"}) {
		t.Errorf("execution input = %v, want par_day 20260303 and par_hour 07", aws.ToString(client.in.Input))
	}
}

// The date keys reach a databricks job as job parameters of its run-now call.
func TestInjectDateArgs_Databricks(t *testing.T) {
	var body string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		body = string(b)
		_, _ = w.Write([]byte(`{"run_id": 77}`))
	}))
	defer srv.Close()
	tc := types.TriggerConfig{Type: types.TriggerDatabricks, Databricks: &types.DatabricksTriggerConfig{
		WorkspaceURL: srv.URL, JobID: "12345", Arguments: map[string]string{"env": "prod"},
		Run: types.RunIdentity{RunID: "exec-1"}}}

	orchestrator.InjectDateArgs(&tc, "2026-03-11T05")
	if _, err := trigger.ExecuteDatabricks(context.Background(), tc.Databricks, srv.Client()); err != nil {
		t.Fatalf("ExecuteDatabricks: %v", err)
	}

	token := sha256.Sum256([]byte("exec-1"))
	want := `{"job_id":12345,"idempotency_token":"` + hex.EncodeToString(token[:]) +
		`","job_parameters":{"env":"prod","par_day":"20260311","par_hour":"05"}}`
	if body != want {
		t.Errorf("run-now body = %s, want %s", body, want)
	}
}

package deploy_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/lambda/orchestrator"
	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// --- JSONPath resolution ---------------------------------------------------

// pathRefRE matches an ASL reference path such as $.a.b[0].c. Used to pull the
// arguments out of intrinsic calls like States.MathAdd($.a, $.b).
var pathRefRE = regexp.MustCompile(`\$(?:\.[A-Za-z0-9_]+(?:\[\d+\])*)+`)

// contextPaths are the context-object paths the ASL may read. Step Functions
// always supplies them; any other "$$." path fails the contract until it is
// checked and added here.
var contextPaths = map[string]bool{
	"$$.Execution.Name":      true,
	"$$.Execution.StartTime": true,
}

// resolvePath walks a reference path against a decoded JSON document and
// reports whether it exists. Supports "$", "$.a.b", "$.a[0].b" and the
// context paths in contextPaths.
func resolvePath(doc interface{}, path string) bool {
	if path == "$" {
		return true
	}
	if strings.HasPrefix(path, "$$.") {
		return contextPaths[path]
	}
	if !strings.HasPrefix(path, "$.") {
		return false
	}
	cur := doc
	for _, seg := range strings.Split(strings.TrimPrefix(path, "$."), ".") {
		name, indices := splitSegment(seg)
		if name != "" {
			m, ok := cur.(map[string]interface{})
			if !ok {
				return false
			}
			v, ok := m[name]
			if !ok {
				return false
			}
			cur = v
		}
		for _, idx := range indices {
			arr, ok := cur.([]interface{})
			if !ok || idx < 0 || idx >= len(arr) {
				return false
			}
			cur = arr[idx]
		}
	}
	return true
}

// splitSegment splits "items[0][1]" into ("items", []int{0, 1}).
func splitSegment(seg string) (name string, indices []int) {
	name = seg
	if i := strings.Index(seg, "["); i >= 0 {
		name = seg[:i]
		for _, raw := range strings.Split(strings.Trim(seg[i:], "[]"), "][") {
			n, err := strconv.Atoi(raw)
			if err != nil {
				continue
			}
			indices = append(indices, n)
		}
	}
	return name, indices
}

// --- ASL reference collection ----------------------------------------------

// aslState is the subset of a state definition that carries reference paths.
type aslState struct {
	Type        string                   `json:"Type"`
	Parameters  map[string]interface{}   `json:"Parameters"`
	SecondsPath string                   `json:"SecondsPath"`
	Choices     []map[string]interface{} `json:"Choices"`
}

// requiredPaths returns every reference path the state dereferences
// unconditionally. A missing path raises States.Runtime at runtime.
func requiredPaths(st aslState) []string {
	out := collectParameterPaths(st.Parameters)
	if strings.HasPrefix(st.SecondsPath, "$") {
		out = append(out, st.SecondsPath)
	}
	for _, rule := range st.Choices {
		out = append(out, requiredChoicePaths(rule)...)
	}
	return out
}

// collectParameterPaths walks a Parameters object and returns every reference
// used by a "<key>.$" entry, including paths inside intrinsic calls.
func collectParameterPaths(params map[string]interface{}) []string {
	var out []string
	for k, v := range params {
		if !strings.HasSuffix(k, ".$") {
			if nested, ok := v.(map[string]interface{}); ok {
				out = append(out, collectParameterPaths(nested)...)
			}
			continue
		}
		s, ok := v.(string)
		if !ok {
			continue
		}
		if strings.HasPrefix(s, "States.") {
			out = append(out, pathRefRE.FindAllString(s, -1)...)
			continue
		}
		if strings.HasPrefix(s, "$") {
			out = append(out, s)
		}
	}
	return out
}

// requiredChoicePaths returns the reference paths a Choice rule dereferences
// unconditionally. A Variable in a rule that also carries IsPresent is
// explicitly allowed to be absent — that is what IsPresent is for.
func requiredChoicePaths(rule map[string]interface{}) []string {
	if _, guarded := rule["IsPresent"]; guarded {
		return nil
	}
	var out []string
	for k, v := range rule {
		switch k {
		case "Variable":
			if s, ok := v.(string); ok && strings.HasPrefix(s, "$") {
				out = append(out, s)
			}
		case "Not":
			if m, ok := v.(map[string]interface{}); ok {
				out = append(out, requiredChoicePaths(m)...)
			}
		case "And", "Or":
			if arr, ok := v.([]interface{}); ok {
				for _, e := range arr {
					if m, ok := e.(map[string]interface{}); ok {
						out = append(out, requiredChoicePaths(m)...)
					}
				}
			}
		default:
			// Comparator paths such as NumericGreaterThanEqualsPath.
			if strings.HasSuffix(k, "Path") {
				if s, ok := v.(string); ok && strings.HasPrefix(s, "$") {
					out = append(out, s)
				}
			}
		}
	}
	return out
}

// --- real handler outputs ---------------------------------------------------

// contractConfigItem builds the control-table row that store.GetConfig
// expects. Every test in this file uses pipeline "gold-orders".
func contractConfigItem(t *testing.T, cfg types.PipelineConfig) map[string]ddbtypes.AttributeValue {
	t.Helper()
	data, err := json.Marshal(cfg)
	require.NoError(t, err)
	return map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("gold-orders")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.ConfigSK},
		"config": &ddbtypes.AttributeValueMemberS{Value: string(data)},
	}
}

type contractExecutor struct{ meta map[string]interface{} }

func (c *contractExecutor) Execute(context.Context, *types.TriggerConfig) (map[string]interface{}, error) {
	return c.meta, nil
}

// evaluateErrorResult returns the output the live evaluate handler produces
// when the control table is unavailable — the worst case for IsReady.
func evaluateErrorResult(t *testing.T) lambda.OrchestratorOutput {
	t.Helper()
	fake := &storetest.FakeDynamo{
		GetItemFn: func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
			return nil, errors.New("dynamodb: internal error")
		},
	}
	d := &lambda.Deps{Store: storetest.NewStore(fake), Logger: slog.Default()}
	out, err := orchestrator.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode: "evaluate", PipelineID: "gold-orders", ScheduleID: "daily", Date: "2026-03-01",
	})
	require.NoError(t, err, "evaluate must not return a Lambda error for a storage failure")
	return out
}

// triggerResult returns the output the live trigger handler produces for an
// http trigger, the state CheckJob reads. It is the worst case for CheckJob:
// the executor returns no metadata, so $.triggerResult.metadata exists only
// because the handler always emits an object.
func triggerResult(t *testing.T) lambda.OrchestratorOutput {
	t.Helper()
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "gold-orders"},
		Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists, Cutoff: "23:59", CutoffDayOffset: 7}},
		Job:      types.JobConfig{Type: types.TriggerHTTP, Config: map[string]interface{}{"url": "https://example.com/run"}},
	}
	fake := &storetest.FakeDynamo{
		GetItemFn: func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: contractConfigItem(t, cfg)}, nil
		},
	}
	d := &lambda.Deps{Store: storetest.NewStore(fake), Logger: slog.Default()}
	d.TriggerRunner = &contractExecutor{}
	out, err := orchestrator.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode: "trigger", PipelineID: "gold-orders", ScheduleID: "daily", Date: "2026-03-01",
		ExecutionName: "gold-orders-daily-2026-03-01-1709280000",
	})
	require.NoError(t, err)
	return out
}

// checkJobTerminalResult returns the terminal check-job output. CompleteTrigger
// is only reachable once IsJobDone has seen a terminal event, so this is the
// document CompleteTrigger's Parameters are resolved against.
func checkJobTerminalResult(t *testing.T) lambda.OrchestratorOutput {
	t.Helper()
	fake := &storetest.FakeDynamo{
		QueryFn: func(context.Context, *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{Items: []map[string]ddbtypes.AttributeValue{{
				"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("gold-orders")},
				"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("daily", "2026-03-01", "1709280000000")},
				"event": &ddbtypes.AttributeValueMemberS{Value: types.JobEventSuccess},
				"runId": &ddbtypes.AttributeValueMemberS{Value: "jr_1"},
			}}}, nil
		},
	}
	d := &lambda.Deps{Store: storetest.NewStore(fake), Logger: slog.Default()}
	out, err := orchestrator.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode: "check-job", PipelineID: "gold-orders", ScheduleID: "daily", Date: "2026-03-01", RunID: "jr_1",
	})
	require.NoError(t, err)
	require.Equal(t, types.JobEventSuccess, out.Event, "the fixture must be a terminal check-job result")
	return out
}

// completeTriggerResult returns the live complete-trigger output for a
// success, the document IsRetrying reads. Every outcome sets status.
func completeTriggerResult(t *testing.T) lambda.OrchestratorOutput {
	t.Helper()
	d := &lambda.Deps{Store: storetest.NewStore(&storetest.FakeDynamo{}), Logger: slog.Default()}
	out, err := orchestrator.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode: "complete-trigger", PipelineID: "gold-orders", ScheduleID: "daily", Date: "2026-03-01",
		Event: types.JobEventSuccess, RunID: "jr_1", ExecutionName: "gold-orders-daily-2026-03-01-1709280000",
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.Status, "IsRetrying dereferences $.completeTriggerResult.status")
	return out
}

// stateDocument builds the JSON document Step Functions holds when each state
// runs: the execution input plus every ResultPath written earlier on the path
// that reaches the state. evalLoop/jobPollLoop mirror the Result blocks of the
// InitEvalLoop and InitJobPollLoop Pass states; errorInfo mirrors the object
// Step Functions writes for a Catch.
func stateDocument(t *testing.T, input lambda.SFNInput) map[string]interface{} {
	t.Helper()
	data, err := json.Marshal(input)
	require.NoError(t, err)

	var doc map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &doc))

	doc["evalLoop"] = map[string]interface{}{"elapsedSeconds": float64(0)}
	doc["jobPollLoop"] = map[string]interface{}{"elapsedSeconds": float64(0)}
	doc["evaluateResult"] = toDoc(t, evaluateErrorResult(t))
	doc["triggerResult"] = toDoc(t, triggerResult(t))
	doc["checkJobResult"] = toDoc(t, checkJobTerminalResult(t))
	doc["completeTriggerResult"] = toDoc(t, completeTriggerResult(t))
	doc["errorInfo"] = map[string]interface{}{
		"Error": "States.TaskFailed",
		"Cause": "trigger execute: glue trigger: StartJobRun failed",
	}
	return doc
}

func toDoc(t *testing.T, v interface{}) map[string]interface{} {
	t.Helper()
	data, err := json.Marshal(v)
	require.NoError(t, err)
	var m map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &m))
	return m
}

// --- the contract -----------------------------------------------------------

// slaOnlyStates are reachable only when the CheckCancelSLA /
// CheckSLAForCompleteTriggerFailure IsPresent guard on $.config.sla passes.
var slaOnlyStates = map[string]bool{
	"CancelSLASchedules":                true,
	"CancelSLAOnCompleteTriggerFailure": true,
}

func contractPipelineConfig(sla *types.SLAConfig) *types.PipelineConfig {
	return &types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "gold-orders"},
		Schedule: types.ScheduleConfig{Evaluation: types.EvaluationWindow{Window: "1h", Interval: "5m"}},
		SLA:      sla,
		Job:      types.JobConfig{Type: types.TriggerGlue, Config: map[string]interface{}{"jobName": "etl"}},
	}
}

// TestASL_EveryDereferencedPathResolves is the Go<->ASL contract guard. Every
// Parameters reference, Wait SecondsPath, Choice comparator path and
// unguarded Choice Variable in the rendered state machine must resolve against
// the real marshaled Go payloads. A path that does not resolve raises
// States.Runtime at runtime, which Retry cannot retry and Catch: States.ALL
// cannot intercept.
func TestASL_EveryDereferencedPathResolves(t *testing.T) {
	asl := loadASL(t)

	scenarios := []struct {
		name            string
		sla             *types.SLAConfig
		sensorArrivalAt string
	}{
		{
			name: "absolute SLA only",
			sla:  &types.SLAConfig{Deadline: "08:00", ExpectedDuration: "30m"},
		},
		{
			name:            "relative SLA with sensor arrival",
			sla:             &types.SLAConfig{MaxDuration: "2h"},
			sensorArrivalAt: "2026-03-01T06:00:00Z",
		},
		{
			name: "relative SLA without sensor arrival",
			sla:  &types.SLAConfig{MaxDuration: "2h"},
		},
		{
			name: "no SLA",
			sla:  nil,
		},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			input := lambda.BuildSFNInput(
				contractPipelineConfig(sc.sla), "gold-orders", "daily", "2026-03-01", sc.sensorArrivalAt)
			doc := stateDocument(t, input)

			// The SLA branch is selected by IsPresent on $.config.sla.
			slaPresent := resolvePath(doc, "$.config.sla")
			require.Equal(t, sc.sla != nil, slaPresent,
				"$.config.sla presence must match whether the pipeline has an SLA")

			for name, raw := range asl.States {
				if sc.sla == nil && slaOnlyStates[name] {
					continue // unreachable without an SLA
				}
				var st aslState
				require.NoError(t, json.Unmarshal(raw, &st), "parsing state %q", name)

				for _, path := range requiredPaths(st) {
					ok := resolvePath(doc, path)
					require.Truef(t, ok,
						"state %q dereferences %q, which is absent from the Step Functions state document; "+
							"this raises States.Runtime and cannot be caught", name, path)
				}
			}
		})
	}
}

func TestResolvePath_ContextPaths(t *testing.T) {
	for path, want := range map[string]bool{
		"$$.Execution.Name":      true,
		"$$.Execution.StartTime": true,
		"$$.Execution.Bogus":     false,
		"$$.State.EnteredTime":   false,
		"$$.Execution.Name.Sub":  false,
	} {
		require.Equalf(t, want, resolvePath(map[string]interface{}{}, path), "resolvePath(%q)", path)
	}
}

// TestASL_TriggerPassesExecutionName guards the run ID of jobs that report
// their own result: the Trigger state must pass the execution name, and the
// orchestrator input must decode it.
func TestASL_TriggerPassesExecutionName(t *testing.T) {
	asl := loadASL(t)
	var st aslState
	require.NoError(t, json.Unmarshal(asl.States["Trigger"], &st))
	require.Equal(t, "$$.Execution.Name", st.Parameters["executionName.$"])

	var in lambda.OrchestratorInput
	require.NoError(t, json.Unmarshal([]byte(`{"mode":"trigger","executionName":"run-1"}`), &in))
	require.Equal(t, "run-1", in.ExecutionName)
}

// TestASL_InfraFailureIsRecorded: an execution that dies in Evaluate,
// ValidationExhausted or CheckJob goes through RecordInfraFailure, which gets
// the caught error and the execution's start time, before it fails. It fails
// the same way when RecordInfraFailure itself fails. CompleteTrigger keeps its
// own route: a retry it started may already hold the trigger row.
func TestASL_InfraFailureIsRecorded(t *testing.T) {
	asl := loadASL(t)
	type caught struct{ Next, ResultPath string }
	type task struct {
		Next       string
		ResultPath string
		Parameters map[string]string
		Retry      []retryEntry
		Catch      []caught
	}
	load := func(name string) task {
		var st task
		require.NoError(t, json.Unmarshal(asl.States[name], &st), "parsing state %q", name)
		return st
	}

	for _, name := range []string{"Evaluate", "ValidationExhausted", "CheckJob"} {
		require.Equalf(t, []caught{{Next: "RecordInfraFailure", ResultPath: "$.errorInfo"}}, load(name).Catch, "state %q", name)
	}
	require.Equal(t, []caught{{Next: "CheckSLAForCompleteTriggerFailure", ResultPath: "$.errorInfo"}}, load("CompleteTrigger").Catch)

	record := load("RecordInfraFailure")
	require.Equal(t, map[string]string{
		"mode":                 "infra-failure",
		"pipelineId.$":         "$.pipelineId",
		"scheduleId.$":         "$.scheduleId",
		"date.$":               "$.date",
		"errorInfo.$":          "$.errorInfo",
		"executionStartedAt.$": "$$.Execution.StartTime",
	}, record.Parameters)
	require.Equal(t, "$.infraFailureResult", record.ResultPath)
	require.Equal(t, load("Evaluate").Retry, record.Retry)
	require.Equal(t, "InfraFailure", record.Next)
	require.Equal(t, []caught{{Next: "InfraFailure", ResultPath: "$.recordErrorInfo"}}, record.Catch)

	var in lambda.OrchestratorInput
	require.NoError(t, json.Unmarshal([]byte(`{"mode":"infra-failure","executionStartedAt":"2026-03-01T11:00:00.000Z","errorInfo":{"Error":"x","Cause":"y"}}`), &in))
	require.Equal(t, map[string]interface{}{"Error": "x", "Cause": "y"}, in.ErrorInfo)
	require.Equal(t, "2026-03-01T11:00:00.000Z", in.ExecutionStartedAt)
}

// TestASL_RetryBacksOffBeforeEvaluating: an execution starts at the Choice that
// sends a retry through the Wait, and any other run straight to the evaluation
// loop. Nothing else leads to these states, so the wait happens once and is not
// part of the evaluation window.
func TestASL_RetryBacksOffBeforeEvaluating(t *testing.T) {
	asl := loadASL(t)
	require.Equal(t, "CheckRetryBackoff", asl.StartAt)

	type state struct {
		Type, Default, SecondsPath, Next string
		Choices                          []map[string]interface{}
	}
	load := func(name string) (st state) {
		require.NoError(t, json.Unmarshal(asl.States[name], &st), "parsing state %q", name)
		return st
	}
	require.Equal(t, state{Type: "Choice", Default: "InitEvalLoop", Choices: []map[string]interface{}{
		{"Variable": "$.retryDelaySeconds", "NumericGreaterThan": float64(0), "Next": "RetryBackoff"},
	}}, load("CheckRetryBackoff"))
	require.Equal(t, state{Type: "Wait", SecondsPath: "$.retryDelaySeconds", Next: "InitEvalLoop"}, load("RetryBackoff"))

	sources := map[string][]string{}
	for name, raw := range asl.States {
		for target := range allTransitionTargets(t, map[string]json.RawMessage{name: raw}) {
			sources[target] = append(sources[target], name)
		}
	}
	require.Empty(t, sources["CheckRetryBackoff"])
	require.Equal(t, []string{"CheckRetryBackoff"}, sources["RetryBackoff"])
	require.ElementsMatch(t, []string{"CheckRetryBackoff", "RetryBackoff"}, sources["InitEvalLoop"])
}

// --- throttling -------------------------------------------------------------

const (
	throttleError = "Lambda.TooManyRequestsException"

	// orchestratorTimeoutSeconds is the orchestrator Lambda's timeout in
	// deploy/terraform/lambda.tf, the longest an invocation holds its instance.
	// The sla-monitor's is shorter.
	orchestratorTimeoutSeconds = 120
)

// retryLadderSeconds is the total a retrier waits across all its attempts. Step
// Functions multiplies the interval by BackoffRate after each attempt and caps
// each wait at MaxDelaySeconds, when set.
func retryLadderSeconds(r retryEntry) float64 {
	total := 0.0
	for i := range r.MaxAttempts {
		delay := float64(r.IntervalSeconds) * math.Pow(r.BackoffRate, float64(i))
		if r.MaxDelaySeconds > 0 {
			delay = min(delay, float64(r.MaxDelaySeconds))
		}
		total += delay
	}
	return total
}

// TestASL_EveryTaskOutlastsThrottling: each Task retries a throttled call for
// longer than any invocation can hold an instance. Step Functions uses the
// first retrier that matches, and States.TaskFailed matches a throttle too, so
// the throttle retrier has to come first.
func TestASL_EveryTaskOutlastsThrottling(t *testing.T) {
	tasks := 0
	for name, raw := range loadASL(t).States {
		var ts taskState
		require.NoError(t, json.Unmarshal(raw, &ts), "parsing state %q", name)
		if ts.Type != "Task" {
			continue
		}
		tasks++
		t.Run(name, func(t *testing.T) {
			require.NotEmpty(t, ts.Retry)
			first := ts.Retry[0]
			require.Equal(t, []string{throttleError}, first.ErrorEquals)
			require.Greater(t, first.BackoffRate, 1.0, "a brief throttle is retried within seconds, a long one less often")
			// Jitter draws each wait from zero up to its delay, so the total
			// could fall below the timeout.
			require.Empty(t, first.JitterStrategy)
			require.Greater(t, retryLadderSeconds(first), float64(orchestratorTimeoutSeconds))
			for _, later := range ts.Retry[1:] {
				require.NotContains(t, later.ErrorEquals, throttleError)
			}
		})
	}
	require.NotZero(t, tasks, "the state machine has no Task state")
}

// TestASL_TriggerThrottleDoesNotSpendLaunchAttempts: the Trigger state's launch
// attempts stay under trigger_max_attempts and are not used up by throttling.
// The variable is rendered to a value no default uses, so a literal fails.
func TestASL_TriggerThrottleDoesNotSpendLaunchAttempts(t *testing.T) {
	data, err := os.ReadFile("statemachine.asl.json")
	require.NoError(t, err)
	const maxAttempts = 17
	s := strings.ReplaceAll(string(data), "${trigger_max_attempts}", strconv.Itoa(maxAttempts))
	var asl aslDefinition
	require.NoError(t, json.Unmarshal(renderASLTemplate([]byte(s)), &asl))

	var trigger taskState
	require.NoError(t, json.Unmarshal(asl.States["Trigger"], &trigger))
	require.Len(t, trigger.Retry, 2)
	throttle, launch := trigger.Retry[0], trigger.Retry[1]
	require.NotEqual(t, maxAttempts, throttle.MaxAttempts, "throttle retries do not follow trigger_max_attempts")
	require.Equal(t, maxAttempts, launch.MaxAttempts)
	require.Equal(t, 30, launch.IntervalSeconds)
	require.InDelta(t, 2, launch.BackoffRate, 0)
	require.NotContains(t, launch.ErrorEquals, throttleError)
}

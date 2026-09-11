package deploy_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
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

// resolvePath walks a reference path against a decoded JSON document and
// reports whether it exists. Supports "$", "$.a.b" and "$.a[0].b".
func resolvePath(doc interface{}, path string) bool {
	if path == "$" {
		return true
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

// triggerResult returns the output the live trigger handler produces for a
// polling (Glue) trigger — the state CheckJob reads.
func triggerResult(t *testing.T) lambda.OrchestratorOutput {
	t.Helper()
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "gold-orders"},
		Job:      types.JobConfig{Type: types.TriggerGlue, Config: map[string]interface{}{"jobName": "etl"}},
	}
	fake := &storetest.FakeDynamo{
		GetItemFn: func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: contractConfigItem(t, cfg)}, nil
		},
	}
	d := &lambda.Deps{Store: storetest.NewStore(fake), Logger: slog.Default()}
	d.TriggerRunner = &contractExecutor{meta: map[string]interface{}{
		"glue_job_name": "etl", "glue_job_run_id": "jr_1",
	}}
	out, err := orchestrator.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode: "trigger", PipelineID: "gold-orders", ScheduleID: "daily", Date: "2026-03-01",
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
			}}}, nil
		},
	}
	d := &lambda.Deps{Store: storetest.NewStore(fake), Logger: slog.Default()}
	out, err := orchestrator.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode: "check-job", PipelineID: "gold-orders", ScheduleID: "daily", Date: "2026-03-01",
	})
	require.NoError(t, err)
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

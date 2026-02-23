---
title: Evaluators
weight: 2
description: JSON protocol, subprocess runner, HTTP runner, composite runner, and builtin handlers.
---

Evaluators are the executables (or HTTP endpoints) that perform individual trait checks. Interlock supports three evaluation modes: subprocess, HTTP, and builtin.

## JSON Protocol

All evaluators — regardless of transport — use the same JSON input/output contract.

### EvaluatorInput

Sent to the evaluator as JSON:

```json
{
  "pipelineID": "my-pipeline",
  "traitType": "check-freshness",
  "config": {
    "thresholdMinutes": 30,
    "sourceTable": "events"
  }
}
```

| Field | Type | Description |
|---|---|---|
| `pipelineID` | string | Pipeline being evaluated |
| `traitType` | string | Trait type name |
| `config` | object | Merged configuration (archetype defaults + pipeline overrides) |

### EvaluatorOutput

Returned by the evaluator as JSON:

```json
{
  "status": "PASS",
  "value": {
    "minutesSinceUpdate": 12,
    "recordCount": 5432
  },
  "reason": "data is fresh (12 min < 30 min threshold)"
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `status` | string | yes | `PASS`, `FAIL`, or `STALE` |
| `value` | object | no | Arbitrary metrics/data from the evaluation |
| `reason` | string | no | Human-readable explanation |
| `failureCategory` | string | no | `TRANSIENT`, `PERMANENT`, `TIMEOUT`, or `EVALUATOR_CRASH` |

### Status Values

| Status | Meaning | Effect |
|---|---|---|
| `PASS` | Trait check succeeded | Contributes toward readiness |
| `FAIL` | Trait check failed | Blocks readiness (if required trait) |
| `STALE` | Previous result expired | Treated as not-ready, triggers re-evaluation |

### Failure Categories

When `status` is `FAIL`, the optional `failureCategory` field drives retry behavior:

| Category | Description | Retryable |
|---|---|---|
| `TRANSIENT` | Temporary infrastructure issue | Yes (default) |
| `TIMEOUT` | Evaluation timed out | Yes (default) |
| `PERMANENT` | Non-recoverable failure | No |
| `EVALUATOR_CRASH` | Evaluator process crashed | No |

## Subprocess Runner

The default runner for local mode. Executes an evaluator as a child process.

```go
type Runner struct{}

func NewRunner() *Runner
func (r *Runner) Run(ctx context.Context, evaluatorPath string, input types.EvaluatorInput, timeout time.Duration) (*types.EvaluatorOutput, error)
```

### How It Works

1. Marshals `EvaluatorInput` to JSON
2. Executes the evaluator binary/script at `evaluatorPath`
3. Writes JSON to the process's stdin
4. Reads JSON from stdout
5. Unmarshals `EvaluatorOutput`
6. Enforces `timeout` via context cancellation

### Writing a Subprocess Evaluator

Any executable that reads JSON from stdin and writes JSON to stdout works. Examples:

**Bash:**
```bash
#!/bin/bash
INPUT=$(cat)
PIPELINE=$(echo "$INPUT" | jq -r '.pipelineID')
THRESHOLD=$(echo "$INPUT" | jq -r '.config.thresholdMinutes')
# ... perform check ...
echo '{"status":"PASS","reason":"check passed"}'
```

**Python:**
```python
#!/usr/bin/env python3
import json, sys

data = json.load(sys.stdin)
threshold = data["config"]["thresholdMinutes"]
# ... perform check ...
json.dump({"status": "PASS", "reason": "check passed"}, sys.stdout)
```

**Go:**
```go
func main() {
    var input types.EvaluatorInput
    json.NewDecoder(os.Stdin).Decode(&input)
    // ... perform check ...
    json.NewEncoder(os.Stdout).Encode(types.EvaluatorOutput{
        Status: types.TraitPass,
        Reason: "check passed",
    })
}
```

## HTTP Runner

Used in Lambda mode where subprocess evaluators aren't available.

```go
type HTTPRunner struct {
    client  *http.Client
    baseURL string
}

func NewHTTPRunner(baseURL string) *HTTPRunner
func (r *HTTPRunner) Run(ctx context.Context, evaluatorPath string, input types.EvaluatorInput, timeout time.Duration) (*types.EvaluatorOutput, error)
```

### How It Works

1. Constructs URL: `{baseURL}/{evaluatorPath}`
2. Marshals `EvaluatorInput` to JSON
3. Sends HTTP POST with `Content-Type: application/json`
4. Reads response body
5. Unmarshals `EvaluatorOutput`

### Configuration

Set the base URL in `interlock.yaml` or via `EVALUATOR_BASE_URL` environment variable:

```yaml
# Not typically in local config — used by Lambda init
```

```bash
export EVALUATOR_BASE_URL=https://api.example.com/evaluate
```

The evaluator endpoint receives `POST /{traitType}` with the `EvaluatorInput` JSON body.

## Composite Runner

Routes evaluator calls to either builtin handlers or the HTTP runner.

```go
type CompositeRunner struct {
    http     *HTTPRunner
    builtins map[string]BuiltinHandler
}

type BuiltinHandler func(ctx context.Context, input types.EvaluatorInput) (*types.EvaluatorOutput, error)

func NewCompositeRunner(http *HTTPRunner) *CompositeRunner
func (c *CompositeRunner) Register(name string, handler BuiltinHandler)
func (c *CompositeRunner) Run(ctx context.Context, evaluatorPath string, input types.EvaluatorInput, timeout time.Duration) (*types.EvaluatorOutput, error)
```

### Routing Logic

| Evaluator Path | Route |
|---|---|
| `builtin:upstream-check` | Calls registered `BuiltinHandler` for `upstream-check` |
| `check-freshness` | Delegates to `HTTPRunner` at `{baseURL}/check-freshness` |

### Registering Builtins

```go
composite := evaluator.NewCompositeRunner(httpRunner)
composite.Register("upstream-check", func(ctx context.Context, input types.EvaluatorInput) (*types.EvaluatorOutput, error) {
    // Custom evaluation logic
    return &types.EvaluatorOutput{Status: types.TraitPass}, nil
})
```

## Engine Integration

The `Engine` type (`internal/engine/engine.go`) uses the `TraitRunner` interface to evaluate traits:

```go
type TraitRunner interface {
    Run(ctx context.Context, path string, input types.EvaluatorInput, timeout time.Duration) (*types.EvaluatorOutput, error)
}
```

All three runners (`Runner`, `HTTPRunner`, `CompositeRunner`) implement this interface. The engine calls `Run()` for each trait in the archetype, stores results via `TraitStore.PutTrait()`, and combines them using the readiness rule.

### EvaluateTrait (Exported)

For Lambda use, the engine exposes single-trait evaluation:

```go
func (e *Engine) EvaluateTrait(ctx context.Context, pipelineID string, trait archetype.ResolvedTrait) (*types.TraitEvaluation, error)
```

This is called by the `evaluator` Lambda handler for each trait in the Step Function's parallel Map state.

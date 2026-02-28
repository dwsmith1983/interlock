package evaluator

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// BuiltinHandler is a function that evaluates a trait using built-in logic.
type BuiltinHandler func(ctx context.Context, input types.EvaluatorInput) (*types.EvaluatorOutput, error)

// CompositeRunner routes evaluator paths to either built-in handlers or the
// underlying HTTP runner. Paths prefixed with "builtin:" are dispatched to
// registered Go handlers; everything else falls through to HTTP.
type CompositeRunner struct {
	http           *HTTPRunner
	builtins       map[string]BuiltinHandler
	circuitBreaker *CircuitBreaker
}

// NewCompositeRunner creates a CompositeRunner wrapping the given HTTPRunner.
func NewCompositeRunner(http *HTTPRunner) *CompositeRunner {
	return &CompositeRunner{
		http:     http,
		builtins: make(map[string]BuiltinHandler),
	}
}

// SetCircuitBreaker enables circuit breaker protection for evaluator calls.
func (c *CompositeRunner) SetCircuitBreaker(cb *CircuitBreaker) {
	c.circuitBreaker = cb
}

// Register adds a built-in handler for the given name (e.g. "upstream-job-log").
// The evaluator path "builtin:upstream-job-log" will dispatch to this handler.
func (c *CompositeRunner) Register(name string, handler BuiltinHandler) {
	c.builtins[name] = handler
}

// Run dispatches to a builtin handler or the HTTP runner.
// If a circuit breaker is configured and the circuit is open, it returns a
// fail-fast result without calling the evaluator.
func (c *CompositeRunner) Run(ctx context.Context, evaluatorPath string, input types.EvaluatorInput, timeout time.Duration) (*types.EvaluatorOutput, error) {
	// Circuit breaker check.
	if c.circuitBreaker != nil && !c.circuitBreaker.Allow(evaluatorPath) {
		return &types.EvaluatorOutput{
			Status:          types.TraitFail,
			Reason:          fmt.Sprintf("circuit breaker open for %s", evaluatorPath),
			FailureCategory: types.FailureTransient,
		}, nil
	}

	out, err := c.dispatch(ctx, evaluatorPath, input, timeout)

	// Record result for circuit breaker.
	if c.circuitBreaker != nil {
		switch {
		case err != nil:
			c.circuitBreaker.RecordFailure(evaluatorPath, types.FailureTransient)
		case out != nil && (out.Status == types.TraitError || out.Status == types.TraitFail):
			c.circuitBreaker.RecordFailure(evaluatorPath, out.FailureCategory)
		case out != nil:
			c.circuitBreaker.RecordSuccess(evaluatorPath)
		}
	}

	return out, err
}

func (c *CompositeRunner) dispatch(ctx context.Context, evaluatorPath string, input types.EvaluatorInput, timeout time.Duration) (*types.EvaluatorOutput, error) {
	if strings.HasPrefix(evaluatorPath, "builtin:") {
		name := strings.TrimPrefix(evaluatorPath, "builtin:")
		handler, ok := c.builtins[name]
		if !ok {
			return &types.EvaluatorOutput{
				Status:          types.TraitFail,
				Reason:          fmt.Sprintf("unknown builtin evaluator: %s", name),
				FailureCategory: types.FailurePermanent,
			}, nil
		}

		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		out, err := handler(ctx, input)
		if err != nil {
			if ctx.Err() == context.DeadlineExceeded {
				return &types.EvaluatorOutput{
					Status:          types.TraitFail,
					Reason:          "EVALUATOR_TIMEOUT",
					FailureCategory: types.FailureTimeout,
				}, nil
			}
			return nil, err
		}
		return out, nil
	}
	return c.http.Run(ctx, evaluatorPath, input, timeout)
}

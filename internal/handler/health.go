package handler

import (
	"context"
	"time"
)

// HealthResult reports the outcome of a serverless health check invocation.
type HealthResult struct {
	Status    string            `json:"status"`
	Checks   map[string]string `json:"checks"`
	Timestamp time.Time         `json:"timestamp"`
}

// HealthChecker probes a single dependency (e.g. DynamoDB, SQS, Secrets Manager).
type HealthChecker interface {
	Check(ctx context.Context) (name string, err error)
}

// HandlePing runs all health checkers and returns a composite result.
// Status is "healthy" when every checker passes, "degraded" when any checker
// reports an error. This is designed for EventBridge-triggered invocations,
// not persistent HTTP endpoints.
func HandlePing(ctx context.Context, checkers []HealthChecker) HealthResult {
	result := HealthResult{
		Status:    "healthy",
		Checks:   make(map[string]string, len(checkers)),
		Timestamp: time.Now().UTC(),
	}

	for _, c := range checkers {
		name, err := c.Check(ctx)
		if err != nil {
			result.Checks[name] = "error: " + err.Error()
			result.Status = "degraded"
		} else {
			result.Checks[name] = "ok"
		}
	}

	return result
}

package handler_test

import (
	"context"
	"errors"
	"testing"

	"github.com/dwsmith1983/interlock/internal/handler"
	"github.com/stretchr/testify/assert"
)

// stubChecker implements handler.HealthChecker for tests.
type stubChecker struct {
	name string
	err  error
}

func (s *stubChecker) Check(_ context.Context) (string, error) {
	return s.name, s.err
}

func TestHandlePing_AllCheckersPass(t *testing.T) {
	checkers := []handler.HealthChecker{
		&stubChecker{name: "dynamodb", err: nil},
		&stubChecker{name: "sqs", err: nil},
	}

	result := handler.HandlePing(context.Background(), checkers)

	assert.Equal(t, "healthy", result.Status)
	assert.Equal(t, "ok", result.Checks["dynamodb"])
	assert.Equal(t, "ok", result.Checks["sqs"])
	assert.False(t, result.Timestamp.IsZero())
}

func TestHandlePing_OneCheckerFails(t *testing.T) {
	checkers := []handler.HealthChecker{
		&stubChecker{name: "dynamodb", err: nil},
		&stubChecker{name: "sqs", err: errors.New("connection refused")},
	}

	result := handler.HandlePing(context.Background(), checkers)

	assert.Equal(t, "degraded", result.Status)
	assert.Equal(t, "ok", result.Checks["dynamodb"])
	assert.Equal(t, "error: connection refused", result.Checks["sqs"])
}

func TestHandlePing_EmptyCheckers(t *testing.T) {
	result := handler.HandlePing(context.Background(), nil)

	assert.Equal(t, "healthy", result.Status)
	assert.Empty(t, result.Checks)
	assert.False(t, result.Timestamp.IsZero())
}

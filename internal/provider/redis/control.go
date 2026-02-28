package redis

import (
	"context"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// GetControlStatus is a stub — Redis provider does not track CONTROL# records.
// Returns nil (no record) which callers treat as "no circuit breaker data".
func (p *RedisProvider) GetControlStatus(_ context.Context, _ string) (*types.ControlRecord, error) {
	return nil, nil
}

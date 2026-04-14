package client

import (
	"fmt"
	"net/http"
	"time"

	"github.com/sony/gobreaker"
)

// HTTPDoer abstracts http.Client for testing.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// BreakerConfig controls the circuit breaker behavior.
type BreakerConfig struct {
	Name        string
	MaxRequests uint32        // max requests in half-open state
	Interval    time.Duration // cyclic period for clearing counts in closed state
	Timeout     time.Duration // how long to stay open before half-open
	ReadyToTrip func(counts gobreaker.Counts) bool
}

// DefaultBreakerConfig returns sensible defaults: 5 consecutive failures to trip,
// 60s open timeout, 1 request allowed in half-open.
func DefaultBreakerConfig(name string) BreakerConfig {
	return BreakerConfig{
		Name:        name,
		MaxRequests: 1,
		Interval:    0, // don't clear counts periodically
		Timeout:     60 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= 5
		},
	}
}

// BreakerClient wraps an HTTPDoer with a circuit breaker.
type BreakerClient struct {
	inner   HTTPDoer
	breaker *gobreaker.CircuitBreaker
}

// NewBreakerClient creates a circuit-breaker-wrapped HTTP client.
func NewBreakerClient(inner HTTPDoer, cfg BreakerConfig) *BreakerClient {
	if cfg.ReadyToTrip == nil {
		cfg.ReadyToTrip = DefaultBreakerConfig(cfg.Name).ReadyToTrip
	}
	settings := gobreaker.Settings{
		Name:        cfg.Name,
		MaxRequests: cfg.MaxRequests,
		Interval:    cfg.Interval,
		Timeout:     cfg.Timeout,
		ReadyToTrip: cfg.ReadyToTrip,
	}
	return &BreakerClient{
		inner:   inner,
		breaker: gobreaker.NewCircuitBreaker(settings),
	}
}

// Do executes the request through the circuit breaker.
func (c *BreakerClient) Do(req *http.Request) (*http.Response, error) {
	result, err := c.breaker.Execute(func() (any, error) {
		return c.inner.Do(req)
	})
	if err != nil {
		return nil, err
	}
	resp, ok := result.(*http.Response)
	if !ok {
		return nil, fmt.Errorf("breaker: unexpected result type %T", result)
	}
	return resp, nil
}

// State returns the current circuit breaker state.
func (c *BreakerClient) State() gobreaker.State {
	return c.breaker.State()
}

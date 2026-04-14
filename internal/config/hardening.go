package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// TimeoutsConfig holds timeout-related settings.
type TimeoutsConfig struct {
	StageDefault   time.Duration
	ShutdownBuffer time.Duration
}

// WorkersConfig holds worker pool settings.
type WorkersConfig struct {
	PerStage int
}

// DLQConfig holds dead-letter queue settings.
type DLQConfig struct {
	QueueURL   string
	MaxRetries int
}

// CircuitBreakerConfig holds circuit breaker settings.
type CircuitBreakerConfig struct {
	TripAfter      int
	TimeoutSeconds int
}

// HardeningConfig aggregates all production-hardening knobs.
type HardeningConfig struct {
	Timeouts       TimeoutsConfig
	Workers        WorkersConfig
	DLQ            DLQConfig
	CircuitBreaker CircuitBreakerConfig
}

// LoadHardening reads hardening configuration from environment variables,
// applies sensible defaults, and validates all values.
func LoadHardening() (HardeningConfig, error) {
	cfg := HardeningConfig{
		Timeouts: TimeoutsConfig{
			StageDefault:   30 * time.Second,
			ShutdownBuffer: 500 * time.Millisecond,
		},
		Workers: WorkersConfig{
			PerStage: 3,
		},
		DLQ: DLQConfig{
			MaxRetries: 3,
		},
		CircuitBreaker: CircuitBreakerConfig{
			TripAfter:      5,
			TimeoutSeconds: 60,
		},
	}

	if v := os.Getenv("STAGE_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return HardeningConfig{}, fmt.Errorf("STAGE_TIMEOUT: invalid duration %q: %w", v, err)
		}
		cfg.Timeouts.StageDefault = d
	}

	if v := os.Getenv("SHUTDOWN_BUFFER"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return HardeningConfig{}, fmt.Errorf("SHUTDOWN_BUFFER: invalid duration %q: %w", v, err)
		}
		cfg.Timeouts.ShutdownBuffer = d
	}

	if v := os.Getenv("WORKERS_PER_STAGE"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return HardeningConfig{}, fmt.Errorf("WORKERS_PER_STAGE: invalid integer %q: %w", v, err)
		}
		cfg.Workers.PerStage = n
	}

	if v := os.Getenv("DLQ_QUEUE_URL"); v != "" {
		cfg.DLQ.QueueURL = v
	}

	if v := os.Getenv("DLQ_MAX_RETRIES"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return HardeningConfig{}, fmt.Errorf("DLQ_MAX_RETRIES: invalid integer %q: %w", v, err)
		}
		cfg.DLQ.MaxRetries = n
	}

	if v := os.Getenv("CB_TRIP_AFTER"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return HardeningConfig{}, fmt.Errorf("CB_TRIP_AFTER: invalid integer %q: %w", v, err)
		}
		cfg.CircuitBreaker.TripAfter = n
	}

	if v := os.Getenv("CB_TIMEOUT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return HardeningConfig{}, fmt.Errorf("CB_TIMEOUT: invalid integer %q: %w", v, err)
		}
		cfg.CircuitBreaker.TimeoutSeconds = n
	}

	if err := validateHardening(cfg); err != nil {
		return HardeningConfig{}, err
	}

	return cfg, nil
}

func validateHardening(cfg HardeningConfig) error {
	if cfg.Timeouts.StageDefault < 100*time.Millisecond {
		return fmt.Errorf("STAGE_TIMEOUT must be >= 100ms, got %s", cfg.Timeouts.StageDefault)
	}
	if cfg.Workers.PerStage < 1 {
		return fmt.Errorf("WORKERS_PER_STAGE must be >= 1, got %d", cfg.Workers.PerStage)
	}
	if cfg.Workers.PerStage > 100 {
		return fmt.Errorf("WORKERS_PER_STAGE must be <= 100, got %d", cfg.Workers.PerStage)
	}
	if cfg.DLQ.MaxRetries < 0 {
		return fmt.Errorf("DLQ_MAX_RETRIES must be >= 0, got %d", cfg.DLQ.MaxRetries)
	}
	if cfg.CircuitBreaker.TripAfter < 1 {
		return fmt.Errorf("CB_TRIP_AFTER must be >= 1, got %d", cfg.CircuitBreaker.TripAfter)
	}
	if cfg.CircuitBreaker.TimeoutSeconds < 1 {
		return fmt.Errorf("CB_TIMEOUT must be >= 1, got %d", cfg.CircuitBreaker.TimeoutSeconds)
	}
	return nil
}

package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"interlock/internal/pipeline"
)

func TestWithTimeout(t *testing.T) {
	t.Error("RED: implementation missing")
	stage := func(ctx context.Context, input []byte) ([]byte, error) {
		select {
		case <-time.After(1 * time.Second):
			return []byte("done"), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	decorated := pipeline.WithTimeout(50 * time.Millisecond)(stage)

	_, err := decorated(context.Background(), []byte("input"))
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestCompose(t *testing.T) {
	t.Error("RED: implementation missing")
	var executionOrder []string

	dec1 := func(next pipeline.StageFunc) pipeline.StageFunc {
		return func(ctx context.Context, input []byte) ([]byte, error) {
			executionOrder = append(executionOrder, "dec1")
			return next(ctx, input)
		}
	}
	dec2 := func(next pipeline.StageFunc) pipeline.StageFunc {
		return func(ctx context.Context, input []byte) ([]byte, error) {
			executionOrder = append(executionOrder, "dec2")
			return next(ctx, input)
		}
	}

	baseStage := func(ctx context.Context, input []byte) ([]byte, error) {
		executionOrder = append(executionOrder, "base")
		return nil, nil
	}

	composed := pipeline.Compose(dec1, dec2)(baseStage)
	_, err := composed(context.Background(), nil)

	require.NoError(t, err)
	assert.Equal(t, []string{"dec1", "dec2", "base"}, executionOrder, "Decorators should execute in correct order")
}

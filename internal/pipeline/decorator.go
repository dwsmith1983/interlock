package pipeline

import (
	"context"
	"time"
)

// StageFunc is a pipeline stage that transforms input bytes into output bytes.
type StageFunc func(ctx context.Context, input []byte) ([]byte, error)

// Decorator wraps a StageFunc with additional behavior.
type Decorator func(next StageFunc) StageFunc

// WithTimeout returns a Decorator that enforces a maximum duration on a stage.
// If the stage does not complete within d, the context is cancelled and
// context.DeadlineExceeded is returned.
//
// Contract: next must honour context cancellation. If next ignores ctx.Done(),
// the goroutine running next will leak until next returns on its own.
func WithTimeout(d time.Duration) Decorator {
	return func(next StageFunc) StageFunc {
		return func(ctx context.Context, input []byte) ([]byte, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}

			ctx, cancel := context.WithTimeout(ctx, d)
			defer cancel()

			type result struct {
				out []byte
				err error
			}

			ch := make(chan result, 1)
			go func() {
				out, err := next(ctx, input)
				ch <- result{out, err}
			}()

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case r := <-ch:
				return r.out, r.err
			}
		}
	}
}

// Compose chains decorators so they execute in order: the first decorator in
// the slice is the outermost wrapper. Compose with zero decorators returns an
// identity decorator that passes through to the wrapped stage unchanged.
func Compose(decorators ...Decorator) Decorator {
	return func(next StageFunc) StageFunc {
		for i := len(decorators) - 1; i >= 0; i-- {
			next = decorators[i](next)
		}
		return next
	}
}

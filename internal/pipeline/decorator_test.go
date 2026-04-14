package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWithTimeout_Exceeded(t *testing.T) {
	slow := func(ctx context.Context, input []byte) ([]byte, error) {
		select {
		case <-time.After(1 * time.Second):
			return input, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	stage := WithTimeout(50 * time.Millisecond)(slow)

	_, err := stage(context.Background(), []byte("hello"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

func TestWithTimeout_Fast(t *testing.T) {
	fast := func(_ context.Context, input []byte) ([]byte, error) {
		return append(input, '!'), nil
	}

	stage := WithTimeout(1 * time.Second)(fast)

	out, err := stage(context.Background(), []byte("hi"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(out) != "hi!" {
		t.Fatalf("expected %q, got %q", "hi!", string(out))
	}
}

func TestCompose_Order(t *testing.T) {
	var order []string

	decoratorA := func(next StageFunc) StageFunc {
		return func(ctx context.Context, input []byte) ([]byte, error) {
			order = append(order, "A-before")
			out, err := next(ctx, input)
			order = append(order, "A-after")
			return out, err
		}
	}

	decoratorB := func(next StageFunc) StageFunc {
		return func(ctx context.Context, input []byte) ([]byte, error) {
			order = append(order, "B-before")
			out, err := next(ctx, input)
			order = append(order, "B-after")
			return out, err
		}
	}

	inner := func(_ context.Context, input []byte) ([]byte, error) {
		order = append(order, "inner")
		return input, nil
	}

	composed := Compose(decoratorA, decoratorB)
	stage := composed(inner)

	_, err := stage(context.Background(), []byte("x"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := []string{"A-before", "B-before", "inner", "B-after", "A-after"}
	if len(order) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, order)
	}
	for i := range expected {
		if order[i] != expected[i] {
			t.Fatalf("at index %d: expected %q, got %q\nfull: %v", i, expected[i], order[i], order)
		}
	}
}

func TestCompose_Empty(t *testing.T) {
	inner := func(_ context.Context, input []byte) ([]byte, error) {
		return append(input, '!'), nil
	}

	stage := Compose()(inner)

	out, err := stage(context.Background(), []byte("pass"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(out) != "pass!" {
		t.Fatalf("expected %q, got %q", "pass!", string(out))
	}
}

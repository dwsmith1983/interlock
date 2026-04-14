package telemetry

import (
	"context"
	"io"
	"log/slog"
)

type correlationKeyType struct{}

var correlationKey correlationKeyType

// WithCorrelationID stores a correlation ID in the context.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationKey, id)
}

// CorrelationIDFromContext retrieves the correlation ID from the context.
// Returns an empty string if none is set.
func CorrelationIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(correlationKey).(string); ok {
		return v
	}
	return ""
}

// NewLogger creates a structured JSON logger writing to w.
func NewLogger(w io.Writer) *slog.Logger {
	return slog.New(NewCorrelationHandler(slog.NewJSONHandler(w, &slog.HandlerOptions{
		AddSource: true,
	})))
}

// CorrelationHandler wraps an slog.Handler and injects the correlation_id
// from the context into every log record.
type CorrelationHandler struct {
	inner slog.Handler
}

// NewCorrelationHandler creates a handler that injects correlation IDs.
func NewCorrelationHandler(inner slog.Handler) *CorrelationHandler {
	return &CorrelationHandler{inner: inner}
}

func (h *CorrelationHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *CorrelationHandler) Handle(ctx context.Context, r slog.Record) error {
	if id := CorrelationIDFromContext(ctx); id != "" {
		r.AddAttrs(slog.String("correlation_id", id))
	}
	return h.inner.Handle(ctx, r)
}

func (h *CorrelationHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &CorrelationHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h *CorrelationHandler) WithGroup(name string) slog.Handler {
	return &CorrelationHandler{inner: h.inner.WithGroup(name)}
}

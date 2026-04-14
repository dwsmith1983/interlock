package handler

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"runtime/pprof"
	"time"
)

// S3Writer abstracts the PutObject operation for uploading profiles to S3.
type S3Writer interface {
	PutObject(ctx context.Context, bucket, key string, body io.Reader) error
}

// HandleProfile captures a CPU profile for the given duration and writes it to
// S3. It returns the S3 object key on success. This is designed for
// EventBridge-triggered invocations, not persistent /debug/pprof endpoints.
func HandleProfile(ctx context.Context, w S3Writer, bucket string, duration time.Duration) (string, error) {
	var buf bytes.Buffer

	if err := pprof.StartCPUProfile(&buf); err != nil {
		return "", fmt.Errorf("start cpu profile: %w", err)
	}

	select {
	case <-time.After(duration):
	case <-ctx.Done():
		pprof.StopCPUProfile()
		return "", fmt.Errorf("profiling interrupted: %w", ctx.Err())
	}

	pprof.StopCPUProfile()

	key := fmt.Sprintf("profiles/cpu-%s-%d.pb.gz", time.Now().UTC().Format("2006-01-02T15-04-05"), time.Now().UnixNano()%1e6)

	if err := w.PutObject(ctx, bucket, key, &buf); err != nil {
		return "", fmt.Errorf("upload profile to s3://%s/%s: %w", bucket, key, err)
	}

	return key, nil
}

package handler_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/dwsmith1983/interlock/internal/handler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockS3Writer records PutObject calls and optionally returns an error.
type mockS3Writer struct {
	bucket string
	key    string
	body   []byte
	err    error
}

func (m *mockS3Writer) PutObject(_ context.Context, bucket, key string, body io.Reader) error {
	m.bucket = bucket
	m.key = key
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	m.body = data
	return m.err
}

func TestHandleProfile_CapturesAndUploads(t *testing.T) {
	w := &mockS3Writer{}

	key, err := handler.HandleProfile(context.Background(), w, "my-bucket", 10*time.Millisecond)

	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(key, "profiles/cpu-"), "key should start with profiles/cpu-, got: %s", key)
	assert.True(t, strings.HasSuffix(key, ".pb.gz"), "key should end with .pb.gz, got: %s", key)
	assert.Equal(t, "my-bucket", w.bucket)
	assert.Equal(t, key, w.key)
	assert.NotEmpty(t, w.body, "profile data should not be empty")
}

func TestHandleProfile_S3WriteFailure(t *testing.T) {
	w := &mockS3Writer{err: errors.New("access denied")}

	key, err := handler.HandleProfile(context.Background(), w, "my-bucket", 10*time.Millisecond)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "access denied")
	assert.Contains(t, err.Error(), "upload profile to s3://")
	assert.Empty(t, key)
}

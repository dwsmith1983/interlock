package alert

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// S3API is the subset of the S3 client used by S3Sink.
type S3API interface {
	PutObject(ctx context.Context, input *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// S3Sink archives alerts to S3.
type S3Sink struct {
	client     S3API
	bucketName string
	prefix     string
}

// S3SinkOption configures an S3Sink.
type S3SinkOption func(*S3Sink)

// WithS3Client sets a custom S3 client (useful for testing).
func WithS3Client(c S3API) S3SinkOption {
	return func(s *S3Sink) { s.client = c }
}

// NewS3Sink creates a new S3 alert sink.
func NewS3Sink(bucketName, prefix string, opts ...S3SinkOption) (*S3Sink, error) {
	if bucketName == "" {
		return nil, fmt.Errorf("S3 bucket name required")
	}
	s := &S3Sink{
		bucketName: bucketName,
		prefix:     strings.TrimRight(prefix, "/"),
	}
	for _, o := range opts {
		o(s)
	}
	if s.client == nil {
		cfg, err := awsconfig.LoadDefaultConfig(context.Background())
		if err != nil {
			return nil, fmt.Errorf("loading AWS config: %w", err)
		}
		s.client = s3.NewFromConfig(cfg)
	}
	return s, nil
}

// Name returns the sink identifier.
func (s *S3Sink) Name() string { return "s3" }

// Send archives the alert as JSON to S3.
// Key format: {prefix}/{date}/{pipelineID}/{scheduleID}/{unix_millis}-{level}.json
func (s *S3Sink) Send(alert types.Alert) error {
	data, err := json.Marshal(alert)
	if err != nil {
		return fmt.Errorf("marshaling alert: %w", err)
	}

	now := alert.Timestamp
	if now.IsZero() {
		now = time.Now()
	}
	date := now.UTC().Format("2006-01-02")
	pipelineID := alert.PipelineID
	if pipelineID == "" {
		pipelineID = "system"
	}

	key := fmt.Sprintf("%s/%s/%s/%d-%s.json",
		s.prefix, date, pipelineID,
		now.UnixMilli(), alert.Level)
	key = strings.TrimLeft(key, "/")

	_, err = s.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket:      aws.String(s.bucketName),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		return fmt.Errorf("putting alert to S3: %w", err)
	}
	return nil
}

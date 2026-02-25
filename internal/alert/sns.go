package alert

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// SNSAPI is the subset of the SNS client used by SNSSink.
type SNSAPI interface {
	Publish(ctx context.Context, input *sns.PublishInput, opts ...func(*sns.Options)) (*sns.PublishOutput, error)
}

// SNSSink publishes alerts to an SNS topic.
type SNSSink struct {
	client   SNSAPI
	topicARN string
}

// SNSSinkOption configures an SNSSink.
type SNSSinkOption func(*SNSSink)

// WithSNSClient sets a custom SNS client (useful for testing).
func WithSNSClient(c SNSAPI) SNSSinkOption {
	return func(s *SNSSink) { s.client = c }
}

// NewSNSSink creates a new SNS alert sink.
func NewSNSSink(topicARN string, opts ...SNSSinkOption) (*SNSSink, error) {
	if topicARN == "" {
		return nil, fmt.Errorf("SNS topic ARN required")
	}
	s := &SNSSink{topicARN: topicARN}
	for _, o := range opts {
		o(s)
	}
	if s.client == nil {
		cfg, err := awsconfig.LoadDefaultConfig(context.Background())
		if err != nil {
			return nil, fmt.Errorf("loading AWS config: %w", err)
		}
		s.client = sns.NewFromConfig(cfg)
	}
	return s, nil
}

// Name returns the sink identifier.
func (s *SNSSink) Name() string { return "sns" }

// Send publishes the alert as JSON to the configured SNS topic.
func (s *SNSSink) Send(alert types.Alert) error {
	data, err := json.Marshal(alert)
	if err != nil {
		return fmt.Errorf("marshaling alert: %w", err)
	}

	subject := fmt.Sprintf("[%s] %s", alert.Level, alert.PipelineID)
	if len(subject) > 100 {
		subject = subject[:100]
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = s.client.Publish(ctx, &sns.PublishInput{
		TopicArn: aws.String(s.topicARN),
		Subject:  aws.String(subject),
		Message:  aws.String(string(data)),
	})
	if err != nil {
		return fmt.Errorf("publishing to SNS: %w", err)
	}

	return nil
}

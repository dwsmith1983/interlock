// Package dynamodb implements the Provider interface using AWS DynamoDB.
package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/internal/provider"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// Compile-time interface satisfaction check.
var _ provider.Provider = (*DynamoDBProvider)(nil)

// Storage defaults.
const (
	defaultReadinessTTL = 5 * time.Minute
	defaultRetentionTTL = 7 * 24 * time.Hour // 7 days
)

// DynamoDBProvider implements the Provider interface backed by DynamoDB.
type DynamoDBProvider struct {
	client       DDBAPI
	tableName    string
	logger       *slog.Logger
	readinessTTL time.Duration
	retentionTTL time.Duration
	createTable  bool
}

// New creates a new DynamoDBProvider.
func New(cfg *types.DynamoDBConfig) (*DynamoDBProvider, error) {
	var opts []func(*awsconfig.LoadOptions) error
	if cfg.Region != "" {
		opts = append(opts, awsconfig.WithRegion(cfg.Region))
	}

	// For DynamoDB Local: use static credentials and custom endpoint.
	if cfg.Endpoint != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("local", "local", ""),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}

	var clientOpts []func(*dynamodb.Options)
	if cfg.Endpoint != "" {
		clientOpts = append(clientOpts, func(o *dynamodb.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		})
	}

	client := dynamodb.NewFromConfig(awsCfg, clientOpts...)

	readinessTTL := defaultReadinessTTL
	if cfg.ReadinessTTL != "" {
		if d, err := time.ParseDuration(cfg.ReadinessTTL); err == nil && d > 0 {
			readinessTTL = d
		}
	}
	retentionTTL := defaultRetentionTTL
	if cfg.RetentionTTL != "" {
		if d, err := time.ParseDuration(cfg.RetentionTTL); err == nil && d > 0 {
			retentionTTL = d
		}
	}

	return &DynamoDBProvider{
		client:       client,
		tableName:    cfg.TableName,
		logger:       slog.Default(),
		readinessTTL: readinessTTL,
		retentionTTL: retentionTTL,
		createTable:  cfg.CreateTable,
	}, nil
}

// Start initializes the provider: pings DynamoDB and optionally creates the table.
func (p *DynamoDBProvider) Start(ctx context.Context) error {
	if p.createTable {
		if err := p.ensureTable(ctx); err != nil {
			return err
		}
	}
	return p.Ping(ctx)
}

// Stop is a no-op for DynamoDB (no persistent connections to close).
func (p *DynamoDBProvider) Stop(_ context.Context) error {
	return nil
}

// Ping checks connectivity by describing the table.
func (p *DynamoDBProvider) Ping(ctx context.Context) error {
	_, err := p.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: &p.tableName,
	})
	if err != nil {
		return fmt.Errorf("dynamodb ping failed: %w", err)
	}
	return nil
}

func (p *DynamoDBProvider) ensureTable(ctx context.Context) error {
	_, err := p.client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: &p.tableName,
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("PK"), KeyType: ddbtypes.KeyTypeHash},
			{AttributeName: aws.String("SK"), KeyType: ddbtypes.KeyTypeRange},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("PK"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("SK"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("GSI1PK"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("GSI1SK"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexes: []ddbtypes.GlobalSecondaryIndex{
			{
				IndexName: aws.String("GSI1"),
				KeySchema: []ddbtypes.KeySchemaElement{
					{AttributeName: aws.String("GSI1PK"), KeyType: ddbtypes.KeyTypeHash},
					{AttributeName: aws.String("GSI1SK"), KeyType: ddbtypes.KeyTypeRange},
				},
				Projection: &ddbtypes.Projection{ProjectionType: ddbtypes.ProjectionTypeAll},
				ProvisionedThroughput: &ddbtypes.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(5),
					WriteCapacityUnits: aws.Int64(5),
				},
			},
		},
		ProvisionedThroughput: &ddbtypes.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})
	if err != nil {
		var riue *ddbtypes.ResourceInUseException
		if errors.As(err, &riue) {
			return nil // table already exists
		}
		return fmt.Errorf("creating table: %w", err)
	}

	// Enable TTL on the "ttl" attribute.
	_, err = p.client.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
		TableName: &p.tableName,
		TimeToLiveSpecification: &ddbtypes.TimeToLiveSpecification{
			Enabled:       aws.Bool(true),
			AttributeName: aws.String("ttl"),
		},
	})
	if err != nil {
		p.logger.Warn("failed to enable TTL (may already be enabled)", "error", err)
	}

	return nil
}

// isConditionalCheckFailed returns true if the error is a DynamoDB ConditionalCheckFailedException.
func isConditionalCheckFailed(err error) bool {
	var ccfe *ddbtypes.ConditionalCheckFailedException
	return errors.As(err, &ccfe)
}

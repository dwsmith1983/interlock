// Package store implements the DynamoDB storage layer.
package store

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// DynamoAPI is the subset of the DynamoDB client used by Store.
type DynamoAPI interface {
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	Query(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	Scan(ctx context.Context, params *dynamodb.ScanInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
}

// Compile-time check that the real DynamoDB client satisfies DynamoAPI.
var _ DynamoAPI = (*dynamodb.Client)(nil)

// Store provides DynamoDB access to the control, job-log, re-run, and events tables.
type Store struct {
	Client       DynamoAPI
	ControlTable string
	JobLogTable  string
	RerunTable   string
	EventsTable  string
}

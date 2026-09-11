// Package storetest provides test doubles for the store package. It is used
// only from _test.go files (internal/lambda/orchestrator, deploy); no
// production code imports it.
package storetest

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/dwsmith1983/interlock/internal/store"
)

// FakeDynamo is a store.DynamoAPI whose behaviour is supplied by optional
// function hooks. Unset hooks return empty successful responses.
type FakeDynamo struct {
	GetItemFn    func(ctx context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error)
	PutItemFn    func(ctx context.Context, in *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
	UpdateItemFn func(ctx context.Context, in *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error)
	DeleteItemFn func(ctx context.Context, in *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error)
	QueryFn      func(ctx context.Context, in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error)
	ScanFn       func(ctx context.Context, in *dynamodb.ScanInput) (*dynamodb.ScanOutput, error)
}

// Compile-time check.
var _ store.DynamoAPI = (*FakeDynamo)(nil)

// GetItem dispatches to GetItemFn or returns an empty response.
func (f *FakeDynamo) GetItem(ctx context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if f.GetItemFn != nil {
		return f.GetItemFn(ctx, in)
	}
	return &dynamodb.GetItemOutput{}, nil
}

// PutItem dispatches to PutItemFn or returns an empty response.
func (f *FakeDynamo) PutItem(ctx context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if f.PutItemFn != nil {
		return f.PutItemFn(ctx, in)
	}
	return &dynamodb.PutItemOutput{}, nil
}

// UpdateItem dispatches to UpdateItemFn or returns an empty response.
func (f *FakeDynamo) UpdateItem(ctx context.Context, in *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	if f.UpdateItemFn != nil {
		return f.UpdateItemFn(ctx, in)
	}
	return &dynamodb.UpdateItemOutput{}, nil
}

// DeleteItem dispatches to DeleteItemFn or returns an empty response.
func (f *FakeDynamo) DeleteItem(ctx context.Context, in *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	if f.DeleteItemFn != nil {
		return f.DeleteItemFn(ctx, in)
	}
	return &dynamodb.DeleteItemOutput{}, nil
}

// Query dispatches to QueryFn or returns an empty response.
func (f *FakeDynamo) Query(ctx context.Context, in *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	if f.QueryFn != nil {
		return f.QueryFn(ctx, in)
	}
	return &dynamodb.QueryOutput{}, nil
}

// Scan dispatches to ScanFn or returns an empty response.
func (f *FakeDynamo) Scan(ctx context.Context, in *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	if f.ScanFn != nil {
		return f.ScanFn(ctx, in)
	}
	return &dynamodb.ScanOutput{}, nil
}

// NewStore returns a *store.Store backed by the given client with fixed table names.
func NewStore(client store.DynamoAPI) *store.Store {
	return &store.Store{
		Client:       client,
		ControlTable: "control",
		JobLogTable:  "joblog",
		RerunTable:   "rerun",
		EventsTable:  "events",
	}
}

package store

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ScanAll runs a paginated DynamoDB Scan. buildInput is called for each page
// with the ExclusiveStartKey (nil for the first page). processPage is called
// for each page of results.
func ScanAll(ctx context.Context, client DynamoAPI, buildInput func(startKey map[string]ddbtypes.AttributeValue) *dynamodb.ScanInput, processPage func(items []map[string]ddbtypes.AttributeValue) error) error {
	var startKey map[string]ddbtypes.AttributeValue
	for {
		input := buildInput(startKey)
		out, err := client.Scan(ctx, input)
		if err != nil {
			return err
		}
		if err := processPage(out.Items); err != nil {
			return err
		}
		if out.LastEvaluatedKey == nil {
			return nil
		}
		startKey = out.LastEvaluatedKey
	}
}

// QueryAll runs a paginated DynamoDB Query. buildInput is called for each page
// with the ExclusiveStartKey (nil for the first page). processPage is called
// for each page of results.
func QueryAll(ctx context.Context, client DynamoAPI, buildInput func(startKey map[string]ddbtypes.AttributeValue) *dynamodb.QueryInput, processPage func(items []map[string]ddbtypes.AttributeValue) error) error {
	var startKey map[string]ddbtypes.AttributeValue
	for {
		input := buildInput(startKey)
		out, err := client.Query(ctx, input)
		if err != nil {
			return err
		}
		if err := processPage(out.Items); err != nil {
			return err
		}
		if out.LastEvaluatedKey == nil {
			return nil
		}
		startKey = out.LastEvaluatedKey
	}
}

// QueryCount runs a paginated DynamoDB Query with Select=COUNT and returns
// the total count across all pages.
func QueryCount(ctx context.Context, client DynamoAPI, buildInput func(startKey map[string]ddbtypes.AttributeValue) *dynamodb.QueryInput) (int, error) {
	total := 0
	var startKey map[string]ddbtypes.AttributeValue
	for {
		input := buildInput(startKey)
		out, err := client.Query(ctx, input)
		if err != nil {
			return 0, err
		}
		total += int(out.Count)
		if out.LastEvaluatedKey == nil {
			return total, nil
		}
		startKey = out.LastEvaluatedKey
	}
}

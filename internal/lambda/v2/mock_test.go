package v2_test

import (
	"context"
	"sort"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/sfn"

	store "github.com/dwsmith1983/interlock/internal/store/v2"
)

// ---------------------------------------------------------------------------
// Mock SFN client
// ---------------------------------------------------------------------------

type mockSFN struct {
	mu         sync.Mutex
	executions []*sfn.StartExecutionInput
	err        error
}

func (m *mockSFN) StartExecution(_ context.Context, input *sfn.StartExecutionInput, _ ...func(*sfn.Options)) (*sfn.StartExecutionOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	m.executions = append(m.executions, input)
	return &sfn.StartExecutionOutput{}, nil
}

// ---------------------------------------------------------------------------
// Mock EventBridge client
// ---------------------------------------------------------------------------

type mockEventBridge struct {
	mu     sync.Mutex
	events []*eventbridge.PutEventsInput
	err    error
}

func (m *mockEventBridge) PutEvents(_ context.Context, input *eventbridge.PutEventsInput, _ ...func(*eventbridge.Options)) (*eventbridge.PutEventsOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	m.events = append(m.events, input)
	return &eventbridge.PutEventsOutput{}, nil
}

// ---------------------------------------------------------------------------
// Mock DynamoDB client (re-implemented for this package's tests)
// ---------------------------------------------------------------------------

type mockDDB struct {
	mu    sync.Mutex
	items map[string]map[string]ddbtypes.AttributeValue
}

func newMockDDB() *mockDDB {
	return &mockDDB{
		items: make(map[string]map[string]ddbtypes.AttributeValue),
	}
}

// Compile-time check.
var _ store.DynamoAPI = (*mockDDB)(nil)

func ddbItemKey(table, pk, sk string) string {
	return table + "#" + pk + "#" + sk
}

func ddbExtractPKSK(item map[string]ddbtypes.AttributeValue) (string, string) {
	pk := item["PK"].(*ddbtypes.AttributeValueMemberS).Value
	sk := item["SK"].(*ddbtypes.AttributeValueMemberS).Value
	return pk, sk
}

func ddbCopyItem(item map[string]ddbtypes.AttributeValue) map[string]ddbtypes.AttributeValue {
	out := make(map[string]ddbtypes.AttributeValue, len(item))
	for k, v := range item {
		out[k] = v
	}
	return out
}

func (m *mockDDB) GetItem(_ context.Context, input *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	pk := input.Key["PK"].(*ddbtypes.AttributeValueMemberS).Value
	sk := input.Key["SK"].(*ddbtypes.AttributeValueMemberS).Value
	key := ddbItemKey(*input.TableName, pk, sk)

	item, ok := m.items[key]
	if !ok {
		return &dynamodb.GetItemOutput{}, nil
	}
	return &dynamodb.GetItemOutput{Item: ddbCopyItem(item)}, nil
}

func (m *mockDDB) PutItem(_ context.Context, input *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	pk, sk := ddbExtractPKSK(input.Item)
	key := ddbItemKey(*input.TableName, pk, sk)

	// Support attribute_not_exists(PK) condition for lock acquisition.
	if input.ConditionExpression != nil && *input.ConditionExpression == "attribute_not_exists(PK)" {
		if _, exists := m.items[key]; exists {
			return nil, &ddbtypes.ConditionalCheckFailedException{
				Message: strPtr("The conditional request failed"),
			}
		}
	}

	m.items[key] = ddbCopyItem(input.Item)
	return &dynamodb.PutItemOutput{}, nil
}

func (m *mockDDB) UpdateItem(_ context.Context, _ *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	return &dynamodb.UpdateItemOutput{}, nil
}

func (m *mockDDB) DeleteItem(_ context.Context, input *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	pk := input.Key["PK"].(*ddbtypes.AttributeValueMemberS).Value
	sk := input.Key["SK"].(*ddbtypes.AttributeValueMemberS).Value
	delete(m.items, ddbItemKey(*input.TableName, pk, sk))
	return &dynamodb.DeleteItemOutput{}, nil
}

func (m *mockDDB) Query(_ context.Context, input *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	pkVal := input.ExpressionAttributeValues[":pk"].(*ddbtypes.AttributeValueMemberS).Value
	prefixVal := ""
	if p, ok := input.ExpressionAttributeValues[":prefix"]; ok {
		prefixVal = p.(*ddbtypes.AttributeValueMemberS).Value
	}

	table := *input.TableName
	tablePrefix := table + "#" + pkVal + "#"

	var items []map[string]ddbtypes.AttributeValue
	for k, item := range m.items {
		if !strings.HasPrefix(k, tablePrefix) {
			continue
		}
		sk := item["SK"].(*ddbtypes.AttributeValueMemberS).Value
		if prefixVal != "" && !strings.HasPrefix(sk, prefixVal) {
			continue
		}
		items = append(items, ddbCopyItem(item))
	}

	sort.Slice(items, func(i, j int) bool {
		skI := items[i]["SK"].(*ddbtypes.AttributeValueMemberS).Value
		skJ := items[j]["SK"].(*ddbtypes.AttributeValueMemberS).Value
		return skI < skJ
	})

	return &dynamodb.QueryOutput{Items: items, Count: int32(len(items))}, nil
}

func (m *mockDDB) Scan(_ context.Context, input *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	table := *input.TableName
	prefix := table + "#"

	var items []map[string]ddbtypes.AttributeValue
	for k, item := range m.items {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		// Basic filter for "SK = :sk"
		if input.FilterExpression != nil {
			if val, ok := input.ExpressionAttributeValues[":sk"]; ok {
				expected := val.(*ddbtypes.AttributeValueMemberS).Value
				actual := item["SK"].(*ddbtypes.AttributeValueMemberS).Value
				if actual != expected {
					continue
				}
			}
		}
		items = append(items, ddbCopyItem(item))
	}

	return &dynamodb.ScanOutput{Items: items, Count: int32(len(items))}, nil
}

// putRaw inserts an item directly into the mock store.
func (m *mockDDB) putRaw(table string, item map[string]ddbtypes.AttributeValue) {
	m.mu.Lock()
	defer m.mu.Unlock()

	pk, sk := ddbExtractPKSK(item)
	m.items[ddbItemKey(table, pk, sk)] = ddbCopyItem(item)
}

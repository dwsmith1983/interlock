package store

import (
	"context"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// mockDDB is an in-memory DynamoDB mock that supports GetItem, PutItem,
// DeleteItem, Query (begins_with), and Scan. Items are keyed by "table#PK#SK".
type mockDDB struct {
	mu    sync.Mutex
	items map[string]map[string]ddbtypes.AttributeValue

	// errFn, if set, is called before each operation. Return a non-nil error
	// to inject failures.
	errFn func(op string) error
}

func newMockDDB() *mockDDB {
	return &mockDDB{
		items: make(map[string]map[string]ddbtypes.AttributeValue),
	}
}

func itemKey(table, pk, sk string) string {
	return table + "#" + pk + "#" + sk
}

func extractPKSK(item map[string]ddbtypes.AttributeValue) (string, string) {
	pk := item["PK"].(*ddbtypes.AttributeValueMemberS).Value
	sk := item["SK"].(*ddbtypes.AttributeValueMemberS).Value
	return pk, sk
}

func copyItem(item map[string]ddbtypes.AttributeValue) map[string]ddbtypes.AttributeValue {
	out := make(map[string]ddbtypes.AttributeValue, len(item))
	for k, v := range item {
		out[k] = v
	}
	return out
}

func (m *mockDDB) checkErr(op string) error {
	if m.errFn != nil {
		return m.errFn(op)
	}
	return nil
}

func (m *mockDDB) GetItem(_ context.Context, input *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.checkErr("GetItem"); err != nil {
		return nil, err
	}

	pk := input.Key["PK"].(*ddbtypes.AttributeValueMemberS).Value
	sk := input.Key["SK"].(*ddbtypes.AttributeValueMemberS).Value
	key := itemKey(*input.TableName, pk, sk)

	item, ok := m.items[key]
	if !ok {
		return &dynamodb.GetItemOutput{}, nil
	}
	return &dynamodb.GetItemOutput{Item: copyItem(item)}, nil
}

func (m *mockDDB) PutItem(_ context.Context, input *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.checkErr("PutItem"); err != nil {
		return nil, err
	}

	pk, sk := extractPKSK(input.Item)
	key := itemKey(*input.TableName, pk, sk)

	// Support attribute_not_exists(PK) condition for lock acquisition.
	if input.ConditionExpression != nil && *input.ConditionExpression == "attribute_not_exists(PK)" {
		if _, exists := m.items[key]; exists {
			return nil, &ddbtypes.ConditionalCheckFailedException{
				Message: strPtr("The conditional request failed"),
			}
		}
	}

	m.items[key] = copyItem(input.Item)
	return &dynamodb.PutItemOutput{}, nil
}

func (m *mockDDB) UpdateItem(_ context.Context, input *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.checkErr("UpdateItem"); err != nil {
		return nil, err
	}

	pk := input.Key["PK"].(*ddbtypes.AttributeValueMemberS).Value
	sk := input.Key["SK"].(*ddbtypes.AttributeValueMemberS).Value
	key := itemKey(*input.TableName, pk, sk)

	item, ok := m.items[key]
	if !ok {
		item = map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: pk},
			"SK": &ddbtypes.AttributeValueMemberS{Value: sk},
		}
	}

	// Parse "SET #name = :val" assignments from UpdateExpression and apply them.
	if input.UpdateExpression != nil {
		expr := strings.TrimPrefix(*input.UpdateExpression, "SET ")
		for _, assignment := range strings.Split(expr, ",") {
			parts := strings.SplitN(strings.TrimSpace(assignment), "=", 2)
			if len(parts) != 2 {
				continue
			}
			nameRef := strings.TrimSpace(parts[0])
			valRef := strings.TrimSpace(parts[1])

			attrName := nameRef
			if resolved, ok := input.ExpressionAttributeNames[nameRef]; ok {
				attrName = resolved
			}
			if val, ok := input.ExpressionAttributeValues[valRef]; ok {
				item[attrName] = val
			}
		}
	}

	m.items[key] = item
	return &dynamodb.UpdateItemOutput{}, nil
}

func (m *mockDDB) DeleteItem(_ context.Context, input *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.checkErr("DeleteItem"); err != nil {
		return nil, err
	}

	pk := input.Key["PK"].(*ddbtypes.AttributeValueMemberS).Value
	sk := input.Key["SK"].(*ddbtypes.AttributeValueMemberS).Value
	key := itemKey(*input.TableName, pk, sk)

	delete(m.items, key)
	return &dynamodb.DeleteItemOutput{}, nil
}

func (m *mockDDB) Query(_ context.Context, input *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.checkErr("Query"); err != nil {
		return nil, err
	}

	// Extract :pk and :prefix from ExpressionAttributeValues.
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
		items = append(items, copyItem(item))
	}

	return &dynamodb.QueryOutput{Items: items, Count: int32(len(items))}, nil
}

func (m *mockDDB) Scan(_ context.Context, input *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.checkErr("Scan"); err != nil {
		return nil, err
	}

	table := *input.TableName
	prefix := table + "#"

	var items []map[string]ddbtypes.AttributeValue
	for k, item := range m.items {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		items = append(items, copyItem(item))
	}

	return &dynamodb.ScanOutput{Items: items, Count: int32(len(items))}, nil
}

// putRaw inserts an item directly into the mock store (test helper).
func (m *mockDDB) putRaw(table string, item map[string]ddbtypes.AttributeValue) {
	m.mu.Lock()
	defer m.mu.Unlock()

	pk, sk := extractPKSK(item)
	m.items[itemKey(table, pk, sk)] = copyItem(item)
}

func strPtr(s string) *string { return &s }

// Compile-time check that mockDDB satisfies DynamoAPI.
var _ DynamoAPI = (*mockDDB)(nil)

// newTestStore returns a Store backed by the given mock with default table names.
func newTestStore(mock *mockDDB) *Store {
	return &Store{
		Client:       mock,
		ControlTable: "control",
		JobLogTable:  "joblog",
		RerunTable:   "rerun",
	}
}

// errAfterN returns an errFn that succeeds the first n calls, then returns err.
func errAfterN(n int, err error) func(string) error {
	count := 0
	return func(_ string) error {
		count++
		if count > n {
			return err
		}
		return nil
	}
}

// errOnOp returns an errFn that fails on a specific operation name.
func errOnOp(op string, err error) func(string) error {
	return func(actual string) error {
		if actual == op {
			return err
		}
		return nil
	}
}

// Blank assignments keep these helpers available for future test files in this package.
var _ = errAfterN
var _ = errOnOp

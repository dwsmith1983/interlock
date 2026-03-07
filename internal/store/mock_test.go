package store

import (
	"context"
	"sort"
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

func extractPKSK(item map[string]ddbtypes.AttributeValue) (pk, sk string) {
	pk = item["PK"].(*ddbtypes.AttributeValueMemberS).Value
	sk = item["SK"].(*ddbtypes.AttributeValueMemberS).Value
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

	// Parse UpdateExpression: supports SET and REMOVE clauses.
	if input.UpdateExpression != nil {
		expr := *input.UpdateExpression

		// Split into SET and REMOVE sections.
		setExpr, removeExpr := "", ""
		if idx := strings.Index(expr, " REMOVE "); idx >= 0 {
			setExpr = expr[:idx]
			removeExpr = expr[idx+len(" REMOVE "):]
		} else {
			setExpr = expr
		}

		// Apply SET assignments.
		setExpr = strings.TrimPrefix(setExpr, "SET ")
		for _, assignment := range strings.Split(setExpr, ",") {
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

		// Apply REMOVE deletions.
		if removeExpr != "" {
			for _, attr := range strings.Split(removeExpr, ",") {
				attr = strings.TrimSpace(attr)
				if resolved, ok := input.ExpressionAttributeNames[attr]; ok {
					attr = resolved
				}
				delete(item, attr)
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

	// Extract :pk and the SK prefix from ExpressionAttributeValues.
	// The prefix variable name is parsed from the KeyConditionExpression
	// (e.g. "begins_with(SK, :prefix)" or "begins_with(SK, :skPrefix)").
	pkVal := input.ExpressionAttributeValues[":pk"].(*ddbtypes.AttributeValueMemberS).Value
	prefixVal := ""
	if input.KeyConditionExpression != nil {
		if idx := strings.Index(*input.KeyConditionExpression, "begins_with(SK, "); idx >= 0 {
			rest := (*input.KeyConditionExpression)[idx+len("begins_with(SK, "):]
			if end := strings.Index(rest, ")"); end >= 0 {
				varName := rest[:end]
				if p, ok := input.ExpressionAttributeValues[varName]; ok {
					prefixVal = p.(*ddbtypes.AttributeValueMemberS).Value
				}
			}
		}
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

	// Sort by SK ascending (default) or descending when ScanIndexForward is false.
	sort.Slice(items, func(i, j int) bool {
		skI := items[i]["SK"].(*ddbtypes.AttributeValueMemberS).Value
		skJ := items[j]["SK"].(*ddbtypes.AttributeValueMemberS).Value
		if input.ScanIndexForward != nil && !*input.ScanIndexForward {
			return skI > skJ
		}
		return skI < skJ
	})

	// Apply Limit.
	if input.Limit != nil && int(*input.Limit) < len(items) {
		items = items[:*input.Limit]
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
		if !m.matchesFilter(input, item) {
			continue
		}
		items = append(items, copyItem(item))
	}

	return &dynamodb.ScanOutput{Items: items, Count: int32(len(items))}, nil
}

// matchesFilter applies basic FilterExpression support for equality and
// compound "begins_with(attr, :val) AND attr = :val" expressions.
func (m *mockDDB) matchesFilter(input *dynamodb.ScanInput, item map[string]ddbtypes.AttributeValue) bool {
	if input.FilterExpression == nil {
		return true
	}

	expr := *input.FilterExpression

	// Support compound AND expressions by splitting on " AND ".
	clauses := strings.Split(expr, " AND ")
	for _, clause := range clauses {
		clause = strings.TrimSpace(clause)
		if !m.matchesClause(clause, input, item) {
			return false
		}
	}
	return true
}

// matchesClause evaluates a single filter clause against an item.
func (m *mockDDB) matchesClause(clause string, input *dynamodb.ScanInput, item map[string]ddbtypes.AttributeValue) bool {
	// Support begins_with(attr, :val).
	if strings.HasPrefix(clause, "begins_with(") {
		inner := strings.TrimPrefix(clause, "begins_with(")
		inner = strings.TrimSuffix(inner, ")")
		parts := strings.SplitN(inner, ",", 2)
		if len(parts) != 2 {
			return true
		}
		attrName := strings.TrimSpace(parts[0])
		valRef := strings.TrimSpace(parts[1])

		expected, ok := input.ExpressionAttributeValues[valRef]
		if !ok {
			return true
		}
		actual, ok := item[attrName]
		if !ok {
			return false
		}
		expectedStr, ok1 := expected.(*ddbtypes.AttributeValueMemberS)
		actualStr, ok2 := actual.(*ddbtypes.AttributeValueMemberS)
		if ok1 && ok2 {
			return strings.HasPrefix(actualStr.Value, expectedStr.Value)
		}
		return true
	}

	// Support simple "attr = :val" equality.
	parts := strings.SplitN(clause, " = ", 2)
	if len(parts) != 2 {
		return true // unsupported expression, pass through
	}

	attrName := strings.TrimSpace(parts[0])
	valRef := strings.TrimSpace(parts[1])

	// Resolve expression attribute names if present.
	if input.ExpressionAttributeNames != nil {
		if resolved, ok := input.ExpressionAttributeNames[attrName]; ok {
			attrName = resolved
		}
	}

	// Get expected value from expression attribute values.
	expected, ok := input.ExpressionAttributeValues[valRef]
	if !ok {
		return true // can't resolve, pass through
	}

	actual, ok := item[attrName]
	if !ok {
		return false
	}

	// Compare string values.
	expectedStr, ok1 := expected.(*ddbtypes.AttributeValueMemberS)
	actualStr, ok2 := actual.(*ddbtypes.AttributeValueMemberS)
	if ok1 && ok2 {
		return expectedStr.Value == actualStr.Value
	}

	return true // non-string comparison not supported, pass through
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

// errOnOp returns an errFn that fails on a specific operation name.
func errOnOp(op string, err error) func(string) error {
	return func(actual string) error {
		if actual == op {
			return err
		}
		return nil
	}
}

// Blank assignment keeps this helper available for future test files in this package.
var _ = errOnOp

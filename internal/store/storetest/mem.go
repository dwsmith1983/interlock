package storetest

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Mem is an in-memory set of DynamoDB tables keyed by PK and SK. It
// implements the subset of the API the store uses. An expression it does not
// support is an error naming the expression, never a silent match.
type Mem struct {
	mu     sync.Mutex
	tables map[string]map[string]map[string]ddbtypes.AttributeValue // table -> PK+"\x00"+SK -> item
}

// NewMem returns a FakeDynamo whose GetItem, PutItem, UpdateItem, DeleteItem,
// Query and Scan hooks are served by the returned Mem.
func NewMem() (*FakeDynamo, *Mem) {
	m := &Mem{tables: map[string]map[string]map[string]ddbtypes.AttributeValue{}}
	return &FakeDynamo{
		GetItemFn: m.getItem, PutItemFn: m.putItem, UpdateItemFn: m.updateItem,
		DeleteItemFn: m.deleteItem, QueryFn: m.query, ScanFn: m.scan,
	}, m
}

// Put stores item in table unconditionally.
func (m *Mem) Put(table string, item map[string]ddbtypes.AttributeValue) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.table(table)[key(item)] = clone(item)
}

// Get returns a copy of the item, or nil when there is none.
func (m *Mem) Get(table, pk, sk string) map[string]ddbtypes.AttributeValue {
	m.mu.Lock()
	defer m.mu.Unlock()
	return clone(m.table(table)[pk+"\x00"+sk])
}

// Items returns copies of every item in table, sorted by PK then SK.
func (m *Mem) Items(table string) []map[string]ddbtypes.AttributeValue {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sorted(table, func(map[string]ddbtypes.AttributeValue) bool { return true })
}

func (m *Mem) table(name string) map[string]map[string]ddbtypes.AttributeValue {
	t, ok := m.tables[name]
	if !ok {
		t = map[string]map[string]ddbtypes.AttributeValue{}
		m.tables[name] = t
	}
	return t
}

func (m *Mem) sorted(table string, keep func(map[string]ddbtypes.AttributeValue) bool) []map[string]ddbtypes.AttributeValue {
	keys := make([]string, 0, len(m.tables[table]))
	for k, item := range m.tables[table] {
		if keep(item) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	out := make([]map[string]ddbtypes.AttributeValue, len(keys))
	for i, k := range keys {
		out[i] = clone(m.tables[table][k])
	}
	return out
}

func str(v ddbtypes.AttributeValue) string {
	if s, ok := v.(*ddbtypes.AttributeValueMemberS); ok {
		return s.Value
	}
	return ""
}

// num is a number attribute's value, or "" for anything else.
func num(v ddbtypes.AttributeValue) string {
	if n, ok := v.(*ddbtypes.AttributeValueMemberN); ok {
		return n.Value
	}
	return ""
}

func key(item map[string]ddbtypes.AttributeValue) string {
	return str(item["PK"]) + "\x00" + str(item["SK"])
}

func clone(item map[string]ddbtypes.AttributeValue) map[string]ddbtypes.AttributeValue {
	if item == nil {
		return nil
	}
	out := make(map[string]ddbtypes.AttributeValue, len(item))
	for k, v := range item {
		out[k] = v
	}
	return out
}

func unsupported(op, field string, expr *string) error {
	return fmt.Errorf("storetest.Mem %s: unsupported %s %q", op, field, deref(expr))
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

var compareRE = regexp.MustCompile(`^([#\w]+) (=|<>|<) (:\w+)$`)

// condition evaluates attribute_exists(PK), attribute_not_exists(PK),
// "<name> = :value", "<name> <> :value" and, on numbers,
// "<name> < :value" clauses joined by AND, with
// groups of those joined by OR (AND binds tighter), against item (nil when
// absent).
func condition(op string, expr *string, item map[string]ddbtypes.AttributeValue,
	names map[string]string, values map[string]ddbtypes.AttributeValue) error {
	if expr == nil {
		return nil
	}
	holds := false
	for _, group := range strings.Split(*expr, " OR ") {
		all := true
		for _, clause := range strings.Split(group, " AND ") {
			ok, known := clauseHolds(clause, item, names, values)
			if !known {
				return unsupported(op, "ConditionExpression", expr)
			}
			all = all && ok
		}
		holds = holds || all
	}
	if !holds {
		return &ddbtypes.ConditionalCheckFailedException{Message: expr}
	}
	return nil
}

func clauseHolds(clause string, item map[string]ddbtypes.AttributeValue,
	names map[string]string, values map[string]ddbtypes.AttributeValue) (holds, known bool) {
	switch m := compareRE.FindStringSubmatch(clause); {
	case clause == "attribute_exists(PK)":
		return item != nil, true
	case clause == "attribute_not_exists(PK)":
		return item == nil, true
	case len(m) == 4 && values[m[3]] != nil:
		name := m[1]
		if resolved, ok := names[name]; ok {
			name = resolved
		}
		if m[2] == "<" {
			a, errA := strconv.ParseFloat(num(item[name]), 64)
			b, errB := strconv.ParseFloat(num(values[m[3]]), 64)
			return errA == nil && errB == nil && a < b, true
		}
		equal := item != nil && str(item[name]) == str(values[m[3]])
		// An absent item satisfies neither comparison.
		return equal == (m[2] == "=") && item != nil, true
	}
	return false, false
}

func (m *Mem) getItem(_ context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	return &dynamodb.GetItemOutput{Item: m.Get(*in.TableName, str(in.Key["PK"]), str(in.Key["SK"]))}, nil
}

func (m *Mem) putItem(_ context.Context, in *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.table(*in.TableName)
	if err := condition("PutItem", in.ConditionExpression, t[key(in.Item)], in.ExpressionAttributeNames, in.ExpressionAttributeValues); err != nil {
		return nil, err
	}
	t[key(in.Item)] = clone(in.Item)
	return &dynamodb.PutItemOutput{}, nil
}

func (m *Mem) deleteItem(_ context.Context, in *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	if in.ConditionExpression != nil {
		return nil, unsupported("DeleteItem", "ConditionExpression", in.ConditionExpression)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.table(*in.TableName), key(in.Key))
	return &dynamodb.DeleteItemOutput{}, nil
}

var (
	updateRE = regexp.MustCompile(`^SET ([^=]+ = :\w+(?:, [^=]+ = :\w+)*)(?: REMOVE ([#\w]+(?:, [#\w]+)*))?$`)
	queryRE  = regexp.MustCompile(`^PK = (:\w+)(?: AND begins_with\(SK, (:\w+)\)| AND SK BETWEEN (:\w+) AND (:\w+))?$`)
)

// updateItem supports "SET a = :x[, b = :y] [REMOVE c[, d]]", creating the
// item when it is absent, as DynamoDB does.
func (m *Mem) updateItem(_ context.Context, in *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
	match := updateRE.FindStringSubmatch(deref(in.UpdateExpression))
	if match == nil {
		return nil, unsupported("UpdateItem", "UpdateExpression", in.UpdateExpression)
	}
	name := func(n string) string {
		if resolved, ok := in.ExpressionAttributeNames[n]; ok {
			return resolved
		}
		return n
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.table(*in.TableName)
	item := t[key(in.Key)]
	if err := condition("UpdateItem", in.ConditionExpression, item, in.ExpressionAttributeNames, in.ExpressionAttributeValues); err != nil {
		return nil, err
	}
	next := clone(in.Key)
	for k, v := range item {
		next[k] = v
	}
	for _, assign := range strings.Split(match[1], ", ") {
		attr, val, _ := strings.Cut(assign, " = ")
		v, ok := in.ExpressionAttributeValues[val]
		if !ok {
			return nil, fmt.Errorf("storetest.Mem UpdateItem: no value for %s", val)
		}
		next[name(attr)] = v
	}
	if match[2] != "" {
		for _, attr := range strings.Split(match[2], ", ") {
			delete(next, name(attr))
		}
	}
	t[key(in.Key)] = next
	return &dynamodb.UpdateItemOutput{}, nil
}

// query supports "PK = :pk [AND begins_with(SK, :prefix) | AND SK BETWEEN
// :from AND :to]" with ScanIndexForward, Limit, ExclusiveStartKey and Select
// COUNT.
func (m *Mem) query(_ context.Context, in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
	match := queryRE.FindStringSubmatch(deref(in.KeyConditionExpression))
	if match == nil {
		return nil, unsupported("Query", "KeyConditionExpression", in.KeyConditionExpression)
	}
	if in.FilterExpression != nil {
		return nil, unsupported("Query", "FilterExpression", in.FilterExpression)
	}
	pk, prefix := str(in.ExpressionAttributeValues[match[1]]), ""
	if match[2] != "" {
		prefix = str(in.ExpressionAttributeValues[match[2]])
	}
	inRange := func(string) bool { return true }
	if match[3] != "" {
		from, to := str(in.ExpressionAttributeValues[match[3]]), str(in.ExpressionAttributeValues[match[4]])
		inRange = func(sk string) bool { return from <= sk && sk <= to }
	}
	m.mu.Lock()
	items := m.sorted(*in.TableName, func(it map[string]ddbtypes.AttributeValue) bool {
		sk := str(it["SK"])
		return str(it["PK"]) == pk && strings.HasPrefix(sk, prefix) && inRange(sk)
	})
	m.mu.Unlock()
	backward := in.ScanIndexForward != nil && !*in.ScanIndexForward
	if backward {
		slices.Reverse(items)
	}
	if start := in.ExclusiveStartKey; start != nil {
		items = slices.DeleteFunc(items, func(it map[string]ddbtypes.AttributeValue) bool {
			return backward && key(it) >= key(start) || !backward && key(it) <= key(start)
		})
	}
	out := &dynamodb.QueryOutput{}
	if in.Limit != nil && int(*in.Limit) < len(items) {
		items = items[:*in.Limit]
		last := items[len(items)-1]
		out.LastEvaluatedKey = map[string]ddbtypes.AttributeValue{"PK": last["PK"], "SK": last["SK"]}
	}
	out.Count = int32(len(items))
	if in.Select != ddbtypes.SelectCount {
		out.Items = items
	}
	return out, nil
}

// scan supports no filter or "SK = :sk" (store.ScanConfigs).
func (m *Mem) scan(_ context.Context, in *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
	sk := ""
	if in.FilterExpression != nil {
		if *in.FilterExpression != "SK = :sk" {
			return nil, unsupported("Scan", "FilterExpression", in.FilterExpression)
		}
		sk = str(in.ExpressionAttributeValues[":sk"])
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	items := m.sorted(*in.TableName, func(it map[string]ddbtypes.AttributeValue) bool { return sk == "" || str(it["SK"]) == sk })
	return &dynamodb.ScanOutput{Items: items, Count: int32(len(items))}, nil
}

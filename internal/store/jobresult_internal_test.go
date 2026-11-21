package store

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestIsTerminalJobEvent pins the terminal set from both sides: every job
// event constant, true only for success, fail and timeout.
func TestIsTerminalJobEvent(t *testing.T) {
	tests := map[string]bool{
		types.JobEventSuccess:               true,
		types.JobEventFail:                  true,
		types.JobEventTimeout:               true,
		types.JobEventInfraTriggerFailure:   false,
		types.JobEventInfraTriggerExhausted: false,
		types.JobEventLateDataArrival:       false,
		types.JobEventRerunAccepted:         false,
		types.JobEventRerunRejected:         false,
		types.JobEventValidationExhausted:   false,
		types.JobEventEvaluationReleased:    false,
		"":                                  false,
	}
	for event, want := range tests {
		if got := isTerminalJobEvent(event); got != want {
			t.Errorf("isTerminalJobEvent(%q) = %v, want %v", event, got, want)
		}
	}
}

// TestIsAuditJobEvent_CoversEveryJobEvent pins the rows GetLatestJobOutcome
// skips: one entry per job event constant, true only for the rows the
// stream-router writes about reruns and late data. An unknown event is an
// outcome.
func TestIsAuditJobEvent_CoversEveryJobEvent(t *testing.T) {
	tests := map[string]bool{
		types.JobEventSuccess:               false,
		types.JobEventFail:                  false,
		types.JobEventTimeout:               false,
		types.JobEventInfraTriggerFailure:   false,
		types.JobEventInfraTriggerExhausted: false,
		types.JobEventLateDataArrival:       true,
		types.JobEventRerunAccepted:         true,
		types.JobEventRerunRejected:         true,
		types.JobEventValidationExhausted:   false,
		types.JobEventEvaluationReleased:    false,
		"some-future-event":                 false,
	}
	for event, want := range tests {
		if got := isAuditJobEvent(event); got != want {
			t.Errorf("isAuditJobEvent(%q) = %v, want %v", event, got, want)
		}
	}
	for name, value := range jobEventConstants(t) {
		if _, ok := tests[value]; !ok {
			t.Errorf("types.%s (%q) has no entry: decide whether it is an outcome", name, value)
		}
	}
}

// jobEventConstants maps each JobEvent* constant declared in pkg/types to its
// value, read from the source so a new constant cannot be missed.
func jobEventConstants(t *testing.T) map[string]string {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), "../../pkg/types/dynamo.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]string{}
	for _, decl := range f.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			vs := spec.(*ast.ValueSpec)
			for i, name := range vs.Names {
				if !strings.HasPrefix(name.Name, "JobEvent") || i >= len(vs.Values) {
					continue
				}
				lit, ok := vs.Values[i].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					t.Fatalf("types.%s is not a string literal", name.Name)
				}
				v, err := strconv.Unquote(lit.Value)
				if err != nil {
					t.Fatal(err)
				}
				found[name.Name] = v
			}
		}
	}
	if len(found) == 0 {
		t.Fatal("no JobEvent constants found in pkg/types/dynamo.go")
	}
	return found
}

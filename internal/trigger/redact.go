package trigger

import (
	"cmp"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"slices"
	"strings"
	"unicode/utf8"
)

// redactedText replaces a secret value in trigger error text.
const redactedText = "[REDACTED]"

// minRedactLen is the shortest value redacted: replacing shorter ones would
// garble unrelated text in the error.
const minRedactLen = 4

// secretValues returns the INTERLOCK_ environment values that expanding the
// header values and bodies puts into a request, in each form an echo may carry
// (see echoForms). Values of the run identity names are included too: airflow
// and databricks headers expand them from the environment, and redacting a
// value that was not sent is harmless. The result is deduplicated and longest
// first, so a value is replaced before any value it contains.
func secretValues(headers map[string]string, bodies ...string) []string {
	seen := map[string]bool{}
	lookup := func(key string) string {
		v := safeEnvLookup(key)
		if utf8.RuneCountInString(v) >= minRedactLen {
			for _, form := range echoForms(v) {
				seen[form] = true
			}
		}
		return v
	}
	for _, h := range headers {
		os.Expand(h, lookup)
	}
	for _, b := range bodies {
		os.Expand(b, lookup)
	}
	values := make([]string, 0, len(seen))
	for v := range seen {
		values = append(values, v)
	}
	slices.SortFunc(values, func(a, b string) int {
		return cmp.Or(cmp.Compare(len(b), len(a)), strings.Compare(a, b))
	})
	return values
}

// echoForms returns v raw, JSON-escaped without and with HTML escaping (each
// also with "/" written as "\/", as PHP and others encode it), and
// URL-encoded for a query and for a path. A doubly escaped echo is not covered.
func echoForms(v string) []string {
	marshalled, _ := json.Marshal(v) // marshalling a string cannot fail
	plain, html := jsonStringContent(v), string(marshalled[1:len(marshalled)-1])
	return []string{
		v,
		plain, strings.ReplaceAll(plain, "/", `\/`),
		html, strings.ReplaceAll(html, "/", `\/`),
		url.QueryEscape(v), url.PathEscape(v),
	}
}

// redact replaces every occurrence of each value in s with [REDACTED]. Values
// are matched in order at each position, so pass them longest first.
func redact(s string, values []string) string {
	if len(values) == 0 {
		return s
	}
	pairs := make([]string, 0, 2*len(values))
	for _, v := range values {
		pairs = append(pairs, v, redactedText)
	}
	return strings.NewReplacer(pairs...).Replace(s)
}

// redactError returns err with values redacted from its text. A
// *JobFailedError or *TriggerError keeps its type, since callers inspect
// them; a TriggerError's message is the whole redacted text, wrappers
// included. Any other error is flattened, as a wrapped cause would still
// print the value.
func redactError(err error, values []string) error {
	if err == nil {
		return nil
	}
	var jobErr *JobFailedError
	if errors.As(err, &jobErr) {
		return &JobFailedError{Message: redact(jobErr.Message, values)}
	}
	var trigErr *TriggerError
	if errors.As(err, &trigErr) {
		return &TriggerError{Category: trigErr.Category, Message: redact(err.Error(), values)}
	}
	return errors.New(redact(err.Error(), values))
}

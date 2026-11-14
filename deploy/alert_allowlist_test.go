package deploy_test

import (
	"os"
	"regexp"
	"sort"
	"testing"
)

var (
	// alertRuleBlock spans the alert_events rule up to the closing "})" of its
	// event_pattern, so detail-type arrays in other rules are never seen.
	alertRuleBlock = regexp.MustCompile(`(?s)resource\s+"aws_cloudwatch_event_rule"\s+"alert_events"\s*\{.*?\n\s*\}\)`)
	tfDetailTypes  = regexp.MustCompile(`(?s)"?detail-type"?\s*=\s*\[(.*?)\]`)
	pyAlertTypes   = regexp.MustCompile(`(?s)\nALERT_DETAIL_TYPES\s*=\s*\[(.*?)\]`)
	quotedString   = regexp.MustCompile(`"([^"]+)"|'([^']+)'`)
	// lineComment covers "#" (HCL and Python) and "//" (HCL). Neither file
	// uses those characters inside the strings this test reads.
	lineComment = regexp.MustCompile(`(?m)(#|//).*$`)
)

// readFile returns the file with line comments removed, so a commented-out
// entry or list never counts as live.
func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return lineComment.ReplaceAllString(string(data), "")
}

// extractSet pulls the quoted strings out of a list body and fails on an
// empty list (the pattern stopped matching) or a repeated entry.
func extractSet(t *testing.T, name, body string) map[string]bool {
	t.Helper()
	set := map[string]bool{}
	for _, m := range quotedString.FindAllStringSubmatch(body, -1) {
		entry := m[1] + m[2] // exactly one quote style matched
		if set[entry] {
			t.Errorf("%s: duplicate entry %q", name, entry)
		}
		set[entry] = true
	}
	if len(set) == 0 {
		t.Fatalf("%s: no entries extracted", name)
	}
	return set
}

func missingFrom(src, dst map[string]bool) []string {
	var out []string
	for k := range src {
		if !dst[k] {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

func TestAlertAllowlistMatchesTerraform(t *testing.T) {
	block := alertRuleBlock.FindString(readFile(t, "terraform/eventbridge.tf"))
	if block == "" {
		t.Fatal("eventbridge.tf: alert_events rule not found")
	}
	tfMatch := tfDetailTypes.FindStringSubmatch(block)
	if tfMatch == nil {
		t.Fatal("eventbridge.tf: detail-type list not found in alert_events")
	}
	pyMatch := pyAlertTypes.FindStringSubmatch(readFile(t, "localstack/deploy.py"))
	if pyMatch == nil {
		t.Fatal("deploy.py: ALERT_DETAIL_TYPES list not found")
	}

	tf := extractSet(t, "eventbridge.tf", tfMatch[1])
	py := extractSet(t, "deploy.py", pyMatch[1])

	onlyTF, onlyPy := missingFrom(tf, py), missingFrom(py, tf)
	if len(onlyTF) > 0 || len(onlyPy) > 0 {
		t.Errorf("alert allowlists differ\nin eventbridge.tf but not deploy.py: %v\nin deploy.py but not eventbridge.tf: %v",
			onlyTF, onlyPy)
	}
}

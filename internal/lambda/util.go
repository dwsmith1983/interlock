package lambda

import "strings"

// SanitizeExecName replaces characters invalid for SFN execution names.
// Valid: a-z, A-Z, 0-9, -, _  (max 80 chars)
func SanitizeExecName(name string) string {
	var b strings.Builder
	for _, c := range name {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-', c == '_':
			b.WriteRune(c)
		default:
			b.WriteRune('_')
		}
	}
	s := b.String()
	if len(s) > 80 {
		s = s[:80]
	}
	return s
}

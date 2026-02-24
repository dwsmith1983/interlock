package firestore

import (
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// isNotFound returns true if the error is a Firestore NotFound error.
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	if status.Code(err) == codes.NotFound {
		return true
	}
	return false
}

// docIDPrefix returns the prefix for range queries on document IDs.
// Firestore doesn't have begins_with, so we use >= prefix and < prefixEnd.
func docIDPrefix(pk, skPrefix string) (string, string) {
	start := pk + sep + skPrefix
	end := pk + sep + incrementLastChar(skPrefix)
	return start, end
}

// incrementLastChar increments the last character of a string to create an exclusive upper bound.
func incrementLastChar(s string) string {
	if s == "" {
		return "\uffff"
	}
	return s[:len(s)-1] + string(rune(s[len(s)-1]+1))
}

// extractSK extracts the SK portion from a document ID.
func extractSK(docName string) string {
	idx := strings.Index(docName, sep)
	if idx < 0 {
		return docName
	}
	return docName[idx+len(sep):]
}

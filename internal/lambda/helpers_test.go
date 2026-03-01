package lambda_test

// strPtr returns a pointer to the given string. Used by mock_test.go for
// DynamoDB ConditionalCheckFailedException messages.
func strPtr(s string) *string { return &s }

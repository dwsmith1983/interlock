package trigger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"unicode"

	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

// SecretGetter is the Secrets Manager call LoadSecretEnv needs.
type SecretGetter interface {
	GetSecretValue(ctx context.Context, in *secretsmanager.GetSecretValueInput, opts ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error)
}

// secretKeyPattern is the shape of a key LoadSecretEnv accepts.
var secretKeyPattern = regexp.MustCompile(`^INTERLOCK_[A-Z0-9_]+$`)

// errSecretNotObject deliberately does not wrap the JSON decoder error, whose
// message can quote characters of the secret text.
var errSecretNotObject = errors.New("trigger secret is not a JSON object of strings")

// errSecretBadKey leaves the key out: an operator who swaps a key and its
// value would otherwise see the token in the error.
var errSecretBadKey = errors.New("trigger secret has a key that is not an INTERLOCK_ variable name (INTERLOCK_ followed by A-Z, 0-9 or _)")

// LoadSecretEnv reads the secret at arn, a JSON object of INTERLOCK_ variable
// names to string values, and sets each pair in the process environment so
// safeEnvLookup resolves it. It validates every key and value before setting
// any of them. Errors never contain a secret value, and name a key only once
// it has passed the shape check.
func LoadSecretEnv(ctx context.Context, client SecretGetter, arn string) error {
	out, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: &arn})
	if err != nil {
		return fmt.Errorf("read trigger secret %s: %w", arn, err)
	}
	if out == nil || out.SecretString == nil {
		return fmt.Errorf("trigger secret %s has no SecretString", arn)
	}

	var raw map[string]any
	if json.Unmarshal([]byte(*out.SecretString), &raw) != nil || raw == nil {
		return errSecretNotObject
	}

	env := make(map[string]string, len(raw))
	for k, v := range raw {
		if !secretKeyPattern.MatchString(k) {
			return errSecretBadKey
		}
		s, ok := v.(string)
		if !ok {
			return fmt.Errorf("trigger secret key %s: value is not a string", k)
		}
		// Includes NUL and a trailing newline left by echo, which would
		// otherwise surface later as an invalid header.
		if strings.ContainsFunc(s, unicode.IsControl) {
			return fmt.Errorf("trigger secret key %s: value contains a control character", k)
		}
		env[k] = s
	}

	for k, v := range env {
		if err := os.Setenv(k, v); err != nil {
			return fmt.Errorf("set trigger secret key %s: %w", k, err)
		}
	}
	return nil
}

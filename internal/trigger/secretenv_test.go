package trigger

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeSecretGetter struct {
	out *secretsmanager.GetSecretValueOutput
	err error
}

func (f *fakeSecretGetter) GetSecretValue(_ context.Context, _ *secretsmanager.GetSecretValueInput, _ ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
	return f.out, f.err
}

func secretOutput(s string) *secretsmanager.GetSecretValueOutput {
	return &secretsmanager.GetSecretValueOutput{SecretString: &s}
}

const (
	testSecretARN     = "arn:aws:secretsmanager:eu-west-1:000000000000:secret:interlock-triggers"
	notObjectMessage  = "trigger secret is not a JSON object of strings"
	badKeyMessage     = "trigger secret has a key that is not an INTERLOCK_ variable name (INTERLOCK_ followed by A-Z, 0-9 or _)"
	plainToken        = "xoxb-1234-SECRETVALUE"
	tokenShapedKey    = "dapi0123456789abcdef"
	fetchFailureCause = "access denied"
)

func TestLoadSecretEnv(t *testing.T) {
	tests := []struct {
		name      string
		getter    *fakeSecretGetter
		presetEnv map[string]string
		touches   []string
		wantErr   bool
		errEquals string
		errHas    []string
		errLacks  []string
		check     func(t *testing.T, err error)
	}{
		{
			name:    "sets every prefixed key",
			getter:  &fakeSecretGetter{out: secretOutput(`{"INTERLOCK_AIRFLOW_TOKEN":"abc","INTERLOCK_DBX":"x"}`)},
			touches: []string{"INTERLOCK_AIRFLOW_TOKEN", "INTERLOCK_DBX"},
			check: func(t *testing.T, _ error) {
				assert.Equal(t, "abc", safeEnvLookup("INTERLOCK_AIRFLOW_TOKEN"))
				assert.Equal(t, "x", safeEnvLookup("INTERLOCK_DBX"))
			},
		},
		{
			name:   "empty object",
			getter: &fakeSecretGetter{out: secretOutput(`{}`)},
		},
		{
			name:      "secret wins over existing env",
			getter:    &fakeSecretGetter{out: secretOutput(`{"INTERLOCK_A":"from-secret"}`)},
			presetEnv: map[string]string{"INTERLOCK_A": "from-env"},
			touches:   []string{"INTERLOCK_A"},
			check: func(t *testing.T, _ error) {
				assert.Equal(t, "from-secret", safeEnvLookup("INTERLOCK_A"))
			},
		},
		{
			// The decoder's own message quotes a single input character, so
			// only the exact fixed text proves it was not wrapped.
			name:      "plain token is rejected without echoing it",
			getter:    &fakeSecretGetter{out: secretOutput(plainToken)},
			wantErr:   true,
			errEquals: notObjectMessage,
			check: func(t *testing.T, err error) {
				for i := 0; i+3 <= len(plainToken); i++ {
					assert.NotContains(t, err.Error(), plainToken[i:i+3])
				}
			},
		},
		{
			name:      "array is not an object",
			getter:    &fakeSecretGetter{out: secretOutput(`["INTERLOCK_A"]`)},
			touches:   []string{"INTERLOCK_A"},
			wantErr:   true,
			errEquals: notObjectMessage,
		},
		{
			name:      "null is not an object",
			getter:    &fakeSecretGetter{out: secretOutput(`null`)},
			wantErr:   true,
			errEquals: notObjectMessage,
		},
		{
			name:     "non-string value names the key only",
			getter:   &fakeSecretGetter{out: secretOutput(`{"INTERLOCK_A":{"t":"s3cret"}}`)},
			touches:  []string{"INTERLOCK_A"},
			wantErr:  true,
			errHas:   []string{"INTERLOCK_A"},
			errLacks: []string{"s3cret"},
		},
		{
			name:     "NUL in a value names the key only and sets nothing",
			getter:   &fakeSecretGetter{out: secretOutput(`{"INTERLOCK_A":"s3c\u0000zq9"}`)},
			touches:  []string{"INTERLOCK_A"},
			wantErr:  true,
			errHas:   []string{"INTERLOCK_A"},
			errLacks: []string{"s3c", "zq9"},
			check: func(t *testing.T, _ error) {
				assert.Empty(t, os.Getenv("INTERLOCK_A"))
			},
		},
		{
			name:     "trailing newline in a value names the key only",
			getter:   &fakeSecretGetter{out: secretOutput(`{"INTERLOCK_A":"s3cret\n"}`)},
			touches:  []string{"INTERLOCK_A"},
			wantErr:  true,
			errHas:   []string{"INTERLOCK_A"},
			errLacks: []string{"s3cret"},
		},
		{
			name:     "unprefixed key is not named and nothing is set",
			getter:   &fakeSecretGetter{out: secretOutput(`{"AIRFLOW_TOKEN":"s3cret"}`)},
			touches:  []string{"AIRFLOW_TOKEN"},
			wantErr:  true,
			errHas:   []string{"INTERLOCK_"},
			errLacks: []string{"AIRFLOW_TOKEN", "s3cret"},
			check: func(t *testing.T, _ error) {
				assert.Empty(t, os.Getenv("AIRFLOW_TOKEN"))
			},
		},
		{
			name:      "swapped key and value does not echo the key",
			getter:    &fakeSecretGetter{out: secretOutput(`{"` + tokenShapedKey + `":"INTERLOCK_DATABRICKS_TOKEN"}`)},
			touches:   []string{tokenShapedKey},
			wantErr:   true,
			errEquals: badKeyMessage,
			errLacks:  []string{tokenShapedKey},
		},
		{
			name:      "trailing newline after a valid name is rejected",
			getter:    &fakeSecretGetter{out: secretOutput(`{"INTERLOCK_A\nxoxb":"x"}`)},
			touches:   []string{"INTERLOCK_A\nxoxb"},
			wantErr:   true,
			errEquals: badKeyMessage,
			errLacks:  []string{"xoxb"},
		},
		{
			name:      "prefix in the middle of a key is rejected",
			getter:    &fakeSecretGetter{out: secretOutput(`{"dapiXXX_INTERLOCK_A":"x"}`)},
			touches:   []string{"dapiXXX_INTERLOCK_A"},
			wantErr:   true,
			errEquals: badKeyMessage,
			errLacks:  []string{"dapiXXX"},
		},
		{
			name:      "lowercase key is rejected",
			getter:    &fakeSecretGetter{out: secretOutput(`{"INTERLOCK_token":"x"}`)},
			touches:   []string{"INTERLOCK_token"},
			wantErr:   true,
			errEquals: badKeyMessage,
		},
		{
			name:    "fetch error names the ARN and the cause",
			getter:  &fakeSecretGetter{err: errors.New(fetchFailureCause)},
			wantErr: true,
			errHas:  []string{testSecretARN, fetchFailureCause},
		},
		{
			name:    "nil SecretString names the ARN",
			getter:  &fakeSecretGetter{out: &secretsmanager.GetSecretValueOutput{}},
			wantErr: true,
			errHas:  []string{testSecretARN},
		},
		{
			name:    "nil output without an error names the ARN",
			getter:  &fakeSecretGetter{},
			wantErr: true,
			errHas:  []string{testSecretARN},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, k := range tt.touches {
				t.Setenv(k, "")
			}
			for k, v := range tt.presetEnv {
				t.Setenv(k, v)
			}

			err := LoadSecretEnv(context.Background(), tt.getter, testSecretARN)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errEquals != "" {
					assert.EqualError(t, err, tt.errEquals)
				}
				for _, s := range tt.errHas {
					assert.Contains(t, err.Error(), s)
				}
				for _, s := range tt.errLacks {
					assert.NotContains(t, err.Error(), s)
				}
			} else {
				require.NoError(t, err)
			}
			if tt.check != nil {
				tt.check(t, err)
			}
		})
	}
}

func TestLoadSecretEnv_SetsNothingWhenAnyKeyIsInvalid(t *testing.T) {
	t.Setenv("INTERLOCK_A", "")
	t.Setenv("BAD", "")
	getter := &fakeSecretGetter{out: secretOutput(`{"INTERLOCK_A":"a","BAD":"s"}`)}

	// Map iteration order is random, so a single call might check BAD before
	// INTERLOCK_A by chance; repeating makes a set-as-you-go loop fail reliably.
	for range 50 {
		err := LoadSecretEnv(context.Background(), getter, testSecretARN)
		require.Error(t, err)
		require.Empty(t, os.Getenv("INTERLOCK_A"))
	}
}

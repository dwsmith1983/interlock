package main

import (
	"testing"

	ilambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateEnv_MissingVars(t *testing.T) {
	t.Setenv("SLACK_CHANNEL_ID", "")
	t.Setenv("EVENTS_TABLE", "")
	t.Setenv("EVENTS_TTL_DAYS", "")

	err := ilambda.ValidateEnv("alert-dispatcher")
	require.Error(t, err, "should report missing env vars")
	assert.Contains(t, err.Error(), "EVENTS_TABLE")
	assert.Contains(t, err.Error(), "EVENTS_TTL_DAYS")
	assert.NotContains(t, err.Error(), "SLACK_CHANNEL_ID", "Slack is optional (logging-only mode)")
}

func TestValidateEnv_AllSet(t *testing.T) {
	envVars := map[string]string{
		"EVENTS_TABLE":    "events",
		"EVENTS_TTL_DAYS": "90",
	}
	for k, v := range envVars {
		t.Setenv(k, v)
	}

	err := ilambda.ValidateEnv("alert-dispatcher")
	require.NoError(t, err, "should pass when all env vars are set")
}

func TestSlackMode(t *testing.T) {
	tests := []struct {
		name             string
		token            string
		channelID        string
		secretConfigured bool
		wantLoggingOnly  bool
		wantErrContains  []string
	}{
		{name: "no token runs logging-only", token: "", channelID: "", wantLoggingOnly: true},
		{name: "no token with channel runs logging-only", token: "", channelID: "C12345", wantLoggingOnly: true},
		{name: "token and channel posts to Slack", token: "xoxb-test", channelID: "C12345", wantLoggingOnly: false},
		{name: "token without channel is a misconfiguration", token: "xoxb-test", channelID: "", wantErrContains: []string{"SLACK_CHANNEL_ID"}},
		{name: "configured secret that is empty is an error", token: "", channelID: "C12345", secretConfigured: true, wantErrContains: []string{"SLACK_SECRET_ARN", "empty"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			loggingOnly, err := slackMode(tt.token, tt.channelID, tt.secretConfigured)
			if len(tt.wantErrContains) > 0 {
				require.Error(t, err)
				for _, want := range tt.wantErrContains {
					assert.Contains(t, err.Error(), want)
				}
				assert.False(t, loggingOnly)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantLoggingOnly, loggingOnly)
		})
	}
}

func TestLoggingOnlyWarning(t *testing.T) {
	tests := []struct {
		name      string
		channelID string
		want      string
	}{
		{name: "no channel keeps the generic warning", channelID: "", want: "no Slack token configured (SLACK_BOT_TOKEN / SLACK_SECRET_ARN); running in logging-only mode"},
		{name: "channel without token says alerts are not delivered", channelID: "C12345", want: "SLACK_CHANNEL_ID is set but no Slack token is configured (SLACK_BOT_TOKEN / SLACK_SECRET_ARN); alerts are NOT delivered to Slack, logging-only mode"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, loggingOnlyWarning(tt.channelID))
		})
	}
}

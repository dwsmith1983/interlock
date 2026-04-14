package client_test

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/dwsmith1983/interlock/internal/client"
	"github.com/sony/gobreaker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockHTTP struct {
	err  error
	resp *http.Response
}

func (m *mockHTTP) Do(_ *http.Request) (*http.Response, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.resp, nil
}

func tripConfig() client.BreakerConfig {
	return client.BreakerConfig{
		Name:        "test",
		MaxRequests: 1,
		Timeout:     100 * time.Millisecond,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= 2
		},
	}
}

func TestBreakerClient_SuccessPassesThrough(t *testing.T) {
	mock := &mockHTTP{resp: &http.Response{StatusCode: 200}}
	bc := client.NewBreakerClient(mock, client.DefaultBreakerConfig("test"))

	resp, err := bc.Do(&http.Request{})
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
}

func TestBreakerClient_FailuresTrip(t *testing.T) {
	mock := &mockHTTP{err: errors.New("connection refused")}
	bc := client.NewBreakerClient(mock, tripConfig())

	// Two failures to trip (per config)
	_, _ = bc.Do(&http.Request{})
	_, _ = bc.Do(&http.Request{})

	assert.Equal(t, gobreaker.StateOpen, bc.State())
}

func TestBreakerClient_OpenRejectsImmediately(t *testing.T) {
	mock := &mockHTTP{err: errors.New("connection refused")}
	bc := client.NewBreakerClient(mock, tripConfig())

	_, _ = bc.Do(&http.Request{})
	_, _ = bc.Do(&http.Request{})

	// Now open — should reject without calling inner
	mock.err = nil
	mock.resp = &http.Response{StatusCode: 200}
	_, err := bc.Do(&http.Request{})

	require.Error(t, err)
	assert.ErrorIs(t, err, gobreaker.ErrOpenState)
}

func TestBreakerClient_HalfOpenAllowsLimited(t *testing.T) {
	mock := &mockHTTP{err: errors.New("fail")}
	bc := client.NewBreakerClient(mock, tripConfig())

	_, _ = bc.Do(&http.Request{})
	_, _ = bc.Do(&http.Request{})
	assert.Equal(t, gobreaker.StateOpen, bc.State())

	// Wait for timeout to transition to half-open
	time.Sleep(150 * time.Millisecond)

	// Now in half-open, one success should close it
	mock.err = nil
	mock.resp = &http.Response{StatusCode: 200}
	resp, err := bc.Do(&http.Request{})
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, gobreaker.StateClosed, bc.State())
}

func TestBreakerClient_StateReflectsCurrent(t *testing.T) {
	mock := &mockHTTP{resp: &http.Response{StatusCode: 200}}
	bc := client.NewBreakerClient(mock, client.DefaultBreakerConfig("test"))

	assert.Equal(t, gobreaker.StateClosed, bc.State())
}

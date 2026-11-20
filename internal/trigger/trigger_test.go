package trigger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecuteHTTP_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	defer func() { defaultHTTPClient = origClient }()

	cfg := &types.TriggerConfig{
		Type: types.TriggerHTTP,
		HTTP: &types.HTTPTriggerConfig{Method: "POST", URL: srv.URL},
	}
	_, err := NewRunner().Execute(context.Background(), cfg)
	require.NoError(t, err)
}

func TestExecuteHTTP_ErrorBodyTruncated(t *testing.T) {
	// Return a body larger than 512 bytes.
	longBody := strings.Repeat("x", 1024)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(longBody))
	}))
	defer srv.Close()

	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	defer func() { defaultHTTPClient = origClient }()

	cfg := &types.TriggerConfig{
		Type: types.TriggerHTTP,
		HTTP: &types.HTTPTriggerConfig{Method: "GET", URL: srv.URL},
	}
	_, err := NewRunner().Execute(context.Background(), cfg)
	require.Error(t, err)

	// Error message must contain the truncated body (512 bytes), not the full 1024.
	msg := err.Error()
	// The error format is "trigger returned status 500: <body>".
	const prefix = "trigger returned status 500: "
	if !strings.HasPrefix(msg, prefix) {
		t.Fatalf("unexpected error format: %s", msg)
	}
	bodyPart := msg[len(prefix):]
	if len(bodyPart) != maxErrorBodyBytes {
		t.Errorf("body length in error = %d, want %d", len(bodyPart), maxErrorBodyBytes)
	}
}

func TestExecuteHTTP_ErrorBodySanitized(t *testing.T) {
	// Return a body with control characters.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("bad\x00input\x1b[31mred"))
	}))
	defer srv.Close()

	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	defer func() { defaultHTTPClient = origClient }()

	cfg := &types.TriggerConfig{
		Type: types.TriggerHTTP,
		HTTP: &types.HTTPTriggerConfig{Method: "GET", URL: srv.URL},
	}
	_, err := NewRunner().Execute(context.Background(), cfg)
	require.Error(t, err)

	msg := err.Error()
	assert.NotContains(t, msg, "\x00")
	assert.NotContains(t, msg, "\x1b")
	assert.Contains(t, msg, "badinput")
}

func TestSafeEnvLookup_AllowsInterlockPrefix(t *testing.T) {
	t.Setenv("INTERLOCK_API_KEY", "safe-value")

	result := safeEnvLookup("INTERLOCK_API_KEY")
	assert.Equal(t, "safe-value", result)
}

func TestSafeEnvLookup_BlocksNonInterlockVars(t *testing.T) {
	t.Setenv("AWS_SECRET_ACCESS_KEY", "should-not-leak")
	t.Setenv("HOME", "/home/test")

	assert.Equal(t, "", safeEnvLookup("AWS_SECRET_ACCESS_KEY"))
	assert.Equal(t, "", safeEnvLookup("HOME"))
	assert.Equal(t, "", safeEnvLookup("PATH"))
}

func TestExecuteHTTP_EnvExpansionRestricted(t *testing.T) {
	t.Setenv("INTERLOCK_TOKEN", "good-token")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "leaked")

	var receivedAuth string
	var receivedBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		body := make([]byte, 1024)
		n, _ := r.Body.Read(body)
		receivedBody = string(body[:n])
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	defer func() { defaultHTTPClient = origClient }()

	cfg := &types.TriggerConfig{
		Type: types.TriggerHTTP,
		HTTP: &types.HTTPTriggerConfig{
			Method:  "POST",
			URL:     srv.URL,
			Headers: map[string]string{"Authorization": "Bearer ${INTERLOCK_TOKEN}"},
			Body:    `{"secret":"${AWS_SECRET_ACCESS_KEY}","token":"${INTERLOCK_TOKEN}"}`,
		},
	}
	_, err := NewRunner().Execute(context.Background(), cfg)
	require.NoError(t, err)

	// INTERLOCK_ prefixed vars should resolve.
	assert.Equal(t, "Bearer good-token", receivedAuth)
	assert.Contains(t, receivedBody, `"token":"good-token"`)

	// Non-INTERLOCK_ vars must NOT resolve (replaced with empty string).
	assert.NotContains(t, receivedBody, "leaked")
	assert.Contains(t, receivedBody, `"secret":""`)
}

func TestExecuteHTTP_RunIdentity(t *testing.T) {
	t.Setenv("INTERLOCK_RUN_ID", "from-env") // the run's own values must win
	t.Setenv("INTERLOCK_DATE", "from-env")
	t.Setenv("INTERLOCK_TOKEN", "good-token")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "leaked")

	var got http.Header
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	defer func() { defaultHTTPClient = origClient }()

	err := ExecuteHTTP(context.Background(), &types.HTTPTriggerConfig{
		Method: "POST",
		URL:    srv.URL,
		Headers: map[string]string{
			"X-Trace":            "${INTERLOCK_PIPELINE_ID}/${INTERLOCK_SCHEDULE_ID}/${INTERLOCK_DATE}/${INTERLOCK_RUN_ID}",
			"Authorization":      "Bearer ${INTERLOCK_TOKEN}",
			"x-interlock-run-id": "spoof",
			"X-Interlock-Date":   "spoof",
		},
		Body: `{"p":"${INTERLOCK_PIPELINE_ID}","s":"${INTERLOCK_SCHEDULE_ID}","d":"${INTERLOCK_DATE}",` +
			`"r":"${INTERLOCK_RUN_ID}","secret":"${AWS_SECRET_ACCESS_KEY}"}`,
		Run: types.RunIdentity{PipelineID: "orders", ScheduleID: "stream", Date: "2026-03-01T05", RunID: "exec-42"},
	})
	require.NoError(t, err)

	for header, want := range map[string]string{
		"X-Interlock-Pipeline-Id": "orders",
		"X-Interlock-Schedule-Id": "stream",
		"X-Interlock-Date":        "2026-03-01T05",
		"X-Interlock-Run-Id":      "exec-42",
		"X-Trace":                 "orders/stream/2026-03-01T05/exec-42",
		"Authorization":           "Bearer good-token",
	} {
		assert.Equal(t, []string{want}, got.Values(header), header)
	}
	assert.Equal(t, `{"p":"orders","s":"stream","d":"2026-03-01T05","r":"exec-42","secret":""}`, gotBody)
}

// identityServer returns the URL of a server that records each request's body.
func identityServer(t *testing.T) (url string, bodies *[]string) {
	t.Helper()
	bodies = &[]string{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		*bodies = append(*bodies, string(b))
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(srv.Close)
	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	t.Cleanup(func() { defaultHTTPClient = origClient })
	return srv.URL, bodies
}

// Identity values are free-form, so in a body they are inserted JSON-escaped
// and a configured JSON body stays valid whatever the pipeline ID holds.
func TestExecuteHTTP_IdentityInBodyIsJSONEscaped(t *testing.T) {
	for _, id := range []string{"team/orders:v2", `we"ird\path$HOME<&>`} {
		t.Run(id, func(t *testing.T) {
			t.Setenv("HOME", "/root")
			url, bodies := identityServer(t)
			err := ExecuteHTTP(context.Background(), &types.HTTPTriggerConfig{
				URL: url, Body: `{"p":"${INTERLOCK_PIPELINE_ID}"}`, Run: types.RunIdentity{PipelineID: id},
			})
			require.NoError(t, err)
			require.Len(t, *bodies, 1)
			var got map[string]string
			require.NoError(t, json.Unmarshal([]byte((*bodies)[0]), &got), "body %s", (*bodies)[0])
			assert.Equal(t, id, got["p"])
			if strings.ContainsAny(id, "<>&") {
				assert.Contains(t, (*bodies)[0], "<&>", "HTML escaping must be off")
			}
		})
	}
}

// The default body is already marshalled, so it is sent as is: a "$" in the
// pipeline ID is not expanded.
func TestExecuteHTTP_GeneratedBodyIsNotExpanded(t *testing.T) {
	t.Setenv("INTERLOCK_X", "expanded")
	url, bodies := identityServer(t)
	body := `{"pipelineId":"a${INTERLOCK_X}$INTERLOCK_X"}`
	err := ExecuteHTTP(context.Background(), &types.HTTPTriggerConfig{URL: url, Body: body, BodyGenerated: true})
	require.NoError(t, err)
	assert.Equal(t, []string{body}, *bodies)
}

// Header values stay raw; net/http refuses CR/LF, so nothing is sent.
func TestExecuteHTTP_CRLFInIdentityHeaderFails(t *testing.T) {
	url, bodies := identityServer(t)
	err := ExecuteHTTP(context.Background(), &types.HTTPTriggerConfig{
		URL: url, Run: types.RunIdentity{RunID: "run\r\nX-Evil: 1"},
	})
	require.Error(t, err)
	assert.Empty(t, *bodies, "the server must receive no request")
}

func TestTriggerError_ErrorMessage(t *testing.T) {
	t.Run("with underlying error", func(t *testing.T) {
		underlying := fmt.Errorf("connection refused")
		te := NewTriggerError(types.FailureTransient, "trigger request failed", underlying)
		assert.Equal(t, "trigger request failed: connection refused", te.Error())
	})

	t.Run("without underlying error", func(t *testing.T) {
		te := NewTriggerError(types.FailurePermanent, "trigger returned status 400: bad request", nil)
		assert.Equal(t, "trigger returned status 400: bad request", te.Error())
	})
}

func TestTriggerError_Unwrap(t *testing.T) {
	underlying := fmt.Errorf("timeout")
	te := NewTriggerError(types.FailureTimeout, "request timed out", underlying)
	assert.Equal(t, underlying, errors.Unwrap(te))
}

func TestTriggerError_ErrorsAs(t *testing.T) {
	te := NewTriggerError(types.FailurePermanent, "status 404", nil)
	wrapped := fmt.Errorf("execute trigger: %w", te)

	var target *TriggerError
	require.True(t, errors.As(wrapped, &target))
	assert.Equal(t, types.FailurePermanent, target.Category)
	assert.Equal(t, "status 404", target.Message)
}

func TestExecuteHTTP_Returns_TriggerError_On4xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("not found"))
	}))
	defer srv.Close()

	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	defer func() { defaultHTTPClient = origClient }()

	cfg := &types.TriggerConfig{
		Type: types.TriggerHTTP,
		HTTP: &types.HTTPTriggerConfig{Method: "GET", URL: srv.URL},
	}
	_, err := NewRunner().Execute(context.Background(), cfg)
	require.Error(t, err)

	var te *TriggerError
	require.True(t, errors.As(err, &te), "expected TriggerError, got %T", err)
	assert.Equal(t, types.FailurePermanent, te.Category)
	assert.Contains(t, te.Message, "status 404")
}

func TestExecuteHTTP_Returns_TriggerError_On5xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("service unavailable"))
	}))
	defer srv.Close()

	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	defer func() { defaultHTTPClient = origClient }()

	cfg := &types.TriggerConfig{
		Type: types.TriggerHTTP,
		HTTP: &types.HTTPTriggerConfig{Method: "GET", URL: srv.URL},
	}
	_, err := NewRunner().Execute(context.Background(), cfg)
	require.Error(t, err)

	var te *TriggerError
	require.True(t, errors.As(err, &te), "expected TriggerError, got %T", err)
	assert.Equal(t, types.FailureTransient, te.Category)
	assert.Contains(t, te.Message, "status 503")
}

// useProductionClient makes defaultHTTPClient the production client with rt,
// which may dial loopback, so the production timeout and redirect rule stay in
// effect.
func useProductionClient(t *testing.T, rt http.RoundTripper) {
	t.Helper()
	origClient := defaultHTTPClient
	c := *defaultHTTPClient
	c.Transport = rt
	defaultHTTPClient = &c
	t.Cleanup(func() { defaultHTTPClient = origClient })
}

// useExampleHost serves http://example.com from plain and https://example.com
// from secure (whose certificate covers example.com), so a redirect can change
// the scheme and keep the Host.
func useExampleHost(t *testing.T, plain, secure *httptest.Server) {
	t.Helper()
	tr := secure.Client().Transport.(*http.Transport).Clone()
	tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		target := plain.Listener.Addr().String()
		if strings.HasSuffix(addr, ":443") {
			target = secure.Listener.Addr().String()
		}
		return (&net.Dialer{}).DialContext(ctx, network, target)
	}
	useProductionClient(t, tr)
}

// redirectServer answers each path in routes with a 307 to its target and
// counts the requests to any other path.
func redirectServer(t *testing.T, tls bool, routes map[string]string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if target, ok := routes[r.URL.Path]; ok {
			http.Redirect(w, r, target, http.StatusTemporaryRedirect)
			return
		}
		hits.Add(1)
	})
	srv := httptest.NewUnstartedServer(h)
	if tls {
		srv.StartTLS()
	} else {
		srv.Start()
	}
	t.Cleanup(srv.Close)
	return srv, &hits
}

const refusedHint = " refused (a redirect must keep the host, port and method and must not go from https to http); point the trigger URL at the final endpoint"

// A redirect is followed only on the same Host, keeping the scheme or going
// from http to https, and keeping the method; anything else is reported.
func TestHTTPClientRedirectRule(t *testing.T) {
	b, bHits := redirectServer(t, false, nil)
	for _, timeout := range []int{0, 5} {
		t.Run(fmt.Sprintf("other port, timeout %d", timeout), func(t *testing.T) {
			a, _ := redirectServer(t, false, map[string]string{"/": b.URL + "/b"})
			useProductionClient(t, a.Client().Transport)

			err := ExecuteHTTP(context.Background(), &types.HTTPTriggerConfig{URL: a.URL, Timeout: timeout})

			require.Error(t, err)
			assert.Contains(t, err.Error(), "trigger redirect to "+b.URL+"/b"+refusedHint)
			assert.Zero(t, bHits.Load(), "the redirect target must not be called")
		})
	}

	t.Run("other host, same port", func(t *testing.T) {
		var aHits atomic.Int32
		a := httptest.NewUnstartedServer(nil)
		_, port, _ := net.SplitHostPort(a.Listener.Addr().String())
		other := "http://localhost:" + port + "/b"
		a.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/" {
				http.Redirect(w, r, other, http.StatusTemporaryRedirect)
				return
			}
			aHits.Add(1)
		})
		a.Start()
		t.Cleanup(a.Close)
		useProductionClient(t, a.Client().Transport)

		err := ExecuteHTTP(context.Background(), &types.HTTPTriggerConfig{URL: a.URL})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "redirect to "+other+refusedHint)
		assert.Zero(t, aHits.Load())
	})

	schemeRows := []struct {
		name, url  string
		plain, tls map[string]string
		ok         bool
		refusedTo  string
	}{
		{"http to https is followed", "http://example.com/start",
			map[string]string{"/start": "https://example.com/done"}, nil, true, ""},
		{"https to http is refused", "https://example.com/start",
			nil, map[string]string{"/start": "http://example.com/b"}, false, "http://example.com/b"},
		{"back to http after an upgrade is refused", "http://example.com/start",
			map[string]string{"/start": "https://example.com/mid"}, map[string]string{"/mid": "http://example.com/b"}, false, "http://example.com/b"},
	}
	for _, row := range schemeRows {
		t.Run(row.name, func(t *testing.T) {
			plain, plainHits := redirectServer(t, false, row.plain)
			secure, secureHits := redirectServer(t, true, row.tls)
			useExampleHost(t, plain, secure)

			err := ExecuteHTTP(context.Background(), &types.HTTPTriggerConfig{URL: row.url})

			if row.ok {
				require.NoError(t, err)
				assert.Equal(t, int32(1), secureHits.Load())
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), "redirect to "+row.refusedTo+refusedHint)
			assert.Zero(t, plainHits.Load(), "the downgraded request must not be sent")
		})
	}

	t.Run("same origin is followed", func(t *testing.T) {
		a, aHits := redirectServer(t, false, map[string]string{"/start": "/done"})
		useProductionClient(t, a.Client().Transport)

		require.NoError(t, ExecuteHTTP(context.Background(), &types.HTTPTriggerConfig{URL: a.URL + "/start"}))
		assert.Equal(t, int32(1), aHits.Load())
	})

	// A 302 turns the POST into a GET without its body, so the job would
	// never be triggered.
	t.Run("method change is refused", func(t *testing.T) {
		var doneHits atomic.Int32
		a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/start" {
				http.Redirect(w, r, "/done", http.StatusFound)
				return
			}
			doneHits.Add(1)
		}))
		t.Cleanup(a.Close)
		useProductionClient(t, a.Client().Transport)

		err := ExecuteHTTP(context.Background(), &types.HTTPTriggerConfig{URL: a.URL + "/start"})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "trigger redirect to "+a.URL+"/done"+refusedHint)
		assert.Zero(t, doneHits.Load())
	})

	// A 303 answers a processed POST: the job started and reports its own
	// result, so the http trigger succeeds without following it.
	t.Run("303 on the http trigger is accepted", func(t *testing.T) {
		var doneHits atomic.Int32
		a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/start" {
				http.Redirect(w, r, "/done", http.StatusSeeOther)
				return
			}
			doneHits.Add(1)
		}))
		t.Cleanup(a.Close)
		useProductionClient(t, a.Client().Transport)

		require.NoError(t, ExecuteHTTP(context.Background(), &types.HTTPTriggerConfig{URL: a.URL + "/start"}))
		assert.Zero(t, doneHits.Load())
	})

	// A GET follows a 303 like any other redirect, so a 303 that reaches the
	// trigger was refused and is not a started job.
	t.Run("303 on a GET to another port is refused", func(t *testing.T) {
		a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, b.URL+"/login", http.StatusSeeOther)
		}))
		t.Cleanup(a.Close)
		useProductionClient(t, a.Client().Transport)

		err := ExecuteHTTP(context.Background(), &types.HTTPTriggerConfig{Method: http.MethodGet, URL: a.URL})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "trigger redirect to "+b.URL+"/login"+refusedHint)
		assert.Zero(t, bHits.Load())
	})

	t.Run("same-origin 303 on a GET is followed", func(t *testing.T) {
		var doneHits atomic.Int32
		a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/start" {
				http.Redirect(w, r, "/done", http.StatusSeeOther)
				return
			}
			doneHits.Add(1)
		}))
		t.Cleanup(a.Close)
		useProductionClient(t, a.Client().Transport)

		require.NoError(t, ExecuteHTTP(context.Background(), &types.HTTPTriggerConfig{Method: http.MethodGet, URL: a.URL + "/start"}))
		assert.Equal(t, int32(1), doneHits.Load())
	})

	t.Run("redirect loop stops", func(t *testing.T) {
		a, _ := redirectServer(t, false, map[string]string{"/api/v1/dags/d/dagRuns/r": "/api/v1/dags/d/dagRuns/r"})
		useProductionClient(t, a.Client().Transport)

		_, err := CheckAirflowStatus(context.Background(), a.URL, "d", "r", 2, nil, defaultHTTPClient)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "stopped after 10 redirects")
	})
}

// airflowAndDatabricksCalls runs each airflow and databricks call against url
// with the production client.
var airflowAndDatabricksCalls = map[string]func(url string) error{
	"airflow trigger": func(url string) error {
		_, err := ExecuteAirflow(context.Background(), &types.AirflowTriggerConfig{URL: url, DagID: "d"})
		return err
	},
	"airflow status": func(url string) error {
		_, err := CheckAirflowStatus(context.Background(), url, "d", "r", 2, nil, defaultHTTPClient)
		return err
	},
	"databricks trigger": func(url string) error {
		_, err := ExecuteDatabricks(context.Background(), &types.DatabricksTriggerConfig{WorkspaceURL: url, JobID: "j"}, defaultHTTPClient)
		return err
	},
	"databricks status": func(url string) error {
		meta := map[string]interface{}{"databricks_workspace_url": url, "databricks_run_id": "1"}
		_, err := NewRunner().checkDatabricksStatus(context.Background(), meta, nil)
		return err
	},
}

// A refused redirect names its target without userinfo or query; a 3xx with
// no Location reports its status.
func TestRefusedRedirectReportsTarget(t *testing.T) {
	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(b.Close)
	target := "http://user:pw@" + b.Listener.Addr().String() + "/final?token=q1"
	for name, call := range airflowAndDatabricksCalls {
		t.Run(name, func(t *testing.T) {
			a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, target, http.StatusTemporaryRedirect)
			}))
			t.Cleanup(a.Close)
			useProductionClient(t, a.Client().Transport)

			err := call(a.URL)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "redirect to "+b.URL+"/final"+refusedHint)
			assert.NotContains(t, err.Error(), "pw")
			assert.NotContains(t, err.Error(), "q1")
		})
		t.Run(name+" without Location", func(t *testing.T) {
			a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusTemporaryRedirect)
			}))
			t.Cleanup(a.Close)
			useProductionClient(t, a.Client().Transport)

			err := call(a.URL)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "returned status 307")
		})
	}
}

// A response is read up to 1 MiB; a larger one fails with fixed text instead
// of a truncated body that would fail to parse.
func TestResponseBodyReadIsBounded(t *testing.T) {
	const limit = 1 << 20
	const prefix = `{"dag_run_id":"r","run_id":1,"state":"running","pad":"`
	body := func(size int) string { return prefix + strings.Repeat("x", size-len(prefix)-2) + `"}` }
	for name, call := range airflowAndDatabricksCalls {
		for _, size := range []int{limit, limit + 1} {
			t.Run(fmt.Sprintf("%s %d bytes", name, size), func(t *testing.T) {
				a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = w.Write([]byte(body(size)))
				}))
				t.Cleanup(a.Close)
				useProductionClient(t, a.Client().Transport)

				err := call(a.URL)

				if size == limit {
					require.NoError(t, err)
					return
				}
				require.Error(t, err)
				assert.Contains(t, err.Error(), "reading response: response exceeds 1 MiB")
			})
		}
	}

	t.Run("http error body", func(t *testing.T) {
		a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(body(limit + 1)))
		}))
		t.Cleanup(a.Close)
		useProductionClient(t, a.Client().Transport)

		err := ExecuteHTTP(context.Background(), &types.HTTPTriggerConfig{URL: a.URL})

		require.EqualError(t, err, "trigger returned status 400: response exceeds 1 MiB")
	})

	t.Run("http 5xx body", func(t *testing.T) {
		a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(body(limit + 1)))
		}))
		t.Cleanup(a.Close)
		useProductionClient(t, a.Client().Transport)

		err := ExecuteHTTP(context.Background(), &types.HTTPTriggerConfig{URL: a.URL})

		require.EqualError(t, err, "trigger returned status 503: response exceeds 1 MiB")
	})
}

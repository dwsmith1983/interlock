package alert

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/interlock-systems/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testAlert() types.Alert {
	return types.Alert{
		Level:      types.AlertLevelError,
		PipelineID: "test-pipeline",
		Message:    "something went wrong",
		Timestamp:  time.Now(),
	}
}

func TestConsoleSink_Send(t *testing.T) {
	sink := NewConsoleSink()
	assert.Equal(t, "console", sink.Name())

	for _, level := range []types.AlertLevel{types.AlertLevelError, types.AlertLevelWarning, types.AlertLevelInfo} {
		a := testAlert()
		a.Level = level
		err := sink.Send(a)
		assert.NoError(t, err)
	}
}

func TestWebhookSink_Send_Success(t *testing.T) {
	var received []byte
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		buf := make([]byte, 4096)
		n, _ := r.Body.Read(buf)
		received = buf[:n]
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	sink := NewWebhookSink(ts.URL)
	alert := testAlert()

	err := sink.Send(alert)
	require.NoError(t, err)

	var got types.Alert
	require.NoError(t, json.Unmarshal(received, &got))
	assert.Equal(t, alert.Message, got.Message)
	assert.Equal(t, alert.PipelineID, got.PipelineID)
}

func TestWebhookSink_Send_ServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	sink := NewWebhookSink(ts.URL)

	err := sink.Send(testAlert())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "500")
}

func TestWebhookSink_Send_Retry(t *testing.T) {
	var callCount atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if callCount.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	sink := NewWebhookSink(ts.URL)

	err := sink.Send(testAlert())
	assert.NoError(t, err)
	assert.Equal(t, int32(2), callCount.Load())
}

func TestFileSink_Send(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "alert-*.jsonl")
	require.NoError(t, err)
	_ = f.Close()

	sink, err := NewFileSink(f.Name())
	require.NoError(t, err)
	assert.Equal(t, "file", sink.Name())

	alert := testAlert()
	require.NoError(t, sink.Send(alert))

	data, err := os.ReadFile(f.Name())
	require.NoError(t, err)

	lines := strings.TrimSpace(string(data))
	var got types.Alert
	require.NoError(t, json.Unmarshal([]byte(lines), &got))
	assert.Equal(t, alert.Message, got.Message)
}

// errSink is a test sink that always returns an error.
type errSink struct{}

func (s *errSink) Send(_ types.Alert) error { return fmt.Errorf("sink error") }
func (s *errSink) Name() string             { return "error-sink" }

// recordSink records all alerts sent to it.
type recordSink struct {
	alerts []types.Alert
}

func (s *recordSink) Send(a types.Alert) error {
	s.alerts = append(s.alerts, a)
	return nil
}
func (s *recordSink) Name() string { return "record-sink" }

func TestDispatcher_MultiSink(t *testing.T) {
	s1 := &recordSink{}
	s2 := &recordSink{}
	d := &Dispatcher{sinks: []Sink{s1, s2}, logger: slog.Default()}

	alert := testAlert()
	d.Dispatch(alert)

	assert.Len(t, s1.alerts, 1)
	assert.Len(t, s2.alerts, 1)
	assert.Equal(t, alert.Message, s1.alerts[0].Message)
}

func TestDispatcher_SinkError_ContinuesOthers(t *testing.T) {
	failing := &errSink{}
	recording := &recordSink{}
	d := &Dispatcher{
		sinks:  []Sink{failing, recording},
		logger: slog.Default(),
	}

	d.Dispatch(testAlert())

	// Even though first sink failed, second should have received the alert
	assert.Len(t, recording.alerts, 1)
}


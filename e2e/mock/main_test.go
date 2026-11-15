package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var start = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

// fakeClock is a hand-advanced real clock.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (f *fakeClock) now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.t
}

func (f *fakeClock) tick(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.t = f.t.Add(d)
}

func newTestServer() (http.Handler, *fakeClock) {
	fc := &fakeClock{t: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)}
	return newServer(fc.now, start).routes(), fc
}

func do(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
	return rec
}

func getClock(t *testing.T, h http.Handler) time.Time {
	t.Helper()
	rec := do(h, http.MethodGet, "/clock", "")
	got, err := time.Parse(time.RFC3339Nano, rec.Body.String())
	if rec.Code != http.StatusOK || err != nil {
		t.Fatalf("GET /clock = %d %q (%v)", rec.Code, rec.Body.String(), err)
	}
	return got
}

type jobState struct {
	Launches int      `json:"launches"`
	Bodies   []string `json:"bodies"`
	RunIDs   []string `json:"runIds"`
	Pending  []int    `json:"pending"`
}

func getJob(t *testing.T, h http.Handler, pipeline string) jobState {
	t.Helper()
	rec := do(h, http.MethodGet, "/jobs/"+pipeline, "")
	var js jobState
	if err := json.Unmarshal(rec.Body.Bytes(), &js); rec.Code != http.StatusOK || err != nil {
		t.Fatalf("GET /jobs/%s = %d %q (%v)", pipeline, rec.Code, rec.Body.String(), err)
	}
	return js
}

func TestClockTicksAndHealthz(t *testing.T) {
	h, fc := newTestServer()
	rec := do(h, http.MethodGet, "/clock", "")
	if ct := rec.Header().Get("Content-Type"); rec.Code != http.StatusOK || !strings.HasPrefix(ct, "text/plain") ||
		rec.Body.String() != "2026-03-01T12:00:00Z" {
		t.Errorf("GET /clock = %d %q %q, want 200 text/plain 2026-03-01T12:00:00Z", rec.Code, ct, rec.Body.String())
	}
	fc.tick(1500 * time.Millisecond)
	if got, want := getClock(t, h), start.Add(1500*time.Millisecond); !got.Equal(want) {
		t.Errorf("clock after tick = %v, want %v", got, want)
	}
	if rec := do(h, http.MethodGet, "/healthz", ""); rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Errorf("healthz = %d %q", rec.Code, rec.Body.String())
	}
}

func TestClockAdvance(t *testing.T) {
	cur := start.Add(time.Second) // the fake clock ticks one second before each POST
	tests := []struct {
		name, body string
		want       time.Time
	}{
		{"by", `{"by":"90m"}`, cur.Add(90 * time.Minute)},
		{"to", `{"to":"2026-03-02T00:00:00Z"}`, time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC)},
		{"to now", `{"to":"2026-03-01T12:00:01Z"}`, cur},
		{"by zero", `{"by":"0s"}`, cur},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, fc := newTestServer()
			fc.tick(time.Second)
			rec := do(h, http.MethodPost, "/clock/advance", tt.body)
			if want := tt.want.Format(time.RFC3339Nano); rec.Code != http.StatusOK || rec.Body.String() != want {
				t.Fatalf("advance %s = %d %q, want 200 %q", tt.body, rec.Code, rec.Body.String(), want)
			}
			// The clock re-anchors on the new time and keeps ticking from it.
			fc.tick(time.Second)
			if got := getClock(t, h); !got.Equal(tt.want.Add(time.Second)) {
				t.Errorf("clock after tick = %v, want %v", got, tt.want.Add(time.Second))
			}
		})
	}
}

func TestRejectsBadInput(t *testing.T) {
	const adv, out = "/clock/advance", "/jobs/p1/outcome"
	tests := []struct {
		name, path, body string
		want             int
	}{
		// 12:00:00.5 is after the anchor (12:00:00) but behind the current 12:00:01.
		{"to backwards", adv, `{"to":"2026-03-01T12:00:00.5Z"}`, http.StatusConflict},
		{"by negative", adv, `{"by":"-1s"}`, http.StatusConflict},
		{"both", adv, `{"to":"2026-03-02T00:00:00Z","by":"1h"}`, http.StatusBadRequest},
		{"neither", adv, `{}`, http.StatusBadRequest},
		{"bad json", adv, `{"by":`, http.StatusBadRequest},
		{"trailing data", adv, `{"by":"1h"}}`, http.StatusBadRequest},
		{"unknown field", adv, `{"by":"1h","extra":1}`, http.StatusBadRequest},
		{"bad duration", adv, `{"by":"soon"}`, http.StatusBadRequest},
		{"bad time", adv, `{"to":"tomorrow"}`, http.StatusBadRequest},
		{"status 1xx", out, `{"statuses":[199]}`, http.StatusBadRequest},
		{"status 600", out, `{"statuses":[200,600]}`, http.StatusBadRequest},
		{"outcome bad json", out, `{"statuses":`, http.StatusBadRequest},
		{"outcome unknown field", out, `{"statuses":[200],"x":1}`, http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, fc := newTestServer()
			fc.tick(time.Second)
			if rec := do(h, http.MethodPost, tt.path, tt.body); rec.Code != tt.want {
				t.Errorf("POST %s %s = %d %q, want %d", tt.path, tt.body, rec.Code, rec.Body.String(), tt.want)
			}
			if got := getClock(t, h); !got.Equal(start.Add(time.Second)) {
				t.Errorf("clock moved to %v on a rejected request", got)
			}
			if got := getJob(t, h, "p1").Pending; len(got) != 0 {
				t.Errorf("pending = %v after a rejected request", got)
			}
		})
	}
}

func TestJobRunStatusQueue(t *testing.T) {
	h, _ := newTestServer()
	// The second outcome replaces the first queue rather than appending.
	for _, body := range []string{`{"statuses":[503,503,503]}`, `{"statuses":[500,202]}`} {
		if rec := do(h, http.MethodPost, "/jobs/p1/outcome", body); rec.Code != http.StatusNoContent {
			t.Fatalf("outcome %s = %d %q", body, rec.Code, rec.Body.String())
		}
	}
	if got := getJob(t, h, "p1").Pending; !reflect.DeepEqual(got, []int{500, 202}) {
		t.Errorf("pending = %v, want [500 202]", got)
	}
	for i, want := range []int{500, 202, 200, 200} {
		rec := do(h, http.MethodPost, "/jobs/p1/run", `{"n":`+strconv.Itoa(i)+`}`)
		if wantBody := fmt.Sprintf(`{"launch":%d}`, i+1); rec.Code != want || strings.TrimSpace(rec.Body.String()) != wantBody {
			t.Errorf("run %d = %d %q, want %d %s", i+1, rec.Code, rec.Body.String(), want, wantBody)
		}
	}
	want := jobState{Launches: 4, Bodies: []string{`{"n":0}`, `{"n":1}`, `{"n":2}`, `{"n":3}`}, RunIDs: []string{"", "", "", ""}, Pending: []int{}}
	if got := getJob(t, h, "p1"); !reflect.DeepEqual(got, want) {
		t.Errorf("state = %+v, want %+v", got, want)
	}
}

// Each launch's X-Interlock-Run-Id is kept in launch order, so the harness
// can report a result for the run the trigger named.
func TestJobRecordsRunIDs(t *testing.T) {
	h, _ := newTestServer()
	for _, id := range []string{"run-a", "run-b"} {
		req := httptest.NewRequest(http.MethodPost, "/jobs/p/run", strings.NewReader("{}"))
		req.Header.Set("X-Interlock-Run-Id", id)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("run %s = %d %q", id, rec.Code, rec.Body.String())
		}
	}
	if got := getJob(t, h, "p").RunIDs; !reflect.DeepEqual(got, []string{"run-a", "run-b"}) {
		t.Errorf("runIds = %v, want [run-a run-b]", got)
	}
}

func TestJobPipelinesIndependentAndUnknownZero(t *testing.T) {
	h, _ := newTestServer()
	do(h, http.MethodPost, "/jobs/a/outcome", `{"statuses":[503]}`)
	do(h, http.MethodPost, "/jobs/a/run", "")
	do(h, http.MethodPost, "/jobs/a/run", "")
	do(h, http.MethodPost, "/jobs/b/run", "")
	if a, b := getJob(t, h, "a"), getJob(t, h, "b"); a.Launches != 2 || b.Launches != 1 {
		t.Errorf("launches a=%d b=%d, want 2 and 1", a.Launches, b.Launches)
	}
	rec := do(h, http.MethodGet, "/jobs/never", "")
	if want := `{"launches":0,"bodies":[],"runIds":[],"pending":[]}`; rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != want {
		t.Errorf("unknown pipeline = %d %q, want 200 %s", rec.Code, rec.Body.String(), want)
	}
}

func TestJobConcurrentRuns(t *testing.T) {
	h, _ := newTestServer()
	const n = 50
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() { do(h, http.MethodPost, "/jobs/p/run", "{}") })
	}
	wg.Wait()
	if js := getJob(t, h, "p"); js.Launches != n || len(js.Bodies) != n {
		t.Errorf("launches = %d bodies = %d, want %d", js.Launches, len(js.Bodies), n)
	}
}

func TestClockStart(t *testing.T) {
	wall := time.Date(2030, 5, 6, 7, 8, 9, 123456789, time.UTC)
	for env, want := range map[string]time.Time{"": wall.Truncate(time.Second), "2026-03-01T12:00:00Z": start} {
		if got, err := clockStart(env, wall); err != nil || !got.Equal(want) {
			t.Errorf("clockStart(%q) = %v, %v; want %v", env, got, err, want)
		}
	}
	if _, err := clockStart("not-a-time", wall); err == nil {
		t.Error("clockStart(not-a-time) gave no error")
	}
}

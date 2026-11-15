// Command mock is the e2e test double the Lambdas and the e2e harness talk to.
// It serves a simulated clock (read by e2e Lambda builds through
// E2E_CLOCK_URL) and a scriptable HTTP job endpoint used as the http trigger
// target. All state lives in memory and resets on restart.
//
// Environment:
//
//	MOCK_ADDR         listen address (default :8080)
//	MOCK_CLOCK_START  simulated start time, RFC3339 (default: now, truncated to the second)
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

const defaultJobStatus = http.StatusOK

// runIDHeader is the run ID the http trigger sends with each launch.
const runIDHeader = "X-Interlock-Run-Id"

// job is one pipeline's scripted outcomes and the launches seen so far.
type job struct {
	launches int
	bodies   []string
	runIDs   []string
	pending  []int
}

type server struct {
	now func() time.Time

	mu         sync.Mutex
	simAnchor  time.Time
	realAnchor time.Time
	jobs       map[string]*job
}

// newServer returns a server whose simulated clock reads start at the current
// now() and then advances at the rate of now().
func newServer(now func() time.Time, start time.Time) *server {
	return &server{
		now:        now,
		simAnchor:  start,
		realAnchor: now(),
		jobs:       map[string]*job{},
	}
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /clock", s.getClock)
	mux.HandleFunc("POST /clock/advance", s.advanceClock)
	mux.HandleFunc("POST /jobs/{pipeline}/outcome", s.setOutcome)
	mux.HandleFunc("POST /jobs/{pipeline}/run", s.runJob)
	mux.HandleFunc("GET /jobs/{pipeline}", s.getJob)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeText(w, http.StatusOK, "ok")
	})
	return mux
}

// simNow must be called with s.mu held.
func (s *server) simNow() time.Time {
	return s.simAnchor.Add(s.now().Sub(s.realAnchor))
}

func (s *server) getClock(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	now := s.simNow()
	s.mu.Unlock()
	writeText(w, http.StatusOK, formatTime(now))
}

func (s *server) advanceClock(w http.ResponseWriter, r *http.Request) {
	var req struct {
		To *string `json:"to"`
		By *string `json:"by"`
	}
	if err := decodeStrict(r.Body, &req); err != nil {
		writeText(w, http.StatusBadRequest, err.Error())
		return
	}
	if (req.To == nil) == (req.By == nil) {
		writeText(w, http.StatusBadRequest, `exactly one of "to" or "by" is required`)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.simNow()
	var target time.Time
	if req.To != nil {
		t, err := time.Parse(time.RFC3339, *req.To)
		if err != nil {
			writeText(w, http.StatusBadRequest, fmt.Sprintf("bad \"to\": %v", err))
			return
		}
		target = t
	} else {
		d, err := time.ParseDuration(*req.By)
		if err != nil {
			writeText(w, http.StatusBadRequest, fmt.Sprintf("bad \"by\": %v", err))
			return
		}
		target = current.Add(d)
	}
	if target.Before(current) {
		writeText(w, http.StatusConflict, fmt.Sprintf("clock cannot move backwards: %s is before %s",
			formatTime(target), formatTime(current)))
		return
	}
	s.simAnchor, s.realAnchor = target, s.now()
	writeText(w, http.StatusOK, formatTime(target))
}

func (s *server) setOutcome(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Statuses []int `json:"statuses"`
	}
	if err := decodeStrict(r.Body, &req); err != nil {
		writeText(w, http.StatusBadRequest, err.Error())
		return
	}
	for _, code := range req.Statuses {
		// A 1xx is an interim response: the caller would still see 200.
		if code < 200 || code > 599 {
			writeText(w, http.StatusBadRequest, fmt.Sprintf("status %d outside 200-599", code))
			return
		}
	}

	s.mu.Lock()
	s.jobFor(r.PathValue("pipeline")).pending = append([]int(nil), req.Statuses...)
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) runJob(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeText(w, http.StatusBadRequest, fmt.Sprintf("read body: %v", err))
		return
	}

	s.mu.Lock()
	j := s.jobFor(r.PathValue("pipeline"))
	j.launches++
	j.bodies = append(j.bodies, string(body))
	j.runIDs = append(j.runIDs, r.Header.Get(runIDHeader))
	status := defaultJobStatus
	if len(j.pending) > 0 {
		status, j.pending = j.pending[0], j.pending[1:]
	}
	launch := j.launches
	s.mu.Unlock()

	writeJSON(w, status, map[string]int{"launch": launch})
}

func (s *server) getJob(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	resp := struct {
		Launches int      `json:"launches"`
		Bodies   []string `json:"bodies"`
		RunIDs   []string `json:"runIds"`
		Pending  []int    `json:"pending"`
	}{Bodies: []string{}, RunIDs: []string{}, Pending: []int{}}
	if j, ok := s.jobs[r.PathValue("pipeline")]; ok {
		resp.Launches = j.launches
		resp.Bodies = append(resp.Bodies, j.bodies...)
		resp.RunIDs = append(resp.RunIDs, j.runIDs...)
		resp.Pending = append(resp.Pending, j.pending...)
	}
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, resp)
}

// jobFor must be called with s.mu held.
func (s *server) jobFor(pipeline string) *job {
	j, ok := s.jobs[pipeline]
	if !ok {
		j = &job{}
		s.jobs[pipeline] = j
	}
	return j
}

// decodeStrict decodes a single JSON value and rejects unknown fields.
func decodeStrict(r io.Reader, v any) error {
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("bad JSON: %w", err)
	}
	if err := dec.Decode(new(json.RawMessage)); !errors.Is(err, io.EOF) {
		return errors.New("bad JSON: trailing data")
	}
	return nil
}

func formatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

func writeText(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil {
		writeText(w, http.StatusInternalServerError, fmt.Sprintf("encode: %v", err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(buf.Bytes())
}

// clockStart parses MOCK_CLOCK_START, falling back to wall truncated to the second.
func clockStart(env string, wall time.Time) (time.Time, error) {
	if env == "" {
		return wall.Truncate(time.Second), nil
	}
	t, err := time.Parse(time.RFC3339, env)
	if err != nil {
		return time.Time{}, fmt.Errorf("MOCK_CLOCK_START %q is not RFC3339: %w", env, err)
	}
	return t, nil
}

func main() {
	start, err := clockStart(os.Getenv("MOCK_CLOCK_START"), time.Now())
	if err != nil {
		log.Fatalf("mock: %v", err)
	}
	addr := os.Getenv("MOCK_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	srv := &http.Server{
		Addr:              addr,
		Handler:           newServer(time.Now, start).routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("mock: listening on %s, simulated clock starts at %s", addr, formatTime(start))
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("mock: %v", err)
	}
}

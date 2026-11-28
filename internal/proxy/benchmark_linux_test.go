//go:build linux

package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type benchmarkAPIFixture struct {
	server     *httptest.Server
	reloader   *Reloader
	path       string
	configText string
	token      string
	diagnostic string
	calls      *atomic.Int32
	started    chan struct{}
	release    chan struct{}
	request    func(method, path, credential, origin, body string) *httptest.ResponseRecorder
}

func newBenchmarkAPIFixture(t *testing.T) *benchmarkAPIFixture {
	t.Helper()
	fixture := &benchmarkAPIFixture{
		token:      strings.Repeat("b", 32),
		diagnostic: strings.Repeat("d", 32),
		calls:      &atomic.Int32{},
		started:    make(chan struct{}),
		release:    make(chan struct{}),
	}
	var once sync.Once
	fixture.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		fixture.calls.Add(1)
		switch req.URL.Path {
		case "/diagnostics/v1/models":
			serveBenchmarkModelObservation(t, w, req, fixture.diagnostic)
		case "/v1/chat/completions":
			once.Do(func() { close(fixture.started) })
			select {
			case <-req.Context().Done():
			case <-fixture.release:
			}
		default:
			http.NotFound(w, req)
		}
	}))
	t.Cleanup(func() {
		close(fixture.release)
		fixture.server.Close()
	})

	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	fixture.path = configPath(t)
	fixture.configText = fmt.Sprintf(`default_backend: local
backends: {local: {url: %q}}
diagnostics_api_key: %q
benchmark: {token: %q, proxy_origin: %q, report_directory: %q, public_read_only: true}
`, fixture.server.URL, fixture.diagnostic, fixture.token, fixture.server.URL, directory)
	if err := os.WriteFile(fixture.path, []byte(fixture.configText), 0o600); err != nil {
		t.Fatal(err)
	}
	reloader, _, err := NewReloader(fixture.path, discardLogger(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	fixture.reloader = reloader
	fixture.request = func(method, path, credential, origin, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, "http://proxy.test"+path, strings.NewReader(body))
		setBenchmarkRequestHeaders(req, credential, origin)
		recorder := httptest.NewRecorder()
		fixture.reloader.ServeHTTP(recorder, req)
		return recorder
	}
	t.Cleanup(fixture.reloader.Stop)
	return fixture
}

func serveBenchmarkModelObservation(t *testing.T, w http.ResponseWriter, req *http.Request, diagnostic string) {
	t.Helper()
	if req.Header.Get("Authorization") != "Bearer "+diagnostic {
		t.Error("wrong diagnostics credential")
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"models": []any{map[string]any{
			"identity":  map[string]string{"backend_id": "local", "public_id": "m", "local_id": "m"},
			"lifecycle": map[string]any{"observed": true, "current_state": "ready", "observed_at": time.Now().UTC()},
		}},
	})
}

func setBenchmarkRequestHeaders(req *http.Request, credential, origin string) {
	if credential != "" {
		req.Header.Set("Authorization", "Bearer "+credential)
	}
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	req.Header.Set("Content-Type", "application/json")
}

func TestBenchmarkAPIConsentAndCancellation(t *testing.T) {
	fixture := newBenchmarkAPIFixture(t)
	assertBenchmarkUIAndInvalidRequests(t, fixture)
	jobID := startBenchmarkJob(t, fixture)
	assertBenchmarkJobIsExclusive(t, fixture, jobID)
	status := waitForCanceledBenchmark(t, fixture, jobID)
	assertBenchmarkReportAccess(t, fixture, status.RunID, jobID)
	assertBenchmarkConfigRestartRequired(t, fixture)
}

func assertBenchmarkUIAndInvalidRequests(t *testing.T, fixture *benchmarkAPIFixture) {
	t.Helper()
	page := fixture.request(http.MethodGet, "/benchmark/", "", "", "")
	if page.Code != http.StatusOK || !strings.Contains(page.Body.String(), "Benchmark Lab") ||
		!strings.Contains(page.Header().Get("Content-Security-Policy"), "script-src 'self'") ||
		!strings.Contains(page.Body.String(), `data-public-read-only="true"`) {
		t.Fatalf("benchmark page = %d, CSP %q", page.Code, page.Header().Get("Content-Security-Policy"))
	}
	for _, asset := range []string{"/benchmark/app.js", "/benchmark/style.css"} {
		if got := fixture.request(http.MethodGet, asset, "", "", ""); got.Code != http.StatusOK {
			t.Fatalf("asset %s = %d", asset, got.Code)
		}
	}
	for _, invalid := range []struct {
		credential, origin, body string
		status                   int
	}{
		{"", "", `{}`, http.StatusUnauthorized},
		{fixture.diagnostic, "", `{}`, http.StatusUnauthorized},
		{fixture.token, "https://other.example", `{}`, http.StatusForbidden},
		{fixture.token, "", `{"backend":"local","model":"m"}`, http.StatusBadRequest},
		{fixture.token, "", `{"backend":"local","model":"m","allow_interference":true,"max_output_tokens":0}`, http.StatusBadRequest},
		{fixture.token, "", `{"backend":"local","model":"m","allow_interference":true,"repetitions":4}`, http.StatusBadRequest},
		{fixture.token, "", `{"backend":"local","model":"m","allow_interference":true,"allow_interference":false}`, http.StatusBadRequest},
	} {
		response := fixture.request(http.MethodPost, "/benchmark/v1/jobs", invalid.credential, invalid.origin, invalid.body)
		if response.Code != invalid.status {
			t.Fatalf("invalid request status %d, want %d", response.Code, invalid.status)
		}
	}
	if fixture.calls.Load() != 0 {
		t.Fatal("invalid request contacted the proxy")
	}
}

func startBenchmarkJob(t *testing.T, fixture *benchmarkAPIFixture) string {
	t.Helper()
	body := `{"backend":"local","model":"m","allow_interference":true,"time_budget_seconds":15}`
	response := fixture.request(http.MethodPost, "/benchmark/v1/jobs", fixture.token, "https://proxy.test", body)
	var job struct {
		ID string `json:"id"`
	}
	if response.Code != http.StatusAccepted || json.Unmarshal(response.Body.Bytes(), &job) != nil || !strings.HasPrefix(job.ID, "job-") {
		t.Fatalf("start = %d %s", response.Code, response.Body.String())
	}
	select {
	case <-fixture.started:
	case <-time.After(5 * time.Second):
		t.Fatal("fake inference did not start")
	}
	return job.ID
}

func assertBenchmarkJobIsExclusive(t *testing.T, fixture *benchmarkAPIFixture, jobID string) {
	t.Helper()
	body := `{"backend":"local","model":"m","allow_interference":true,"time_budget_seconds":15}`
	if got := fixture.request(http.MethodPost, "/benchmark/v1/jobs", fixture.token, "", body); got.Code != http.StatusConflict {
		t.Fatalf("concurrent job = %d", got.Code)
	}
	if got := fixture.request(http.MethodPost, "/benchmark/v1/jobs/"+jobID+"/cancel", fixture.token, "", ""); got.Code != http.StatusAccepted {
		t.Fatalf("cancel = %d %s", got.Code, got.Body.String())
	}
}

func waitForCanceledBenchmark(t *testing.T, fixture *benchmarkAPIFixture, jobID string) struct {
	State string `json:"state"`
	RunID string `json:"run_id"`
} {
	t.Helper()
	var status struct {
		State string `json:"state"`
		RunID string `json:"run_id"`
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got := fixture.request(http.MethodGet, "/benchmark/v1/jobs/"+jobID, fixture.token, "", "")
		if got.Code != http.StatusOK || json.Unmarshal(got.Body.Bytes(), &status) != nil {
			t.Fatalf("job status = %d %s", got.Code, got.Body.String())
		}
		if status.State == "canceled" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if status.State != "canceled" || status.RunID == "" {
		t.Fatalf("canceled job = %+v", status)
	}
	return status
}

func assertBenchmarkReportAccess(t *testing.T, fixture *benchmarkAPIFixture, runID, jobID string) {
	t.Helper()
	got := fixture.request(http.MethodGet, "/benchmark/v1/runs", fixture.token, "", "")
	if got.Code != http.StatusOK || !strings.Contains(got.Body.String(), runID) {
		t.Fatalf("run listing = %d %s", got.Code, got.Body.String())
	}
	publicListing := fixture.request(http.MethodGet, "/benchmark/v1/runs", "", "", "")
	if publicListing.Code != http.StatusOK || !strings.Contains(publicListing.Body.String(), runID) {
		t.Fatalf("public read-only run listing = %d %s", publicListing.Code, publicListing.Body.String())
	}
	if got := fixture.request(http.MethodGet, "/benchmark/v1/runs", "", "https://other.example", ""); got.Code != http.StatusForbidden {
		t.Fatalf("public read-only cross-origin listing = %d", got.Code)
	}
	if got := fixture.request(http.MethodGet, "/benchmark/v1/runs/"+runID, "", "", ""); got.Code != http.StatusUnauthorized {
		t.Fatalf("public read-only report details = %d", got.Code)
	}
	if got := fixture.request(http.MethodGet, "/benchmark/v1/jobs/"+jobID, "", "", ""); got.Code != http.StatusUnauthorized {
		t.Fatalf("public read-only active-job status = %d", got.Code)
	}
	got = fixture.request(http.MethodGet, "/benchmark/v1/runs/"+runID, fixture.token, "", "")
	if got.Code != http.StatusOK || strings.Contains(got.Body.String(), "TOOL_ROUND_TRIP_OK") {
		t.Fatalf("saved report = %d", got.Code)
	}
	compare := fmt.Sprintf(`{"baseline":%q,"candidate":%q}`, runID, runID)
	got = fixture.request(http.MethodPost, "/benchmark/v1/compare", fixture.token, "", compare)
	if got.Code != http.StatusOK || !strings.Contains(got.Body.String(), `"coverage"`) {
		t.Fatalf("compare = %d %s", got.Code, got.Body.String())
	}
}

func assertBenchmarkConfigRestartRequired(t *testing.T, fixture *benchmarkAPIFixture) {
	t.Helper()
	changed := strings.Replace(fixture.configText, fixture.token, strings.Repeat("n", 32), 1)
	if err := os.WriteFile(fixture.path, []byte(changed), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := fixture.reloader.reload(); err == nil {
		t.Fatal("benchmark token change did not require restart")
	}
}

func TestBenchmarkPublicReadOnlyDefaultsOff(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer server.Close()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	path := configPath(t)
	configText := fmt.Sprintf("default_backend: local\nbackends: {local: {url: %q}}\ndiagnostics_api_key: %q\nbenchmark: {token: %q, proxy_origin: %q, report_directory: %q}\n",
		server.URL, strings.Repeat("d", 32), strings.Repeat("b", 32), server.URL, directory)
	if err := os.WriteFile(path, []byte(configText), 0o600); err != nil {
		t.Fatal(err)
	}
	reloader, _, err := NewReloader(path, discardLogger(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reloader.Stop()
	request := httptest.NewRequest(http.MethodGet, "http://proxy.test/benchmark/v1/runs", nil)
	response := httptest.NewRecorder()
	reloader.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized || calls.Load() != 0 {
		t.Fatalf("default no-auth listing = %d, backend calls=%d", response.Code, calls.Load())
	}
}

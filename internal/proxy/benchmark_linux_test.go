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

func TestBenchmarkAPIConsentAndCancellation(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		calls.Add(1)
		switch req.URL.Path {
		case "/diagnostics/v1/models":
			if req.Header.Get("Authorization") != "Bearer "+strings.Repeat("d", 32) {
				t.Error("wrong diagnostics credential")
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"records": []any{map[string]any{
					"identity":  map[string]string{"backend_id": "local", "public_id": "m", "local_id": "m"},
					"lifecycle": map[string]any{"observed": true, "current_state": "ready", "observed_at": time.Now().UTC()},
				}},
			})
		case "/v1/chat/completions":
			once.Do(func() { close(started) })
			select {
			case <-req.Context().Done():
			case <-release:
			}
		default:
			http.NotFound(w, req)
		}
	}))
	defer server.Close()
	defer close(release)
	token, diagnostic := strings.Repeat("b", 32), strings.Repeat("d", 32)
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	path := configPath(t)
	configText := fmt.Sprintf(`default_backend: local
backends: {local: {url: %q}}
diagnostics_api_key: %q
benchmark: {token: %q, proxy_origin: %q, report_directory: %q}
`, server.URL, diagnostic, token, server.URL, directory)
	if err := os.WriteFile(path, []byte(configText), 0o600); err != nil {
		t.Fatal(err)
	}
	reloader, _, err := NewReloader(path, discardLogger(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reloader.Stop()
	request := func(method, path, credential, origin, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, "http://proxy.test"+path, strings.NewReader(body))
		if credential != "" {
			req.Header.Set("Authorization", "Bearer "+credential)
		}
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		req.Header.Set("Content-Type", "application/json")
		recorder := httptest.NewRecorder()
		reloader.ServeHTTP(recorder, req)
		return recorder
	}
	page := request(http.MethodGet, "/benchmark/", "", "", "")
	if page.Code != http.StatusOK || !strings.Contains(page.Body.String(), "Benchmark Lab") ||
		!strings.Contains(page.Header().Get("Content-Security-Policy"), "script-src 'self'") {
		t.Fatalf("benchmark page = %d, CSP %q", page.Code, page.Header().Get("Content-Security-Policy"))
	}
	for _, asset := range []string{"/benchmark/app.js", "/benchmark/style.css"} {
		if got := request(http.MethodGet, asset, "", "", ""); got.Code != http.StatusOK {
			t.Fatalf("asset %s = %d", asset, got.Code)
		}
	}
	for _, invalid := range []struct {
		credential, origin, body string
		status                   int
	}{
		{"", "", `{}`, http.StatusUnauthorized},
		{diagnostic, "", `{}`, http.StatusUnauthorized},
		{token, "https://other.example", `{}`, http.StatusForbidden},
		{token, "", `{"backend":"local","model":"m"}`, http.StatusBadRequest},
		{token, "", `{"backend":"local","model":"m","allow_interference":true,"max_output_tokens":0}`, http.StatusBadRequest},
		{token, "", `{"backend":"local","model":"m","allow_interference":true,"allow_interference":false}`, http.StatusBadRequest},
	} {
		response := request(http.MethodPost, "/benchmark/v1/jobs", invalid.credential, invalid.origin, invalid.body)
		if response.Code != invalid.status {
			t.Fatalf("invalid request status %d, want %d", response.Code, invalid.status)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid request contacted the proxy")
	}
	body := `{"backend":"local","model":"m","allow_interference":true,"time_budget_seconds":15}`
	response := request(http.MethodPost, "/benchmark/v1/jobs", token, "https://proxy.test", body)
	var job struct {
		ID string `json:"id"`
	}
	if response.Code != http.StatusAccepted || json.Unmarshal(response.Body.Bytes(), &job) != nil || !strings.HasPrefix(job.ID, "job-") {
		t.Fatalf("start = %d %s", response.Code, response.Body.String())
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("fake inference did not start")
	}
	if got := request(http.MethodPost, "/benchmark/v1/jobs", token, "", body); got.Code != http.StatusConflict {
		t.Fatalf("concurrent job = %d", got.Code)
	}
	if got := request(http.MethodPost, "/benchmark/v1/jobs/"+job.ID+"/cancel", token, "", ""); got.Code != http.StatusAccepted {
		t.Fatalf("cancel = %d %s", got.Code, got.Body.String())
	}
	var status struct {
		State string `json:"state"`
		RunID string `json:"run_id"`
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got := request(http.MethodGet, "/benchmark/v1/jobs/current", token, "", "")
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
	got := request(http.MethodGet, "/benchmark/v1/runs", token, "", "")
	if got.Code != http.StatusOK || !strings.Contains(got.Body.String(), status.RunID) {
		t.Fatalf("run listing = %d %s", got.Code, got.Body.String())
	}
	got = request(http.MethodGet, "/benchmark/v1/runs/"+status.RunID, token, "", "")
	if got.Code != http.StatusOK || strings.Contains(got.Body.String(), "TOOL_ROUND_TRIP_OK") {
		t.Fatalf("saved report = %d", got.Code)
	}
	compare := fmt.Sprintf(`{"baseline":%q,"candidate":%q}`, status.RunID, status.RunID)
	got = request(http.MethodPost, "/benchmark/v1/compare", token, "", compare)
	if got.Code != http.StatusOK || !strings.Contains(got.Body.String(), `"coverage"`) {
		t.Fatalf("compare = %d %s", got.Code, got.Body.String())
	}
	changed := strings.Replace(configText, token, strings.Repeat("n", 32), 1)
	if err := os.WriteFile(path, []byte(changed), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := reloader.reload(); err == nil {
		t.Fatal("benchmark token change did not require restart")
	}
}

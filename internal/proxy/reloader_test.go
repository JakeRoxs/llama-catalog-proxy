package proxy

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

func TestReloaderAppliesNewConfigWithoutRestart(t *testing.T) {
	serverA := markerServer("A")
	defer serverA.Close()
	serverB := markerServer("B")
	defer serverB.Close()

	path := configPath(t)
	writeReloaderConfig(t, path, serverA.URL)
	reloader, initial, err := NewReloader(path, discardLogger(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if initial.DefaultBackend != "default" {
		t.Fatalf("DefaultBackend = %q", initial.DefaultBackend)
	}
	reloader.SetWatchInterval(20 * time.Millisecond)
	reloader.Start()
	t.Cleanup(reloader.Stop)

	if got := probeUpstream(t, reloader); got != "A" {
		t.Fatalf("initial upstream = %q, want A", got)
	}

	writeReloaderConfig(t, path, serverB.URL)
	if err := waitForUpstream(t, reloader, "B"); err != nil {
		t.Fatalf("config change was not applied: %v", err)
	}
}

func TestReloaderKeepsPreviousConfigWhenNewConfigIsInvalid(t *testing.T) {
	serverA := markerServer("A")
	defer serverA.Close()

	path := configPath(t)
	writeReloaderConfig(t, path, serverA.URL)
	reloader, _, err := NewReloader(path, discardLogger(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	reloader.SetWatchInterval(20 * time.Millisecond)
	reloader.Start()
	t.Cleanup(reloader.Stop)

	if got := probeUpstream(t, reloader); got != "A" {
		t.Fatalf("initial upstream = %q, want A", got)
	}

	if err := os.WriteFile(path, []byte("unknown_field: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	if got := probeUpstream(t, reloader); got != "A" {
		t.Fatalf("upstream after invalid config = %q, want A (previous config)", got)
	}
}

func TestReloaderInvokesReloadCallbackWithNewConfig(t *testing.T) {
	serverA := markerServer("A")
	defer serverA.Close()
	serverB := markerServer("B")
	defer serverB.Close()

	path := configPath(t)
	writeReloaderConfig(t, path, serverA.URL)
	reloaded := make(chan config.Config, 1)
	reloader, _, err := NewReloader(path, discardLogger(), func(cfg config.Config) {
		reloaded <- cfg
	}, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	reloader.SetWatchInterval(20 * time.Millisecond)
	reloader.Start()
	t.Cleanup(reloader.Stop)

	contents := "default_backend: default\nhealth_timeout: 40ms\ncatalog_timeout: 80ms\nbackends:\n  default:\n    url: " + serverB.URL + "\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := waitForUpstream(t, reloader, "B"); err != nil {
		t.Fatal(err)
	}

	select {
	case cfg := <-reloaded:
		if cfg.Backends["default"].URL != serverB.URL {
			t.Fatalf("callback config URL = %q, want %q", cfg.Backends["default"].URL, serverB.URL)
		}
		if cfg.HealthTimeout != 40*time.Millisecond || cfg.CatalogTimeout != 80*time.Millisecond {
			t.Fatalf("reloaded health/catalog budgets = %s/%s, want 40ms/80ms", cfg.HealthTimeout, cfg.CatalogTimeout)
		}
	case <-time.After(time.Second):
		t.Fatal("reload callback was not invoked")
	}
}

func TestReloaderDetectsSameSizeChangeWithUnchangedModTime(t *testing.T) {
	serverA := markerServer("A")
	defer serverA.Close()
	serverB := markerServer("B")
	defer serverB.Close()
	if len(serverA.URL) != len(serverB.URL) {
		t.Skip("test servers unexpectedly produced different-length URLs")
	}

	path := configPath(t)
	writeReloaderConfig(t, path, serverA.URL)
	initial, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	reloader, _, err := NewReloader(path, discardLogger(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	reloader.SetWatchInterval(20 * time.Millisecond)
	reloader.Start()
	t.Cleanup(reloader.Stop)

	writeReloaderConfig(t, path, serverB.URL)
	if err := os.Chtimes(path, initial.ModTime(), initial.ModTime()); err != nil {
		t.Fatal(err)
	}
	changed, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if changed.Size() != initial.Size() || !changed.ModTime().Equal(initial.ModTime()) {
		t.Fatal("test did not preserve file metadata")
	}
	if err := waitForUpstream(t, reloader, "B"); err != nil {
		t.Fatalf("same-metadata config change was not detected: %v", err)
	}
}

func TestReloaderReloadResetsHistoryStore(t *testing.T) {
	serverA := markerServer("A")
	defer serverA.Close()
	serverB := markerServer("B")
	defer serverB.Close()

	path := configPath(t)
	writeReloaderConfig(t, path, serverA.URL)
	reloader, _, err := NewReloader(path, discardLogger(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	reloader.SetWatchInterval(20 * time.Millisecond)
	reloader.Start()
	t.Cleanup(reloader.Stop)

	if got := probeUpstream(t, reloader); got != "A" {
		t.Fatalf("initial upstream = %q, want A", got)
	}

	// Initial handler should have a nil history store (request_history disabled by default).
	initialHandler := reloader.state.Load().handler.(*Handler)
	if initialHandler.history != nil {
		t.Fatal("initial history store should be nil when request_history is disabled")
	}

	// Change config to enable request_history and point at B.
	contents := "default_backend: default\nrequest_history: true\nbackends:\n  default:\n    url: " + serverB.URL + "\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := waitForUpstream(t, reloader, "B"); err != nil {
		t.Fatalf("config change was not applied: %v", err)
	}

	// Reloaded handler should have a non-nil, fresh history store (new pointer
	// after reload). It may contain records from post-reload probe traffic.
	reloadedHandler := reloader.state.Load().handler.(*Handler)
	if reloadedHandler.history == nil {
		t.Fatal("reloaded history store should be non-nil when request_history is enabled")
	}
	if reloadedHandler.history == initialHandler.history {
		t.Fatal("reloaded history store should be a new store after reload")
	}
}

func TestReloaderReloadReconstructsDiagnosticsCollectors(t *testing.T) {
	serverA := markerServer("A")
	defer serverA.Close()
	serverB := markerServer("B")
	defer serverB.Close()

	path := configPath(t)
	// Start with diagnostics API disabled (no key).
	writeReloaderConfig(t, path, serverA.URL)
	reloader, _, err := NewReloader(path, discardLogger(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	reloader.SetWatchInterval(20 * time.Millisecond)
	reloader.Start()
	t.Cleanup(reloader.Stop)

	if got := probeUpstream(t, reloader); got != "A" {
		t.Fatalf("initial upstream = %q, want A", got)
	}

	// Initial handler should have nil collectors (diagnostics API disabled).
	initialHandler := reloader.state.Load().handler.(*Handler)
	if initialHandler.activityCollector != nil || initialHandler.aliasCollector != nil ||
		initialHandler.gpuCollector != nil || initialHandler.lifecycleCollector != nil {
		t.Fatal("collectors should be nil when diagnostics API is disabled")
	}

	// Enable diagnostics API with a dedicated key and point at B.
	contents := "default_backend: default\ndiagnostics_api_key: test-key\nbackends:\n  default:\n    url: " + serverB.URL + "\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := waitForUpstream(t, reloader, "B"); err != nil {
		t.Fatalf("config change was not applied: %v", err)
	}

	// Reloaded handler should have non-nil, fresh collectors.
	reloadedHandler := reloader.state.Load().handler.(*Handler)
	if reloadedHandler.activityCollector == nil || reloadedHandler.aliasCollector == nil ||
		reloadedHandler.gpuCollector == nil || reloadedHandler.lifecycleCollector == nil {
		t.Fatal("collectors should be non-nil when diagnostics API is enabled")
	}
	if reloadedHandler.activityCollector == initialHandler.activityCollector {
		t.Fatal("activity collector should be a new store after reload")
	}
	if reloadedHandler.aliasCollector == initialHandler.aliasCollector {
		t.Fatal("alias collector should be a new store after reload")
	}

	// Disable diagnostics API again and point back at A.
	contents = "default_backend: default\nbackends:\n  default:\n    url: " + serverA.URL + "\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := waitForUpstream(t, reloader, "A"); err != nil {
		t.Fatalf("config change was not applied: %v", err)
	}

	// Collectors should be nil again after reload.
	disabledHandler := reloader.state.Load().handler.(*Handler)
	if disabledHandler.activityCollector != nil || disabledHandler.aliasCollector != nil ||
		disabledHandler.gpuCollector != nil || disabledHandler.lifecycleCollector != nil {
		t.Fatal("collectors should be nil when diagnostics API is disabled after reload")
	}
}

func markerServer(marker string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("X-Upstream", marker)
		writer.WriteHeader(http.StatusNoContent)
	}))
}

func configPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "config.yaml")
}

func writeReloaderConfig(t *testing.T, path, backendURL string) {
	t.Helper()
	contents := "default_backend: default\nbackends:\n  default:\n    url: " + backendURL + "\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
}

func probeUpstream(t *testing.T, reloader *Reloader) string {
	t.Helper()
	recorder := httptest.NewRecorder()
	reloader.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/probe", nil))
	return recorder.Header().Get("X-Upstream")
}

func waitForUpstream(t *testing.T, reloader *Reloader, want string) error {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if got := probeUpstream(t, reloader); got == want {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for upstream %q", want)
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type synchronizedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *synchronizedBuffer) Write(value []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(value)
}

func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func captureLogger() (*slog.Logger, *synchronizedBuffer) {
	logs := &synchronizedBuffer{}
	return slog.New(slog.NewJSONHandler(logs, nil)), logs
}

func TestReloaderSuppressesIdenticalReloadFailures(t *testing.T) {
	serverA := markerServer("A")
	defer serverA.Close()

	path := configPath(t)
	writeReloaderConfig(t, path, serverA.URL)
	logger, logs := captureLogger()
	reloader, _, err := NewReloader(path, logger, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	reloader.SetWatchInterval(20 * time.Millisecond)
	reloader.Start()
	t.Cleanup(reloader.Stop)

	if got := probeUpstream(t, reloader); got != "A" {
		t.Fatalf("initial upstream = %q, want A", got)
	}

	// Write an invalid config; the first failure is logged.
	if err := os.WriteFile(path, []byte("unknown_field: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Wait for at least one watch tick and a logged failure.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(logs.String(), "config reload failed") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !strings.Contains(logs.String(), "config reload failed") {
		t.Fatal("reload failure was not logged")
	}
	firstCount := strings.Count(logs.String(), "config reload failed")

	// Keep the same invalid config for several ticks; identical failures are suppressed.
	time.Sleep(120 * time.Millisecond)
	secondCount := strings.Count(logs.String(), "config reload failed")
	if secondCount != firstCount {
		t.Fatalf("identical reload failure was logged more than once: %d vs %d", firstCount, secondCount)
	}

	// The previous config is still used.
	if got := probeUpstream(t, reloader); got != "A" {
		t.Fatalf("upstream after suppressed failures = %q, want A", got)
	}
}

func TestReloaderLogsRecoveryAndChangedFailure(t *testing.T) {
	serverA := markerServer("A")
	serverB := markerServer("B")
	defer serverA.Close()
	defer serverB.Close()

	path := configPath(t)
	writeReloaderConfig(t, path, serverA.URL)
	logger, logs := captureLogger()
	reloader, _, err := NewReloader(path, logger, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	reloader.SetWatchInterval(20 * time.Millisecond)
	reloader.Start()
	t.Cleanup(reloader.Stop)

	if got := probeUpstream(t, reloader); got != "A" {
		t.Fatalf("initial upstream = %q, want A", got)
	}

	// First invalid config failure.
	if err := os.WriteFile(path, []byte("unknown_field: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(logs.String(), "config reload failed") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !strings.Contains(logs.String(), "config reload failed") {
		t.Fatal("initial reload failure was not logged")
	}

	// Recovery: valid config pointing at B logs success and applies.
	writeReloaderConfig(t, path, serverB.URL)
	if err := waitForUpstream(t, reloader, "B"); err != nil {
		t.Fatalf("config change was not applied: %v", err)
	}
	if !strings.Contains(logs.String(), "configuration reloaded") {
		t.Fatal("reload recovery was not logged")
	}

	// A different invalid config failure is logged promptly.
	if err := os.WriteFile(path, []byte("another_unknown_field: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(logs.String(), "config reload failed") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !strings.Contains(logs.String(), "config reload failed") {
		t.Fatal("changed reload failure was not logged")
	}
}

// recordingHandler records whether the current state handler was invoked so
// tests can distinguish route dispatch from falling through to the handler.
type recordingHandler struct {
	mu     sync.Mutex
	called bool
	path   string
}

func (h *recordingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.called = true
	h.path = r.URL.Path
	w.WriteHeader(http.StatusOK)
}

func newTestReloader(benchmark *benchmarkRuntime, adminAuth *adminAuth, peerListen, managementListen string, handler http.Handler) *Reloader {
	r := &Reloader{
		benchmark:        benchmark,
		adminAuth:        adminAuth,
		peerListen:       peerListen,
		managementListen: managementListen,
	}
	r.state.Store(&reloaderState{handler: handler})
	return r
}

func TestServeHTTPRouteDispatch(t *testing.T) {
	for _, tc := range routeDispatchCases() {
		t.Run(tc.name, func(t *testing.T) {
			assertRouteDispatchCase(t, tc)
		})
	}
}

type routeDispatchCase struct {
	name             string
	path             string
	benchmark        *benchmarkRuntime
	auth             *adminAuth
	peerListen       string
	managementListen string
	wantStatus       int
	wantHandlerCall  bool
}

func routeDispatchCases() []routeDispatchCase {
	return []routeDispatchCase{
		{name: "benchmark route handled before fallback", path: "/benchmark", benchmark: &benchmarkRuntime{}, wantStatus: http.StatusTemporaryRedirect},
		{name: "benchmark nil falls through to current handler", path: "/benchmark", wantStatus: http.StatusOK, wantHandlerCall: true},
		{name: "admin prefix with nil auth is 404", path: "/admin", wantStatus: http.StatusNotFound},
		{name: "admin unmatched path falls through to adminAuth", path: "/admin/api/v1/session", auth: &adminAuth{}, wantStatus: http.StatusUnauthorized},
		{name: "reserved peer prefix 404 when enabled", path: "/peer", peerListen: "127.0.0.1:1234", wantStatus: http.StatusNotFound},
		{name: "reserved management prefix 404 when enabled", path: "/management", managementListen: "127.0.0.1:5678", wantStatus: http.StatusNotFound},
		{name: "reserved prefix disabled falls through to current handler", path: "/peer", wantStatus: http.StatusOK, wantHandlerCall: true},
		{name: "unmatched path falls through to current handler", path: "/v1/models", wantStatus: http.StatusOK, wantHandlerCall: true},
	}
}

func assertRouteDispatchCase(t *testing.T, tc routeDispatchCase) {
	t.Helper()
	handler := &recordingHandler{}
	reloader := newTestReloader(tc.benchmark, tc.auth, tc.peerListen, tc.managementListen, handler)
	response := httptest.NewRecorder()
	reloader.ServeHTTP(response, httptest.NewRequest(http.MethodGet, tc.path, nil))
	if response.Code != tc.wantStatus {
		t.Fatalf("status = %d, want %d", response.Code, tc.wantStatus)
	}
	if handler.called != tc.wantHandlerCall {
		t.Fatalf("current state handler called = %t, want %t", handler.called, tc.wantHandlerCall)
	}
}

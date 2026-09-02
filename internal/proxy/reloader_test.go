package proxy

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
	reloader, initial, err := NewReloader(path, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if initial.DefaultBackend != "default" {
		t.Fatalf("DefaultBackend = %q", initial.DefaultBackend)
	}
	reloader.SetWatchInterval(20 * time.Millisecond)
	reloader.Start()

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
	reloader, _, err := NewReloader(path, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	reloader.SetWatchInterval(20 * time.Millisecond)
	reloader.Start()

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
	})
	if err != nil {
		t.Fatal(err)
	}
	reloader.SetWatchInterval(20 * time.Millisecond)
	reloader.Start()

	writeReloaderConfig(t, path, serverB.URL)
	if err := waitForUpstream(t, reloader, "B"); err != nil {
		t.Fatal(err)
	}

	select {
	case cfg := <-reloaded:
		if cfg.Backends["default"].URL != serverB.URL {
			t.Fatalf("callback config URL = %q, want %q", cfg.Backends["default"].URL, serverB.URL)
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
	reloader, _, err := NewReloader(path, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	reloader.SetWatchInterval(20 * time.Millisecond)
	reloader.Start()

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

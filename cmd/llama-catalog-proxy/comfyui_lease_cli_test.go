package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
)

func TestComfyUILeaseReleaseCLIRequiresAcknowledgementAndUsesOwnerConfig(t *testing.T) {
	var queueCalls, freeCalls, statsCalls atomic.Int32
	server := newComfyUILeaseTestServer(t, &queueCalls, &freeCalls, &statsCalls)
	defer server.Close()

	configPath, _ := peerCLIConfig(t)
	pristine, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	writeComfyUIPeerConfig(t, configPath, pristine, server.URL, "")

	// Unacknowledged release is rejected and makes no backend calls.
	assertReleaseRequiresAcknowledgement(t, configPath)
	assertNoComfyUICalls(t, "unacknowledged", &queueCalls, &freeCalls, &statsCalls)

	// Proxy admission coordination disables standalone release.
	writeComfyUIPeerConfig(t, configPath, pristine, server.URL, "local")
	assertReleaseDisabledByAdmission(t, configPath)
	assertNoComfyUICalls(t, "disabled standalone", &queueCalls, &freeCalls, &statsCalls)

	// Acknowledged release succeeds and does not leak owner URL or secret.
	writeComfyUIPeerConfig(t, configPath, pristine, server.URL, "")
	output := assertSuccessfulRelease(t, configPath, &queueCalls, &freeCalls, &statsCalls)
	if strings.Contains(output, server.URL) || strings.Contains(output, "owner-secret") {
		t.Fatalf("release output leaked owner endpoint or credential: %s", output)
	}
}

// newComfyUILeaseTestServer returns an httptest server that records /queue, /free and
// /system_stats calls and requires the owner credential on every request.
func newComfyUILeaseTestServer(t *testing.T, queueCalls, freeCalls, statsCalls *atomic.Int32) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Authorization") != "Bearer owner-secret" {
			t.Errorf("owner credential missing from %s: %v", req.URL.Path, req.Header)
		}
		switch {
		case req.Method == http.MethodGet && req.URL.Path == "/queue":
			queueCalls.Add(1)
			_, _ = w.Write([]byte(`{"queue_running":[],"queue_pending":[]}`))
		case req.Method == http.MethodPost && req.URL.Path == "/free":
			freeCalls.Add(1)
			w.WriteHeader(http.StatusOK)
		case req.Method == http.MethodGet && req.URL.Path == "/system_stats":
			statsCalls.Add(1)
			_, _ = w.Write([]byte(`{"devices":[{"name":"GPU 0","type":"cuda","index":0,"vram_total":1000,"vram_free":900}]}`))
		default:
			t.Errorf("unexpected lease request %s %s", req.Method, req.URL.Path)
			http.NotFound(w, req)
		}
	}))
}

// writeComfyUIPeerConfig writes the comfyui peer section into the config at configPath,
// derived from the pristine peer config. It always operates on the pristine contents so
// repeated calls (e.g. toggling admission) do not duplicate the inserted block.
func writeComfyUIPeerConfig(t *testing.T, configPath string, pristine []byte, serverURL, admissionBackends string) {
	t.Helper()
	admission := ""
	if admissionBackends != "" {
		admission = "    admission_backends: [" + admissionBackends + "]\n"
	}
	insert := `path_routes:
  /comfyui: local
`
	updated := strings.Replace(string(pristine), "peer:\n", insert+`peer:
  comfyui_result_read:
    url: "`+serverURL+`"
    api_key: owner-secret
  comfyui_lease:
    release_timeout: 5s
    poll_interval: 100ms
    queue_idle_samples: 2
    minimum_free_vram_bytes:
      "cuda:0": 800
`+admission, 1)
	if updated == string(pristine) {
		t.Fatal("test config peer section was not replaced")
	}
	if err := os.WriteFile(configPath, []byte(updated), 0o600); err != nil {
		t.Fatal(err)
	}
}

// assertNoComfyUICalls fails the test if the ComfyUI backend was called during the
// phase described by label.
func assertNoComfyUICalls(t *testing.T, label string, queueCalls, freeCalls, statsCalls *atomic.Int32) {
	t.Helper()
	if queueCalls.Load() != 0 || freeCalls.Load() != 0 || statsCalls.Load() != 0 {
		t.Fatalf("%s release called ComfyUI: queue=%d free=%d stats=%d", label, queueCalls.Load(), freeCalls.Load(), statsCalls.Load())
	}
}

// assertReleaseRequiresAcknowledgement runs a release without --acknowledge and
// fails the test unless the command is rejected for the missing acknowledgement.
func assertReleaseRequiresAcknowledgement(t *testing.T, configPath string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	if err := runComfyUILeaseCommand([]string{"release", "--config", configPath}, &stdout, &stderr); err == nil ||
		!strings.Contains(err.Error(), "--acknowledge") {
		t.Fatalf("release without acknowledgement = %v", err)
	}
}

// assertReleaseDisabledByAdmission runs an acknowledged release with proxy
// admission enabled and fails the test unless the command is rejected.
func assertReleaseDisabledByAdmission(t *testing.T, configPath string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	if err := runComfyUILeaseCommand([]string{"release", "--config", configPath, "--acknowledge"}, &stdout, &stderr); err == nil ||
		!strings.Contains(err.Error(), "standalone release is disabled") {
		t.Fatalf("standalone release during proxy admission = %v", err)
	}
}

// assertSuccessfulRelease runs an acknowledged release, validates the result shape
// and backend call counters, and returns the command output.
func assertSuccessfulRelease(t *testing.T, configPath string, queueCalls, freeCalls, statsCalls *atomic.Int32) string {
	t.Helper()
	var stdout, stderr bytes.Buffer
	if err := runComfyUILeaseCommand([]string{"release", "--config", configPath, "--acknowledge"}, &stdout, &stderr); err != nil {
		t.Fatalf("acknowledged release command: %v stderr=%s", err, stderr.String())
	}
	var result struct {
		State          string `json:"state"`
		FreeRequested  bool   `json:"free_requested"`
		FreeHTTPStatus int    `json:"free_http_status"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("release output %q: %v", stdout.String(), err)
	}
	if result.State != "confirmed" || !result.FreeRequested || result.FreeHTTPStatus != http.StatusOK ||
		freeCalls.Load() != 1 || statsCalls.Load() < 2 || queueCalls.Load() < 4 {
		t.Fatalf("release result=%+v queue=%d free=%d stats=%d", result, queueCalls.Load(), freeCalls.Load(), statsCalls.Load())
	}
	return stdout.String()
}

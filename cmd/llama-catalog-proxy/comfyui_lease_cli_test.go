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
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
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
	defer server.Close()

	configPath, _ := peerCLIConfig(t)
	contents, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	insert := `path_routes:
  /comfyui: local
`
	updated := strings.Replace(string(contents), "peer:\n", insert+`peer:
  comfyui_result_read:
    url: "`+server.URL+`"
    api_key: owner-secret
  comfyui_lease:
    release_timeout: 5s
    poll_interval: 100ms
    queue_idle_samples: 2
    minimum_free_vram_bytes:
      "cuda:0": 800
`, 1)
	if updated == string(contents) {
		t.Fatal("test config peer section was not replaced")
	}
	if err := os.WriteFile(configPath, []byte(updated), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if err := runComfyUILeaseCommand([]string{"release", "--config", configPath}, &stdout, &stderr); err == nil ||
		!strings.Contains(err.Error(), "--acknowledge") {
		t.Fatalf("release without acknowledgement = %v", err)
	}
	if queueCalls.Load() != 0 || freeCalls.Load() != 0 || statsCalls.Load() != 0 {
		t.Fatalf("unacknowledged release called ComfyUI: queue=%d free=%d stats=%d", queueCalls.Load(), freeCalls.Load(), statsCalls.Load())
	}
	admissionEnabled := strings.Replace(updated, "    minimum_free_vram_bytes:", "    admission_backends: [local]\n    minimum_free_vram_bytes:", 1)
	if err := os.WriteFile(configPath, []byte(admissionEnabled), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runComfyUILeaseCommand([]string{"release", "--config", configPath, "--acknowledge"}, &stdout, &stderr); err == nil ||
		!strings.Contains(err.Error(), "standalone release is disabled") {
		t.Fatalf("standalone release during proxy admission = %v", err)
	}
	if queueCalls.Load() != 0 || freeCalls.Load() != 0 || statsCalls.Load() != 0 {
		t.Fatalf("disabled standalone release called ComfyUI: queue=%d free=%d stats=%d", queueCalls.Load(), freeCalls.Load(), statsCalls.Load())
	}
	if err := os.WriteFile(configPath, []byte(updated), 0o600); err != nil {
		t.Fatal(err)
	}
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
	if strings.Contains(stdout.String(), server.URL) || strings.Contains(stdout.String(), "owner-secret") {
		t.Fatalf("release output leaked owner endpoint or credential: %s", stdout.String())
	}
}

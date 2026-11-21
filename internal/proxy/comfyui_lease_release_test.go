package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

func TestVerifyComfyUIReleaseRequiresStableQueueFreeAndPerDeviceVRAM(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	freeCalled := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		paths = append(paths, req.Method+" "+req.URL.Path)
		mu.Unlock()
		if req.Header.Get("Authorization") != "Bearer owner-secret" {
			t.Errorf("owner API credential missing: %v", req.Header)
		}
		switch {
		case req.Method == http.MethodGet && req.URL.Path == "/queue":
			_, _ = io.WriteString(w, `{"queue_running":[],"queue_pending":[]}`)
		case req.Method == http.MethodPost && req.URL.Path == "/free":
			var payload map[string]bool
			if err := json.NewDecoder(req.Body).Decode(&payload); err != nil || !payload["unload_models"] || !payload["free_memory"] {
				t.Errorf("/free payload = %v, %v", payload, err)
			}
			freeCalled = true
			w.WriteHeader(http.StatusOK)
		case req.Method == http.MethodGet && req.URL.Path == "/system_stats":
			if !freeCalled {
				t.Error("system_stats was checked before /free")
			}
			_, _ = io.WriteString(w, `{"devices":[{"name":"GPU 0","type":"cuda","index":0,"vram_total":1000,"vram_free":900}]}`)
		default:
			t.Errorf("unexpected ComfyUI lease request %s %s", req.Method, req.URL.Path)
			http.NotFound(w, req)
		}
	}))
	defer server.Close()
	client := &http.Client{Timeout: time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	result, err := VerifyComfyUIRelease(context.Background(), server.URL, http.Header{"Authorization": []string{"Bearer owner-secret"}}, &config.ComfyUILeaseConfig{
		ReleaseTimeout: "5s", PollInterval: "100ms", QueueIdleSamples: 2,
		MinimumFreeVRAMBytes: map[string]int64{"cuda:0": 800},
	}, client)
	if err != nil || result.State != "confirmed" || !result.FreeRequested || result.QueueIdleSamples != 2 ||
		len(result.Devices) != 1 || result.Devices[0].FreeVRAMBytes != 900 || result.Devices[0].MinimumFreeVRAMBytes != 800 {
		t.Fatalf("release result = %+v, error %v", result, err)
	}
	mu.Lock()
	defer mu.Unlock()
	freeIndex, statsIndex := -1, -1
	for index, path := range paths {
		if path == "POST /free" && freeIndex < 0 {
			freeIndex = index
		}
		if path == "GET /system_stats" && statsIndex < 0 {
			statsIndex = index
		}
	}
	if freeIndex < 2 || statsIndex <= freeIndex {
		t.Fatalf("release endpoint order = %v", paths)
	}
}

func TestVerifyComfyUIReleaseIsUnknownWhenQueueBusyOrEvidenceMissing(t *testing.T) {
	t.Run("busy queue prevents free", func(t *testing.T) {
		var freeCalls int
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if req.Method == http.MethodGet && req.URL.Path == "/queue" {
				_, _ = io.WriteString(w, `{"queue_running":[["job"]],"queue_pending":[]}`)
				return
			}
			if req.Method == http.MethodPost && req.URL.Path == "/free" {
				freeCalls++
			}
			http.NotFound(w, req)
		}))
		defer server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		result, err := VerifyComfyUIRelease(ctx, server.URL, nil, &config.ComfyUILeaseConfig{
			ReleaseTimeout: "5s", PollInterval: "100ms", QueueIdleSamples: 2,
			MinimumFreeVRAMBytes: map[string]int64{"cuda:0": 800},
		}, &http.Client{Timeout: time.Second})
		if !errors.Is(err, ErrComfyUIReleaseUnknown) || result.State != "unknown" || result.FreeRequested || freeCalls != 0 {
			t.Fatalf("busy queue release result = %+v, free calls %d, err %v", result, freeCalls, err)
		}
	})

	t.Run("free success without device evidence remains unknown", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			switch req.URL.Path {
			case "/queue":
				_, _ = io.WriteString(w, `{"queue_running":[],"queue_pending":[]}`)
			case "/free":
				w.WriteHeader(http.StatusOK)
			case "/system_stats":
				_, _ = io.WriteString(w, `{"devices":[]}`)
			default:
				http.NotFound(w, req)
			}
		}))
		defer server.Close()
		result, err := VerifyComfyUIRelease(context.Background(), server.URL, nil, &config.ComfyUILeaseConfig{
			ReleaseTimeout: "5s", PollInterval: "100ms", QueueIdleSamples: 2,
			MinimumFreeVRAMBytes: map[string]int64{"cuda:0": 800},
		}, &http.Client{Timeout: time.Second})
		if !errors.Is(err, ErrComfyUIReleaseUnknown) || result.State != "unknown" || !result.FreeRequested ||
			!strings.Contains(err.Error(), "unknown") {
			t.Fatalf("missing GPU evidence release result = %+v, err %v", result, err)
		}
	})

	t.Run("queue activity after free prevents confirmation", func(t *testing.T) {
		var queueCalls, freeCalls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			switch req.URL.Path {
			case "/queue":
				if queueCalls.Add(1) <= 2 {
					_, _ = io.WriteString(w, `{"queue_running":[],"queue_pending":[]}`)
				} else {
					_, _ = io.WriteString(w, `{"queue_running":[["new-job"]],"queue_pending":[]}`)
				}
			case "/free":
				freeCalls.Add(1)
				w.WriteHeader(http.StatusOK)
			case "/system_stats":
				_, _ = io.WriteString(w, `{"devices":[{"name":"GPU 0","type":"cuda","index":0,"vram_total":1000,"vram_free":900}]}`)
			default:
				http.NotFound(w, req)
			}
		}))
		defer server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
		defer cancel()
		result, err := VerifyComfyUIRelease(ctx, server.URL, nil, &config.ComfyUILeaseConfig{
			ReleaseTimeout: "5s", PollInterval: "100ms", QueueIdleSamples: 2,
			MinimumFreeVRAMBytes: map[string]int64{"cuda:0": 800},
		}, &http.Client{Timeout: time.Second})
		if !errors.Is(err, ErrComfyUIReleaseUnknown) || result.State != "unknown" || !result.FreeRequested || freeCalls.Load() != 1 {
			t.Fatalf("queue race release result = %+v, free calls %d, err %v", result, freeCalls.Load(), err)
		}
	})

	t.Run("free server failure has unknown outcome", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			switch req.URL.Path {
			case "/queue":
				_, _ = io.WriteString(w, `{"queue_running":[],"queue_pending":[]}`)
			case "/free":
				w.WriteHeader(http.StatusInternalServerError)
			}
		}))
		defer server.Close()
		result, err := VerifyComfyUIRelease(context.Background(), server.URL, nil, &config.ComfyUILeaseConfig{
			ReleaseTimeout: "5s", PollInterval: "100ms", QueueIdleSamples: 2,
			MinimumFreeVRAMBytes: map[string]int64{"cuda:0": 800},
		}, &http.Client{Timeout: time.Second})
		if !errors.Is(err, ErrComfyUIReleaseUnknown) || result.State != "unknown" || !result.FreeRequested || result.FreeHTTPStatus != http.StatusInternalServerError {
			t.Fatalf("server-failed release result = %+v, err %v", result, err)
		}
	})

	t.Run("http 200 does not prove memory release", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			switch req.URL.Path {
			case "/queue":
				_, _ = io.WriteString(w, `{"queue_running":[],"queue_pending":[]}`)
			case "/free":
				w.WriteHeader(http.StatusOK)
			case "/system_stats":
				_, _ = io.WriteString(w, `{"devices":[{"name":"GPU 0","type":"cuda","index":0,"vram_total":1000,"vram_free":500}]}`)
			default:
				http.NotFound(w, req)
			}
		}))
		defer server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		result, err := VerifyComfyUIRelease(ctx, server.URL, nil, &config.ComfyUILeaseConfig{
			ReleaseTimeout: "5s", PollInterval: "100ms", QueueIdleSamples: 2,
			MinimumFreeVRAMBytes: map[string]int64{"cuda:0": 800},
		}, &http.Client{Timeout: time.Second})
		if !errors.Is(err, ErrComfyUIReleaseUnknown) || result.State != "unknown" || !result.FreeRequested {
			t.Fatalf("insufficient VRAM release result = %+v, err %v", result, err)
		}
	})
}

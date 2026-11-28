package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

func TestVerifyComfyUIReleaseDistinguishesStoppedServiceBeforeFree(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	url := server.URL
	server.Close()

	result, err := VerifyComfyUIRelease(context.Background(), url, nil, &config.ComfyUILeaseConfig{
		ReleaseTimeout: "5s", PollInterval: "100ms", QueueIdleSamples: 2,
		MinimumFreeVRAMBytes: map[string]int64{"cuda:0": 800},
	}, &http.Client{Timeout: time.Second})
	if !errors.Is(err, errComfyUIServiceUnavailable) || result.FreeRequested {
		t.Fatalf("stopped service result=%+v err=%v", result, err)
	}

	client := &http.Client{Transport: comfyUIReleaseRoundTripper(func(*http.Request) (*http.Response, error) {
		return nil, &net.DNSError{Err: "no such host", Name: "comfyui", IsNotFound: true}
	})}
	result, err = VerifyComfyUIRelease(context.Background(), "http://comfyui:8188", nil, &config.ComfyUILeaseConfig{
		ReleaseTimeout: "5s", PollInterval: "100ms", QueueIdleSamples: 2,
		MinimumFreeVRAMBytes: map[string]int64{"cuda:0": 800},
	}, client)
	if !errors.Is(err, errComfyUIServiceUnavailable) || result.FreeRequested {
		t.Fatalf("unregistered stopped service result=%+v err=%v", result, err)
	}
}

type comfyUIReleaseRoundTripper func(*http.Request) (*http.Response, error)

func (roundTrip comfyUIReleaseRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}

func TestVerifyComfyUIReleaseRequiresStableQueueFreeAndPerDeviceVRAM(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	freeCalled := false
	server := newStableQueueFreeVRAMTestServer(t, &mu, &paths, &freeCalled)
	defer server.Close()
	client := &http.Client{Timeout: time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	result, err := VerifyComfyUIRelease(context.Background(), server.URL, http.Header{"Authorization": []string{"Bearer owner-secret"}}, comfyUIReleaseLeaseConfig(), client)
	assertConfirmedComfyUIRelease(t, result, err)
	assertReleaseRequestOrdering(t, &mu, paths)
}

// newStableQueueFreeVRAMTestServer builds the ComfyUI test server for the stable-queue
// and per-device-VRAM verification scenario. It records every request path, requires
// the owner bearer credential, answers an idle queue, records /free, and reports a
// single GPU device with 900 MiB free against an 800 MiB minimum.
func newStableQueueFreeVRAMTestServer(t *testing.T, mu *sync.Mutex, paths *[]string, freeCalled *bool) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		*paths = append(*paths, req.Method+" "+req.URL.Path)
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
			*freeCalled = true
			w.WriteHeader(http.StatusOK)
		case req.Method == http.MethodGet && req.URL.Path == "/system_stats":
			if !*freeCalled {
				t.Error("system_stats was checked before /free")
			}
			_, _ = io.WriteString(w, `{"devices":[{"name":"GPU 0","type":"cuda","index":0,"vram_total":1000,"vram_free":900}]}`)
		default:
			t.Errorf("unexpected ComfyUI lease request %s %s", req.Method, req.URL.Path)
			http.NotFound(w, req)
		}
	}))
}

// comfyUIReleaseLeaseConfig returns the standard lease configuration used by the
// ComfyUI release verification tests.
func comfyUIReleaseLeaseConfig() *config.ComfyUILeaseConfig {
	return &config.ComfyUILeaseConfig{
		ReleaseTimeout:       "5s",
		PollInterval:         "100ms",
		QueueIdleSamples:     2,
		MinimumFreeVRAMBytes: map[string]int64{"cuda:0": 800},
	}
}

// assertConfirmedComfyUIRelease verifies the release completed with the expected
// confirmed state, free request, two stable idle samples, and one GPU device at the
// configured minimum free VRAM.
func assertConfirmedComfyUIRelease(t *testing.T, result ComfyUIReleaseResult, err error) {
	if err != nil || result.State != "confirmed" || !result.FreeRequested || result.QueueIdleSamples != 2 ||
		len(result.Devices) != 1 || result.Devices[0].FreeVRAMBytes != 900 || result.Devices[0].MinimumFreeVRAMBytes != 800 {
		t.Fatalf("release result = %+v, error %v", result, err)
	}
}

// assertReleaseRequestOrdering verifies /queue is sampled at least twice before
// /free and that /system_stats is observed only after /free.
func assertReleaseRequestOrdering(t *testing.T, mu *sync.Mutex, paths []string) {
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

// newComfyUIReleaseTestServer builds a test ComfyUI server that routes /queue, /free,
// and /system_stats to the provided handlers and returns 404 for everything else.
func newComfyUIReleaseTestServer(queue, free, stats http.HandlerFunc) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/queue":
			queue(w, req)
		case "/free":
			free(w, req)
		case "/system_stats":
			stats(w, req)
		default:
			http.NotFound(w, req)
		}
	}))
}

// requestComfyUIRelease issues a VerifyComfyUIRelease request against the test server
// using the standard lease configuration and a one-second HTTP client timeout.
func requestComfyUIRelease(ctx context.Context, server *httptest.Server) (ComfyUIReleaseResult, error) {
	return VerifyComfyUIRelease(ctx, server.URL, nil, comfyUIReleaseLeaseConfig(), &http.Client{Timeout: time.Second})
}

func TestVerifyComfyUIReleaseIsUnknownWhenQueueBusyOrEvidenceMissing(t *testing.T) {
	t.Run("busy queue prevents free", testComfyUIReleaseBusyQueuePreventsFree)
	t.Run("free success without device evidence remains unknown", testComfyUIReleaseFreeWithoutDeviceEvidence)
	t.Run("queue activity after free prevents confirmation", testComfyUIReleaseQueueActivityAfterFree)
	t.Run("free server failure has unknown outcome", testComfyUIReleaseFreeServerFailure)
	t.Run("http 200 does not prove memory release", testComfyUIReleaseHTTP200WithoutMemoryEvidence)
}

// testComfyUIReleaseBusyQueuePreventsFree verifies a busy queue blocks the /free
// request and leaves the release unknown with no free call.
func testComfyUIReleaseBusyQueuePreventsFree(t *testing.T) {
	var freeCalls int
	server := newComfyUIReleaseTestServer(
		http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if req.Method == http.MethodGet && req.URL.Path == "/queue" {
				_, _ = io.WriteString(w, `{"queue_running":[["job"]],"queue_pending":[]}`)
				return
			}
			http.NotFound(w, req)
		}),
		http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if req.Method == http.MethodPost && req.URL.Path == "/free" {
				freeCalls++
			}
		}),
		http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			http.NotFound(w, req)
		}),
	)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	result, err := requestComfyUIRelease(ctx, server)
	if !errors.Is(err, ErrComfyUIReleaseUnknown) || result.State != "unknown" || result.FreeRequested || freeCalls != 0 {
		t.Fatalf("busy queue release result = %+v, free calls %d, err %v", result, freeCalls, err)
	}
}

// testComfyUIReleaseFreeWithoutDeviceEvidence verifies a successful /free with no GPU
// device evidence leaves the release unknown.
func testComfyUIReleaseFreeWithoutDeviceEvidence(t *testing.T) {
	server := newComfyUIReleaseTestServer(
		http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			_, _ = io.WriteString(w, `{"queue_running":[],"queue_pending":[]}`)
		}),
		http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
		http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			_, _ = io.WriteString(w, `{"devices":[]}`)
		}),
	)
	defer server.Close()
	result, err := requestComfyUIRelease(context.Background(), server)
	if !errors.Is(err, ErrComfyUIReleaseUnknown) || result.State != "unknown" || !result.FreeRequested ||
		!strings.Contains(err.Error(), "unknown") {
		t.Fatalf("missing GPU evidence release result = %+v, err %v", result, err)
	}
}

// testComfyUIReleaseQueueActivityAfterFree verifies queue activity observed after /free
// prevents confirmation while still recording exactly one free call.
func testComfyUIReleaseQueueActivityAfterFree(t *testing.T) {
	var queueCalls, freeCalls atomic.Int32
	server := newComfyUIReleaseTestServer(
		http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if req.Method == http.MethodGet && req.URL.Path == "/queue" {
				if queueCalls.Add(1) <= 2 {
					_, _ = io.WriteString(w, `{"queue_running":[],"queue_pending":[]}`)
				} else {
					_, _ = io.WriteString(w, `{"queue_running":[["new-job"]],"queue_pending":[]}`)
				}
				return
			}
			http.NotFound(w, req)
		}),
		http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if req.Method == http.MethodPost && req.URL.Path == "/free" {
				freeCalls.Add(1)
				w.WriteHeader(http.StatusOK)
			}
		}),
		http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if req.Method == http.MethodGet && req.URL.Path == "/system_stats" {
				_, _ = io.WriteString(w, `{"devices":[{"name":"GPU 0","type":"cuda","index":0,"vram_total":1000,"vram_free":900}]}`)
			}
		}),
	)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
	defer cancel()
	result, err := requestComfyUIRelease(ctx, server)
	if !errors.Is(err, ErrComfyUIReleaseUnknown) || result.State != "unknown" || !result.FreeRequested || freeCalls.Load() != 1 {
		t.Fatalf("queue race release result = %+v, free calls %d, err %v", result, freeCalls.Load(), err)
	}
}

// testComfyUIReleaseFreeServerFailure verifies a /free HTTP 500 leaves the release
// unknown with the failure status recorded.
func testComfyUIReleaseFreeServerFailure(t *testing.T) {
	server := newComfyUIReleaseTestServer(
		http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			_, _ = io.WriteString(w, `{"queue_running":[],"queue_pending":[]}`)
		}),
		http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}),
		http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			http.NotFound(w, req)
		}),
	)
	defer server.Close()
	result, err := requestComfyUIRelease(context.Background(), server)
	if !errors.Is(err, ErrComfyUIReleaseUnknown) || result.State != "unknown" || !result.FreeRequested || result.FreeHTTPStatus != http.StatusInternalServerError {
		t.Fatalf("server-failed release result = %+v, err %v", result, err)
	}
}

// testComfyUIReleaseHTTP200WithoutMemoryEvidence verifies an HTTP 200 /free with
// insufficient free VRAM leaves the release unknown.
func testComfyUIReleaseHTTP200WithoutMemoryEvidence(t *testing.T) {
	server := newComfyUIReleaseTestServer(
		http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			_, _ = io.WriteString(w, `{"queue_running":[],"queue_pending":[]}`)
		}),
		http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
		http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			_, _ = io.WriteString(w, `{"devices":[{"name":"GPU 0","type":"cuda","index":0,"vram_total":1000,"vram_free":500}]}`)
		}),
	)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	result, err := requestComfyUIRelease(ctx, server)
	if !errors.Is(err, ErrComfyUIReleaseUnknown) || result.State != "unknown" || !result.FreeRequested {
		t.Fatalf("insufficient VRAM release result = %+v, err %v", result, err)
	}
}

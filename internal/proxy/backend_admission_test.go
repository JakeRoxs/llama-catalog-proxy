package proxy

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

func TestBackendAdmissionDenialLogOmitsRequestPath(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status func() comfyUICoordinatorStatus
	}{
		{name: "without coordinator status"},
		{name: "with coordinator status", status: func() comfyUICoordinatorStatus {
			return comfyUICoordinatorStatus{State: "active", LeaseID: "private-lease-id"}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const path = "/private-request-path"
			var logs bytes.Buffer
			h := &Handler{
				logger:                 slog.New(slog.NewTextHandler(&logs, nil)),
				backendAdmission:       func(*http.Request, string) (func(), bool) { return nil, false },
				backendAdmissionStatus: tc.status,
			}
			requestID := generateRequestID()
			req := httptest.NewRequestWithContext(context.WithValue(t.Context(), requestIDKey{}, requestID), http.MethodPost, path, nil)
			w := httptest.NewRecorder()
			h.dispatchBackend(w, req, "test-backend")

			if w.Code != http.StatusServiceUnavailable {
				t.Fatalf("denial status = %d, want %d", w.Code, http.StatusServiceUnavailable)
			}
			logged := logs.String()
			if strings.Contains(logged, " path=") || strings.Contains(logged, path) || strings.Contains(logged, "private-lease-id") {
				t.Fatalf("denial log includes a request path or lease ID: %s", logged)
			}
			for _, want := range []string{"backend admission denied", "backend_id=test-backend", "request_id=" + requestID} {
				if !strings.Contains(logged, want) {
					t.Errorf("denial log missing %q: %s", want, logged)
				}
			}
			if tc.status != nil && !strings.Contains(logged, "lease_id_present=true") {
				t.Errorf("denial log missing lease presence: %s", logged)
			}
		})
	}
}

func TestOwnerBackendDispatchSharesAdmissionBoundary(t *testing.T) {
	var forwarded atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/models" {
			_, _ = io.WriteString(w, `{"data":[{"id":"example"}]}`)
			return
		}
		forwarded.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer backend.Close()
	cfg := baseConfig(backend.URL)
	cfg.PathRoutes = map[string]string{"/comfyui": "default"}
	h := newHandler(t, cfg).(*Handler)
	h.catalog.RefreshBackendModels(t.Context(), []string{"default"}, time.Minute)
	h.modelRoutes = []route{{prefix: "alias/", backendID: "default"}}
	owner := &Reloader{logger: h.logger, instanceID: strings.Repeat("a", 32),
		peerPathSlots: make(chan struct{}, 1), peerInferenceSlots: make(chan struct{}, 1)}
	token := strings.Repeat("b", 32)
	owner.state.Store(&reloaderState{handler: h, peerContext: context.Background(), cacheTTL: time.Minute, generation: 1,
		peer: &config.PeerConfig{NodeID: "owner", LocalBackends: []string{"default"},
			ExposedPathRoutes: map[string]string{"comfyui": "/comfyui"},
			Peers:             map[string]config.PeerGrant{"caller": {Token: token, Inference: true, PathAccess: []string{"comfyui"}}}}})

	for _, test := range []struct {
		name, method, path, body, target string
	}{
		{"default GET", "GET", "/props", "", "/props"},
		{"model POST", "POST", "/v1/chat/completions", `{"model":"alias/example"}`, "/v1/chat/completions"},
		{"local path", "POST", "/comfyui/api/prompt", `{}`, "/comfyui/api/prompt"},
		{"peer path", "POST", peerPathEndpoint, `{}`, "/comfyui/prompt"},
		{"peer inference", "POST", "/peer/v1/inference", `{"model":"example"}`, "/v1/chat/completions"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := &admissionCaseContext{t: t, h: h, owner: owner, forwarded: &forwarded, token: token}
			for _, allowed := range []bool{false, true} {
				ctx.run(test.name, test.method, test.path, test.body, test.target, allowed)
			}
		})
	}
}

// admissionCaseContext bundles the shared fixtures for running one admission-matrix
// request and asserting the admission decision. Grouping the fixtures here collapses
// the runAdmissionCase parameter list from 11 to a cohesive context plus per-case data.
type admissionCaseContext struct {
	t         *testing.T
	h         *Handler
	owner     *Reloader
	forwarded *atomic.Int32
	token     string
}

// run builds and dispatches one admission-matrix request, then asserts the
// admission callback ran once, the release/send counters match the allowed
// decision, and the upstream was forwarded exactly when admission passed.
func (ctx *admissionCaseContext) run(name, method, path, body, target string, allowed bool) {
	ctx.t.Helper()
	var checks, releases int
	ctx.h.backendAdmission = func(req *http.Request, backendID string) (func(), bool) {
		checks++
		if backendID != "default" || req.URL.Path != target || req.Method != method {
			ctx.t.Errorf("incorrect admission target: %s %s %s", backendID, req.Method, req.URL.Path)
		}
		if !allowed {
			return nil, false
		}
		return func() { releases++ }, true
	}
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+ctx.token)
	req.TLS = &tls.ConnectionState{}
	w := httptest.NewRecorder()
	before := ctx.forwarded.Load()
	switch name {
	case "peer path":
		req.Header.Set(peerPathServiceHeader, "comfyui")
		req.Header.Set(peerServicePathHeader, base64.RawURLEncoding.EncodeToString([]byte(target)))
		ctx.owner.PeerHandler().ServeHTTP(w, req)
	case "peer inference":
		req.Header.Set(peerBackendHeader, base64.RawURLEncoding.EncodeToString([]byte("default")))
		req.Header.Set(peerModelHeader, base64.RawURLEncoding.EncodeToString([]byte("example")))
		req.Header.Set(peerPathHeader, target)
		req.Header.Set(peerInstanceHeader, ctx.owner.instanceID)
		req.Header.Set(peerGenerationHeader, "1")
		ctx.owner.PeerHandler().ServeHTTP(w, req)
	default:
		ctx.h.ServeHTTP(w, req)
	}
	wantStatus, wantSends := http.StatusServiceUnavailable, 0
	if allowed {
		wantStatus, wantSends = http.StatusAccepted, 1
	}
	if w.Code != wantStatus || checks != 1 || releases != wantSends || ctx.forwarded.Load()-before != int32(wantSends) {
		ctx.t.Fatalf("allowed=%v status=%d checks=%d releases=%d sends=%d body=%q", allowed, w.Code, checks, releases, ctx.forwarded.Load()-before, w.Body.String())
	}
}

func TestConfiguredComfyUIAdmissionWaitsForReleaseAndAllowsRetryAfterLoadFailure(t *testing.T) {
	var freeRequests, statsRequests, competingRequests, promptRequests atomic.Int32
	comfy := newComfyTestServer(t, &freeRequests, &statsRequests)
	defer comfy.Close()

	backend := newBackendTestServer(t, &competingRequests, &promptRequests)
	defer backend.Close()

	cfg := baseConfig(backend.URL)
	cfg.PathRoutes = map[string]string{"/comfyui": "default"}
	cfg.Peer = &config.PeerConfig{
		NodeID: "owner", Listen: "127.0.0.1:9294", TLSCertFile: "cert.pem", TLSKeyFile: "key.pem",
		ComfyUIResultRead: &config.ComfyUIResultReadConfig{URL: comfy.URL},
		ComfyUILease: &config.ComfyUILeaseConfig{
			ReleaseTimeout: "5s", PollInterval: "100ms", QueueIdleSamples: 2,
			MinimumFreeVRAMBytes: map[string]int64{"cuda:0": 800},
			AdmissionBackends:    []string{"default"},
		},
	}
	h := newHandler(t, cfg).(*Handler)
	request := func(method, path string) *httptest.ResponseRecorder {
		t.Helper()
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(method, path, nil))
		return w
	}

	// The initial mediated competing request reconciles an unknown coordinator.
	// It waits for free VRAM evidence, polling after one /free request.
	failedLoad := request(http.MethodGet, "/props")
	if failedLoad.Code != http.StatusBadGateway || freeRequests.Load() != 1 || statsRequests.Load() < 3 || competingRequests.Load() != 1 {
		t.Fatalf("initial release/load status=%d frees=%d stats=%d competing=%d", failedLoad.Code, freeRequests.Load(), statsRequests.Load(), competingRequests.Load())
	}
	// A backend load failure is not treated as proof of a new ComfyUI release,
	// but it also does not poison the explicit lease transition: the scheduler
	// remains responsible for its own ordinary model routing on the next prompt.
	accepted := request(http.MethodPost, "/comfyui/api/prompt")
	if accepted.Code != http.StatusAccepted || promptRequests.Load() != 1 || freeRequests.Load() != 1 {
		t.Fatalf("prompt retry status=%d prompts=%d frees=%d", accepted.Code, promptRequests.Load(), freeRequests.Load())
	}
	secondCompeting := request(http.MethodGet, "/props")
	if secondCompeting.Code != http.StatusAccepted || freeRequests.Load() != 2 || competingRequests.Load() != 2 {
		t.Fatalf("active lease handoff status=%d frees=%d competing=%d", secondCompeting.Code, freeRequests.Load(), competingRequests.Load())
	}
}

func newComfyTestServer(t *testing.T, freeRequests, statsRequests *atomic.Int32) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/queue":
			_, _ = io.WriteString(w, `{"queue_running":[],"queue_pending":[]}`)
		case r.Method == http.MethodPost && r.URL.Path == "/free":
			freeRequests.Add(1)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Path == "/system_stats":
			count := statsRequests.Add(1)
			freeVRAM := int64(600)
			if count >= 3 {
				freeVRAM = 900
			}
			_, _ = fmt.Fprintf(w, `{"devices":[{"name":"GPU","type":"cuda","index":0,"vram_total":1000,"vram_free":%d}]}`, freeVRAM)
		default:
			http.NotFound(w, r)
		}
	}))
}

func newBackendTestServer(t *testing.T, competingRequests, promptRequests *atomic.Int32) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/props":
			if competingRequests.Add(1) == 1 {
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			w.WriteHeader(http.StatusAccepted)
		case "/comfyui/api/prompt":
			promptRequests.Add(1)
			w.WriteHeader(http.StatusAccepted)
		default:
			w.WriteHeader(http.StatusAccepted)
		}
	}))
}

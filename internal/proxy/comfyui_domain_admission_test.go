package proxy

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

func TestLlamaSwapUIAndControlPassThroughModelRoutesUseLease(t *testing.T) {
	var backendCalls, verifications atomic.Int32
	coordinator := newComfyUICoordinator(func(context.Context) (ComfyUIReleaseResult, error) {
		verifications.Add(1)
		return confirmedCoordinatorEvidence(), nil
	})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendCalls.Add(1)
		switch r.URL.Path {
		case "/":
			http.Redirect(w, r, "/ui/", http.StatusFound)
		case "/ui/":
			_, _ = w.Write([]byte("llama-swap-ui"))
		case "/ui/assets/app.js":
			_, _ = w.Write([]byte("app"))
		case "/api/version":
			_, _ = w.Write([]byte(`{"version":"test"}`))
		case "/api/models/unload":
			w.WriteHeader(http.StatusNoContent)
		case "/v1/chat/completions":
			if state := coordinator.snapshot(); state.State != "released" || state.Competing != 1 {
				t.Errorf("model dispatch forwarded without released lease: %+v", state)
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer backend.Close()
	cfg := baseConfig(backend.URL)
	h := newHandler(t, cfg).(*Handler)
	admit, err := newComfyUIDomainAdmission(coordinator, []string{"default"}, "default", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	h.backendAdmission = admit

	for _, test := range []struct {
		path string
		want int
	}{
		{"/", http.StatusFound},
		{"/ui/", http.StatusOK},
		{"/ui/assets/app.js", http.StatusOK},
		{"/api/version", http.StatusOK},
		{"/api/profiles", http.StatusNotFound},
	} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, test.path, nil))
		if w.Code != test.want {
			t.Errorf("GET %s status=%d want=%d body=%q", test.path, w.Code, test.want, w.Body.String())
		}
	}
	if backendCalls.Load() != 5 || verifications.Load() != 0 || coordinator.snapshot().State != "unknown" {
		t.Fatalf("read-only UI touched lease state: backend=%d verify=%d state=%+v", backendCalls.Load(), verifications.Load(), coordinator.snapshot())
	}
	management := httptest.NewRecorder()
	h.ServeHTTP(management, httptest.NewRequest(http.MethodPost, "/api/models/unload", nil))
	if management.Code != http.StatusNoContent || backendCalls.Load() != 6 || verifications.Load() != 0 {
		t.Fatalf("management request was unnecessarily lease-gated: status=%d backend=%d verify=%d", management.Code, backendCalls.Load(), verifications.Load())
	}
	model := httptest.NewRecorder()
	h.ServeHTTP(model, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil))
	if model.Code != http.StatusNoContent || backendCalls.Load() != 7 || verifications.Load() != 1 {
		t.Fatalf("model request did not hand off through the lease: status=%d backend=%d verify=%d", model.Code, backendCalls.Load(), verifications.Load())
	}
}

func TestComfyUIDomainFirstPromptActivation(t *testing.T) {
	for _, test := range []struct {
		name       string
		status     int
		wantState  string
		wantStatus int
	}{
		{"accepted prompt activates lease", http.StatusAccepted, "active", http.StatusAccepted},
		{"failed prompt leaves lease unknown", http.StatusBadGateway, "unknown", http.StatusBadGateway},
	} {
		t.Run(test.name, func(t *testing.T) {
			var sends atomic.Int32
			c := newComfyUICoordinator(func(context.Context) (ComfyUIReleaseResult, error) {
				return confirmedCoordinatorEvidence(), nil
			})
			if _, err := c.reconcile(t.Context()); err != nil {
				t.Fatal(err)
			}
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				sends.Add(1)
				state := c.snapshot()
				if state.State != "acquiring" || state.Submissions != 1 || state.LeaseID == "" {
					t.Errorf("first prompt was forwarded without an acquisition reservation: %+v", state)
				}
				w.WriteHeader(test.status)
			}))
			defer backend.Close()
			cfg := baseConfig(backend.URL)
			cfg.PathRoutes = map[string]string{"/comfyui": "default"}
			h := newHandler(t, cfg).(*Handler)
			admit, err := newComfyUIDomainAdmission(c, []string{"default"}, "default", time.Second)
			if err != nil {
				t.Fatal(err)
			}
			h.backendAdmission = admit
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/comfyui/prompt", nil))
			if state := c.snapshot(); w.Code != test.wantStatus || sends.Load() != 1 || state.State != test.wantState || state.Submissions != 0 {
				t.Fatalf("first prompt status=%d sends=%d state=%+v", w.Code, sends.Load(), state)
			}
			if test.wantState == "unknown" {
				if _, allowed := admit(httptest.NewRequest(http.MethodGet, "/props", nil), "default"); allowed {
					t.Fatal("ambiguous first prompt opened competing admission")
				}
			}
		})
	}
}

func TestComfyUIDomainAdmissionBootstrapsWhenComfyUIIsStopped(t *testing.T) {
	var sends atomic.Int32
	c := newComfyUICoordinator(func(context.Context) (ComfyUIReleaseResult, error) {
		return ComfyUIReleaseResult{State: "unknown"}, errComfyUIServiceUnavailable
	})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		sends.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer backend.Close()
	h := newHandler(t, baseConfig(backend.URL)).(*Handler)
	admit, err := newComfyUIDomainAdmission(c, []string{"default"}, "default", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	h.backendAdmission = admit

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/props", nil))
	if state := c.snapshot(); w.Code != http.StatusAccepted || sends.Load() != 1 || state.State != "released" {
		t.Fatalf("stopped-service bootstrap status=%d sends=%d state=%+v", w.Code, sends.Load(), state)
	}
	if receipt := c.receipt; receipt.State != "stopped" || receipt.FreeRequested {
		t.Fatalf("stopped-service bootstrap claimed a free operation: %+v", receipt)
	}
}

func TestLlamaSwapModelStartRecoversWhenActiveComfyUIIsStopped(t *testing.T) {
	var verifications, schedulerCalls atomic.Int32
	c := newComfyUICoordinator(func(context.Context) (ComfyUIReleaseResult, error) {
		if verifications.Add(1) == 1 {
			return confirmedCoordinatorEvidence(), nil
		}
		return ComfyUIReleaseResult{State: "unknown"}, errComfyUIServiceUnavailable
	})
	if _, err := c.reconcileInitial(context.Background()); err != nil {
		t.Fatal(err)
	}
	leaseID, finishPrompt, err := c.admitPrompt()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.activate(leaseID); err != nil {
		t.Fatal(err)
	}
	finishPrompt()

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		schedulerCalls.Add(1)
		if state := c.snapshot(); state.State != "released" || state.Competing != 1 {
			t.Errorf("model start reached scheduler without a stopped ComfyUI handoff: %+v", state)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer backend.Close()
	h := newHandler(t, baseConfig(backend.URL)).(*Handler)
	admit, err := newComfyUIDomainAdmission(c, []string{"default"}, "default", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	h.backendAdmission = admit
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/upstream/GLM-4.7-Flash-30B-A3B/", nil))
	if w.Code != http.StatusAccepted || schedulerCalls.Load() != 1 || verifications.Load() != 2 || c.snapshot().State != "released" {
		t.Fatalf("model start recovery: status=%d scheduler=%d verifications=%d state=%+v", w.Code, schedulerCalls.Load(), verifications.Load(), c.snapshot())
	}
}

func TestComfyUIRootBootstrapsThroughSchedulerWhenPersistentServiceIsStopped(t *testing.T) {
	var schedulerCalls atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		schedulerCalls.Add(1)
		if req.Method != http.MethodGet || req.URL.Path != "/comfyui/" {
			t.Errorf("unexpected scheduler request: %s %s", req.Method, req.URL.Path)
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("comfy-ui"))
	}))
	defer backend.Close()

	cfg := baseConfig(backend.URL)
	cfg.PathRoutes = map[string]string{"/comfyui": "default"}
	cfg.Peer = &config.PeerConfig{
		NodeID: "owner", Listen: "127.0.0.1:9294", TLSCertFile: "cert.pem", TLSKeyFile: "key.pem",
		ComfyUIResultRead: &config.ComfyUIResultReadConfig{URL: "http://127.0.0.1:1"},
		ComfyUILease: &config.ComfyUILeaseConfig{
			ReleaseTimeout: "5s", PollInterval: "100ms", QueueIdleSamples: 2,
			MinimumFreeVRAMBytes: map[string]int64{"cuda:0": 1},
			AdmissionBackends:    []string{"default"},
		},
	}
	h := newHandler(t, cfg).(*Handler)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/comfyui/", nil))
	if w.Code != http.StatusOK || w.Body.String() != "comfy-ui" || schedulerCalls.Load() != 1 {
		t.Fatalf("ComfyUI root bootstrap: status=%d body=%q scheduler=%d", w.Code, w.Body.String(), schedulerCalls.Load())
	}
}

func TestComfyUIDomainAdmissionHandoff(t *testing.T) {
	for _, verifyOK := range []bool{true, false} {
		name := "confirmed"
		if !verifyOK {
			name = "unknown"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			var checks, sends atomic.Int32
			started, unblock := make(chan struct{}), make(chan struct{})
			c := newComfyUICoordinator(func(ctx context.Context) (ComfyUIReleaseResult, error) {
				if checks.Add(1) == 1 {
					return confirmedCoordinatorEvidence(), nil
				}
				close(started)
				select {
				case <-ctx.Done():
					return ComfyUIReleaseResult{}, ctx.Err()
				case <-unblock:
				}
				if !verifyOK {
					return ComfyUIReleaseResult{}, ErrComfyUIReleaseUnknown
				}
				return confirmedCoordinatorEvidence(), nil
			})
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				sends.Add(1)
				if state := c.snapshot(); state.State != "released" || state.Competing != 1 {
					t.Errorf("backend called without a competing reservation: %+v", state)
				}
				w.WriteHeader(http.StatusAccepted)
			}))
			defer backend.Close()
			h := newHandler(t, baseConfig(backend.URL)).(*Handler)
			admit, err := newComfyUIDomainAdmission(c, []string{"default", "alternate"}, "default", 3*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			h.backendAdmission = admit
			if done, allowed := admit(httptest.NewRequest(http.MethodGet, "/props", nil), "outside-domain"); !allowed || done != nil {
				t.Fatal("unmanaged backend was gated")
			}
			status := func(method, path string) int {
				w := httptest.NewRecorder()
				h.ServeHTTP(w, httptest.NewRequestWithContext(ctx, method, path, nil))
				return w.Code
			}
			if c.snapshot().State != "unknown" || checks.Load() != 0 {
				t.Fatalf("coordinator did not start unknown: state=%+v checks=%d", c.snapshot(), checks.Load())
			}
			if _, err := c.reconcile(ctx); err != nil {
				t.Fatal(err)
			}
			for _, path := range []string{"/upstream/example/", "/v1/realtime"} {
				if got := status(http.MethodGet, path); got != http.StatusAccepted {
					t.Fatalf("model dispatch route %s did not use lease handoff: status=%d", path, got)
				}
			}
			if got := status(http.MethodGet, "/%70rops"); got != http.StatusServiceUnavailable {
				t.Fatalf("noncanonical model path was admitted: status=%d", got)
			}
			if sends.Load() != 2 {
				t.Fatalf("recognized model dispatch routes were not forwarded: %d", sends.Load())
			}
			prompt := httptest.NewRequestWithContext(ctx, http.MethodPost, "/comfyui/api/prompt", nil)
			if _, allowed := admit(prompt, "alternate"); allowed {
				t.Fatal("managed non-prompt backend admitted a prompt")
			}
			finish, allowed := admit(prompt, "default")
			if !allowed || finish == nil {
				t.Fatal("prompt could not reserve during acquisition")
			}
			defer finish()
			id := c.snapshot().LeaseID // Test observation only, not a helper credential exchange.
			if _, allowed := admit(prompt, "default"); allowed {
				t.Fatal("second prompt was admitted before helper activation")
			}
			if err := c.activate(id); err != nil {
				t.Fatal(err)
			}
			result := make(chan int, 1)
			go func() { result <- status(http.MethodGet, "/props?model=example") }()
			waitCoordinatorState(t, c, "draining")
			if sends.Load() != 2 || checks.Load() != 1 {
				t.Fatal("forwarding or verification started before the prompt drained")
			}
			if _, allowed := admit(prompt, "default"); allowed {
				t.Fatal("draining admitted a new prompt")
			}
			finish()
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("release verification did not start")
			}
			if sends.Load() != 2 {
				t.Fatal("competing GET reached the backend before verification")
			}
			close(unblock)
			wantStatus, wantSends := http.StatusServiceUnavailable, int32(2)
			if verifyOK {
				wantStatus, wantSends = http.StatusAccepted, 3
			}
			if got := <-result; got != wantStatus || sends.Load() != wantSends || c.snapshot().Competing != 0 {
				t.Fatalf("handoff status=%d sends=%d state=%+v", got, sends.Load(), c.snapshot())
			}
			if verifyOK {
				if _, err := c.release(ctx, id); err != nil || checks.Load() != 2 {
					t.Fatalf("stop callback did not reuse release receipt: checks=%d err=%v", checks.Load(), err)
				}
			} else if _, err := c.reserveCompeting(); !errors.Is(err, errComfyUIAdmissionBlocked) {
				t.Fatal("unconfirmed release opened admission")
			} else if got := status(http.MethodPost, "/v1/chat/completions"); got != http.StatusServiceUnavailable || checks.Load() != 2 || sends.Load() != 2 {
				t.Fatal("competing POST retried an unconfirmed release or forwarded")
			}
		})
	}
}

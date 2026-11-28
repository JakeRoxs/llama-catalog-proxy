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
		{"validation failure leaves lease unknown until verified reconciliation", http.StatusBadRequest, "unknown", http.StatusBadRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			runComfyUIDomainFirstPromptActivationCase(t, test.status, test.wantState, test.wantStatus)
		})
	}
}

func TestComfyUIStartModelUsesComfyUILease(t *testing.T) {
	var verifications atomic.Int32
	coordinator := newComfyUICoordinator(func(context.Context) (ComfyUIReleaseResult, error) {
		verifications.Add(1)
		return confirmedCoordinatorEvidence(), nil
	})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()
	h := newHandler(t, baseConfig(backend.URL)).(*Handler)
	admit, err := newComfyUIDomainAdmission(coordinator, []string{"default"}, "default", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	h.backendAdmission = admit

	for _, tc := range []struct {
		path, state string
		checks      int32
	}{
		{"/upstream/comfyui_auto/?_=1", "active", 0},
		{"/upstream/comfyui_auto/?_=2", "active", 0},
		{"/upstream/other/?_=3", "released", 1},
	} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, tc.path, nil))
		if state := coordinator.snapshot(); w.Code != http.StatusOK || state.State != tc.state || verifications.Load() != tc.checks {
			t.Fatalf("GET %s: status=%d state=%+v verifications=%d", tc.path, w.Code, state, verifications.Load())
		}
	}
}

func runComfyUIDomainFirstPromptActivationCase(t *testing.T, status int, wantState string, wantStatus int) {
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
		if status == http.StatusBadRequest {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":{"type":"prompt_outputs_failed_validation","message":"Prompt outputs failed validation"}}`))
			return
		}
		w.WriteHeader(status)
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
	if state := c.snapshot(); w.Code != wantStatus || sends.Load() != 1 || state.State != wantState || state.Submissions != 0 {
		t.Fatalf("first prompt status=%d sends=%d state=%+v", w.Code, sends.Load(), state)
	}
	if wantState == "unknown" {
		done, allowed := admit(httptest.NewRequest(http.MethodGet, "/props", nil), "default")
		if !allowed || done == nil {
			t.Fatal("failed first prompt did not trigger verified release before competing admission")
		}
		if state := c.snapshot(); state.State != "released" || state.Competing != 1 {
			t.Fatalf("competing admission was not backed by confirmed release: %+v", state)
		}
		done()
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

func TestCompetingStartRetriesAfterTransientInitialReconciliationFailure(t *testing.T) {
	var verifications, starts atomic.Int32
	coordinator := newComfyUICoordinator(func(context.Context) (ComfyUIReleaseResult, error) {
		if verifications.Add(1) == 1 {
			return ComfyUIReleaseResult{State: "unknown"}, errors.New("temporary queue probe failure")
		}
		return confirmedCoordinatorEvidence(), nil
	})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		starts.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer backend.Close()

	h := newHandler(t, baseConfig(backend.URL)).(*Handler)
	admit, err := newComfyUIDomainAdmission(coordinator, []string{"default"}, "default", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	h.backendAdmission = admit

	for i, wantStatus := range []int{http.StatusServiceUnavailable, http.StatusAccepted} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/upstream/other/?_=retry", nil))
		if w.Code != wantStatus {
			t.Fatalf("start attempt %d status=%d want=%d body=%q", i+1, w.Code, wantStatus, w.Body.String())
		}
	}
	if starts.Load() != 1 || verifications.Load() != 2 || coordinator.snapshot().State != "released" {
		t.Fatalf("starts=%d verifications=%d state=%+v", starts.Load(), verifications.Load(), coordinator.snapshot())
	}
}

func TestCompetingStartRetriesAfterAmbiguousReconciliationFailure(t *testing.T) {
	var verifications, starts atomic.Int32
	coordinator := newComfyUICoordinator(func(context.Context) (ComfyUIReleaseResult, error) {
		if verifications.Add(1) == 1 {
			return ComfyUIReleaseResult{State: "unknown", FreeRequested: true}, ErrComfyUIReleaseUnknown
		}
		return confirmedCoordinatorEvidence(), nil
	})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		starts.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer backend.Close()

	h := newHandler(t, baseConfig(backend.URL)).(*Handler)
	admit, err := newComfyUIDomainAdmission(coordinator, []string{"default"}, "default", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	h.backendAdmission = admit

	for i, wantStatus := range []int{http.StatusServiceUnavailable, http.StatusAccepted} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/upstream/other/?_=retry", nil))
		if w.Code != wantStatus {
			t.Fatalf("start attempt %d status=%d want=%d body=%q", i+1, w.Code, wantStatus, w.Body.String())
		}
	}
	if starts.Load() != 1 || verifications.Load() != 2 || coordinator.snapshot().State != "released" {
		t.Fatalf("starts=%d verifications=%d state=%+v", starts.Load(), verifications.Load(), coordinator.snapshot())
	}
}

func TestFirstComfyUIRunLetsSchedulerSwapBeforeCheckingVRAM(t *testing.T) {
	var verifies, sends atomic.Int32
	for _, endpoint := range []string{"/comfyui/prompt", "/comfyui/api/prompt"} {
		t.Run(endpoint, func(t *testing.T) {
			verifies.Store(0)
			sends.Store(0)
			c := newComfyUICoordinator(func(context.Context) (ComfyUIReleaseResult, error) {
				verifies.Add(1)
				return ComfyUIReleaseResult{FreeRequested: true}, ErrComfyUIReleaseUnknown
			})
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				sends.Add(1)
				if req.Method != http.MethodPost || req.URL.Path != endpoint || c.snapshot().State != "acquiring" {
					t.Errorf("unexpected scheduler request %s %s, lease state=%+v", req.Method, req.URL.Path, c.snapshot())
				}
				w.WriteHeader(http.StatusAccepted)
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
			h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, endpoint, nil))
			if w.Code != http.StatusAccepted || sends.Load() != 1 || verifies.Load() != 0 || c.snapshot().State != "active" {
				t.Fatalf("status=%d sends=%d checks=%d state=%+v", w.Code, sends.Load(), verifies.Load(), c.snapshot())
			}
		})
	}
}

func TestComfyUIRunDoesNotBypassUnknownRelease(t *testing.T) {
	c := newComfyUICoordinator(func(context.Context) (ComfyUIReleaseResult, error) {
		return ComfyUIReleaseResult{FreeRequested: true}, ErrComfyUIReleaseUnknown
	})
	admit, err := newComfyUIDomainAdmission(c, []string{"default"}, "default", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, allowed := admit(httptest.NewRequest(http.MethodGet, "/props", nil), "default"); allowed {
		t.Fatal("ambiguous release admitted a competing request")
	}
	if _, allowed := admit(httptest.NewRequest(http.MethodPost, "/comfyui/api/prompt", nil), "default"); allowed {
		t.Fatal("ambiguous release admitted a prompt")
	}
	if _, allowed := admit(httptest.NewRequest(http.MethodGet, "/upstream/comfyui_auto/?_=1", nil), "default"); allowed {
		t.Fatal("ambiguous release admitted an explicit start")
	}
}

func TestComfyUIRunCanAcquireAfterPreFreeProbeFailure(t *testing.T) {
	checks := 0
	c := newComfyUICoordinator(func(context.Context) (ComfyUIReleaseResult, error) {
		checks++
		return ComfyUIReleaseResult{}, ErrComfyUIReleaseUnknown
	})
	admit, err := newComfyUIDomainAdmission(c, []string{"default"}, "default", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, allowed := admit(httptest.NewRequest(http.MethodGet, "/props", nil), "default"); allowed {
		t.Fatal("failed probe admitted a competing request")
	}
	done, allowed := admit(httptest.NewRequest(http.MethodPost, "/comfyui/api/prompt", nil), "default")
	if !allowed || done == nil || checks != 1 || c.snapshot().State != "acquiring" {
		t.Fatalf("allowed=%v checks=%d state=%+v", allowed, checks, c.snapshot())
	}
	done()
	if _, allowed := admit(httptest.NewRequest(http.MethodPost, "/comfyui/api/prompt", nil), "default"); allowed {
		t.Fatal("scheduler dispatch without activation reopened admission")
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

// handoffFixture holds the shared scenario state for the admission handoff
// test so the top-level test stays a simple true/false subtest loop.
type handoffFixture struct {
	ctx     context.Context
	cancel  context.CancelFunc
	checks  atomic.Int32
	sends   atomic.Int32
	started chan struct{}
	unblock chan struct{}
	c       *comfyUICoordinator
	backend *httptest.Server
	h       *Handler
	admit   func(*http.Request, string) (func(), bool)
	status  func(method, path string) int
	result  chan int
	id      string
}

// newHandoffFixture builds the coordinator, backend, handler and admission
// used by the handoff scenario. The verifier confirms on its first call and
// then blocks on the fixture's unblock channel until the test releases it.
func newHandoffFixture(t *testing.T, verifyOK bool) *handoffFixture {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	f := &handoffFixture{
		ctx:     ctx,
		cancel:  cancel,
		started: make(chan struct{}),
		unblock: make(chan struct{}),
	}
	f.c = newComfyUICoordinator(func(ctx context.Context) (ComfyUIReleaseResult, error) {
		call := f.checks.Add(1)
		if call == 1 {
			return confirmedCoordinatorEvidence(), nil
		}
		if call > 2 {
			if !verifyOK {
				return ComfyUIReleaseResult{FreeRequested: true}, ErrComfyUIReleaseUnknown
			}
			return confirmedCoordinatorEvidence(), nil
		}
		close(f.started)
		select {
		case <-ctx.Done():
			return ComfyUIReleaseResult{}, ctx.Err()
		case <-f.unblock:
		}
		if !verifyOK {
			return ComfyUIReleaseResult{}, ErrComfyUIReleaseUnknown
		}
		return confirmedCoordinatorEvidence(), nil
	})
	f.backend = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		f.sends.Add(1)
		if state := f.c.snapshot(); state.State != "released" || state.Competing != 1 {
			t.Errorf("backend called without a competing reservation: %+v", state)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	f.h = newHandler(t, baseConfig(f.backend.URL)).(*Handler)
	admit, err := newComfyUIDomainAdmission(f.c, []string{"default", "alternate"}, "default", 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	f.h.backendAdmission = admit
	f.admit = admit
	f.status = func(method, path string) int {
		w := httptest.NewRecorder()
		f.h.ServeHTTP(w, httptest.NewRequestWithContext(ctx, method, path, nil))
		return w.Code
	}
	return f
}

// exercisePreconditions validates the unmanaged backend, unknown startup state,
// reconciliation, lease-gated dispatch routes and the noncanonical path.
func exercisePreconditions(t *testing.T, f *handoffFixture) {
	if done, allowed := f.admit(httptest.NewRequest(http.MethodGet, "/props", nil), "outside-domain"); !allowed || done != nil {
		t.Fatal("unmanaged backend was gated")
	}
	if f.c.snapshot().State != "unknown" || f.checks.Load() != 0 {
		t.Fatalf("coordinator did not start unknown: state=%+v checks=%d", f.c.snapshot(), f.checks.Load())
	}
	if _, err := f.c.reconcile(f.ctx); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/upstream/example/", "/v1/realtime"} {
		if got := f.status(http.MethodGet, path); got != http.StatusAccepted {
			t.Fatalf("model dispatch route %s did not use lease handoff: status=%d", path, got)
		}
	}
	if got := f.status(http.MethodGet, "/%70rops"); got != http.StatusServiceUnavailable {
		t.Fatalf("noncanonical model path was admitted: status=%d", got)
	}
	if f.sends.Load() != 2 {
		t.Fatalf("recognized model dispatch routes were not forwarded: %d", f.sends.Load())
	}
}

// reserveAndDrain reserves the first prompt, activates the lease, drains the
// submission, and blocks until release verification has started.
func reserveAndDrain(t *testing.T, f *handoffFixture) {
	prompt := httptest.NewRequestWithContext(f.ctx, http.MethodPost, "/comfyui/api/prompt", nil)
	if _, allowed := f.admit(prompt, "alternate"); allowed {
		t.Fatal("managed non-prompt backend admitted a prompt")
	}
	finish, allowed := f.admit(prompt, "default")
	if !allowed || finish == nil {
		t.Fatal("prompt could not reserve during acquisition")
	}
	f.id = f.c.snapshot().LeaseID // Test observation only, not a helper credential exchange.
	if _, allowed := f.admit(prompt, "default"); allowed {
		t.Fatal("second prompt was admitted before helper activation")
	}
	if err := f.c.activate(f.id); err != nil {
		t.Fatal(err)
	}
	f.result = make(chan int, 1)
	go func() { f.result <- f.status(http.MethodGet, "/props?model=example") }()
	waitCoordinatorState(t, f.c, "draining")
	if f.sends.Load() != 2 || f.checks.Load() != 1 {
		t.Fatal("forwarding or verification started before the prompt drained")
	}
	if _, allowed := f.admit(prompt, "default"); allowed {
		t.Fatal("draining admitted a new prompt")
	}
	finish()
	select {
	case <-f.started:
	case <-f.ctx.Done():
		t.Fatal("release verification did not start")
	}
	if f.sends.Load() != 2 {
		t.Fatal("competing GET reached the backend before verification")
	}
	close(f.unblock)
}

// unblockAndVerify releases the unblock channel, validates the handoff result,
// and exercises the confirmed stop-receipt reuse or the blocked rejection path.
func unblockAndVerify(t *testing.T, f *handoffFixture, verifyOK bool) {
	wantStatus, wantSends := http.StatusServiceUnavailable, int32(2)
	if verifyOK {
		wantStatus, wantSends = http.StatusAccepted, 3
	}
	if got := <-f.result; got != wantStatus || f.sends.Load() != wantSends || f.c.snapshot().Competing != 0 {
		t.Fatalf("handoff status=%d sends=%d state=%+v", got, f.sends.Load(), f.c.snapshot())
	}
	if verifyOK {
		if _, err := f.c.release(f.ctx, f.id); err != nil || f.checks.Load() != 2 {
			t.Fatalf("stop callback did not reuse release receipt: checks=%d err=%v", f.checks.Load(), err)
		}
	} else if _, err := f.c.reserveCompeting(); !errors.Is(err, errComfyUIAdmissionBlocked) {
		t.Fatal("unconfirmed release opened admission")
	} else if got := f.status(http.MethodPost, "/v1/chat/completions"); got != http.StatusServiceUnavailable || f.checks.Load() != 3 || f.sends.Load() != 2 {
		t.Fatal("competing POST retried an unconfirmed release or forwarded")
	}
}

func TestComfyUIDomainAdmissionHandoff(t *testing.T) {
	for _, verifyOK := range []bool{true, false} {
		name := "confirmed"
		if !verifyOK {
			name = "unknown"
		}
		t.Run(name, func(t *testing.T) {
			f := newHandoffFixture(t, verifyOK)
			defer f.backend.Close()
			defer f.cancel()
			exercisePreconditions(t, f)
			reserveAndDrain(t, f)
			unblockAndVerify(t, f, verifyOK)
		})
	}
}

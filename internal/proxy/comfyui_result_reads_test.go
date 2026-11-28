package proxy

import (
	"crypto/tls"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

func TestValidateComfyUIResultReadAllowlist(t *testing.T) {
	tests := []struct {
		name       string
		request    *http.Request
		wantMatch  bool
		wantStatus int
	}{
		{"all history", httptest.NewRequest(http.MethodGet, "/comfyui/history", nil), true, 0},
		{"queue status", httptest.NewRequest(http.MethodGet, "/comfyui/queue", nil), true, 0},
		{"prompt status", httptest.NewRequest(http.MethodGet, "/comfyui/prompt", nil), true, 0},
		{"system stats", httptest.NewRequest(http.MethodGet, "/comfyui/system_stats", nil), true, 0},
		{"API queue alias", httptest.NewRequest(http.MethodGet, "/comfyui/api/queue", nil), true, 0},
		{"API prompt alias", httptest.NewRequest(http.MethodGet, "/comfyui/api/prompt", nil), true, 0},
		{"API stats alias", httptest.NewRequest(http.MethodGet, "/comfyui/api/system_stats", nil), true, 0},
		{"API history alias", httptest.NewRequest(http.MethodGet, "/comfyui/api/history/id", nil), true, 0},
		{"API view alias", httptest.NewRequest(http.MethodGet, "/comfyui/api/view?filename=output.png", nil), true, 0},
		{"view input image", httptest.NewRequest(http.MethodGet, "/comfyui/api/view?filename=input.png&type=input", nil), true, 0},
		{"view temporary preview", httptest.NewRequest(http.MethodGet, "/comfyui/api/view?filename=preview.png&type=temp", nil), true, 0},
		{"view frontend preview parameters", httptest.NewRequest(http.MethodGet, "/comfyui/api/view?filename=output.png&type=output&preview=webp%3B90&rand=0.123&channel=rgba", nil), true, 0},
		{"history offset", httptest.NewRequest(http.MethodGet, "/comfyui/api/history?max_items=20&offset=-1", nil), true, 0},
		{"jobs status", httptest.NewRequest(http.MethodGet, "/comfyui/api/jobs?status=completed,failed&limit=20&offset=0&sort_by=created_at&sort_order=desc", nil), true, 0},
		{"job item", httptest.NewRequest(http.MethodGet, "/comfyui/api/jobs/prompt-id", nil), true, 0},
		{"status query rejected", httptest.NewRequest(http.MethodGet, "/comfyui/queue?clear=true", nil), true, http.StatusBadRequest},
		{"jobs unknown query", httptest.NewRequest(http.MethodGet, "/comfyui/api/jobs?user_id=other", nil), true, http.StatusBadRequest},
		{"jobs oversized limit", httptest.NewRequest(http.MethodGet, "/comfyui/api/jobs?limit=1001", nil), true, http.StatusBadRequest},
		{"jobs duplicate query", httptest.NewRequest(http.MethodGet, "/comfyui/api/jobs?limit=1&limit=2", nil), true, http.StatusBadRequest},
		{"job cancellation GET rejected", httptest.NewRequest(http.MethodGet, "/comfyui/api/jobs/id/cancel", nil), true, http.StatusBadRequest},
		{"API generation excluded", httptest.NewRequest(http.MethodPost, "/comfyui/api/prompt", strings.NewReader(`{}`)), false, 0},
		{"queue mutation excluded", httptest.NewRequest(http.MethodPost, "/comfyui/queue", strings.NewReader(`{}`)), false, 0},
		{"job cancellation excluded", httptest.NewRequest(http.MethodPost, "/comfyui/api/jobs/id/cancel", strings.NewReader(`{}`)), false, 0},
		{"custom GET excluded", httptest.NewRequest(http.MethodGet, "/comfyui/custom/load_model", nil), false, 0},
		{"health not invented", httptest.NewRequest(http.MethodGet, "/comfyui/health", nil), false, 0},
		{"specific history item", httptest.NewRequest(http.MethodGet, "/comfyui/history/550e8400-e29b-41d4-a716-446655440000", nil), true, 0},
		{"history filter", httptest.NewRequest(http.MethodGet, "/comfyui/history?prompt_id=550e8400-e29b-41d4-a716-446655440000&max_items=20", nil), true, 0},
		{"view output", httptest.NewRequest(http.MethodGet, "/comfyui/view?filename=output.png&subfolder=run&type=output", nil), true, 0},
		{"head view rejected", httptest.NewRequest(http.MethodHead, "/comfyui/view?filename=output.png&type=output", nil), true, http.StatusMethodNotAllowed},
		{"prompt write excluded", httptest.NewRequest(http.MethodPost, "/comfyui/prompt", strings.NewReader(`{"prompt":{}}`)), false, 0},
		{"history write rejected", httptest.NewRequest(http.MethodPost, "/comfyui/history/id", strings.NewReader("body")), true, http.StatusMethodNotAllowed},
		{"invalid history ID rejected", httptest.NewRequest(http.MethodGet, "/comfyui/history/../queue", nil), true, http.StatusBadRequest},
		{"nested history ID rejected", httptest.NewRequest(http.MethodGet, "/comfyui/history/id/child", nil), true, http.StatusNotFound},
		{"encoded slash rejected", httptest.NewRequest(http.MethodGet, "/comfyui/history%2Fid", nil), true, http.StatusBadRequest},
		{"unsupported view type denied", httptest.NewRequest(http.MethodGet, "/comfyui/view?filename=secret.png&type=models", nil), true, http.StatusForbidden},
		{"unsupported preview format rejected", httptest.NewRequest(http.MethodGet, "/comfyui/view?filename=output.png&preview=html", nil), true, http.StatusBadRequest},
		{"invalid cache buster rejected", httptest.NewRequest(http.MethodGet, "/comfyui/view?filename=output.png&rand=../../secret", nil), true, http.StatusBadRequest},
		{"peer hash filename denied", httptest.NewRequest(http.MethodGet, "/comfyui/view?filename=blake3%3Aabc&type=output", nil), true, http.StatusForbidden},
		{"view traversal rejected", httptest.NewRequest(http.MethodGet, "/comfyui/view?filename=..%2Fsecret.png", nil), true, http.StatusBadRequest},
		{"unknown view query rejected", httptest.NewRequest(http.MethodGet, "/comfyui/view?filename=output.png&user_id=other", nil), true, http.StatusBadRequest},
		{"duplicate view parameter rejected", httptest.NewRequest(http.MethodGet, "/comfyui/view?filename=a.png&filename=b.png", nil), true, http.StatusBadRequest},
		{"history item query rejected", httptest.NewRequest(http.MethodGet, "/comfyui/history/id?prompt_id=id", nil), true, http.StatusBadRequest},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			matched, status, _ := validateComfyUIResultReadRequest(test.request, false)
			if matched != test.wantMatch || status != test.wantStatus {
				t.Fatalf("result-read validation = matched %v, status %d; want %v, %d", matched, status, test.wantMatch, test.wantStatus)
			}
		})
	}
	bodyRequest := httptest.NewRequest(http.MethodGet, "/comfyui/view?filename=output.png", strings.NewReader("unexpected"))
	if matched, status, _ := validateComfyUIResultReadRequest(bodyRequest, false); !matched || status != http.StatusBadRequest {
		t.Fatalf("GET with body = matched %v, status %d", matched, status)
	}
	ownerHashRequest := httptest.NewRequest(http.MethodGet, "/comfyui/view?filename=blake3%3Aabc&type=output", nil)
	if matched, status, _ := validateComfyUIResultReadRequest(ownerHashRequest, true); !matched || status != 0 {
		t.Fatalf("owner hash filename = matched %v, status %d", matched, status)
	}
}

func TestComfyUIStatusPollingBypassesSchedulerLocallyAndOverPeer(t *testing.T) {
	var directReads, schedulerCalls atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		directReads.Add(1)
		if req.Method != http.MethodGet || strings.HasPrefix(req.URL.Path, "/comfyui/") {
			t.Errorf("unexpected direct request: %s %s", req.Method, req.URL.Path)
		}
		_, _ = io.WriteString(w, req.URL.RequestURI())
	}))
	defer backend.Close()
	scheduler := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		schedulerCalls.Add(1)
		_, _ = io.WriteString(w, "scheduled "+req.Method+" "+req.URL.Path)
	}))
	defer scheduler.Close()
	schedulerURL, err := url.Parse(scheduler.URL)
	if err != nil {
		t.Fatal(err)
	}
	resultProxy, err := newComfyUIResultReadProxy(&config.ComfyUIResultReadConfig{URL: backend.URL}, discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	handler := &Handler{
		logger: discardLogger(), comfyUIResultReadProxy: resultProxy, comfyUIResultSlots: make(chan struct{}, 4),
		pathRoutes: []route{{prefix: "/comfyui", backendID: "scheduler"}},
		proxies:    map[string]*httputil.ReverseProxy{"scheduler": httputil.NewSingleHostReverseProxy(schedulerURL)},
	}
	token := strings.Repeat("s", 32)
	owner := &Reloader{logger: discardLogger(), peerSlots: make(chan struct{}, 4), peerPathSlots: make(chan struct{}, 4)}
	owner.state.Store(&reloaderState{handler: handler, peer: &config.PeerConfig{
		NodeID: "owner", ExposedPathRoutes: map[string]string{"comfyui": "/comfyui"},
		Peers: map[string]config.PeerGrant{"caller": {Token: token, PathAccess: []string{"comfyui"}}},
	}})

	assertStatusPollsRouteDirectly(t, &directReads, &schedulerCalls, handler, token, owner, peerPathEndpoint)
	assertUIRootScheduled(t, &directReads, &schedulerCalls, handler, token, owner, peerPathEndpoint)
	assertMutationsScheduled(t, &directReads, &schedulerCalls, handler)
}

// assertStatusPollsRouteDirectly verifies the nine result-read endpoints are polled
// directly against the result backend (scheduler untouched) in both local and peer
// modes, preserving peer TLS/path headers and the bearer token.
func assertStatusPollsRouteDirectly(t *testing.T, directReads, schedulerCalls *atomic.Int32,
	handler *Handler, token string, owner *Reloader, peerPathEndpoint string) {
	endpoints := []string{"/queue", "/prompt", "/system_stats", "/api/queue", "/api/prompt",
		"/api/system_stats", "/api/history/id", "/api/jobs?limit=20", "/api/jobs/id"}
	for _, endpoint := range endpoints {
		assertStatusPollRoute(t, false, endpoint, handler, token, owner, peerPathEndpoint)
		assertStatusPollRoute(t, true, endpoint, handler, token, owner, peerPathEndpoint)
	}
	if directReads.Load() != 18 || schedulerCalls.Load() != 0 {
		t.Fatalf("polling requests: direct=%d scheduler=%d", directReads.Load(), schedulerCalls.Load())
	}
}

func assertStatusPollRoute(t *testing.T, viaPeer bool, endpoint string, handler *Handler, token string,
	owner *Reloader, peerPathEndpoint string) {
	requestPath := "/comfyui" + endpoint
	endpointPath, query, _ := strings.Cut(endpoint, "?")
	if viaPeer {
		requestPath = peerPathEndpoint
		if query != "" {
			requestPath += "?" + query
		}
	}
	request := httptest.NewRequest(http.MethodGet, requestPath, nil)
	request.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	if viaPeer {
		request.TLS = &tls.ConnectionState{}
		request.Header.Set(peerPathServiceHeader, "comfyui")
		request.Header.Set(peerServicePathHeader, base64.RawURLEncoding.EncodeToString([]byte("/comfyui"+endpointPath)))
		owner.PeerHandler().ServeHTTP(response, request)
	} else {
		handler.ServeHTTP(response, request)
	}
	if response.Code != http.StatusOK || response.Body.String() != endpoint {
		t.Fatalf("peer=%v endpoint=%s: status=%d body=%q", viaPeer, endpoint, response.Code, response.Body.String())
	}
}

// assertUIRootScheduled verifies the UI root path (/comfyui/) is routed through the
// scheduler in both local and peer modes, preserving peer TLS/path headers and the
// bearer token.
func assertUIRootScheduled(t *testing.T, directReads, schedulerCalls *atomic.Int32,
	handler *Handler, token string, owner *Reloader, peerPathEndpoint string) {
	for _, viaPeer := range []bool{false, true} {
		request := httptest.NewRequest(http.MethodGet, "/comfyui/", nil)
		if viaPeer {
			request = httptest.NewRequest(http.MethodGet, peerPathEndpoint, nil)
			request.TLS = &tls.ConnectionState{}
			request.Header.Set("Authorization", "Bearer "+token)
			request.Header.Set(peerPathServiceHeader, "comfyui")
			request.Header.Set(peerServicePathHeader, base64.RawURLEncoding.EncodeToString([]byte("/comfyui/")))
			response := httptest.NewRecorder()
			owner.PeerHandler().ServeHTTP(response, request)
			if response.Code != http.StatusOK || response.Body.String() != "scheduled GET /comfyui/" {
				t.Fatalf("peer UI root was not scheduled: status=%d body=%q", response.Code, response.Body.String())
			}
			continue
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK || response.Body.String() != "scheduled GET /comfyui/" {
			t.Fatalf("local UI root was not scheduled: status=%d body=%q", response.Code, response.Body.String())
		}
	}
	if schedulerCalls.Load() != 2 || directReads.Load() != 18 {
		t.Fatalf("UI root routing: direct=%d scheduler=%d", directReads.Load(), schedulerCalls.Load())
	}
}

// assertMutationsScheduled verifies the POST mutation endpoints are routed through
// the scheduler rather than the result backend.
func assertMutationsScheduled(t *testing.T, directReads, schedulerCalls *atomic.Int32,
	handler *Handler) {
	for _, endpoint := range []string{"/prompt", "/api/prompt", "/queue", "/api/queue"} {
		request := httptest.NewRequest(http.MethodPost, "/comfyui"+endpoint, strings.NewReader(`{}`))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK || response.Body.String() != "scheduled POST /comfyui"+endpoint {
			t.Fatalf("mutation %s was not scheduled: status=%d body=%q", endpoint, response.Code, response.Body.String())
		}
	}
	if schedulerCalls.Load() != 6 || directReads.Load() != 18 {
		t.Fatalf("mutation routing: direct=%d scheduler=%d", directReads.Load(), schedulerCalls.Load())
	}
}

func TestComfyUIViewRoutesFrontendInputAndOutputImages(t *testing.T) {
	requests := make(chan string, 3)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requests <- req.URL.RequestURI()
		w.Header().Set("Content-Type", "image/png")
		_, _ = io.WriteString(w, "image")
	}))
	defer backend.Close()

	resultProxy, err := newComfyUIResultReadProxy(&config.ComfyUIResultReadConfig{URL: backend.URL}, discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	handler := &Handler{
		logger: discardLogger(), comfyUIResultReadProxy: resultProxy, comfyUIResultSlots: make(chan struct{}, 1),
	}
	for _, test := range []struct {
		requestURI string
		wantURI    string
	}{
		{
			"/comfyui/api/view?filename=marinara-ref.png&type=input&preview=webp%3B90&rand=0.123",
			"/api/view?filename=marinara-ref.png&type=input&preview=webp%3B90&rand=0.123",
		},
		{
			"/comfyui/api/view?filename=ComfyUI_00001_.png&subfolder=&type=output&preview=webp%3B90&rand=0.456",
			"/api/view?filename=ComfyUI_00001_.png&subfolder=&type=output&preview=webp%3B90&rand=0.456",
		},
		{
			"/comfyui/api/view?filename=ComfyUI_temp_12345.png&type=temp&rand=0.789",
			"/api/view?filename=ComfyUI_temp_12345.png&type=temp&rand=0.789",
		},
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, test.requestURI, nil))
		if response.Code != http.StatusOK || response.Body.String() != "image" {
			t.Fatalf("GET %s status=%d body=%q", test.requestURI, response.Code, response.Body.String())
		}
		if got := <-requests; got != test.wantURI {
			t.Errorf("upstream request URI=%q want=%q", got, test.wantURI)
		}
	}
}

func TestLocalComfyUIResultReadsBypassSchedulerAndStripCallerCredentials(t *testing.T) {
	var resultReads, llamaReads, jobQueueProbes, llamaQueueRequests atomic.Int32
	resultBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		resultReads.Add(1)
		if req.URL.Path != "/view" || req.URL.RawQuery != "filename=output.png&subfolder=run&type=output" {
			t.Errorf("result request path/query = %q?%s", req.URL.Path, req.URL.RawQuery)
		}
		if req.Header.Get("Authorization") != "Bearer owner-secret" || req.Header.Get("X-Owner-Header") != "owner-value" {
			t.Errorf("owner credentials were not applied: %v", req.Header)
		}
		if req.Header.Get("Cookie") != "" || req.Header.Get("X-Caller-Secret") != "" {
			t.Errorf("caller credentials reached ComfyUI: %v", req.Header)
		}
		if req.Header.Get("Range") != "bytes=0-3" {
			t.Errorf("safe range header was dropped: %v", req.Header)
		}
		w.Header().Set("Set-Cookie", "owner-session=secret")
		w.Header().Set("Authorization", "Bearer backend-secret")
		_, _ = io.WriteString(w, "image")
	}))
	defer resultBackend.Close()

	llama := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		llamaReads.Add(1)
		if req.URL.Path == "/comfyui/queue" {
			llamaQueueRequests.Add(1)
		}
		_, _ = io.WriteString(w, "llama-swap")
	}))
	defer llama.Close()
	queue := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		jobQueueProbes.Add(1)
		_, _ = io.WriteString(w, `{"queue_running":[["active"]],"queue_pending":[]}`)
	}))
	defer queue.Close()
	tracker := newComfyUIJobTracker(config.Backend{URL: queue.URL}, queue.Client())
	tracker.promptResponse(http.StatusOK)
	llamaTarget, err := url.Parse(llama.URL)
	if err != nil {
		t.Fatal(err)
	}
	resultProxy, err := newComfyUIResultReadProxy(&config.ComfyUIResultReadConfig{
		URL: resultBackend.URL, APIKey: "owner-secret", Headers: map[string]string{"X-Owner-Header": "owner-value"},
	}, discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	handler := &Handler{
		logger: discardLogger(), comfyUIResultReadProxy: resultProxy, comfyUIResultSlots: make(chan struct{}, 1),
		comfyUIWaitTimeout: time.Second, comfyUIBackendID: "llama", comfyUIJobs: tracker,
		defaultBackend: "llama", pathRoutes: []route{{prefix: "/comfyui", backendID: "llama"}},
		proxies: map[string]*httputil.ReverseProxy{"llama": httputil.NewSingleHostReverseProxy(llamaTarget)},
	}
	counters := resultReadCounters{
		resultReads: &resultReads, llamaReads: &llamaReads, jobQueueProbes: &jobQueueProbes,
		llamaQueueRequests: &llamaQueueRequests,
	}
	request := httptest.NewRequest(http.MethodGet, "/comfyui/view?filename=output.png&subfolder=run&type=output", nil)
	request.Header.Set("Authorization", "Bearer caller-secret")
	request.Header.Set("Cookie", "session=caller-secret")
	request.Header.Set("X-Caller-Secret", "caller-secret")
	request.Header.Set("Range", "bytes=0-3")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	assertResultReadServed(t, response, counters, resultReadExpectation{
		status: http.StatusOK, body: "image", resultReads: 1,
	})

	repeat := httptest.NewRecorder()
	repeatRequest := httptest.NewRequest(http.MethodGet, "/comfyui/view?filename=output.png&subfolder=run&type=output", nil)
	repeatRequest.Header.Set("Range", "bytes=0-3")
	handler.ServeHTTP(repeat, repeatRequest)
	assertResultReadRepeated(t, repeat, counters, resultReadExpectation{
		status: http.StatusOK, resultReads: 2,
	})

	invalidWrite := httptest.NewRecorder()
	handler.ServeHTTP(invalidWrite, httptest.NewRequest(http.MethodPost, "/comfyui/history/id", strings.NewReader("{}")))
	assertHistoryWriteRejected(t, invalidWrite, &llamaReads, http.StatusMethodNotAllowed, int32(0))

	otherRead := httptest.NewRecorder()
	handler.ServeHTTP(otherRead, httptest.NewRequest(http.MethodGet, "/comfyui/other", nil))
	assertNonResultFallsBackToLlama(t, otherRead, counters, resultReadExpectation{
		status: http.StatusOK, llamaReads: 1,
	})
}

type resultReadCounters struct {
	resultReads        *atomic.Int32
	llamaReads         *atomic.Int32
	jobQueueProbes     *atomic.Int32
	llamaQueueRequests *atomic.Int32
}

type resultReadExpectation struct {
	status             int
	body               string
	resultReads        int32
	llamaReads         int32
	jobQueueProbes     int32
	llamaQueueRequests int32
}

// assertResultReadServed verifies the initial result read: the body passes through
// unchanged, caller credentials (Set-Cookie, Authorization) are stripped from the
// response, the result backend was contacted once with owner credentials and the
// safe Range, and no llama-swap or job-queue activity is triggered.
func assertResultReadServed(t *testing.T, response *httptest.ResponseRecorder,
	counters resultReadCounters, want resultReadExpectation) {
	if response.Code != want.status || response.Body.String() != want.body ||
		response.Header().Get("Set-Cookie") != "" ||
		response.Header().Get("Authorization") != "" ||
		counters.resultReads.Load() != want.resultReads || counters.llamaReads.Load() != want.llamaReads ||
		counters.jobQueueProbes.Load() != want.jobQueueProbes {
		t.Fatalf("result-read response status=%d body=%q headers=%v result=%d llama=%d job_queue=%d",
			response.Code, response.Body.String(), response.Header(), counters.resultReads.Load(),
			counters.llamaReads.Load(), counters.jobQueueProbes.Load())
	}
}

// assertResultReadRepeated verifies a second result read reuses the cached result
// (another backend read, no scheduler or job-queue activity).
func assertResultReadRepeated(t *testing.T, response *httptest.ResponseRecorder,
	counters resultReadCounters, want resultReadExpectation) {
	if response.Code != want.status || counters.resultReads.Load() != want.resultReads ||
		counters.llamaReads.Load() != want.llamaReads || counters.jobQueueProbes.Load() != want.jobQueueProbes {
		t.Fatalf("repeated result read acquired scheduler activity: status=%d result=%d llama=%d job_queue=%d",
			response.Code, counters.resultReads.Load(), counters.llamaReads.Load(), counters.jobQueueProbes.Load())
	}
}

// assertHistoryWriteRejected verifies a POST to a result-read path is rejected
// with MethodNotAllowed and does not reach llama-swap.
func assertHistoryWriteRejected(t *testing.T, response *httptest.ResponseRecorder,
	llamaReads *atomic.Int32, wantStatus int, wantLlamaReads int32) {
	if response.Code != wantStatus || llamaReads.Load() != wantLlamaReads {
		t.Fatalf("POST to result path fell through: status=%d llama=%d", response.Code, llamaReads.Load())
	}
}

// assertNonResultFallsBackToLlama verifies a non-result path remains on llama-swap
// (one llama read, no queue probing).
func assertNonResultFallsBackToLlama(t *testing.T, response *httptest.ResponseRecorder,
	counters resultReadCounters, want resultReadExpectation) {
	if response.Code != want.status || counters.llamaReads.Load() != want.llamaReads ||
		counters.llamaQueueRequests.Load() != want.llamaQueueRequests ||
		counters.jobQueueProbes.Load() != want.jobQueueProbes {
		t.Fatalf("non-result path did not remain on llama-swap: status=%d llama=%d llama_queue=%d job_queue=%d",
			response.Code, counters.llamaReads.Load(), counters.llamaQueueRequests.Load(), counters.jobQueueProbes.Load())
	}
}

func TestPeerComfyUIResultReadsHonorPathGrantAndKeepPromptRouteOnLlamaSwap(t *testing.T) {
	var resultReads, promptRequests atomic.Int32
	resultBackend := newPeerResultReadBackend(t, &resultReads)
	llama := newPeerLlamaServer(t, &promptRequests, nil)
	llamaURL, err := url.Parse(llama.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, ownerHandler, requesterToken := newPeerOwnerProxyAndHandler(t, resultBackend, llamaURL)
	ownerPeer := &config.PeerConfig{
		NodeID: "bigboi", ExposedPathRoutes: map[string]string{"comfyui": "/comfyui"},
		Peers: map[string]config.PeerGrant{"udms": {Token: requesterToken, PathAccess: []string{"comfyui"}}},
	}
	owner := &Reloader{logger: discardLogger(), peerSlots: make(chan struct{}, 2), peerPathSlots: make(chan struct{}, 2)}
	owner.state.Store(&reloaderState{peer: ownerPeer, handler: ownerHandler, cacheTTL: time.Minute})
	ownerServer := httptest.NewTLSServer(owner.PeerHandler())
	defer ownerServer.Close()
	peerHTTPClient := ownerServer.Client()
	peer := &peerClient{
		id: "bigboi", origin: ownerServer.URL, token: requesterToken, client: peerHTTPClient,
		inferenceTransport: peerHTTPClient.Transport.(*http.Transport).Clone(),
		routePaths:         map[string]string{"/comfyui": "comfyui"},
	}
	requester := &Handler{
		logger: discardLogger(), defaultBackend: "llama", pathRoutes: []route{{prefix: "/comfyui", backendID: "llama"}},
		proxies:        map[string]*httputil.ReverseProxy{"llama": httputil.NewSingleHostReverseProxy(llamaURL)},
		peerPathRoutes: []peerPathRoute{{prefix: "/comfyui", peerID: "bigboi", service: "comfyui"}},
		peerPathSlots:  make(chan struct{}, 2), peerClients: map[string]*peerClient{"bigboi": peer},
	}

	request := httptest.NewRequest(http.MethodGet, "/comfyui/view?filename=output.png&type=output", nil)
	request.Header.Set("Authorization", "Bearer frontend-secret")
	request.Header.Set("Cookie", "session=frontend-secret")
	request.Header.Set("X-Caller-Secret", "frontend-secret")
	response := httptest.NewRecorder()
	requester.ServeHTTP(response, request)
	counters := peerReadCounters{resultReads: &resultReads, promptRequests: &promptRequests}
	assertPeerResultRead(t, peerReadObservation{
		status: response.Code, body: response.Body.String(), header: response.Header(),
	}, counters, peerReadExpectation{
		status: http.StatusOK, body: "owned output", checkBody: true, resultReads: 1,
	})

	write := httptest.NewRecorder()
	requester.ServeHTTP(write, httptest.NewRequest(http.MethodPost, "/comfyui/history/prompt-id", strings.NewReader("body")))
	assertPeerResultRead(t, peerReadObservation{
		status: write.Code, body: write.Body.String(), header: write.Header(),
	}, counters, peerReadExpectation{
		status: http.StatusMethodNotAllowed, resultReads: 1, checkPrompt: true,
	})

	hashView := httptest.NewRecorder()
	requester.ServeHTTP(hashView, httptest.NewRequest(http.MethodGet, "/comfyui/view?filename=blake3%3Aabc&type=output", nil))
	assertPeerResultRead(t, peerReadObservation{
		status: hashView.Code, body: hashView.Body.String(), header: hashView.Header(),
	}, counters, peerReadExpectation{status: http.StatusForbidden, resultReads: 1})

	prompt := httptest.NewRecorder()
	requester.ServeHTTP(prompt, httptest.NewRequest(http.MethodPost, "/comfyui/prompt", strings.NewReader(`{"prompt":{}}`)))
	assertPeerPromptFallback(t, prompt, &promptRequests, http.StatusOK, int32(1))

	ownerPeer.Peers["udms"] = config.PeerGrant{Token: requesterToken, PathAccess: []string{"unrelated-service"}}
	unauthorized := httptest.NewRecorder()
	requester.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/comfyui/view?filename=output.png&type=output", nil))
	assertPeerResultRead(t, peerReadObservation{
		status: unauthorized.Code, body: unauthorized.Body.String(), header: unauthorized.Header(),
	}, counters, peerReadExpectation{status: http.StatusForbidden, resultReads: 1})

	closePeerClients(map[string]*peerClient{"bigboi": peer})
}

func TestPeerComfyUIResultReadsUseOwnerTargetAndKeepPromptOnLlamaSwap(t *testing.T) {
	var resultReads, promptRequests, fallbackRequests atomic.Int32
	resultBackend := newPeerResultReadBackend(t, &resultReads)
	llama := newPeerLlamaServer(t, &promptRequests, &fallbackRequests)
	llamaURL, err := url.Parse(llama.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, ownerHandler, requesterToken := newPeerOwnerProxyAndHandler(t, resultBackend, llamaURL)
	ownerPeer := &config.PeerConfig{
		NodeID: "bigboi", ExposedPathRoutes: map[string]string{"comfyui": "/comfyui"},
		Peers: map[string]config.PeerGrant{"udms": {Token: requesterToken, PathAccess: []string{"comfyui"}}},
	}
	owner := &Reloader{logger: discardLogger(), peerSlots: make(chan struct{}, 2), peerPathSlots: make(chan struct{}, 2)}
	owner.state.Store(&reloaderState{peer: ownerPeer, handler: ownerHandler, cacheTTL: time.Minute})
	ownerServer := httptest.NewTLSServer(owner.PeerHandler())
	defer ownerServer.Close()

	peerHTTPClient := ownerServer.Client()
	outbound := &peerClient{
		id: "bigboi", origin: ownerServer.URL, token: requesterToken, client: peerHTTPClient,
		inferenceTransport: peerHTTPClient.Transport.(*http.Transport).Clone(),
		routePaths:         map[string]string{"/comfyui": "comfyui"},
	}
	requester := &Handler{
		logger: discardLogger(), defaultBackend: "llama", pathRoutes: []route{{prefix: "/comfyui", backendID: "llama"}},
		proxies:        map[string]*httputil.ReverseProxy{"llama": httputil.NewSingleHostReverseProxy(llamaURL)},
		peerPathRoutes: []peerPathRoute{{prefix: "/comfyui", peerID: "bigboi", service: "comfyui"}},
		peerPathSlots:  make(chan struct{}, 2), peerClients: map[string]*peerClient{"bigboi": outbound},
	}
	requesterServer := httptest.NewServer(requester)
	defer requesterServer.Close()

	request, err := http.NewRequest(http.MethodGet, requesterServer.URL+"/comfyui/view?filename=output.png&type=output", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer caller-secret")
	request.Header.Set("Cookie", "session=caller-secret")
	request.Header.Set("X-Caller-Secret", "caller-secret")
	response, err := requesterServer.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	counters := peerReadCounters{
		resultReads: &resultReads, fallbackRequests: &fallbackRequests, promptRequests: &promptRequests,
	}
	assertPeerResultRead(t, peerReadObservation{
		status: response.StatusCode, body: string(body), header: response.Header,
	}, counters, peerReadExpectation{
		status: http.StatusOK, body: "owned output", checkBody: true, resultReads: 1,
		checkFallback: true, fallbackRequests: 0,
	})

	writeResult := httptest.NewRecorder()
	requester.ServeHTTP(writeResult, httptest.NewRequest(http.MethodPost, "/comfyui/history/id", strings.NewReader("body")))
	assertPeerResultRead(t, peerReadObservation{
		status: writeResult.Code, body: writeResult.Body.String(), header: writeResult.Header(),
	}, counters, peerReadExpectation{
		status: http.StatusMethodNotAllowed, resultReads: 1, checkFallback: true,
	})

	prompt := httptest.NewRecorder()
	requester.ServeHTTP(prompt, httptest.NewRequest(http.MethodPost, "/comfyui/prompt", strings.NewReader(`{"prompt":{}}`)))
	assertPeerResultRead(t, peerReadObservation{
		status: prompt.Code, body: prompt.Body.String(), header: prompt.Header(),
	}, counters, peerReadExpectation{
		status: http.StatusOK, resultReads: 1, checkFallback: true, fallbackRequests: 1,
		checkPrompt: true, promptRequests: 1,
	})

	ownerPeer.Peers["udms"] = config.PeerGrant{Token: requesterToken, PathAccess: []string{"unrelated-service"}}
	unauthorized := httptest.NewRecorder()
	requester.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/comfyui/view?filename=output.png&type=output", nil))
	assertPeerResultRead(t, peerReadObservation{
		status: unauthorized.Code, body: unauthorized.Body.String(), header: unauthorized.Header(),
	}, counters, peerReadExpectation{status: http.StatusForbidden, resultReads: 1})
	closePeerClients(map[string]*peerClient{"bigboi": outbound})
}

type peerReadCounters struct {
	resultReads      *atomic.Int32
	fallbackRequests *atomic.Int32
	promptRequests   *atomic.Int32
}

type peerReadObservation struct {
	status int
	body   string
	header http.Header
}

type peerReadExpectation struct {
	status           int
	body             string
	checkBody        bool
	resultReads      int32
	checkFallback    bool
	fallbackRequests int32
	checkPrompt      bool
	promptRequests   int32
}

// assertPeerResultRead verifies a federated result-read response reached the
// owner result backend, that no Set-Cookie (caller credentials) leaked in the
// response, and that the request counters match expectations. Optional checks
// (body, fallback, prompt) are gated so the same helper covers the initial
// read, the write allowlist, the llama-swap prompt route, and the revoked-grant
// denial without adding control flow to the test function.
func assertPeerResultRead(t *testing.T, response peerReadObservation,
	counters peerReadCounters, want peerReadExpectation) {
	if response.status != want.status || (want.checkBody && response.body != want.body) ||
		response.header.Get("Set-Cookie") != "" || counters.resultReads.Load() != want.resultReads ||
		(want.checkFallback && counters.fallbackRequests.Load() != want.fallbackRequests) ||
		(want.checkPrompt && counters.promptRequests.Load() != want.promptRequests) {
		t.Fatalf("peer result read status=%d body=%q result=%d fallback=%d prompt=%d",
			response.status, response.body, counters.resultReads.Load(),
			counterValue(counters.fallbackRequests), counterValue(counters.promptRequests))
	}
}

func counterValue(counter *atomic.Int32) int32 {
	if counter == nil {
		return 0
	}
	return counter.Load()
}

// assertPeerPromptFallback verifies a prompt request remains routed through
// llama-swap and does not trigger a result read.
func assertPeerPromptFallback(t *testing.T, response *httptest.ResponseRecorder,
	promptRequests *atomic.Int32, wantStatus int, wantPrompt int32) {
	if response.Code != wantStatus || promptRequests.Load() != wantPrompt {
		t.Fatalf("prompt did not remain routed through llama-swap: status=%d prompts=%d",
			response.Code, promptRequests.Load())
	}
}

// newPeerResultReadBackend creates a ComfyUI result backend for peer result-read
// tests. It counts reads, verifies the request path/query and that caller
// credentials are stripped while owner credentials are applied, then writes
// "owned output". The server is cleaned up when the test ends.
func newPeerResultReadBackend(t *testing.T, resultReads *atomic.Int32) *httptest.Server {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		resultReads.Add(1)
		if req.Method != http.MethodGet || req.URL.Path != "/view" || req.URL.RawQuery != "filename=output.png&type=output" {
			t.Errorf("peer result request = %s %s?%s", req.Method, req.URL.Path, req.URL.RawQuery)
		}
		if req.Header.Get("Authorization") != "Bearer owner-result-key" || req.Header.Get("Cookie") != "" || req.Header.Get("X-Caller-Secret") != "" {
			t.Errorf("caller credentials reached owner result backend: %v", req.Header)
		}
		w.Header().Set("Set-Cookie", "owner-session=secret")
		_, _ = io.WriteString(w, "owned output")
	}))
	t.Cleanup(backend.Close)
	return backend
}

// newPeerLlamaServer creates a llama-swap backend for peer result-read tests.
// It counts prompt requests and, when fallbackRequests is non-nil, counts every
// request as a fallback. It writes "llama-swap". The server is cleaned up when
// the test ends.
func newPeerLlamaServer(t *testing.T, promptRequests *atomic.Int32, fallbackRequests *atomic.Int32) *httptest.Server {
	llama := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if fallbackRequests != nil {
			fallbackRequests.Add(1)
		}
		if req.Method == http.MethodPost && req.URL.Path == "/comfyui/prompt" {
			promptRequests.Add(1)
		}
		_, _ = io.WriteString(w, "llama-swap")
	}))
	t.Cleanup(llama.Close)
	return llama
}

// newPeerOwnerProxyAndHandler builds the result-read proxy and the owner handler
// for peer result-read tests. The owner handler routes result reads through the
// proxy and all other requests to the llama backend. It returns the proxy,
// handler, and requester bearer token.
func newPeerOwnerProxyAndHandler(t *testing.T, resultBackend *httptest.Server, llamaURL *url.URL) (*httputil.ReverseProxy, *Handler, string) {
	resultProxy, err := newComfyUIResultReadProxy(&config.ComfyUIResultReadConfig{
		URL: resultBackend.URL, APIKey: "owner-result-key",
	}, discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	ownerHandler := &Handler{
		logger: discardLogger(), pathRoutes: []route{{prefix: "/comfyui", backendID: "llama"}},
		proxies:                map[string]*httputil.ReverseProxy{"llama": httputil.NewSingleHostReverseProxy(llamaURL)},
		comfyUIResultReadProxy: resultProxy, comfyUIResultSlots: make(chan struct{}, 2),
	}
	return resultProxy, ownerHandler, strings.Repeat("r", 32)
}

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
		{"view input files denied", httptest.NewRequest(http.MethodGet, "/comfyui/view?filename=input.png&type=input", nil), true, http.StatusForbidden},
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
	for _, viaPeer := range []bool{false, true} {
		for _, endpoint := range []string{"/queue", "/prompt", "/system_stats", "/api/queue", "/api/prompt", "/api/system_stats", "/api/history/id", "/api/jobs?limit=20", "/api/jobs/id"} {
			requestPath := "/comfyui" + endpoint
			if viaPeer {
				requestPath = peerPathEndpoint
				if _, query, ok := strings.Cut(endpoint, "?"); ok {
					requestPath += "?" + query
				}
			}
			request := httptest.NewRequest(http.MethodGet, requestPath, nil)
			request.Header.Set("Authorization", "Bearer "+token)
			response := httptest.NewRecorder()
			if viaPeer {
				request.TLS = &tls.ConnectionState{}
				endpointPath, _, _ := strings.Cut(endpoint, "?")
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
	}
	if directReads.Load() != 18 || schedulerCalls.Load() != 0 {
		t.Fatalf("polling requests: direct=%d scheduler=%d", directReads.Load(), schedulerCalls.Load())
	}
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
	request := httptest.NewRequest(http.MethodGet, "/comfyui/view?filename=output.png&subfolder=run&type=output", nil)
	request.Header.Set("Authorization", "Bearer caller-secret")
	request.Header.Set("Cookie", "session=caller-secret")
	request.Header.Set("X-Caller-Secret", "caller-secret")
	request.Header.Set("Range", "bytes=0-3")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Body.String() != "image" || response.Header().Get("Set-Cookie") != "" ||
		response.Header().Get("Authorization") != "" || resultReads.Load() != 1 || llamaReads.Load() != 0 || jobQueueProbes.Load() != 0 {
		t.Fatalf("result-read response status=%d body=%q headers=%v result=%d llama=%d job_queue=%d",
			response.Code, response.Body.String(), response.Header(), resultReads.Load(), llamaReads.Load(), jobQueueProbes.Load())
	}
	repeat := httptest.NewRecorder()
	repeatRequest := httptest.NewRequest(http.MethodGet, "/comfyui/view?filename=output.png&subfolder=run&type=output", nil)
	repeatRequest.Header.Set("Range", "bytes=0-3")
	handler.ServeHTTP(repeat, repeatRequest)
	if repeat.Code != http.StatusOK || resultReads.Load() != 2 || llamaReads.Load() != 0 || jobQueueProbes.Load() != 0 {
		t.Fatalf("repeated result read acquired scheduler activity: status=%d result=%d llama=%d job_queue=%d",
			repeat.Code, resultReads.Load(), llamaReads.Load(), jobQueueProbes.Load())
	}

	invalidWrite := httptest.NewRecorder()
	handler.ServeHTTP(invalidWrite, httptest.NewRequest(http.MethodPost, "/comfyui/history/id", strings.NewReader("{}")))
	if invalidWrite.Code != http.StatusMethodNotAllowed || llamaReads.Load() != 0 {
		t.Fatalf("POST to result path fell through: status=%d llama=%d", invalidWrite.Code, llamaReads.Load())
	}
	otherRead := httptest.NewRecorder()
	handler.ServeHTTP(otherRead, httptest.NewRequest(http.MethodGet, "/comfyui/other", nil))
	if otherRead.Code != http.StatusOK || llamaReads.Load() != 1 || llamaQueueRequests.Load() != 0 || jobQueueProbes.Load() != 0 {
		t.Fatalf("non-result path did not remain on llama-swap: status=%d llama=%d llama_queue=%d job_queue=%d",
			otherRead.Code, llamaReads.Load(), llamaQueueRequests.Load(), jobQueueProbes.Load())
	}
}

func TestPeerComfyUIResultReadsHonorPathGrantAndKeepPromptRouteOnLlamaSwap(t *testing.T) {
	var resultReads, promptRequests atomic.Int32
	resultBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		resultReads.Add(1)
		if req.Method != http.MethodGet || req.URL.Path != "/view" || req.URL.RawQuery != "filename=output.png&type=output" {
			t.Errorf("peer result request = %s %s?%s", req.Method, req.URL.Path, req.URL.RawQuery)
		}
		if req.Header.Get("Authorization") != "Bearer owner-result-key" || req.Header.Get("Cookie") != "" || req.Header.Get("X-Caller-Secret") != "" {
			t.Errorf("caller credentials reached owner ComfyUI: %v", req.Header)
		}
		w.Header().Set("Set-Cookie", "owner-session=secret")
		_, _ = io.WriteString(w, "owned output")
	}))
	defer resultBackend.Close()
	llama := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost && req.URL.Path == "/comfyui/prompt" {
			promptRequests.Add(1)
		}
		_, _ = io.WriteString(w, "llama-swap")
	}))
	defer llama.Close()
	llamaURL, err := url.Parse(llama.URL)
	if err != nil {
		t.Fatal(err)
	}
	resultProxy, err := newComfyUIResultReadProxy(&config.ComfyUIResultReadConfig{URL: resultBackend.URL, APIKey: "owner-result-key"}, discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	ownerHandler := &Handler{
		logger: discardLogger(), pathRoutes: []route{{prefix: "/comfyui", backendID: "llama"}},
		proxies:                map[string]*httputil.ReverseProxy{"llama": httputil.NewSingleHostReverseProxy(llamaURL)},
		comfyUIResultReadProxy: resultProxy, comfyUIResultSlots: make(chan struct{}, 2),
	}
	requesterToken := strings.Repeat("r", 32)
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
	if response.Code != http.StatusOK || response.Body.String() != "owned output" || response.Header().Get("Set-Cookie") != "" || resultReads.Load() != 1 {
		t.Fatalf("federated result read status=%d body=%q headers=%v results=%d", response.Code, response.Body.String(), response.Header(), resultReads.Load())
	}

	write := httptest.NewRecorder()
	requester.ServeHTTP(write, httptest.NewRequest(http.MethodPost, "/comfyui/history/prompt-id", strings.NewReader("body")))
	if write.Code != http.StatusMethodNotAllowed || resultReads.Load() != 1 || promptRequests.Load() != 0 {
		t.Fatalf("federated history write bypassed result allowlist: status=%d results=%d prompts=%d", write.Code, resultReads.Load(), promptRequests.Load())
	}
	hashView := httptest.NewRecorder()
	requester.ServeHTTP(hashView, httptest.NewRequest(http.MethodGet, "/comfyui/view?filename=blake3%3Aabc&type=output", nil))
	if hashView.Code != http.StatusForbidden || resultReads.Load() != 1 {
		t.Fatalf("peer hash result bypassed identity boundary: status=%d reads=%d", hashView.Code, resultReads.Load())
	}
	prompt := httptest.NewRecorder()
	requester.ServeHTTP(prompt, httptest.NewRequest(http.MethodPost, "/comfyui/prompt", strings.NewReader(`{"prompt":{}}`)))
	if prompt.Code != http.StatusOK || promptRequests.Load() != 1 {
		t.Fatalf("prompt did not remain routed through llama-swap: status=%d prompts=%d", prompt.Code, promptRequests.Load())
	}
	ownerPeer.Peers["udms"] = config.PeerGrant{Token: requesterToken, PathAccess: []string{"unrelated-service"}}
	unauthorized := httptest.NewRecorder()
	requester.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/comfyui/view?filename=output.png&type=output", nil))
	if unauthorized.Code != http.StatusForbidden || resultReads.Load() != 1 {
		t.Fatalf("missing peer path grant did not deny result read: status=%d results=%d", unauthorized.Code, resultReads.Load())
	}
	closePeerClients(map[string]*peerClient{"bigboi": peer})
}

func TestPeerComfyUIResultReadsUseOwnerTargetAndKeepPromptOnLlamaSwap(t *testing.T) {
	var resultReads, promptRequests, fallbackRequests atomic.Int32
	resultBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
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
	defer resultBackend.Close()
	llama := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		fallbackRequests.Add(1)
		if req.Method == http.MethodPost && req.URL.Path == "/comfyui/prompt" {
			promptRequests.Add(1)
		}
		_, _ = io.WriteString(w, "llama-swap")
	}))
	defer llama.Close()
	llamaURL, err := url.Parse(llama.URL)
	if err != nil {
		t.Fatal(err)
	}
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
	requesterToken := strings.Repeat("r", 32)
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
	if response.StatusCode != http.StatusOK || string(body) != "owned output" || response.Header.Get("Set-Cookie") != "" ||
		resultReads.Load() != 1 || fallbackRequests.Load() != 0 {
		t.Fatalf("peer result read status=%d body=%q result=%d fallback=%d", response.StatusCode, body, resultReads.Load(), fallbackRequests.Load())
	}

	writeResult := httptest.NewRecorder()
	requester.ServeHTTP(writeResult, httptest.NewRequest(http.MethodPost, "/comfyui/history/id", strings.NewReader("body")))
	if writeResult.Code != http.StatusMethodNotAllowed || resultReads.Load() != 1 || fallbackRequests.Load() != 0 {
		t.Fatalf("peer result write escaped read allowlist: status=%d result=%d fallback=%d", writeResult.Code, resultReads.Load(), fallbackRequests.Load())
	}
	prompt := httptest.NewRecorder()
	requester.ServeHTTP(prompt, httptest.NewRequest(http.MethodPost, "/comfyui/prompt", strings.NewReader(`{"prompt":{}}`)))
	if prompt.Code != http.StatusOK || promptRequests.Load() != 1 || fallbackRequests.Load() != 1 {
		t.Fatalf("prompt did not remain on llama-swap path: status=%d prompts=%d fallback=%d", prompt.Code, promptRequests.Load(), fallbackRequests.Load())
	}
	ownerPeer.Peers["udms"] = config.PeerGrant{Token: requesterToken, PathAccess: []string{"unrelated-service"}}
	unauthorized := httptest.NewRecorder()
	requester.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/comfyui/view?filename=output.png&type=output", nil))
	if unauthorized.Code != http.StatusForbidden || resultReads.Load() != 1 {
		t.Fatalf("missing path_access did not fail closed: status=%d result=%d", unauthorized.Code, resultReads.Load())
	}
	closePeerClients(map[string]*peerClient{"bigboi": outbound})
}

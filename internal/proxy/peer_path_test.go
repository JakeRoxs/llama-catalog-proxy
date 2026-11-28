package proxy

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/catalog"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

type pathRequestObservation struct {
	Method        string
	Path          string
	Query         string
	Authorization string
	Cookie        string
	Origin        string
	Upgrade       string
	Body          string
}

// federationFixture holds the shared state for the federated ComfyUI path
// service integration test. Scenario helpers mutate it in sequence so the
// orchestration stays linear and each helper stays below complexity 15.
type federationFixture struct {
	ctx              context.Context
	observed         chan pathRequestObservation
	releaseWebSocket chan struct{}
	comfy            *httptest.Server
	ownerServer      *httptest.Server
	requesterServer  *httptest.Server
	owner            *Reloader
	ownerPeer        *config.PeerConfig
	outbound         *peerClient
	cache            *peerCache
	requester        *Handler
	directFallback   *atomic.Int32
}

func newFederationFixture(t *testing.T, ctx context.Context) *federationFixture {
	observed := make(chan pathRequestObservation, 2)
	releaseWebSocket := make(chan struct{})

	comfy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		observed <- pathRequestObservation{
			Method:        req.Method,
			Path:          req.URL.Path,
			Query:         req.URL.RawQuery,
			Authorization: req.Header.Get("Authorization"),
			Cookie:        req.Header.Get("Cookie"),
			Origin:        req.Header.Get("Origin"),
			Upgrade:       req.Header.Get("Upgrade"),
			Body:          string(body),
		}
		if websocketUpgrade(req.Header) {
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Error("backend response writer does not support hijacking")
				return
			}
			connection, rw, err := hijacker.Hijack()
			if err != nil {
				t.Errorf("hijack websocket: %v", err)
				return
			}
			key := req.Header.Get("Sec-WebSocket-Key")
			digest := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
			accept := base64.StdEncoding.EncodeToString(digest[:])
			_, _ = fmt.Fprintf(rw, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", accept)
			if err := rw.Flush(); err != nil {
				t.Errorf("flush websocket handshake: %v", err)
				_ = connection.Close()
				return
			}
			<-releaseWebSocket
			_ = connection.Close()
			return
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, "comfy accepted")
	}))
	comfyURL, err := url.Parse(comfy.URL)
	if err != nil {
		t.Fatal(err)
	}
	logger := discardLogger()
	backendConfig := config.Backend{URL: comfy.URL, APIKey: "owner-backend-credential"}
	ownerCatalog := catalog.New([]catalog.Backend{{ID: "comfyui", URL: comfyURL, Default: true}},
		comfy.Client(), time.Minute, time.Second, time.Second, logger)
	ownerHandler := &Handler{
		logger: logger, catalog: ownerCatalog,
		pathRoutes: []route{{prefix: "/comfyui/admin", backendID: "admin"}, {prefix: "/comfyui", backendID: "comfyui"}},
		proxies: map[string]*httputil.ReverseProxy{
			"comfyui": newReverseProxy(config.Config{}, logger, "comfyui", backendConfig, comfyURL, nil),
		},
		backendEndpoints: map[string]string{"comfyui": backendEndpointKey(comfyURL)},
	}
	requesterToken := strings.Repeat("r", 32)
	ownerPeer := &config.PeerConfig{
		NodeID: "bigboi", ExposedPathRoutes: map[string]string{"comfyui": "/comfyui"},
		Peers: map[string]config.PeerGrant{"udms": {
			Token: requesterToken, CatalogRead: true, PathAccess: []string{"comfyui"},
		}},
	}
	owner := &Reloader{
		logger: logger, instanceID: strings.Repeat("b", 32),
		peerSlots: make(chan struct{}, 2), peerPathSlots: make(chan struct{}, 2),
	}
	owner.state.Store(&reloaderState{
		peer: ownerPeer, handler: ownerHandler, generation: 1, cacheTTL: time.Minute,
	})
	ownerServer := httptest.NewTLSServer(owner.PeerHandler())
	ownerClient := ownerServer.Client()
	ownerTransport := ownerClient.Transport.(*http.Transport).Clone()
	outbound := &peerClient{
		id: "bigboi", endpoint: ownerServer.URL + "/peer/v1/report", origin: ownerServer.URL,
		token: requesterToken, client: ownerClient, inferenceTransport: ownerTransport,
		routePaths: map[string]string{"/comfyui": "comfyui"},
	}
	receipt, reason := outbound.fetch(ctx)
	if receipt == nil || reason != "" {
		t.Fatalf("fetch owner service report: receipt=%v reason=%q", receipt != nil, reason)
	}
	if !slices.Contains(receipt.Report.PathServices, "comfyui") {
		t.Fatalf("owner report omitted the exposed path service: %v", receipt.Report.PathServices)
	}
	cache := &peerCache{entries: map[string]peerCacheEntry{"bigboi": {}}}
	cache.update("bigboi", receipt, "", time.Now())

	var directFallback atomic.Int32
	fallbackProxy := httputil.NewSingleHostReverseProxy(comfyURL)
	requester := &Handler{
		logger: logger, defaultBackend: "fallback", maxJSONBody: 1 << 20,
		peerCache: cache, peerPathSlots: make(chan struct{}, 2),
		peerClients:    map[string]*peerClient{"bigboi": outbound},
		peerPathRoutes: []peerPathRoute{{prefix: "/comfyui", peerID: "bigboi", service: "comfyui"}},
		pathRoutes:     []route{{prefix: "/comfyui", backendID: "fallback"}},
		proxies: map[string]*httputil.ReverseProxy{"fallback": {
			Director:  func(req *http.Request) { directFallback.Add(1); fallbackProxy.Director(req) },
			Transport: fallbackProxy.Transport,
		}},
	}
	requesterServer := httptest.NewServer(requester)

	return &federationFixture{
		ctx:              ctx,
		observed:         observed,
		releaseWebSocket: releaseWebSocket,
		comfy:            comfy,
		ownerServer:      ownerServer,
		requesterServer:  requesterServer,
		owner:            owner,
		ownerPeer:        ownerPeer,
		outbound:         outbound,
		cache:            cache,
		requester:        requester,
		directFallback:   &directFallback,
	}
}

func (f *federationFixture) teardown() {
	f.comfy.Close()
	f.ownerServer.Close()
	f.requesterServer.Close()
}

func TestFederatedComfyUIPathServicePreservesHTTPAndWebSocket(t *testing.T) {
	ctx := t.Context()
	f := newFederationFixture(t, ctx)
	defer f.teardown()

	assertHTTPForwarding(t, f)
	assertWebSocketUpgrade(t, f)
	assertNestedOwnerRejected(t, f)
	assertGrantRevocation(t, f)
	assertPathOnlyAccess(t, f)
	assertRemovedService(t, f)
	assertOwnerOutage(t, f)
}

func TestFederatedComfyUIPromptUsesOwnerLeaseAdmission(t *testing.T) {
	f := newFederationFixture(t, t.Context())
	defer f.teardown()
	coordinator := newComfyUICoordinator(func(context.Context) (ComfyUIReleaseResult, error) {
		return confirmedCoordinatorEvidence(), nil
	})
	admit, err := newComfyUIDomainAdmission(coordinator, []string{"comfyui"}, "comfyui", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	f.owner.state.Load().handler.(*Handler).backendAdmission = admit

	for _, path := range []string{"/comfyui/prompt", "/comfyui/api/prompt"} {
		w := httptest.NewRecorder()
		f.requester.ServeHTTP(w, httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"prompt":{}}`)))
		if w.Code != http.StatusCreated || w.Body.String() != "comfy accepted" {
			t.Fatalf("peer POST %s: status=%d body=%q", path, w.Code, w.Body.String())
		}
		if state := coordinator.snapshot(); state.State != "active" || state.Submissions != 0 {
			t.Fatalf("peer POST %s did not activate and finish its lease reservation: %+v", path, state)
		}
	}
	if f.directFallback.Load() != 0 {
		t.Fatal("peer prompt reached the requester fallback")
	}
}

// assertHTTPForwarding verifies the HTTP peer path forwards the exact request
// semantics (method, path, query, body, Origin preserved; Cookie stripped;
// Authorization rewritten to the owner backend key) and does not fall back.
func assertHTTPForwarding(t *testing.T, f *federationFixture) {
	request, err := http.NewRequest(http.MethodPost,
		f.requesterServer.URL+"/comfyui/prompt?clientId=peer-test&overwrite=true",
		strings.NewReader(`{"prompt":{"node":"workflow"}}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer frontend-user-secret")
	request.Header.Set("Cookie", "session=frontend-secret")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Origin", "https://frontend.example")
	response, err := f.requesterServer.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	responseBody, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	first := <-f.observed
	if response.StatusCode != http.StatusCreated || string(responseBody) != "comfy accepted" ||
		first.Method != http.MethodPost || first.Path != "/comfyui/prompt" ||
		first.Query != "clientId=peer-test&overwrite=true" || first.Body != `{"prompt":{"node":"workflow"}}` ||
		first.Origin != "https://frontend.example" || first.Cookie != "" || first.Authorization != "Bearer owner-backend-credential" ||
		f.directFallback.Load() != 0 {
		t.Fatalf("peer path request lost semantics or bypassed federation: status=%d body=%q observed=%+v fallback=%d",
			response.StatusCode, responseBody, first, f.directFallback.Load())
	}
}

// assertWebSocketUpgrade verifies the raw WebSocket upgrade through the peer
// route, the handshake headers, the SHA-1 accept, and the channel release
// ordering that keeps the backend goroutine from leaking.
func assertWebSocketUpgrade(t *testing.T, f *federationFixture) {
	connection, err := net.Dial("tcp", strings.TrimPrefix(f.requesterServer.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	_ = connection.SetReadDeadline(time.Now().Add(5 * time.Second))

	key := base64.StdEncoding.EncodeToString(make([]byte, 16))
	_, err = fmt.Fprintf(connection, "GET /comfyui/ws?clientId=ws-peer HTTP/1.1\r\nHost: %s\r\nConnection: keep-alive, Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\nOrigin: https://frontend.example\r\n\r\n",
		strings.TrimPrefix(f.requesterServer.URL, "http://"), key)
	if err != nil {
		_ = connection.Close()
		t.Fatal(err)
	}

	websocketResponse, err := http.ReadResponse(bufio.NewReader(connection), &http.Request{Method: http.MethodGet})
	if err != nil {
		_ = connection.Close()
		close(f.releaseWebSocket)
		t.Fatalf("read websocket response through peer route: %v", err)
	}
	_ = websocketResponse.Body.Close()
	ws := <-f.observed
	_ = connection.Close()
	close(f.releaseWebSocket)

	if websocketResponse.StatusCode != http.StatusSwitchingProtocols || ws.Path != "/comfyui/ws" ||
		ws.Query != "clientId=ws-peer" || ws.Origin != "https://frontend.example" || ws.Upgrade != "websocket" ||
		ws.Authorization != "Bearer owner-backend-credential" || ws.Cookie != "" || f.directFallback.Load() != 0 {
		t.Fatalf("websocket peer route failed: status=%d observed=%+v fallback=%d", websocketResponse.StatusCode, ws, f.directFallback.Load())
	}
}

// assertNestedOwnerRejected verifies a nested owner route (/comfyui/admin)
// escapes its owner-side prefix and is rejected with 503, without falling back.
func assertNestedOwnerRejected(t *testing.T, f *federationFixture) {
	nested, err := http.NewRequest(http.MethodGet, f.requesterServer.URL+"/comfyui/admin/secret", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := f.requesterServer.Client().Do(nested)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable || f.directFallback.Load() != 0 {
		t.Fatalf("exposed path route escaped its owner-side prefix: status=%d fallback=%d", response.StatusCode, f.directFallback.Load())
	}
}

// assertGrantRevocation verifies revoking PathAccess rejects the path with 401
// and does not fall back.
func assertGrantRevocation(t *testing.T, f *federationFixture) {
	f.ownerPeer.Peers["udms"] = config.PeerGrant{Token: f.ownerPeer.Peers["udms"].Token, CatalogRead: true}
	denied, err := http.NewRequest(http.MethodPost, f.requesterServer.URL+"/comfyui/prompt", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	denied.Header.Set("Content-Type", "application/json")
	response, err := f.requesterServer.Client().Do(denied)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized || f.directFallback.Load() != 0 {
		t.Fatalf("ungranted path access was accepted or fell back: status=%d fallback=%d", response.StatusCode, f.directFallback.Load())
	}
}

// assertPathOnlyAccess verifies restoring PathAccess (without CatalogRead) and
// clearing the peer cache still allows path access, preserving the body.
func assertPathOnlyAccess(t *testing.T, f *federationFixture) {
	f.ownerPeer.Peers["udms"] = config.PeerGrant{Token: f.ownerPeer.Peers["udms"].Token, PathAccess: []string{"comfyui"}}
	f.requester.peerCache = nil
	pathOnly, err := http.NewRequest(http.MethodPost, f.requesterServer.URL+"/comfyui/prompt", strings.NewReader("path-only access"))
	if err != nil {
		t.Fatal(err)
	}
	pathOnly.Header.Set("Content-Type", "text/plain")
	response, err := f.requesterServer.Client().Do(pathOnly)
	if err != nil {
		t.Fatal(err)
	}
	pathOnlyBody, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	pathOnlyObservation := <-f.observed
	if response.StatusCode != http.StatusCreated || string(pathOnlyBody) != "comfy accepted" ||
		pathOnlyObservation.Body != "path-only access" || f.directFallback.Load() != 0 {
		t.Fatalf("path access incorrectly depended on catalog_read: status=%d body=%q observed=%+v",
			response.StatusCode, pathOnlyBody, pathOnlyObservation)
	}
}

// assertRemovedService verifies removing the exposed path service yields 503
// and does not fall back.
func assertRemovedService(t *testing.T, f *federationFixture) {
	f.ownerPeer.ExposedPathRoutes = nil
	removedService, err := http.NewRequest(http.MethodGet, f.requesterServer.URL+"/comfyui/view?filename=one.png", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := f.requesterServer.Client().Do(removedService)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable || f.directFallback.Load() != 0 {
		t.Fatalf("removed owner service fell back locally: status=%d fallback=%d", response.StatusCode, f.directFallback.Load())
	}
}

// assertOwnerOutage verifies an unavailable owner peer path yields 502 and
// does not fall back to a local route.
func assertOwnerOutage(t *testing.T, f *federationFixture) {
	f.ownerServer.Close()
	unavailable, err := http.NewRequest(http.MethodGet, f.requesterServer.URL+"/comfyui/view?filename=one.png", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := f.requesterServer.Client().Do(unavailable)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusBadGateway || f.directFallback.Load() != 0 {
		t.Fatalf("unavailable peer path fell back to a local route: status=%d fallback=%d", response.StatusCode, f.directFallback.Load())
	}
}

func TestValidPeerPathRequest(t *testing.T) {
	encode := func(p string) string {
		return base64.RawURLEncoding.EncodeToString([]byte(p))
	}
	tests := []struct {
		name        string
		encodedPath string
		service     string
		localPrefix string
		method      string
		wantPath    string
		wantError   bool
	}{
		{
			name:        "normal path",
			encodedPath: encode("/comfyui/prompt"),
			service:     "comfyui",
			localPrefix: "/comfyui",
			method:      http.MethodGet,
			wantPath:    "/comfyui/prompt",
		},
		{
			name:        "normal path with nested segments",
			encodedPath: encode("/comfyui/admin/secret"),
			service:     "comfyui",
			localPrefix: "/comfyui",
			method:      http.MethodPost,
			wantPath:    "/comfyui/admin/secret",
		},
		{
			name:        "comfyui frontend root GET exception",
			encodedPath: encode("/comfyui/"),
			service:     "comfyui",
			localPrefix: "/comfyui",
			method:      http.MethodGet,
			wantPath:    "/comfyui/",
		},
		{
			name:        "comfyui frontend root only excepted for GET",
			encodedPath: encode("/comfyui/"),
			service:     "comfyui",
			localPrefix: "/comfyui",
			method:      http.MethodPost,
			wantError:   true,
		},
		{
			name:        "invalid base64",
			encodedPath: "not valid base64!!",
			service:     "comfyui",
			localPrefix: "/comfyui",
			method:      http.MethodGet,
			wantError:   true,
		},
		{
			name:        "empty decoded path",
			encodedPath: encode(""),
			service:     "comfyui",
			localPrefix: "/comfyui",
			method:      http.MethodGet,
			wantError:   true,
		},
		{
			name:        "invalid percent escape",
			encodedPath: encode("%ZZ"),
			service:     "comfyui",
			localPrefix: "/comfyui",
			method:      http.MethodGet,
			wantError:   true,
		},
		{
			name:        "disallowed query",
			encodedPath: encode("/comfyui/prompt?x=1"),
			service:     "comfyui",
			localPrefix: "/comfyui",
			method:      http.MethodGet,
			wantError:   true,
		},
		{
			name:        "disallowed fragment",
			encodedPath: encode("/comfyui/prompt#x"),
			service:     "comfyui",
			localPrefix: "/comfyui",
			method:      http.MethodGet,
			wantError:   true,
		},
		{
			name:        "disallowed backslash",
			encodedPath: encode("/comfyui/prompt\\x"),
			service:     "comfyui",
			localPrefix: "/comfyui",
			method:      http.MethodGet,
			wantError:   true,
		},
		{
			name:        "path traversal",
			encodedPath: encode("/comfyui/../etc/passwd"),
			service:     "comfyui",
			localPrefix: "/comfyui",
			method:      http.MethodGet,
			wantError:   true,
		},
		{
			name:        "out of prefix",
			encodedPath: encode("/other/path"),
			service:     "comfyui",
			localPrefix: "/comfyui",
			method:      http.MethodGet,
			wantError:   true,
		},
		{
			name:        "non-root prefix out of prefix",
			encodedPath: encode("/comfyui-admin/x"),
			service:     "comfyui",
			localPrefix: "/comfyui",
			method:      http.MethodGet,
			wantError:   true,
		},
		{
			name:        "missing leading slash",
			encodedPath: encode("comfyui/prompt"),
			service:     "comfyui",
			localPrefix: "/comfyui",
			method:      http.MethodGet,
			wantError:   true,
		},
		{
			name:        "empty path",
			encodedPath: encode("/"),
			service:     "comfyui",
			localPrefix: "/comfyui",
			method:      http.MethodGet,
			wantError:   true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertPeerPathRequest(t, test.encodedPath, test.service, test.localPrefix, test.method, test.wantPath, test.wantError)
		})
	}
}

func assertPeerPathRequest(t *testing.T, encodedPath, service, localPrefix, method, wantPath string, wantError bool) {
	t.Helper()
	gotPath, gotEscaped, err := validPeerPathRequest(encodedPath, service, localPrefix, method)
	if wantError {
		if err == nil {
			t.Fatalf("validPeerPathRequest(%q) = %q, %q, nil, want error", encodedPath, gotPath, gotEscaped)
		}
		return
	}
	if err != nil {
		t.Fatalf("validPeerPathRequest(%q) = %q, %q, %v, want %q", encodedPath, gotPath, gotEscaped, err, wantPath)
	}
	if gotPath != wantPath {
		t.Errorf("validPeerPathRequest(%q) = %q, want %q", encodedPath, gotPath, wantPath)
	}
	decoded, err := base64.RawURLEncoding.DecodeString(encodedPath)
	if err != nil {
		t.Fatalf("decode test input %q: %v", encodedPath, err)
	}
	if string(decoded) != gotEscaped {
		t.Errorf("validPeerPathRequest(%q) escapedPath = %q, want %q", encodedPath, gotEscaped, decoded)
	}
}

func TestPathPrefixMatchesBoundaries(t *testing.T) {
	for _, test := range []struct {
		path string
		want bool
	}{
		{path: "/comfyui", want: true},
		{path: "/comfyui/prompt", want: true},
		{path: "/comfyui-admin", want: false},
	} {
		if got := pathHasPrefix(test.path, "/comfyui"); got != test.want {
			t.Errorf("pathHasPrefix(%q) = %v, want %v", test.path, got, test.want)
		}
	}
}

func TestBuildPeerPathRoutesSortsMostSpecificPrefixFirst(t *testing.T) {
	peer := &config.PeerConfig{Peers: map[string]config.PeerGrant{
		"bigboi": {RoutePaths: map[string]string{"/comfyui": "comfyui", "/comfyui/admin": "admin"}},
	}}
	routes := buildPeerPathRoutes(peer)
	if len(routes) != 2 || routes[0].prefix != "/comfyui/admin" || routes[1].prefix != "/comfyui" {
		t.Fatalf("peer path routes were not most-specific-first: %+v", routes)
	}
}

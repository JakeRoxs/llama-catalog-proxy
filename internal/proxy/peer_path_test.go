package proxy

import (
	"bufio"
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

func TestFederatedComfyUIPathServicePreservesHTTPAndWebSocket(t *testing.T) {
	observed := make(chan pathRequestObservation, 2)
	releaseWebSocket := make(chan struct{})
	comfy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		observed <- pathRequestObservation{
			Method: req.Method, Path: req.URL.Path, Query: req.URL.RawQuery,
			Authorization: req.Header.Get("Authorization"), Cookie: req.Header.Get("Cookie"),
			Origin: req.Header.Get("Origin"), Upgrade: req.Header.Get("Upgrade"), Body: string(body),
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
	defer comfy.Close()

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
	defer ownerServer.Close()

	ownerClient := ownerServer.Client()
	ownerTransport := ownerClient.Transport.(*http.Transport).Clone()
	outbound := &peerClient{
		id: "bigboi", endpoint: ownerServer.URL + "/peer/v1/report", origin: ownerServer.URL,
		token: requesterToken, client: ownerClient, inferenceTransport: ownerTransport,
		routePaths: map[string]string{"/comfyui": "comfyui"},
	}
	receipt, reason := outbound.fetch(t.Context())
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
	defer requesterServer.Close()

	request, err := http.NewRequest(http.MethodPost,
		requesterServer.URL+"/comfyui/prompt?clientId=peer-test&overwrite=true",
		strings.NewReader(`{"prompt":{"node":"workflow"}}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer frontend-user-secret")
	request.Header.Set("Cookie", "session=frontend-secret")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Origin", "https://frontend.example")
	response, err := requesterServer.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	responseBody, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	first := <-observed
	if response.StatusCode != http.StatusCreated || string(responseBody) != "comfy accepted" ||
		first.Method != http.MethodPost || first.Path != "/comfyui/prompt" ||
		first.Query != "clientId=peer-test&overwrite=true" || first.Body != `{"prompt":{"node":"workflow"}}` ||
		first.Origin != "https://frontend.example" || first.Cookie != "" || first.Authorization != "Bearer owner-backend-credential" ||
		directFallback.Load() != 0 {
		t.Fatalf("peer path request lost semantics or bypassed federation: status=%d body=%q observed=%+v fallback=%d",
			response.StatusCode, responseBody, first, directFallback.Load())
	}

	connection, err := net.Dial("tcp", strings.TrimPrefix(requesterServer.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	key := "dGhlIHNhbXBsZSBub25jZQ=="
	_, err = fmt.Fprintf(connection, "GET /comfyui/ws?clientId=ws-peer HTTP/1.1\r\nHost: %s\r\nConnection: keep-alive, Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\nOrigin: https://frontend.example\r\n\r\n",
		strings.TrimPrefix(requesterServer.URL, "http://"), key)
	if err != nil {
		_ = connection.Close()
		t.Fatal(err)
	}
	_ = connection.SetReadDeadline(time.Now().Add(5 * time.Second))
	websocketResponse, err := http.ReadResponse(bufio.NewReader(connection), &http.Request{Method: http.MethodGet})
	if err != nil {
		_ = connection.Close()
		close(releaseWebSocket)
		t.Fatalf("read websocket response through peer route: %v", err)
	}
	_ = websocketResponse.Body.Close()
	ws := <-observed
	_ = connection.Close()
	close(releaseWebSocket)
	if websocketResponse.StatusCode != http.StatusSwitchingProtocols || ws.Path != "/comfyui/ws" ||
		ws.Query != "clientId=ws-peer" || ws.Origin != "https://frontend.example" || ws.Upgrade != "websocket" ||
		ws.Authorization != "Bearer owner-backend-credential" || ws.Cookie != "" || directFallback.Load() != 0 {
		t.Fatalf("websocket peer route failed: status=%d observed=%+v fallback=%d", websocketResponse.StatusCode, ws, directFallback.Load())
	}

	nested, err := http.NewRequest(http.MethodGet, requesterServer.URL+"/comfyui/admin/secret", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err = requesterServer.Client().Do(nested)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable || directFallback.Load() != 0 {
		t.Fatalf("exposed path route escaped its owner-side prefix: status=%d fallback=%d", response.StatusCode, directFallback.Load())
	}

	ownerPeer.Peers["udms"] = config.PeerGrant{Token: requesterToken, CatalogRead: true}
	denied, err := http.NewRequest(http.MethodPost, requesterServer.URL+"/comfyui/prompt", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	denied.Header.Set("Content-Type", "application/json")
	response, err = requesterServer.Client().Do(denied)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized || directFallback.Load() != 0 {
		t.Fatalf("ungranted path access was accepted or fell back: status=%d fallback=%d", response.StatusCode, directFallback.Load())
	}

	ownerPeer.Peers["udms"] = config.PeerGrant{Token: requesterToken, PathAccess: []string{"comfyui"}}
	requester.peerCache = nil
	pathOnly, err := http.NewRequest(http.MethodPost, requesterServer.URL+"/comfyui/prompt", strings.NewReader("path-only access"))
	if err != nil {
		t.Fatal(err)
	}
	pathOnly.Header.Set("Content-Type", "text/plain")
	response, err = requesterServer.Client().Do(pathOnly)
	if err != nil {
		t.Fatal(err)
	}
	pathOnlyBody, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	pathOnlyObservation := <-observed
	if response.StatusCode != http.StatusCreated || string(pathOnlyBody) != "comfy accepted" ||
		pathOnlyObservation.Body != "path-only access" || directFallback.Load() != 0 {
		t.Fatalf("path access incorrectly depended on catalog_read: status=%d body=%q observed=%+v",
			response.StatusCode, pathOnlyBody, pathOnlyObservation)
	}

	ownerPeer.ExposedPathRoutes = nil
	removedService, err := http.NewRequest(http.MethodGet, requesterServer.URL+"/comfyui/view?filename=one.png", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err = requesterServer.Client().Do(removedService)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable || directFallback.Load() != 0 {
		t.Fatalf("removed owner service fell back locally: status=%d fallback=%d", response.StatusCode, directFallback.Load())
	}

	ownerServer.Close()
	unavailable, err := http.NewRequest(http.MethodGet, requesterServer.URL+"/comfyui/view?filename=one.png", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err = requesterServer.Client().Do(unavailable)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusBadGateway || directFallback.Load() != 0 {
		t.Fatalf("unavailable peer path fell back to a local route: status=%d fallback=%d", response.StatusCode, directFallback.Load())
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

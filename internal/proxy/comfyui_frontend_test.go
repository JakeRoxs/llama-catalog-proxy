package proxy

import (
	"bufio"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

func comfyUIWebSocketTestKey() string {
	return base64.StdEncoding.EncodeToString(make([]byte, 16))
}

// comfyUIFrontendEndpoints is the exact set of ComfyUI frontend asset and API
// endpoints exercised by the HTTP and WebSocket bypass scheduler test. The
// result backend echoes each request URI verbatim, so the response body must
// equal the endpoint entry itself.
var comfyUIFrontendEndpoints = []string{
	"/assets/app.js?v=1", "/extensions/custom/ui.js", "/api/features", "/object_info",
	"/api/i18n", "/api/global_subgraphs", "/api/node_replacements",
	"/api/experiment/models", "/api/experiment/models/checkpoints",
	"/api/global_subgraphs/" + strings.Repeat("a", 64),
	"/api/jobs?status=completed,failed,cancelled&limit=64&offset=0",
	"/user.css", "/materialdesignicons.min.css", "/fonts/materialdesignicons-webfont.woff2?v=7.4.47",
	"/api/users", "/api/settings", "/api/userdata/user.css", "/api/userdata/workflows%2Fexample.json",
	"/api/userdata?dir=workflows&recurse=true&full_info=true", "/api/v2/userdata?path=workflows%2Fproject",
	"/templates/index.json", "/workflow_templates",
}

func TestComfyUIFrontendAllowlist(t *testing.T) {
	assertSubgraphRequestsRejected(t, []string{
		"/api/global_subgraphs/not-a-hash",
		"/api/global_subgraphs/" + strings.Repeat("g", 64),
		"/api/global_subgraphs/" + strings.Repeat("a", 64) + "?path=other",
	})
	assertFrontendRoutesAllowed(t, []string{
		"/comfyui", "/comfyui/", "/comfyui/index.html", "/comfyui/assets/app.js?v=1",
		"/comfyui/scripts/app.js", "/comfyui/extensions/custom/ui.js", "/comfyui/assets/style.css",
		"/comfyui/assets/font.woff2", "/comfyui/favicon.ico", "/comfyui/features",
		"/comfyui/api/features", "/comfyui/extensions", "/comfyui/embeddings",
		"/comfyui/models/checkpoints", "/comfyui/object_info", "/comfyui/api/object_info/LoadImage",
		"/comfyui/experiment/models", "/comfyui/experiment/models/checkpoints",
	})
	assertUnsafeFrontendRoutesRejected(t, []string{
		"/comfyui/assets/../secret.js", "/comfyui/assets/.secret.js", "/comfyui/assets/file.py",
		"/comfyui/models/folder/extra", "/comfyui/features?user_id=other", "/comfyui/assets%2fapp.js",
		"/comfyui/api/experiment/models?dir=other", "/comfyui/api/experiment/models/checkpoints%2fother",
	})
	assertNonFrontendRoutesRejected(t, []string{
		"/comfyui/prompt", "/comfyui/custom/load_model", "/comfyui/health", "/comfyui/free",
		"/settings", "/api/settings", "/userdata/workflows%2Fexample.json",
		"/comfyui/api/experiment/models/preview/checkpoints/0/model.webp",
	})
	assertGenerationBypassRejected(t)
	assertWebSocketAllowed(t)
	assertWebSocketDuplicateClientIDsRejected(t)
}

func assertSubgraphRequestsRejected(t *testing.T, endpoints []string) {
	t.Helper()
	for _, endpoint := range endpoints {
		matched, status, _ := validateComfyUIFrontendRequest(httptest.NewRequest(http.MethodGet, "/comfyui"+endpoint, nil))
		if !matched || status != http.StatusBadRequest {
			t.Fatalf("subgraph request %s: matched=%v status=%d", endpoint, matched, status)
		}
	}
}

func assertFrontendRoutesAllowed(t *testing.T, endpoints []string) {
	t.Helper()
	for _, endpoint := range endpoints {
		matched, status, _ := validateComfyUIFrontendRequest(httptest.NewRequest(http.MethodGet, endpoint, nil))
		if !matched || status != 0 {
			t.Fatalf("frontend route %s: matched=%v status=%d", endpoint, matched, status)
		}
	}
}

func assertUnsafeFrontendRoutesRejected(t *testing.T, endpoints []string) {
	t.Helper()
	for _, endpoint := range endpoints {
		matched, status, _ := validateComfyUIFrontendRequest(httptest.NewRequest(http.MethodGet, endpoint, nil))
		if !matched || status < 400 {
			t.Fatalf("unsafe frontend route %s: matched=%v status=%d", endpoint, matched, status)
		}
	}
}

func assertNonFrontendRoutesRejected(t *testing.T, endpoints []string) {
	t.Helper()
	for _, endpoint := range endpoints {
		if matched, _, _ := validateComfyUIFrontendRequest(httptest.NewRequest(http.MethodGet, endpoint, nil)); matched {
			t.Fatalf("non-frontend route classified: %s", endpoint)
		}
	}
}

func assertGenerationBypassRejected(t *testing.T) {
	t.Helper()
	if matched, _, _ := validateComfyUIFrontendRequest(httptest.NewRequest(http.MethodPost, "/comfyui/api/prompt", strings.NewReader(`{}`))); matched {
		t.Fatal("generation matched frontend bypass")
	}
}

func assertWebSocketAllowed(t *testing.T) {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, "/comfyui/ws?clientId=frontend", nil)
	request.Header.Set("Connection", "Upgrade")
	request.Header.Set("Upgrade", "websocket")
	request.Header.Set("Sec-WebSocket-Key", comfyUIWebSocketTestKey())
	request.Header.Set("Sec-WebSocket-Version", "13")
	if matched, status, _ := validateComfyUIFrontendRequest(request); !matched || status != 0 {
		t.Fatalf("valid websocket denied: %v %d", matched, status)
	}
}

func assertWebSocketDuplicateClientIDsRejected(t *testing.T) {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, "/comfyui/ws?clientId=frontend", nil)
	request.Header.Set("Connection", "Upgrade")
	request.Header.Set("Upgrade", "websocket")
	request.Header.Set("Sec-WebSocket-Key", comfyUIWebSocketTestKey())
	request.Header.Set("Sec-WebSocket-Version", "13")
	request.URL.RawQuery = "clientId=a&clientId=b"
	if _, status, _ := validateComfyUIFrontendRequest(request); status != http.StatusBadRequest {
		t.Fatalf("duplicate WebSocket client IDs accepted: %d", status)
	}
}

func TestComfyUIUserAndWorkflowReadAllowlist(t *testing.T) {
	for _, endpoint := range []string{
		"/comfyui/user.css", "/comfyui/materialdesignicons.min.css", "/comfyui/fonts/materialdesignicons-webfont.woff2?v=7.4.47",
		"/comfyui/api/users", "/comfyui/settings", "/comfyui/api/settings/Comfy.Workflow.WorkflowTabsPosition",
		"/comfyui/api/userdata/user.css", "/comfyui/api/userdata/workflows%2Fexample.json",
		"/comfyui/api/userdata?dir=workflows&recurse=true&split=false&full_info=true",
		"/comfyui/api/v2/userdata", "/comfyui/api/v2/userdata?path=workflows%2Fproject",
		"/comfyui/workflow_templates", "/comfyui/templates/index.json", "/comfyui/templates/example.json",
	} {
		matched, status, _ := validateComfyUIFrontendRequest(httptest.NewRequest(http.MethodGet, endpoint, nil))
		if !matched || status != 0 {
			t.Fatalf("verified frontend dependency %s: matched=%v status=%d", endpoint, matched, status)
		}
	}
	for _, endpoint := range []string{
		"/comfyui/api/userdata/workflows%2F..%2Fsecret.json",
		"/comfyui/api/userdata/workflows%252Fsecret.json",
		"/comfyui/api/userdata/.credentials.json", "/comfyui/api/userdata/credentials.pem",
		"/comfyui/api/userdata?dir=workflows&dir=other", "/comfyui/api/userdata?dir=../other",
		"/comfyui/api/userdata?dir=workflows&user_id=other", "/comfyui/api/userdata?dir=workflows&recurse=1",
		"/comfyui/api/v2/userdata?path=%252e%252e", "/comfyui/api/settings?user_id=other",
		"/comfyui/api/userdata/workflows%2Fexample.json?user_id=other",
	} {
		matched, status, _ := validateComfyUIFrontendRequest(httptest.NewRequest(http.MethodGet, endpoint, nil))
		if !matched || status < 400 {
			t.Fatalf("unsafe frontend dependency %s: matched=%v status=%d", endpoint, matched, status)
		}
	}
	for _, endpoint := range []string{"/comfyui/api/users", "/comfyui/api/settings", "/comfyui/api/userdata/workflows%2Fexample.json"} {
		for _, method := range []string{http.MethodPost, http.MethodDelete} {
			if matched, _, _ := validateComfyUIFrontendRequest(httptest.NewRequest(method, endpoint, strings.NewReader(`{}`))); matched {
				t.Fatalf("mutation included in read bypass: %s %s", method, endpoint)
			}
		}
	}
}

// newComfyUIFrontendBypassTestBackend returns an httptest server that echoes
// request URIs and performs the WebSocket handshake. It verifies owner
// credentials are present on every request and are not leaked to the result
// backend as a cookie or header.
func newComfyUIFrontendBypassTestBackend(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Authorization") != "Bearer owner-key" || req.Header.Get("Cookie") != "" || req.Header.Get("Comfy-User") != "owner-user" {
			t.Errorf("frontend credentials leaked or owner credential missing: %v", req.Header)
		}
		if req.URL.Path != "/ws" {
			if req.URL.Path == "/assets/missing.js" {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			_, _ = io.WriteString(w, req.URL.RequestURI())
			return
		}
		if !websocketUpgrade(req.Header) || req.Header.Get("Sec-WebSocket-Version") != "13" {
			t.Error("WebSocket upgrade headers lost")
			http.Error(w, "bad upgrade", http.StatusBadRequest)
			return
		}
		connection, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		defer connection.Close()
		_ = connection.SetDeadline(time.Now().Add(5 * time.Second))
		digest := sha1.Sum([]byte(req.Header.Get("Sec-WebSocket-Key") + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
		_, _ = fmt.Fprintf(rw, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: %s\r\n\r\n", base64.StdEncoding.EncodeToString(digest[:]))
		_ = rw.Flush()
		frame := make([]byte, 8)
		if _, err := io.ReadFull(rw, frame); err != nil {
			t.Errorf("read masked frame: %v", err)
			return
		}
		if string(frame) != "\x81\x82\x00\x00\x00\x00hi" {
			t.Errorf("WebSocket frame changed: %q", frame)
		}
		_, _ = rw.WriteString("\x81\x02ok")
		_ = rw.Flush()
	}))
}

// runComfyUIFrontendBypassPhase exercises one bypass phase (local or peer),
// verifying endpoints, root, asset fallback, and WebSocket, then closes the server.
func runComfyUIFrontendBypassPhase(t *testing.T, handler *Handler, owner *Reloader, viaPeer bool, token string, server *httptest.Server, schedulerCalls *atomic.Int32) {
	t.Helper()
	assertComfyUIFrontendEndpoints(t, server, comfyUIFrontendEndpoints, viaPeer)
	assertComfyUIFrontendRoot(t, server, schedulerCalls, viaPeer)
	assertComfyUIFrontendAssetUnavailable(t, server, schedulerCalls)
	assertComfyUIFrontendWebSocket(t, server, viaPeer)
	server.Close()
}

func TestComfyUIFrontendHTTPAndWebSocketBypassScheduler(t *testing.T) {
	var schedulerCalls atomic.Int32
	backend := newComfyUIFrontendBypassTestBackend(t)
	defer backend.Close()
	scheduler := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		schedulerCalls.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer scheduler.Close()
	cfg := baseConfig(scheduler.URL)
	cfg.PathRoutes = map[string]string{"/comfyui": "default"}
	cfg.Peer = &config.PeerConfig{NodeID: "owner", Listen: "127.0.0.1:9294", TLSCertFile: "cert.pem", TLSKeyFile: "key.pem",
		ComfyUIResultRead: &config.ComfyUIResultReadConfig{URL: backend.URL, APIKey: "owner-key", Headers: map[string]string{"Comfy-User": "owner-user"}}}
	handler := newHandler(t, cfg).(*Handler)
	token := strings.Repeat("p", 32)
	owner := &Reloader{logger: discardLogger(), peerPathSlots: make(chan struct{}, 4)}
	owner.state.Store(&reloaderState{handler: handler, peer: &config.PeerConfig{NodeID: "owner",
		ExposedPathRoutes: map[string]string{"comfyui": "/comfyui"},
		Peers:             map[string]config.PeerGrant{"caller": {Token: token, PathAccess: []string{"comfyui"}}},
	}})
	for _, viaPeer := range []bool{false, true} {
		server := serveComfyUIFrontendMode(t, handler, owner, viaPeer, token)
		runComfyUIFrontendBypassPhase(t, handler, owner, viaPeer, token, server, &schedulerCalls)
	}
	if schedulerCalls.Load() != 0 {
		t.Fatalf("running UI requests unexpectedly hit scheduler: calls=%d", schedulerCalls.Load())
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/comfyui/api/prompt", strings.NewReader(`{}`)))
	if response.Code != http.StatusAccepted || schedulerCalls.Load() != 1 {
		t.Fatal("generation no longer uses scheduler")
	}
}

// serveComfyUIFrontendMode returns an httptest server that serves the ComfyUI
// frontend either directly (local mode) or through the owner peer (peer mode).
// In peer mode the request carries the peer TLS marker, service/path headers and
// bearer token, and the owner peer path endpoint is invoked.
func serveComfyUIFrontendMode(t *testing.T, handler *Handler, owner *Reloader, viaPeer bool, token string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if viaPeer {
			req.TLS = &tls.ConnectionState{}
			req.Header.Set(peerPathServiceHeader, "comfyui")
			req.Header.Set(peerServicePathHeader, base64.RawURLEncoding.EncodeToString([]byte(req.URL.EscapedPath())))
			req.Header.Set("Authorization", "Bearer "+token)
			req.URL.Path = peerPathEndpoint
			owner.PeerHandler().ServeHTTP(w, req)
		} else {
			handler.ServeHTTP(w, req)
		}
	}))
}

// assertComfyUIFrontendEndpoints verifies every frontend endpoint is served by
// the result backend with a 200 and a body that echoes the endpoint, in both
// local and peer modes. The forged Comfy-User header must not leak as a cookie
// to the result backend.
func assertComfyUIFrontendEndpoints(t *testing.T, server *httptest.Server, endpoints []string, viaPeer bool) {
	t.Helper()
	for _, endpoint := range endpoints {
		request, err := http.NewRequest(http.MethodGet, server.URL+"/comfyui"+endpoint, nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Comfy-User", "forged-caller")
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if response.StatusCode != http.StatusOK || string(body) != endpoint {
			t.Fatalf("peer=%v endpoint=%s status=%d body=%q", viaPeer, endpoint, response.StatusCode, body)
		}
	}
}

// assertComfyUIFrontendRoot verifies the UI root is served persistently by the
// result backend (scheduler untouched) and that the non-root path redirects to
// the root with a temporary redirect, in both local and peer modes.
func assertComfyUIFrontendRoot(t *testing.T, server *httptest.Server, schedulerCalls *atomic.Int32, viaPeer bool) {
	t.Helper()
	beforeRoot := schedulerCalls.Load()
	root, err := server.Client().Get(server.URL + "/comfyui/")
	if err != nil {
		t.Fatal(err)
	}
	_ = root.Body.Close()
	if root.StatusCode != http.StatusOK || schedulerCalls.Load() != beforeRoot {
		t.Fatalf("peer=%v running UI root should use persistent origin: status=%d calls=%d", viaPeer, root.StatusCode, schedulerCalls.Load())
	}
	noRedirectClient := *server.Client()
	noRedirectClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	rootResponse, err := noRedirectClient.Get(server.URL + "/comfyui")
	if err != nil {
		t.Fatal(err)
	}
	_ = rootResponse.Body.Close()
	if rootResponse.StatusCode != http.StatusTemporaryRedirect || rootResponse.Header.Get("Location") != "/comfyui/" {
		t.Fatalf("root redirect peer=%v: status=%d location=%s", viaPeer, rootResponse.StatusCode, rootResponse.Header.Get("Location"))
	}
}

// assertComfyUIFrontendAssetUnavailable verifies an unavailable asset returns
// 503 from the result backend and does not fall back to the scheduler, in both
// local and peer modes.
func assertComfyUIFrontendAssetUnavailable(t *testing.T, server *httptest.Server, schedulerCalls *atomic.Int32) {
	t.Helper()
	beforeAsset := schedulerCalls.Load()
	unavailable, err := server.Client().Get(server.URL + "/comfyui/assets/missing.js")
	if err != nil {
		t.Fatal(err)
	}
	_ = unavailable.Body.Close()
	if unavailable.StatusCode != http.StatusServiceUnavailable || schedulerCalls.Load() != beforeAsset {
		t.Fatalf("unavailable asset fell back to scheduler: status=%d calls=%d", unavailable.StatusCode, schedulerCalls.Load())
	}
}

// assertComfyUIFrontendWebSocket performs a raw TCP WebSocket handshake against
// the server, echoes a masked frame, and verifies the reply. The connection is
// closed on every path, including failure paths.
func assertComfyUIFrontendWebSocket(t *testing.T, server *httptest.Server, viaPeer bool) {
	t.Helper()
	host := strings.TrimPrefix(server.URL, "http://")
	connection, err := net.DialTimeout("tcp", host, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = connection.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = fmt.Fprintf(connection, "GET /comfyui/ws?clientId=frontend HTTP/1.1\r\nHost: %s\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\nCookie: caller=secret\r\n\r\n", host, comfyUIWebSocketTestKey())
	reader := bufio.NewReader(connection)
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodGet})
	if err != nil {
		_ = connection.Close()
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusSwitchingProtocols {
		_ = connection.Close()
		t.Fatalf("peer=%v WebSocket status=%d", viaPeer, response.StatusCode)
	}
	_, _ = connection.Write([]byte("\x81\x82\x00\x00\x00\x00hi"))
	frame := make([]byte, 4)
	_, err = io.ReadFull(reader, frame)
	_ = connection.Close()
	if err != nil || string(frame) != "\x81\x02ok" {
		t.Fatalf("peer=%v WebSocket reply=%q error=%v", viaPeer, frame, err)
	}
}

// newComfyUIRootTestBackend returns an httptest server that serves the ComfyUI
// result fixture responses and a queue endpoint, mirroring the result backend
// used by the scheduler fallback test.
func newComfyUIRootTestBackend(t *testing.T, responses map[string]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/queue" {
			_, _ = io.WriteString(w, `{"queue_running":[],"queue_pending":[]}`)
			return
		}
		if body, ok := responses[r.URL.RequestURI()]; ok && r.Method == http.MethodGet {
			_, _ = io.WriteString(w, body)
			return
		}
		http.NotFound(w, r)
	}))
}

// newComfyUIRootTestScheduler returns an httptest server that counts scheduler
// calls, serves the UI root on GET /comfyui/, and accepts generation requests.
func newComfyUIRootTestScheduler(t *testing.T, schedulerCalls *atomic.Int32, page string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		schedulerCalls.Add(1)
		if r.Method == http.MethodGet && r.URL.Path == "/comfyui/" {
			_, _ = io.WriteString(w, page)
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/comfyui/api/prompt" {
			t.Errorf("unexpected scheduler request: %s %s", r.Method, r.URL.Path)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
}

// getComfyUI performs a GET against the proxy and asserts the expected status,
// returning the resolved request URL and response body.
func getComfyUI(t *testing.T, client *http.Client, target string, status int) (*url.URL, string) {
	t.Helper()
	res, err := client.Get(target)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil || res.StatusCode != status {
		t.Fatalf("GET %s: status=%d want=%d body=%q err=%v", target, res.StatusCode, status, body, err)
	}
	return res.Request.URL, string(body)
}

// assertComfyUIRootResponses verifies each frontend asset/API response matches
// the expected fixture, served persistently by the result backend.
func assertComfyUIRootResponses(t *testing.T, client *http.Client, pageURL *url.URL, responses map[string]string) {
	t.Helper()
	for _, target := range []string{"assets/app.js", "api/history/job-1", "view?filename=output.png"} {
		ref, err := url.Parse(target)
		if err != nil {
			t.Fatal(err)
		}
		_, body := getComfyUI(t, client, pageURL.ResolveReference(ref).String(), http.StatusOK)
		if body != responses["/"+target] {
			t.Fatalf("GET %s returned %q", target, body)
		}
	}
}

// assertComfyUIRootFallback verifies that unavailable assets return 503 from the
// result backend without hitting the scheduler.
func assertComfyUIRootFallback(t *testing.T, client *http.Client, proxyURL string) {
	t.Helper()
	for _, target := range []string{"/comfyui/assets/app.js", "/comfyui/api/history/job-1", "/comfyui/view?filename=output.png"} {
		getComfyUI(t, client, proxyURL+target, http.StatusBadGateway)
	}
}

func TestComfyUIRootUsesSchedulerAndResultsDoNotFallBack(t *testing.T) {
	const page = `<script src="assets/app.js"></script>`
	responses := map[string]string{
		"/": page, "/assets/app.js": "// frontend fixture",
		"/api/history/job-1": `{"job-1":{}}`, "/view?filename=output.png": "image fixture",
	}
	backend := newComfyUIRootTestBackend(t, responses)
	defer backend.Close()
	var schedulerCalls atomic.Int32
	scheduler := newComfyUIRootTestScheduler(t, &schedulerCalls, page)
	defer scheduler.Close()
	cfg := baseConfig(scheduler.URL)
	cfg.PathRoutes = map[string]string{"/comfyui": "default"}
	cfg.Peer = &config.PeerConfig{NodeID: "owner", Listen: "127.0.0.1:9294", TLSCertFile: "cert.pem", TLSKeyFile: "key.pem",
		ComfyUIResultRead: &config.ComfyUIResultReadConfig{URL: backend.URL}}
	proxyServer := httptest.NewServer(newHandler(t, cfg))
	defer proxyServer.Close()
	client := proxyServer.Client()
	client.Timeout = 5 * time.Second

	// The running UI root must be served persistently by the result backend,
	// never through the scheduler.
	pageURL, body := getComfyUI(t, client, proxyServer.URL+"/comfyui", http.StatusOK)
	if pageURL.Path != "/comfyui/" || body != page {
		t.Fatalf("unexpected page or redirect: %s %q", pageURL.Path, body)
	}
	if schedulerCalls.Load() != 0 {
		t.Fatalf("running ComfyUI UI root unnecessarily loaded through scheduler: %d", schedulerCalls.Load())
	}
	assertComfyUIRootResponses(t, client, pageURL, responses)

	// Once the result backend stops, the UI root falls back to the scheduler
	// exactly once, and assets return 503 without further scheduler calls.
	backend.Close()
	if _, body := getComfyUI(t, client, proxyServer.URL+"/comfyui/", http.StatusOK); body != page {
		t.Fatalf("scheduler UI root after result backend stopped: %q", body)
	}
	assertComfyUIRootFallback(t, client, proxyServer.URL)
	if schedulerCalls.Load() != 1 {
		t.Fatalf("unavailable ComfyUI root did not fall back exactly once: %d", schedulerCalls.Load())
	}

	// Generation still uses the scheduler.
	res, err := client.Post(proxyServer.URL+"/comfyui/api/prompt", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusAccepted || schedulerCalls.Load() != 2 {
		t.Fatalf("generation status=%d scheduler calls=%d", res.StatusCode, schedulerCalls.Load())
	}
}

func TestComfyUIWebSocketsDoNotConsumeResultReadSlots(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer backend.Close()
	resultProxy, err := newComfyUIResultReadProxy(&config.ComfyUIResultReadConfig{URL: backend.URL}, discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	handler := &Handler{logger: discardLogger(), comfyUIResultReadProxy: resultProxy,
		comfyUIResultSlots: make(chan struct{}, 1), comfyUIWebSocketSlots: make(chan struct{}, 1)}
	handler.comfyUIWebSocketSlots <- struct{}{}
	handler.comfyUIFrontendSlots = make(chan struct{}, 1)
	page := httptest.NewRecorder()
	handler.ServeHTTP(page, httptest.NewRequest(http.MethodGet, "/comfyui/assets/app.js", nil))
	if page.Code != http.StatusOK {
		t.Fatalf("WebSocket slots starved page read: %d", page.Code)
	}
	request := httptest.NewRequest(http.MethodGet, "/comfyui/ws", nil)
	request.Header.Set("Connection", "Upgrade")
	request.Header.Set("Upgrade", "websocket")
	request.Header.Set("Sec-WebSocket-Version", "13")
	request.Header.Set("Sec-WebSocket-Key", comfyUIWebSocketTestKey())
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("full WebSocket slots status=%d", response.Code)
	}
}

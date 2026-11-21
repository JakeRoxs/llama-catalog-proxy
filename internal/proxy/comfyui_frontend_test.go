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

func TestComfyUIFrontendAllowlist(t *testing.T) {
	for _, endpoint := range []string{"/api/global_subgraphs/not-a-hash", "/api/global_subgraphs/" + strings.Repeat("g", 64), "/api/global_subgraphs/" + strings.Repeat("a", 64) + "?path=other"} {
		if matched, status, _ := validateComfyUIFrontendRequest(httptest.NewRequest(http.MethodGet, "/comfyui"+endpoint, nil)); !matched || status != http.StatusBadRequest {
			t.Fatalf("subgraph request %s: matched=%v status=%d", endpoint, matched, status)
		}
	}
	for _, endpoint := range []string{"/comfyui", "/comfyui/", "/comfyui/index.html", "/comfyui/assets/app.js?v=1", "/comfyui/scripts/app.js", "/comfyui/extensions/custom/ui.js", "/comfyui/assets/style.css", "/comfyui/assets/font.woff2", "/comfyui/favicon.ico", "/comfyui/features", "/comfyui/api/features", "/comfyui/extensions", "/comfyui/embeddings", "/comfyui/models/checkpoints", "/comfyui/object_info", "/comfyui/api/object_info/LoadImage"} {
		matched, status, _ := validateComfyUIFrontendRequest(httptest.NewRequest(http.MethodGet, endpoint, nil))
		if !matched || status != 0 {
			t.Fatalf("frontend route %s: matched=%v status=%d", endpoint, matched, status)
		}
	}
	for _, endpoint := range []string{"/comfyui/assets/../secret.js", "/comfyui/assets/.secret.js", "/comfyui/assets/file.py", "/comfyui/models/folder/extra", "/comfyui/features?user_id=other", "/comfyui/assets%2fapp.js"} {
		matched, status, _ := validateComfyUIFrontendRequest(httptest.NewRequest(http.MethodGet, endpoint, nil))
		if !matched || status < 400 {
			t.Fatalf("unsafe frontend route %s: matched=%v status=%d", endpoint, matched, status)
		}
	}
	for _, endpoint := range []string{"/comfyui/prompt", "/comfyui/custom/load_model", "/comfyui/health", "/comfyui/free", "/settings", "/api/settings", "/userdata/workflows%2Fexample.json"} {
		if matched, _, _ := validateComfyUIFrontendRequest(httptest.NewRequest(http.MethodGet, endpoint, nil)); matched {
			t.Fatalf("non-frontend route classified: %s", endpoint)
		}
	}
	if matched, _, _ := validateComfyUIFrontendRequest(httptest.NewRequest(http.MethodPost, "/comfyui/api/prompt", strings.NewReader(`{}`))); matched {
		t.Fatal("generation matched frontend bypass")
	}
	request := httptest.NewRequest(http.MethodGet, "/comfyui/ws?clientId=frontend", nil)
	request.Header.Set("Connection", "Upgrade")
	request.Header.Set("Upgrade", "websocket")
	request.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	request.Header.Set("Sec-WebSocket-Version", "13")
	if matched, status, _ := validateComfyUIFrontendRequest(request); !matched || status != 0 {
		t.Fatalf("valid websocket denied: %v %d", matched, status)
	}
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

func TestComfyUIFrontendHTTPAndWebSocketBypassScheduler(t *testing.T) {
	var schedulerCalls atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
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
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
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
		for _, endpoint := range []string{"/", "/assets/app.js?v=1", "/extensions/custom/ui.js", "/api/features", "/object_info",
			"/api/i18n", "/api/global_subgraphs", "/api/node_replacements",
			"/api/global_subgraphs/" + strings.Repeat("a", 64),
			"/api/jobs?status=completed,failed,cancelled&limit=64&offset=0",
			"/user.css", "/materialdesignicons.min.css", "/fonts/materialdesignicons-webfont.woff2?v=7.4.47",
			"/api/users", "/api/settings", "/api/userdata/user.css", "/api/userdata/workflows%2Fexample.json",
			"/api/userdata?dir=workflows&recurse=true&full_info=true", "/api/v2/userdata?path=workflows%2Fproject",
			"/templates/index.json", "/workflow_templates"} {
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
		unavailable, err := server.Client().Get(server.URL + "/comfyui/assets/missing.js")
		if err != nil {
			t.Fatal(err)
		}
		_ = unavailable.Body.Close()
		if unavailable.StatusCode != http.StatusServiceUnavailable || schedulerCalls.Load() != 0 {
			t.Fatalf("unavailable asset fell back to scheduler: status=%d calls=%d", unavailable.StatusCode, schedulerCalls.Load())
		}
		connection, err := net.DialTimeout("tcp", strings.TrimPrefix(server.URL, "http://"), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		_ = connection.SetDeadline(time.Now().Add(5 * time.Second))
		_, _ = fmt.Fprintf(connection, "GET /comfyui/ws?clientId=frontend HTTP/1.1\r\nHost: %s\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\nCookie: caller=secret\r\n\r\n", strings.TrimPrefix(server.URL, "http://"))
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
		server.Close()
	}
	if schedulerCalls.Load() != 0 {
		t.Fatalf("frontend triggered %d scheduler requests", schedulerCalls.Load())
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/comfyui/api/prompt", strings.NewReader(`{}`)))
	if response.Code != http.StatusAccepted || schedulerCalls.Load() != 1 {
		t.Fatal("generation no longer uses scheduler")
	}
}

func TestComfyUIPageAndResultsNeverFallBackWhenBackendStops(t *testing.T) {
	const page = `<script src="assets/app.js"></script>`
	responses := map[string]string{
		"/": page, "/assets/app.js": "// frontend fixture",
		"/api/history/job-1": `{"job-1":{}}`, "/view?filename=output.png": "image fixture",
	}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if body, ok := responses[r.URL.RequestURI()]; ok && r.Method == http.MethodGet {
			_, _ = io.WriteString(w, body)
			return
		}
		http.NotFound(w, r)
	}))
	defer backend.Close()
	var schedulerCalls atomic.Int32
	scheduler := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		schedulerCalls.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != "/comfyui/api/prompt" {
			t.Errorf("unexpected scheduler request: %s %s", r.Method, r.URL.Path)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer scheduler.Close()
	cfg := baseConfig(scheduler.URL)
	cfg.PathRoutes = map[string]string{"/comfyui": "default"}
	cfg.Peer = &config.PeerConfig{NodeID: "owner", Listen: "127.0.0.1:9294", TLSCertFile: "cert.pem", TLSKeyFile: "key.pem",
		ComfyUIResultRead: &config.ComfyUIResultReadConfig{URL: backend.URL}}
	proxyServer := httptest.NewServer(newHandler(t, cfg))
	defer proxyServer.Close()
	client := proxyServer.Client()
	client.Timeout = 5 * time.Second
	get := func(target string, status int) (*url.URL, string) {
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
	pageURL, body := get(proxyServer.URL+"/comfyui", http.StatusOK)
	if pageURL.Path != "/comfyui/" || body != page {
		t.Fatalf("unexpected page or redirect: %s %q", pageURL.Path, body)
	}
	for _, target := range []string{"assets/app.js", "api/history/job-1", "view?filename=output.png"} {
		ref, err := url.Parse(target)
		if err != nil {
			t.Fatal(err)
		}
		_, body := get(pageURL.ResolveReference(ref).String(), http.StatusOK)
		if body != responses["/"+target] {
			t.Fatalf("GET %s returned %q", target, body)
		}
	}

	backend.Close()
	for _, target := range []string{"/comfyui", "/comfyui/assets/app.js", "/comfyui/api/history/job-1", "/comfyui/view?filename=output.png"} {
		get(proxyServer.URL+target, http.StatusBadGateway)
	}
	if schedulerCalls.Load() != 0 {
		t.Fatalf("direct requests reached scheduler: %d", schedulerCalls.Load())
	}
	res, err := client.Post(proxyServer.URL+"/comfyui/api/prompt", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusAccepted || schedulerCalls.Load() != 1 {
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
	page := httptest.NewRecorder()
	handler.ServeHTTP(page, httptest.NewRequest(http.MethodGet, "/comfyui/", nil))
	if page.Code != http.StatusOK {
		t.Fatalf("WebSocket slots starved page read: %d", page.Code)
	}
	request := httptest.NewRequest(http.MethodGet, "/comfyui/ws", nil)
	request.Header.Set("Connection", "Upgrade")
	request.Header.Set("Upgrade", "websocket")
	request.Header.Set("Sec-WebSocket-Version", "13")
	request.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("full WebSocket slots status=%d", response.Code)
	}
}

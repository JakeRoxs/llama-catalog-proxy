package proxy

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
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

func TestRoutesAndRewritesPrefixedModel(t *testing.T) {
	defaultServer := noContentServer()
	defer defaultServer.Close()
	remote := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["model"] != "qwen38" || body["custom"] != "keep" {
			t.Errorf("rewritten body = %#v", body)
		}
		if request.ContentLength <= 0 || request.Header.Get("Content-Length") == "" {
			t.Error("rewritten Content-Length was not set")
		}
		writer.Header().Set("X-Upstream", "remote")
		writer.WriteHeader(http.StatusCreated)
	}))
	defer remote.Close()
	handler := testHandler(t, defaultServer.URL, map[string]config.Backend{"remote": {URL: remote.URL, Prefix: "remote/"}})
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"remote/qwen38","custom":"keep"}`))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusCreated || recorder.Header().Get("X-Upstream") != "remote" {
		t.Fatalf("response = %d %#v", recorder.Code, recorder.Header())
	}
}

func TestLongestPrefixWins(t *testing.T) {
	defaultServer := noContentServer()
	defer defaultServer.Close()
	short := noContentServer()
	defer short.Close()
	selected := make(chan string, 1)
	long := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(request.Body).Decode(&body)
		selected <- body["model"].(string)
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer long.Close()
	handler := testHandler(t, defaultServer.URL, map[string]config.Backend{
		"short": {URL: short.URL, Prefix: "abc/"},
		"long":  {URL: long.URL, Prefix: "abc/123/"},
	})
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"abc/123/model"}`)))
	if model := <-selected; model != "model" {
		t.Fatalf("local model = %q", model)
	}
}

func TestUnknownModelRoutesToDefaultUnchanged(t *testing.T) {
	received := make(chan string, 1)
	defaultServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		received <- string(body)
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer defaultServer.Close()
	remote := noContentServer()
	defer remote.Close()
	handler := testHandler(t, defaultServer.URL, map[string]config.Backend{"remote": {URL: remote.URL, Prefix: "remote/"}})
	body := `{"model":"local-model","messages":[]}`
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body)))
	if got := <-received; got != body {
		t.Fatalf("default body = %q", got)
	}
}

func TestRoutingDoesNotDependOnCatalog(t *testing.T) {
	defaultServer := noContentServer()
	defer defaultServer.Close()
	var routed atomic.Bool
	remote := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/v1/models" {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		routed.Store(true)
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer remote.Close()
	handler := testHandler(t, defaultServer.URL, map[string]config.Backend{"remote": {URL: remote.URL, Prefix: "remote/"}})
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/embeddings", strings.NewReader(`{"model":"remote/not-cached"}`)))
	if !routed.Load() {
		t.Fatal("request was not routed directly from its prefix")
	}
}

func TestMalformedJSONRoutesToDefaultByteForByte(t *testing.T) {
	received := make(chan string, 1)
	defaultServer := bodyCaptureServer(received)
	defer defaultServer.Close()
	handler := testHandler(t, defaultServer.URL, nil)
	body := `{"model":broken`
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/custom", strings.NewReader(body)))
	if got := <-received; got != body {
		t.Fatalf("body = %q", got)
	}
}

func TestCompressedBodyRoutesToDefaultByteForByte(t *testing.T) {
	received := make(chan string, 1)
	defaultServer := bodyCaptureServer(received)
	defer defaultServer.Close()
	remote := noContentServer()
	defer remote.Close()
	handler := testHandler(t, defaultServer.URL, map[string]config.Backend{"remote": {URL: remote.URL, Prefix: "remote/"}})
	body := "compressed bytes"
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	request.Header.Set("Content-Encoding", "gzip")
	handler.ServeHTTP(httptest.NewRecorder(), request)
	if got := <-received; got != body {
		t.Fatalf("body = %q", got)
	}
}

func TestJSONBodyAtConfiguredLimitIsAccepted(t *testing.T) {
	received := make(chan string, 1)
	defaultServer := bodyCaptureServer(received)
	defer defaultServer.Close()
	cfg := baseConfig(defaultServer.URL)
	body := `{"value":"1234"}`
	cfg.MaxJSONBody = int64(len(body))
	handler := newHandler(t, cfg)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/custom", strings.NewReader(body)))
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d", recorder.Code)
	}
	if got := <-received; got != body {
		t.Fatalf("body = %q", got)
	}
}

func TestOversizedJSONBodyIsRejectedWithOrWithoutContentLength(t *testing.T) {
	defaultServer := noContentServer()
	defer defaultServer.Close()
	cfg := baseConfig(defaultServer.URL)
	cfg.MaxJSONBody = 8
	handler := newHandler(t, cfg)
	for _, contentLength := range []int64{16, -1} {
		t.Run(fmt.Sprint(contentLength), func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"remote/model"}`))
			request.ContentLength = contentLength
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusRequestEntityTooLarge || !strings.Contains(recorder.Body.String(), "exceeds the configured limit") {
				t.Fatalf("response = %d %s", recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestOversizedNonJSONBodyStreamsToDefaultBackend(t *testing.T) {
	received := make(chan string, 1)
	defaultServer := bodyCaptureServer(received)
	defer defaultServer.Close()
	cfg := baseConfig(defaultServer.URL)
	cfg.MaxJSONBody = 8
	handler := newHandler(t, cfg)
	body := strings.Repeat("x", 64)
	request := httptest.NewRequest(http.MethodPost, "/upload", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/octet-stream")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNoContent || <-received != body {
		t.Fatalf("non-JSON body was not proxied")
	}
}

func TestNonModelRequestPreservesPathQueryAndHeaders(t *testing.T) {
	defaultServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/custom/path" || request.URL.RawQuery != "one=1&two=2" {
			t.Errorf("URL = %s", request.URL.String())
		}
		if request.Header.Get("Authorization") != "Bearer client" || request.Header.Get("X-Custom") != "value" {
			t.Errorf("headers = %#v", request.Header)
		}
		writer.WriteHeader(http.StatusAccepted)
	}))
	defer defaultServer.Close()
	handler := testHandler(t, defaultServer.URL, nil)
	request := httptest.NewRequest(http.MethodPatch, "/custom/path?one=1&two=2", strings.NewReader(`{"value":1}`))
	request.Header.Set("Authorization", "Bearer client")
	request.Header.Set("X-Custom", "value")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d", recorder.Code)
	}
}

func TestPathRoutesUseLongestSegmentBoundaryMatchBeforeModelRouting(t *testing.T) {
	defaultServer := upstreamMarkerServer("default")
	defer defaultServer.Close()
	comfyServer := upstreamMarkerServer("comfyui")
	defer comfyServer.Close()
	adminServer := upstreamMarkerServer("admin")
	defer adminServer.Close()

	cfg := baseConfig(defaultServer.URL)
	cfg.Backends["comfyui"] = config.Backend{URL: comfyServer.URL, Prefix: "comfyui/"}
	cfg.Backends["admin"] = config.Backend{URL: adminServer.URL, Prefix: "admin/"}
	cfg.PathRoutes = map[string]string{"/comfyui": "comfyui", "/comfyui/admin": "admin"}
	handler := newHandler(t, cfg)

	tests := []struct {
		path string
		body string
		want string
	}{
		{path: "/comfyui", body: `{"model":"admin/ignored"}`, want: "comfyui"},
		{path: "/comfyui/ws", body: `{"model":"admin/ignored"}`, want: "comfyui"},
		{path: "/comfyui/admin/jobs", body: `{"model":"comfyui/ignored"}`, want: "admin"},
		{path: "/comfyui-other", body: `{"model":"local"}`, want: "default"},
	}
	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, test.path, strings.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if got := recorder.Header().Get("X-Upstream"); got != test.want {
				t.Fatalf("upstream = %q, want %q", got, test.want)
			}
		})
	}
}

func TestPathRoutePreservesRequestAndAppliesBackendCredentials(t *testing.T) {
	type receivedRequest struct {
		method  string
		path    string
		query   string
		rawPath string
		body    string
		header  http.Header
	}
	received := make(chan receivedRequest, 1)
	comfyServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		received <- receivedRequest{
			method: request.Method, path: request.URL.Path, query: request.URL.RawQuery,
			rawPath: request.URL.RawPath, body: string(body), header: request.Header.Clone(),
		}
		writer.Header().Set("Location", "/comfyui/?tab=queue")
		writer.WriteHeader(http.StatusPermanentRedirect)
	}))
	defer comfyServer.Close()
	defaultServer := noContentServer()
	defer defaultServer.Close()

	cfg := baseConfig(defaultServer.URL)
	cfg.Backends["ai5090"] = config.Backend{
		URL: comfyServer.URL, Prefix: "ai5090/", APIKey: "configured",
		Headers: map[string]string{"X-Backend": "ai5090"},
	}
	cfg.PathRoutes = map[string]string{"/comfyui": "ai5090"}
	handler := newHandler(t, cfg)

	request := httptest.NewRequest(http.MethodPatch, "/comfyui/api/workflows%2Ftest.json?overwrite=true", strings.NewReader("payload"))
	request.Header.Set("X-Client", "preserved")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	got := <-received
	if got.method != http.MethodPatch || got.path != "/comfyui/api/workflows/test.json" || got.rawPath != "/comfyui/api/workflows%2Ftest.json" || got.query != "overwrite=true" || got.body != "payload" {
		t.Fatalf("request was changed: %#v", got)
	}
	if got.header.Get("Authorization") != "Bearer configured" || got.header.Get("X-Api-Key") != "configured" || got.header.Get("X-Backend") != "ai5090" || got.header.Get("X-Client") != "preserved" {
		t.Fatalf("headers = %#v", got.header)
	}
	if recorder.Code != http.StatusPermanentRedirect || recorder.Header().Get("Location") != "/comfyui/?tab=queue" {
		t.Fatalf("response = %d %#v", recorder.Code, recorder.Header())
	}
}

func TestPathRouteProxiesWebSocketUpgrade(t *testing.T) {
	backendPath := make(chan string, 1)
	comfyServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		backendPath <- request.URL.Path
		connection, readWriter, err := writer.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer connection.Close()
		_, _ = readWriter.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
		_ = readWriter.Flush()
		line, err := readWriter.ReadString('\n')
		if err == nil {
			_, _ = readWriter.WriteString("echo:" + line)
			_ = readWriter.Flush()
		}
	}))
	defer comfyServer.Close()
	defaultServer := noContentServer()
	defer defaultServer.Close()

	cfg := baseConfig(defaultServer.URL)
	cfg.Backends["ai5090"] = config.Backend{URL: comfyServer.URL, Prefix: "ai5090/"}
	cfg.PathRoutes = map[string]string{"/comfyui": "ai5090"}
	proxyServer := httptest.NewServer(newHandler(t, cfg))
	defer proxyServer.Close()

	connection, err := net.DialTimeout("tcp", strings.TrimPrefix(proxyServer.URL, "http://"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(3 * time.Second))
	_, _ = io.WriteString(connection, "GET /comfyui/ws HTTP/1.1\r\nHost: proxy.test\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
	reader := bufio.NewReader(connection)
	status, err := reader.ReadString('\n')
	if err != nil || !strings.Contains(status, "101 Switching Protocols") {
		t.Fatalf("upgrade status = %q, error = %v", status, err)
	}
	for {
		line, readErr := reader.ReadString('\n')
		if readErr != nil {
			t.Fatal(readErr)
		}
		if line == "\r\n" {
			break
		}
	}
	_, _ = io.WriteString(connection, "ping\n")
	if echo, readErr := reader.ReadString('\n'); readErr != nil || echo != "echo:ping\n" {
		t.Fatalf("websocket tunnel response = %q, error = %v", echo, readErr)
	}
	if path := <-backendPath; path != "/comfyui/ws" {
		t.Fatalf("backend path = %q", path)
	}
}

func TestBackendHeadersAndAPIKeyDoNotOverrideClientCredentials(t *testing.T) {
	headers := make(chan http.Header, 2)
	defaultServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		headers <- request.Header.Clone()
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer defaultServer.Close()
	cfg := baseConfig(defaultServer.URL)
	cfg.Backends["default"] = config.Backend{URL: defaultServer.URL, APIKey: "configured", Headers: map[string]string{"X-Backend": "yes"}}
	handler := newHandler(t, cfg)
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/custom", nil))
	injected := <-headers
	if injected.Get("Authorization") != "Bearer configured" || injected.Get("X-Api-Key") != "configured" || injected.Get("X-Backend") != "yes" {
		t.Fatalf("configured headers = %#v", injected)
	}
	request := httptest.NewRequest(http.MethodPost, "/custom", nil)
	request.Header.Set("Authorization", "Bearer client")
	handler.ServeHTTP(httptest.NewRecorder(), request)
	preserved := <-headers
	if preserved.Get("Authorization") != "Bearer client" || preserved.Get("X-Api-Key") != "" {
		t.Fatalf("client credentials = %#v", preserved)
	}
}

func TestStreamingResponseIsFlushedBeforeCompletion(t *testing.T) {
	firstSent := make(chan struct{})
	releaseSecond := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(releaseSecond) }) }
	defer release()
	defaultServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(writer, "data: first\n\n")
		writer.(http.Flusher).Flush()
		close(firstSent)
		<-releaseSecond
		_, _ = io.WriteString(writer, "data: second\n\n")
		writer.(http.Flusher).Flush()
	}))
	defer defaultServer.Close()
	proxyServer := httptest.NewServer(testHandler(t, defaultServer.URL, nil))
	defer proxyServer.Close()
	response, err := (&http.Client{Timeout: 3 * time.Second}).Get(proxyServer.URL + "/v1/chat/completions")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	<-firstSent
	chunk := make(chan string, 1)
	go func() {
		line, _ := bufio.NewReader(response.Body).ReadString('\n')
		chunk <- line
	}()
	select {
	case line := <-chunk:
		if line != "data: first\n" {
			t.Fatalf("first chunk = %q", line)
		}
	case <-time.After(time.Second):
		t.Fatal("streaming response was buffered")
	}
	release()
}

func TestHealthIsLocal(t *testing.T) {
	var requests atomic.Int32
	defaultServer := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer defaultServer.Close()
	recorder := httptest.NewRecorder()
	testHandler(t, defaultServer.URL, nil).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/health", nil))
	if recorder.Code != http.StatusOK || requests.Load() != 0 {
		t.Fatalf("status = %d, backend requests = %d", recorder.Code, requests.Load())
	}
}

func TestReadyRequiresOnlyDefaultBackend(t *testing.T) {
	var defaultUnhealthy atomic.Bool
	defaultServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/health" {
			t.Errorf("path = %q", request.URL.Path)
		}
		if defaultUnhealthy.Load() {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		writer.WriteHeader(http.StatusOK)
	}))
	defer defaultServer.Close()
	optional := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { writer.WriteHeader(http.StatusServiceUnavailable) }))
	defer optional.Close()
	handler := testHandler(t, defaultServer.URL, map[string]config.Backend{"optional": {URL: optional.URL, Prefix: "optional/"}})
	ready := httptest.NewRecorder()
	handler.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if ready.Code != http.StatusOK || !strings.Contains(ready.Body.String(), `"optional":"unavailable"`) {
		t.Fatalf("ready response = %d %s", ready.Code, ready.Body.String())
	}
	defaultUnhealthy.Store(true)
	notReady := httptest.NewRecorder()
	handler.ServeHTTP(notReady, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if notReady.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", notReady.Code)
	}
}

func TestRequestServedIsLogged(t *testing.T) {
	defaultServer := noContentServer()
	defer defaultServer.Close()
	var logs bytes.Buffer
	handler, err := New(baseConfig(defaultServer.URL), slog.New(slog.NewJSONHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/custom", nil))
	if !strings.Contains(logs.String(), `"msg":"request served"`) || !strings.Contains(logs.String(), `"status":204`) {
		t.Fatalf("logs = %s", logs.String())
	}
}

func testHandler(t *testing.T, defaultURL string, optional map[string]config.Backend) http.Handler {
	t.Helper()
	cfg := baseConfig(defaultURL)
	for id, backend := range optional {
		cfg.Backends[id] = backend
	}
	return newHandler(t, cfg)
}

func baseConfig(defaultURL string) config.Config {
	return config.Config{
		Listen: "127.0.0.1:0", LogLevel: "INFO", DefaultBackend: "default",
		Backends:       map[string]config.Backend{"default": {URL: defaultURL}},
		ModelsCacheTTL: time.Minute, RequestTimeout: time.Second, MaxJSONBody: 64 << 20,
	}
}

func newHandler(t *testing.T, cfg config.Config) http.Handler {
	t.Helper()
	handler, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func noContentServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { writer.WriteHeader(http.StatusNoContent) }))
}

func upstreamMarkerServer(marker string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("X-Upstream", marker)
		writer.WriteHeader(http.StatusNoContent)
	}))
}

func bodyCaptureServer(received chan<- string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		received <- string(body)
		writer.WriteHeader(http.StatusNoContent)
	}))
}

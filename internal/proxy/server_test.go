package proxy

import (
	"bufio"
	"bytes"
	"context"
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

func assertRawWebSocketEcho(t *testing.T, proxyURL string, path string, failOnHeaderRead bool) {
	t.Helper()
	connection, err := net.DialTimeout("tcp", strings.TrimPrefix(proxyURL, "http://"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(3 * time.Second))
	_, _ = io.WriteString(connection, "GET "+path+" HTTP/1.1\r\nHost: proxy.test\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
	reader := bufio.NewReader(connection)
	status, err := reader.ReadString('\n')
	if err != nil || !strings.Contains(status, "101 Switching Protocols") {
		t.Fatalf("upgrade status = %q, error = %v", status, err)
	}
	for {
		line, readErr := reader.ReadString('\n')
		if readErr != nil {
			if failOnHeaderRead {
				t.Fatal(readErr)
			}
			break
		}
		if line == "\r\n" {
			break
		}
	}
	_, _ = io.WriteString(connection, "ping\n")
	if echo, readErr := reader.ReadString('\n'); readErr != nil || echo != "echo:ping\n" {
		t.Fatalf("websocket tunnel response = %q, error = %v", echo, readErr)
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

	assertRawWebSocketEcho(t, proxyServer.URL, "/comfyui/ws", true)
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
	cfg := baseConfig(defaultServer.URL)
	cfg.RequestTimeout = 10 * time.Millisecond
	cfg.HealthTimeout = 20 * time.Millisecond
	cfg.CatalogTimeout = 30 * time.Millisecond
	proxyServer := httptest.NewServer(newHandler(t, cfg))
	defer proxyServer.Close()
	response, err := (&http.Client{Timeout: 3 * time.Second}).Get(proxyServer.URL + "/v1/chat/completions")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	<-firstSent
	time.Sleep(50 * time.Millisecond)
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

func TestReadyBoundsDefaultHealthCheck(t *testing.T) {
	slowServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
		writer.WriteHeader(http.StatusOK)
	}))
	defer slowServer.Close()
	handler := testHandler(t, slowServer.URL, nil)
	deadline := time.Now().Add(5 * time.Second)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if time.Now().After(deadline) {
		t.Fatal("readiness did not return within the health budget")
	}
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 for slow default health", recorder.Code)
	}
}

func TestReadyOptionalSlowHealthDoesNotBlock(t *testing.T) {
	defaultServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { writer.WriteHeader(http.StatusOK) }))
	defer defaultServer.Close()
	optionalServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
		writer.WriteHeader(http.StatusOK)
	}))
	defer optionalServer.Close()
	handler := testHandler(t, defaultServer.URL, map[string]config.Backend{"optional": {URL: optionalServer.URL, Prefix: "optional/"}})
	deadline := time.Now().Add(5 * time.Second)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if time.Now().After(deadline) {
		t.Fatal("readiness did not return within the health budget")
	}
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 when only optional backend is slow", recorder.Code)
	}
	if !strings.Contains(recorder.Body.String(), `"optional":"unavailable"`) {
		t.Fatalf("body = %s", recorder.Body.String())
	}
}

func TestModelsBoundsDefaultCatalogRefresh(t *testing.T) {
	slowServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
		writeTestJSON(writer, http.StatusOK, `{"data":[]}`)
	}))
	defer slowServer.Close()
	handler := testHandler(t, slowServer.URL, nil)
	deadline := time.Now().Add(5 * time.Second)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	if time.Now().After(deadline) {
		t.Fatal("models did not return within the catalog budget")
	}
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 for slow default catalog", recorder.Code)
	}
}

func TestControlPlaneBudgetsCanExceedRequestTimeout(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		time.Sleep(40 * time.Millisecond)
		if request.URL.Path == "/health" {
			writer.WriteHeader(http.StatusOK)
			return
		}
		writeTestJSON(writer, http.StatusOK, `{"data":[]}`)
	}))
	defer backend.Close()

	cfg := baseConfig(backend.URL)
	cfg.RequestTimeout = 10 * time.Millisecond
	cfg.HealthTimeout = 200 * time.Millisecond
	cfg.CatalogTimeout = 200 * time.Millisecond
	handler := newHandler(t, cfg)

	ready := httptest.NewRecorder()
	handler.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if ready.Code != http.StatusOK {
		t.Fatalf("readiness status = %d, want 200 despite shorter request_timeout", ready.Code)
	}

	models := httptest.NewRecorder()
	handler.ServeHTTP(models, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	if models.Code != http.StatusOK {
		t.Fatalf("models status = %d, want 200 despite shorter request_timeout", models.Code)
	}
}

func TestRequestServedIsLogged(t *testing.T) {
	defaultServer := noContentServer()
	defer defaultServer.Close()
	var logs bytes.Buffer
	handler, err := New(baseConfig(defaultServer.URL), slog.New(slog.NewJSONHandler(&logs, nil)), nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/custom", nil))
	if !strings.Contains(logs.String(), `"msg":"request served"`) || !strings.Contains(logs.String(), `"status":204`) {
		t.Fatalf("logs = %s", logs.String())
	}
}

func TestHistoryDisabledByDefault(t *testing.T) {
	defaultServer := noContentServer()
	defer defaultServer.Close()
	handler, err := New(baseConfig(defaultServer.URL), slog.New(slog.NewTextHandler(io.Discard, nil)), nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if handler.(*Handler).history != nil {
		t.Fatal("history should be nil when request_history is disabled")
	}
}

func TestHistoryEnabledStoresRequest(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	cfg := baseConfig(backend.URL)
	cfg.RequestHistory = true
	handler, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := handler.(*Handler)
	if h.history == nil {
		t.Fatal("history should be non-nil when request_history is enabled")
	}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/custom", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d", recorder.Code)
	}

	requestID := recorder.Header().Get("X-Request-ID")
	if requestID == "" {
		t.Fatal("no request ID in response")
	}

	record, ok := h.history.GetByRequestID(requestID)
	if !ok || record == nil {
		t.Fatal("no history record for request")
	}
	if record.RequestID != requestID {
		t.Fatalf("record request ID = %q, want %q", record.RequestID, requestID)
	}
	if record.BackendID != "default" {
		t.Fatalf("record backend ID = %q, want default", record.BackendID)
	}
	if record.Status != statusHTTPSuccess {
		t.Fatalf("record status = %q, want %q", record.Status, statusHTTPSuccess)
	}
	// Duration is retained in metadata as duration_ms; verify it is set.
	if _, ok := record.Metadata["duration_ms"]; !ok {
		t.Fatal("record duration_ms not present in metadata")
	}
}

func TestHistoryEnabledStoresBackendAndStatus(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusInternalServerError)
	}))
	defer remote.Close()

	cfg := baseConfig(remote.URL)
	cfg.Backends["remote"] = config.Backend{URL: remote.URL, Prefix: "remote/"}
	cfg.RequestHistory = true
	handler, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := handler.(*Handler)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"remote/qwen"}`)))
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d", recorder.Code)
	}

	requestID := recorder.Header().Get("X-Request-ID")
	record, ok := h.history.GetByRequestID(requestID)
	if !ok || record == nil {
		t.Fatal("no history record for request")
	}
	if record.BackendID != "remote" {
		t.Fatalf("record backend ID = %q, want remote", record.BackendID)
	}
	if record.Status != statusHTTPServerError {
		t.Fatalf("record status = %q, want %q", record.Status, statusHTTPServerError)
	}
}

func TestHistoryDoesNotRetainSecrets(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	cfg := baseConfig(backend.URL)
	cfg.RequestHistory = true
	handler, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions?token=super-secret&key=query-key", strings.NewReader(`{"model":"qwen","password":"sensitive-pw"}`))
	request.Header.Set("Authorization", "Bearer client-secret-token")
	request.Header.Set("X-Api-Key", "client-api-key")

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	requestID := recorder.Header().Get("X-Request-ID")
	record, ok := handler.(*Handler).history.GetByRequestID(requestID)
	if !ok || record == nil {
		t.Fatal("no history record")
	}
	if record.RequestID != requestID {
		t.Fatalf("record request ID = %q, want %q", record.RequestID, requestID)
	}
	if record.BackendID != "default" {
		t.Fatalf("record backend ID = %q, want default", record.BackendID)
	}
	// CorrelationRecord retains only RequestID, BackendID, ModelID, ActivityID,
	// ActivityTime, RequestTime, Duration, Phase, Status, Metadata. No URL,
	// path, query, body, headers, or errors are retained by construction.
}

func TestHistoryConcurrentAndSSE(t *testing.T) {
	sseBackend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "text/event-stream")
		flusher := writer.(http.Flusher)
		_, _ = io.WriteString(writer, "data: hello\n\n")
		flusher.Flush()
		_, _ = io.WriteString(writer, "data: world\n\n")
		flusher.Flush()
	}))
	defer sseBackend.Close()

	cfg := baseConfig(sseBackend.URL)
	cfg.RequestHistory = true
	handler, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := handler.(*Handler)

	// Concurrent normal traffic: 10 concurrent requests, then history count.
	concurrentRequests(t, h, 10)

	// SSE through proxy must remain byte-exact.
	resp, err := validateSSEResponse(t, h)
	if err != nil {
		t.Fatal(err)
	}

	requestID := resp.Header.Get("X-Request-ID")
	record, ok := h.history.GetByRequestID(requestID)
	if !ok || record == nil {
		t.Fatal("no history record for SSE request")
	}
	if record.Status != statusHTTPSuccess {
		t.Fatalf("record status = %q, want %q", record.Status, statusHTTPSuccess)
	}
}

// concurrentRequests issues count concurrent GET /custom requests through h,
// waits for all to complete, and asserts the history contains count records.
func concurrentRequests(t *testing.T, h *Handler, count int) {
	t.Helper()
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			recorder := httptest.NewRecorder()
			h.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/custom", nil))
			if recorder.Code != http.StatusOK {
				t.Errorf("concurrent request %d status = %d", i, recorder.Code)
			}
		}(i)
	}
	wg.Wait()

	stats := h.history.Stats()
	if stats["record_count"] != count {
		t.Fatalf("history record count = %v, want %d", stats["record_count"], count)
	}
}

// validateSSEResponse proxies a GET through h and asserts the SSE response is
// byte-exact: Content-Type text/event-stream with events "data: hello" and
// "data: world". It returns the response so the caller can inspect headers.
func validateSSEResponse(t *testing.T, h *Handler) (*http.Response, error) {
	t.Helper()
	proxyServer := httptest.NewServer(h)
	defer proxyServer.Close()
	resp, err := http.DefaultClient.Get(proxyServer.URL + "/v1/chat/completions")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("SSE content type = %q", resp.Header.Get("Content-Type"))
	}
	events := sseDataLines(t, resp)
	if len(events) < 2 {
		t.Fatalf("expected SSE events, got %d", len(events))
	}
	for _, line := range events {
		if line != "data: hello" && line != "data: world" {
			t.Fatalf("unexpected SSE line = %q", line)
		}
	}
	return resp, nil
}

// sseDataLines reads resp.Body and returns the non-empty data lines.
func sseDataLines(t *testing.T, resp *http.Response) []string {
	t.Helper()
	var lines []string
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	var events []string
	for _, line := range lines {
		if line == "" {
			continue
		}
		events = append(events, line)
	}
	return events
}

func TestHistoryClientCanceled(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, req *http.Request) {
		<-req.Context().Done()
		writer.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	cfg := baseConfig(backend.URL)
	cfg.RequestHistory = true
	handler, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := handler.(*Handler)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodGet, "/custom", nil).WithContext(ctx)
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, req)

	requestID := recorder.Header().Get("X-Request-ID")
	record, ok := h.history.GetByRequestID(requestID)
	if !ok || record == nil {
		t.Fatal("no history record for canceled request")
	}
	if record.Status != statusClientCanceled {
		t.Fatalf("record status = %q, want %q", record.Status, statusClientCanceled)
	}
}

func TestHistoryBackendPanicRecovered(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		panic("boom")
	}))
	defer backend.Close()

	cfg := baseConfig(backend.URL)
	cfg.RequestHistory = true
	handler, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := handler.(*Handler)

	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/custom", nil))
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 for backend panic", recorder.Code)
	}

	requestID := recorder.Header().Get("X-Request-ID")
	record, ok := h.history.GetByRequestID(requestID)
	if !ok || record == nil {
		t.Fatal("no history record for backend panic request")
	}
	if record.Status != statusHTTPServerError {
		t.Fatalf("record status = %q, want %q", record.Status, statusHTTPServerError)
	}
}

func TestHistoryAbortPreservesPanicAndOutcome(t *testing.T) {
	cfg := baseConfig("http://backend.invalid")
	cfg.RequestHistory = true
	h := newHandler(t, cfg).(*Handler)
	h.proxies["default"].Transport = panicHistoryTransport{}
	w := httptest.NewRecorder()
	var caught any
	func() {
		defer func() { caught = recover() }()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/stream", nil))
	}()
	if caught != http.ErrAbortHandler {
		t.Fatalf("panic = %v, want original ErrAbortHandler", caught)
	}
	if w.Code != http.StatusOK {
		t.Fatalf("partial response status = %d, want 200 before abort", w.Code)
	}
	record, ok := h.history.GetByRequestID(w.Header().Get("X-Request-ID"))
	if !ok || record.Status != statusAborted || record.ActivityID != "" {
		t.Fatalf("aborted proxy-only record = %#v, found=%v", record, ok)
	}
}

type panicHistoryTransport struct{}

func (panicHistoryTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: panicHistoryBody{}, ContentLength: -1}, nil
}

type panicHistoryBody struct{}

func (panicHistoryBody) Read([]byte) (int, error) {
	panic(http.ErrAbortHandler)
}

func (panicHistoryBody) Close() error { return nil }

func TestResponseWriterRecordsFinalStatusAfterEarlyHints(t *testing.T) {
	w := &headerSequenceWriter{ResponseRecorder: httptest.NewRecorder()}
	observed := &responseWriter{ResponseWriter: w}
	observed.WriteHeader(http.StatusEarlyHints)
	if observed.status != 0 {
		t.Fatalf("informational response recorded as final status: %d", observed.status)
	}
	observed.WriteHeader(http.StatusOK)
	if observed.status != http.StatusOK {
		t.Fatalf("final status = %d, want 200", observed.status)
	}
	if len(w.statuses) != 2 || w.statuses[0] != http.StatusEarlyHints || w.statuses[1] != http.StatusOK {
		t.Fatalf("forwarded statuses = %v", w.statuses)
	}
	upgraded := &responseWriter{ResponseWriter: httptest.NewRecorder()}
	upgraded.WriteHeader(http.StatusSwitchingProtocols)
	if upgraded.status != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade status = %d, want 101", upgraded.status)
	}
}

type headerSequenceWriter struct {
	*httptest.ResponseRecorder
	statuses []int
}

func (w *headerSequenceWriter) WriteHeader(status int) {
	w.statuses = append(w.statuses, status)
	w.ResponseRecorder.WriteHeader(status)
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
		ModelsCacheTTL: time.Minute, RequestTimeout: time.Second, HealthTimeout: time.Second, CatalogTimeout: time.Second, MaxJSONBody: 64 << 20,
	}
}

func newHandler(t *testing.T, cfg config.Config) http.Handler {
	t.Helper()
	handler, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func TestDiagnosticsAPIDisabledReturns404AndDoesNotHitBackend(t *testing.T) {
	var backendCalled atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		backendCalled.Add(1)
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer backend.Close()

	// diagnostics_api_key is empty: API is disabled.
	cfg := baseConfig(backend.URL)
	handler := newHandler(t, cfg)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/diagnostics", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 when diagnostics API disabled", recorder.Code)
	}
	if backendCalled.Load() != 0 {
		t.Fatal("backend was hit when diagnostics API disabled")
	}

	// Unknown diagnostics subpath is reserved and not proxied.
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/diagnostics/v1/unknown", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 for unknown diagnostics subpath", recorder.Code)
	}
	if backendCalled.Load() != 0 {
		t.Fatal("backend was hit for unknown diagnostics subpath")
	}
}

func TestDiagnosticsAPIReservedBeforePathRouting(t *testing.T) {
	var backendCalled atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		backendCalled.Add(1)
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer backend.Close()

	// Path route would normally proxy /comfyui to the backend, but
	// /diagnostics is reserved before path routing.
	cfg := baseConfig(backend.URL)
	cfg.Backends["comfyui"] = config.Backend{URL: backend.URL, Prefix: "comfyui/"}
	cfg.PathRoutes = map[string]string{"/comfyui": "comfyui"}
	handler := newHandler(t, cfg)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/diagnostics", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 when diagnostics API disabled", recorder.Code)
	}
	if backendCalled.Load() != 0 {
		t.Fatal("backend was hit for /diagnostics despite path route")
	}
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

func writeTestJSON(writer http.ResponseWriter, status int, body string) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_, _ = io.WriteString(writer, body)
}

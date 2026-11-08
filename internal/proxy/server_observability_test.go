package proxy

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

type requestIDEchoBackend struct {
	server *httptest.Server
	mu     sync.Mutex
	seen   string
}

func newRequestIDEchoBackend() *requestIDEchoBackend {
	b := &requestIDEchoBackend{}
	b.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		b.mu.Lock()
		b.seen = request.Header.Get("X-Request-ID")
		b.mu.Unlock()
		writer.Header().Set("X-Request-ID-Echo", b.seen)
		writer.WriteHeader(http.StatusNoContent)
	}))
	return b
}

func (b *requestIDEchoBackend) Close() { b.server.Close() }

func (b *requestIDEchoBackend) RequestID() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.seen
}

func capturingHandler(t *testing.T, defaultURL string, optional map[string]config.Backend) (*Handler, *bytes.Buffer) {
	t.Helper()
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	cfg := baseConfig(defaultURL)
	for id, backend := range optional {
		cfg.Backends[id] = backend
	}
	handler, err := New(cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	return handler.(*Handler), &logs
}

func logContains(logs string, key, value string) bool {
	return strings.Contains(logs, `"`+key+`":"`+value+`"`)
}

func TestRequestIDGeneratedAndReturnedToClient(t *testing.T) {
	backend := newRequestIDEchoBackend()
	defer backend.Close()
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	cfg := baseConfig(backend.server.URL)
	handler, err := New(cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/custom", nil))
	requestID := recorder.Header().Get("X-Request-ID")
	if requestID == "" {
		t.Fatal("no X-Request-ID returned to client")
	}
	if len(requestID) != 32 {
		t.Fatalf("request ID length = %d, want 32 (16 hex bytes)", len(requestID))
	}
	if !logContains(logs.String(), "request_id", requestID) {
		t.Fatalf("request ID not present in request summary log: %s", logs.String())
	}
	if !logContains(logs.String(), "backend_id", "default") {
		t.Fatalf("default backend not logged: %s", logs.String())
	}
	if !logContains(logs.String(), "route_kind", "default") {
		t.Fatalf("default route kind not logged: %s", logs.String())
	}
}

func TestRequestIDForwardedUpstream(t *testing.T) {
	backend := newRequestIDEchoBackend()
	defer backend.Close()
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	cfg := baseConfig(backend.server.URL)
	handler, err := New(cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"qwen"}`)))
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d", recorder.Code)
	}
	got := backend.RequestID()
	if got == "" {
		t.Fatal("backend did not receive an X-Request-ID")
	}
	if got != recorder.Header().Get("X-Request-ID") {
		t.Fatalf("upstream request ID %q does not match client response ID %q", got, recorder.Header().Get("X-Request-ID"))
	}
	if !logContains(logs.String(), "request_id", got) {
		t.Fatalf("request ID not correlated in log: %s", logs.String())
	}
}

func TestUpstreamCannotOverrideResponseRequestID(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("X-Request-ID", "upstream-spoof")
		writer.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()
	handler := newHandler(t, baseConfig(backend.URL))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/unrelated", nil))
	requestID := recorder.Header().Get("X-Request-ID")
	if requestID == "" || requestID == "upstream-spoof" {
		t.Fatalf("response request ID = %q, want proxy-generated ID", requestID)
	}
}

func TestIncomingRequestIDIsReplacedNotTrusted(t *testing.T) {
	backend := newRequestIDEchoBackend()
	defer backend.Close()
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	cfg := baseConfig(backend.server.URL)
	handler, err := New(cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	spoof := "spoof-id-12345"
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"qwen"}`))
	request.Header.Set("X-Request-ID", spoof)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	responseID := recorder.Header().Get("X-Request-ID")
	if responseID == spoof {
		t.Fatal("incoming spoof request ID was reflected back to client")
	}
	if responseID == "" {
		t.Fatal("no request ID returned")
	}
	got := backend.RequestID()
	if got == spoof {
		t.Fatal("incoming spoof request ID was forwarded upstream")
	}
	if got != responseID {
		t.Fatalf("upstream ID %q does not match response ID %q", got, responseID)
	}
	if strings.Contains(logs.String(), spoof) {
		t.Fatalf("spoof request ID leaked into logs: %s", logs.String())
	}
}

func TestRouteKindAndBackendLogged(t *testing.T) {
	defaultServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { writer.WriteHeader(http.StatusNoContent) }))
	defer defaultServer.Close()
	remote := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { writer.WriteHeader(http.StatusNoContent) }))
	defer remote.Close()

	// Local route.
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	cfg := baseConfig(defaultServer.URL)
	handler, err := New(cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/health", nil))
	if !logContains(logs.String(), "route_kind", "local") {
		t.Fatalf("local route not logged: %s", logs.String())
	}

	// Default route (no model prefix).
	logs.Reset()
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/custom", strings.NewReader(`{"model":"local"}`)))
	if !logContains(logs.String(), "route_kind", "default") || !logContains(logs.String(), "backend_id", "default") {
		t.Fatalf("default route not logged: %s", logs.String())
	}

	// Model prefix route.
	logs.Reset()
	cfg2 := baseConfig(defaultServer.URL)
	cfg2.Backends["remote"] = config.Backend{URL: remote.URL, Prefix: "remote/"}
	handler2, err := New(cfg2, logger)
	if err != nil {
		t.Fatal(err)
	}
	handler2.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"remote/qwen"}`)))
	if !logContains(logs.String(), "route_kind", "model_prefix") || !logContains(logs.String(), "backend_id", "remote") {
		t.Fatalf("model prefix route not logged: %s", logs.String())
	}

	// Path route.
	logs.Reset()
	cfg3 := baseConfig(defaultServer.URL)
	cfg3.Backends["remote"] = config.Backend{URL: remote.URL}
	cfg3.PathRoutes = map[string]string{"/comfyui": "remote"}
	handler3, err := New(cfg3, logger)
	if err != nil {
		t.Fatal(err)
	}
	handler3.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/comfyui/ws", strings.NewReader(`{"model":"local"}`)))
	if !logContains(logs.String(), "route_kind", "path") || !logContains(logs.String(), "backend_id", "remote") {
		t.Fatalf("path route not logged: %s", logs.String())
	}
}

func TestHTTP5xxPassthroughAndSummaryStatus(t *testing.T) {
	errorBackend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(writer, `{"error":"boom"}`)
	}))
	defer errorBackend.Close()
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	cfg := baseConfig(errorBackend.URL)
	handler, err := New(cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"qwen"}`)))
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 passthrough", recorder.Code)
	}
	if !strings.Contains(logs.String(), `"status":500`) {
		t.Fatalf("summary status not 500: %s", logs.String())
	}
	if strings.Contains(logs.String(), "reverse proxy request failed") {
		t.Fatal("ErrorHandler should not fire for a backend HTTP 5xx")
	}
}

func TestCatalogFailureLogCorrelatesWithRequestSummary(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusInternalServerError)
	}))
	defer backend.Close()
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	handler, err := New(baseConfig(backend.URL), logger)
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	requestID := recorder.Header().Get("X-Request-ID")
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", recorder.Code)
	}
	if requestID == "" || !strings.Contains(logs.String(), `"request_id":"`+requestID+`"`) {
		t.Fatalf("catalog error did not correlate with request summary: %s", logs.String())
	}
	if !strings.Contains(logs.String(), "model catalog request failed") {
		t.Fatalf("catalog error log missing: %s", logs.String())
	}
}

func TestAbortedStreamStillLogsRequestSummary(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseBackend := func() { releaseOnce.Do(func() { close(release) }) }
	streamBackend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "text/event-stream")
		flusher := writer.(http.Flusher)
		_, _ = io.WriteString(writer, "data: first\n\n")
		flusher.Flush()
		<-release
		_, _ = io.WriteString(writer, "data: second\n\n")
		flusher.Flush()
	}))
	defer streamBackend.Close()

	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	handler, err := New(baseConfig(streamBackend.URL), logger)
	if err != nil {
		t.Fatal(err)
	}
	proxyDone := make(chan struct{})
	proxyServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		defer close(proxyDone)
		handler.ServeHTTP(writer, request)
	}))
	defer proxyServer.Close()
	defer releaseBackend()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, proxyServer.URL+"/v1/chat/completions", strings.NewReader(`{"model":"qwen","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(response.Body)
	if _, err := reader.ReadString('\n'); err != nil {
		_ = response.Body.Close()
		t.Fatalf("read first stream chunk: %v", err)
	}
	cancel()
	_ = response.Body.Close()
	releaseBackend()
	select {
	case <-proxyDone:
	case <-time.After(time.Second):
		t.Fatalf("proxy did not finish aborted stream; logs: %s", logs.String())
	}
	if !strings.Contains(logs.String(), "request served") || !strings.Contains(logs.String(), `"route_kind":"default"`) {
		t.Fatalf("aborted streaming request summary missing: %s", logs.String())
	}
}

func TestSSEPreserved(t *testing.T) {
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
	handler := newHandler(t, cfg)
	proxyServer := httptest.NewServer(handler)
	defer proxyServer.Close()
	resp, err := http.DefaultClient.Get(proxyServer.URL + "/v1/chat/completions")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("SSE content type = %q", resp.Header.Get("Content-Type"))
	}
	var lines []string
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(lines) < 2 {
		t.Fatalf("expected SSE chunks, got %d", len(lines))
	}
}

func TestWebSocketUpgradePreserved(t *testing.T) {
	wsBackend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, readWriter, err := writer.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer connection.Close()
		_, _ = readWriter.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
		_ = readWriter.Flush()
		line, _ := readWriter.ReadString('\n')
		_, _ = readWriter.WriteString("echo:" + line)
		_ = readWriter.Flush()
	}))
	defer wsBackend.Close()
	cfg := baseConfig(wsBackend.URL)
	cfg.PathRoutes = map[string]string{"/ws": "default"}
	handler := newHandler(t, cfg)
	proxyServer := httptest.NewServer(handler)
	defer proxyServer.Close()

	connection, err := net.DialTimeout("tcp", strings.TrimPrefix(proxyServer.URL, "http://"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(3 * time.Second))
	_, _ = io.WriteString(connection, "GET /ws HTTP/1.1\r\nHost: proxy.test\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
	reader := bufio.NewReader(connection)
	status, err := reader.ReadString('\n')
	if err != nil || !strings.Contains(status, "101 Switching Protocols") {
		t.Fatalf("upgrade status = %q, error = %v", status, err)
	}
	for {
		line, readErr := reader.ReadString('\n')
		if readErr != nil {
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

func TestRedactionOmitsQueryBodyAndSecret(t *testing.T) {
	secretBackend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer secretBackend.Close()
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	cfg := baseConfig(secretBackend.URL)
	handler, err := New(cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions?token=super-secret&key=query-key-sentinel-abc", strings.NewReader(`{"model":"qwen","password":"sensitive-pw"}`))
	request.Header.Set("Authorization", "Bearer client-secret-token")
	request.Header.Set("X-Api-Key", "client-api-key")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	logText := logs.String()
	for _, token := range []string{"super-secret", "query-key-sentinel-abc", "sensitive-pw", "client-secret-token", "client-api-key", "token=", "password"} {
		if strings.Contains(logText, token) {
			t.Fatalf("secret/query/body token %q leaked into logs: %s", token, logText)
		}
	}
}

func TestProxyTransportErrorRedactsURLCredentials(t *testing.T) {
	closedBackend := httptest.NewServer(http.NotFoundHandler())
	backendURL := closedBackend.URL
	closedBackend.Close()
	parsed, err := url.Parse(backendURL)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword("proxy-user", "url-password-secret")

	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	handler, err := New(baseConfig(parsed.String()), logger)
	if err != nil {
		t.Fatal(err)
	}
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/unavailable", nil))
	if strings.Contains(logs.String(), "url-password-secret") || strings.Contains(logs.String(), "proxy-user") {
		t.Fatalf("backend URL credentials leaked into proxy error log: %s", logs.String())
	}
	if !strings.Contains(logs.String(), "reverse proxy request failed") {
		t.Fatalf("transport failure log missing: %s", logs.String())
	}
}

func TestTransportErrorLogsRequestID(t *testing.T) {
	// Backend that accepts the connection then closes it without a response -> transport error.
	closed := make(chan struct{})
	transportBackend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		hijacker, ok := writer.(http.Hijacker)
		if !ok {
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
		conn, _, err := hijacker.Hijack()
		if err != nil {
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
		_ = conn.Close()
		close(closed)
	}))
	defer transportBackend.Close()

	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	cfg := baseConfig(transportBackend.URL)
	handler, err := New(cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"qwen"}`)))
	_ = recorder
	// Wait for the backend to close the connection.
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
	}
	// A transport error triggers the ErrorHandler, which logs the request ID.
	time.Sleep(200 * time.Millisecond)
	logText := logs.String()
	if !strings.Contains(logText, "request_id") {
		t.Fatalf("transport error log missing request ID: %s", logText)
	}
}

func TestProxyCancellationErrorUsesCorrelatedWarning(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	ctx, cancel := context.WithCancel(context.Background())
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil).WithContext(
		context.WithValue(ctx, requestIDKey{}, "request-id-test"),
	)
	cancel()
	recorder := httptest.NewRecorder()
	proxyErrorHandler(logger, "default")(recorder, request, context.Canceled)
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("canceled request status = %d, want 502", recorder.Code)
	}
	logText := logs.String()
	if !strings.Contains(logText, "reverse proxy request canceled by client") || !strings.Contains(logText, `"request_id":"request-id-test"`) {
		t.Fatalf("cancellation log is not correlated: %s", logText)
	}
}

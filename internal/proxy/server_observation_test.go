package proxy

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/correlation"
)

const observedStream = "data: {\"choices\":[{\"delta\":{\"content\":\"PRIVATE_COMPLETION\"},\"finish_reason\":\"stop\"}]}\n\ndata: {\"usage\":{\"prompt_tokens\":0,\"completion_tokens\":2}}\n\ndata: [DONE]\n\n"

func loggedObservation(t *testing.T, logs string) map[string]any {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(logs))
	for {
		var row map[string]any
		if err := dec.Decode(&row); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		if row["msg"] == "request_observation" {
			s, ok := row["summary"].(map[string]any)
			if !ok {
				t.Fatal("missing summary")
			}
			return s
		}
	}
	t.Fatal("missing observation log")
	return nil
}
func observedHandler(t *testing.T, cfg config.Config) (http.Handler, *synchronizedBuffer) {
	t.Helper()
	logger, logs := captureLogger()
	h, err := New(cfg, logger, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return h, logs
}

// observationRequest executes a streaming chat request against the proxy backed by
// backend with the given observation setting, returning the response recorder and
// the captured log text. The shared backend fixture is owned by the caller.
func observationRequest(t *testing.T, backend *httptest.Server, enabled bool) (*httptest.ResponseRecorder, string) {
	t.Helper()
	cfg := baseConfig(backend.URL)
	cfg.RequestObservation = enabled
	h, logs := observedHandler(t, cfg)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"PRIVATE_MODEL","messages":[{"role":"user","content":"PRIVATE_PROMPT"}],"stream":true}`))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "PRIVATE_AUTH")
	h.ServeHTTP(w, r)
	return w, logs.String()
}

func TestObservationDefaultOffAndSafeSummary(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request map[string]any
		_ = json.NewDecoder(r.Body).Decode(&request)
		if request["stream_options"] != nil {
			t.Error("usage settings were forced")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-Private", "PRIVATE_HEADER")
		_, _ = io.WriteString(w, observedStream)
	}))
	defer backend.Close()

	t.Run("disabled default off does not observe", func(t *testing.T) {
		w, logs := observationRequest(t, backend, false)
		if w.Body.String() != observedStream || w.Header().Get("X-Private") != "PRIVATE_HEADER" || w.Code != 200 {
			t.Fatal("passthrough changed")
		}
		if strings.Contains(logs, "request_observation") {
			t.Fatal("default-off still observed")
		}
	})

	t.Run("enabled observes safe summary", func(t *testing.T) {
		w, logs := observationRequest(t, backend, true)
		if w.Body.String() != observedStream || w.Header().Get("X-Private") != "PRIVATE_HEADER" || w.Code != 200 {
			t.Fatal("passthrough changed")
		}
		s := loggedObservation(t, logs)
		if s["generation_outcome"] != "completed" || s["transport_outcome"] != "returned" {
			t.Fatalf("bad outcomes: %+v", s)
		}
		u := s["usage"].(map[string]any)
		if u["prompt_tokens"] != float64(0) || u["completion_tokens"] != float64(2) {
			t.Fatal("late usage lost")
		}
		for _, secret := range []string{"PRIVATE_COMPLETION", "PRIVATE_MODEL", "PRIVATE_PROMPT", "PRIVATE_AUTH", "PRIVATE_HEADER"} {
			if strings.Contains(logs, secret) {
				t.Fatal("secret in request logs")
			}
		}
	})
}

func TestObservationFlushBeforeCompletionAndHeaderPhase(t *testing.T) {
	release := make(chan struct{})
	first := ": heartbeat\n\n"
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		time.Sleep(5 * time.Millisecond)
		_, _ = io.WriteString(w, first)
		w.(http.Flusher).Flush()
		<-release
		_, _ = io.WriteString(w, observedStream)
	}))
	defer backend.Close()
	cfg := baseConfig(backend.URL)
	cfg.RequestObservation = true
	h, logs := observedHandler(t, cfg)
	server := httptest.NewServer(h)
	defer server.Close()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	client := &http.Client{Timeout: 3 * time.Second}
	r, err := client.Post(server.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"model":"m","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	reader := bufio.NewReader(r.Body)
	line, err := reader.ReadString('\n')
	if err != nil || line != ": heartbeat\n" {
		t.Fatal("stream was buffered")
	}
	close(release)
	rest, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if line+string(rest) != first+observedStream {
		t.Fatal("stream bytes changed")
	}
	s := loggedObservation(t, logs.String())
	head, ok := s["upstream_header_ms"].(float64)
	if !ok {
		t.Fatal("real upstream header not observed")
	}
	body, ok := s["first_byte_ms"].(float64)
	if !ok || body <= head {
		t.Fatalf("header/body timing conflated: %+v", s)
	}
}

func TestObservationCompressedBypass(t *testing.T) {
	var zipped bytes.Buffer
	zw := gzip.NewWriter(&zipped)
	_, _ = io.WriteString(zw, observedStream)
	_ = zw.Close()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Content-Encoding", "gzip")
		_, _ = w.Write(zipped.Bytes())
	}))
	defer backend.Close()
	cfg := baseConfig(backend.URL)
	cfg.RequestObservation = true
	h, logs := observedHandler(t, cfg)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":"m"}`))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Accept-Encoding", "gzip")
	h.ServeHTTP(w, r)
	if !bytes.Equal(w.Body.Bytes(), zipped.Bytes()) || w.Header().Get("Content-Encoding") != "gzip" {
		t.Fatal("compressed passthrough changed")
	}
	s := loggedObservation(t, logs.String())
	if s["inspection_state"] != "uninspected" || s["generation_outcome"] != "uninspected" {
		t.Fatal("compressed response inspected")
	}
}

func TestObservationAbortAndCancellationNeverComplete(t *testing.T) {
	cfg := baseConfig("http://backend.invalid")
	cfg.RequestObservation = true
	handler, logs := observedHandler(t, cfg)
	h := handler.(*Handler)
	h.proxies["default"].Transport = panicHistoryTransport{}
	w := httptest.NewRecorder()
	var caught any
	func() {
		defer func() { caught = recover() }()
		r := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":"m"}`))
		r.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(w, r)
	}()
	if caught != http.ErrAbortHandler {
		t.Fatal("original abort not preserved")
	}
	s := loggedObservation(t, logs.String())
	if s["transport_outcome"] != "aborted" || s["generation_outcome"] == "completed" {
		t.Fatal("aborted response called successful")
	}
	handler, logs = observedHandler(t, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":"m"}`)).WithContext(ctx)
	r.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(httptest.NewRecorder(), r)
	s = loggedObservation(t, logs.String())
	if s["transport_outcome"] != "canceled" || s["generation_outcome"] == "completed" {
		t.Fatalf("cancellation lost: %+v", s)
	}
}

func TestObservationRouteGateAndReload(t *testing.T) {
	for _, path := range []string{"/health", "/ready", "/v1/models", "/diagnostics"} {
		r := httptest.NewRequest(http.MethodPost, path, nil)
		if isGenerationPath(r) {
			t.Fatal("local route observed")
		}
	}
	r := httptest.NewRequest(http.MethodPost, "/v1/completions", nil)
	r.Header.Set("Upgrade", "websocket")
	if isGenerationPath(r) {
		t.Fatal("upgrade observed")
	}
	backend := markerServer("ok")
	defer backend.Close()
	path := configPath(t)
	writeReloaderConfig(t, path, backend.URL)
	reload, _, err := NewReloader(path, discardLogger(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if reload.state.Load().handler.(*Handler).requestObservation {
		t.Fatal("enabled by default")
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	contents = append(contents, []byte("\nrequest_observation: true\n")...)
	if err = os.WriteFile(path, contents, 0600); err != nil {
		t.Fatal(err)
	}
	if err = reload.reload(); err != nil {
		t.Fatal(err)
	}
	if !reload.state.Load().handler.(*Handler).requestObservation {
		t.Fatal("reload did not enable observer")
	}
}

// newObservedStreamBackend returns an httptest server that echoes observedStream
// with Content-Type text/event-stream, mirroring the shared observedStream
// fixture used across observation tests.
func newObservedStreamBackend(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, observedStream)
	}))
}

func TestObservationConcurrentRequests(t *testing.T) {
	backend := newObservedStreamBackend(t)
	defer backend.Close()
	cfg := baseConfig(backend.URL)
	cfg.RequestObservation = true
	h, logs := observedHandler(t, cfg)
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":"m","stream":true}`))
			r.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Body.String() != observedStream {
				t.Error("concurrent passthrough changed")
			}
		}()
	}
	wg.Wait()
	if strings.Count(logs.String(), `"msg":"request_observation"`) != 12 {
		t.Fatal("observations were lost or mixed across requests")
	}
}

// fourFlagCombo builds a handler for the given RequestHistory/RequestObservation
// flags, issues a streaming chat request against the shared backend, and returns
// the handler, captured log text, and response recorder.
func fourFlagCombo(t *testing.T, backend *httptest.Server, history, observation bool) (*Handler, string, *httptest.ResponseRecorder) {
	t.Helper()
	cfg := baseConfig(backend.URL)
	cfg.RequestHistory = history
	cfg.RequestObservation = observation
	logger, logs := captureLogger()
	h, err := New(cfg, logger, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler := h.(*Handler)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"m","stream":true}`))
	r.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(w, r)
	return handler, logs.String(), w
}

func assertFourFlagPassthrough(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()
	if w.Body.String() != observedStream {
		t.Fatal("passthrough changed")
	}
}

func assertFourFlagRequestID(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	requestID := w.Header().Get("X-Request-ID")
	if requestID == "" {
		t.Fatal("no request ID")
	}
	return requestID
}

func assertFourFlagObservationLog(t *testing.T, logs string, want bool) {
	t.Helper()
	if want {
		if !strings.Contains(logs, "request_observation") {
			t.Fatal("observation log missing")
		}
	} else {
		if strings.Contains(logs, "request_observation") {
			t.Fatal("observation log should not be present")
		}
	}
}

func assertFourFlagHistory(t *testing.T, handler *Handler, requestID string, history, observation bool) {
	t.Helper()
	if history {
		if handler.history == nil {
			t.Fatal("history should be non-nil")
		}
		record, ok := handler.history.GetByRequestID(requestID)
		if !ok {
			t.Fatal("no history record")
		}
		assertFourFlagObservationMetadata(t, record, observation)
	} else {
		if handler.history != nil {
			t.Fatal("history should be nil when disabled")
		}
	}
}

func assertFourFlagObservationMetadata(t *testing.T, record *correlation.CorrelationRecord, observation bool) {
	t.Helper()
	if observation {
		if record.ProxyObservation == nil {
			t.Fatal("ProxyObservation should be present")
		}
		if record.ProxyObservation.Source != "proxy" {
			t.Fatalf("source = %q, want proxy", record.ProxyObservation.Source)
		}
		if record.ProxyObservation.Scope != "proxy_request_only" {
			t.Fatalf("scope = %q, want proxy_request_only", record.ProxyObservation.Scope)
		}
		if record.ProxyObservation.UpstreamRequestMapping != "not_established" {
			t.Fatalf("mapping = %q, want not_established", record.ProxyObservation.UpstreamRequestMapping)
		}
		if record.ActivityID != "" {
			t.Fatalf("activity ID should be empty, got %q", record.ActivityID)
		}
		if record.ModelID != "" {
			t.Fatalf("model ID should be empty, got %q", record.ModelID)
		}
	} else {
		if record.ProxyObservation != nil {
			t.Fatal("ProxyObservation should be nil when observation disabled")
		}
	}
}

func TestObservationFourFlagCombos(t *testing.T) {
	backend := newObservedStreamBackend(t)
	defer backend.Close()

	combos := []struct {
		name        string
		history     bool
		observation bool
		wantRecord  bool
		wantObsLog  bool
	}{
		{"both false", false, false, false, false},
		{"history only", true, false, true, false},
		{"observation only", false, true, false, true},
		{"both true", true, true, true, true},
	}

	for _, combo := range combos {
		t.Run(combo.name, func(t *testing.T) {
			handler, logs, w := fourFlagCombo(t, backend, combo.history, combo.observation)

			assertFourFlagPassthrough(t, w)
			requestID := assertFourFlagRequestID(t, w)
			assertFourFlagObservationLog(t, logs, combo.observation)
			assertFourFlagHistory(t, handler, requestID, combo.history, combo.observation)
		})
	}
}

// openAIStyleRequest builds a handler for the given backend, enables request
// observation, issues a streaming chat request, and returns the response
// recorder and captured log text.
func openAIStyleRequest(t *testing.T, backend *httptest.Server) (*httptest.ResponseRecorder, string) {
	t.Helper()
	cfg := baseConfig(backend.URL)
	cfg.RequestObservation = true
	h, logs := observedHandler(t, cfg)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","stream":true}`))
	r.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(w, r)
	return w, logs.String()
}

func assertOpenAIStyleSecrets(t *testing.T, logs string) {
	t.Helper()
	for _, secret := range []string{"hello", "boom"} {
		if strings.Contains(logs, secret) {
			t.Fatalf("secret %q leaked into logs", secret)
		}
	}
}

func TestObservationOpenAIStyles(t *testing.T) {
	sseUsage0 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"},\"finish_reason\":\"stop\"}]}\n\ndata: {\"usage\":{\"prompt_tokens\":0,\"completion_tokens\":0}}\n\ndata: [DONE]\n\n")
	}))
	defer sseUsage0.Close()

	sseUsageMissing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	}))
	defer sseUsageMissing.Close()

	sseTruncated := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"},\"finish_reason\":\"length\"}]}\n\ndata: {\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1}}\n\ndata: [DONE]\n\n")
	}))
	defer sseTruncated.Close()

	jsonBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":0,"completion_tokens":0}}`)
	}))
	defer jsonBackend.Close()

	http200Backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer http200Backend.Close()

	abortBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		panic("boom")
	}))
	defer abortBackend.Close()

	tests := []struct {
		name     string
		backend  *httptest.Server
		wantCode int
		check    func(t *testing.T, summary map[string]any, code int)
	}{
		{
			name:     "sse usage0",
			backend:  sseUsage0,
			wantCode: http.StatusOK,
			check: func(t *testing.T, summary map[string]any, code int) {
				u := summary["usage"].(map[string]any)
				if u["prompt_tokens"] != float64(0) || u["completion_tokens"] != float64(0) {
					t.Fatalf("usage = %+v, want 0/0", u)
				}
				if summary["generation_outcome"] != "completed" {
					t.Fatalf("generation_outcome = %q, want completed", summary["generation_outcome"])
				}
			},
		},
		{
			name:     "sse usage missing",
			backend:  sseUsageMissing,
			wantCode: http.StatusOK,
			check: func(t *testing.T, summary map[string]any, code int) {
				if summary["usage"] == nil {
					t.Fatal("usage should be present (nil map)")
				}
				u := summary["usage"].(map[string]any)
				if u["prompt_tokens"] != nil || u["completion_tokens"] != nil {
					t.Fatalf("usage should be unknown (nil pointers), got %+v", u)
				}
			},
		},
		{
			name:     "sse truncated",
			backend:  sseTruncated,
			wantCode: http.StatusOK,
			check: func(t *testing.T, summary map[string]any, code int) {
				if summary["generation_outcome"] != "truncated" {
					t.Fatalf("generation_outcome = %q, want truncated", summary["generation_outcome"])
				}
			},
		},
		{
			name:     "json usage0",
			backend:  jsonBackend,
			wantCode: http.StatusOK,
			check: func(t *testing.T, summary map[string]any, code int) {
				u := summary["usage"].(map[string]any)
				if u["prompt_tokens"] != float64(0) || u["completion_tokens"] != float64(0) {
					t.Fatalf("usage = %+v, want 0/0", u)
				}
				if summary["generation_outcome"] != "completed" {
					t.Fatalf("generation_outcome = %q, want completed", summary["generation_outcome"])
				}
			},
		},
		{
			name:     "http200 protected",
			backend:  http200Backend,
			wantCode: http.StatusOK,
			check: func(t *testing.T, summary map[string]any, code int) {
				if code != http.StatusOK {
					t.Fatalf("code = %d, want 200", code)
				}
				if summary["transport_outcome"] != "returned" {
					t.Fatalf("transport_outcome = %q, want returned", summary["transport_outcome"])
				}
				if summary["generation_outcome"] == "completed" {
					t.Fatal("HTTP 200 alone established generation completion")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, logs := openAIStyleRequest(t, tt.backend)
			if w.Code != tt.wantCode {
				t.Fatalf("code = %d, want %d", w.Code, tt.wantCode)
			}
			if w.Body.String() == "" {
				t.Fatal("empty body")
			}
			s := loggedObservation(t, logs)
			tt.check(t, s, w.Code)
			assertOpenAIStyleSecrets(t, logs)
		})
	}
}

func TestObservationUnknownFormatRetainsNoGenerationEvidence(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, observedStream)
	}))
	defer backend.Close()
	cfg := baseConfig(backend.URL)
	cfg.RequestObservation = true
	cfg.RequestHistory = true
	h, logs := observedHandler(t, cfg)
	r := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":"m"}`))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Body.String() != observedStream || w.Code != http.StatusOK {
		t.Fatal("unsupported format forwarding changed")
	}
	summary := loggedObservation(t, logs.String())
	if summary["generation_outcome"] != "uninspected" || summary["inspection_state"] != "uninspected" {
		t.Fatal("unsupported format was interpreted")
	}
	record, ok := h.(*Handler).history.GetByRequestID(w.Header().Get("X-Request-ID"))
	if !ok || record.ProxyObservation == nil || record.ProxyObservation.GenerationOutcome != "uninspected" || record.ProxyObservation.CompletionTokens != nil || record.ProxyObservation.FirstOutputMs != nil {
		t.Fatal("history invented body-derived evidence for uninspected format")
	}
}

func TestObservationAbortAndCanceled(t *testing.T) {
	// Aborted: backend panics.
	abortBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		panic("boom")
	}))
	defer abortBackend.Close()
	cfg := baseConfig(abortBackend.URL)
	cfg.RequestObservation = true
	h, logs := observedHandler(t, cfg)
	handler := h.(*Handler)
	handler.proxies["default"].Transport = &panicTransport{}
	w := httptest.NewRecorder()
	var caught any
	func() {
		defer func() { caught = recover() }()
		r := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":"m"}`))
		r.Header.Set("Content-Type", "application/json")
		handler.ServeHTTP(w, r)
	}()
	if caught != http.ErrAbortHandler {
		t.Fatalf("panic = %v, want ErrAbortHandler", caught)
	}
	s := loggedObservation(t, logs.String())
	if s["transport_outcome"] != "aborted" || s["generation_outcome"] == "completed" {
		t.Fatalf("abort not recorded: %+v", s)
	}

	// Canceled: client cancels.
	cfg2 := baseConfig("http://backend.invalid")
	cfg2.RequestObservation = true
	h2, logs2 := observedHandler(t, cfg2)
	handler2 := h2.(*Handler)
	handler2.proxies["default"].Transport = &httpTransport{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":"m"}`)).WithContext(ctx)
	r.Header.Set("Content-Type", "application/json")
	handler2.ServeHTTP(httptest.NewRecorder(), r)
	s = loggedObservation(t, logs2.String())
	if s["transport_outcome"] != "canceled" || s["generation_outcome"] == "completed" {
		t.Fatalf("cancellation not recorded: %+v", s)
	}
}

type panicTransport struct{}

func (panicTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: panicBody{}, ContentLength: -1}, nil
}

type panicBody struct{}

func (panicBody) Read([]byte) (int, error) {
	panic(http.ErrAbortHandler)
}

func (panicBody) Close() error { return nil }

type httpTransport struct{}

func (httpTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: httpBody{}, ContentLength: -1}, nil
}

type httpBody struct{}

func (httpBody) Read([]byte) (int, error) {
	return 0, io.EOF
}

func (httpBody) Close() error { return nil }

// newObservedHandler returns a handler with request history and observation
// enabled against the given backend, using a discard logger. It preserves the
// exact config flags and logger of the tests that share this setup.
func newObservedHandler(t *testing.T, backend *httptest.Server) *Handler {
	t.Helper()
	cfg := baseConfig(backend.URL)
	cfg.RequestHistory = true
	cfg.RequestObservation = true
	handler, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return handler.(*Handler)
}

// serveObservedStream issues a streaming chat request through h and asserts the
// response body is byte-exact observedStream, returning the recorder.
func serveObservedStream(t *testing.T, h *Handler) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"m","stream":true}`)))
	if w.Body.String() != observedStream {
		t.Fatal("passthrough changed")
	}
	return w
}

func TestObservationHistorySecrets(t *testing.T) {
	backend := newObservedStreamBackend(t)
	defer backend.Close()

	h := newObservedHandler(t, backend)

	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions?token=super-secret&key=query-key",
		strings.NewReader(`{"model":"qwen","password":"sensitive-pw","stream":true}`))
	request.Header.Set("Authorization", "Bearer client-secret-token")
	request.Header.Set("X-Api-Key", "client-api-key")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, request)
	if w.Body.String() != observedStream {
		t.Fatal("passthrough changed")
	}

	requestID := w.Header().Get("X-Request-ID")
	record, ok := h.history.GetByRequestID(requestID)
	if !ok {
		t.Fatal("no history record")
	}
	recordJSON, _ := json.Marshal(record)
	recordStr := string(recordJSON)
	for _, secret := range []string{"super-secret", "query-key", "sensitive-pw", "client-secret-token", "client-api-key", "qwen", "PRIVATE_COMPLETION", "PRIVATE_PROMPT", "PRIVATE_HEADER"} {
		if strings.Contains(recordStr, secret) {
			t.Fatalf("secret %q in history JSON: %s", secret, recordStr)
		}
	}
	// Allowed fields should be present.
	if !strings.Contains(recordStr, "request_id") || !strings.Contains(recordStr, "backend_id") {
		t.Fatalf("missing allowed fields in history JSON: %s", recordStr)
	}
}

func TestObservationHealthCatalogSkipAttachment(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/v1/models" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"data":[]}`)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	h := newObservedHandler(t, backend)

	for _, path := range []string{"/health", "/ready", "/v1/models"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != http.StatusOK {
			t.Fatalf("path %s code = %d", path, w.Code)
		}
	}
	// Local routes have empty backendID, so no history records are created.
	if h.history.Stats()["record_count"] != 0 {
		t.Fatal("local routes should not create history records")
	}
}

func TestObservationReloadResetsStore(t *testing.T) {
	serverA := markerServer("A")
	defer serverA.Close()
	serverB := markerServer("B")
	defer serverB.Close()

	path := configPath(t)
	writeReloaderConfig(t, path, serverA.URL)
	reloader, _, err := NewReloader(path, discardLogger(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	reloader.SetWatchInterval(20 * time.Millisecond)
	reloader.Start()
	t.Cleanup(reloader.Stop)

	if got := probeUpstream(t, reloader); got != "A" {
		t.Fatalf("initial upstream = %q, want A", got)
	}

	// Enable request_history and request_observation, point at B.
	contents := "default_backend: default\nrequest_history: true\nrequest_observation: true\nbackends:\n  default:\n    url: " + serverB.URL + "\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := waitForUpstream(t, reloader, "B"); err != nil {
		t.Fatalf("config change was not applied: %v", err)
	}

	reloadedHandler := reloader.state.Load().handler.(*Handler)
	if reloadedHandler.history == nil {
		t.Fatal("history should be non-nil after reload")
	}

	// Make a request through the reloaded handler.
	recorder := httptest.NewRecorder()
	reloadedHandler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"m","stream":true}`)))
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("code = %d, want 204", recorder.Code)
	}

	requestID := recorder.Header().Get("X-Request-ID")
	record, ok := reloadedHandler.history.GetByRequestID(requestID)
	if !ok {
		t.Fatal("no history record after reload")
	}
	if record.ProxyObservation == nil {
		t.Fatal("ProxyObservation should be present after reload")
	}
	if record.ProxyObservation.Source != "proxy" {
		t.Fatalf("source = %q, want proxy", record.ProxyObservation.Source)
	}
}

func TestObservationUnknownModel(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer backend.Close()

	h := newObservedHandler(t, backend)

	// Unknown model (no prefix match) routes to default backend with empty ModelID.
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"unknown-model","stream":true}`)))
	if w.Code != http.StatusNoContent {
		t.Fatalf("code = %d, want 204", w.Code)
	}

	requestID := w.Header().Get("X-Request-ID")
	record, ok := h.history.GetByRequestID(requestID)
	if !ok {
		t.Fatal("no history record")
	}
	if record.ModelID != "" {
		t.Fatalf("model ID should be empty for unknown model, got %q", record.ModelID)
	}
	if record.ActivityID != "" {
		t.Fatalf("activity ID should be empty, got %q", record.ActivityID)
	}
}

func TestObservationRequestIDLookup(t *testing.T) {
	backend := newObservedStreamBackend(t)
	defer backend.Close()

	h := newObservedHandler(t, backend)

	w := serveObservedStream(t, h)

	requestID := w.Header().Get("X-Request-ID")
	if requestID == "" {
		t.Fatal("no request ID")
	}
	if len(requestID) != 32 {
		t.Fatalf("request ID length = %d, want 32", len(requestID))
	}

	// Look up the record by the returned request ID.
	record, ok := h.history.GetByRequestID(requestID)
	if !ok {
		t.Fatal("no history record for returned request ID")
	}
	if record.RequestID != requestID {
		t.Fatalf("record request ID = %q, want %q", record.RequestID, requestID)
	}
	if record.ProxyObservation == nil {
		t.Fatal("ProxyObservation should be present")
	}
}

func TestObservationConcurrentIndependent(t *testing.T) {
	backend := newObservedStreamBackend(t)
	defer backend.Close()

	cfg := baseConfig(backend.URL)
	cfg.RequestHistory = true
	cfg.RequestObservation = true
	h, logs := observedHandler(t, cfg)
	handler := h.(*Handler)

	var wg sync.WaitGroup
	ids := make([]string, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":"m","stream":true}`))
			r.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if w.Body.String() != observedStream {
				t.Errorf("concurrent passthrough changed for request %d", i)
			}
			ids[i] = w.Header().Get("X-Request-ID")
		}(i)
	}
	wg.Wait()

	if strings.Count(logs.String(), `"msg":"request_observation"`) != 12 {
		t.Fatal("observations were lost or mixed across requests")
	}

	// Each request must have its own independent record.
	seen := make(map[string]bool)
	for _, id := range ids {
		record, ok := handler.history.GetByRequestID(id)
		if !ok {
			t.Fatalf("no record for request ID %q", id)
		}
		if record.ProxyObservation == nil {
			t.Fatalf("no ProxyObservation for request ID %q", id)
		}
		if seen[id] {
			t.Fatalf("duplicate request ID %q", id)
		}
		seen[id] = true
	}
}

func TestObservationRetentionBounds(t *testing.T) {
	backend := newObservedStreamBackend(t)
	defer backend.Close()

	h := newObservedHandler(t, backend)

	// Tighten retention to a small count.
	h.history.WithRetention(3, 0, 0)

	for i := 0; i < 5; i++ {
		serveObservedStream(t, h)
	}

	stats := h.history.Stats()
	if stats["record_count"] != 3 {
		t.Fatalf("record count = %v, want 3 after count retention", stats["record_count"])
	}
}

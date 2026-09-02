package proxy

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

// ollamaBackend simulates an Ollama server: liveness via GET /api/version, no
// /health endpoint, OpenAI-compatible /v1 endpoints, native /api/chat, and
// model details via POST /api/show.
type ollamaBackend struct {
	server       *httptest.Server
	chats        chan string
	showDetails  map[string]string
	showFail     atomic.Bool
	showLatency  time.Duration
	showInFlight atomic.Int64
	showPeak     atomic.Int64
}

func newOllamaBackend(t *testing.T, models ...string) *ollamaBackend {
	t.Helper()
	data := make([]map[string]any, 0, len(models))
	for _, model := range models {
		data = append(data, map[string]any{"id": model, "object": "model", "created": 0, "owned_by": "library"})
	}
	modelsJSON, err := json.Marshal(map[string]any{"object": "list", "data": data})
	if err != nil {
		t.Fatal(err)
	}
	backend := &ollamaBackend{chats: make(chan string, 16), showDetails: make(map[string]string, len(models))}
	for _, model := range models {
		backend.showDetails[model] = fmt.Sprintf(`{
			"capabilities": ["completion", "toolcalling"],
			"details": {
				"family": "qwen3",
				"format": "safetensors",
				"parameter_size": "8.3B",
				"quantization_level": "Q4_K_M",
				"context_length": 40960,
				"max_completion_tokens": 8192
			}
		}`)
	}
	backend.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodGet {
			switch request.URL.Path {
			case "/api/version":
				writer.Header().Set("Content-Type", "application/json")
				_, _ = writer.Write([]byte(`{"version":"0.11.0"}`))
			case "/v1/models":
				writer.Header().Set("Content-Type", "application/json")
				_, _ = writer.Write(modelsJSON)
			default:
				writer.WriteHeader(http.StatusNotFound)
			}
			return
		}
		if request.URL.Path == "/api/show" {
			current := backend.showInFlight.Add(1)
			for {
				peak := backend.showPeak.Load()
				if current <= peak || backend.showPeak.CompareAndSwap(peak, current) {
					break
				}
			}
			defer backend.showInFlight.Add(-1)
			var showRequest struct {
				Model string `json:"model"`
			}
			if err := json.NewDecoder(request.Body).Decode(&showRequest); err != nil {
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			if backend.showFail.Load() {
				writer.WriteHeader(http.StatusInternalServerError)
				return
			}
			details, exists := backend.showDetails[showRequest.Model]
			if !exists {
				writer.WriteHeader(http.StatusNotFound)
				return
			}
			if backend.showLatency > 0 {
				time.Sleep(backend.showLatency)
			}
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(details))
			return
		}
		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		model, _ := body["model"].(string)
		backend.chats <- request.URL.Path + " " + model
		if stream, _ := body["stream"].(bool); stream {
			writer.Header().Set("Content-Type", "text/event-stream")
			flusher := writer.(http.Flusher)
			_, _ = writer.Write([]byte("data: {\"model\":\"" + model + "\"}\n\n"))
			flusher.Flush()
			_, _ = writer.Write([]byte("data: [DONE]\n\n"))
			flusher.Flush()
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		response, _ := json.Marshal(map[string]any{"model": model, "path": request.URL.Path})
		_, _ = writer.Write(response)
	}))
	t.Cleanup(func() { backend.server.Close() })
	return backend
}

func openaiCatalogServer(t *testing.T, models ...string) *httptest.Server {
	t.Helper()
	data := make([]map[string]any, 0, len(models))
	for _, model := range models {
		data = append(data, map[string]any{"id": model, "object": "model", "created": 0, "owned_by": "default"})
	}
	modelsJSON, err := json.Marshal(map[string]any{"object": "list", "data": data})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.Method + " " + request.URL.Path {
		case "GET /health":
			writer.WriteHeader(http.StatusOK)
		case "GET /v1/models":
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write(modelsJSON)
		default:
			writer.WriteHeader(http.StatusNoContent)
		}
	}))
	t.Cleanup(func() { server.Close() })
	return server
}

func TestOllamaDefaultBackendReadinessUsesConfiguredHealthPath(t *testing.T) {
	ollama := newOllamaBackend(t, "qwen3:8b")
	cfg := baseConfig(ollama.server.URL)
	cfg.Backends["default"] = config.Backend{URL: ollama.server.URL, HealthPath: "/api/version"}
	handler := newHandler(t, cfg)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"default":"ready"`) {
		t.Fatalf("ready response = %d %s", recorder.Code, recorder.Body.String())
	}
}

func TestOllamaDefaultBackendWithoutHealthPathIsNotReady(t *testing.T) {
	ollama := newOllamaBackend(t, "qwen3:8b")
	handler := newHandler(t, baseConfig(ollama.server.URL))

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), `"default":"unavailable"`) {
		t.Fatalf("ready response = %d %s", recorder.Code, recorder.Body.String())
	}
}

func TestOllamaModelsAggregateWithConfiguredPrefix(t *testing.T) {
	ollama := newOllamaBackend(t, "qwen3:8b", "llama3.2:3b")
	defaultServer := openaiCatalogServer(t, "local-model")
	cfg := baseConfig(defaultServer.URL)
	cfg.Backends["ollama"] = config.Backend{URL: ollama.server.URL, Prefix: "ollama/", HealthPath: "/api/version"}
	handler := newHandler(t, cfg)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d", recorder.Code)
	}
	var catalog struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&catalog); err != nil {
		t.Fatal(err)
	}
	ids := make(map[string]bool, len(catalog.Data))
	for _, model := range catalog.Data {
		ids[model.ID] = true
	}
	for _, want := range []string{"local-model", "ollama/qwen3:8b", "ollama/llama3.2:3b"} {
		if !ids[want] {
			t.Fatalf("catalog IDs = %#v", ids)
		}
	}
}

func TestOllamaOpenAIChatCompletionsRouteAndStripPrefix(t *testing.T) {
	ollama := newOllamaBackend(t, "qwen3:8b")
	handler := ollamaPrefixedHandler(t, ollama)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"ollama/qwen3:8b","messages":[]}`)))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	expectChatRequest(t, ollama, "/v1/chat/completions qwen3:8b")
}

func TestOllamaNativeChatRoutesAndStripsPrefix(t *testing.T) {
	ollama := newOllamaBackend(t, "qwen3:8b")
	handler := ollamaPrefixedHandler(t, ollama)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/chat",
		strings.NewReader(`{"model":"ollama/qwen3:8b","messages":[]}`)))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	expectChatRequest(t, ollama, "/api/chat qwen3:8b")
}

func TestOllamaOpenAICompatEndpointsRoute(t *testing.T) {
	for _, path := range []string{"/v1/completions", "/v1/embeddings", "/v1/responses"} {
		t.Run(path, func(t *testing.T) {
			ollama := newOllamaBackend(t, "qwen3:8b")
			handler := ollamaPrefixedHandler(t, ollama)

			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, path,
				strings.NewReader(`{"model":"ollama/qwen3:8b"}`)))
			if recorder.Code != http.StatusOK {
				t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
			}
			expectChatRequest(t, ollama, path+" qwen3:8b")
		})
	}
}

func TestOllamaStreamingChatCompletionsProxySSE(t *testing.T) {
	ollama := newOllamaBackend(t, "qwen3:8b")
	cfg := baseConfig(ollama.server.URL)
	cfg.Backends["default"] = config.Backend{URL: ollama.server.URL, HealthPath: "/api/version"}
	proxyServer := httptest.NewServer(newHandler(t, cfg))
	t.Cleanup(proxyServer.Close)

	response, err := (&http.Client{Timeout: 3 * time.Second}).Post(proxyServer.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"qwen3:8b","stream":true,"messages":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("content type = %q", response.Header.Get("Content-Type"))
	}
	line, err := bufio.NewReader(response.Body).ReadString('\n')
	if err != nil || !strings.HasPrefix(line, "data: ") {
		t.Fatalf("first event = %q, error = %v", line, err)
	}
	expectChatRequest(t, ollama, "/v1/chat/completions qwen3:8b")
}

func TestOllamaShowEnrichmentAddsNamespacedMetadata(t *testing.T) {
	ollama := newOllamaBackend(t, "qwen3:8b", "llama3.2:3b")
	defaultServer := openaiCatalogServer(t, "local-model")
	cfg := baseConfig(defaultServer.URL)
	cfg.Backends["ollama"] = config.Backend{
		URL: ollama.server.URL, Prefix: "ollama/", HealthPath: "/api/version",
		OllamaShowEnrichment: true,
	}
	handler := newHandler(t, cfg)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d", recorder.Code)
	}
	catalog := ollamaCatalog(t, recorder)
	for _, want := range []string{"ollama/qwen3:8b", "ollama/llama3.2:3b"} {
		model := catalog[want]
		if model == nil {
			t.Fatalf("missing model %q", want)
		}
		metadata, ok := model["ollama"].(map[string]any)
		if !ok {
			t.Fatalf("model %q has no ollama metadata: %#v", want, model)
		}
		if metadata["family"] != "qwen3" || metadata["parameter_size"] != "8.3B" ||
			metadata["quantization_level"] != "Q4_K_M" || metadata["context_length"] != json.Number("40960") ||
			metadata["max_completion_tokens"] != json.Number("8192") || metadata["format"] != "safetensors" {
			t.Fatalf("model %q metadata = %#v", want, metadata)
		}
		capabilities, ok := metadata["capabilities"].([]any)
		if !ok || len(capabilities) != 2 || capabilities[0] != "completion" || capabilities[1] != "toolcalling" {
			t.Fatalf("model %q capabilities = %#v", want, metadata["capabilities"])
		}
	}
}

func TestOllamaShowEnrichmentFallsBackToModelInfoContextLength(t *testing.T) {
	ollama := newOllamaBackend(t, "gemma4")
	ollama.showDetails["gemma4"] = `{
		"capabilities": ["completion"],
		"details": {"family": "gemma4", "format": "gguf", "parameter_size": "8.0B", "quantization_level": "Q4_K_M"},
		"model_info": {"general.architecture": "gemma4", "gemma4.context_length": 131072}
	}`
	defaultServer := openaiCatalogServer(t, "local-model")
	cfg := baseConfig(defaultServer.URL)
	cfg.Backends["ollama"] = config.Backend{
		URL: ollama.server.URL, Prefix: "ollama/", HealthPath: "/api/version",
		OllamaShowEnrichment: true,
	}
	handler := newHandler(t, cfg)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	catalog := ollamaCatalog(t, recorder)
	metadata, ok := catalog["ollama/gemma4"]["ollama"].(map[string]any)
	if !ok {
		t.Fatal("model has no ollama metadata")
	}
	if metadata["context_length"] != json.Number("131072") {
		t.Fatalf("context length = %#v", metadata["context_length"])
	}
}

func TestOllamaShowEnrichmentDisabledByDefault(t *testing.T) {
	ollama := newOllamaBackend(t, "qwen3:8b")
	cfg := baseConfig(noContentServer().URL)
	cfg.Backends["ollama"] = config.Backend{URL: ollama.server.URL, Prefix: "ollama/", HealthPath: "/api/version"}
	handler := newHandler(t, cfg)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	catalog := ollamaCatalog(t, recorder)
	if _, exists := catalog["ollama/qwen3:8b"]["ollama"]; exists {
		t.Fatal("ollama metadata present although enrichment is disabled")
	}
}

func TestOllamaShowEnrichmentFailureLeavesCatalogUsable(t *testing.T) {
	ollama := newOllamaBackend(t, "qwen3:8b", "llama3.2:3b")
	delete(ollama.showDetails, "llama3.2:3b")
	defaultServer := openaiCatalogServer(t, "local-model")
	cfg := baseConfig(defaultServer.URL)
	cfg.Backends["ollama"] = config.Backend{
		URL: ollama.server.URL, Prefix: "ollama/", HealthPath: "/api/version",
		OllamaShowEnrichment: true,
	}
	handler := newHandler(t, cfg)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d", recorder.Code)
	}
	catalog := ollamaCatalog(t, recorder)
	if metadata, exists := catalog["ollama/qwen3:8b"]["ollama"]; !exists || metadata == nil {
		t.Fatal("healthy model lost its ollama metadata")
	}
	if _, exists := catalog["ollama/llama3.2:3b"]["ollama"]; exists {
		t.Fatal("model with a failing /api/show response gained metadata")
	}
}

func TestOllamaShowEnrichmentErrorDoesNotFailReadiness(t *testing.T) {
	ollama := newOllamaBackend(t, "qwen3:8b")
	ollama.showFail.Store(true)
	cfg := baseConfig(ollama.server.URL)
	cfg.Backends["default"] = config.Backend{URL: ollama.server.URL, HealthPath: "/api/version", OllamaShowEnrichment: true}
	handler := newHandler(t, cfg)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "qwen3:8b") {
		t.Fatalf("catalog response = %d %s", recorder.Code, recorder.Body.String())
	}
}

func TestOllamaShowEnrichmentFetchesConcurrently(t *testing.T) {
	models := make([]string, 16)
	for index := range models {
		models[index] = fmt.Sprintf("model-%d", index)
	}
	ollama := newOllamaBackend(t, models...)
	ollama.showLatency = 50 * time.Millisecond
	cfg := baseConfig(ollama.server.URL)
	cfg.Backends["default"] = config.Backend{URL: ollama.server.URL, HealthPath: "/api/version", OllamaShowEnrichment: true}
	handler := newHandler(t, cfg)

	start := time.Now()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	elapsed := time.Since(start)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d", recorder.Code)
	}
	if peak := ollama.showPeak.Load(); peak < 4 {
		t.Fatalf("show concurrency peak = %d, requests appear serialized", peak)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("enrichment took %s, requests appear serialized", elapsed)
	}
}

func TestOllamaShowEnrichmentSurvivesCatalogCache(t *testing.T) {
	ollama := newOllamaBackend(t, "qwen3:8b")
	cfg := baseConfig(ollama.server.URL)
	cfg.Backends["default"] = config.Backend{URL: ollama.server.URL, HealthPath: "/api/version", OllamaShowEnrichment: true}
	handler := newHandler(t, cfg)

	for attempt := 1; attempt <= 2; attempt++ {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("status = %d", recorder.Code)
		}
		metadata, ok := ollamaCatalog(t, recorder)["qwen3:8b"]["ollama"].(map[string]any)
		if !ok || metadata["context_length"] != json.Number("40960") {
			t.Fatalf("attempt %d metadata = %#v", attempt, metadata)
		}
	}
}

func ollamaCatalog(t *testing.T, recorder *httptest.ResponseRecorder) map[string]map[string]any {
	t.Helper()
	var response struct {
		Data []map[string]any `json:"data"`
	}
	decoder := json.NewDecoder(recorder.Body)
	decoder.UseNumber()
	if err := decoder.Decode(&response); err != nil {
		t.Fatal(err)
	}
	catalog := make(map[string]map[string]any, len(response.Data))
	for _, model := range response.Data {
		id, _ := model["id"].(string)
		catalog[id] = model
	}
	return catalog
}

func ollamaPrefixedHandler(t *testing.T, ollama *ollamaBackend) http.Handler {
	t.Helper()
	return testHandler(t, noContentServer().URL, map[string]config.Backend{
		"ollama": {URL: ollama.server.URL, Prefix: "ollama/", HealthPath: "/api/version"},
	})
}

func expectChatRequest(t *testing.T, ollama *ollamaBackend, want string) {
	t.Helper()
	select {
	case chat := <-ollama.chats:
		if chat != want {
			t.Fatalf("received = %q, want %q", chat, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ollama backend did not receive the expected request")
	}
}

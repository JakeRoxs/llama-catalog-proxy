package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestModelsAggregatesAndPreservesBackendObjects(t *testing.T) {
	defaultServer := catalogServer(t, http.StatusOK, `{"object":"list","custom":"keep","data":[{"id":"local","context_length":8192,"owned_by":"local","unknown":{"keep":true}}]}`)
	defer defaultServer.Close()
	remoteServer := catalogServer(t, http.StatusOK, `{"data":[{"id":"model","name":"Remote model","context_length":262144,"meta":{"n_ctx":262144},"capabilities":{"tools":true},"status":{"value":"unloaded"}}]}`)
	defer remoteServer.Close()
	service := newService(t, defaultServer.URL, map[string]testBackend{"remote": {url: remoteServer.URL, prefix: "remote/"}}, time.Minute, nil)

	response, err := service.Models(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	root, models := decodeResponse(t, response.Body)
	if root["custom"] != "keep" {
		t.Error("default catalog root fields were not preserved")
	}
	local := models["local"]
	if local["owned_by"] != "local" || !reflect.DeepEqual(local["unknown"], map[string]any{"keep": true}) {
		t.Fatalf("local model fields changed: %#v", local)
	}
	remote := models["remote/model"]
	if remote["name"] != "Remote model" || remote["context_length"] != json.Number("262144") || remote["status"] == nil || remote["capabilities"] == nil {
		t.Fatalf("remote model fields changed: %#v", remote)
	}
}

func TestModelsOptionalBackendFailureDoesNotFailAggregation(t *testing.T) {
	defaultServer := catalogServer(t, http.StatusOK, `{"data":[{"id":"local"}]}`)
	defer defaultServer.Close()
	remoteServer := catalogServer(t, http.StatusServiceUnavailable, `offline`)
	defer remoteServer.Close()
	service := newService(t, defaultServer.URL, map[string]testBackend{"remote": {url: remoteServer.URL, prefix: "remote/"}}, time.Minute, nil)
	response, err := service.Models(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, models := decodeResponse(t, response.Body)
	if len(models) != 1 || models["local"] == nil {
		t.Fatalf("models = %#v", models)
	}
}

func TestModelsDefaultBackendFailureFailsAggregation(t *testing.T) {
	defaultServer := catalogServer(t, http.StatusServiceUnavailable, `offline`)
	defer defaultServer.Close()
	service := newService(t, defaultServer.URL, nil, time.Minute, nil)
	if _, err := service.Models(context.Background(), nil); err == nil {
		t.Fatal("default backend failure was ignored")
	}
}

func TestModelsRejectsDuplicatePublicID(t *testing.T) {
	defaultServer := catalogServer(t, http.StatusOK, `{"data":[{"id":"duplicate","source":"default"}]}`)
	defer defaultServer.Close()
	remoteServer := catalogServer(t, http.StatusOK, `{"data":[{"id":"duplicate","source":"remote"}]}`)
	defer remoteServer.Close()
	var logs bytes.Buffer
	service := newService(t, defaultServer.URL, map[string]testBackend{"remote": {url: remoteServer.URL}}, time.Minute, slog.New(slog.NewJSONHandler(&logs, nil)))
	response, err := service.Models(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, models := decodeResponse(t, response.Body)
	if len(models) != 1 || models["duplicate"]["source"] != "default" {
		t.Fatalf("collision result = %#v", models)
	}
	if !strings.Contains(logs.String(), "duplicate public model ID rejected") {
		t.Fatal("collision was not logged")
	}
}

func TestModelsUsesStaleBackendCatalogAfterRefreshFailure(t *testing.T) {
	defaultServer := catalogServer(t, http.StatusOK, `{"data":[]}`)
	defer defaultServer.Close()
	var unavailable atomic.Bool
	remoteServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		if unavailable.Load() {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		writeJSON(writer, http.StatusOK, `{"data":[{"id":"model"}]}`)
	}))
	defer remoteServer.Close()
	service := newService(t, defaultServer.URL, map[string]testBackend{"remote": {url: remoteServer.URL, prefix: "remote/"}}, 10*time.Millisecond, nil)
	if _, err := service.Models(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	unavailable.Store(true)
	time.Sleep(15 * time.Millisecond)
	response, err := service.Models(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, models := decodeResponse(t, response.Body)
	if models["remote/model"] == nil {
		t.Fatal("stale backend model was not returned")
	}
}

func TestConcurrentCatalogRequestsShareOneRefresh(t *testing.T) {
	var requests atomic.Int32
	defaultServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		time.Sleep(20 * time.Millisecond)
		writeJSON(writer, http.StatusOK, `{"data":[]}`)
	}))
	defer defaultServer.Close()
	service := newService(t, defaultServer.URL, nil, time.Minute, nil)
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := service.Models(context.Background(), nil); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if requests.Load() != 1 {
		t.Fatalf("catalog requests = %d, want 1", requests.Load())
	}
}

func TestCatalogSanitizesHeadersAndUsesBackendCredentials(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer client" || request.Header.Get("X-Configured") != "value" {
			t.Errorf("headers = %#v", request.Header)
		}
		if request.Header.Get("X-Private") != "" {
			t.Error("connection-scoped header leaked")
		}
		writer.Header().Set("ETag", "stale")
		writeJSON(writer, http.StatusOK, `{"data":[]}`)
	}))
	defer server.Close()
	parsed, _ := url.Parse(server.URL)
	service := New([]Backend{{ID: "default", URL: parsed, Default: true, Headers: http.Header{"X-Configured": []string{"value"}}}}, &http.Client{Timeout: time.Second}, time.Minute, time.Second, time.Minute, testLogger())
	header := http.Header{"Authorization": []string{"Bearer client"}, "Connection": []string{"X-Private"}, "X-Private": []string{"secret"}}
	response, err := service.Models(context.Background(), header)
	if err != nil {
		t.Fatal(err)
	}
	if response.Header.Get("ETag") != "" || response.Header.Get("Content-Length") != "" {
		t.Fatalf("stale response headers retained: %#v", response.Header)
	}
}

type testBackend struct {
	url    string
	prefix string
}

func newService(t *testing.T, defaultURL string, optional map[string]testBackend, ttl time.Duration, logger *slog.Logger) *Service {
	t.Helper()
	parsedDefault, err := url.Parse(defaultURL)
	if err != nil {
		t.Fatal(err)
	}
	backends := []Backend{{ID: "default", URL: parsedDefault, Default: true}}
	for id, backend := range optional {
		parsed, err := url.Parse(backend.url)
		if err != nil {
			t.Fatal(err)
		}
		backends = append(backends, Backend{ID: id, URL: parsed, Prefix: backend.prefix})
	}
	if logger == nil {
		logger = testLogger()
	}
	return New(backends, &http.Client{Timeout: time.Second}, ttl, time.Second, time.Second, logger)
}

func catalogServer(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { writeJSON(writer, status, body) }))
}

func decodeResponse(t *testing.T, body []byte) (map[string]any, map[string]map[string]any) {
	t.Helper()
	root, values, err := decodeCatalog(body)
	if err != nil {
		t.Fatal(err)
	}
	models := make(map[string]map[string]any)
	for _, model := range modelObjects(values) {
		models[stringValue(model["id"])] = model
	}
	return root, models
}

func writeJSON(writer http.ResponseWriter, status int, body string) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_, _ = io.WriteString(writer, body)
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestHealthTimeoutBoundsSlowHealthCheck(t *testing.T) {
	slowServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
		writer.WriteHeader(http.StatusOK)
	}))
	defer slowServer.Close()
	service := newService(t, slowServer.URL, nil, time.Minute, nil)
	// health_timeout defaults to request_timeout (1s in newService), so a 30s health check is bounded.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := service.backendReady(ctx, &backendState{Backend: Backend{ID: "default", URL: mustParseURL(t, slowServer.URL), Default: true}})
	if err == nil {
		t.Fatal("slow health check was not bounded")
	}
}

func TestCatalogTimeoutBoundsSlowCatalogRefresh(t *testing.T) {
	slowServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
		writeJSON(writer, http.StatusOK, `{"data":[]}`)
	}))
	defer slowServer.Close()
	service := newService(t, slowServer.URL, nil, time.Minute, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _, _, err := service.backendCatalog(ctx, &backendState{Backend: Backend{ID: "default", URL: mustParseURL(t, slowServer.URL), Default: true}}, nil)
	if err == nil {
		t.Fatal("slow catalog refresh was not bounded")
	}
}

func TestEnrichmentDeadlineAllowsPartialMetadata(t *testing.T) {
	combined := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/v1/models":
			writeJSON(writer, http.StatusOK, `{"data":[{"id":"fast"},{"id":"slow"}]}`)
		case "/api/show":
			var body struct{ Model string }
			_ = json.NewDecoder(request.Body).Decode(&body)
			if body.Model == "slow" {
				<-request.Context().Done()
			}
			writeJSON(writer, http.StatusOK, `{"capabilities":["completion"],"details":{"family":"qwen3","format":"safetensors","parameter_size":"8.3B","quantization_level":"Q4_K_M","context_length":40960,"max_completion_tokens":8192}}`)
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer combined.Close()
	parsed, _ := url.Parse(combined.URL)
	service := New([]Backend{
		{ID: "default", URL: parsed, Default: true, ShowEnrichment: true},
	}, &http.Client{Timeout: time.Second}, time.Minute, 1*time.Second, 1*time.Second, testLogger())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	root, models, _, err := service.backendCatalog(ctx, &backendState{Backend: Backend{ID: "default", URL: parsed, Default: true, ShowEnrichment: true}}, nil)
	if err != nil {
		t.Fatalf("catalog refresh failed at deadline: %v", err)
	}
	if root == nil {
		t.Fatal("catalog root should still be returned")
	}
	_ = models
}

func TestCatalogRefreshLogsDurationOnSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, http.StatusOK, `{"data":[{"id":"model"}]}`)
	}))
	defer server.Close()
	parsed, _ := url.Parse(server.URL)
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	service := New([]Backend{{ID: "default", URL: parsed, Default: true}}, &http.Client{Timeout: time.Second}, time.Minute, time.Second, time.Minute, logger)
	backendState := &backendState{Backend: Backend{ID: "default", URL: parsed, Default: true}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, _, _, err := service.backendCatalog(ctx, backendState, nil); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "refresh_duration_ms") {
		t.Fatalf("refresh duration not logged on success: %s", logs.String())
	}
	if !strings.Contains(logs.String(), "backend model catalog refreshed") {
		t.Fatalf("success refresh not logged: %s", logs.String())
	}
}

func TestCatalogRefreshDurationIncludesOllamaEnrichment(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/api/show" {
			time.Sleep(40 * time.Millisecond)
			writeJSON(writer, http.StatusOK, `{"model_info":{"general.architecture":"qwen"}}`)
			return
		}
		writeJSON(writer, http.StatusOK, `{"data":[{"id":"qwen:latest"}]}`)
	}))
	defer server.Close()
	parsed := mustParseURL(t, server.URL)
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	backend := Backend{ID: "default", URL: parsed, Default: true, ShowEnrichment: true}
	service := New([]Backend{backend}, &http.Client{Timeout: time.Second}, time.Minute, time.Second, time.Second, logger)
	state := &backendState{Backend: backend}
	if _, _, _, err := service.backendCatalog(context.Background(), state, nil); err != nil {
		t.Fatal(err)
	}
	for _, line := range bytes.Split(logs.Bytes(), []byte{'\n'}) {
		var entry map[string]any
		if json.Unmarshal(line, &entry) == nil && entry["msg"] == "backend model catalog refreshed" {
			duration, ok := entry["refresh_duration_ms"].(float64)
			if !ok || duration < 30 {
				t.Fatalf("refresh duration %vms does not include the delayed enrichment: %s", entry["refresh_duration_ms"], line)
			}
			return
		}
	}
	t.Fatalf("successful refresh duration log missing: %s", logs.String())
}

func TestCatalogRefreshFailureLogsStaleAge(t *testing.T) {
	var unavailable atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		if unavailable.Load() {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		writeJSON(writer, http.StatusOK, `{"data":[{"id":"model"}]}`)
	}))
	defer server.Close()
	parsed, _ := url.Parse(server.URL)
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	service := New([]Backend{{ID: "default", URL: parsed, Default: true}}, &http.Client{Timeout: time.Second}, 10*time.Millisecond, time.Second, time.Minute, logger)
	backendState := &backendState{Backend: Backend{ID: "default", URL: parsed, Default: true}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, _, _, err := service.backendCatalog(ctx, backendState, nil); err != nil {
		t.Fatal(err)
	}
	// Cache is now stale after a short sleep; make the backend fail.
	unavailable.Store(true)
	time.Sleep(15 * time.Millisecond)
	if _, _, _, err := service.backendCatalog(ctx, backendState, nil); err != nil {
		t.Fatalf("expected stale cache to be returned on failure: %v", err)
	}
	if !strings.Contains(logs.String(), "refresh_duration_ms") {
		t.Fatalf("refresh duration not logged on failure: %s", logs.String())
	}
	if !strings.Contains(logs.String(), "stale_cache_age_ms") {
		t.Fatalf("stale cache age not logged on failure: %s", logs.String())
	}
	if !strings.Contains(logs.String(), "using_stale") {
		t.Fatalf("using_stale not logged: %s", logs.String())
	}
}

func TestCatalogRefreshRedactsCredentials(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, http.StatusOK, `{"data":[]}`)
	}))
	defer server.Close()
	parsed, _ := url.Parse(server.URL)
	parsed.User = url.UserPassword("catalog-user", "secret-password")
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	service := New([]Backend{{ID: "default", URL: parsed, Default: true}}, &http.Client{Timeout: time.Second}, time.Minute, time.Second, time.Minute, logger)
	backendState := &backendState{Backend: Backend{ID: "default", URL: parsed, Default: true}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, _, _, err := service.backendCatalog(ctx, backendState, nil); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), "secret-password") || strings.Contains(logs.String(), "catalog-user") {
		t.Fatal("credentials leaked into catalog refresh log")
	}
}

func TestCatalogRefreshFailureRedactsURLCredentials(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	serverURL := server.URL
	server.Close()
	parsed := mustParseURL(t, serverURL)
	parsed.User = url.UserPassword("catalog-user", "catalog-password-secret")
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	backend := Backend{ID: "default", URL: parsed, Default: true}
	service := New([]Backend{backend}, &http.Client{Timeout: time.Second}, time.Minute, time.Second, time.Second, logger)
	state := &backendState{Backend: backend}
	if _, _, _, err := service.backendCatalog(context.Background(), state, nil); err == nil {
		t.Fatal("expected catalog refresh failure")
	}
	if strings.Contains(logs.String(), "catalog-password-secret") || strings.Contains(logs.String(), "catalog-user") {
		t.Fatalf("backend URL credentials leaked into catalog failure log: %s", logs.String())
	}
}

func TestReadinessOnlyDefaultFailureMakesUnready(t *testing.T) {
	defaultServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
	}))
	defer defaultServer.Close()
	optionalServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
		writer.WriteHeader(http.StatusOK)
	}))
	defer optionalServer.Close()
	service := newService(t, defaultServer.URL, map[string]testBackend{"optional": {url: optionalServer.URL, prefix: "optional/"}}, time.Minute, nil)
	statuses, err := service.BackendStatuses(context.Background())
	if err != nil {
		t.Fatalf("readiness should not error when optional backend is slow: %v", err)
	}
	if statuses["default"] != "ready" {
		t.Fatalf("default status = %q", statuses["default"])
	}
	if statuses["optional"] != "unavailable" {
		t.Fatalf("optional status = %q, want unavailable (bounded by health timeout)", statuses["optional"])
	}
}

func mustParseURL(t *testing.T, urlString string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(urlString)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

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
	service := New([]Backend{{ID: "default", URL: parsed, Default: true, Headers: http.Header{"X-Configured": []string{"value"}}}}, &http.Client{Timeout: time.Second}, time.Minute, testLogger())
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
	return New(backends, &http.Client{Timeout: time.Second}, ttl, logger)
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

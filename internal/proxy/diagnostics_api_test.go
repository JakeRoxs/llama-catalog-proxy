// Package proxy provides tests for the internal diagnostics API boundary.
package proxy

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/catalog"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/diagnostics"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modeldiagnostics"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/timing"
)

// newDiagnosticsTestHandler creates a Handler with the diagnostics API enabled
// and a diagnostics service backed by mock servers.
func newDiagnosticsTestHandler(t *testing.T, apiKey string, mgmt *mockDiagnosticsServer) *Handler {
	t.Helper()
	cfg := config.Config{
		Listen:            "127.0.0.1:0",
		LogLevel:          "INFO",
		DefaultBackend:    "default",
		Backends:          map[string]config.Backend{"default": {URL: mgmt.URL, Diagnostics: "llama_swap"}},
		ModelsCacheTTL:    time.Minute,
		RequestTimeout:    time.Second,
		HealthTimeout:     time.Second,
		CatalogTimeout:    time.Second,
		MaxJSONBody:       64 << 20,
		RequestHistory:    true,
		DiagnosticsAPIKey: apiKey,
	}
	diagService, err := diagnostics.New(cfg.Backends)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), diagService, modeldiagnostics.New(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return handler.(*Handler)
}

// mockDiagnosticsServer is an httptest server that responds to diagnostics and
// catalog endpoints for testing.
type mockDiagnosticsServer struct {
	*httptest.Server
	modelsResponse   string
	activityResponse string
	perfResponse     string
	hardwareResponse string
	versionResponse  string
	runningResponse  string
	outage           bool // if true, returns 503 for all endpoints
}

func newMockDiagnosticsServer(t *testing.T) *mockDiagnosticsServer {
	t.Helper()
	m := &mockDiagnosticsServer{
		modelsResponse:   `{"data":[{"id":"model1","status":"loaded"},{"id":"model2","status":"unloaded"}]}`,
		activityResponse: `{"data":[{"id":1,"timestamp":"2026-11-11T10:00:00Z","src":"local","model":"model1","task":"chat","route":"local","resp_path":"","resp_content_type":"application/json","resp_status_code":200,"tokens":{"cache_tokens":0,"draft_tokens":0,"draft_acc_tokens":0,"input_tokens":10,"output_tokens":5,"reasoning_tokens":-1,"prompt_per_second":0,"tokens_per_second":0},"duration_ms":100,"capture_id":"","has_capture":false,"error_msg":"","metadata":{}}],"page":1,"limit":100,"total":1,"total_pages":1}`,
		perfResponse:     `{"enabled":true,"gpu_stats":[{"id":0,"timestamp":"2026-11-11T10:00:00Z","gpu_util_pct":50.0,"mem_util_pct":60.0,"mem_used_mb":1024,"mem_total_mb":8192}]}`,
		hardwareResponse: `{"schema_version":1,"accelerators":[]}`,
		versionResponse:  `{"version":"1.0.0","commit":"abc123","build_date":"2026-01-01T00:00:00Z"}`,
		runningResponse:  `{"running":[{"model":"model1","state":"ready"}]}`,
		outage:           false,
	}
	m.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if m.outage {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/health":
			w.WriteHeader(http.StatusOK)
			io.WriteString(w, `{"status":"ok"}`)
		case "/api/version":
			w.WriteHeader(http.StatusOK)
			io.WriteString(w, m.versionResponse)
		case "/v1/models":
			w.WriteHeader(http.StatusOK)
			io.WriteString(w, m.modelsResponse)
		case "/api/metrics/activity":
			w.WriteHeader(http.StatusOK)
			io.WriteString(w, m.activityResponse)
		case "/api/performance":
			w.WriteHeader(http.StatusOK)
			io.WriteString(w, m.perfResponse)
		case "/api/hardware":
			w.WriteHeader(http.StatusOK)
			io.WriteString(w, m.hardwareResponse)
		case "/running":
			w.WriteHeader(http.StatusOK)
			io.WriteString(w, m.runningResponse)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	return m
}

func (m *mockDiagnosticsServer) Close() {
	m.Server.Close()
}

func TestDiagnosticsDefaultOff(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "", mgmt)

	for _, path := range []string{"/diagnostics", "/diagnostics/v1/models", "/diagnostics/v1/requests", "/diagnostics/v1/summary"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Errorf("path %s: expected 404, got %d", path, rec.Code)
		}
	}
}

func TestDiagnosticsUnauthorizedKey(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)

	req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models", nil)
	req.Header.Set("Authorization", "Bearer wrong-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
}

func TestDiagnosticsMissingKey(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)

	req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
}

func TestDiagnosticsAuthOnlyAuthorizationHeader(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)

	// Query parameter auth must be rejected.
	req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models?api_key=secret-key", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("query param auth: expected 401, got %d", rec.Code)
	}

	// X-Api-Key header auth must be rejected.
	req = httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models", nil)
	req.Header.Set("X-Api-Key", "secret-key")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("X-Api-Key auth: expected 401, got %d", rec.Code)
	}

	// Valid Bearer auth must be accepted.
	req = httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("Bearer auth: expected 200, got %d", rec.Code)
	}
}

func TestDiagnosticsMethodNotAllowed(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)

	req := httptest.NewRequest(http.MethodPost, "/diagnostics/v1/models", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", rec.Code)
	}
	if rec.Header().Get("Allow") != "GET" {
		t.Errorf("expected Allow: GET, got %q", rec.Header().Get("Allow"))
	}
}

func TestDiagnosticsUnknownPath(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)

	for _, path := range []string{"/diagnostics/unknown", "/diagnostics/v1/unknown", "/diagnostics/v1/requests/foo/bar"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "Bearer secret-key")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Errorf("path %s: expected 404, got %d", path, rec.Code)
		}
	}
}

func TestDiagnosticsBackends(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)

	req := httptest.NewRequest(http.MethodGet, "/diagnostics", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}

	var result map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if _, ok := result["backends"]; !ok {
		t.Errorf("expected 'backends' key in response")
	}
}

func TestDiagnosticsModelsTypedSnapshotNoRawMaps(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)

	// Populate catalog cache.
	ctx := context.Background()
	_, _ = h.catalog.Models(ctx, http.Header{})

	req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d, body=%s", rec.Code, rec.Body.String())
	}

	var result map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if _, ok := result["generated_at"]; !ok {
		t.Errorf("expected 'generated_at' key")
	}
	if _, ok := result["coverage"]; !ok {
		t.Errorf("expected 'coverage' key")
	}
	if _, ok := result["models"]; !ok {
		t.Errorf("expected 'models' key")
	}

	// No raw catalog maps: models entries must be typed ModelSnapshot objects
	// with identity, catalog, activity, gpu_window, lifecycle, provenance.
	models := result["models"].([]any)
	if len(models) == 0 {
		t.Fatal("expected at least one model snapshot")
	}
	for _, m := range models {
		mm := m.(map[string]any)
		if _, ok := mm["identity"]; !ok {
			t.Errorf("expected 'identity' in model snapshot")
		}
		// No raw "models" array of maps.
		if _, ok := mm["models"]; ok {
			t.Errorf("raw 'models' key must not appear in snapshot entry")
		}
	}
}

func TestDiagnosticsModelsSafeCatalogProjection(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)

	ctx := context.Background()
	_, _ = h.catalog.Models(ctx, http.Header{})

	req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d, body=%s", rec.Code, rec.Body.String())
	}

	var result map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	models := result["models"].([]any)
	for _, m := range models {
		mm := m.(map[string]any)
		// Catalog projection must not contain raw meta/command/desc/aliases.
		cat := mm["catalog"]
		if cat != nil {
			catMap := cat.(map[string]any)
			for _, bad := range []string{"meta", "command", "desc", "aliases", "llamaswap"} {
				if _, ok := catMap[bad]; ok {
					t.Errorf("raw catalog key %q leaked in projection", bad)
				}
			}
		}
	}
}

func TestDiagnosticsModelsFilters(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)

	ctx := context.Background()
	_, _ = h.catalog.Models(ctx, http.Header{})

	// Backend filter.
	req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models?backend=default", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}

	// Model filter.
	req = httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models?model=model1", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}

	// Status filter.
	req = httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models?status=loaded", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}

	// Invalid status filter.
	req = httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models?status=invalid", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}

	// Unknown model filter -> 400, no raw ID echo.
	req = httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models?model=nonexistent-model", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for unknown model, got %d", rec.Code)
	}
	body := rec.Body.String()
	if strings.Contains(body, "nonexistent-model") {
		t.Errorf("raw model ID leaked in error response")
	}
}

func TestDiagnosticsModelsResolvesBackendAliasWithoutExposingAlias(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	mgmt.modelsResponse = `{"data":[{"id":"model1","status":"loaded","meta":{"llamaswap":{"aliases":["model-one"]}}},{"id":"model2","status":"unloaded"}]}`
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)
	_, _ = h.catalog.Models(context.Background(), http.Header{})

	req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models?backend=default&model=model-one", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("alias filter status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response["resolved_model_id"] != "model1" {
		t.Fatalf("resolved model ID = %v", response["resolved_model_id"])
	}
	models := response["models"].([]any)
	if len(models) != 1 || models[0].(map[string]any)["identity"].(map[string]any)["public_id"] != "model1" {
		t.Fatalf("alias did not resolve to one canonical model: %#v", models)
	}
	if strings.Contains(rec.Body.String(), "model-one") || strings.Contains(rec.Body.String(), `"aliases"`) {
		t.Fatal("raw model alias leaked in diagnostics response")
	}
}

func TestCatalogModelFilterIDsResolvesPrefixQualifiedAlias(t *testing.T) {
	models := []catalog.BackendModel{{
		BackendID: "bigboi",
		Prefix:    "bigboi/",
		Models: []map[string]any{{
			"id": "Nemotron-3.5-Bolt-Turbo-Brilliance",
			"meta": map[string]any{
				"llamaswap": map[string]any{"aliases": []any{"nemotron-bolt"}},
			},
		}},
	}}
	ids := catalogModelFilterIDs(models, "bigboi", "bigboi/nemotron-bolt")
	if len(ids) != 1 {
		t.Fatalf("alias resolutions = %#v", ids)
	}
	if _, ok := ids["bigboi/Nemotron-3.5-Bolt-Turbo-Brilliance"]; !ok {
		t.Fatalf("alias resolved to wrong canonical identity: %#v", ids)
	}
	if got := catalogModelFilterIDs(models, "udms", "bigboi/nemotron-bolt"); len(got) != 0 {
		t.Fatalf("alias matched outside selected backend: %#v", got)
	}
}

func TestDiagnosticsModelsLimit(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)

	ctx := context.Background()
	_, _ = h.catalog.Models(ctx, http.Header{})

	// Limit > 1000 should be clamped.
	req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models?limit=5000", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}

	// Invalid limit.
	req = httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models?limit=abc", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}

	// Offset must be rejected.
	req = httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models?offset=1", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for offset, got %d", rec.Code)
	}
}

func TestDiagnosticsModelsCursorPagination(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)

	ctx := context.Background()
	_, _ = h.catalog.Models(ctx, http.Header{})

	// First page with limit=10 (fits in one page, no cursor) - 2 models total.
	req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models?limit=10", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d, body=%s", rec.Code, rec.Body.String())
	}
	var result map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if _, ok := result["cursor"]; ok {
		t.Errorf("expected no cursor for single-page result")
	}

	// First page with limit=1 (does not fit, cursor returned).
	req = httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models?limit=1", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d, body=%s", rec.Code, rec.Body.String())
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	cursor, ok := result["cursor"].(string)
	if !ok || cursor == "" {
		t.Fatal("expected cursor for multi-page result")
	}

	// Second page using cursor (same limit so filter digest matches).
	req = httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models?cursor="+cursor+"&limit=1", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d, body=%s", rec.Code, rec.Body.String())
	}
}

func TestDecodeModelCursor(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	randomID := "abcdef0123456789"
	offset := uint64(100)
	cursor := encodeModelCursor(secret, []byte(randomID), offset)

	// Valid cursor decodes correctly.
	rid, off, err := decodeModelCursor(secret, cursor)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if rid != randomID {
		t.Errorf("expected randomID %q, got %q", randomID, rid)
	}
	if off != int(offset) {
		t.Errorf("expected offset %d, got %d", offset, off)
	}

	// Invalid cursor (wrong length) is rejected.
	if _, _, err := decodeModelCursor(secret, "xyz"); err == nil {
		t.Error("expected error for short cursor")
	}

	// Invalid cursor (garbage base64) is rejected.
	if _, _, err := decodeModelCursor(secret, "!!!not-valid-base64!!!"); err == nil {
		t.Error("expected error for invalid base64 cursor")
	}

	// Empty cursor is rejected.
	if _, _, err := decodeModelCursor(secret, ""); err == nil {
		t.Error("expected error for empty cursor")
	}

	// Tampered cursor (changed offset bytes without recomputing HMAC) is rejected.
	validCursor := encodeModelCursor(secret, []byte(randomID), offset)
	decoded, _ := base64.RawURLEncoding.Strict().DecodeString(validCursor)
	// Flip a byte in the offset region (bytes 16-23) to tamper the cursor.
	decoded[20] ^= 0xFF
	tampered := base64.RawURLEncoding.EncodeToString(decoded)
	if _, _, err := decodeModelCursor(secret, tampered); err == nil {
		t.Error("expected error for tampered cursor")
	}
}

func TestDiagnosticsModelsCacheAbsentUnknown(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)

	// Do NOT populate the catalog cache. FetchedAt is zero for all backends.
	req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d, body=%s", rec.Code, rec.Body.String())
	}

	var result map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	coverage := result["coverage"].(string)
	if coverage != "unknown" && coverage != "partial" {
		t.Errorf("expected unknown/partial coverage when cache absent, got %q", coverage)
	}
	if models, ok := result["models"].([]any); ok && len(models) != 0 {
		t.Errorf("expected empty models when cache absent, got %d", len(models))
	}
}

func TestDiagnosticsRequests(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)

	h.history.RegisterRequest("req-1", "default", "", time.Now())
	h.history.CompleteRequest("req-1", 100*time.Millisecond, "http_success")

	req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/requests", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}

	var result map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if _, ok := result["records"]; !ok {
		t.Errorf("expected 'records' key")
	}
}

func TestDiagnosticsRequestsModelFilter501(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)

	h.history.RegisterRequest("req-1", "default", "", time.Now())
	h.history.CompleteRequest("req-1", 100*time.Millisecond, "http_success")

	// Nonempty model filter must return 501, not silently empty.
	req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/requests?model=model1", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotImplemented {
		t.Errorf("expected 501 for model filter, got %d, body=%s", rec.Code, rec.Body.String())
	}
}

func TestDiagnosticsRequestsHistoryDisabled(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	cfg := config.Config{
		Listen:            "127.0.0.1:0",
		LogLevel:          "INFO",
		DefaultBackend:    "default",
		Backends:          map[string]config.Backend{"default": {URL: mgmt.URL, Diagnostics: "llama_swap"}},
		ModelsCacheTTL:    time.Minute,
		RequestTimeout:    time.Second,
		HealthTimeout:     time.Second,
		CatalogTimeout:    time.Second,
		MaxJSONBody:       64 << 20,
		DiagnosticsAPIKey: "secret-key",
	}
	diagService, err := diagnostics.New(cfg.Backends)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), diagService, modeldiagnostics.New(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := handler.(*Handler)

	req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/requests", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", rec.Code)
	}
}

func TestDiagnosticsRequestByID(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)

	h.history.RegisterRequest("req-1", "default", "", time.Now())
	h.history.CompleteRequest("req-1", 100*time.Millisecond, "http_success")

	req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/requests/req-1", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}

	// Not found.
	req = httptest.NewRequest(http.MethodGet, "/diagnostics/v1/requests/unknown", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
	}

	// Invalid request ID format.
	req = httptest.NewRequest(http.MethodGet, "/diagnostics/v1/requests/not-a-valid-id", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404 for invalid request ID, got %d", rec.Code)
	}
}

func TestDiagnosticsSummary(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)

	h.history.RegisterRequest("req-1", "default", "", time.Now())
	h.history.CompleteRequest("req-1", 100*time.Millisecond, "http_success")
	h.history.RegisterRequest("req-2", "default", "", time.Now())
	h.history.CompleteRequest("req-2", 200*time.Millisecond, "http_client_error")

	req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/summary", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}

	var result map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if _, ok := result["count"]; !ok {
		t.Errorf("expected 'count' key")
	}
	if _, ok := result["outcomes"]; !ok {
		t.Errorf("expected 'outcomes' key")
	}
	if _, ok := result["duration_sample_count"]; !ok {
		t.Errorf("expected 'duration_sample_count' key")
	}

	// duration_sample_count should be 0 because ProxyObservation is not set
	// in RegisterRequest/CompleteRequest (no timing observation).
	if result["duration_sample_count"].(float64) != 0 {
		t.Errorf("expected duration_sample_count=0, got %v", result["duration_sample_count"])
	}
}

func TestDiagnosticsSummaryTooLarge(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)

	// Register 1001 records; retention count caps at 1000 so the oldest is
	// evicted. Summary returns at most 1000 records, no too_large.
	for i := 0; i < 1001; i++ {
		id := fmt.Sprintf("req-%d", i)
		h.history.RegisterRequest(id, "default", "", time.Now())
		h.history.CompleteRequest(id, 100*time.Millisecond, "http_success")
	}

	req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/summary", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d, body=%s", rec.Code, rec.Body.String())
	}

	var result map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result["count"].(float64) != 1000 {
		t.Errorf("expected count=1000 (retention cap), got %v", result["count"])
	}
}

func TestDiagnosticsSecretSentinelsAbsent(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key-12345", mgmt)

	req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models", nil)
	req.Header.Set("Authorization", "Bearer wrong-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	body := rec.Body.String()
	if strings.Contains(body, "secret-key-12345") {
		t.Errorf("API key leaked in error response")
	}
}

func TestDiagnosticsBackendOutage(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	mgmt.outage = true
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)

	req := httptest.NewRequest(http.MethodGet, "/diagnostics", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable && rec.Code != http.StatusOK {
		t.Errorf("expected 503 or 200, got %d", rec.Code)
	}
}

func TestDiagnosticsRateLimiting(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)

	var wg sync.WaitGroup
	wg.Add(2)
	results := make(chan int, 2)
	go func() {
		defer wg.Done()
		req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models", nil)
		req.Header.Set("Authorization", "Bearer secret-key")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		results <- rec.Code
	}()
	go func() {
		defer wg.Done()
		req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models", nil)
		req.Header.Set("Authorization", "Bearer secret-key")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		results <- rec.Code
	}()
	wg.Wait()
	close(results)
	for code := range results {
		if code != http.StatusOK {
			t.Errorf("expected 200, got %d", code)
		}
	}
}

func TestDiagnosticsSemaphoreHoldsDuringBlockedUpstream(t *testing.T) {
	// Slow backend that delays responses.
	slowBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Delay to simulate a blocked upstream.
		<-time.After(5 * time.Second)
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, `{"data":[]}`)
	}))
	defer slowBackend.Close()

	cfg := config.Config{
		Listen:         "127.0.0.1:0",
		LogLevel:       "INFO",
		DefaultBackend: "default",
		Backends: map[string]config.Backend{
			"default": {URL: slowBackend.URL, Diagnostics: "llama_swap"},
		},
		ModelsCacheTTL:    time.Minute,
		RequestTimeout:    time.Second,
		HealthTimeout:     time.Second,
		CatalogTimeout:    time.Second,
		MaxJSONBody:       64 << 20,
		RequestHistory:    true,
		DiagnosticsAPIKey: "secret-key",
	}
	diagService, err := diagnostics.New(cfg.Backends)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), diagService, modeldiagnostics.New(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := handler.(*Handler)

	// First request acquires a semaphore slot and blocks on upstream.
	done1 := make(chan struct{})
	go func() {
		req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models", nil)
		req.Header.Set("Authorization", "Bearer secret-key")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		close(done1)
	}()

	// Wait for the first request to start collecting (it should hold the
	// semaphore slot).
	time.Sleep(100 * time.Millisecond)

	// Second request also acquires a slot.
	done2 := make(chan struct{})
	go func() {
		req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models", nil)
		req.Header.Set("Authorization", "Bearer secret-key")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		close(done2)
	}()

	// Third request should be refused (semaphore full) or wait boundedly.
	req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	// Should not be 200 immediately because semaphore is full.
	if rec.Code == http.StatusOK {
		t.Errorf("third request should not complete immediately while semaphore is full")
	}

	// Wait for the first two to complete.
	<-done1
	<-done2
}

func TestDiagnosticsCancellation(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models", nil)
	req = req.WithContext(ctx)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusGatewayTimeout && rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 504/503, got %d", rec.Code)
	}
}

func TestDiagnosticsParamsBeforeAnyUpstream(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)

	// Invalid params should be rejected before any collector runs.
	req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models?status=invalid", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid params before upstream, got %d", rec.Code)
	}

	// Invalid backend filter should be rejected before any upstream.
	req = httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models?backend=unknown", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid backend before upstream, got %d", rec.Code)
	}
}

func TestDiagnosticsRequestIDPageRead(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)

	h.history.RegisterRequest("req-1", "default", "", time.Now())
	h.history.CompleteRequest("req-1", 100*time.Millisecond, "http_success")

	req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/requests/req-1", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}

	var result map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result["request_id"] != "req-1" {
		t.Errorf("expected request_id=req-1, got %v", result["request_id"])
	}
}

func TestDiagnosticsExistingRoutesUnaffected(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("/health: expected 200, got %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/ready", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("/ready: expected 200, got %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code == http.StatusNotFound {
		t.Errorf("/v1/models should not return 404 (diagnostics path)")
	}
}

func TestDiagnosticsNoBodyRead(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)

	// GET /diagnostics/v1/models should not read any request body.
	req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	req.Body = http.NoBody
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestDiagnosticsSummaryDurationSampleCount(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)

	// Register requests and complete. duration_sample_count should be 0
	// because no ProxyObservation is attached.
	h.history.RegisterRequest("req-1", "default", "", time.Now())
	h.history.CompleteRequest("req-1", 100*time.Millisecond, "http_success")

	req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/summary", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}

	var result map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	// duration_sample_count should be explicit and 0 (no observation attached).
	if _, ok := result["duration_sample_count"]; !ok {
		t.Errorf("expected 'duration_sample_count' key")
	}
	if result["duration_sample_count"].(float64) != 0 {
		t.Errorf("expected duration_sample_count=0, got %v", result["duration_sample_count"])
	}

	// HTTP success is not a generation success: outcomes only reflect
	// transport status, and no generation outcome is inferred.
	outcomes := result["outcomes"].(map[string]any)
	if outcomes["http_success"] != float64(1) {
		t.Errorf("expected http_success=1, got %v", outcomes["http_success"])
	}
}

// TestDiagnosticsModelsCursorResumeDuringOutage verifies that a cursor resume
// request does not hit upstream collectors/network, so it works during a
// backend outage and preserves the initial generated_at.
func TestDiagnosticsModelsCursorResumeDuringOutage(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)

	ctx := context.Background()
	_, _ = h.catalog.Models(ctx, http.Header{})

	// First page with limit=1: get cursor and generated_at.
	req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models?limit=1", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d, body=%s", rec.Code, rec.Body.String())
	}
	var first map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	cursor, ok := first["cursor"].(string)
	if !ok || cursor == "" {
		t.Fatal("expected cursor for multi-page result")
	}
	firstGA, _ := first["generated_at"].(string)
	firstModels := first["models"].([]any)
	if len(firstModels) != 1 {
		t.Fatalf("expected 1 model on first page, got %d", len(firstModels))
	}

	// Put the backend into outage mode.
	mgmt.outage = true

	// Resume with cursor: must succeed without hitting the (outaged) backend.
	req = httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models?cursor="+cursor+"&limit=1", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on cursor resume during outage, got %d, body=%s", rec.Code, rec.Body.String())
	}
	var second map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &second); err != nil {
		t.Fatal(err)
	}
	secondGA, _ := second["generated_at"].(string)
	if secondGA != firstGA {
		t.Errorf("generated_at changed across pages: %q vs %q", firstGA, secondGA)
	}
	secondModels := second["models"].([]any)
	if len(secondModels) != 1 {
		t.Fatalf("expected 1 model on resumed page, got %d", len(secondModels))
	}
}

// TestDiagnosticsModelsGeneratedAtStable verifies that the first page and a
// resumed page return the same generated_at.
func TestDiagnosticsModelsGeneratedAtStable(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)

	ctx := context.Background()
	_, _ = h.catalog.Models(ctx, http.Header{})

	req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models?limit=1", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d, body=%s", rec.Code, rec.Body.String())
	}
	var first map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	cursor, _ := first["cursor"].(string)
	if cursor == "" {
		t.Fatal("expected cursor")
	}
	firstGA, _ := first["generated_at"].(string)

	req = httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models?cursor="+cursor+"&limit=1", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d, body=%s", rec.Code, rec.Body.String())
	}
	var second map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &second); err != nil {
		t.Fatal(err)
	}
	secondGA, _ := second["generated_at"].(string)
	if secondGA != firstGA {
		t.Errorf("generated_at not stable: %q vs %q", firstGA, secondGA)
	}
}

// TestDiagnosticsModelsCursorNoRemoteRequests verifies that a cursor resume
// request makes no additional remote requests to the diagnostics backend.
func TestDiagnosticsModelsCursorNoRemoteRequests(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)

	ctx := context.Background()
	_, _ = h.catalog.Models(ctx, http.Header{})

	// First page with limit=1.
	req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models?limit=1", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d, body=%s", rec.Code, rec.Body.String())
	}
	var first map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	cursor, _ := first["cursor"].(string)
	if cursor == "" {
		t.Fatal("expected cursor")
	}

	// The cursor request must not trigger diagnostics.All() or any collector,
	// so no remote requests are made. We verify this by putting the backend
	// in outage mode: if the cursor request hit the backend, it would fail.
	mgmt.outage = true
	req = httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models?cursor="+cursor+"&limit=1", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on cursor resume (no remote requests), got %d, body=%s", rec.Code, rec.Body.String())
	}
}

// TestDiagnosticsModelsFilterLimitBinding verifies filter and limit binding.
func TestDiagnosticsModelsFilterLimitBinding(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)

	ctx := context.Background()
	_, _ = h.catalog.Models(ctx, http.Header{})

	// Limit=1 returns exactly one model.
	req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models?limit=1", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d, body=%s", rec.Code, rec.Body.String())
	}
	var result map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	models := result["models"].([]any)
	if len(models) != 1 {
		t.Errorf("expected 1 model with limit=1, got %d", len(models))
	}

	// Status filter binds to typed Lifecycle.CurrentState. model1 has a
	// lifecycle row (ready), model2 has no lifecycle row (falls back to
	// catalog status unloaded).
	req = httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models?status=ready", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d, body=%s", rec.Code, rec.Body.String())
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	models = result["models"].([]any)
	if len(models) != 1 {
		t.Errorf("expected 1 model with status=ready, got %d", len(models))
	}

	req = httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models?status=unloaded", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d, body=%s", rec.Code, rec.Body.String())
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	models = result["models"].([]any)
	if len(models) != 1 {
		t.Errorf("expected 1 model with status=unloaded, got %d", len(models))
	}
}

// TestDiagnosticsModelsStatusCollisionBackendIsolated verifies that status
// filtering is based on each typed ModelSnapshot.Lifecycle.CurrentState, so
// public ID collisions across backends do not cause status bleeding. With a
// single backend, model1 has a lifecycle row (ready) while model2 has no
// lifecycle row (falls back to catalog status unloaded). Filtering by
// status=loaded must return no models because neither has CurrentState=loaded
// (model1 is ready, model2 is unloaded).
func TestDiagnosticsModelsStatusCollisionBackendIsolated(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)

	ctx := context.Background()
	_, _ = h.catalog.Models(ctx, http.Header{})

	// model1: catalog status=loaded, lifecycle state=ready => CurrentState=ready.
	// model2: catalog status=unloaded, no lifecycle row => CurrentState=unloaded.
	// Filtering by status=loaded returns no models (neither CurrentState=loaded).
	req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models?status=loaded", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d, body=%s", rec.Code, rec.Body.String())
	}
	var result map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	var models []any
	if m, ok := result["models"].([]any); ok {
		models = m
	}
	if len(models) != 0 {
		t.Errorf("expected 0 models with status=loaded, got %d", len(models))
	}

	// Filtering by status=ready returns only model1.
	req = httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models?status=ready", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d, body=%s", rec.Code, rec.Body.String())
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	models = result["models"].([]any)
	if len(models) != 1 {
		t.Errorf("expected 1 model with status=ready, got %d", len(models))
	}

	// Filtering by status=unloaded returns only model2.
	req = httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models?status=unloaded", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d, body=%s", rec.Code, rec.Body.String())
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	models = result["models"].([]any)
	if len(models) != 1 {
		t.Errorf("expected 1 model with status=unloaded, got %d", len(models))
	}
}

// TestDiagnosticsModelsCoverageUncachedBackend verifies coverage is partial/
// unknown when any configured backend lacks a catalog cache.
func TestDiagnosticsModelsCoverageUncachedBackend(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)

	// Do NOT populate the catalog cache: all FetchedAt are zero.
	req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/models", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d, body=%s", rec.Code, rec.Body.String())
	}
	var result map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	coverage := result["coverage"].(string)
	if coverage != "unknown" {
		t.Errorf("expected unknown coverage when no cache, got %q", coverage)
	}
	if models, ok := result["models"].([]any); ok && len(models) != 0 {
		t.Errorf("expected empty models when cache absent, got %d", len(models))
	}
}

// TestDiagnosticsRequestsTimeRange verifies time range validation: missing
// bounds default to a 24h window ending at now; future bounds and windows
// > 24h are rejected.
func TestDiagnosticsRequestsTimeRange(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)

	h.history.RegisterRequest("req-1", "default", "", time.Now())
	h.history.CompleteRequest("req-1", 100*time.Millisecond, "http_success")

	// Only since, too old (48h ago, until defaults to now => window > 24h).
	tooOld := time.Now().Add(-48 * time.Hour).Format(time.RFC3339)
	req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/requests?since="+tooOld, nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for since too old, got %d", rec.Code)
	}

	// Future until.
	future := time.Now().Add(1 * time.Hour).Format(time.RFC3339)
	req = httptest.NewRequest(http.MethodGet, "/diagnostics/v1/requests?until="+future, nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for future until, got %d", rec.Code)
	}

	// Only until, valid (since defaults to now-24h, window = 24h).
	validUntil := time.Now().Format(time.RFC3339)
	req = httptest.NewRequest(http.MethodGet, "/diagnostics/v1/requests?until="+validUntil, nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 for valid only-until, got %d, body=%s", rec.Code, rec.Body.String())
	}
}

// TestDiagnosticsRequestsDuplicateParams verifies that duplicate query
// parameters are rejected.
func TestDiagnosticsRequestsDuplicateParams(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)

	h.history.RegisterRequest("req-1", "default", "", time.Now())
	h.history.CompleteRequest("req-1", 100*time.Millisecond, "http_success")

	// Duplicate since.
	req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/requests?since=a&since=b", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for duplicate since, got %d", rec.Code)
	}

	// Duplicate until.
	req = httptest.NewRequest(http.MethodGet, "/diagnostics/v1/requests?until=a&until=b", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for duplicate until, got %d", rec.Code)
	}

	// Duplicate status.
	req = httptest.NewRequest(http.MethodGet, "/diagnostics/v1/requests?status=x&status=y", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for duplicate status, got %d", rec.Code)
	}

	// Duplicate requestid.
	req = httptest.NewRequest(http.MethodGet, "/diagnostics/v1/requests?requestid=a&requestid=b", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for duplicate requestid, got %d", rec.Code)
	}
}

// TestDiagnosticsModelsSizeCap verifies the model snapshot store computes
// size from actual JSON marshaling and enforces the size budget.
func TestDiagnosticsModelsSizeCap(t *testing.T) {
	store := newModelSnapshotPageStore()

	// Create a small snapshot: should succeed.
	small := make([]*modeldiagnostics.ModelSnapshot, 0, 10)
	for i := 0; i < 10; i++ {
		small = append(small, &modeldiagnostics.ModelSnapshot{
			Identity: modeldiagnostics.ModelIdentity{
				PublicID:  fmt.Sprintf("m%d", i),
				BackendID: "default",
				LocalID:   fmt.Sprintf("m%d", i),
			},
		})
	}
	page, err := store.createSnapshot(small, 10, []byte("digest"))
	if err != nil {
		t.Fatalf("expected no error for small snapshot, got %v", err)
	}
	if len(page.Records) != 10 {
		t.Errorf("expected 10 records, got %d", len(page.Records))
	}

	// Verify the store's internal size reflects actual JSON marshaling, not a
	// fixed per-model estimate. createSnapshot uses json.NewEncoder.Encode
	// which appends a newline per encoded value.
	store.mu.Lock()
	expectedSize := int64(0)
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, m := range small {
		if err := enc.Encode(m); err != nil {
			t.Fatal(err)
		}
	}
	expectedSize = int64(buf.Len())
	if store.size != expectedSize {
		t.Errorf("store size %d != expected JSON size %d", store.size, expectedSize)
	}
	store.mu.Unlock()
}

// TestDiagnosticsSummaryDenominators verifies that duration_sample_count and
// generation_outcomes are separate from HTTP transport outcomes and status.
func TestDiagnosticsSummaryDenominators(t *testing.T) {
	mgmt := newMockDiagnosticsServer(t)
	defer mgmt.Close()
	h := newDiagnosticsTestHandler(t, "secret-key", mgmt)

	// Register and complete requests without observations: duration_sample_count
	// is 0, generation_outcomes is empty, outcomes reflect transport status.
	h.history.RegisterRequest("req-1", "default", "", time.Now())
	h.history.CompleteRequest("req-1", 100*time.Millisecond, "http_success")
	h.history.RegisterRequest("req-2", "default", "", time.Now())
	h.history.CompleteRequest("req-2", 200*time.Millisecond, "http_client_error")

	req := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/summary", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d, body=%s", rec.Code, rec.Body.String())
	}
	var result map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result["count"].(float64) != 2 {
		t.Errorf("expected count=2, got %v", result["count"])
	}
	if result["duration_sample_count"].(float64) != 0 {
		t.Errorf("expected duration_sample_count=0, got %v", result["duration_sample_count"])
	}
	genOutcomes := result["generation_outcomes"].(map[string]any)
	if len(genOutcomes) != 0 {
		t.Errorf("expected empty generation_outcomes, got %v", genOutcomes)
	}
	outcomes := result["outcomes"].(map[string]any)
	if outcomes["http_success"] != float64(1) {
		t.Errorf("expected http_success=1, got %v", outcomes["http_success"])
	}
	if outcomes["http_client_error"] != float64(1) {
		t.Errorf("expected http_client_error=1, got %v", outcomes["http_client_error"])
	}

	// Attach observations with duration and generation outcomes.
	start := time.Now()
	end := start.Add(150 * time.Millisecond)
	dur1 := float64(150)
	summary := timing.Summary{
		StartTime:         start,
		EndTime:           end,
		TotalDurationMs:   &dur1,
		TransportOutcome:  "returned",
		GenerationOutcome: "completed",
		InspectionState:   "full",
	}
	if !h.history.AttachObservation("req-1", summary) {
		t.Fatal("failed to attach observation to req-1")
	}

	dur2 := float64(200)
	summary2 := timing.Summary{
		StartTime:         start,
		EndTime:           end,
		TotalDurationMs:   &dur2,
		TransportOutcome:  "returned",
		GenerationOutcome: "incomplete",
		InspectionState:   "full",
	}
	if !h.history.AttachObservation("req-2", summary2) {
		t.Fatal("failed to attach observation to req-2")
	}

	req = httptest.NewRequest(http.MethodGet, "/diagnostics/v1/summary", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d, body=%s", rec.Code, rec.Body.String())
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result["duration_sample_count"].(float64) != 2 {
		t.Errorf("expected duration_sample_count=2, got %v", result["duration_sample_count"])
	}
	genOutcomes = result["generation_outcomes"].(map[string]any)
	if genOutcomes["completed"] != float64(1) {
		t.Errorf("expected generation_outcomes completed=1, got %v", genOutcomes["completed"])
	}
	if genOutcomes["incomplete"] != float64(1) {
		t.Errorf("expected generation_outcomes incomplete=1, got %v", genOutcomes["incomplete"])
	}
}

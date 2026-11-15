package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/correlation"
)

var activityFixtureTime = time.Now().UTC()

func writeTempConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func diagnosticsConfig(t *testing.T, serverURL string, diagnostics string, requestHistory bool) string {
	t.Helper()
	return fmt.Sprintf(`
listen: 127.0.0.1:0
default_backend: test
backends:
  test:
    url: %s
    diagnostics: %s
    api_key: sentinel-key
    headers:
      X-Custom: sentinel-header
request_history: %v
`, serverURL, diagnostics, requestHistory)
}

func validCapabilityBodies() map[string]string {
	return map[string]string{
		"/api/version":          `{"version":"v1.2.3","commit":"abcdef1234","build_date":"2026-11-11T00:00:00Z"}`,
		"/v1/models":            `{"data":[]}`,
		"/running":              `{"running":[]}`,
		"/api/metrics/activity": `{"data":[],"page":1,"total":0}`,
		"/api/metrics/stats":    `{"total_requests":0,"total_input_tokens":0,"total_output_tokens":0}`,
		"/api/performance":      `{"enabled":true,"sys_stats":[],"gpu_stats":[]}`,
		"/api/hardware":         `{"schema_version":1,"accelerators":[]}`,
	}
}

func activitySchema() string {
	entry := map[string]any{
		"id":               42,
		"timestamp":        activityFixtureTime.Format(time.RFC3339),
		"resp_status_code": 200,
		"model":            "SHORT_MODEL_SECRET",
		"task":             "SHORT_TASK_SECRET",
		"route":            "SHORT_ROUTE_SECRET",
		"peer_id":          "SHORT_PEER_SECRET",
		"req_path":         "/SHORT_PATH_SECRET",
		"error_msg":        "SHORT_ERROR_SECRET",
		"metadata":         map[string]string{"SHORT_KEY_SECRET": "SHORT_VALUE_SECRET"},
		"tokens":           map[string]any{"input_tokens": -1, "output_tokens": 0},
	}
	page := map[string]any{
		"data":        []any{entry},
		"page":        1,
		"limit":       100,
		"total":       1,
		"total_pages": 1,
	}
	b, err := json.Marshal(page)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func newTestServer(t *testing.T, prefix, apiKey, customHeader, activityBody, modelsBody, runningBody string) (*httptest.Server, map[string]int) {
	t.Helper()
	seen := make(map[string]int)
	var mu sync.Mutex
	bodies := validCapabilityBodies()
	if modelsBody != "" {
		bodies["/v1/models"] = modelsBody
	}
	if runningBody != "" {
		bodies["/running"] = runningBody
	}
	bodies["/api/metrics/activity"] = activityBody
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("unexpected method %s on %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if r.Header.Get("Authorization") != "Bearer "+apiKey {
			t.Errorf("authorization not applied on %s", r.URL.Path)
		}
		if r.Header.Get("X-Api-Key") != apiKey {
			t.Errorf("x-api-key not applied on %s", r.URL.Path)
		}
		if customHeader != "" && r.Header.Get("X-Custom") != customHeader {
			t.Errorf("custom header not applied on %s", r.URL.Path)
		}
		path := r.URL.Path
		if prefix != "" {
			if !strings.HasPrefix(path, prefix) {
				t.Errorf("prefix not preserved on %s", r.URL.Path)
			}
			path = strings.TrimPrefix(path, prefix)
		}
		if _, ok := bodies[path]; !ok {
			t.Errorf("unsafe/unexpected path %s", r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if path == "/api/metrics/activity" {
			query := r.URL.RawQuery
			if query != "limit=1" && query != "limit=100&page=1" {
				t.Errorf("activity query is not bounded: %s", query)
			}
		} else if r.URL.RawQuery != "" {
			t.Errorf("unexpected query on %s: %s", r.URL.Path, r.URL.RawQuery)
		}
		mu.Lock()
		seen[path]++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, bodies[path])
	}))
	return server, seen
}

func parseReport(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	var report map[string]any
	if err := json.Unmarshal(buf.Bytes(), &report); err != nil {
		t.Fatalf("failed to parse report: %v", err)
	}
	return report
}

// toStruct round-trips a JSON-decoded report value into a concrete struct so
// assertions can use typed fields without reflecting over map[string]any.
func toStruct[T any](t *testing.T, v any) T {
	var out T
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal report value: %v", err)
	}
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal report value into %T: %v", out, err)
	}
	return out
}

func asFloat64(v any) float64 {
	switch vv := v.(type) {
	case float64:
		return vv
	case int:
		return float64(vv)
	case int64:
		return float64(vv)
	default:
		return 0
	}
}

type errorWriter struct{}

func (w *errorWriter) Write(p []byte) (int, error) {
	return 0, errors.New("write error")
}

func (w *errorWriter) Header() http.Header        { return http.Header{} }
func (w *errorWriter) WriteHeader(statusCode int) {}

func TestRunDiagnosticsOmitsBackendActivityWhenRequestHistoryFalse(t *testing.T) {
	server, seen := newTestServer(t, "/proxy", "sentinel-key", "sentinel-header", activitySchema(), "", "")
	defer server.Close()

	path := writeTempConfig(t, diagnosticsConfig(t, server.URL+"/proxy", "llama_swap", false))
	var buf bytes.Buffer
	ctx := context.Background()
	if err := runDiagnostics(ctx, path, &buf); err != nil {
		t.Fatalf("runDiagnostics error: %v", err)
	}
	report := parseReport(t, &buf)
	if _, ok := report["backend_activity"]; ok {
		t.Fatal("backend_activity should be omitted when request_history is false")
	}
	backends, ok := report["backends"].([]any)
	if !ok || len(backends) != 1 {
		t.Fatalf("expected 1 backend snapshot, got %v", backends)
	}
	if seen["/api/metrics/activity"] != 1 {
		t.Fatalf("activity path probed %d times, want 1 (capabilities only)", seen["/api/metrics/activity"])
	}
	for _, op := range []string{"/api/version", "/v1/models", "/api/metrics/stats", "/api/performance", "/api/hardware"} {
		if seen[op] != 1 {
			t.Fatalf("capability op %s probed %d times, want 1", op, seen[op])
		}
	}
	output := buf.String()
	for _, secret := range []string{server.URL, "sentinel-key", "sentinel-header", "SHORT_MODEL_SECRET", "SHORT_TASK_SECRET", "SHORT_ROUTE_SECRET", "SHORT_PEER_SECRET", "SHORT_PATH_SECRET", "SHORT_ERROR_SECRET", "SHORT_KEY_SECRET", "SHORT_VALUE_SECRET"} {
		if strings.Contains(output, secret) {
			t.Fatalf("secret leaked in output: %q", secret)
		}
	}
}

func TestRunDiagnosticsRequestHistoryDisabledBackendNoHTTP(t *testing.T) {
	server, seen := newTestServer(t, "/proxy", "sentinel-key", "sentinel-header", activitySchema(), "", "")
	defer server.Close()

	path := writeTempConfig(t, diagnosticsConfig(t, server.URL+"/proxy", "", true))
	var buf bytes.Buffer
	ctx := context.Background()
	if err := runDiagnostics(ctx, path, &buf); err != nil {
		t.Fatalf("runDiagnostics error: %v", err)
	}
	report := parseReport(t, &buf)
	backendActivity, ok := report["backend_activity"].(map[string]any)
	if !ok {
		t.Fatal("backend_activity should be present when request_history is true")
	}
	backendStates, ok := backendActivity["backend_states"].(map[string]any)
	if !ok {
		t.Fatal("backend_states should be a map")
	}
	testState, ok := backendStates["test"].(map[string]any)
	if !ok {
		t.Fatal("backend_states.test should be a map")
	}
	if testState["state"] != "disabled" {
		t.Fatalf("disabled backend state = %v, want disabled", testState["state"])
	}
	if len(seen) != 0 {
		t.Fatalf("backend was probed %d times, want 0", len(seen))
	}
	output := buf.String()
	for _, secret := range []string{server.URL, "sentinel-key", "sentinel-header"} {
		if strings.Contains(output, secret) {
			t.Fatalf("secret leaked in output: %q", secret)
		}
	}
}

func TestRunDiagnosticsRequestHistoryLlamaSwap(t *testing.T) {
	server, seen := newTestServer(t, "/proxy", "sentinel-key", "sentinel-header", activitySchema(), "", "")
	defer server.Close()

	path := writeTempConfig(t, diagnosticsConfig(t, server.URL+"/proxy", "llama_swap", true))
	var buf bytes.Buffer
	ctx := context.Background()
	if err := runDiagnostics(ctx, path, &buf); err != nil {
		t.Fatalf("runDiagnostics error: %v", err)
	}
	report := parseReport(t, &buf)
	backendActivity, ok := report["backend_activity"].(map[string]any)
	if !ok {
		t.Fatal("backend_activity should be present when request_history is true")
	}
	records, ok := backendActivity["records"].([]any)
	if !ok || len(records) != 1 {
		t.Fatalf("expected exactly one normalized record, got %v", records)
	}
	record := records[0].(map[string]any)
	if record["backend_id"] != "test" {
		t.Fatalf("backend_id = %v, want test", record["backend_id"])
	}
	if asFloat64(record["epoch"]) != 1 {
		t.Fatalf("epoch = %v, want 1", record["epoch"])
	}
	if record["activity_id"] != "42" {
		t.Fatalf("activity_id = %v, want 42", record["activity_id"])
	}
	if record["timestamp"] != activityFixtureTime.Format(time.RFC3339) {
		t.Fatalf("timestamp = %v, want %s", record["timestamp"], activityFixtureTime.Format(time.RFC3339))
	}
	if record["model_id"] != "unknown" {
		t.Fatalf("model_id = %v, want unknown (no allowlist)", record["model_id"])
	}
	if record["status"] != "http_success" {
		t.Fatalf("status = %v, want http_success (not generation completion)", record["status"])
	}
	if record["request_id_mapping"] != "unsupported" {
		t.Fatalf("request_id_mapping = %v, want unsupported", record["request_id_mapping"])
	}
	if record["upstream_generation"] != "unknown" {
		t.Fatalf("upstream_generation = %v, want unknown", record["upstream_generation"])
	}
	if record["epoch_scope"] != "collector" {
		t.Fatalf("epoch_scope = %v, want collector", record["epoch_scope"])
	}
	for _, field := range []string{"task", "route", "peer_id", "req_path", "error_msg", "metadata", "tokens"} {
		if _, ok := record[field]; ok {
			t.Fatalf("raw field %q should not be copied", field)
		}
	}
	output := buf.String()
	for _, secret := range []string{server.URL, "sentinel-key", "sentinel-header", "SHORT_MODEL_SECRET", "SHORT_TASK_SECRET", "SHORT_ROUTE_SECRET", "SHORT_PEER_SECRET", "SHORT_PATH_SECRET", "SHORT_ERROR_SECRET", "SHORT_KEY_SECRET", "SHORT_VALUE_SECRET"} {
		if strings.Contains(output, secret) {
			t.Fatalf("secret leaked in output: %q", secret)
		}
	}
	_, ok = backendActivity["max_records"].(float64)
	if !ok {
		t.Fatal("max_records should be a number (no any sentinel)")
	}
	states, ok := backendActivity["backend_states"].(map[string]any)
	if !ok {
		t.Fatal("backend_states should be a map")
	}
	testState, ok := states["test"].(map[string]any)
	if !ok {
		t.Fatal("backend_states.test should be a map")
	}
	if testState["state"] != "collected" || testState["reason"] != "ok" || asFloat64(testState["http_status"]) != 200 || asFloat64(testState["rows"]) != 1 || testState["coverage"] != "complete" {
		t.Fatalf("backend state wrong: %#v", testState)
	}
	if asFloat64(backendActivity["record_count"]) != 1 {
		t.Fatalf("record_count = %v, want 1", backendActivity["record_count"])
	}
	if asFloat64(backendActivity["epoch"]) != 1 {
		t.Fatalf("epoch = %v, want 1", backendActivity["epoch"])
	}
	if seen["/api/metrics/activity"] != 2 {
		t.Fatalf("activity path probed %d times, want 2 (capabilities + collector)", seen["/api/metrics/activity"])
	}
}

func TestRunDiagnosticsCancellationReturnsErrorNoIncompleteJSON(t *testing.T) {
	server, _ := newTestServer(t, "/proxy", "sentinel-key", "sentinel-header", activitySchema(), "", "")
	defer server.Close()

	path := writeTempConfig(t, diagnosticsConfig(t, server.URL+"/proxy", "llama_swap", false))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var buf bytes.Buffer
	err := runDiagnostics(ctx, path, &buf)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if buf.Len() != 0 {
		t.Fatal("incomplete JSON written on cancellation")
	}
}

func TestRunDiagnosticsWriterError(t *testing.T) {
	server, _ := newTestServer(t, "/proxy", "sentinel-key", "sentinel-header", activitySchema(), "", "")
	defer server.Close()

	path := writeTempConfig(t, diagnosticsConfig(t, server.URL+"/proxy", "llama_swap", false))
	ctx := context.Background()
	if err := runDiagnostics(ctx, path, &errorWriter{}); err == nil {
		t.Fatal("expected write error")
	}
}

// TestRunDiagnosticsRequestHistoryLinksLifecycle verifies that, within the
// request-history branch, alias collection (GET /v1/models) and lifecycle
// collection (GET /running) share the activity collector epoch, that the
// resolved identity links to the current lifecycle snapshot, and that raw
// aliases, model IDs, commands, URLs, and secrets are never emitted.
func TestRunDiagnosticsRequestHistoryLinksLifecycle(t *testing.T) {
	// Canonical model "canon" declares alias "shortalias"; an explicit alias
	// row shortalias -> canon. Both resolve to the canonical ref.
	modelsBody, err := json.Marshal(map[string]any{
		"data": []any{
			map[string]any{
				"id": "canon",
				"meta": map[string]any{
					"llamaswap": map[string]any{
						"type":    "model",
						"modelID": "canon",
						"aliases": []string{"shortalias"},
					},
				},
			},
			map[string]any{
				"id": "shortalias",
				"meta": map[string]any{
					"llamaswap": map[string]any{
						"type":    "alias",
						"modelID": "canon",
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// /running readiness snapshot for the canonical model; raw fields must
	// never be copied.
	runningBody, err := json.Marshal(map[string]any{
		"running": []any{
			map[string]any{
				"model":       "canon",
				"state":       "ready",
				"cmd":         "SHORT_CMD_SECRET",
				"proxy":       "SHORT_PROXY_SECRET",
				"ttl":         1,
				"name":        "SHORT_NAME_SECRET",
				"description": "SHORT_DESC_SECRET",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Activity source model is "shortalias" (resolves to canon) plus an
	// activity for an absent model "nosuchmodel" (must remain unknown).
	activityBody, err := json.Marshal(map[string]any{
		"data": []any{
			map[string]any{
				"id":               42,
				"timestamp":        activityFixtureTime.Format(time.RFC3339),
				"resp_status_code": 200,
				"model":            "shortalias",
				"task":             "chat",
				"route":            "local",
				"peer_id":          "",
				"req_path":         "/",
				"error_msg":        "",
				"metadata":         map[string]any{},
				"tokens":           map[string]any{"input_tokens": 0, "output_tokens": 0},
			},
			map[string]any{
				"id":               43,
				"timestamp":        activityFixtureTime.Format(time.RFC3339),
				"resp_status_code": 200,
				"model":            "nosuchmodel",
				"task":             "chat",
				"route":            "local",
				"peer_id":          "",
				"req_path":         "/",
				"error_msg":        "",
				"metadata":         map[string]any{},
				"tokens":           map[string]any{"input_tokens": 0, "output_tokens": 0},
			},
		},
		"page":        1,
		"limit":       100,
		"total":       2,
		"total_pages": 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	server, seen := newTestServer(t, "/proxy", "sentinel-key", "sentinel-header", string(activityBody), string(modelsBody), string(runningBody))
	defer server.Close()

	path := writeTempConfig(t, diagnosticsConfig(t, server.URL+"/proxy", "llama_swap", true))
	var buf bytes.Buffer
	ctx := context.Background()
	if err := runDiagnostics(ctx, path, &buf); err != nil {
		t.Fatalf("runDiagnostics error: %v", err)
	}
	report := parseReport(t, &buf)

	// backend_aliases: alias collector ran GET /v1/models.
	backendAliases := toStruct[correlation.AliasReport](t, report["backend_aliases"])
	if len(backendAliases.Edges) != 2 {
		t.Fatalf("expected 2 alias edges (canon self + shortalias->canon), got %d", len(backendAliases.Edges))
	}
	shortaliasRef, _ := correlation.ModelReference("test", "shortalias")
	canonRef, _ := correlation.ModelReference("test", "canon")
	var aliasEdge *correlation.AliasEdge
	for i := range backendAliases.Edges {
		if backendAliases.Edges[i].Ref == shortaliasRef {
			aliasEdge = &backendAliases.Edges[i]
		}
	}
	if aliasEdge == nil {
		t.Fatal("expected alias edge for shortalias")
	}
	if aliasEdge.State != "catalog_resolved" || aliasEdge.CanonicalRef != canonRef {
		t.Fatalf("alias edge wrong: %#v", aliasEdge)
	}

	// activity_model_identity: the shortalias activity resolves to canon.
	identities := toStruct[[]correlation.ActivityModelIdentity](t, report["activity_model_identity"])
	if len(identities) != 2 {
		t.Fatalf("expected 2 identities, got %d", len(identities))
	}
	resolved := identities[0]
	if resolved.State != "catalog_resolved" {
		t.Fatalf("resolved identity state = %q, want catalog_resolved", resolved.State)
	}
	if resolved.CanonicalRef != canonRef {
		t.Fatalf("resolved identity canonical_ref = %q, want %q", resolved.CanonicalRef, canonRef)
	}
	if resolved.ReportedModelRef != shortaliasRef {
		t.Fatalf("resolved identity reported_model_ref = %q, want %q", resolved.ReportedModelRef, shortaliasRef)
	}
	if resolved.Source != "catalog_metadata" {
		t.Fatalf("source = %q", resolved.Source)
	}
	absent := identities[1]
	if absent.State != "unknown" || absent.CanonicalRef != "" {
		t.Fatalf("absent identity should be unknown: %#v", absent)
	}

	// backend_lifecycle: lifecycle collector ran GET /running.
	lifecycleReport := toStruct[correlation.LifecycleReport](t, report["backend_lifecycle"])
	if len(lifecycleReport.Models) != 1 {
		t.Fatalf("expected 1 lifecycle model, got %d", len(lifecycleReport.Models))
	}
	lm := lifecycleReport.Models[0]
	if lm.State != "ready" {
		t.Fatalf("lifecycle state = %q, want ready", lm.State)
	}
	if lm.ModelRef != canonRef {
		t.Fatalf("lifecycle model_ref = %q, want %q", lm.ModelRef, canonRef)
	}
	if lm.ReadySince != nil {
		t.Fatal("ReadySince should be nil (/running does not expose it)")
	}

	// activity_lifecycle: link resolves to the ready model.
	observations := toStruct[[]correlation.ActivityLifecycleObservation](t, report["activity_lifecycle"])
	if len(observations) != 2 {
		t.Fatalf("expected 2 observations, got %d", len(observations))
	}
	o := observations[0]
	// Opaque canonical ref agrees between identity and lifecycle.
	if o.CanonicalRef != canonRef {
		t.Fatalf("observation canonical_ref = %q, want %q", o.CanonicalRef, canonRef)
	}
	if o.ReportedModelRef != shortaliasRef {
		t.Fatalf("observation reported_model_ref = %q, want %q", o.ReportedModelRef, shortaliasRef)
	}
	// CurrentState is the observed lifecycle enum.
	if o.CurrentState != "ready" {
		t.Fatalf("current_state = %q, want ready", o.CurrentState)
	}
	if !o.Observed {
		t.Fatal("observed should be true")
	}
	if o.ObservedAt == nil {
		t.Fatal("observed_at should point to the lifecycle row")
	}
	// StateAtActivity is unknown despite CurrentState ready.
	if o.StateAtActivity != "unknown" {
		t.Fatalf("state_at_activity = %q, want unknown", o.StateAtActivity)
	}
	if o.Kind != "current_metadata_only" {
		t.Fatalf("kind = %q", o.Kind)
	}
	if o.ClockAlignmentUnverified != true {
		t.Fatalf("clock_alignment_unverified = %v", o.ClockAlignmentUnverified)
	}
	if o.UpstreamGeneration != "unknown" {
		t.Fatalf("upstream_generation = %q", o.UpstreamGeneration)
	}
	if o.RequestIDMapping != "unsupported" {
		t.Fatalf("request_id_mapping = %q", o.RequestIDMapping)
	}
	if o.ExecutionAttribution != "unverified" {
		t.Fatalf("execution_attribution = %q", o.ExecutionAttribution)
	}
	// Second observation (absent model) is unknown and unobserved.
	o2 := observations[1]
	if o2.CurrentState != "unknown" || o2.Observed || o2.ObservedAt != nil {
		t.Fatalf("absent observation should be unknown and unobserved: %#v", o2)
	}

	// Raw aliases, model IDs, commands, URLs, and secrets must never be
	// emitted as values. Opaque refs must not contain raw IDs. Field names
	// such as "canonical_ref" legitimately contain the substring "canon", so
	// check for quoted JSON string values instead of bare substrings.
	output := buf.String()
	for _, secret := range []string{
		server.URL, "sentinel-key", "sentinel-header",
		"SHORT_CMD_SECRET", "SHORT_PROXY_SECRET", "SHORT_NAME_SECRET", "SHORT_DESC_SECRET",
	} {
		if strings.Contains(output, secret) {
			t.Fatalf("secret leaked in output: %q", secret)
		}
	}
	for _, raw := range []string{"canon", "shortalias", "nosuchmodel"} {
		if strings.Contains(output, `"`+raw+`"`) {
			t.Fatalf("raw model ID %q leaked as a value in output", raw)
		}
	}
	if strings.Contains(canonRef, "canon") || strings.Contains(shortaliasRef, "shortalias") {
		t.Fatal("opaque ref must not leak raw model ID")
	}
	// GET-only base-path auth: only the expected paths were probed.
	for _, op := range []string{"/v1/models", "/running", "/api/metrics/activity", "/api/version", "/api/metrics/stats", "/api/performance", "/api/hardware"} {
		if _, ok := seen[op]; !ok {
			t.Fatalf("expected path %s to be probed", op)
		}
	}
}

// TestRunDiagnosticsRequestHistoryLifecycleEventsOnlyWhenFlagTrue verifies
// that backend_lifecycle_events is present only when request_history is true,
// and that no /api/events or /api/events/logs requests are made.
func TestRunDiagnosticsRequestHistoryLifecycleEventsOnlyWhenFlagTrue(t *testing.T) {
	server, seen := newTestServer(t, "/proxy", "sentinel-key", "sentinel-header", activitySchema(), "", "")
	defer server.Close()

	// request_history: false -> backend_lifecycle_events absent
	path := writeTempConfig(t, diagnosticsConfig(t, server.URL+"/proxy", "llama_swap", false))
	var buf bytes.Buffer
	ctx := context.Background()
	if err := runDiagnostics(ctx, path, &buf); err != nil {
		t.Fatalf("runDiagnostics error: %v", err)
	}
	report := parseReport(t, &buf)
	if _, ok := report["backend_lifecycle_events"]; ok {
		t.Fatal("backend_lifecycle_events should be absent when request_history is false")
	}
	if _, ok := report["activity_lifecycle_events"]; ok {
		t.Fatal("activity_lifecycle_events should be absent when request_history is false")
	}

	// request_history: true -> backend_lifecycle_events present
	path = writeTempConfig(t, diagnosticsConfig(t, server.URL+"/proxy", "llama_swap", true))
	buf.Reset()
	if err := runDiagnostics(ctx, path, &buf); err != nil {
		t.Fatalf("runDiagnostics error: %v", err)
	}
	report = parseReport(t, &buf)
	support, ok := report["backend_lifecycle_events"].(map[string]any)
	if !ok {
		t.Fatal("backend_lifecycle_events should be a map when request_history is true")
	}
	if support["source"] != "adapter_capability" {
		t.Fatalf("source = %v, want adapter_capability", support["source"])
	}
	if support["epoch_scope"] != "collector" {
		t.Fatalf("epoch_scope = %v, want collector", support["epoch_scope"])
	}
	backendStatuses, ok := support["backend_statuses"].(map[string]any)
	if !ok {
		t.Fatal("backend_statuses should be a map")
	}
	testStatus, ok := backendStatuses["test"].(map[string]any)
	if !ok {
		t.Fatal("backend_statuses.test should be a map")
	}
	if testStatus["state"] != "unsupported" {
		t.Fatalf("state = %v, want unsupported", testStatus["state"])
	}
	if testStatus["reason"] != "transition_payload_not_exposed" {
		t.Fatalf("reason = %v, want transition_payload_not_exposed", testStatus["reason"])
	}
	// Explicit unavailability fields must be present.
	for _, field := range []string{"source_time_unavailable", "source_event_id_unavailable", "replay_unsupported"} {
		if _, ok := support[field]; !ok {
			t.Fatalf("support field %q should be present", field)
		}
	}
	if support["collection_performed"] != false {
		t.Fatal("unsupported transition source must not claim collection")
	}
	// ObservedTransitions is an empty array.
	if _, ok := support["observed_transitions"]; !ok {
		t.Fatal("observed_transitions should be present")
	}
	if support["observed_transitions"] != nil {
		t.Fatal("uncollected transitions should be unknown, not an observed empty list")
	}
	// No /api/events or /api/events/logs requests even if server advertises SSE.
	for _, op := range []string{"/api/events", "/api/events/logs"} {
		if _, ok := seen[op]; ok {
			t.Fatalf("unexpected path %s was probed", op)
		}
	}
	// No secrets leaked.
	output := buf.String()
	for _, secret := range []string{server.URL, "sentinel-key", "sentinel-header"} {
		if strings.Contains(output, secret) {
			t.Fatalf("secret leaked in output: %q", secret)
		}
	}
}

// TestRunDiagnosticsRequestHistoryActivityLifecycleEvents verifies that,
// within the request-history branch, activity_lifecycle_events is populated
// after identity resolution. With an unsupported adapter contract (no
// transition events), associations correctly report no_matching_event for
// resolved identities and identity_unresolved for unresolved identities.
// We do not pretend 0 events were collected.
func TestRunDiagnosticsRequestHistoryActivityLifecycleEvents(t *testing.T) {
	// Canonical model "canon" declares alias "shortalias".
	modelsBody, err := json.Marshal(map[string]any{
		"data": []any{
			map[string]any{
				"id": "canon",
				"meta": map[string]any{
					"llamaswap": map[string]any{
						"type":    "model",
						"modelID": "canon",
						"aliases": []string{"shortalias"},
					},
				},
			},
			map[string]any{
				"id": "shortalias",
				"meta": map[string]any{
					"llamaswap": map[string]any{
						"type":    "alias",
						"modelID": "canon",
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// /running readiness snapshot for the canonical model.
	runningBody, err := json.Marshal(map[string]any{
		"running": []any{
			map[string]any{
				"model": "canon",
				"state": "ready",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Activity for shortalias (resolves to canon) and nosuchmodel (unknown).
	activityBody, err := json.Marshal(map[string]any{
		"data": []any{
			map[string]any{
				"id":               42,
				"timestamp":        activityFixtureTime.Format(time.RFC3339),
				"resp_status_code": 200,
				"model":            "shortalias",
				"task":             "chat",
				"route":            "local",
				"peer_id":          "",
				"req_path":         "/",
				"error_msg":        "",
				"metadata":         map[string]any{},
				"tokens":           map[string]any{"input_tokens": 0, "output_tokens": 0},
			},
			map[string]any{
				"id":               43,
				"timestamp":        activityFixtureTime.Format(time.RFC3339),
				"resp_status_code": 200,
				"model":            "nosuchmodel",
				"task":             "chat",
				"route":            "local",
				"peer_id":          "",
				"req_path":         "/",
				"error_msg":        "",
				"metadata":         map[string]any{},
				"tokens":           map[string]any{"input_tokens": 0, "output_tokens": 0},
			},
		},
		"page":        1,
		"limit":       100,
		"total":       2,
		"total_pages": 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	server, _ := newTestServer(t, "/proxy", "sentinel-key", "sentinel-header", string(activityBody), string(modelsBody), string(runningBody))
	defer server.Close()

	path := writeTempConfig(t, diagnosticsConfig(t, server.URL+"/proxy", "llama_swap", true))
	var buf bytes.Buffer
	ctx := context.Background()
	if err := runDiagnostics(ctx, path, &buf); err != nil {
		t.Fatalf("runDiagnostics error: %v", err)
	}
	report := parseReport(t, &buf)

	// activity_lifecycle_events is present after identity resolution.
	events, ok := report["activity_lifecycle_events"].([]any)
	if !ok || len(events) != 2 {
		t.Fatalf("expected 2 activity lifecycle events, got %v", events)
	}

	// First event: shortalias resolves to canon (catalog_resolved), but no
	// transition events are available -> no_matching_event.
	e0 := events[0].(map[string]any)
	if e0["kind"] != "unavailable" {
		t.Fatalf("kind = %v, want unavailable", e0["kind"])
	}
	if e0["reason"] != "no_matching_event" {
		t.Fatalf("reason = %v, want no_matching_event", e0["reason"])
	}
	if asFloat64(e0["observed_event_count"]) != 0 {
		t.Fatalf("observed_event_count = %v, want 0", e0["observed_event_count"])
	}
	if asFloat64(e0["matched_event_count"]) != 0 {
		t.Fatalf("matched_event_count = %v, want 0", e0["matched_event_count"])
	}

	// Second event: nosuchmodel is unknown -> identity_unresolved.
	e1 := events[1].(map[string]any)
	if e1["kind"] != "unavailable" {
		t.Fatalf("kind = %v, want unavailable", e1["kind"])
	}
	if e1["reason"] != "identity_unresolved" {
		t.Fatalf("reason = %v, want identity_unresolved", e1["reason"])
	}
	if asFloat64(e1["observed_event_count"]) != 0 {
		t.Fatalf("observed_event_count = %v, want 0", e1["observed_event_count"])
	}

	// No secrets leaked.
	output := buf.String()
	for _, secret := range []string{server.URL, "sentinel-key", "sentinel-header"} {
		if strings.Contains(output, secret) {
			t.Fatalf("secret leaked in output: %q", secret)
		}
	}
	// Raw model IDs must not appear as JSON string values.
	for _, raw := range []string{"canon", "shortalias", "nosuchmodel"} {
		if strings.Contains(output, `"`+raw+`"`) {
			t.Fatalf("raw model ID %q leaked as a value in output", raw)
		}
	}
}

// TestRunDiagnosticsSSEFixtureNeverRead verifies that no /api/events or
// /api/events/logs request is made even when the server advertises SSE with
// secrets. The SSE fixture handler counts requests and fails the test if
// called.
func TestRunDiagnosticsSSEFixtureNeverRead(t *testing.T) {
	var eventsCount, logsCount int
	var mu sync.Mutex
	bodies := validCapabilityBodies()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("unexpected method %s on %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if r.Header.Get("Authorization") != "Bearer sentinel-key" {
			t.Errorf("authorization not applied on %s", r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if r.Header.Get("X-Api-Key") != "sentinel-key" {
			t.Errorf("x-api-key not applied on %s", r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if r.Header.Get("X-Custom") != "sentinel-header" {
			t.Errorf("custom header not applied on %s", r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		path := r.URL.Path
		if !strings.HasPrefix(path, "/proxy") {
			t.Errorf("prefix not preserved on %s", r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		path = strings.TrimPrefix(path, "/proxy")
		if _, ok := bodies[path]; ok {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, bodies[path])
			return
		}
		if path == "/api/events" {
			mu.Lock()
			eventsCount++
			mu.Unlock()
			t.Errorf("unexpected /api/events request")
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "data:{\"type\":\"modelStatus\",\"data\":\"[...]\"}\n\n")
			return
		}
		if path == "/api/events/logs" {
			mu.Lock()
			logsCount++
			mu.Unlock()
			t.Errorf("unexpected /api/events/logs request")
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "data:{\"type\":\"modelStatus\",\"data\":\"[...]\"}\n\n")
			return
		}
		t.Errorf("unsafe/unexpected path %s", r.URL.Path)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	path := writeTempConfig(t, diagnosticsConfig(t, server.URL+"/proxy", "llama_swap", true))
	var buf bytes.Buffer
	ctx := context.Background()
	if err := runDiagnostics(ctx, path, &buf); err != nil {
		t.Fatalf("runDiagnostics error: %v", err)
	}
	if eventsCount != 0 {
		t.Fatalf("/api/events handler called %d times, want 0", eventsCount)
	}
	if logsCount != 0 {
		t.Fatalf("/api/events/logs handler called %d times, want 0", logsCount)
	}
	report := parseReport(t, &buf)
	if _, ok := report["backend_lifecycle_events"]; !ok {
		t.Fatal("backend_lifecycle_events should be present when request_history is true")
	}
	// No secrets from the SSE fixture leaked.
	output := buf.String()
	for _, secret := range []string{server.URL, "sentinel-key", "sentinel-header", "modelStatus"} {
		if strings.Contains(output, secret) {
			t.Fatalf("secret leaked in output: %q", secret)
		}
	}
}

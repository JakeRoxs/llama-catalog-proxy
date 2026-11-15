package correlation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

var lifecycleFixtureTime = time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC)

type lifecycleMockClock struct {
	now time.Time
}

func (m *lifecycleMockClock) Now() time.Time { return m.now }

func newTestLifecycleCollector(t *testing.T, backends map[string]config.Backend) *LifecycleCollector {
	t.Helper()
	clock := &lifecycleMockClock{now: lifecycleFixtureTime}
	c := NewLifecycleCollector(backends)
	c.now = func() time.Time { return clock.now }
	return c
}

// lifecycleRunningEntry builds one /running row. Raw fields (Cmd, Proxy, TTL,
// Name, Description) are intentionally absent: the decode type ignores them.
func lifecycleRunningEntry(model, state string) lifecycleRunningModel {
	return lifecycleRunningModel{Model: model, State: state}
}

// lifecyclePageBody marshals a /running page.
func lifecyclePageBody(t *testing.T, entries ...lifecycleRunningModel) string {
	t.Helper()
	page := lifecycleRunningPage{Running: entries}
	b, err := json.Marshal(page)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// lifecyclePageBodyRaw marshals an arbitrary raw body used to test absent/
// malformed fields.
func lifecyclePageBodyRaw(t *testing.T, body string) string {
	t.Helper()
	return body
}

func TestLifecycleConstructionMakesNoRequestsAndSkipsDisabledBackends(t *testing.T) {
	var count atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, lifecyclePageBody(t, lifecycleRunningEntry("m", "ready")))
	}))
	defer server.Close()

	backends := map[string]config.Backend{
		"off": {URL: server.URL}, // no Diagnostics -> disabled
		"on":  {URL: server.URL, Diagnostics: "llama_swap"},
		"bad": {URL: "not-a-url", Diagnostics: "llama_swap"},
	}
	c := newTestLifecycleCollector(t, backends)
	if count.Load() != 0 {
		t.Fatal("construction made requests")
	}
	if c.backendStates["off"].State != lifecycleDisabledState || c.backendStates["off"].Reason != "not_enabled" {
		t.Fatalf("disabled backend state = %#v", c.backendStates["off"])
	}
	if c.backendStates["bad"].State != lifecycleUnsupportedState || c.backendStates["bad"].Reason != "invalid_url" {
		t.Fatalf("invalid URL backend state = %#v", c.backendStates["bad"])
	}
	if _, ok := c.backends["off"]; ok {
		t.Fatal("disabled backend should not be in backends map")
	}
	if _, ok := c.backends["bad"]; ok {
		t.Fatal("invalid URL backend should not be in backends map")
	}

	// Only the enabled backend is collected.
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if count.Load() != 1 {
		t.Fatalf("enabled backend request count = %d, want 1", count.Load())
	}
	if len(report.Models) != 1 {
		t.Fatalf("expected 1 model (only enabled backend), got %d", len(report.Models))
	}
}

func TestLifecycleCollectRealSchemaMapsStates(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != lifecyclePath {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, lifecyclePageBody(t,
			lifecycleRunningEntry("ready-model", "ready"),
			lifecycleRunningEntry("starting-model", "starting"),
			lifecycleRunningEntry("stopping-model", "stopping"),
		))
	}))
	defer server.Close()

	c := newTestLifecycleCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Models) != 3 {
		t.Fatalf("expected 3 models, got %d", len(report.Models))
	}
	byRef := make(map[string]*LifecycleModel)
	for i := range report.Models {
		byRef[report.Models[i].ModelRef] = &report.Models[i]
	}
	// Verify states are mapped strictly from the source finite enum.
	ready := byRef[modelRef("backend", "ready-model")]
	if ready == nil || ready.State != lifecycleStateReady {
		t.Fatalf("ready model: %#v", ready)
	}
	starting := byRef[modelRef("backend", "starting-model")]
	if starting == nil || starting.State != lifecycleStateStarting {
		t.Fatalf("starting model: %#v", starting)
	}
	stopping := byRef[modelRef("backend", "stopping-model")]
	if stopping == nil || stopping.State != lifecycleStateStopping {
		t.Fatalf("stopping model: %#v", stopping)
	}
	// ReadySince is nil because /running does not expose a ready time.
	if ready.ReadySince != nil {
		t.Fatalf("ReadySince should be nil when source does not expose it, got %v", ready.ReadySince)
	}
	// ObservedAt is local collection time (set by the collector's clock,
	// not a source timestamp). The mock clock returns the fixture time.
	if ready.ObservedAt.IsZero() {
		t.Fatalf("ObservedAt should be non-zero local collection time, got %v", ready.ObservedAt)
	}
	if !ready.ObservedAt.Equal(lifecycleFixtureTime) {
		t.Fatalf("ObservedAt = %v, want fixture time %v", ready.ObservedAt, lifecycleFixtureTime)
	}
	// BackendID is the configured ID.
	if ready.BackendID != "backend" {
		t.Fatalf("BackendID = %q, want %q", ready.BackendID, "backend")
	}
	// Epoch is the caller-supplied local epoch.
	if ready.Epoch != 1 {
		t.Fatalf("Epoch = %d, want 1", ready.Epoch)
	}
	// Attribution fields are always serialized.
	if ready.UpstreamGeneration != lifecycleUpstreamGenerationUnknown {
		t.Fatalf("upstream_generation = %q", ready.UpstreamGeneration)
	}
	if ready.AliasResolution != lifecycleAliasResolutionUnsupported {
		t.Fatalf("alias_resolution = %q", ready.AliasResolution)
	}
	if ready.RequestIDMapping != lifecycleRequestIDMappingUnsupported {
		t.Fatalf("request_id_mapping = %q", ready.RequestIDMapping)
	}
	if ready.ModelExecutionAttribution != lifecycleModelExecutionAttributionUnverified {
		t.Fatalf("model_execution_attribution = %q", ready.ModelExecutionAttribution)
	}
	// Report-level attribution fields.
	if report.UpstreamGenerationUnknown != lifecycleUpstreamGenerationUnknown {
		t.Fatalf("report upstream_generation = %q", report.UpstreamGenerationUnknown)
	}
	if report.AliasResolutionUnsupported != lifecycleAliasResolutionUnsupported {
		t.Fatalf("report alias_resolution = %q", report.AliasResolutionUnsupported)
	}
	if report.RequestIDMappingUnsupported != lifecycleRequestIDMappingUnsupported {
		t.Fatalf("report request_id_mapping = %q", report.RequestIDMappingUnsupported)
	}
	if report.ModelExecutionAttributionUnverified != lifecycleModelExecutionAttributionUnverified {
		t.Fatalf("report model_execution_attribution = %q", report.ModelExecutionAttributionUnverified)
	}
	// Bounds are serialized.
	if report.MaxModels != lifecycleMaxModelsGlobal {
		t.Fatalf("max_models = %d", report.MaxModels)
	}
	if report.MaxModelsPerBackend != lifecycleMaxModelsPerBackend {
		t.Fatalf("max_models_per_backend = %d", report.MaxModelsPerBackend)
	}
	if report.Coverage != "complete" {
		t.Fatalf("coverage = %q, want complete", report.Coverage)
	}
	if report.Truncated {
		t.Fatal("should not be truncated")
	}
	if report.TotalModels != 3 {
		t.Fatalf("total_models = %d, want 3", report.TotalModels)
	}
	// Backend state is enabled with complete coverage.
	st := report.BackendStates["backend"]
	if st.State != lifecycleEnabledState {
		t.Fatalf("backend state = %q, want enabled", st.State)
	}
	if st.Coverage != "complete" {
		t.Fatalf("backend coverage = %q, want complete", st.Coverage)
	}
	if st.Rows != 3 {
		t.Fatalf("backend rows = %d, want 3", st.Rows)
	}
}

func TestLifecycleRawFieldsAndSecretsNotCopied(t *testing.T) {
	shortSecret := "SECRET"
	longSecret := strings.Repeat("SECRET-LONG-", 50)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// All raw fields carry secrets; only model and state are decoded.
		io.WriteString(w, `{"running":[{"model":"m1","state":"ready","cmd":"`+shortSecret+`","proxy":"`+longSecret+`","ttl":300,"name":"`+shortSecret+`","description":"`+longSecret+`"}]}`)
	}))
	defer server.Close()

	c := newTestLifecycleCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	// No secret (short or long) may appear anywhere in the output.
	for _, secret := range []string{shortSecret, longSecret} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("secret leaked in output: %q", secret)
		}
	}
	// Raw fields are absent from the model (only backend_id, epoch, model_ref,
	// state, ready_since, observed_at, and attribution fields).
	model := report.Models[0]
	if model.ModelRef == "" {
		t.Fatal("ModelRef must be set")
	}
	// ModelRef is opaque; the raw model ID is not derivable from output.
	if strings.Contains(model.ModelRef, "m1") {
		t.Fatalf("ModelRef should not leak raw model ID: %q", model.ModelRef)
	}
	if model.ReadySince != nil {
		t.Fatalf("ReadySince should be nil, got %v", model.ReadySince)
	}
}

func TestLifecycleReadySinceIsNilWhenNotExposed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, lifecyclePageBody(t, lifecycleRunningEntry("m", "ready")))
	}))
	defer server.Close()

	c := newTestLifecycleCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Models) != 1 {
		t.Fatalf("expected 1 model, got %d", len(report.Models))
	}
	// ReadySince is nil: /running does not expose a ready time. It is not a
	// zero time and not derived from any source field.
	if report.Models[0].ReadySince != nil {
		t.Fatalf("ReadySince should be nil, got %v", report.Models[0].ReadySince)
	}
}

func TestLifecycleAbsentModelsAreNotReported(t *testing.T) {
	// /running returns only "m1"; "m2" is absent and must not be reported as
	// unloaded. We simply do not create a row for it.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, lifecyclePageBody(t, lifecycleRunningEntry("m1", "ready")))
	}))
	defer server.Close()

	c := newTestLifecycleCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Models) != 1 {
		t.Fatalf("expected 1 model (only returned rows), got %d", len(report.Models))
	}
	if report.Models[0].ModelRef != modelRef("backend", "m1") {
		t.Fatalf("ModelRef = %q, want hash of backend:m1", report.Models[0].ModelRef)
	}
	if report.Models[0].State != lifecycleStateReady {
		t.Fatalf("state = %q, want ready", report.Models[0].State)
	}
	// No "unloaded" row for absent m2.
	for _, m := range report.Models {
		if m.State == "unloaded" || m.State == "offline" {
			t.Fatalf("must not infer unloaded for absent model: %#v", m)
		}
	}
}

func TestLifecycleDisabledBackendMakesNoRequests(t *testing.T) {
	var count atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		count.Add(1)
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, lifecyclePageBody(t, lifecycleRunningEntry("m", "ready")))
	}))
	defer server.Close()

	c := newTestLifecycleCollector(t, map[string]config.Backend{
		"off": {URL: server.URL}, // no Diagnostics -> disabled
	})
	if count.Load() != 0 {
		t.Fatal("disabled backend made requests")
	}
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Models) != 0 {
		t.Fatalf("expected 0 models for disabled backend, got %d", len(report.Models))
	}
	st := report.BackendStates["off"]
	if st.State != lifecycleDisabledState {
		t.Fatalf("disabled backend state = %q, want disabled", st.State)
	}
	if st.Reason != "not_enabled" {
		t.Fatalf("disabled backend reason = %q, want not_enabled", st.Reason)
	}
}

func TestLifecycleUnauthorized(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()

	c := newTestLifecycleCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Models) != 0 {
		t.Fatalf("expected 0 models on 401, got %d", len(report.Models))
	}
	st := report.BackendStates["backend"]
	if st.State != lifecycleUnauthorizedState {
		t.Fatalf("state = %q, want unauthorized", st.State)
	}
	if st.Reason != "authorization_required" {
		t.Fatalf("reason = %q, want authorization_required", st.Reason)
	}
	if st.HTTPStatus != http.StatusUnauthorized {
		t.Fatalf("http_status = %d, want 401", st.HTTPStatus)
	}
}

func TestLifecycleNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	c := newTestLifecycleCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	st := report.BackendStates["backend"]
	if st.State != lifecycleUnsupportedState {
		t.Fatalf("state = %q, want unsupported", st.State)
	}
	if st.Reason != "not_available" {
		t.Fatalf("reason = %q, want not_available", st.Reason)
	}
}

func TestLifecycleRedirectRefused(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", "/elsewhere")
		w.WriteHeader(http.StatusMovedPermanently)
	}))
	defer server.Close()

	c := newTestLifecycleCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	st := report.BackendStates["backend"]
	if st.State != lifecycleUnknownState {
		t.Fatalf("state = %q, want unknown", st.State)
	}
	if st.Reason != "redirect_refused" {
		t.Fatalf("reason = %q, want redirect_refused", st.Reason)
	}
	// No redirect is followed: the client never issues a second request.
}

func TestLifecycleOversizedBody(t *testing.T) {
	// Body larger than 1MiB.
	big := strings.Repeat("x", int(lifecycleMaxResponseBytes)+1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"running":[],"padding":"`+big+`"}`)
	}))
	defer server.Close()

	c := newTestLifecycleCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	st := report.BackendStates["backend"]
	if st.State != lifecycleUnknownState {
		t.Fatalf("state = %q, want unknown", st.State)
	}
	if st.Reason != "response_too_large" {
		t.Fatalf("reason = %q, want response_too_large", st.Reason)
	}
	if len(report.Models) != 0 {
		t.Fatalf("expected 0 models on oversized body, got %d", len(report.Models))
	}
}

func TestLifecycleMalformedJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, lifecyclePageBodyRaw(t, `{not valid json}`))
	}))
	defer server.Close()

	c := newTestLifecycleCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	st := report.BackendStates["backend"]
	if st.State != lifecycleUnknownState {
		t.Fatalf("state = %q, want unknown", st.State)
	}
	if st.Reason != "invalid_json" {
		t.Fatalf("reason = %q, want invalid_json", st.Reason)
	}
}

func TestLifecycleMissingRunningField(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"foo":"bar"}`)
	}))
	defer server.Close()

	c := newTestLifecycleCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	st := report.BackendStates["backend"]
	if st.State != lifecycleUnknownState {
		t.Fatalf("state = %q, want unknown", st.State)
	}
	if st.Reason != "malformed_page" {
		t.Fatalf("reason = %q, want malformed_page", st.Reason)
	}
}

func TestLifecycleRunningFieldNull(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"running":null}`)
	}))
	defer server.Close()

	c := newTestLifecycleCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	st := report.BackendStates["backend"]
	if st.State != lifecycleUnknownState {
		t.Fatalf("state = %q, want unknown", st.State)
	}
	if st.Reason != "malformed_page" {
		t.Fatalf("reason = %q, want malformed_page", st.Reason)
	}
}

func TestLifecycleUnknownStateNotGuessed(t *testing.T) {
	// Unrecognized state strings are reported unknown and never guessed.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, lifecyclePageBody(t,
			lifecycleRunningEntry("m1", "ready"),
			lifecycleRunningEntry("m2", "frobbing"),
			lifecycleRunningEntry("m3", ""),
		))
	}))
	defer server.Close()

	c := newTestLifecycleCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Models) != 3 {
		t.Fatalf("expected 3 models, got %d", len(report.Models))
	}
	byRef := make(map[string]*LifecycleModel)
	for i := range report.Models {
		byRef[report.Models[i].ModelRef] = &report.Models[i]
	}
	if byRef[modelRef("backend", "m1")].State != lifecycleStateReady {
		t.Fatalf("m1 state = %q, want ready", byRef[modelRef("backend", "m1")].State)
	}
	if byRef[modelRef("backend", "m2")].State != lifecycleStateUnknown {
		t.Fatalf("m2 state = %q, want unknown (unrecognized)", byRef[modelRef("backend", "m2")].State)
	}
	if byRef[modelRef("backend", "m3")].State != lifecycleStateUnknown {
		t.Fatalf("m3 state = %q, want unknown (empty)", byRef[modelRef("backend", "m3")].State)
	}
}

func TestLifecycleEpochMustBePositive(t *testing.T) {
	c := newTestLifecycleCollector(t, map[string]config.Backend{})
	_, err := c.Collect(context.Background(), 0)
	if err == nil || err.Error() != "epoch must be positive" {
		t.Fatalf("expected epoch error, got %v", err)
	}
	_, err = c.Collect(context.Background(), -1)
	if err == nil || err.Error() != "epoch must be positive" {
		t.Fatalf("expected error for negative epoch, got %v", err)
	}
}

func TestLifecycleContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Block until cancelled to test that cancellation propagates.
		<-time.After(5 * time.Second)
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, lifecyclePageBody(t, lifecycleRunningEntry("m", "ready")))
	}))
	defer server.Close()

	c := newTestLifecycleCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.Collect(ctx, 1)
	if err == nil {
		t.Fatal("expected context cancellation error")
	}
	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context error, got %v", err)
	}
}

func TestLifecycleOutage(t *testing.T) {
	// Connection refused: no server.
	c := newTestLifecycleCollector(t, map[string]config.Backend{
		"backend": {URL: "http://127.0.0.1:1", Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Models) != 0 {
		t.Fatalf("expected 0 models on outage, got %d", len(report.Models))
	}
	st := report.BackendStates["backend"]
	if st.State != lifecycleUnavailableState {
		t.Fatalf("state = %q, want unavailable", st.State)
	}
	if st.Reason != "request_failed" {
		t.Fatalf("reason = %q, want request_failed", st.Reason)
	}
	if st.HTTPStatus != 0 {
		t.Fatalf("http_status = %d, want 0", st.HTTPStatus)
	}
}

func TestLifecyclePerBackendCountCap(t *testing.T) {
	// 150 models from one backend: capped at 100.
	entries := make([]lifecycleRunningModel, 150)
	for i := 0; i < 150; i++ {
		entries[i] = lifecycleRunningEntry(fmt.Sprintf("model-%d", i), "ready")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, lifecyclePageBody(t, entries...))
	}))
	defer server.Close()

	c := newTestLifecycleCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Models) != lifecycleMaxModelsPerBackend {
		t.Fatalf("expected %d models (per-backend cap), got %d", lifecycleMaxModelsPerBackend, len(report.Models))
	}
	st := report.BackendStates["backend"]
	if !st.Truncated {
		t.Fatal("per-backend should be truncated")
	}
	if st.Coverage != "truncated" {
		t.Fatalf("backend coverage = %q, want truncated", st.Coverage)
	}
}

func TestLifecycleGlobalCountCap(t *testing.T) {
	// 11 backends, each returning 200 models (capped at 100 per backend =
	// 1100 total): global cap at 1000.
	backends := make(map[string]config.Backend, 11)
	for i := 0; i < 11; i++ {
		id := fmt.Sprintf("backend-%d", i)
		entries := make([]lifecycleRunningModel, 200)
		for j := 0; j < 200; j++ {
			entries[j] = lifecycleRunningEntry(fmt.Sprintf("%s-model-%d", id, j), "ready")
		}
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, lifecyclePageBody(t, entries...))
		}))
		defer s.Close()
		backends[id] = config.Backend{URL: s.URL, Diagnostics: "llama_swap"}
	}
	c := newTestLifecycleCollector(t, backends)
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Models) != lifecycleMaxModelsGlobal {
		t.Fatalf("expected %d models (global cap), got %d", lifecycleMaxModelsGlobal, len(report.Models))
	}
	if !report.Truncated {
		t.Fatal("global should be truncated")
	}
	if report.Coverage != "truncated" {
		t.Fatalf("report coverage = %q, want truncated", report.Coverage)
	}
	// Models are sorted deterministically by backend ID then ModelRef.
	for i := 1; i < len(report.Models); i++ {
		if report.Models[i-1].BackendID > report.Models[i].BackendID {
			t.Fatalf("models not sorted by backend ID at %d", i)
		}
		if report.Models[i-1].BackendID == report.Models[i].BackendID &&
			report.Models[i-1].ModelRef > report.Models[i].ModelRef {
			t.Fatalf("models not sorted by ModelRef at %d", i)
		}
	}
}

func TestLifecycleFourWorkersConcurrency(t *testing.T) {
	var mu sync.Mutex
	var active int
	var maxActive int
	servers := make([]*httptest.Server, 4)
	for i := 0; i < 4; i++ {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			mu.Lock()
			active++
			if active > maxActive {
				maxActive = active
			}
			mu.Unlock()
			time.Sleep(100 * time.Millisecond) // ensure overlap across workers
			mu.Lock()
			active--
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, lifecyclePageBody(t, lifecycleRunningEntry("m", "ready")))
		}))
		defer s.Close()
		servers[i] = s
	}
	backends := make(map[string]config.Backend, 4)
	for i, s := range servers {
		backends[fmt.Sprintf("backend-%d", i)] = config.Backend{URL: s.URL, Diagnostics: "llama_swap"}
	}
	c := newTestLifecycleCollector(t, backends)
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if maxActive < 4 {
		t.Fatalf("expected 4 concurrent workers, got maxActive=%d", maxActive)
	}
	if len(report.Models) != 4 {
		t.Fatalf("expected 4 models, got %d", len(report.Models))
	}
}

func TestLifecycleHeadersAndAPIKeyPassthrough(t *testing.T) {
	var gotAuth, gotCustom string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotCustom = r.Header.Get("X-Custom")
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, lifecyclePageBody(t, lifecycleRunningEntry("m", "ready")))
	}))
	defer server.Close()

	c := newTestLifecycleCollector(t, map[string]config.Backend{
		"backend": {
			URL:         server.URL,
			Diagnostics: "llama_swap",
			APIKey:      "secret-key",
			Headers:     map[string]string{"X-Custom": "custom-value"},
		},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if gotAuth != "Bearer secret-key" {
		t.Fatalf("Authorization = %q, want Bearer secret-key", gotAuth)
	}
	if gotCustom != "custom-value" {
		t.Fatalf("X-Custom = %q, want custom-value", gotCustom)
	}
	// Request path is base URL + /running only (no Prefix/HealthPath).
	if gotAuth == "" && gotCustom == "" {
		t.Fatal("headers should be passed through")
	}
	if len(report.Models) != 1 {
		t.Fatalf("expected 1 model, got %d", len(report.Models))
	}
}

func TestLifecycleNoRedirectFollowing(t *testing.T) {
	var redirects int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirects++
		if redirects == 1 {
			w.Header().Set("Location", "/running")
			w.WriteHeader(http.StatusMovedPermanently)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, lifecyclePageBody(t, lifecycleRunningEntry("m", "ready")))
	}))
	defer server.Close()

	c := newTestLifecycleCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	// Redirect is refused: no models collected, backend state unknown.
	if len(report.Models) != 0 {
		t.Fatalf("expected 0 models on redirect, got %d", len(report.Models))
	}
	st := report.BackendStates["backend"]
	if st.State != lifecycleUnknownState {
		t.Fatalf("state = %q, want unknown", st.State)
	}
	if st.Reason != "redirect_refused" {
		t.Fatalf("reason = %q, want redirect_refused", st.Reason)
	}
}

func TestLifecycleDefensiveCopies(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, lifecyclePageBody(t, lifecycleRunningEntry("m", "ready")))
	}))
	defer server.Close()

	c := newTestLifecycleCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	// Mutate the returned report; the collector's internal store must be
	// unaffected (there is no internal store for lifecycle, but returned
	// models must be copies).
	report.Models[0].State = "mutated"
	report.Models[0].ModelRef = "mutated"
	// Re-collect: original values preserved.
	report2, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if report2.Models[0].State != lifecycleStateReady {
		t.Fatalf("state changed after mutation: %q", report2.Models[0].State)
	}
	if report2.Models[0].ModelRef == "mutated" {
		t.Fatal("ModelRef was mutated")
	}
}

func TestLifecycleInvalidURLForms(t *testing.T) {
	for _, url := range []string{"", "not-a-url", "ftp://example.com", "http://example.com?query", "http://example.com#frag"} {
		c := newTestLifecycleCollector(t, map[string]config.Backend{
			"backend": {URL: url, Diagnostics: "llama_swap"},
		})
		report, err := c.Collect(context.Background(), 1)
		if err != nil {
			t.Fatalf("URL %q: unexpected error: %v", url, err)
		}
		st := report.BackendStates["backend"]
		if st.State != lifecycleUnsupportedState {
			t.Fatalf("URL %q: state = %q, want unsupported", url, st.State)
		}
		if len(report.Models) != 0 {
			t.Fatalf("URL %q: expected 0 models, got %d", url, len(report.Models))
		}
	}
}

func TestLifecycleSourceSnapshotNotActivityCausation(t *testing.T) {
	// The collector reports the current /running status snapshot only. It does
	// not infer activity, queue depth, cold start, or time since start.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, lifecyclePageBody(t, lifecycleRunningEntry("m", "starting")))
	}))
	defer server.Close()

	c := newTestLifecycleCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Models) != 1 {
		t.Fatalf("expected 1 model, got %d", len(report.Models))
	}
	m := report.Models[0]
	// State is strictly from the source.
	if m.State != lifecycleStateStarting {
		t.Fatalf("state = %q, want starting", m.State)
	}
	// No derived fields: no start time, queue depth, cold start.
	if m.ReadySince != nil {
		t.Fatalf("ReadySince should be nil (no derived start time), got %v", m.ReadySince)
	}
	// One observation does not prove state change or backend boot.
	if report.Coverage != "complete" {
		t.Fatalf("coverage = %q, want complete", report.Coverage)
	}
}

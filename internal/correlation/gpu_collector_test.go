package correlation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

var gpuFixtureTime = time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC)

func TestGPUUnavailableIsNotCompleteCoverage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()
	c := NewGPUCollector(map[string]config.Backend{"backend": {URL: server.URL, Diagnostics: "llama_swap"}})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if report.Coverage != "unknown" || len(report.Samples) != 0 {
		t.Fatalf("unavailable data reported as complete: coverage=%s, samples=%d", report.Coverage, len(report.Samples))
	}
}

type gpuMockClock struct {
	now time.Time
}

func (m *gpuMockClock) Now() time.Time { return m.now }

func newTestGPUCollector(t *testing.T, backends map[string]config.Backend) *GPUCollector {
	t.Helper()
	clock := &gpuMockClock{now: gpuFixtureTime}
	c := NewGPUCollector(backends)
	c.now = func() time.Time { return clock.now }
	return c
}

func boolPtr(b bool) *bool { return &b }

// gpuStatEntry builds a public fixture GpuStat. All numeric fields are present
// (so they decode as explicit values, not absent).
func gpuStatEntry(id int, name string, gpuUtilPct, memUtilPct float64, memUsedMB, memTotalMB int) GpuStat {
	return GpuStat{
		Timestamp:  gpuFixtureTime,
		ID:         id,
		Name:       name,
		UUID:       "uuid-" + fmt.Sprintf("%d", id),
		GpuUtilPct: gpuUtilPct,
		MemUtilPct: memUtilPct,
		MemUsedMB:  memUsedMB,
		MemTotalMB: memTotalMB,
	}
}

// gpuStatToWire converts a fixture GpuStat into the private wire type used for
// production decoding. Every field is present, mirroring an explicit JSON body.
func gpuStatToWire(g GpuStat) wireGpuStat {
	return wireGpuStat{
		Timestamp:  g.Timestamp,
		ID:         &g.ID,
		GpuUtilPct: &g.GpuUtilPct,
		MemUtilPct: &g.MemUtilPct,
		MemUsedMB:  &g.MemUsedMB,
		MemTotalMB: &g.MemTotalMB,
	}
}

func gpuPageBody(t *testing.T, enabled bool, stats ...GpuStat) string {
	t.Helper()
	wireStats := make([]wireGpuStat, 0, len(stats))
	for _, s := range stats {
		wireStats = append(wireStats, gpuStatToWire(s))
	}
	page := gpuPerformancePage{
		Enabled:  boolPtr(enabled),
		GPUStats: &wireStats,
	}
	b, err := json.Marshal(page)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// gpuPageBodyRaw marshals an arbitrary raw body used to test absent fields.
func gpuPageBodyRaw(t *testing.T, body string) string {
	t.Helper()
	return body
}

func TestGpuConstructionMakesNoRequestsAndSkipsDisabledBackends(t *testing.T) {
	var count atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, gpuPageBody(t, true, gpuStatEntry(1, "gpu-0", 50.0, 50.0, 1024, 4096)))
	}))
	defer server.Close()

	backends := map[string]config.Backend{
		"off": {URL: server.URL}, // no Diagnostics -> disabled
		"on":  {URL: server.URL, Diagnostics: "llama_swap"},
	}
	c := newTestGPUCollector(t, backends)
	if count.Load() != 0 {
		t.Fatal("construction made requests")
	}
	if c.backendStates["off"].State != gpuDisabledState || c.backendStates["off"].Reason != "not_enabled" {
		t.Fatalf("disabled backend state = %#v", c.backendStates["off"])
	}
	if _, exists := c.backends["off"]; exists {
		t.Fatal("disabled backend must not be in backends map")
	}

	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if count.Load() != 1 {
		t.Fatalf("enabled backend request count = %d, want 1", count.Load())
	}
	if report.BackendStates["off"].State != gpuDisabledState {
		t.Fatal("disabled backend should not be collected")
	}
}

func TestCollectValidatesEpoch(t *testing.T) {
	c := newTestGPUCollector(t, nil)
	if _, err := c.Collect(context.Background(), 0); err == nil {
		t.Fatal("expected error for epoch 0")
	}
	if _, err := c.Collect(context.Background(), -1); err == nil {
		t.Fatal("expected error for negative epoch")
	}
	if _, err := c.Collect(context.Background(), 1); err != nil {
		t.Fatalf("expected no error for positive epoch, got %v", err)
	}
}

func TestCollectSourceRealSchemaAndUnits(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/performance" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, gpuPageBody(t, true,
			gpuStatEntry(0, "NVIDIA GPU", 42.5, 30.0, 2048, 8192),
		))
	}))
	defer server.Close()

	c := newTestGPUCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Samples) != 1 {
		t.Fatalf("expected 1 sample, got %d", len(report.Samples))
	}
	s := report.Samples[0]
	if s.BackendID != "backend" {
		t.Fatalf("backend_id = %q, want backend", s.BackendID)
	}
	if s.Epoch != 1 {
		t.Fatalf("epoch = %d, want 1", s.Epoch)
	}
	if !strings.HasPrefix(s.DeviceID, "gpu-") {
		t.Fatalf("device_id = %q, want gpu- prefix", s.DeviceID)
	}
	// DeviceID must be opaque: no raw name/UUID.
	if strings.Contains(s.DeviceID, "NVIDIA") || strings.Contains(s.DeviceID, "uuid-0") {
		t.Fatalf("device_id leaks raw metadata: %q", s.DeviceID)
	}
	if s.Timestamp != gpuFixtureTime {
		t.Fatalf("timestamp = %v, want source timestamp", s.Timestamp)
	}
	if s.ObservedAt.IsZero() {
		t.Fatal("observed_at must be set")
	}
	// Units: percent for utilization, MB for memory. Explicit 0 is preserved
	// as a non-nil pointer here (values are non-zero).
	if s.GpuUtilPct == nil || *s.GpuUtilPct != 42.5 {
		t.Fatalf("gpu_util_pct = %v, want 42.5 (percent)", s.GpuUtilPct)
	}
	if s.MemUtilPct == nil || *s.MemUtilPct != 30.0 {
		t.Fatalf("mem_util_pct = %v, want 30.0 (percent)", s.MemUtilPct)
	}
	if s.MemUsedMB == nil || *s.MemUsedMB != 2048 {
		t.Fatalf("mem_used_mb = %v, want 2048 (MB)", s.MemUsedMB)
	}
	if s.MemTotalMB == nil || *s.MemTotalMB != 8192 {
		t.Fatalf("mem_total_mb = %v, want 8192 (MB)", s.MemTotalMB)
	}
	// No raw upstream fields in output.
	encoded, _ := json.Marshal(report)
	for _, raw := range []string{"NVIDIA GPU", "uuid-0", "temp_c", "fan_speed_pct", "power_draw_w"} {
		if strings.Contains(string(encoded), raw) {
			t.Fatalf("raw upstream field leaked: %q", raw)
		}
	}
	// Attribution limits are always serialized and reflect the collection
	// contract, not an inference-GPU claim.
	if report.SharedDeviceOnly != true {
		t.Fatal("shared_device_only should be true")
	}
	if report.ExecutionDeviceUnverified != true {
		t.Fatal("execution_device_unverified should be true")
	}
	if report.ResourceScope != "backend_host" {
		t.Fatalf("resource_scope = %q, want backend_host", report.ResourceScope)
	}
	if report.LifecycleState != "unknown" {
		t.Fatalf("lifecycle_state = %q, want unknown", report.LifecycleState)
	}
	if report.EpochScope != "collector" {
		t.Fatalf("epoch_scope = %q, want collector", report.EpochScope)
	}
	if report.ClockAlignmentUnverified != true {
		t.Fatal("clock_alignment_unverified should be true")
	}
	if report.UpstreamGenerationUnknown != gpuUpstreamGenerationUnknown {
		t.Fatalf("upstream_generation = %q, want %q", report.UpstreamGenerationUnknown, gpuUpstreamGenerationUnknown)
	}
	if report.RequestIDMappingUnsupported != gpuRequestIDMappingUnsupported {
		t.Fatalf("request_id_mapping = %q, want %q", report.RequestIDMappingUnsupported, gpuRequestIDMappingUnsupported)
	}
}

func TestDisabledMonitoringReportsDisabledEven503(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, `{"enabled":false}`)
	}))
	defer server.Close()

	c := newTestGPUCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if report.BackendStates["backend"].State != gpuDisabledState {
		t.Fatalf("expected disabled state on enabled:false, got %#v", report.BackendStates["backend"])
	}
	if report.BackendStates["backend"].HTTPStatus != http.StatusServiceUnavailable {
		t.Fatalf("http_status = %d, want 503", report.BackendStates["backend"].HTTPStatus)
	}
}

func Test503EnabledTrueIsUnavailable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, `{"enabled":true,"gpu_stats":[]}`)
	}))
	defer server.Close()

	c := newTestGPUCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if report.BackendStates["backend"].State != gpuUnavailableState {
		t.Fatalf("expected unavailable state on 503+enabled:true, got %#v", report.BackendStates["backend"])
	}
	if report.BackendStates["backend"].HTTPStatus != http.StatusServiceUnavailable {
		t.Fatalf("http_status = %d, want 503", report.BackendStates["backend"].HTTPStatus)
	}
	if len(report.Samples) != 0 {
		t.Fatalf("expected 0 samples on unavailable, got %d", len(report.Samples))
	}
}

func TestUnsupported404405(t *testing.T) {
	for _, code := range []int{http.StatusNotFound, http.StatusMethodNotAllowed} {
		t.Run(fmt.Sprintf("code-%d", code), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(code)
				fmt.Fprint(w, `{"enabled":true,"gpu_stats":[]}`)
			}))
			defer server.Close()

			c := newTestGPUCollector(t, map[string]config.Backend{
				"backend": {URL: server.URL, Diagnostics: "llama_swap"},
			})
			report, err := c.Collect(context.Background(), 1)
			if err != nil {
				t.Fatal(err)
			}
			if report.BackendStates["backend"].State != gpuUnsupportedState {
				t.Fatalf("expected unsupported state on %d, got %#v", code, report.BackendStates["backend"])
			}
			if report.BackendStates["backend"].HTTPStatus != code {
				t.Fatalf("http_status = %d, want %d", report.BackendStates["backend"].HTTPStatus, code)
			}
		})
	}
}

func TestUnauthorized401403(t *testing.T) {
	for _, code := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(fmt.Sprintf("code-%d", code), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(code)
			}))
			defer server.Close()

			c := newTestGPUCollector(t, map[string]config.Backend{
				"backend": {URL: server.URL, Diagnostics: "llama_swap"},
			})
			report, err := c.Collect(context.Background(), 1)
			if err != nil {
				t.Fatal(err)
			}
			if report.BackendStates["backend"].State != gpuUnauthorizedState {
				t.Fatalf("expected unauthorized state on %d, got %#v", code, report.BackendStates["backend"])
			}
		})
	}
}

func TestRedirectRefusalNoCredentialForwarding(t *testing.T) {
	var reached atomic.Int32
	tripwire := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached.Add(1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, gpuPageBody(t, true, gpuStatEntry(1, "gpu-0", 50.0, 50.0, 1024, 4096)))
	}))
	defer tripwire.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, tripwire.URL+"/api/performance", http.StatusFound)
	}))
	defer server.Close()

	// No credential forwarding: the redirect target must not be hit.
	c := newTestGPUCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap", APIKey: "SECRET-KEY"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if report.BackendStates["backend"].State != gpuUnknownState || report.BackendStates["backend"].Reason != "redirect_refused" {
		t.Fatalf("redirect not refused: %#v", report.BackendStates["backend"])
	}
	if reached.Load() != 0 {
		t.Fatal("redirect was followed despite no-redirect policy")
	}
	// No credential in output.
	encoded, _ := json.Marshal(report)
	if strings.Contains(string(encoded), "SECRET-KEY") {
		t.Fatal("API key leaked in output")
	}
}

func TestTimeoutAndOutage(t *testing.T) {
	// Timeout: server never responds. The collector's own request timeout
	// bounds the call; with no parent cancellation Collect returns no error
	// and an unavailable backend state.
	slowServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-time.After(time.Hour) // never responds
	}))
	c := newTestGPUCollector(t, map[string]config.Backend{
		"backend": {URL: slowServer.URL, Diagnostics: "llama_swap"},
	})
	c.timeout = 100 * time.Millisecond
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatalf("expected no error on timeout, got %v", err)
	}
	if report.BackendStates["backend"].State != gpuUnavailableState {
		t.Fatalf("expected unavailable state on timeout, got %#v", report.BackendStates["backend"])
	}
	// Close the slow server in a background goroutine so Close() does not
	// block on the in-flight connection.
	go func() {
		time.Sleep(2 * time.Second)
		slowServer.Close()
	}()

	// Outage: server closed.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, gpuPageBody(t, true, gpuStatEntry(1, "gpu-0", 50.0, 50.0, 1024, 4096)))
	}))
	defer server.Close()
	c2 := newTestGPUCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	if _, err := c2.Collect(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	server.Close()
	report2, err := c2.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if report2.BackendStates["backend"].State != gpuUnavailableState {
		t.Fatalf("expected unavailable state on outage, got %#v", report2.BackendStates["backend"])
	}
}

func TestMalformedAndOversizedBody(t *testing.T) {
	// Malformed JSON.
	for _, body := range []string{`not json`, `{"enabled":true,"gpu_stats":[]} trailing`} {
		t.Run(fmt.Sprintf("body-%d", len(body)), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, body)
			}))
			defer server.Close()

			c := newTestGPUCollector(t, map[string]config.Backend{
				"backend": {URL: server.URL, Diagnostics: "llama_swap"},
			})
			report, err := c.Collect(context.Background(), 1)
			if err != nil {
				t.Fatal(err)
			}
			if report.BackendStates["backend"].State != gpuUnknownState {
				t.Fatalf("expected unknown state on malformed body, got %#v", report.BackendStates["backend"])
			}
			if len(report.Samples) != 0 {
				t.Fatalf("expected 0 samples on malformed body, got %d", len(report.Samples))
			}
		})
	}

	// Oversized body (> 1MiB).
	t.Run("oversized", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"enabled":true,"gpu_stats":[]}`)
			// Pad with > 1MiB of data.
			pad := strings.Repeat("x", int(gpuMaxResponseBytes+1))
			fmt.Fprint(w, pad)
		}))
		defer server.Close()

		c := newTestGPUCollector(t, map[string]config.Backend{
			"backend": {URL: server.URL, Diagnostics: "llama_swap"},
		})
		report, err := c.Collect(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if report.BackendStates["backend"].State != gpuUnknownState {
			t.Fatalf("expected unknown state on oversized body, got %#v", report.BackendStates["backend"])
		}
		if len(report.Samples) != 0 {
			t.Fatalf("expected 0 samples on oversized body, got %d", len(report.Samples))
		}
	})
}

func TestMissingEnabledIsUnknown(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"gpu_stats":[]}`)
	}))
	defer server.Close()

	c := newTestGPUCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if report.BackendStates["backend"].State != gpuUnknownState {
		t.Fatalf("expected unknown state on missing enabled, got %#v", report.BackendStates["backend"])
	}
	if report.BackendStates["backend"].Coverage != "partial" {
		t.Fatalf("coverage = %q, want partial", report.BackendStates["backend"].Coverage)
	}
	if len(report.Samples) != 0 {
		t.Fatalf("expected 0 samples on missing enabled, got %d", len(report.Samples))
	}
}

func TestMissingGpuStatsIsUnknownPartial(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"enabled":true}`)
	}))
	defer server.Close()

	c := newTestGPUCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if report.BackendStates["backend"].State != gpuUnknownState {
		t.Fatalf("expected unknown state on missing gpu_stats, got %#v", report.BackendStates["backend"])
	}
	if report.BackendStates["backend"].Coverage != "partial" {
		t.Fatalf("coverage = %q, want partial", report.BackendStates["backend"].Coverage)
	}
}

func TestExplicitEmptyGpuStatsIsComplete(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"enabled":true,"gpu_stats":[]}`)
	}))
	defer server.Close()

	c := newTestGPUCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if report.BackendStates["backend"].State != gpuEnabledState {
		t.Fatalf("expected enabled state on explicit empty gpu_stats, got %#v", report.BackendStates["backend"])
	}
	if report.BackendStates["backend"].Coverage != "complete" {
		t.Fatalf("coverage = %q, want complete", report.BackendStates["backend"].Coverage)
	}
	if len(report.Samples) != 0 {
		t.Fatalf("expected 0 samples, got %d", len(report.Samples))
	}
}

func TestExplicitZeroVsUnknown(t *testing.T) {
	// Explicit 0 utilization and memory are preserved as non-nil pointers.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, gpuPageBody(t, true,
			// All explicit 0 values are preserved.
			gpuStatEntry(0, "gpu-0", 0.0, 0.0, 0, 0),
		))
	}))
	defer server.Close()

	c := newTestGPUCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Samples) != 1 {
		t.Fatalf("expected 1 sample, got %d", len(report.Samples))
	}
	s := report.Samples[0]
	if s.GpuUtilPct == nil || *s.GpuUtilPct != 0.0 {
		t.Fatalf("gpu_util_pct = %v, want explicit 0.0", s.GpuUtilPct)
	}
	if s.MemUtilPct == nil || *s.MemUtilPct != 0.0 {
		t.Fatalf("mem_util_pct = %v, want explicit 0.0", s.MemUtilPct)
	}
	if s.MemUsedMB == nil || *s.MemUsedMB != 0 {
		t.Fatalf("mem_used_mb = %v, want explicit 0 (non-nil)", s.MemUsedMB)
	}
	if s.MemTotalMB == nil || *s.MemTotalMB != 0 {
		t.Fatalf("mem_total_mb = %v, want explicit 0 (non-nil)", s.MemTotalMB)
	}
}

func TestAbsentMemoryIsNil(t *testing.T) {
	// Absent memory fields decode to nil; explicit 0 decodes to non-nil 0.
	body := `{"enabled":true,"gpu_stats":[{"id":0,"timestamp":"2026-11-11T10:00:00Z","gpu_util_pct":50.0,"mem_util_pct":50.0,"mem_used_mb":0}]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, body)
	}))
	defer server.Close()

	c := newTestGPUCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Samples) != 1 {
		t.Fatalf("expected 1 sample, got %d", len(report.Samples))
	}
	s := report.Samples[0]
	// mem_used_mb is explicit 0 -> non-nil 0.
	if s.MemUsedMB == nil || *s.MemUsedMB != 0 {
		t.Fatalf("mem_used_mb = %v, want explicit 0 (non-nil)", s.MemUsedMB)
	}
	// mem_total_mb is absent -> nil.
	if s.MemTotalMB != nil {
		t.Fatalf("mem_total_mb = %v, want nil (absent)", s.MemTotalMB)
	}
}

func TestAbsentIDVsZeroID(t *testing.T) {
	// A stat without an id is skipped; a stat with id 0 is retained.
	body := `{"enabled":true,"gpu_stats":[{"timestamp":"2026-11-11T10:00:00Z","gpu_util_pct":50.0},{"id":0,"timestamp":"2026-11-11T10:00:00Z","gpu_util_pct":50.0}]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, body)
	}))
	defer server.Close()

	c := newTestGPUCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Samples) != 1 {
		t.Fatalf("expected 1 sample (id 0 only), got %d", len(report.Samples))
	}
	if report.Samples[0].DeviceID != deviceID("backend", 0) {
		t.Fatalf("device_id = %q, want %q", report.Samples[0].DeviceID, deviceID("backend", 0))
	}
}

func TestNegativeIDSkipped(t *testing.T) {
	body := `{"enabled":true,"gpu_stats":[{"id":-1,"timestamp":"2026-11-11T10:00:00Z","gpu_util_pct":50.0},{"id":1,"timestamp":"2026-11-11T10:00:00Z","gpu_util_pct":50.0}]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, body)
	}))
	defer server.Close()

	c := newTestGPUCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Samples) != 1 {
		t.Fatalf("expected 1 sample (negative id skipped), got %d", len(report.Samples))
	}
	if report.Samples[0].DeviceID != deviceID("backend", 1) {
		t.Fatalf("device_id = %q, want %q", report.Samples[0].DeviceID, deviceID("backend", 1))
	}
}

func TestMultipleDevices(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, gpuPageBody(t, true,
			gpuStatEntry(0, "gpu-0", 50.0, 50.0, 1024, 4096),
			gpuStatEntry(1, "gpu-1", 60.0, 60.0, 2048, 8192),
			gpuStatEntry(2, "gpu-2", 70.0, 70.0, 4096, 16384),
		))
	}))
	defer server.Close()

	c := newTestGPUCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Samples) != 3 {
		t.Fatalf("expected 3 samples, got %d", len(report.Samples))
	}
	// Device IDs must be distinct and opaque.
	ids := map[string]bool{}
	for _, s := range report.Samples {
		if !strings.HasPrefix(s.DeviceID, "gpu-") {
			t.Fatalf("device_id = %q, want gpu- prefix", s.DeviceID)
		}
		if ids[s.DeviceID] {
			t.Fatalf("duplicate device_id: %q", s.DeviceID)
		}
		ids[s.DeviceID] = true
		// No raw name/UUID in device ID.
		if strings.Contains(s.DeviceID, "uuid-") {
			t.Fatalf("device_id leaks raw metadata: %q", s.DeviceID)
		}
	}
	if report.BackendStates["backend"].Devices != 3 {
		t.Fatalf("devices = %d, want 3", report.BackendStates["backend"].Devices)
	}
	if report.BackendStates["backend"].Samples != 3 {
		t.Fatalf("samples = %d, want 3", report.BackendStates["backend"].Samples)
	}
}

func TestMaxDevicesPerBackend(t *testing.T) {
	// 10 devices, capped at 8 distinct devices.
	stats := make([]GpuStat, 0, 10)
	for i := 0; i < 10; i++ {
		stats = append(stats, gpuStatEntry(i, fmt.Sprintf("gpu-%d", i), 50.0, 50.0, 1024, 4096))
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, gpuPageBody(t, true, stats...))
	}))
	defer server.Close()

	c := newTestGPUCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Samples) != gpuMaxDevicesPerBackend {
		t.Fatalf("expected %d samples (device cap), got %d", gpuMaxDevicesPerBackend, len(report.Samples))
	}
	if report.BackendStates["backend"].Devices != gpuMaxDevicesPerBackend {
		t.Fatalf("devices = %d, want %d", report.BackendStates["backend"].Devices, gpuMaxDevicesPerBackend)
	}
	if report.BackendStates["backend"].Coverage != "truncated" {
		t.Fatalf("coverage = %q, want truncated", report.BackendStates["backend"].Coverage)
	}
	if !report.BackendStates["backend"].Truncated {
		t.Fatal("backend truncated should be true")
	}
}

func TestTwentyTimestampsOneDeviceRetained(t *testing.T) {
	// Many time points on one device must all be retained (not capped at 8).
	stats := make([]GpuStat, 0, 25)
	for i := 0; i < 25; i++ {
		stats = append(stats, GpuStat{
			Timestamp:  gpuFixtureTime.Add(time.Duration(i) * time.Second),
			ID:         0,
			GpuUtilPct: 50.0,
			MemUtilPct: 50.0,
			MemUsedMB:  1024,
			MemTotalMB: 4096,
		})
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, gpuPageBody(t, true, stats...))
	}))
	defer server.Close()

	c := newTestGPUCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Samples) != 25 {
		t.Fatalf("expected 25 samples (one device, many time points), got %d", len(report.Samples))
	}
	if report.BackendStates["backend"].Devices != 1 {
		t.Fatalf("devices = %d, want 1", report.BackendStates["backend"].Devices)
	}
	if report.BackendStates["backend"].Coverage != "complete" {
		t.Fatalf("coverage = %q, want complete", report.BackendStates["backend"].Coverage)
	}
	// Stable timestamp+device sort.
	for i := 1; i < len(report.Samples); i++ {
		if report.Samples[i].Timestamp.Before(report.Samples[i-1].Timestamp) {
			t.Fatal("samples not sorted by timestamp")
		}
	}
}

func TestTimestampsOutOfOrderDuplicates(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Out of order and duplicate timestamps.
		e1 := gpuStatEntry(0, "gpu-0", 50.0, 50.0, 1024, 4096)
		e2 := gpuStatEntry(1, "gpu-1", 60.0, 60.0, 2048, 8192)
		e2.Timestamp = gpuFixtureTime.Add(-time.Hour) // out of order
		e3 := gpuStatEntry(2, "gpu-2", 70.0, 70.0, 4096, 16384)
		e3.Timestamp = e1.Timestamp // duplicate timestamp
		fmt.Fprint(w, gpuPageBody(t, true, e1, e2, e3))
	}))
	defer server.Close()

	c := newTestGPUCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Samples) != 3 {
		t.Fatalf("expected 3 samples, got %d", len(report.Samples))
	}
	// All samples collected regardless of timestamp order/duplicates.
	for _, s := range report.Samples {
		if s.Timestamp.IsZero() {
			t.Fatal("timestamp must not be zero")
		}
		if s.ObservedAt.IsZero() {
			t.Fatal("observed_at must not be zero")
		}
	}
	// Stable sort by timestamp then device.
	ts := report.Samples[0].Timestamp
	for i := 1; i < len(report.Samples); i++ {
		if report.Samples[i].Timestamp.Before(ts) {
			t.Fatal("samples not sorted by timestamp")
		}
		ts = report.Samples[i].Timestamp
	}
}

func TestInvalidNumericPercentCounters(t *testing.T) {
	// Out-of-range percent and negative memory => nil field, row retained.
	body := `{"enabled":true,"gpu_stats":[{"id":0,"timestamp":"2026-11-11T10:00:00Z","gpu_util_pct":150.0,"mem_util_pct":-5.0,"mem_used_mb":-1,"mem_total_mb":4096}]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, body)
	}))
	defer server.Close()

	c := newTestGPUCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Samples) != 1 {
		t.Fatalf("expected 1 sample (row retained), got %d", len(report.Samples))
	}
	s := report.Samples[0]
	if s.GpuUtilPct != nil {
		t.Fatalf("gpu_util_pct = %v, want nil (out of range)", s.GpuUtilPct)
	}
	if s.MemUtilPct != nil {
		t.Fatalf("mem_util_pct = %v, want nil (negative)", s.MemUtilPct)
	}
	if s.MemUsedMB != nil {
		t.Fatalf("mem_used_mb = %v, want nil (negative)", s.MemUsedMB)
	}
	if s.MemTotalMB == nil || *s.MemTotalMB != 4096 {
		t.Fatalf("mem_total_mb = %v, want 4096", s.MemTotalMB)
	}
}

func TestNormalizeWireStatOutOfRangeIsNilRowRetained(t *testing.T) {
	c := newTestGPUCollector(t, nil)
	id := 0
	// NaN and Inf utilization => nil field, row retained.
	nan := math.NaN()
	s := c.normalizeWireStat("backend", wireGpuStat{
		ID:         &id,
		Timestamp:  gpuFixtureTime,
		GpuUtilPct: &nan,
	})
	if s == nil {
		t.Fatal("row with NaN util must be retained")
	}
	if s.GpuUtilPct != nil {
		t.Fatalf("gpu_util_pct = %v, want nil (NaN)", s.GpuUtilPct)
	}
	inf := math.Inf(1)
	s2 := c.normalizeWireStat("backend", wireGpuStat{
		ID:         &id,
		Timestamp:  gpuFixtureTime,
		GpuUtilPct: &inf,
	})
	if s2 == nil {
		t.Fatal("row with Inf util must be retained")
	}
	if s2.GpuUtilPct != nil {
		t.Fatalf("gpu_util_pct = %v, want nil (Inf)", s2.GpuUtilPct)
	}
	// > 100 percent => nil, row retained.
	over := 150.0
	s3 := c.normalizeWireStat("backend", wireGpuStat{
		ID:         &id,
		Timestamp:  gpuFixtureTime,
		GpuUtilPct: &over,
	})
	if s3 == nil {
		t.Fatal("row with >100 util must be retained")
	}
	if s3.GpuUtilPct != nil {
		t.Fatalf("gpu_util_pct = %v, want nil (>100)", s3.GpuUtilPct)
	}
	// Negative memory => nil, row retained.
	memNeg := -1
	s4 := c.normalizeWireStat("backend", wireGpuStat{
		ID:        &id,
		Timestamp: gpuFixtureTime,
		MemUsedMB: &memNeg,
	})
	if s4 == nil {
		t.Fatal("row with negative memory must be retained")
	}
	if s4.MemUsedMB != nil {
		t.Fatalf("mem_used_mb = %v, want nil (negative)", s4.MemUsedMB)
	}
}

func TestSecretsNotInOutputOrLogs(t *testing.T) {
	short := "SHORT-SECRET"
	long := strings.Repeat("LONG-SECRET-", 100)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Secret in Name field (raw upstream metadata, not copied to output).
		e := gpuStatEntry(0, short, 50.0, 50.0, 1024, 4096)
		e.Name = long
		e.UUID = short
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, gpuPageBody(t, true, e))
	}))
	defer server.Close()

	// Capture stderr to ensure no logging.
	pipeReader, pipeWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe failed: %v", err)
	}
	defer pipeReader.Close()
	defer pipeWriter.Close()
	oldErr := os.Stderr
	os.Stderr = pipeWriter
	defer func() { os.Stderr = oldErr }()

	c := newTestGPUCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Samples) != 1 {
		t.Fatalf("expected 1 sample, got %d", len(report.Samples))
	}
	encoded, _ := json.Marshal(report)
	// No secret (short or long) may appear anywhere in the output.
	for _, secret := range []string{short, long} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("secret leaked in output: %q", secret)
		}
	}
	// No logging to stderr.
	pipeWriter.Close()
	out, _ := io.ReadAll(pipeReader)
	if len(out) != 0 {
		t.Fatal("collector logged backend data or secrets to stderr")
	}
}

func TestAuthAndBasePaths(t *testing.T) {
	var authHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		if r.URL.Path != "/api/performance" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, gpuPageBody(t, true, gpuStatEntry(0, "gpu-0", 50.0, 50.0, 1024, 4096)))
	}))
	defer server.Close()

	// API key in Authorization header.
	c := newTestGPUCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap", APIKey: "API-KEY"},
	})
	if _, err := c.Collect(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if authHeader != "Bearer API-KEY" {
		t.Fatalf("authorization = %q, want Bearer API-KEY", authHeader)
	}

	// Custom header preserved.
	customHeader := ""
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		customHeader = r.Header.Get("X-Custom")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, gpuPageBody(t, true, gpuStatEntry(0, "gpu-0", 50.0, 50.0, 1024, 4096)))
	}))
	defer server2.Close()
	c2 := newTestGPUCollector(t, map[string]config.Backend{
		"backend": {URL: server2.URL, Diagnostics: "llama_swap", Headers: map[string]string{"X-Custom": "custom-value"}},
	})
	if _, err := c2.Collect(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if customHeader != "custom-value" {
		t.Fatalf("x-custom = %q, want custom-value", customHeader)
	}
}

func TestGlobalSampleCapAndConcurrency(t *testing.T) {
	// Multiple backends, each with many devices. Global cap of 4096.
	numBackends := 2
	devicesPerBackend := 8 // at the per-backend cap
	servers := make([]*httptest.Server, numBackends)
	for i := 0; i < numBackends; i++ {
		stats := make([]GpuStat, 0, devicesPerBackend)
		for j := 0; j < devicesPerBackend; j++ {
			stats = append(stats, gpuStatEntry(j, fmt.Sprintf("gpu-%d", j), 50.0, 50.0, 1024, 4096))
		}
		servers[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, gpuPageBody(t, true, stats...))
		}))
		defer servers[i].Close()
	}

	backends := make(map[string]config.Backend, numBackends)
	for i := 0; i < numBackends; i++ {
		backends[fmt.Sprintf("backend-%d", i)] = config.Backend{URL: servers[i].URL, Diagnostics: "llama_swap"}
	}
	c := newTestGPUCollector(t, backends)
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Samples) > gpuMaxSamplesGlobal {
		t.Fatalf("total samples = %d, exceed global cap %d", len(report.Samples), gpuMaxSamplesGlobal)
	}
	// Each backend contributes at most gpuMaxDevicesPerBackend samples.
	for id, st := range report.BackendStates {
		if st.Devices > gpuMaxDevicesPerBackend {
			t.Fatalf("backend %s devices = %d, exceed cap %d", id, st.Devices, gpuMaxDevicesPerBackend)
		}
	}
}

func TestGlobalCapVisible(t *testing.T) {
	// Two backends each with 2500 records (one device, many time points).
	// Per backend: 2500 < 4096 record cap, not truncated. Global: 5000 > 4096
	// => global truncated, 4096 samples retained deterministically.
	gen := func(count int) []GpuStat {
		stats := make([]GpuStat, 0, count)
		for i := 0; i < count; i++ {
			stats = append(stats, GpuStat{
				Timestamp:  gpuFixtureTime.Add(time.Duration(i) * time.Millisecond),
				ID:         0,
				GpuUtilPct: 50.0,
				MemUtilPct: 50.0,
				MemUsedMB:  1024,
				MemTotalMB: 4096,
			})
		}
		return stats
	}
	server0 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, gpuPageBody(t, true, gen(2500)...))
	}))
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, gpuPageBody(t, true, gen(2500)...))
	}))
	defer server0.Close()
	defer server1.Close()

	backends := map[string]config.Backend{
		"backend-0": {URL: server0.URL, Diagnostics: "llama_swap"},
		"backend-1": {URL: server1.URL, Diagnostics: "llama_swap"},
	}
	c := newTestGPUCollector(t, backends)
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Samples) != gpuMaxSamplesGlobal {
		t.Fatalf("total samples = %d, want %d (global cap)", len(report.Samples), gpuMaxSamplesGlobal)
	}
	if !report.Truncated {
		t.Fatal("global truncated should be true")
	}
	if report.Coverage != "truncated" {
		t.Fatalf("coverage = %q, want truncated", report.Coverage)
	}
	// Per-backend states truthful: neither backend hit its own record cap.
	for id, st := range report.BackendStates {
		if st.Truncated {
			t.Fatalf("backend %s should not be truncated (under its record cap)", id)
		}
		if st.Samples != 2500 {
			t.Fatalf("backend %s samples = %d, want 2500", id, st.Samples)
		}
	}
	// Deterministic retained subset: backend-0 samples first (sorted ID order).
	seenBackend0 := 0
	for _, s := range report.Samples {
		if s.BackendID == "backend-0" {
			seenBackend0++
		} else if s.BackendID == "backend-1" && seenBackend0 < 2500 {
			t.Fatal("backend-1 samples appear before all backend-0 samples")
		}
	}
	if seenBackend0 != 2500 {
		t.Fatalf("backend-0 samples = %d, want 2500", seenBackend0)
	}
}

func TestConcurrencyMax4Workers(t *testing.T) {
	var active, max atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := active.Add(1)
		defer active.Add(-1)
		for old := max.Load(); n > old; old = max.Load() {
			if max.CompareAndSwap(old, n) {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, gpuPageBody(t, true, gpuStatEntry(0, "gpu-0", 50.0, 50.0, 1024, 4096)))
	}))
	defer server.Close()

	backends := make(map[string]config.Backend)
	for i := 8; i > 0; i-- {
		backends[fmt.Sprintf("backend-%d", i)] = config.Backend{URL: server.URL, Diagnostics: "llama_swap"}
	}
	c := newTestGPUCollector(t, backends)
	if _, err := c.Collect(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if max.Load() > 4 {
		t.Fatalf("max concurrent HTTP = %d, want <= 4", max.Load())
	}
}

func TestCollectCancelReturnsContextError(t *testing.T) {
	// Cancel before Collect: the gate returns ctx.Err() immediately.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := newTestGPUCollector(t, map[string]config.Backend{
		"backend": {URL: "http://127.0.0.1:1", Diagnostics: "llama_swap"},
	})
	_, err := c.Collect(ctx, 1)
	if err == nil {
		t.Fatal("expected ctx error on canceled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestCollectCancelMidCollectionDrainsThenErrors(t *testing.T) {
	// Cancel mid-collection: workers are drained, then ctx.Err() is returned
	// with no partial success.
	slowServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(300 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, gpuPageBody(t, true, gpuStatEntry(0, "gpu-0", 50.0, 50.0, 1024, 4096)))
	}))
	defer slowServer.Close()

	backends := make(map[string]config.Backend, 4)
	for i := 0; i < 4; i++ {
		backends[fmt.Sprintf("backend-%d", i)] = config.Backend{URL: slowServer.URL, Diagnostics: "llama_swap"}
	}
	c := newTestGPUCollector(t, backends)
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, func() { cancel() })
	_, err := c.Collect(ctx, 1)
	if err == nil {
		t.Fatal("expected ctx error after mid-collection cancel")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestReportReturnsDefensiveCopies(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, gpuPageBody(t, true, gpuStatEntry(0, "gpu-0", 50.0, 50.0, 1024, 4096)))
	}))
	defer server.Close()

	c := newTestGPUCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	// Mutating the returned sample must not affect subsequent collections.
	report.Samples[0].BackendID = "mutated"
	report.Samples[0].GpuUtilPct = nil
	// Re-collect: fresh samples.
	report2, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if report2.Samples[0].BackendID == "mutated" || report2.Samples[0].GpuUtilPct == nil {
		t.Fatal("collect returned aliased samples")
	}
}

func TestUnknownBackendNotInBackendsMap(t *testing.T) {
	longID := strings.Repeat("invalid-id-", 50)
	c := newTestGPUCollector(t, map[string]config.Backend{
		longID: {URL: "http://example.test", Diagnostics: "llama_swap"},
		"ok":   {URL: "http://example.test", Diagnostics: "llama_swap"},
	})
	// Invalid (and overlong) backend IDs must not be registered.
	if _, exists := c.backends[longID]; exists {
		t.Fatal("invalid backend ID must not be in backends map")
	}
	if _, exists := c.backendStates[longID]; exists {
		t.Fatal("invalid backend ID must not be in backend states")
	}
}

func TestReportUncertaintyFields(t *testing.T) {
	// A backend with missing enabled reports unknown; the report-level
	// uncertainty fields are always true and describe the collection contract.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"gpu_stats":[]}`)
	}))
	defer server.Close()

	c := newTestGPUCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if report.BackendStates["backend"].State != gpuUnknownState {
		t.Fatalf("expected unknown backend state, got %#v", report.BackendStates["backend"])
	}
	// Report-level uncertainty fields are always serialized.
	if !report.SharedDeviceOnly {
		t.Fatal("shared_device_only should be true")
	}
	if !report.ExecutionDeviceUnverified {
		t.Fatal("execution_device_unverified should be true")
	}
	if report.ResourceScope != "backend_host" {
		t.Fatalf("resource_scope = %q, want backend_host", report.ResourceScope)
	}
	if report.LifecycleState != "unknown" {
		t.Fatalf("lifecycle_state = %q, want unknown", report.LifecycleState)
	}
	if report.EpochScope != "collector" {
		t.Fatalf("epoch_scope = %q, want collector", report.EpochScope)
	}
	// No raw hardware ids in output.
	encoded, _ := json.Marshal(report)
	if strings.Contains(string(encoded), "gpu-0") {
		t.Fatal("raw device index leaked in output")
	}
}

func TestDeterministicMergeOrder(t *testing.T) {
	// Two backends with out-of-order timestamps must merge in deterministic
	// backend-ID order, and repeated Collects must agree.
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, gpuPageBody(t, true,
			gpuStatEntry(0, "gpu-0", 50.0, 50.0, 1024, 4096),
			gpuStatEntry(1, "gpu-1", 60.0, 60.0, 2048, 8192),
		))
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, gpuPageBody(t, true,
			gpuStatEntry(2, "gpu-2", 70.0, 70.0, 4096, 16384),
		))
	}))
	defer server2.Close()

	c := newTestGPUCollector(t, map[string]config.Backend{
		"backend-b": {URL: server1.URL, Diagnostics: "llama_swap"},
		"backend-a": {URL: server2.URL, Diagnostics: "llama_swap"},
	})
	report1, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	report2, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(report1.Samples) != len(report2.Samples) {
		t.Fatalf("sample counts differ: %d vs %d", len(report1.Samples), len(report2.Samples))
	}
	for i := range report1.Samples {
		if report1.Samples[i].BackendID != report2.Samples[i].BackendID {
			t.Fatalf("sample %d backend differs: %q vs %q", i, report1.Samples[i].BackendID, report2.Samples[i].BackendID)
		}
		if report1.Samples[i].Timestamp != report2.Samples[i].Timestamp {
			t.Fatalf("sample %d timestamp differs", i)
		}
		if report1.Samples[i].DeviceID != report2.Samples[i].DeviceID {
			t.Fatalf("sample %d device differs", i)
		}
	}
	// backend-a sorts before backend-b.
	firstA := -1
	for i, s := range report1.Samples {
		if s.BackendID == "backend-a" && firstA == -1 {
			firstA = i
		}
	}
	if firstA != 0 {
		t.Fatal("backend-a samples must come first (sorted backend ID order)")
	}
	// Within each backend, samples sorted by timestamp.
	for _, rep := range []GPUReport{report1, report2} {
		for i := 1; i < len(rep.Samples); i++ {
			if rep.Samples[i].BackendID == rep.Samples[i-1].BackendID &&
				rep.Samples[i].Timestamp.Before(rep.Samples[i-1].Timestamp) {
				t.Fatal("samples within a backend not sorted by timestamp")
			}
		}
	}
}

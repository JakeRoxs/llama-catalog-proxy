package correlation

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

func TestCollectorLimitsOptionsClamped(t *testing.T) {
	c := newTestCollector(t, map[string]config.Backend{
		"backend": {URL: "http://example.test", Diagnostics: "llama_swap"},
	},
		WithMaxRecords(999999999),
		// WithMaxRowsPerBackend rejects >999 in the option itself; value stays at default.
		WithMaxRowsPerBackend(999999999),
		WithMaxSize(999999999),
		WithMaxResponseBytes(999999999),
		WithMaxAge(time.Hour*100000),
		WithTimeout(time.Second*100000),
	)
	if c.maxRecords != maxRecords {
		t.Fatalf("maxRecords = %d, want %d (clamped)", c.maxRecords, maxRecords)
	}
	// Option guards n <= 999, so huge value is rejected, not clamped.
	if c.maxRowsPerBackend != maxRowsPerBackend {
		t.Fatalf("maxRowsPerBackend = %d, want %d (default, option rejected huge)", c.maxRowsPerBackend, maxRowsPerBackend)
	}
	if c.maxAge != defaultMaxAge {
		t.Fatalf("maxAge = %v, want %v (clamped)", c.maxAge, defaultMaxAge)
	}
	if c.maxSize != defaultMaxSize {
		t.Fatalf("maxSize = %d, want %d (clamped)", c.maxSize, defaultMaxSize)
	}
	if c.timeout != defaultTimeout {
		t.Fatalf("timeout = %v, want %v (clamped)", c.timeout, defaultTimeout)
	}
	if c.maxResponseBytes != maxResponseBytes {
		t.Fatalf("maxResponseBytes = %d, want %d (clamped)", c.maxResponseBytes, maxResponseBytes)
	}
}

func TestCollectorLimitsMultiBackendRetention(t *testing.T) {
	// WithMaxRecords(2) across 6 backends: only 2 records retained total.
	backends := make(map[string]config.Backend, 6)
	for i := 0; i < 6; i++ {
		id := i + 1
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, activityPageBody(t, activityEntry(id, "m", 200)))
		}))
		defer server.Close()
		backends[fmt.Sprintf("backend-%d", i)] = config.Backend{URL: server.URL, Diagnostics: "llama_swap"}
	}
	c := newTestCollector(t, backends, WithMaxRecords(2))
	if _, err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.Snapshot()) != 2 {
		t.Fatalf("snapshot count = %d, want 2 (max_records=2)", len(c.Snapshot()))
	}
	// Stats returns defensive copies.
	stats := c.Stats()
	recs := stats["records"].([]*ActivityRecord)
	if len(recs) != 2 {
		t.Fatalf("stats records count = %d, want 2", len(recs))
	}
	recs[0].ModelID = "mutated"
	recs[0].IdentityConflict = true
	if c.Snapshot()[0].ModelID == "mutated" || c.Snapshot()[0].IdentityConflict {
		t.Fatal("stats returned aliased records")
	}
}

// assertMalformedActivityPage starts an httptest server serving the given body,
// runs the collector Refresh, and asserts the malformed page yields zero records
// and leaves the backend in the unknown state. The server is closed when the
// helper returns.
func assertMalformedActivityPage(t *testing.T, body string) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, body)
	}))
	defer server.Close()

	c := newTestCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	records, err := c.Refresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("expected 0 records on malformed page, got %d", len(records))
	}
	if c.backendState["backend"].State != "unknown" {
		t.Fatalf("expected unknown state, got %#v", c.backendState["backend"])
	}
}

func TestCollectorNoRawSecretNotPrinted(t *testing.T) {
	secret := "SUPER-SECRET-VALUE"
	pipeReader, pipeWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe failed: %v", err)
	}
	defer pipeReader.Close()
	defer pipeWriter.Close()
	oldOut := os.Stdout
	os.Stdout = pipeWriter
	defer func() { os.Stdout = oldOut }()

	assertMalformedActivityPage(t, `{"data":`+secret+`}`)

	pipeWriter.Close()
	out, _ := io.ReadAll(pipeReader)
	if len(out) != 0 {
		t.Fatal("collector printed backend data or diagnostics to stdout")
	}
}

func TestCollectorLimitsDeterministicBackendTies(t *testing.T) {
	c := newTestCollector(t, nil)
	for _, backend := range []string{"b", "a"} {
		r := &ActivityRecord{BackendID: backend, Epoch: 1, ActivityID: "1", Timestamp: fixtureTime}
		c.store[activityKey{backendID: backend, epoch: 1, activityID: "1"}] = r
		c.totalSize += estimateActivitySize(r)
	}
	for i := 0; i < 25; i++ {
		rows := c.Snapshot()
		if len(rows) != 2 || rows[0].BackendID != "a" || rows[1].BackendID != "b" {
			t.Fatal("backend tie order was not deterministic")
		}
	}
	stats := NewActivityCollector(nil).Stats()
	if stats["request_id_mapping"] != "unsupported" || stats["upstream_generation"] != "unknown" || stats["epoch_scope"] != "collector" {
		t.Fatal("empty report omitted correlation uncertainty")
	}
}

func TestCollectorNoRawInvalidIDsNotInStats(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, activityPageBody(t, activityEntry(1, "m", 200)))
	}))
	defer server.Close()

	longID := strings.Repeat("invalid-id-", 50)
	c := newTestCollector(t, map[string]config.Backend{
		longID: {URL: "http://example.test", Diagnostics: "llama_swap"},
		"":     {URL: "http://example.test", Diagnostics: "llama_swap"},
		"ok":   {URL: server.URL, Diagnostics: "llama_swap"},
	})
	// Invalid and overlong backend IDs must not be registered in backend state.
	if _, exists := c.backendState[longID]; exists {
		t.Fatal("overlong backend ID must not appear in backend state")
	}
	if _, exists := c.backendState[""]; exists {
		t.Fatal("empty backend ID must not appear in backend state")
	}
	// No requests made for invalid IDs; valid backend is collected.
	if _, err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.Snapshot()) != 1 {
		t.Fatalf("expected 1 record (only valid backend), got %d", len(c.Snapshot()))
	}
	// JSON must not contain the overlong sentinel key.
	encoded, _ := json.Marshal(c.Stats())
	if strings.Contains(string(encoded), longID) {
		t.Fatal("overlong backend ID leaked in stats JSON")
	}
}

func TestCollectorResetKeepsPreservesDisabled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, activityPageBody(t, activityEntry(1, "m", 200)))
	}))
	defer server.Close()

	backends := map[string]config.Backend{
		"off": {URL: server.URL}, // no Diagnostics -> disabled
		"on":  {URL: server.URL, Diagnostics: "llama_swap"},
	}
	c := newTestCollector(t, backends)
	if c.backendState["off"].State != "disabled" || c.backendState["off"].Reason != "not_enabled" {
		t.Fatalf("disabled backend state = %#v", c.backendState["off"])
	}
	if c.backendState["on"].State != "unknown" || c.backendState["on"].Reason != "not_collected" {
		t.Fatalf("enabled backend state = %#v", c.backendState["on"])
	}

	// Refresh the enabled backend.
	if _, err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.backendState["on"].State != "collected" {
		t.Fatalf("enabled backend state after refresh = %#v", c.backendState["on"])
	}

	// ResetEpoch: disabled state preserved, enabled resets to not_collected.
	c.ResetEpoch()
	if c.backendState["off"].State != "disabled" || c.backendState["off"].Reason != "not_enabled" {
		t.Fatalf("disabled backend state not preserved after reset: %#v", c.backendState["off"])
	}
	if c.backendState["on"].State != "unknown" || c.backendState["on"].Reason != "not_collected" {
		t.Fatalf("enabled backend state not reset after reset: %#v", c.backendState["on"])
	}
}

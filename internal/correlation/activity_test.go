package correlation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

var activityFixtureTime = time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC)

func activityDuration(value int) *int { return &value }

type activityMockClock struct {
	now time.Time
}

func (m *activityMockClock) Now() time.Time { return m.now }

func newTestCollector(t *testing.T, backends map[string]config.Backend, opts ...Option) *ActivityCollector {
	t.Helper()
	clock := &activityMockClock{now: activityFixtureTime}
	opts = append(opts, WithNow(func() time.Time { return clock.now }))
	return NewActivityCollector(backends, opts...)
}

// activityEntry builds one upstream activity log entry. Override fields via
// the returned entry directly in tests.
func activityEntry(id int, model string, statusCode int) activityLogEntry {
	return activityLogEntry{
		ID:             id,
		Timestamp:      activityFixtureTime,
		Src:            "ip:127.0.0.1",
		Model:          model,
		Task:           "chat",
		Route:          "local",
		PeerID:         "peer-1",
		ReqPath:        "/v1/chat/completions",
		RespStatusCode: statusCode,
		Tokens: activityTokenMetrics{
			InputTokens:     10,
			OutputTokens:    5,
			CachedTokens:    2,
			ReasoningTokens: 0,
		},
		DurationMs: activityDuration(100),
		Metadata:   map[string]string{"completion_tokens": "5"},
	}
}

func activityPageBody(t *testing.T, entries ...activityLogEntry) string {
	t.Helper()
	page := activityPage{}
	page.Data = entries
	page.Page = 1
	page.Limit = len(entries)
	page.Total = len(entries)
	page.TotalPages = 1
	b, err := json.Marshal(page)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestConstructionMakesNoRequestsAndSkipsDisabledBackends(t *testing.T) {
	var count atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, activityPageBody(t, activityEntry(1, "m", 200)))
	}))
	defer server.Close()

	backends := map[string]config.Backend{
		"off": {URL: server.URL},
		"on":  {URL: server.URL, Diagnostics: "llama_swap"},
	}
	c := newTestCollector(t, backends)
	if count.Load() != 0 {
		t.Fatal("construction made requests")
	}
	if c.backendState["off"].State != "disabled" || c.backendState["off"].Reason != "not_enabled" {
		t.Fatalf("disabled backend state = %#v", c.backendState["off"])
	}

	records, err := c.Refresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record (only enabled backend), got %d", len(records))
	}
	if count.Load() != 1 {
		t.Fatalf("enabled backend request count = %d, want 1", count.Load())
	}
	if c.backendState["on"].State == "disabled" {
		t.Fatal("enabled backend should not be disabled")
	}
	// Snapshot returns the full sorted snapshot.
	snap := c.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("snapshot count = %d, want 1", len(snap))
	}
}

// checkRefreshNormalizesRealSchemaAndResolvesModels verifies the records and
// collector state produced by TestRefreshNormalizesRealSchemaAndResolvesModels.
// Extracted to keep the test function's cognitive complexity below the S3776
// threshold while preserving every assertion.
func checkRefreshNormalizesRealSchemaAndResolvesModels(t *testing.T, records []*ActivityRecord, c *ActivityCollector) {
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	byID := map[string]*ActivityRecord{}
	for _, r := range records {
		byID[r.ActivityID] = r
	}
	known := byID["1"]
	if known == nil || known.ModelID != "known-model" || known.BackendID != "backend" {
		t.Fatalf("known model not resolved: %#v", known)
	}
	if known.ActivityID != "1" || known.Status != "http_success" {
		t.Fatalf("known record fields wrong: %#v", known)
	}
	// Explicit collection facts serialized.
	if known.RequestIDMapping != requestIDMappingUnsupported ||
		known.UpstreamGeneration != upstreamGenerationUnknown ||
		known.EpochScope != epochScopeCollector {
		t.Fatalf("collection facts not serialized: %#v", known)
	}
	unknown := byID["2"]
	if unknown == nil || unknown.ModelID != "unknown" {
		t.Fatalf("unknown model should be unattributed: %#v", unknown)
	}
	// Raw fields must not be copied.
	encoded, _ := json.Marshal(records)
	// No secret or raw upstream field may appear anywhere in the output.
	for _, raw := range []string{"peer-1", "/v1/chat/completions", "ip:127.0.0.1", "completion_tokens"} {
		if strings.Contains(string(encoded), raw) {
			t.Fatalf("raw upstream field leaked: %q", raw)
		}
	}
	if c.Epoch() != 1 {
		t.Fatalf("epoch = %d, want 1", c.Epoch())
	}
}

func TestRefreshNormalizesRealSchemaAndResolvesModels(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/metrics/activity" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		if !strings.Contains(r.URL.RawQuery, "limit=") || !strings.Contains(r.URL.RawQuery, "page=1") {
			t.Errorf("expected bounded limit+page query, got %q", r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, activityPageBody(t,
			activityEntry(1, "known-model", 200),
			activityEntry(2, "unknown-model", 200),
		))
	}))
	defer server.Close()

	c := newTestCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	}, WithKnownModels("known-model"))

	records, err := c.Refresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	checkRefreshNormalizesRealSchemaAndResolvesModels(t, records, c)
}

func TestRawFieldsAndSecretsNotCopied(t *testing.T) {
	shortSecret := "SECRET"
	longSecret := strings.Repeat("SECRET-LONG-", 50)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		entry := activityEntry(1, shortSecret, 200)
		entry.Task = longSecret
		entry.Route = shortSecret
		entry.PeerID = longSecret
		entry.ReqPath = shortSecret
		entry.ErrorMsg = longSecret
		entry.Metadata = map[string]string{
			shortSecret:         longSecret,
			"completion_tokens": "5",
		}
		entry.Src = longSecret
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, activityPageBody(t, entry))
	}))
	defer server.Close()

	c := newTestCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	}, WithKnownModels("known-model"))
	records, err := c.Refresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	r := records[0]
	// Model secret resolved to unknown, never attributed.
	if r.ModelID != "unknown" {
		t.Fatalf("model secret should be unknown: %q", r.ModelID)
	}
	// Task/Route secrets not in allowlist -> omitted.
	if r.Task != "" || r.Route != "" {
		t.Fatalf("task/route secret should be omitted: task=%q route=%q", r.Task, r.Route)
	}
	encoded, _ := json.Marshal(r)
	// No secret (short or long) may appear anywhere in the output.
	for _, secret := range []string{shortSecret, longSecret, "peer-1", "/v1/chat/completions", "completion_tokens"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("secret leaked in output: %q", secret)
		}
	}
	// Dropped fields must not be present in the serialized output.
}

func TestModelAllowlistAndUnknownAttribution(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, activityPageBody(t,
			activityEntry(1, "allowed", 200),
			activityEntry(2, "not-allowed", 200),
			activityEntry(3, "", 200),
		))
	}))
	defer server.Close()

	c := newTestCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	}, WithKnownModels("allowed"))
	records, err := c.Refresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}
	byID := map[string]*ActivityRecord{}
	for _, r := range records {
		byID[r.ActivityID] = r
	}
	if byID["1"].ModelID != "allowed" {
		t.Fatalf("allowed model not resolved: %q", byID["1"].ModelID)
	}
	if byID["2"].ModelID != "unknown" {
		t.Fatalf("unknown model should be unattributed: %q", byID["2"].ModelID)
	}
	if byID["3"].ModelID != "unknown" {
		t.Fatalf("empty model should be unknown: %q", byID["3"].ModelID)
	}
}

func TestIdentityConflictOnConflictingTimestamp(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Same activity ID twice in one page with conflicting timestamps.
		e1 := activityEntry(1, "m", 200)
		e2 := activityEntry(1, "m", 200)
		e2.Timestamp = activityFixtureTime.Add(time.Minute)
		fmt.Fprint(w, activityPageBody(t, e1, e2))
	}))
	defer server.Close()

	c := newTestCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	records, err := c.Refresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record (no merge), got %d", len(records))
	}
	r := records[0]
	if !r.IdentityConflict {
		t.Fatal("identity conflict not detected")
	}
	if r.Timestamp != activityFixtureTime {
		t.Fatalf("first timestamp should win: %v", r.Timestamp)
	}
	// Backend state should reflect partial coverage.
	if c.backendState["backend"].Coverage != "partial" {
		t.Fatalf("coverage = %q, want partial", c.backendState["backend"].Coverage)
	}
}

func TestRepeatedPollsDedupAndEpochResetSeparatesReusedIDs(t *testing.T) {
	var call atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		call.Add(1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, activityPageBody(t, activityEntry(int(call.Load()), "m", 200)))
	}))
	defer server.Close()

	c := newTestCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})

	// First poll: one new record under the stable epoch 1.
	records, err := c.Refresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].ActivityID != "1" {
		t.Fatalf("first poll wrong: %#v", records)
	}
	if c.Epoch() != 1 {
		t.Fatalf("epoch = %d, want 1", c.Epoch())
	}
	if len(c.Snapshot()) != 1 {
		t.Fatalf("store count = %d, want 1", len(c.Snapshot()))
	}

	// Second poll (stable epoch 1): backend returns a new ID. The full
	// snapshot includes the earlier record plus the new one.
	records, err = c.Refresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("second poll wrong: %#v", records)
	}
	if len(c.Snapshot()) != 2 {
		t.Fatalf("store count = %d, want 2", len(c.Snapshot()))
	}

	// Replay the same IDs: idempotent, no growth.
	replayServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, activityPageBody(t, activityEntry(1, "m", 200), activityEntry(2, "m", 200)))
	}))
	defer replayServer.Close()
	c2 := newTestCollector(t, map[string]config.Backend{
		"backend": {URL: replayServer.URL, Diagnostics: "llama_swap"},
	})
	if _, err := c2.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c2.Snapshot()) != 2 {
		t.Fatalf("first replay store count = %d, want 2", len(c2.Snapshot()))
	}
	if _, err := c2.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c2.Snapshot()) != 2 {
		t.Fatalf("second replay store count = %d, want 2 (dedup)", len(c2.Snapshot()))
	}

	// ResetEpoch bumps the epoch and clears records/dedup; reused IDs become
	// re-collectable under the new epoch.
	c2.ResetEpoch()
	if c2.Epoch() != 2 {
		t.Fatalf("epoch after reset = %d, want 2 (bumped)", c2.Epoch())
	}
	if len(c2.Snapshot()) != 0 {
		t.Fatalf("store count after reset = %d, want 0 (cleared)", len(c2.Snapshot()))
	}
	// Re-poll after reset: same IDs stored under the new epoch.
	if _, err := c2.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c2.Epoch() != 2 {
		t.Fatalf("epoch after reset+refresh = %d, want 2", c2.Epoch())
	}
	if len(c2.Snapshot()) != 2 {
		t.Fatalf("store count after reset+refresh = %d, want 2", len(c2.Snapshot()))
	}
}

func TestStableEpochRepeatedRefreshDedups(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, activityPageBody(t, activityEntry(1, "m", 200)))
	}))
	defer server.Close()
	c := newTestCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	// Two Refresh calls with a stable epoch dedup the same activity ID.
	if _, err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.Snapshot()) != 1 {
		t.Fatalf("expected 1 record (dedup within epoch), got %d", len(c.Snapshot()))
	}
	if c.Epoch() != 1 {
		t.Fatalf("epoch = %d, want 1 (stable)", c.Epoch())
	}
}

func TestResetEpochIncrementsAndReusesIDs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, activityPageBody(t, activityEntry(1, "m", 200)))
	}))
	defer server.Close()
	c := newTestCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	if _, err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.Epoch() != 1 {
		t.Fatalf("epoch = %d, want 1", c.Epoch())
	}
	if len(c.Snapshot()) != 1 {
		t.Fatalf("expected 1 record, got %d", len(c.Snapshot()))
	}
	// ResetEpoch bumps the epoch and clears store/dedup.
	c.ResetEpoch()
	if c.Epoch() != 2 {
		t.Fatalf("epoch after reset = %d, want 2 (bumped)", c.Epoch())
	}
	if len(c.Snapshot()) != 0 {
		t.Fatalf("expected empty store after reset, got %d", len(c.Snapshot()))
	}
	// Same ID re-collectable under the new epoch.
	if _, err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.Epoch() != 2 {
		t.Fatalf("epoch after reset+refresh = %d, want 2", c.Epoch())
	}
	if len(c.Snapshot()) != 1 {
		t.Fatalf("expected 1 record after reset+refresh, got %d", len(c.Snapshot()))
	}
}

func TestResetEpochDuringBlockedHTTPDoesNotRestoreOldRecords(t *testing.T) {
	var requestCount atomic.Int32
	started := make(chan struct{}, 1)
	block := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := requestCount.Add(1)
		if n == 1 {
			// First request: respond immediately.
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, activityPageBody(t, activityEntry(1, "m", 200)))
			return
		}
		// Second request: signal in flight, then block.
		started <- struct{}{}
		<-block
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, activityPageBody(t, activityEntry(1, "m", 200)))
	}))
	defer server.Close()

	c := newTestCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	if _, err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.Snapshot()) != 1 {
		t.Fatalf("expected 1 record after first poll, got %d", len(c.Snapshot()))
	}

	// Second Refresh blocks on HTTP while the epoch is bumped.
	done := make(chan error, 1)
	go func() {
		_, err := c.Refresh(context.Background())
		done <- err
	}()
	<-started      // HTTP request in flight, gate held
	c.ResetEpoch() // bump epoch, clear store
	close(block)   // let the in-flight request complete
	err := <-done
	if !errors.Is(err, ErrActivityEpochChanged) {
		t.Fatalf("expected ErrActivityEpochChanged, got %v", err)
	}
	// Old records must not be restored; nothing repopulated under the stale epoch.
	if len(c.Snapshot()) != 0 {
		t.Fatalf("expected no records after epoch change, got %d", len(c.Snapshot()))
	}
}

func TestSameNumericIDAcrossBackendsIsDistinct(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, activityPageBody(t, activityEntry(1, "m", 200)))
	}))
	defer server.Close()

	c := newTestCollector(t, map[string]config.Backend{
		"backend-1": {URL: server.URL, Diagnostics: "llama_swap"},
		"backend-2": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	records, err := c.Refresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records (same ID across backends distinct), got %d", len(records))
	}
	if len(c.Snapshot()) != 2 {
		t.Fatalf("store count = %d, want 2", len(c.Snapshot()))
	}
}

func TestBoundedStoreEviction(t *testing.T) {
	var call atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		call.Add(1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, activityPageBody(t, activityEntry(int(call.Load()), "m", 200)))
	}))
	defer server.Close()

	c := newTestCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	}, WithMaxRecords(3))
	for i := 0; i < 5; i++ {
		if _, err := c.Refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if len(c.Snapshot()) != 3 {
		t.Fatalf("store count after count eviction = %d, want 3 (max_records=3)", len(c.Snapshot()))
	}
	// Oldest evicted: IDs 1 and 2 gone, IDs 3,4,5 remain.
	ids := map[string]bool{}
	for _, r := range c.Snapshot() {
		ids[r.ActivityID] = true
	}
	if ids["1"] || ids["2"] {
		t.Fatal("oldest records should be evicted")
	}
	if !ids["3"] || !ids["4"] || !ids["5"] {
		t.Fatalf("newest records should remain: %#v", ids)
	}
}

func TestAgeRetentionExpiresStaleRecords(t *testing.T) {
	oldTime := activityFixtureTime.Add(-48 * time.Hour)
	recentTime := activityFixtureTime.Add(-1 * time.Hour)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		e := activityEntry(1, "m", 200)
		e.Timestamp = oldTime
		fmt.Fprint(w, activityPageBody(t, e))
	}))
	defer server.Close()

	c := newTestCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	}, WithMaxAge(24*time.Hour))
	if _, err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Old record should be age-evicted on read.
	if len(c.Snapshot()) != 0 {
		t.Fatalf("stale record should be age-evicted, got %d", len(c.Snapshot()))
	}

	// Recent record survives.
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		e := activityEntry(2, "m", 200)
		e.Timestamp = recentTime
		fmt.Fprint(w, activityPageBody(t, e))
	}))
	defer server2.Close()
	c2 := newTestCollector(t, map[string]config.Backend{
		"backend": {URL: server2.URL, Diagnostics: "llama_swap"},
	}, WithMaxAge(24*time.Hour))
	if _, err := c2.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c2.Snapshot()) != 1 {
		t.Fatalf("recent record should survive, got %d", len(c2.Snapshot()))
	}
}

func TestConcurrentRefreshesRaceFreeAndBoundedWorkers(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, activityPageBody(t, activityEntry(1, "m", 200)))
	}))
	defer server.Close()
	c := newTestCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.Refresh(context.Background()); err != nil {
				t.Errorf("concurrent refresh error: %v", err)
			}
		}()
	}
	wg.Wait()
	// Deduped: same activity ID stored once.
	if len(c.Snapshot()) != 1 {
		t.Fatalf("concurrent store count = %d, want 1 (dedup)", len(c.Snapshot()))
	}
}

func TestWaitingRefreshCancelHonored(t *testing.T) {
	started := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		started <- struct{}{}
		// Keep the gate held while the request is in flight.
		time.Sleep(50 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, activityPageBody(t, activityEntry(1, "m", 200)))
	}))
	defer server.Close()
	c := newTestCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	// An in-flight Refresh holds the gate.
	go func() { _, _ = c.Refresh(context.Background()) }()
	<-started // HTTP request in flight, gate held
	// A waiting Refresh is cancelled before it can acquire the gate.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Refresh(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestWithClientRedirectsStillRefused(t *testing.T) {
	var reached atomic.Int32
	tripwire := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached.Add(1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, activityPageBody(t, activityEntry(1, "m", 200)))
	}))
	defer tripwire.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, tripwire.URL+"/api/metrics/activity", http.StatusFound)
	}))
	defer server.Close()
	// A redirect-following client must NOT bypass the collector's policy.
	customClient := &http.Client{}
	c := newTestCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	}, WithClient(customClient))
	records, err := c.Refresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("expected 0 records on redirect, got %d", len(records))
	}
	if c.backendState["backend"].State != "unknown" || c.backendState["backend"].Reason != "redirect_refused" {
		t.Fatalf("redirect not refused: %#v", c.backendState["backend"])
	}
	if reached.Load() != 0 {
		t.Fatal("redirect was followed despite no-redirect policy")
	}
}

func TestMalformedPageRejected(t *testing.T) {
	for _, body := range []string{`{}`, `{"data":null}`, `not json`, `{"data":[]} trailing`} {
		t.Run(fmt.Sprintf("body-%d", len(body)), func(t *testing.T) {
			assertMalformedActivityPage(t, body)
		})
	}
}

func TestInvalidEntriesSkippedAndCoverage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Valid entry, ID<=0 entry, missing-timestamp entry, out-of-range status entry.
		e1 := activityEntry(1, "m", 200)
		e2 := activityEntry(0, "m", 200) // invalid ID
		e3 := activityEntry(3, "m", 200)
		e3.Timestamp = time.Time{}       // missing timestamp
		e4 := activityEntry(4, "m", 600) // out-of-range status
		fmt.Fprint(w, activityPageBody(t, e1, e2, e3, e4))
	}))
	defer server.Close()

	c := newTestCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	records, err := c.Refresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 valid record, got %d", len(records))
	}
	if records[0].ActivityID != "1" {
		t.Fatalf("valid record ID = %q, want 1", records[0].ActivityID)
	}
	if c.backendState["backend"].Coverage != "partial" {
		t.Fatalf("coverage = %q, want partial (invalid entries skipped)", c.backendState["backend"].Coverage)
	}
}

func TestCoverageTruncatedByRowCapAndPageTotal(t *testing.T) {
	// Row cap truncation.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		entries := make([]activityLogEntry, 0, 5)
		for i := 0; i < 5; i++ {
			e := activityEntry(i+1, "m", 200)
			entries = append(entries, e)
		}
		page := activityPage{}
		page.Data = entries
		page.Page = 1
		page.Limit = 5
		page.Total = 5
		page.TotalPages = 1
		b, _ := json.Marshal(page)
		_, _ = w.Write(b)
	}))
	defer server.Close()
	c := newTestCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	}, WithMaxRowsPerBackend(2))
	records, err := c.Refresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records (row cap), got %d", len(records))
	}
	if c.backendState["backend"].Truncated != true || c.backendState["backend"].Coverage != "truncated" {
		t.Fatalf("expected truncated coverage, got %#v", c.backendState["backend"])
	}

	// page.Total > returned rows => truncated.
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		e := activityEntry(1, "m", 200)
		page := activityPage{}
		page.Data = []activityLogEntry{e}
		page.Page = 1
		page.Limit = 1
		page.Total = 10
		page.TotalPages = 10
		b, _ := json.Marshal(page)
		_, _ = w.Write(b)
	}))
	defer server2.Close()
	c2 := newTestCollector(t, map[string]config.Backend{
		"backend": {URL: server2.URL, Diagnostics: "llama_swap"},
	})
	if _, err := c2.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c2.backendState["backend"].Truncated != true || c2.backendState["backend"].Coverage != "truncated" {
		t.Fatalf("expected truncated coverage from page.total, got %#v", c2.backendState["backend"])
	}
}

func TestSecretsShortAndLongInEveryRawTextField(t *testing.T) {
	short := "SHORT-SECRET"
	long := strings.Repeat("LONG-SECRET-", 100)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		entry := activityEntry(1, short, 200)
		entry.Task = long
		entry.Route = short
		entry.PeerID = long
		entry.ReqPath = short
		entry.ErrorMsg = long
		entry.Metadata = map[string]string{
			short:               long,
			"completion_tokens": "5",
		}
		entry.Src = long
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, activityPageBody(t, entry))
	}))
	defer server.Close()

	c := newTestCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	}, WithKnownModels("known-model"))
	records, err := c.Refresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	encoded, _ := json.Marshal(records)
	// Every secret (short and long) must be absent from output.
	for _, secret := range []string{short, long, "peer-1", "/v1/chat/completions", "ip:127.0.0.1", "completion_tokens"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("secret leaked in output: %q", secret)
		}
	}
	// Model secret -> unknown.
	if records[0].ModelID != "unknown" {
		t.Fatalf("model secret should be unknown: %q", records[0].ModelID)
	}
}

func TestOutageRetainsAgedSnapshot(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, activityPageBody(t, activityEntry(1, "m", 200)))
	}))
	defer server.Close()
	c := newTestCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	if _, err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.Snapshot()) != 1 {
		t.Fatalf("expected 1 record after first poll, got %d", len(c.Snapshot()))
	}
	// Backend goes outage (server closed).
	server.Close()
	// Second poll: backend unavailable, no new records, existing record not
	// refreshed (retains original timestamp).
	records, err := c.Refresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// Full snapshot semantics: existing record retained, not refreshed.
	if len(records) != 1 {
		t.Fatalf("expected 1 record after outage, got %d", len(records))
	}
	if records[0].Timestamp != activityFixtureTime {
		t.Fatalf("record timestamp should not be refreshed on outage: %v", records[0].Timestamp)
	}
	if c.backendState["backend"].State != "unavailable" {
		t.Fatalf("expected unavailable state on outage, got %#v", c.backendState["backend"])
	}
}

func TestStatsReturnsDefensiveCopies(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, activityPageBody(t, activityEntry(1, "m", 200)))
	}))
	defer server.Close()
	c := newTestCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	if _, err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	stats := c.Stats()
	if stats["record_count"] != 1 {
		t.Fatalf("record_count = %v, want 1", stats["record_count"])
	}
	if stats["epoch"] != int64(1) {
		t.Fatalf("epoch = %v, want 1", stats["epoch"])
	}
	states := stats["backend_states"].(map[string]BackendState)
	if states["backend"].State != "collected" || states["backend"].Rows != 1 {
		t.Fatalf("backend state wrong: %#v", states["backend"])
	}
	// Mutating returned records must not affect internal state.
	records := stats["records"].([]*ActivityRecord)
	originalModelID := c.Snapshot()[0].ModelID
	if len(records) == 1 {
		records[0].ModelID = "mutated"
		records[0].IdentityConflict = true
	}
	if c.Snapshot()[0].ModelID != originalModelID || c.Snapshot()[0].IdentityConflict {
		t.Fatal("stats returned aliased records")
	}
}

func TestInvalidBackendIDNotSerialized(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, activityPageBody(t, activityEntry(1, "m", 200)))
	}))
	defer server.Close()
	longID := strings.Repeat("invalid-id-", 50)
	c := newTestCollector(t, map[string]config.Backend{
		longID: {URL: "http://example.test", Diagnostics: "llama_swap"},
		"ok":   {URL: server.URL, Diagnostics: "llama_swap"},
	})
	// Invalid (and overlong) backend IDs must not be registered in backend state.
	if _, exists := c.backendState[longID]; exists {
		t.Fatal("invalid backend ID must not appear in backend state")
	}
	if _, exists := c.backendState[""]; exists {
		t.Fatal("empty backend ID must not appear in backend state")
	}
	// No requests made for invalid ID; valid backend is collected.
	_, err := c.Refresh(context.Background())
	if err != nil {
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

func TestOverlappingRefreshesReturnDefensiveCopies(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, activityPageBody(t, activityEntry(1, "m", 200)))
	}))
	defer server.Close()
	c := newTestCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	done := make(chan []*ActivityRecord, 2)
	go func() { r, _ := c.Refresh(context.Background()); done <- r }()
	go func() { r, _ := c.Refresh(context.Background()); done <- r }()
	r1 := <-done
	r2 := <-done
	if len(r1) != 1 || len(r2) != 1 {
		t.Fatalf("expected 1 record each, got %d and %d", len(r1), len(r2))
	}
	// Mutating one copy must not affect the other.
	r1[0].ModelID = "mutated"
	r1[0].IdentityConflict = true
	if r2[0].ModelID == "mutated" || r2[0].IdentityConflict {
		t.Fatal("refreshes returned aliased records")
	}
}

func TestRefreshBoundsConcurrentHTTPRequests(t *testing.T) {
	var active, maxActive atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := active.Add(1)
		defer active.Add(-1)
		for old := maxActive.Load(); n > old; old = maxActive.Load() {
			if maxActive.CompareAndSwap(old, n) {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, activityPageBody(t, activityEntry(1, "m", 200)))
	}))
	defer server.Close()
	backends := make(map[string]config.Backend)
	for i := 8; i > 0; i-- {
		backends[fmt.Sprintf("backend-%d", i)] = config.Backend{URL: server.URL, Diagnostics: "llama_swap"}
	}
	c := newTestCollector(t, backends)
	if _, err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if maxActive.Load() > 4 {
		t.Fatalf("maxActive concurrent HTTP = %d, want <= 4", maxActive.Load())
	}
}

func TestReportedModelRefIsOpaqueHash(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, activityPageBody(t,
			activityEntry(1, "known-model", 200),
			activityEntry(2, "unknown-model", 200),
		))
	}))
	defer server.Close()

	c := newTestCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	}, WithKnownModels("known-model"))
	records, err := c.Refresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	byID := map[string]*ActivityRecord{}
	for _, r := range records {
		byID[r.ActivityID] = r
	}
	known := byID["1"]
	knownRef, ok := ModelReference("backend", "known-model")
	if !ok {
		t.Fatal("ModelReference must succeed for valid model label")
	}
	if known.ReportedModelRef == "" {
		t.Fatal("ReportedModelRef must be set for a valid model label")
	}
	if known.ReportedModelRef != knownRef {
		t.Fatalf("ReportedModelRef = %q, want hash of backend:known-model", known.ReportedModelRef)
	}
	// The hash must not leak the raw model label.
	if strings.Contains(known.ReportedModelRef, "known-model") {
		t.Fatal("ReportedModelRef must not contain the raw model label")
	}
	// ModelID is retained for allowlisted models.
	if known.ModelID != "known-model" {
		t.Fatalf("ModelID = %q, want known-model", known.ModelID)
	}
	unknown := byID["2"]
	unknownRef, ok := ModelReference("backend", "unknown-model")
	if !ok {
		t.Fatal("ModelReference must succeed for valid model label")
	}
	if unknown.ReportedModelRef == "" {
		t.Fatal("ReportedModelRef must be set even for unknown models")
	}
	if unknown.ReportedModelRef != unknownRef {
		t.Fatalf("ReportedModelRef = %q, want hash of backend:unknown-model", unknown.ReportedModelRef)
	}
	// ModelID remains unknown for non-allowlisted models.
	if unknown.ModelID != "unknown" {
		t.Fatalf("ModelID = %q, want unknown", unknown.ModelID)
	}
	// ModelID is intentionally serialized as the allowlisted identity (see
	// the proxy diagnostics contract), so the serialized output may contain
	// the allowlisted model name. The opacity guarantee is that
	// ReportedModelRef is a hash and never contains the raw label, which is
	// verified above.
}

func TestReportedModelRefRejectsOverlongModelLabel(t *testing.T) {
	longModel := strings.Repeat("model-label-", 100) // well over 256 bytes
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		e := activityEntry(1, longModel, 200)
		fmt.Fprint(w, activityPageBody(t, e))
	}))
	defer server.Close()

	c := newTestCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	records, err := c.Refresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	// Overlong model label: no reported model ref, ModelID remains unknown.
	if records[0].ReportedModelRef != "" {
		t.Fatal("overlong model label must not produce a reported model ref")
	}
	if records[0].ModelID != "unknown" {
		t.Fatal("overlong model label must be unknown")
	}
	encoded, _ := json.Marshal(records)
	if strings.Contains(string(encoded), longModel) {
		t.Fatal("overlong model label leaked in output")
	}
}

func TestMetricFieldsPreservedPositiveZeroSentinelMissing(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Positive, explicit zero, -1 sentinel, and missing (default 0) values.
		e := activityLogEntry{
			ID:             1,
			Timestamp:      activityFixtureTime,
			Src:            "ip:127.0.0.1",
			Model:          "m",
			Task:           "chat",
			Route:          "local",
			PeerID:         "peer-1",
			ReqPath:        "/v1/chat/completions",
			RespStatusCode: 200,
			Tokens: activityTokenMetrics{
				InputTokens:     10,  // positive
				OutputTokens:    0,   // ambiguous default/presence in the source API
				CachedTokens:    -1,  // sentinel -> nil
				DraftTokens:     5,   // positive
				DraftAccTokens:  0,   // explicit zero
				ReasoningTokens: -1,  // sentinel -> nil
				PromptPerSecond: 2.5, // positive
				TokensPerSecond: -1,  // sentinel -> nil
			},
			DurationMs: activityDuration(100), // positive
		}
		fmt.Fprint(w, activityPageBody(t, e))
	}))
	defer server.Close()

	c := newTestCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	records, err := c.Refresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	r := records[0]

	// Positive values preserved as pointers.
	if r.InputTokens == nil || *r.InputTokens != 10 {
		t.Fatalf("InputTokens = %#v, want pointer(10)", r.InputTokens)
	}
	if r.OutputTokens != nil {
		t.Fatalf("OutputTokens = %#v, zero without a source presence marker must remain unknown", r.OutputTokens)
	}
	if r.DraftTokens == nil || *r.DraftTokens != 5 {
		t.Fatalf("DraftTokens = %#v, want pointer(5)", r.DraftTokens)
	}
	if r.DurationMS == nil || *r.DurationMS != 100 {
		t.Fatalf("DurationMS = %#v, want pointer(100)", r.DurationMS)
	}
	if r.PromptTokensPerSecond == nil || *r.PromptTokensPerSecond != 2.5 {
		t.Fatalf("PromptTokensPerSecond = %#v, want pointer(2.5)", r.PromptTokensPerSecond)
	}

	// Sentinel -1 -> nil (missing), never zero or rejection.
	if r.CachedTokens != nil {
		t.Fatalf("CachedTokens should be nil (sentinel -1), got %#v", r.CachedTokens)
	}
	if r.ReasoningTokens != nil {
		t.Fatalf("ReasoningTokens should be nil (sentinel -1), got %#v", r.ReasoningTokens)
	}
	if r.TokensPerSecond != nil {
		t.Fatalf("TokensPerSecond should be nil (sentinel -1), got %#v", r.TokensPerSecond)
	}

	// Explicit zero is a distinct pointer to zero, not nil.
	if r.DraftAcceptedTokens == nil || *r.DraftAcceptedTokens != 0 {
		t.Fatalf("DraftAcceptedTokens = %#v, want pointer(0)", r.DraftAcceptedTokens)
	}
}

func TestOmittedDurationDiffersFromExplicitZero(t *testing.T) {
	c := newTestCollector(t, nil)
	missing := activityEntry(1, "model", http.StatusOK)
	missing.DurationMs = nil
	gotMissing, _ := c.normalizeActivity("backend", missing)
	if gotMissing.DurationMS != nil {
		t.Fatal("omitted duration was fabricated as zero")
	}
	explicitZero := activityEntry(2, "model", http.StatusOK)
	explicitZero.DurationMs = activityDuration(0)
	gotZero, _ := c.normalizeActivity("backend", explicitZero)
	if gotZero.DurationMS == nil || *gotZero.DurationMS != 0 {
		t.Fatal("explicit zero duration was lost")
	}
}

func TestUnreportedUsageDefaultZerosRemainUnknown(t *testing.T) {
	c := newTestCollector(t, nil)
	record, ok := c.normalizeActivity("backend", activityLogEntry{
		ID: 1, Timestamp: activityFixtureTime, RespStatusCode: http.StatusOK,
		DurationMs: activityDuration(12),
		Tokens:     activityTokenMetrics{ReasoningTokens: -1},
	})
	if !ok {
		t.Fatal("valid activity row rejected")
	}
	if record.DurationMS == nil || *record.DurationMS != 12 {
		t.Fatal("duration should remain independently available")
	}
	for name, value := range map[string]*int64{
		"input": record.InputTokens, "output": record.OutputTokens, "cache": record.CachedTokens,
		"draft": record.DraftTokens, "draft accepted": record.DraftAcceptedTokens, "reasoning": record.ReasoningTokens,
	} {
		if value != nil {
			t.Fatalf("unreported %s default zero fabricated as %d", name, *value)
		}
	}
	if record.PromptTokensPerSecond != nil || record.TokensPerSecond != nil {
		t.Fatal("unreported rate default zero fabricated")
	}
}

func TestReportedAllZeroUsageCanPreserveSentinelMetrics(t *testing.T) {
	c := newTestCollector(t, nil)
	record, ok := c.normalizeActivity("backend", activityLogEntry{
		ID: 1, Timestamp: activityFixtureTime, RespStatusCode: http.StatusOK,
		Tokens: activityTokenMetrics{ReasoningTokens: 0}, // differs from newEntry's -1 default
	})
	if !ok {
		t.Fatal("valid activity row rejected")
	}
	if record.CachedTokens == nil || *record.CachedTokens != 0 || record.DraftTokens == nil || *record.DraftTokens != 0 ||
		record.DraftAcceptedTokens == nil || *record.DraftAcceptedTokens != 0 || record.ReasoningTokens == nil || *record.ReasoningTokens != 0 {
		t.Fatal("explicit zero optional metrics were lost after a reported-usage sentinel")
	}
	if record.InputTokens != nil || record.OutputTokens != nil {
		t.Fatal("ambiguous input/output zero was reported")
	}
}

func TestCloneDeepCopiesMetricPointers(t *testing.T) {
	c := newTestCollector(t, nil)
	r, _ := c.normalizeActivity("backend", activityLogEntry{
		ID:             1,
		Timestamp:      activityFixtureTime,
		Src:            "ip:127.0.0.1",
		Model:          "m",
		Task:           "chat",
		Route:          "local",
		PeerID:         "peer-1",
		ReqPath:        "/v1/chat/completions",
		RespStatusCode: 200,
		Tokens: activityTokenMetrics{
			InputTokens:     10,
			OutputTokens:    5,
			CachedTokens:    -1, // nil
			DraftTokens:     3,
			DraftAccTokens:  0,
			ReasoningTokens: -1, // nil
			PromptPerSecond: 2.5,
			TokensPerSecond: -1, // nil
		},
		DurationMs: activityDuration(100),
	})
	if r == nil {
		t.Fatal("expected valid record")
	}

	clone := r.Clone()

	// Mutate clone's non-nil pointers.
	*clone.InputTokens = 999
	*clone.PromptTokensPerSecond = 999.0
	*clone.DurationMS = 999

	// Original must be unchanged.
	if *r.InputTokens != 10 {
		t.Fatal("original InputTokens was mutated by clone")
	}
	if *r.PromptTokensPerSecond != 2.5 {
		t.Fatal("original PromptTokensPerSecond was mutated by clone")
	}
	if *r.DurationMS != 100 {
		t.Fatal("original DurationMS was mutated by clone")
	}

	// Nil pointers must stay nil in clone.
	if clone.CachedTokens != nil {
		t.Fatal("clone CachedTokens should be nil (original was nil)")
	}
	if clone.ReasoningTokens != nil {
		t.Fatal("clone ReasoningTokens should be nil (original was nil)")
	}
	if clone.TokensPerSecond != nil {
		t.Fatal("clone TokensPerSecond should be nil (original was nil)")
	}

	// Clone's non-nil pointers must be independent.
	if clone.InputTokens == nil || *clone.InputTokens != 999 {
		t.Fatal("clone InputTokens should be independently mutable")
	}
}

func TestRecordJSONIncludesMetricsOmitsNil(t *testing.T) {
	c := newTestCollector(t, nil)
	r, _ := c.normalizeActivity("backend", activityLogEntry{
		ID:             1,
		Timestamp:      activityFixtureTime,
		Src:            "ip:127.0.0.1",
		Model:          "m",
		Task:           "chat",
		Route:          "local",
		PeerID:         "peer-1",
		ReqPath:        "/v1/chat/completions",
		RespStatusCode: 200,
		Tokens: activityTokenMetrics{
			InputTokens:     10,
			OutputTokens:    5,
			CachedTokens:    -1, // nil
			DraftTokens:     3,
			DraftAccTokens:  0,  // explicit zero -> pointer(0)
			ReasoningTokens: -1, // nil
			PromptPerSecond: 2.5,
			TokensPerSecond: -1, // nil
		},
		DurationMs: activityDuration(100),
	})

	encoded, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	encodedStr := string(encoded)

	// Non-nil metrics appear in JSON.
	if !strings.Contains(encodedStr, `"input_tokens":10`) {
		t.Fatalf("JSON missing input_tokens: %s", encodedStr)
	}
	if !strings.Contains(encodedStr, `"output_tokens":5`) {
		t.Fatalf("JSON missing output_tokens: %s", encodedStr)
	}
	if !strings.Contains(encodedStr, `"duration_ms":100`) {
		t.Fatalf("JSON missing duration_ms: %s", encodedStr)
	}
	if !strings.Contains(encodedStr, `"prompt_per_second":2.5`) {
		t.Fatalf("JSON missing prompt_per_second: %s", encodedStr)
	}

	// Nil metrics (sentinel) are omitted by omitempty.
	if strings.Contains(encodedStr, `"cache_tokens"`) {
		t.Fatalf("JSON should not contain cache_tokens (nil): %s", encodedStr)
	}
	if strings.Contains(encodedStr, `"reasoning_tokens"`) {
		t.Fatalf("JSON should not contain reasoning_tokens (nil): %s", encodedStr)
	}
	if strings.Contains(encodedStr, `"tokens_per_second"`) {
		t.Fatalf("JSON should not contain tokens_per_second (nil): %s", encodedStr)
	}

	// Explicit zero is a distinct pointer, appears as 0 (not omitted).
	if !strings.Contains(encodedStr, `"draft_acc_tokens":0`) {
		t.Fatalf("JSON should contain draft_acc_tokens:0 (explicit zero): %s", encodedStr)
	}
}

// malformedMetricsCase describes one normalizeActivity malformed-metrics case.
// The tokens/duration fields vary per case; every other entry field uses the
// standard boilerplate built by malformedEntry.
type malformedMetricsCase struct {
	name        string
	tokens      activityTokenMetrics
	durationMs  *int
	expectValid bool
	// nilMsg is the exact fatal message when the row is unexpectedly rejected.
	nilMsg string
	// okMsg is the exact fatal message when metricsValid does not match.
	okMsg string
	// fieldChecks validates the normalized record for this case.
	fieldChecks func(t *testing.T, r *ActivityRecord)
}

// malformedEntry builds an activityLogEntry with the standard boilerplate
// fields shared by every malformed-metrics case.
func malformedEntry(tokens activityTokenMetrics, durationMs *int) activityLogEntry {
	return activityLogEntry{
		ID:             1,
		Timestamp:      activityFixtureTime,
		Src:            "ip:127.0.0.1",
		Model:          "m",
		Task:           "chat",
		Route:          "local",
		PeerID:         "peer-1",
		ReqPath:        "/v1/chat/completions",
		RespStatusCode: 200,
		Tokens:         tokens,
		DurationMs:     durationMs,
	}
}

// checkMalformedMetricsCase validates one normalizeActivity malformed-metrics case.
// It asserts the record is non-nil, the validity flag matches expectValid, and
// then runs the provided field checks. The nilMsg and okMsg preserve the exact
// fatal messages from the original test.
func checkMalformedMetricsCase(t *testing.T, c *ActivityCollector, entry activityLogEntry, expectValid bool, nilMsg, okMsg string, fieldChecks func(t *testing.T, r *ActivityRecord)) {
	r, ok := c.normalizeActivity("backend", entry)
	if r == nil {
		t.Fatal(nilMsg)
	}
	if ok != expectValid {
		t.Fatal(okMsg)
	}
	fieldChecks(t, r)
}

// checkNegativeNonSentinel validates the negative non-sentinel malformed case.
func checkNegativeNonSentinel(t *testing.T, r *ActivityRecord) {
	if r.DurationMS != nil {
		t.Fatal("DurationMS should be nil (negative non-sentinel)")
	}
	if r.InputTokens != nil {
		t.Fatal("InputTokens should be nil (negative non-sentinel)")
	}
	if r.OutputTokens == nil || *r.OutputTokens != 5 {
		t.Fatalf("OutputTokens = %#v, want pointer(5)", r.OutputTokens)
	}
}

// checkNaNInf validates the NaN/Inf malformed case.
func checkNaNInf(t *testing.T, r *ActivityRecord) {
	if r.PromptTokensPerSecond != nil {
		t.Fatal("PromptTokensPerSecond should be nil (NaN)")
	}
	if r.TokensPerSecond != nil {
		t.Fatal("TokensPerSecond should be nil (Inf)")
	}
}

// checkRateAboveBound validates the rate-above-bound malformed case.
func checkRateAboveBound(t *testing.T, r *ActivityRecord) {
	if r.PromptTokensPerSecond != nil {
		t.Fatal("PromptTokensPerSecond should be nil (rate above bound)")
	}
}

// checkMaxInt64 validates the MaxInt64 valid case.
func checkMaxInt64(t *testing.T, r *ActivityRecord) {
	if r.InputTokens == nil || *r.InputTokens != math.MaxInt64 {
		t.Fatalf("InputTokens = %#v, want pointer(%d)", r.InputTokens, math.MaxInt64)
	}
	if r.DurationMS == nil || *r.DurationMS != math.MaxInt64 {
		t.Fatalf("DurationMS = %#v, want pointer(%d)", r.DurationMS, math.MaxInt64)
	}
}

// checkRateAtBound validates the rate-at-bound valid case (no field assertions).
func checkRateAtBound(t *testing.T, r *ActivityRecord) {}

func TestMalformedMetricsDirect(t *testing.T) {
	c := newTestCollector(t, nil)

	cases := []malformedMetricsCase{
		{
			name: "negative non-sentinel metrics are malformed",
			tokens: activityTokenMetrics{
				InputTokens:     -5,
				OutputTokens:    5,
				CachedTokens:    2,
				DraftTokens:     3,
				DraftAccTokens:  1,
				ReasoningTokens: 0,
				PromptPerSecond: 1.5,
				TokensPerSecond: 2.5,
			},
			durationMs:  activityDuration(-10),
			expectValid: false,
			nilMsg:      "expected valid record (entry not rejected for bad metrics)",
			okMsg:       "expected metricsValid=false for negative non-sentinel metrics",
			fieldChecks: checkNegativeNonSentinel,
		},
		{
			name: "NaN/Inf metrics are malformed",
			tokens: activityTokenMetrics{
				InputTokens:     5,
				OutputTokens:    5,
				CachedTokens:    2,
				DraftTokens:     3,
				DraftAccTokens:  1,
				ReasoningTokens: 0,
				PromptPerSecond: math.NaN(),
				TokensPerSecond: math.Inf(1),
			},
			durationMs:  activityDuration(100),
			expectValid: false,
			nilMsg:      "expected valid record (entry not rejected for bad metrics)",
			okMsg:       "expected metricsValid=false for NaN/Inf metrics",
			fieldChecks: checkNaNInf,
		},
		{
			name: "rate above bound is malformed",
			tokens: activityTokenMetrics{
				InputTokens:     5,
				OutputTokens:    5,
				CachedTokens:    2,
				DraftTokens:     3,
				DraftAccTokens:  1,
				ReasoningTokens: 0,
				PromptPerSecond: 1e9 + 1,
				TokensPerSecond: 2.5,
			},
			durationMs:  activityDuration(100),
			expectValid: false,
			nilMsg:      "expected valid record (entry not rejected for bad metrics)",
			okMsg:       "expected metricsValid=false for rate above bound",
			fieldChecks: checkRateAboveBound,
		},
		{
			name: "MaxInt64 values are valid",
			tokens: activityTokenMetrics{
				InputTokens:     math.MaxInt64,
				OutputTokens:    5,
				CachedTokens:    2,
				DraftTokens:     3,
				DraftAccTokens:  1,
				ReasoningTokens: 0,
				PromptPerSecond: 1.5,
				TokensPerSecond: 2.5,
			},
			durationMs:  activityDuration(math.MaxInt),
			expectValid: true,
			nilMsg:      "expected valid record with MaxInt64 values",
			okMsg:       "expected metricsValid=true for MaxInt64 values",
			fieldChecks: checkMaxInt64,
		},
		{
			name: "rate at bound is valid",
			tokens: activityTokenMetrics{
				InputTokens:     5,
				OutputTokens:    5,
				CachedTokens:    2,
				DraftTokens:     3,
				DraftAccTokens:  1,
				ReasoningTokens: 0,
				PromptPerSecond: 1e9,
				TokensPerSecond: 1e9,
			},
			durationMs:  activityDuration(100),
			expectValid: true,
			nilMsg:      "expected valid record with rate at bound",
			okMsg:       "expected metricsValid=true for rate at bound",
			fieldChecks: checkRateAtBound,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			entry := malformedEntry(tc.tokens, tc.durationMs)
			checkMalformedMetricsCase(t, c, entry, tc.expectValid, tc.nilMsg, tc.okMsg, tc.fieldChecks)
		})
	}
}

func TestMalformedMetricsWithCoverage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Mix of valid and malformed metrics.
		e := activityLogEntry{
			ID:             1,
			Timestamp:      activityFixtureTime,
			Src:            "ip:127.0.0.1",
			Model:          "m",
			Task:           "chat",
			Route:          "local",
			PeerID:         "peer-1",
			ReqPath:        "/v1/chat/completions",
			RespStatusCode: 200,
			Tokens: activityTokenMetrics{
				InputTokens:     -5,   // malformed (negative non-sentinel)
				OutputTokens:    5,    // valid
				CachedTokens:    2,    // valid
				DraftTokens:     3,    // valid
				DraftAccTokens:  1,    // valid
				ReasoningTokens: 0,    // valid
				PromptPerSecond: -1.5, // malformed (negative non-sentinel)
				TokensPerSecond: 2.5,  // valid
			},
			DurationMs: activityDuration(100), // valid
		}
		fmt.Fprint(w, activityPageBody(t, e))
	}))
	defer server.Close()

	c := newTestCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	records, err := c.Refresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record (entry not rejected for bad metrics), got %d", len(records))
	}
	r := records[0]

	// Malformed metrics are nil.
	if r.InputTokens != nil {
		t.Fatal("InputTokens should be nil (malformed)")
	}
	if r.PromptTokensPerSecond != nil {
		t.Fatal("PromptTokensPerSecond should be nil (malformed)")
	}
	// Valid metrics preserved.
	if r.OutputTokens == nil || *r.OutputTokens != 5 {
		t.Fatalf("OutputTokens = %#v, want pointer(5)", r.OutputTokens)
	}
	if r.DurationMS == nil || *r.DurationMS != 100 {
		t.Fatalf("DurationMS = %#v, want pointer(100)", r.DurationMS)
	}

	// Coverage should be partial due to malformed metrics.
	if c.backendState["backend"].Coverage != "partial" {
		t.Fatalf("coverage = %q, want partial (malformed metrics)", c.backendState["backend"].Coverage)
	}
}

func TestSizeTrackingWithMetrics(t *testing.T) {
	// A record with non-nil metric pointers is larger than one with nil pointers.
	var ten = int64(10)
	r1 := &ActivityRecord{
		BackendID:  "b",
		Epoch:      1,
		ActivityID: "1",
		Timestamp:  fixtureTime,
	}
	r2 := &ActivityRecord{
		BackendID:   "b",
		Epoch:       1,
		ActivityID:  "1",
		Timestamp:   fixtureTime,
		InputTokens: &ten,
	}
	size1 := estimateActivitySize(r1)
	size2 := estimateActivitySize(r2)
	if size2 <= size1 {
		t.Fatalf("record with metric pointer should be larger: %d vs %d", size1, size2)
	}

	// All nine non-nil pointers add 9*8 for the values (pointers already
	// counted in size1).
	r3 := &ActivityRecord{
		BackendID:  "b",
		Epoch:      1,
		ActivityID: "1",
		Timestamp:  fixtureTime,
	}
	var v = int64(1)
	r3.DurationMS = &v
	r3.InputTokens = &v
	r3.OutputTokens = &v
	r3.CachedTokens = &v
	r3.DraftTokens = &v
	r3.DraftAcceptedTokens = &v
	r3.ReasoningTokens = &v
	var fv = float64(1)
	r3.PromptTokensPerSecond = &fv
	r3.TokensPerSecond = &fv
	size3 := estimateActivitySize(r3)
	expectedSize3 := size1 + 9*8 // 9 non-nil values
	if size3 != expectedSize3 {
		t.Fatalf("estimateActivitySize = %d, want %d (9 non-nil pointers)", size3, expectedSize3)
	}

	// End-to-end: totalSize tracking with metrics.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		e := activityLogEntry{
			ID:             1,
			Timestamp:      activityFixtureTime,
			Src:            "ip:127.0.0.1",
			Model:          "m",
			Task:           "chat",
			Route:          "local",
			PeerID:         "peer-1",
			ReqPath:        "/v1/chat/completions",
			RespStatusCode: 200,
			Tokens: activityTokenMetrics{
				InputTokens:     10,
				OutputTokens:    5,
				CachedTokens:    2,
				DraftTokens:     3,
				DraftAccTokens:  1,
				ReasoningTokens: 0,
				PromptPerSecond: 2.5,
				TokensPerSecond: 1.5,
			},
			DurationMs: activityDuration(100),
		}
		fmt.Fprint(w, activityPageBody(t, e))
	}))
	defer server.Close()

	c := newTestCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	}, WithMaxSize(1000000))

	if _, err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.Snapshot()) != 1 {
		t.Fatalf("expected 1 record, got %d", len(c.Snapshot()))
	}

	// totalSize should match estimateActivitySize for the stored record.
	stats := c.Stats()
	totalSize := stats["total_size"].(int64)
	stored := c.Snapshot()[0]
	expected := estimateActivitySize(stored)
	if totalSize != expected {
		t.Fatalf("total_size = %d, want %d (estimateActivitySize)", totalSize, expected)
	}
	if totalSize <= 0 {
		t.Fatalf("total_size = %d, want positive", totalSize)
	}
}

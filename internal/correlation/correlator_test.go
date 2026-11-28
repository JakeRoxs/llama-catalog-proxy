package correlation

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// fixtureTime is the deterministic "now" injected into the correlator clock so
// tests are not coupled to the real wall clock and do not age-evict their
// fixed-date fixtures as time passes.
var fixtureTime = time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC)

// newTestCorrelator returns a Correlator with production-default retention and
// a deterministic clock fixed at fixtureTime.
func newTestCorrelator(t *testing.T) *Correlator {
	t.Helper()
	c := New()
	c.SetClock(&mockClock{now: fixtureTime})
	return c
}

// newTestCorrelatorWithRetention returns a Correlator with the given retention
// policies and a deterministic clock fixed at fixtureTime.
func newTestCorrelatorWithRetention(t *testing.T, count int, age time.Duration, size int64) *Correlator {
	t.Helper()
	c := NewWithRetention(count, age, size)
	c.SetClock(&mockClock{now: fixtureTime})
	return c
}

type mockClock struct {
	now time.Time
}

func (m *mockClock) Now() time.Time { return m.now }

func assertSizeConsistent(t *testing.T, c *Correlator) {
	t.Helper()
	stats := c.Stats()
	totalSize := stats["total_size"].(int64)
	var expected int64
	for _, r := range c.records {
		expected += estimateRecordSize(r)
	}
	if totalSize != expected {
		t.Fatalf("total size = %d, expected %d (sum of record sizes)", totalSize, expected)
	}
}

// checkReaderResult validates a single concurrent reader's Query result and
// returns an error describing the first problem found, or nil when the result
// is acceptable. Extracting this pure check out of runConcurrentReaders
// flattens the goroutine's control flow, reducing its cognitive complexity.
func checkReaderResult(result []*CorrelationRecord, expectCount, maxRecords int) error {
	if expectCount >= 0 {
		if len(result) != expectCount {
			return errors.New("unexpected result count")
		}
	} else if len(result) > maxRecords {
		return errors.New("unexpected result count")
	}
	for _, r := range result {
		if r.RequestID == "" {
			return errors.New("empty request ID")
		}
	}
	return nil
}

// runConcurrentReaders launches n goroutines that each query the correlator
// and validate the returned records, sending any validation errors to the
// returned channel. The channel is buffered with capacity n so readers never
// block on sending. Callers must drain exactly n values from the channel.
//
// maxRecords is the per-reader expected result bound:
//   - expectCount >= 0: each reader asserts len(result) == expectCount exactly
//     (pre-registered, query-only workloads).
//   - expectCount < 0: each reader asserts len(result) <= maxRecords (upper
//     bound, appropriate when reads overlap with in-flight writes).
func runConcurrentReaders(t *testing.T, c *Correlator, n, maxRecords, expectCount int) <-chan error {
	t.Helper()
	done := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			result := c.Query(QueryFilter{})
			done <- checkReaderResult(result, expectCount, maxRecords)
		}()
	}
	return done
}

func TestCorrelatorBasicRegistrationAndCorrelation(t *testing.T) {
	c := newTestCorrelator(t)

	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
	c.CorrelateActivity("act-1", "req-1", fixtureTime.Add(time.Second), map[string]any{
		"completion_tokens": 5,
		"finish_reason":     "stop",
	})

	result := c.Query(QueryFilter{RequestID: "req-1"})
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	r := result[0]
	if r.RequestID != "req-1" || r.BackendID != "backend-A" || r.ModelID != "model-1" {
		t.Fatalf("record fields incorrect: %#v", r)
	}
	if r.ActivityID != "act-1" {
		t.Fatalf("activity ID = %q, want act-1", r.ActivityID)
	}
	if r.Metadata == nil || r.Metadata["completion_tokens"] != 5 || r.Metadata["finish_reason"] != "stop" {
		t.Fatalf("metadata incorrect: %#v", r.Metadata)
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorActivityWithoutRequest(t *testing.T) {
	c := newTestCorrelator(t)

	c.CorrelateActivity("act-orphan", "req-orphan", fixtureTime, nil)

	result := c.Query(QueryFilter{RequestID: "req-orphan"})
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	r := result[0]
	if r.ActivityID != "act-orphan" {
		t.Fatalf("activity ID = %q, want act-orphan", r.ActivityID)
	}
	if r.Status != "unmatched" {
		t.Fatalf("status = %q, want unmatched", r.Status)
	}
	if r.RequestTime.IsZero() {
		t.Fatal("orphan RequestTime must not be zero")
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorMultipleRequestsSameBackend(t *testing.T) {
	c := newTestCorrelator(t)

	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
	c.RegisterRequest("req-2", "backend-A", "model-2", fixtureTime.Add(time.Minute))

	result := c.Query(QueryFilter{BackendID: "backend-A"})
	if len(result) != 2 {
		t.Fatalf("expected 2 results, got %d", len(result))
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorMultipleBackends(t *testing.T) {
	c := newTestCorrelator(t)

	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
	c.RegisterRequest("req-2", "backend-B", "model-2", fixtureTime.Add(time.Minute))

	result := c.Query(QueryFilter{BackendID: "backend-A"})
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	if result[0].RequestID != "req-1" {
		t.Fatalf("request ID = %q, want req-1", result[0].RequestID)
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorQueryByTimeRange(t *testing.T) {
	c := newTestCorrelator(t)

	t1 := fixtureTime
	t2 := fixtureTime.Add(5 * time.Minute)
	t3 := fixtureTime.Add(10 * time.Minute)

	c.RegisterRequest("req-1", "backend-A", "model-1", t1)
	c.RegisterRequest("req-2", "backend-A", "model-2", t2)
	c.RegisterRequest("req-3", "backend-A", "model-3", t3)

	result := c.Query(QueryFilter{TimeStart: t1, TimeEnd: t2})
	if len(result) != 2 {
		t.Fatalf("expected 2 results, got %d", len(result))
	}

	result = c.Query(QueryFilter{TimeStart: t2})
	if len(result) != 2 {
		t.Fatalf("expected 2 results, got %d", len(result))
	}
	if result[0].RequestID != "req-3" {
		t.Fatalf("request ID = %q, want req-3", result[0].RequestID)
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorQueryByStatus(t *testing.T) {
	c := newTestCorrelator(t)

	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
	c.RegisterRequest("req-2", "backend-A", "model-2", fixtureTime.Add(time.Minute))

	c.CorrelateActivity("act-1", "req-1", fixtureTime.Add(2*time.Minute), map[string]any{"status": "completed"})

	result := c.Query(QueryFilter{Status: "completed"})
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	if result[0].RequestID != "req-1" {
		t.Fatalf("request ID = %q, want req-1", result[0].RequestID)
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorPagination(t *testing.T) {
	c := newTestCorrelator(t)

	for i := 0; i < 5; i++ {
		c.RegisterRequest("req-"+string(rune('1'+i)), "backend-A", "model-1", fixtureTime.Add(time.Duration(i)*time.Minute))
	}

	result := c.Query(QueryFilter{Limit: 2, Offset: 0})
	if len(result) != 2 {
		t.Fatalf("expected 2 results, got %d", len(result))
	}

	result = c.Query(QueryFilter{Limit: 2, Offset: 2})
	if len(result) != 2 {
		t.Fatalf("expected 2 results, got %d", len(result))
	}

	result = c.Query(QueryFilter{Limit: 2, Offset: 4})
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorRetentionCount(t *testing.T) {
	c := newTestCorrelatorWithRetention(t, 3, 0, 0)

	for i := 0; i < 5; i++ {
		c.RegisterRequest("req-"+string(rune('1'+i)), "backend-A", "model-1", fixtureTime.Add(time.Duration(i)*time.Minute))
	}

	result := c.Query(QueryFilter{})
	if len(result) != 3 {
		t.Fatalf("expected 3 results after count retention, got %d", len(result))
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorRetentionSurvivors(t *testing.T) {
	c := newTestCorrelatorWithRetention(t, 3, 0, 0)

	times := []time.Time{
		fixtureTime,
		fixtureTime.Add(time.Minute),
		fixtureTime.Add(2 * time.Minute),
		fixtureTime.Add(3 * time.Minute),
		fixtureTime.Add(4 * time.Minute),
	}

	for i, t := range times {
		c.RegisterRequest(fmt.Sprintf("req-%d", i), "backend-A", "model-1", t)
	}

	result := c.Query(QueryFilter{})
	if len(result) != 3 {
		t.Fatalf("expected 3 survivors, got %d", len(result))
	}

	// Oldest evicted: the 3 newest survive. Query sorts newest first.
	expected := []string{"req-4", "req-3", "req-2"}
	for i, r := range result {
		if r.RequestID != expected[i] {
			t.Fatalf("survivor %d = %q, want %q", i, r.RequestID, expected[i])
		}
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorRetentionAge(t *testing.T) {
	c := newTestCorrelatorWithRetention(t, 0, time.Hour, 0)

	now := fixtureTime
	old := now.Add(-2 * time.Hour)
	recent := now.Add(-30 * time.Minute)

	c.RegisterRequest("req-old", "backend-A", "model-1", old)
	c.RegisterRequest("req-recent", "backend-A", "model-2", recent)

	result := c.Query(QueryFilter{})
	if len(result) != 1 {
		t.Fatalf("expected 1 result after age retention, got %d", len(result))
	}
	if result[0].RequestID != "req-recent" {
		t.Fatalf("request ID = %q, want req-recent", result[0].RequestID)
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorRetentionSize(t *testing.T) {
	c := newTestCorrelatorWithRetention(t, 0, 0, 500) // size limit of 500 bytes

	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
	c.CorrelateActivity("act-1", "req-1", fixtureTime.Add(time.Minute), map[string]any{"completion_tokens": 5})

	result := c.Query(QueryFilter{})
	if len(result) != 1 {
		t.Fatalf("expected 1 result after size retention, got %d", len(result))
	}
	if result[0].RequestID != "req-1" {
		t.Fatalf("request ID = %q, want req-1", result[0].RequestID)
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorGetByRequestID(t *testing.T) {
	c := newTestCorrelator(t)

	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
	c.CorrelateActivity("act-1", "req-1", fixtureTime.Add(time.Minute), nil)

	r, ok := c.GetByRequestID("req-1")
	if !ok {
		t.Fatal("expected to find request")
	}
	if r.RequestID != "req-1" {
		t.Fatalf("request ID = %q, want req-1", r.RequestID)
	}

	_, ok = c.GetByRequestID("req-nonexistent")
	if ok {
		t.Fatal("expected not found for nonexistent request")
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorGetByActivityID(t *testing.T) {
	for _, activity := range []string{"act-1", "act-shared"} {
		t.Run(activity, func(t *testing.T) {
			c := newTestCorrelator(t)

			c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
			c.RegisterRequest("req-2", "backend-A", "model-2", fixtureTime.Add(time.Minute))
			c.CorrelateActivity(activity, "req-1", fixtureTime.Add(2*time.Minute), nil)
			c.CorrelateActivity(activity, "req-2", fixtureTime.Add(3*time.Minute), nil)

			result := c.GetByActivityID(activity)
			if len(result) != 2 {
				t.Fatalf("expected 2 results, got %d", len(result))
			}
			assertSizeConsistent(t, c)
		})
	}
}

func TestCorrelatorStats(t *testing.T) {
	c := newTestCorrelator(t)

	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
	c.CorrelateActivity("act-1", "req-1", fixtureTime.Add(time.Minute), nil)

	stats := c.Stats()
	if stats["record_count"] != 1 {
		t.Fatalf("record count = %v, want 1", stats["record_count"])
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorSensitiveDataExclusion(t *testing.T) {
	c := newTestCorrelator(t)

	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
	c.CorrelateActivity("act-1", "req-1", fixtureTime.Add(time.Minute), map[string]any{
		"prompt":            "secret prompt text",
		"reasoning":         "secret reasoning",
		"api_key":           "secret-api-key",
		"completion_tokens": 5,
		"finish_reason":     "stop",
	})

	result := c.Query(QueryFilter{})
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	r := result[0]
	// Sensitive/unknown data should not be in plain metadata.
	if _, ok := r.Metadata["prompt"]; ok {
		t.Fatal("prompt text should not be retained in plain form")
	}
	if _, ok := r.Metadata["api_key"]; ok {
		t.Fatal("API key should not be retained in plain form")
	}
	// Non-sensitive allowlisted data should be retained.
	if v, _ := r.Metadata["completion_tokens"]; v != 5 {
		t.Fatalf("completion_tokens = %v, want 5", v)
	}
	if v, _ := r.Metadata["finish_reason"]; v != "stop" {
		t.Fatalf("finish_reason = %v, want stop", v)
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorUnknownSecretMetadata(t *testing.T) {
	c := newTestCorrelator(t)

	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
	c.CorrelateActivity("act-1", "req-1", fixtureTime.Add(time.Minute), map[string]any{
		"password":          "secret-password",
		"secret":            "secret-value",
		"api_key":           "secret-api-key",
		"huge_value":        strings.Repeat("a", 5000),
		"slice_value":       []string{"a", "b"},
		"map_value":         map[string]string{"a": "b"},
		"completion_tokens": 5,
		"duration_ms":       42,
		"finish_reason":     "stop",
		"status":            "completed",
	})

	result := c.Query(QueryFilter{})
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	r := result[0]
	// Unknown/secret keys should be discarded without retaining their names.
	for _, key := range []string{"password", "secret", "api_key", "huge_value", "slice_value", "map_value"} {
		if _, ok := r.Metadata[key]; ok {
			t.Fatalf("unknown/secret key %q should be discarded", key)
		}
	}
	// Allowlisted typed values should be retained.
	if v, _ := r.Metadata["completion_tokens"]; v != 5 {
		t.Fatalf("completion_tokens = %v, want 5", v)
	}
	if v, _ := r.Metadata["duration_ms"]; v != 42 {
		t.Fatalf("duration_ms = %v, want 42", v)
	}
	if v, _ := r.Metadata["finish_reason"]; v != "stop" {
		t.Fatalf("finish_reason = %v, want stop", v)
	}
	if v, _ := r.Metadata["status"]; v != "completed" {
		t.Fatalf("status = %v, want completed", v)
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorConcurrentQueries(t *testing.T) {
	c := newTestCorrelator(t)

	for i := 0; i < 10; i++ {
		c.RegisterRequest("req-"+string(rune('1'+i)), "backend-A", "model-1", fixtureTime.Add(time.Duration(i)*time.Minute))
	}

	done := runConcurrentReaders(t, c, 10, 10, 10)

	for i := 0; i < 10; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorConcurrentWritesAndReads(t *testing.T) {
	c := newTestCorrelator(t)

	// Concurrent writes.
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c.RegisterRequest(fmt.Sprintf("req-%d", i), "backend-A", "", fixtureTime.Add(time.Duration(i)*time.Minute))
		}(i)
	}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c.CorrelateActivity(fmt.Sprintf("act-%d", i), fmt.Sprintf("req-%d", i), fixtureTime.Add(time.Duration(i)*time.Minute).Add(time.Minute), nil)
		}(i)
	}

	// Concurrent reads overlapping with writes.
	done := runConcurrentReaders(t, c, 20, 20, -1)

	// Wait for all writes to complete.
	wg.Wait()

	// Verify all 20 records are present after writes complete.
	result := c.Query(QueryFilter{})
	if len(result) != 20 {
		t.Fatalf("expected 20 results after writes, got %d", len(result))
	}

	// Verify no concurrent reader reported errors.
	for i := 0; i < 20; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorReplayDedup(t *testing.T) {
	c := newTestCorrelator(t)

	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)

	// Correlate the same activity twice (replay).
	c.CorrelateActivity("act-1", "req-1", fixtureTime.Add(time.Minute), nil)
	c.CorrelateActivity("act-1", "req-1", fixtureTime.Add(2*time.Minute), nil)

	// Verify no duplication in activity index.
	result := c.GetByActivityID("act-1")
	if len(result) != 1 {
		t.Fatalf("expected 1 result after dedup, got %d", len(result))
	}

	// Verify single record.
	result = c.Query(QueryFilter{RequestID: "req-1"})
	if len(result) != 1 {
		t.Fatalf("expected 1 record, got %d", len(result))
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorOrphanRegistration(t *testing.T) {
	c := newTestCorrelator(t)

	// Correlate activity before registering the request (out of order).
	actTime := fixtureTime
	c.CorrelateActivity("act-1", "req-orphan", actTime, nil)

	// Verify orphan is not immediately evicted (RequestTime is not zero).
	result := c.Query(QueryFilter{RequestID: "req-orphan"})
	if len(result) != 1 {
		t.Fatalf("expected 1 orphan result, got %d", len(result))
	}
	if result[0].RequestTime.IsZero() {
		t.Fatal("orphan RequestTime must not be zero")
	}

	// Now register the request.
	reqTime := actTime.Add(-time.Minute)
	c.RegisterRequest("req-orphan", "backend-A", "model-1", reqTime)

	// Verify the record is updated with request info.
	result = c.Query(QueryFilter{RequestID: "req-orphan"})
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	r := result[0]
	if r.BackendID != "backend-A" {
		t.Fatalf("backend ID = %q, want backend-A", r.BackendID)
	}
	if r.ModelID != "model-1" {
		t.Fatalf("model ID = %q, want model-1", r.ModelID)
	}
	if !r.RequestTime.Equal(reqTime) {
		t.Fatalf("request time = %v, want %v", r.RequestTime, reqTime)
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorSizeTracking(t *testing.T) {
	c := newTestCorrelatorWithRetention(t, 0, 0, 0) // default retention, no eviction for recent/small records

	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
	assertSizeConsistent(t, c)
	size1 := c.Stats()["total_size"].(int64)
	if size1 <= 0 {
		t.Fatalf("total size should be positive after registration, got %d", size1)
	}

	c.CorrelateActivity("act-1", "req-1", fixtureTime.Add(time.Minute), map[string]any{"completion_tokens": 5})
	assertSizeConsistent(t, c)
	size2 := c.Stats()["total_size"].(int64)
	if size2 <= size1 {
		t.Fatalf("total size should increase after activity metadata, got %d <= %d", size2, size1)
	}

	// Register a second request; size should increase again.
	c.RegisterRequest("req-2", "backend-A", "model-1", fixtureTime.Add(time.Minute))
	assertSizeConsistent(t, c)
	size3 := c.Stats()["total_size"].(int64)
	if size3 <= size2 {
		t.Fatalf("total size should increase after second registration, got %d <= %d", size3, size2)
	}

	// Evict oldest by tightening count retention to 1.
	c.WithRetention(1, 0, 0)
	assertSizeConsistent(t, c)
	size4 := c.Stats()["total_size"].(int64)
	if size4 >= size3 {
		t.Fatalf("total size should decrease after eviction, got %d >= %d", size4, size3)
	}
	if c.Stats()["record_count"] != 1 {
		t.Fatalf("expected 1 record after eviction, got %d", c.Stats()["record_count"])
	}
}

func TestCorrelatorOutputMutation(t *testing.T) {
	c := newTestCorrelator(t)

	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
	c.CorrelateActivity("act-1", "req-1", fixtureTime.Add(time.Minute), map[string]any{"completion_tokens": 5})

	result := c.Query(QueryFilter{})
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	r := result[0]

	// Mutate the returned record.
	r.RequestID = "mutated"
	r.Metadata["completion_tokens"] = 99
	r.Status = "mutated-status"

	// Verify internal state is unaffected.
	r2, ok := c.GetByRequestID("req-1")
	if !ok || r2.RequestID != "req-1" {
		t.Fatal("internal state was mutated by output mutation")
	}
	if v, _ := r2.Metadata["completion_tokens"]; v != 5 {
		t.Fatal("internal metadata was mutated by output mutation")
	}
	if r2.Status != "pending" {
		t.Fatalf("internal status was mutated, got %q", r2.Status)
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorPaginationBounds(t *testing.T) {
	c := newTestCorrelator(t)

	for i := 0; i < 5; i++ {
		c.RegisterRequest(fmt.Sprintf("req-%d", i), "backend-A", "model-1", fixtureTime.Add(time.Duration(i)*time.Minute))
	}

	// Limit larger than max should be capped.
	result := c.Query(QueryFilter{Limit: 9999})
	if len(result) != 5 {
		t.Fatalf("expected 5 results with large limit, got %d", len(result))
	}

	// Offset beyond the range should return empty.
	result = c.Query(QueryFilter{Limit: 2, Offset: 10})
	if len(result) != 0 {
		t.Fatalf("expected 0 results with offset beyond range, got %d", len(result))
	}

	// Offset equal to length should return empty.
	result = c.Query(QueryFilter{Limit: 2, Offset: 5})
	if len(result) != 0 {
		t.Fatalf("expected 0 results with offset equal to length, got %d", len(result))
	}

	// Last page: records are sorted newest first.
	result = c.Query(QueryFilter{Limit: 2, Offset: 4})
	if len(result) != 1 {
		t.Fatalf("expected 1 result on last page, got %d", len(result))
	}
	if result[0].RequestID != "req-0" {
		t.Fatalf("request ID = %q, want req-0", result[0].RequestID)
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorZeroNegativePaging(t *testing.T) {
	c := newTestCorrelator(t)

	for i := 0; i < 5; i++ {
		c.RegisterRequest(fmt.Sprintf("req-%d", i), "backend-A", "model-1", fixtureTime.Add(time.Duration(i)*time.Minute))
	}

	// Limit 0 should default to defaultQueryLimit (50 >= 5 records).
	result := c.Query(QueryFilter{Limit: 0})
	if len(result) != 5 {
		t.Fatalf("expected 5 results with limit 0, got %d", len(result))
	}

	// Limit negative should default to defaultQueryLimit.
	result = c.Query(QueryFilter{Limit: -1})
	if len(result) != 5 {
		t.Fatalf("expected 5 results with negative limit, got %d", len(result))
	}

	// Offset negative should clamp to 0.
	result = c.Query(QueryFilter{Limit: 2, Offset: -1})
	if len(result) != 2 {
		t.Fatalf("expected 2 results with negative offset, got %d", len(result))
	}

	// Offset beyond range should return empty.
	result = c.Query(QueryFilter{Limit: 2, Offset: 10})
	if len(result) != 0 {
		t.Fatalf("expected 0 results with offset beyond range, got %d", len(result))
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorCompleteRequest(t *testing.T) {
	c := newTestCorrelator(t)
	startTime := fixtureTime
	c.RegisterRequest("req-1", "backend-A", "", startTime)

	// All valid transport statuses are retained.
	for _, status := range []string{
		"pending", "in_progress", "completed", "failed", "cancelled",
		"http_success", "http_client_error", "http_server_error",
		"http_other", "client_canceled", "aborted", "transport_error",
	} {
		c.CompleteRequest("req-1", time.Second, status)
		result := c.Query(QueryFilter{RequestID: "req-1"})
		if len(result) != 1 {
			t.Fatalf("expected 1 result for status %q, got %d", status, len(result))
		}
		r := result[0]
		if r.Status != status {
			t.Fatalf("status = %q, want %q", r.Status, status)
		}
		if v, ok := r.Metadata["duration_ms"]; !ok {
			t.Fatalf("duration_ms not found for status %q", status)
		} else if ms, ok := v.(int64); !ok || ms != 1000 {
			t.Fatalf("duration_ms = %v (%T), want 1000 for status %q", v, v, status)
		}
		assertSizeConsistent(t, c)
	}

	// Invalid status should be ignored (last valid status retained).
	c.CompleteRequest("req-1", time.Second, "invalid-status")
	result := c.Query(QueryFilter{RequestID: "req-1"})
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	r := result[0]
	if r.Status != "transport_error" {
		t.Fatalf("status should remain transport_error, got %q", r.Status)
	}
	assertSizeConsistent(t, c)

	// Non-existent request should be a no-op.
	c.CompleteRequest("req-nonexistent", time.Second, "completed")
	result = c.Query(QueryFilter{RequestID: "req-nonexistent"})
	if len(result) != 0 {
		t.Fatalf("expected 0 results for nonexistent request, got %d", len(result))
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorHugeIDs(t *testing.T) {
	c := newTestCorrelator(t)

	// Oversized request ID is rejected (not stored/truncated).
	hugeID := strings.Repeat("x", 1000)
	c.RegisterRequest(hugeID, "backend-A", "model-1", fixtureTime)
	result := c.Query(QueryFilter{})
	if len(result) != 0 {
		t.Fatalf("expected 0 results with oversized request ID, got %d", len(result))
	}

	// Oversized activity ID is rejected (not indexed).
	c.RegisterRequest("req-valid", "backend-A", "model-1", fixtureTime)
	c.CorrelateActivity(strings.Repeat("y", 1000), "req-valid", fixtureTime.Add(time.Minute), nil)
	result = c.Query(QueryFilter{RequestID: "req-valid"})
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	r := result[0]
	if r.ActivityID != "" {
		t.Fatalf("activity ID should be empty (oversized rejected), got %q", r.ActivityID)
	}

	// Two distinct long IDs (exactly maxIdentifierSize) never merge.
	c3 := newTestCorrelator(t)
	longID1 := strings.Repeat("a", maxIdentifierSize)
	longID2 := strings.Repeat("b", maxIdentifierSize)
	c3.RegisterRequest(longID1, "backend-A", "model-1", fixtureTime)
	c3.RegisterRequest(longID2, "backend-A", "model-1", fixtureTime.Add(time.Minute))
	result = c3.Query(QueryFilter{})
	if len(result) != 2 {
		t.Fatalf("expected 2 results for distinct long IDs, got %d", len(result))
	}
	ids := map[string]bool{}
	for _, r := range result {
		ids[r.RequestID] = true
	}
	if !ids[longID1] || !ids[longID2] {
		t.Fatalf("distinct long IDs should not merge; found %v", ids)
	}
	assertSizeConsistent(t, c3)
}

// TestCorrelatorIDCollisionRegression registers a valid 256-byte ID and then
// attempts its oversized prefix and suffix variants. The valid record must
// remain unchanged and the oversized variants must be rejected.
func TestCorrelatorIDCollisionRegression(t *testing.T) {
	c := newTestCorrelator(t)

	// Valid identifier at exactly maxIdentifierSize bytes.
	validID := strings.Repeat("a", maxIdentifierSize)
	c.RegisterRequest(validID, "backend-A", "model-1", fixtureTime)

	// Oversized prefix and suffix variants of the valid ID.
	prefixID := "x" + validID // 257 bytes
	suffixID := validID + "x" // 257 bytes
	c.RegisterRequest(prefixID, "backend-A", "model-1", fixtureTime)
	c.RegisterRequest(suffixID, "other-backend", "other-model", fixtureTime.Add(-time.Hour))

	// Valid record must be unchanged.
	result := c.Query(QueryFilter{RequestID: validID})
	if len(result) != 1 || result[0].RequestID != validID {
		t.Fatalf("valid record should be unchanged, got %d results", len(result))
	}
	if !result[0].RequestTime.Equal(fixtureTime) || result[0].BackendID != "backend-A" || result[0].ModelID != "model-1" {
		t.Fatalf("oversized ID mutated the valid record: %#v", result[0])
	}
	// Oversized variants must not be stored.
	if result = c.Query(QueryFilter{RequestID: prefixID}); len(result) != 0 {
		t.Fatal("oversized prefix variant should be rejected")
	}
	if result = c.Query(QueryFilter{RequestID: suffixID}); len(result) != 0 {
		t.Fatal("oversized suffix variant should be rejected")
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorEmptyIDs(t *testing.T) {
	c := newTestCorrelator(t)

	// Empty request ID is rejected.
	c.RegisterRequest("", "backend-A", "model-1", fixtureTime)
	result := c.Query(QueryFilter{})
	if len(result) != 0 {
		t.Fatalf("expected 0 results with empty request ID, got %d", len(result))
	}

	// Empty activity ID is rejected in CorrelateActivity.
	c.RegisterRequest("req-valid", "backend-A", "model-1", fixtureTime)
	c.CorrelateActivity("", "req-valid", fixtureTime.Add(time.Minute), nil)
	result = c.Query(QueryFilter{RequestID: "req-valid"})
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	r := result[0]
	if r.ActivityID != "" {
		t.Fatalf("activity ID should be empty (empty activity ID rejected), got %q", r.ActivityID)
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorSetClockNil(t *testing.T) {
	c := New()
	// Use the real clock's current time so the record is not age-evicted by the
	// real 24h retention when the clock is restored.
	c.RegisterRequest("req-1", "backend-A", "model-1", time.Now())

	// nil clock restores default; no panic on reads.
	c.SetClock(nil)
	stats := c.Stats()
	if stats["record_count"] != 1 {
		t.Fatalf("expected 1 record, got %d", stats["record_count"])
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorSizeConsistency(t *testing.T) {
	c := newTestCorrelatorWithRetention(t, 0, 0, 0) // default retention

	// Register a request.
	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
	assertSizeConsistent(t, c)

	// Correlate an activity.
	c.CorrelateActivity("act-1", "req-1", fixtureTime.Add(time.Minute), map[string]any{"completion_tokens": 5})
	assertSizeConsistent(t, c)

	// Complete the request.
	c.CompleteRequest("req-1", time.Second, "completed")
	assertSizeConsistent(t, c)

	// Create two records and evict the oldest by tightening count retention.
	c2 := newTestCorrelatorWithRetention(t, 0, 0, 0)
	c2.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
	c2.RegisterRequest("req-2", "backend-A", "model-1", fixtureTime.Add(time.Minute))
	assertSizeConsistent(t, c2)
	c2.WithRetention(1, 0, 0)
	assertSizeConsistent(t, c2)
	if c2.Stats()["record_count"] != 1 {
		t.Fatalf("expected 1 record after count eviction, got %d", c2.Stats()["record_count"])
	}
}

package correlation

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

// newEntropyFailingCorrelator constructs a Correlator via the public New()
// API and then injects a failing entropy source. The constructor itself never
// panics on entropy failure because storeID and storeSecret are initialized
// lazily on first snapshot creation. The clock is pinned to fixtureTime so
// registered records are not age-evicted. No global rand state is touched and
// no defer restore is used; the failing reader is scoped to this correlator.
func newEntropyFailingCorrelator(t *testing.T) *Correlator {
	t.Helper()
	c := New()
	c.SetClock(&mockClock{now: fixtureTime})
	c.entropyReader = entropyFailingReader{}
	return c
}

// TestCorrelatorEntropyStartupDoesNotPanic verifies that ordinary
// request_history startup (New + RegisterRequest + Query) never requires
// cursor entropy and never panics when the entropy source fails. A
// single-page QueryPage also works without entropy (no cursor, no snapshot,
// no storage, no entropy consumed).
func TestCorrelatorEntropyStartupDoesNotPanic(t *testing.T) {
	c := newEntropyFailingCorrelator(t)

	// Constructor and registration must not require entropy and must not panic.
	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
	c.CorrelateActivity("act-1", "req-1", fixtureTime.Add(time.Minute), map[string]any{
		"completion_tokens": 5,
		"finish_reason":     "stop",
	})

	// Query (non-paging) works without entropy.
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

	// Single-page QueryPage works without entropy: no cursor, no snapshot,
	// no storage, no entropy consumed.
	page, err := c.QueryPage(QueryFilter{}, PageRequest{Limit: 50})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(page.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(page.Records))
	}
	if page.Cursor != "" {
		t.Fatal("expected empty cursor when all records fit in one page")
	}
	if c.snapshotCount != 0 {
		t.Fatalf("expected no snapshot stored for one-page result, got %d", c.snapshotCount)
	}

	// GetByRequestID and Stats also work without entropy.
	if _, ok := c.GetByRequestID("req-1"); !ok {
		t.Fatal("expected to find request")
	}
	if c.Stats()["record_count"] != 1 {
		t.Fatalf("expected record_count 1, got %v", c.Stats()["record_count"])
	}
}

// TestCorrelatorEntropyPagedQueryFailsClosed verifies that a paged query
// requiring a cursor (multi-page snapshot) fails closed with
// ErrSnapshotEntropy instead of panicking when entropy is unavailable. Cursor
// creation must fail closed if rand is unavailable.
func TestCorrelatorEntropyPagedQueryFailsClosed(t *testing.T) {
	c := newEntropyFailingCorrelator(t)

	for i := 0; i < 3; i++ {
		c.RegisterRequest(fmt.Sprintf("req-%d", i), "backend-A", "model-1", fixtureTime.Add(time.Duration(i)*time.Minute))
	}

	// Multi-page snapshot creation requires entropy. It must fail closed
	// with ErrSnapshotEntropy, not panic.
	_, err := c.QueryPage(QueryFilter{}, PageRequest{Limit: 1})
	if err != nil {
		if !errors.Is(err, ErrSnapshotEntropy) {
			t.Fatalf("expected ErrSnapshotEntropy, got %v", err)
		}
		return
	}
	t.Fatal("expected ErrSnapshotEntropy for paged query without entropy, got nil error")
}

// TestCorrelatorEntropyCrossStoreRejection verifies that a store without
// entropy cannot resume a cursor (fail closed) and that cross-store cursor
// rejection still holds when both stores have entropy. The HMAC per store and
// snapshot map keep cursors bound to their originating store; no predictable
// cursor secrets are used.
func TestCorrelatorEntropyCrossStoreRejection(t *testing.T) {
	c1 := newTestCorrelator(t) // normal entropy
	c2 := newEntropyFailingCorrelator(t)

	c1.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
	c1.RegisterRequest("req-2", "backend-A", "model-1", fixtureTime.Add(time.Minute))

	// c1 (entropy available) creates a non-empty cursor.
	resp, err := c1.QueryPage(QueryFilter{}, PageRequest{Limit: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(resp.Records))
	}
	if resp.Cursor == "" {
		t.Fatal("expected non-empty cursor")
	}

	// c1 can resume its own cursor (same store, entropy available).
	_, err = c1.QueryPage(QueryFilter{}, PageRequest{Cursor: resp.Cursor, Limit: 1})
	if err != nil {
		t.Fatalf("expected same-store resume to succeed, got %v", err)
	}

	// c2 (entropy unavailable) cannot resume c1's cursor: ensureStoreSecret
	// fails closed with ErrSnapshotEntropy. The cursor is never accepted.
	_, err = c2.QueryPage(QueryFilter{}, PageRequest{Cursor: resp.Cursor, Limit: 1})
	if err != nil {
		if !errors.Is(err, ErrSnapshotEntropy) {
			t.Fatalf("expected ErrSnapshotEntropy for entropy-less store resume, got %v", err)
		}
		return
	}
	t.Fatal("expected ErrSnapshotEntropy for entropy-less store resume, got nil error")
}

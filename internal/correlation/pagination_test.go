package correlation

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestPaginationFilterDistinguishesUnsetAndUnixEpochBounds(t *testing.T) {
	startOnly, err := filterDigest(QueryFilter{TimeStart: time.Unix(0, 0).UTC()})
	if err != nil {
		t.Fatal(err)
	}
	endOnly, err := filterDigest(QueryFilter{TimeEnd: time.Unix(0, 0).UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if string(startOnly) == string(endOnly) {
		t.Fatal("distinct time-bound filters have the same digest")
	}
}

func TestQueryPageSinglePageNeedsNoEntropy(t *testing.T) {
	c := newTestCorrelator(t)
	c.entropyReader = entropyFailingReader{}
	c.RegisterRequest("req", "backend", "", fixtureTime)
	page, err := c.QueryPage(QueryFilter{}, PageRequest{Limit: 2})
	if err != nil || len(page.Records) != 1 || page.Cursor != "" {
		t.Fatalf("single page = %#v, error=%v", page, err)
	}
}

func TestPaginationRejectsNonCanonicalBase64(t *testing.T) {
	key := make([]byte, storeSecretSize)
	token, err := encodeCursor(key, make([]byte, snapshotIDSize), 1)
	if err != nil {
		t.Fatal(err)
	}
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	i := strings.IndexByte(alphabet, token[len(token)-1])
	nonCanonical := token[:len(token)-1] + string(alphabet[i|1])
	if _, _, err := decodeCursor(key, nonCanonical); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatal("accepted non-canonical base64 cursor")
	}
}

// advanceableClock is a mock clock that can be advanced for TTL expiry tests.
type advanceableClock struct {
	now time.Time
}

func (m *advanceableClock) Now() time.Time { return m.now }

func (m *advanceableClock) Advance(d time.Duration) { m.now = m.now.Add(d) }

func newTestCorrelatorWithClock(t *testing.T, clock Clock) *Correlator {
	t.Helper()
	c := New()
	c.SetClock(clock)
	return c
}

func TestQueryPageBasicPagination(t *testing.T) {
	c := newTestCorrelator(t)

	for i := 0; i < 5; i++ {
		c.RegisterRequest(fmt.Sprintf("req-%d", i), "backend-A", "model-1", fixtureTime.Add(time.Duration(i)*time.Minute))
	}

	// First page.
	resp, err := c.QueryPage(QueryFilter{}, PageRequest{Limit: 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Records) != 2 {
		t.Fatalf("expected 2 records on first page, got %d", len(resp.Records))
	}
	if resp.Records[0].RequestID != "req-4" {
		t.Fatalf("first record = %q, want req-4 (newest first)", resp.Records[0].RequestID)
	}
	if resp.Records[1].RequestID != "req-3" {
		t.Fatalf("second record = %q, want req-3", resp.Records[1].RequestID)
	}
	if resp.Cursor == "" {
		t.Fatal("expected non-empty cursor on first page")
	}

	// Second page.
	resp2, err := c.QueryPage(QueryFilter{}, PageRequest{Cursor: resp.Cursor, Limit: 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp2.Records) != 2 {
		t.Fatalf("expected 2 records on second page, got %d", len(resp2.Records))
	}
	if resp2.Records[0].RequestID != "req-2" {
		t.Fatalf("first record = %q, want req-2", resp2.Records[0].RequestID)
	}
	if resp2.Records[1].RequestID != "req-1" {
		t.Fatalf("second record = %q, want req-1", resp2.Records[1].RequestID)
	}

	// Third (last) page.
	resp3, err := c.QueryPage(QueryFilter{}, PageRequest{Cursor: resp2.Cursor, Limit: 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp3.Records) != 1 {
		t.Fatalf("expected 1 record on last page, got %d", len(resp3.Records))
	}
	if resp3.Records[0].RequestID != "req-0" {
		t.Fatalf("record = %q, want req-0", resp3.Records[0].RequestID)
	}
	if resp3.Cursor != "" {
		t.Fatal("expected empty cursor on last page")
	}

	assertSizeConsistent(t, c)
}

func TestQueryPageDefaultLimit(t *testing.T) {
	c := newTestCorrelator(t)

	for i := 0; i < 3; i++ {
		c.RegisterRequest(fmt.Sprintf("req-%d", i), "backend-A", "model-1", fixtureTime.Add(time.Duration(i)*time.Minute))
	}

	resp, err := c.QueryPage(QueryFilter{}, PageRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Records) != 3 {
		t.Fatalf("expected 3 records with default limit, got %d", len(resp.Records))
	}
	if resp.Cursor != "" {
		t.Fatal("expected empty cursor when all records fit in one page")
	}
	assertSizeConsistent(t, c)
}

func TestQueryPageTieTimestamps(t *testing.T) {
	c := newTestCorrelator(t)

	// Same timestamp, different request IDs -> deterministic ordering by ID.
	c.RegisterRequest("req-b", "backend-A", "model-1", fixtureTime)
	c.RegisterRequest("req-a", "backend-A", "model-1", fixtureTime)
	c.RegisterRequest("req-c", "backend-A", "model-1", fixtureTime)

	resp, err := c.QueryPage(QueryFilter{}, PageRequest{Limit: 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(resp.Records))
	}
	// Sorted by request ID ascending for ties.
	if resp.Records[0].RequestID != "req-a" {
		t.Fatalf("first record = %q, want req-a", resp.Records[0].RequestID)
	}
	if resp.Records[1].RequestID != "req-b" {
		t.Fatalf("second record = %q, want req-b", resp.Records[1].RequestID)
	}
	assertSizeConsistent(t, c)
}

func TestQueryPageBackdatedTimestamps(t *testing.T) {
	c := newTestCorrelator(t)

	// Register in order but with backdated times.
	c.RegisterRequest("req-late", "backend-A", "model-1", fixtureTime.Add(10*time.Minute))
	c.RegisterRequest("req-early", "backend-A", "model-1", fixtureTime)

	resp, err := c.QueryPage(QueryFilter{}, PageRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(resp.Records))
	}
	// Newest first: req-late has later RequestTime.
	if resp.Records[0].RequestID != "req-late" {
		t.Fatalf("first record = %q, want req-late", resp.Records[0].RequestID)
	}
	if resp.Records[1].RequestID != "req-early" {
		t.Fatalf("second record = %q, want req-early", resp.Records[1].RequestID)
	}
	assertSizeConsistent(t, c)
}

func TestQueryPageSnapshotFrozenOnUpdate(t *testing.T) {
	c := newTestCorrelator(t)

	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
	c.RegisterRequest("req-2", "backend-A", "model-1", fixtureTime.Add(time.Minute))
	c.CorrelateActivity("act-1", "req-1", fixtureTime.Add(time.Minute), map[string]any{"completion_tokens": 5})

	// Small limit to get a non-empty cursor.
	resp, err := c.QueryPage(QueryFilter{}, PageRequest{Limit: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(resp.Records))
	}
	if resp.Cursor == "" {
		t.Fatal("expected non-empty cursor")
	}
	// Newest first: req-2 is on page 1, req-1 is on page 2.
	if resp.Records[0].RequestID != "req-2" {
		t.Fatalf("first record = %q, want req-2", resp.Records[0].RequestID)
	}

	// Update the record after snapshot creation.
	c.CompleteRequest("req-1", time.Second, "completed")

	// Snapshot should still reflect the pre-update state.
	resp2, err := c.QueryPage(QueryFilter{}, PageRequest{Cursor: resp.Cursor, Limit: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp2.Records) != 1 {
		t.Fatalf("expected 1 record on resumed page, got %d", len(resp2.Records))
	}
	if resp2.Records[0].RequestID != "req-1" {
		t.Fatalf("resumed record = %q, want req-1", resp2.Records[0].RequestID)
	}
	// Snapshot is frozen: req-1 should still be pending.
	if resp2.Records[0].Status != "pending" {
		t.Fatalf("snapshot should be frozen, status = %q, want pending", resp2.Records[0].Status)
	}
	if v, _ := resp2.Records[0].Metadata["completion_tokens"]; v != 5 {
		t.Fatalf("snapshot should be frozen, completion_tokens = %v, want 5", v)
	}
	assertSizeConsistent(t, c)
}

func TestQueryPagePrimaryEvictionDoesNotAffectSnapshot(t *testing.T) {
	c := newTestCorrelatorWithRetention(t, 10, 0, 0)

	for i := 0; i < 5; i++ {
		c.RegisterRequest(fmt.Sprintf("req-%d", i), "backend-A", "model-1", fixtureTime.Add(time.Duration(i)*time.Minute))
	}

	resp, err := c.QueryPage(QueryFilter{}, PageRequest{Limit: 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(resp.Records))
	}

	// Evict primary records by tightening retention.
	c.WithRetention(2, 0, 0)

	// Snapshot should still return the same records.
	resp2, err := c.QueryPage(QueryFilter{}, PageRequest{Cursor: resp.Cursor, Limit: 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp2.Records) != 2 {
		t.Fatalf("expected 2 records after primary eviction, got %d", len(resp2.Records))
	}
	if resp2.Records[0].RequestID != "req-2" {
		t.Fatalf("first record = %q, want req-2", resp2.Records[0].RequestID)
	}
	assertSizeConsistent(t, c)
}

func TestQueryPageEmptyLastPage(t *testing.T) {
	c := newTestCorrelator(t)

	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)

	resp, err := c.QueryPage(QueryFilter{}, PageRequest{Limit: 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(resp.Records))
	}
	if resp.Cursor != "" {
		t.Fatal("expected empty cursor on last page")
	}
	assertSizeConsistent(t, c)
}

func TestQueryPageExpiredCursor(t *testing.T) {
	clock := &advanceableClock{now: fixtureTime}
	c := New()
	c.SetClock(clock)
	// Override TTL to a very short value for testing.
	c.snapshotTTL = time.Second

	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
	c.RegisterRequest("req-2", "backend-A", "model-1", fixtureTime.Add(time.Minute))

	// Small limit to get a non-empty cursor.
	resp, err := c.QueryPage(QueryFilter{}, PageRequest{Limit: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(resp.Records))
	}
	if resp.Cursor == "" {
		t.Fatal("expected non-empty cursor")
	}

	// Advance clock past TTL.
	clock.Advance(c.snapshotTTL + time.Second)

	// Resume with expired cursor -> stable error.
	_, err = c.QueryPage(QueryFilter{}, PageRequest{Cursor: resp.Cursor, Limit: 1})
	if err != nil {
		if !errors.Is(err, ErrSnapshotExpired) {
			t.Fatalf("expected ErrSnapshotExpired, got %v", err)
		}
		return
	}
	t.Fatal("expected ErrSnapshotExpired for expired cursor, got nil error")
}

func TestQueryPageEvictedCursorV2(t *testing.T) {
	c := newTestCorrelator(t)
	c.snapshotCountCap = 2
	c.snapshotSizeCap = 0

	// Register 3 records.
	for i := 0; i < 3; i++ {
		c.RegisterRequest(fmt.Sprintf("req-%d", i), "backend-A", "model-1", fixtureTime.Add(time.Duration(i)*time.Minute))
	}

	// Create one snapshot with multiple pages (Limit: 1).
	var firstCursor string
	for i := 0; i < 3; i++ {
		resp, err := c.QueryPage(QueryFilter{}, PageRequest{Limit: 1})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(resp.Records) != 1 {
			t.Fatalf("expected 1 record, got %d", len(resp.Records))
		}
		if i == 0 {
			firstCursor = resp.Cursor
		}
		if resp.Cursor == "" {
			break
		}
	}

	if firstCursor == "" {
		t.Fatal("expected non-empty cursor for eviction test")
	}

	// Create a second snapshot to evict the first due to count cap.
	c.RegisterRequest("req-extra", "backend-A", "model-1", fixtureTime.Add(3*time.Minute))
	_, _ = c.QueryPage(QueryFilter{}, PageRequest{Limit: 1})

	// First cursor should be evicted due to count cap.
	_, err := c.QueryPage(QueryFilter{}, PageRequest{Cursor: firstCursor, Limit: 1})
	if err != nil {
		if !errors.Is(err, ErrSnapshotNotFound) {
			t.Fatalf("expected ErrSnapshotNotFound for evicted cursor, got %v", err)
		}
	} else {
		t.Fatal("expected ErrSnapshotNotFound for evicted cursor, got nil error")
	}
	assertSizeConsistent(t, c)
}

func TestQueryPageFilterChangeDoesNotAffectSnapshot(t *testing.T) {
	c := newTestCorrelator(t)

	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
	c.RegisterRequest("req-2", "backend-A", "model-1", fixtureTime.Add(time.Minute))
	c.RegisterRequest("req-3", "backend-B", "model-2", fixtureTime.Add(2*time.Minute))

	// Snapshot with backend-A filter, small limit for multi-page.
	resp, err := c.QueryPage(QueryFilter{BackendID: "backend-A"}, PageRequest{Limit: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(resp.Records))
	}
	if resp.Cursor == "" {
		t.Fatal("expected non-empty cursor")
	}
	if resp.Records[0].RequestID != "req-2" {
		t.Fatalf("record = %q, want req-2 (newest first)", resp.Records[0].RequestID)
	}

	// Add a new record matching backend-A after snapshot creation.
	c.RegisterRequest("req-4", "backend-A", "model-1", fixtureTime.Add(3*time.Minute))

	// Snapshot should still only contain req-2 and req-1 (frozen at creation).
	resp2, err := c.QueryPage(QueryFilter{BackendID: "backend-A"}, PageRequest{Cursor: resp.Cursor, Limit: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp2.Records) != 1 {
		t.Fatalf("expected 1 record after filter change, got %d", len(resp2.Records))
	}
	if resp2.Records[0].RequestID != "req-1" {
		t.Fatalf("record = %q, want req-1", resp2.Records[0].RequestID)
	}
	assertSizeConsistent(t, c)
}

func TestQueryPageWrongStore(t *testing.T) {
	c1 := newTestCorrelator(t)
	c2 := newTestCorrelator(t)

	c1.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
	c1.RegisterRequest("req-2", "backend-A", "model-1", fixtureTime.Add(time.Minute))

	// Get a non-empty cursor from c1.
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

	// Use c1's cursor with c2 -> cross-store rejection.
	_, err = c2.QueryPage(QueryFilter{}, PageRequest{Cursor: resp.Cursor, Limit: 1})
	if err != nil {
		if !errors.Is(err, ErrSnapshotNotFound) {
			t.Fatalf("expected ErrSnapshotNotFound for cross-store cursor, got %v", err)
		}
	} else {
		t.Fatal("expected ErrSnapshotNotFound for cross-store cursor, got nil error")
	}
}

func TestQueryPageMalformedCursor(t *testing.T) {
	c := newTestCorrelator(t)

	// Empty cursor with non-empty expected -> treated as new snapshot (not an error).
	// But invalid cursor format should error.
	invalidCursors := []string{
		"not-valid-base64!!!",
		"",
		":0",
		"abc:0",                         // too short base64
		strings.Repeat("a", 300) + ":0", // overlong
	}

	for _, invalid := range invalidCursors {
		if invalid == "" {
			continue // empty cursor means new snapshot
		}
		_, err := c.QueryPage(QueryFilter{}, PageRequest{Cursor: invalid, Limit: 1})
		if err != nil {
			if !errors.Is(err, ErrSnapshotNotFound) {
				t.Fatalf("expected ErrSnapshotNotFound for malformed cursor %q, got %v", invalid, err)
			}
			continue
		}
		t.Fatalf("expected ErrSnapshotNotFound for malformed cursor %q, got nil error", invalid)
	}
}

func TestQueryPageOverlongCursor(t *testing.T) {
	c := newTestCorrelator(t)

	// Construct a cursor that is overlong.
	overlong := strings.Repeat("x", 300) + ":0"
	_, err := c.QueryPage(QueryFilter{}, PageRequest{Cursor: overlong, Limit: 1})
	if err != nil {
		if !errors.Is(err, ErrSnapshotNotFound) {
			t.Fatalf("expected ErrSnapshotNotFound for overlong cursor, got %v", err)
		}
	} else {
		t.Fatal("expected ErrSnapshotNotFound for overlong cursor, got nil error")
	}
}

func TestQueryPageCaps(t *testing.T) {
	c := newTestCorrelator(t)
	c.snapshotCountCap = 3
	c.snapshotSizeCap = 0

	// Register 5 records.
	for i := 0; i < 5; i++ {
		c.RegisterRequest(fmt.Sprintf("req-%d", i), "backend-A", "model-1", fixtureTime.Add(time.Duration(i)*time.Minute))
	}

	// Create one snapshot with multiple pages (Limit: 1).
	var cursors []string
	for i := 0; i < 5; i++ {
		resp, err := c.QueryPage(QueryFilter{}, PageRequest{Limit: 1})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(resp.Records) != 1 {
			t.Fatalf("expected 1 record, got %d", len(resp.Records))
		}
		if resp.Cursor != "" {
			cursors = append(cursors, resp.Cursor)
		}
	}

	if len(cursors) < 2 {
		t.Fatal("expected at least 2 non-empty cursors for cap test")
	}

	// Create a second snapshot to evict the oldest due to count cap.
	c.RegisterRequest("req-extra", "backend-A", "model-1", fixtureTime.Add(5*time.Minute))
	_, _ = c.QueryPage(QueryFilter{}, PageRequest{Limit: 1})

	// The oldest cursors should be evicted due to count cap (cap=3).
	// After creating the second snapshot, we have 2 snapshots total, which is
	// under the cap of 3, so no eviction yet. Create a third snapshot.
	c.RegisterRequest("req-extra2", "backend-A", "model-1", fixtureTime.Add(6*time.Minute))
	_, _ = c.QueryPage(QueryFilter{}, PageRequest{Limit: 1})

	// Now we have 3 snapshots, which is at the cap. The first cursor from the
	// original snapshot should still be valid (3 snapshots total, cap=3).
	// Create a fourth snapshot to evict the oldest.
	c.RegisterRequest("req-extra3", "backend-A", "model-1", fixtureTime.Add(7*time.Minute))
	_, _ = c.QueryPage(QueryFilter{}, PageRequest{Limit: 1})

	// The oldest cursor should now be evicted.
	if len(cursors) > 0 {
		_, err := c.QueryPage(QueryFilter{}, PageRequest{Cursor: cursors[0], Limit: 1})
		if err != nil {
			if !errors.Is(err, ErrSnapshotNotFound) {
				t.Fatalf("expected ErrSnapshotNotFound for evicted cursor, got %v", err)
			}
		} else {
			t.Fatal("expected ErrSnapshotNotFound for evicted cursor, got nil error")
		}
	}
	assertSizeConsistent(t, c)
}

func TestQueryPageConcurrentUse(t *testing.T) {
	c := newTestCorrelator(t)

	for i := 0; i < 20; i++ {
		c.RegisterRequest(fmt.Sprintf("req-%d", i), "backend-A", "model-1", fixtureTime.Add(time.Duration(i)*time.Minute))
	}

	done := make(chan error, 20)
	for i := 0; i < 20; i++ {
		go func() {
			resp, err := c.QueryPage(QueryFilter{}, PageRequest{Limit: 5})
			if err != nil {
				done <- err
				return
			}
			if len(resp.Records) > 5 {
				done <- errors.New("too many records")
				return
			}
			done <- nil
		}()
	}

	for i := 0; i < 20; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	assertSizeConsistent(t, c)
}

func TestQueryPageConcurrentCursorUse(t *testing.T) {
	c := newTestCorrelator(t)

	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
	c.RegisterRequest("req-2", "backend-A", "model-1", fixtureTime.Add(time.Minute))
	c.RegisterRequest("req-3", "backend-A", "model-1", fixtureTime.Add(2*time.Minute))

	resp, err := c.QueryPage(QueryFilter{}, PageRequest{Limit: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(resp.Records))
	}

	// Concurrent use of the same cursor.
	done := make(chan error, 10)
	for i := 0; i < 10; i++ {
		go func() {
			r, err := c.QueryPage(QueryFilter{}, PageRequest{Cursor: resp.Cursor, Limit: 1})
			if err != nil {
				done <- err
				return
			}
			if len(r.Records) != 1 {
				done <- errors.New("unexpected record count")
				return
			}
			done <- nil
		}()
	}

	for i := 0; i < 10; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	assertSizeConsistent(t, c)
}

func TestQueryPageDefensiveCopies(t *testing.T) {
	c := newTestCorrelator(t)

	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
	c.CorrelateActivity("act-1", "req-1", fixtureTime.Add(time.Minute), map[string]any{"completion_tokens": 5})

	resp, err := c.QueryPage(QueryFilter{}, PageRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(resp.Records))
	}

	// Mutate the returned record.
	r := resp.Records[0]
	r.RequestID = "mutated"
	r.Metadata["completion_tokens"] = 99
	r.Status = "mutated-status"
	r.ActivityID = "mutated"

	// Verify internal state and snapshot are unaffected.
	resp2, err := c.QueryPage(QueryFilter{}, PageRequest{Cursor: resp.Cursor})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp2.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(resp2.Records))
	}
	if resp2.Records[0].RequestID != "req-1" {
		t.Fatalf("snapshot was mutated, request ID = %q", resp2.Records[0].RequestID)
	}
	if v, _ := resp2.Records[0].Metadata["completion_tokens"]; v != 5 {
		t.Fatalf("snapshot metadata was mutated, got %v", v)
	}
	if resp2.Records[0].Status != "pending" {
		t.Fatalf("snapshot status was mutated, got %q", resp2.Records[0].Status)
	}

	// Verify internal state directly.
	r2, ok := c.GetByRequestID("req-1")
	if !ok {
		t.Fatal("expected to find request")
	}
	if r2.RequestID != "req-1" {
		t.Fatalf("internal state was mutated, request ID = %q", r2.RequestID)
	}
	assertSizeConsistent(t, c)
}

func TestQueryPageNoFreeTextRetention(t *testing.T) {
	c := newTestCorrelator(t)

	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
	c.CorrelateActivity("act-1", "req-1", fixtureTime.Add(time.Minute), map[string]any{
		"completion_tokens": 5,
		"finish_reason":     "stop",
	})

	resp, err := c.QueryPage(QueryFilter{}, PageRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(resp.Records))
	}

	// Verify only allowlisted structured data is present.
	r := resp.Records[0]
	if _, ok := r.Metadata["prompt"]; ok {
		t.Fatal("prompt text should not be retained")
	}
	if _, ok := r.Metadata["reasoning"]; ok {
		t.Fatal("reasoning text should not be retained")
	}
	if v, _ := r.Metadata["completion_tokens"]; v != 5 {
		t.Fatalf("completion_tokens = %v, want 5", v)
	}
	if v, _ := r.Metadata["finish_reason"]; v != "stop" {
		t.Fatalf("finish_reason = %v, want stop", v)
	}
	assertSizeConsistent(t, c)
}

func TestQueryPageLimitClamping(t *testing.T) {
	c := newTestCorrelator(t)

	for i := 0; i < 3; i++ {
		c.RegisterRequest(fmt.Sprintf("req-%d", i), "backend-A", "model-1", fixtureTime.Add(time.Duration(i)*time.Minute))
	}

	// Limit larger than max should be capped.
	resp, err := c.QueryPage(QueryFilter{}, PageRequest{Limit: 9999})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Records) != 3 {
		t.Fatalf("expected 3 records with large limit, got %d", len(resp.Records))
	}
	if resp.Cursor != "" {
		t.Fatal("expected empty cursor when all records fit")
	}

	// Limit 0 should default to defaultQueryLimit.
	resp, err = c.QueryPage(QueryFilter{}, PageRequest{Limit: 0})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Records) != 3 {
		t.Fatalf("expected 3 records with limit 0, got %d", len(resp.Records))
	}
	assertSizeConsistent(t, c)
}

func TestQueryPageFilter(t *testing.T) {
	c := newTestCorrelator(t)

	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
	c.RegisterRequest("req-2", "backend-B", "model-2", fixtureTime.Add(time.Minute))
	c.RegisterRequest("req-3", "backend-A", "model-1", fixtureTime.Add(2*time.Minute))

	// Filter by backend.
	resp, err := c.QueryPage(QueryFilter{BackendID: "backend-A"}, PageRequest{Limit: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(resp.Records))
	}
	if resp.Records[0].RequestID != "req-3" {
		t.Fatalf("record = %q, want req-3 (newest first)", resp.Records[0].RequestID)
	}

	// Resume: should get req-1.
	resp2, err := c.QueryPage(QueryFilter{BackendID: "backend-A"}, PageRequest{Cursor: resp.Cursor, Limit: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp2.Records) != 1 {
		t.Fatalf("expected 1 record on resume, got %d", len(resp2.Records))
	}
	if resp2.Records[0].RequestID != "req-1" {
		t.Fatalf("record = %q, want req-1", resp2.Records[0].RequestID)
	}

	// Last page: cursor should be empty.
	if resp2.Cursor != "" {
		t.Fatal("expected empty cursor on last page")
	}
	assertSizeConsistent(t, c)
}

func TestQueryPageTimeRangeFilter(t *testing.T) {
	c := newTestCorrelator(t)

	t1 := fixtureTime
	t2 := fixtureTime.Add(5 * time.Minute)
	t3 := fixtureTime.Add(10 * time.Minute)

	c.RegisterRequest("req-1", "backend-A", "model-1", t1)
	c.RegisterRequest("req-2", "backend-A", "model-2", t2)
	c.RegisterRequest("req-3", "backend-A", "model-3", t3)

	// Time range filter.
	resp, err := c.QueryPage(QueryFilter{TimeStart: t1, TimeEnd: t2}, PageRequest{Limit: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(resp.Records))
	}
	if resp.Records[0].RequestID != "req-2" {
		t.Fatalf("record = %q, want req-2", resp.Records[0].RequestID)
	}
	assertSizeConsistent(t, c)
}

// entropyFailingReader is an entropy reader that always fails, used to test
// fail-closed entropy failure behavior.
type entropyFailingReader struct{}

func (entropyFailingReader) Read(p []byte) (int, error) {
	return 0, errors.New("entropy failure")
}

// TestQueryPageTamperedOffset verifies that changing the offset in a valid
// cursor invalidates the HMAC and is rejected.
func TestQueryPageTamperedOffset(t *testing.T) {
	c := newTestCorrelator(t)

	for i := 0; i < 3; i++ {
		c.RegisterRequest(fmt.Sprintf("req-%d", i), "backend-A", "model-1", fixtureTime.Add(time.Duration(i)*time.Minute))
	}

	resp, err := c.QueryPage(QueryFilter{}, PageRequest{Limit: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(resp.Records))
	}
	if resp.Cursor == "" {
		t.Fatal("expected non-empty cursor")
	}

	// Tamper with the offset in the cursor.
	tampered := resp.Cursor + "X"
	_, err = c.QueryPage(QueryFilter{}, PageRequest{Cursor: tampered, Limit: 1})
	if err != nil {
		if !errors.Is(err, ErrSnapshotNotFound) {
			t.Fatalf("expected ErrSnapshotNotFound for tampered cursor, got %v", err)
		}
	} else {
		t.Fatal("expected ErrSnapshotNotFound for tampered cursor, got nil error")
	}
	assertSizeConsistent(t, c)
}

// TestQueryPageChangedFilterOnResume verifies that presenting a different
// filter on resume is rejected.
func TestQueryPageChangedFilterOnResume(t *testing.T) {
	c := newTestCorrelator(t)

	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
	c.RegisterRequest("req-2", "backend-A", "model-1", fixtureTime.Add(time.Minute))
	c.RegisterRequest("req-3", "backend-B", "model-2", fixtureTime.Add(2*time.Minute))

	// Snapshot with backend-A filter (2 records -> non-empty cursor).
	resp, err := c.QueryPage(QueryFilter{BackendID: "backend-A"}, PageRequest{Limit: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(resp.Records))
	}
	if resp.Cursor == "" {
		t.Fatal("expected non-empty cursor")
	}

	// Resume with a different filter -> rejected.
	_, err = c.QueryPage(QueryFilter{BackendID: "backend-B"}, PageRequest{Cursor: resp.Cursor, Limit: 1})
	if err != nil {
		if !errors.Is(err, ErrSnapshotNotFound) {
			t.Fatalf("expected ErrSnapshotNotFound for changed filter, got %v", err)
		}
	} else {
		t.Fatal("expected ErrSnapshotNotFound for changed filter, got nil error")
	}
	assertSizeConsistent(t, c)
}

// TestQueryPageChangedLimitOnResume verifies that presenting a different
// limit on resume is rejected.
func TestQueryPageChangedLimitOnResume(t *testing.T) {
	c := newTestCorrelator(t)

	for i := 0; i < 3; i++ {
		c.RegisterRequest(fmt.Sprintf("req-%d", i), "backend-A", "model-1", fixtureTime.Add(time.Duration(i)*time.Minute))
	}

	// Snapshot with Limit: 1.
	resp, err := c.QueryPage(QueryFilter{}, PageRequest{Limit: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(resp.Records))
	}

	// Resume with a different limit -> rejected.
	_, err = c.QueryPage(QueryFilter{}, PageRequest{Cursor: resp.Cursor, Limit: 2})
	if err != nil {
		if !errors.Is(err, ErrSnapshotNotFound) {
			t.Fatalf("expected ErrSnapshotNotFound for changed limit, got %v", err)
		}
	} else {
		t.Fatal("expected ErrSnapshotNotFound for changed limit, got nil error")
	}
	assertSizeConsistent(t, c)
}

// TestQueryPageEntropyFailure verifies that entropy failure fails closed with
// ErrSnapshotEntropy instead of a predictable fallback.
func TestQueryPageEntropyFailure(t *testing.T) {
	c := New()
	c.SetClock(&advanceableClock{now: fixtureTime})
	c.entropyReader = entropyFailingReader{}

	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
	c.RegisterRequest("req-2", "backend-A", "model-1", fixtureTime)

	_, err := c.QueryPage(QueryFilter{}, PageRequest{Limit: 1})
	if err != nil {
		if !errors.Is(err, ErrSnapshotEntropy) {
			t.Fatalf("expected ErrSnapshotEntropy, got %v", err)
		}
	} else {
		t.Fatal("expected ErrSnapshotEntropy, got nil error")
	}
}

// TestQueryPageOversizeSnapshot verifies that a snapshot exceeding the page
// cap is rejected with ErrSnapshotTooLarge instead of silently truncating.
func TestQueryPageOversizeSnapshot(t *testing.T) {
	c := newTestCorrelator(t)
	c.snapshotPageCap = 1 // only 1 record per snapshot

	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
	c.RegisterRequest("req-2", "backend-A", "model-1", fixtureTime.Add(time.Minute))

	_, err := c.QueryPage(QueryFilter{}, PageRequest{})
	if err != nil {
		if !errors.Is(err, ErrSnapshotTooLarge) {
			t.Fatalf("expected ErrSnapshotTooLarge, got %v", err)
		}
	} else {
		t.Fatal("expected ErrSnapshotTooLarge, got nil error")
	}
	assertSizeConsistent(t, c)
}

// TestQueryPageOnePageStoresNoSnapshot verifies that a result that fits in a
// single page is returned directly without storing a snapshot or consuming
// entropy, and that no cursor is produced.
func TestQueryPageOnePageStoresNoSnapshot(t *testing.T) {
	c := newTestCorrelator(t)

	for i := 0; i < 3; i++ {
		c.RegisterRequest(fmt.Sprintf("req-%d", i), "backend-A", "model-1", fixtureTime.Add(time.Duration(i)*time.Minute))
	}

	resp, err := c.QueryPage(QueryFilter{}, PageRequest{Limit: 50})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(resp.Records))
	}
	if resp.Cursor != "" {
		t.Fatal("expected empty cursor when all records fit in one page")
	}
	// No snapshot is stored for a one-page result.
	if c.snapshotCount != 0 {
		t.Fatalf("expected no snapshot stored for one-page result, got %d", c.snapshotCount)
	}
	assertSizeConsistent(t, c)
}

// forgeCursor decodes a cursor, flips a byte inside the given byte region of
// the fixed-width payload, and re-encodes it. The HMAC is computed over the
// original payload, so any flipped byte invalidates authentication.
func forgeCursor(t *testing.T, cursor string, region [2]int) string {
	t.Helper()
	decoded, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		t.Fatalf("could not decode cursor: %v", err)
	}
	if len(decoded) != cursorPayloadSize {
		t.Fatalf("unexpected cursor payload size %d, want %d", len(decoded), cursorPayloadSize)
	}
	decoded[region[0]] ^= 0xFF
	return base64.RawURLEncoding.EncodeToString(decoded)
}

// TestQueryPageForgedOffset verifies that tampering with the offset bytes of
// a valid cursor invalidates the HMAC and is rejected.
func TestQueryPageForgedOffset(t *testing.T) {
	c := newTestCorrelator(t)

	for i := 0; i < 3; i++ {
		c.RegisterRequest(fmt.Sprintf("req-%d", i), "backend-A", "model-1", fixtureTime.Add(time.Duration(i)*time.Minute))
	}

	resp, err := c.QueryPage(QueryFilter{}, PageRequest{Limit: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(resp.Records))
	}
	if resp.Cursor == "" {
		t.Fatal("expected non-empty cursor")
	}

	// Flip a byte inside the offset region (bytes 16:24 of the payload).
	forged := forgeCursor(t, resp.Cursor, [2]int{16, 20})
	_, err = c.QueryPage(QueryFilter{}, PageRequest{Cursor: forged, Limit: 1})
	if err != nil {
		if !errors.Is(err, ErrSnapshotNotFound) {
			t.Fatalf("expected ErrSnapshotNotFound for forged offset, got %v", err)
		}
		return
	}
	t.Fatal("expected ErrSnapshotNotFound for forged offset, got nil error")
}

// TestQueryPageForgedID verifies that tampering with the snapshot ID bytes of
// a valid cursor invalidates the HMAC and is rejected.
func TestQueryPageForgedID(t *testing.T) {
	c := newTestCorrelator(t)

	for i := 0; i < 3; i++ {
		c.RegisterRequest(fmt.Sprintf("req-%d", i), "backend-A", "model-1", fixtureTime.Add(time.Duration(i)*time.Minute))
	}

	resp, err := c.QueryPage(QueryFilter{}, PageRequest{Limit: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(resp.Records))
	}
	if resp.Cursor == "" {
		t.Fatal("expected non-empty cursor")
	}

	// Flip a byte inside the random ID region (bytes 0:16 of the payload).
	forged := forgeCursor(t, resp.Cursor, [2]int{0, 8})
	_, err = c.QueryPage(QueryFilter{}, PageRequest{Cursor: forged, Limit: 1})
	if err != nil {
		if !errors.Is(err, ErrSnapshotNotFound) {
			t.Fatalf("expected ErrSnapshotNotFound for forged ID, got %v", err)
		}
		return
	}
	t.Fatal("expected ErrSnapshotNotFound for forged ID, got nil error")
}

// TestQueryPageExpiredExactBoundary verifies that a snapshot expires exactly
// at the TTL boundary (now == createdAt + TTL is expired).
func TestQueryPageExpiredExactBoundary(t *testing.T) {
	clock := &advanceableClock{now: fixtureTime}
	c := New()
	c.SetClock(clock)
	c.snapshotTTL = time.Second

	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
	c.RegisterRequest("req-2", "backend-A", "model-1", fixtureTime.Add(time.Minute))

	resp, err := c.QueryPage(QueryFilter{}, PageRequest{Limit: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(resp.Records))
	}
	if resp.Cursor == "" {
		t.Fatal("expected non-empty cursor")
	}

	// Advance clock by exactly the TTL: the boundary instant is expired.
	clock.Advance(c.snapshotTTL)

	_, err = c.QueryPage(QueryFilter{}, PageRequest{Cursor: resp.Cursor, Limit: 1})
	if err != nil {
		if !errors.Is(err, ErrSnapshotExpired) {
			t.Fatalf("expected ErrSnapshotExpired at TTL boundary, got %v", err)
		}
		return
	}
	t.Fatal("expected ErrSnapshotExpired at TTL boundary, got nil error")
}

// TestQueryPageFilterOversized verifies that a filter with an oversized
// string field is rejected before hashing and no raw filter is retained.
func TestQueryPageFilterOversized(t *testing.T) {
	c := newTestCorrelator(t)

	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)

	oversized := QueryFilter{ModelID: strings.Repeat("a", 257)}
	_, err := c.QueryPage(oversized, PageRequest{})
	if err != nil {
		if !errors.Is(err, ErrSnapshotNotFound) {
			t.Fatalf("expected ErrSnapshotNotFound for oversized filter, got %v", err)
		}
		return
	}
	t.Fatal("expected ErrSnapshotNotFound for oversized filter, got nil error")
	assertSizeConsistent(t, c)
}

// TestQueryPageFilterInvalidUTF8 verifies that a filter with an invalid
// UTF8 string field is rejected before hashing.
func TestQueryPageFilterInvalidUTF8(t *testing.T) {
	c := newTestCorrelator(t)

	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)

	invalidUTF8 := QueryFilter{ModelID: "\xff\xfe"}
	_, err := c.QueryPage(invalidUTF8, PageRequest{})
	if err != nil {
		if !errors.Is(err, ErrSnapshotNotFound) {
			t.Fatalf("expected ErrSnapshotNotFound for invalid UTF8 filter, got %v", err)
		}
		return
	}
	t.Fatal("expected ErrSnapshotNotFound for invalid UTF8 filter, got nil error")
	assertSizeConsistent(t, c)
}

// TestQueryPageIDCollisionNoOverwrite verifies that an injected entropy
// source returning a duplicate snapshot ID does not overwrite an existing
// snapshot or misaccount its size.
func TestQueryPageIDCollisionNoOverwrite(t *testing.T) {
	c := New()
	c.SetClock(&advanceableClock{now: fixtureTime})
	// Deterministic entropy that always returns the same 16-byte ID.
	c.entropyReader = &fixedIDReader{id: []byte("0123456789abcdef")}

	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
	c.RegisterRequest("req-2", "backend-A", "model-1", fixtureTime.Add(time.Minute))

	// First page: a multi-page snapshot is stored with the fixed ID.
	resp1, err := c.QueryPage(QueryFilter{}, PageRequest{Limit: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp1.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(resp1.Records))
	}
	if resp1.Cursor == "" {
		t.Fatal("expected non-empty cursor")
	}
	if c.snapshotCount != 1 {
		t.Fatalf("expected 1 snapshot after first page, got %d", c.snapshotCount)
	}

	// Register more records and request a new snapshot (empty cursor). The
	// injected entropy returns the same ID, colliding with the existing
	// snapshot. It must not overwrite or misaccount it.
	c.RegisterRequest("req-3", "backend-A", "model-1", fixtureTime.Add(2*time.Minute))
	c.RegisterRequest("req-4", "backend-A", "model-1", fixtureTime.Add(3*time.Minute))
	_, err = c.QueryPage(QueryFilter{}, PageRequest{Limit: 1})
	if err != nil {
		if !errors.Is(err, ErrSnapshotEntropy) {
			t.Fatalf("expected ErrSnapshotEntropy on ID collision, got %v", err)
		}
	} else {
		t.Fatal("expected ErrSnapshotEntropy on ID collision, got nil error")
	}
	// Snapshot count must remain 1 (no overwrite, no misaccounting).
	if c.snapshotCount != 1 {
		t.Fatalf("expected snapshot count to remain 1 after ID collision, got %d", c.snapshotCount)
	}

	// The original snapshot is still intact and resumable.
	resp2, err := c.QueryPage(QueryFilter{}, PageRequest{Cursor: resp1.Cursor, Limit: 1})
	if err != nil {
		t.Fatalf("unexpected error on resume: %v", err)
	}
	if len(resp2.Records) != 1 {
		t.Fatalf("expected 1 record on resume, got %d", len(resp2.Records))
	}
	if c.snapshotCount != 1 {
		t.Fatalf("expected snapshot count to remain 1, got %d", c.snapshotCount)
	}
	assertSizeConsistent(t, c)
}

// fixedIDReader is a deterministic entropy reader that returns the same
// 16-byte ID on every read, used to exercise ID collision handling.
type fixedIDReader struct {
	id []byte
}

func (r *fixedIDReader) Read(p []byte) (int, error) {
	if len(p) < snapshotIDSize {
		return 0, ErrSnapshotEntropy
	}
	copy(p, r.id)
	return snapshotIDSize, nil
}

// TestQueryPageCursorZeroID verifies that a crypto-random snapshot ID
// containing NUL bytes (including an all-zero ID) round-trips through the
// fixed-width binary cursor layout without being split apart, the way the old
// separator-based encoding could fail.
func TestQueryPageCursorZeroID(t *testing.T) {
	c := New()
	c.SetClock(&advanceableClock{now: fixtureTime})
	// All-zero 16-byte ID: embedded NULs that the old layout would split on.
	c.entropyReader = &fixedIDReader{id: make([]byte, snapshotIDSize)}

	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
	c.RegisterRequest("req-2", "backend-A", "model-1", fixtureTime.Add(time.Minute))

	// Multi-page snapshot stored with the zero ID.
	resp1, err := c.QueryPage(QueryFilter{}, PageRequest{Limit: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp1.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(resp1.Records))
	}
	if resp1.Cursor == "" {
		t.Fatal("expected non-empty cursor")
	}
	if c.snapshotCount != 1 {
		t.Fatalf("expected 1 snapshot, got %d", c.snapshotCount)
	}

	// Resume with the zero-ID cursor.
	resp2, err := c.QueryPage(QueryFilter{}, PageRequest{Cursor: resp1.Cursor, Limit: 1})
	if err != nil {
		t.Fatalf("unexpected error on resume: %v", err)
	}
	if len(resp2.Records) != 1 {
		t.Fatalf("expected 1 record on resume, got %d", len(resp2.Records))
	}
	if resp2.Records[0].RequestID != "req-1" {
		t.Fatalf("resumed record = %q, want req-1", resp2.Records[0].RequestID)
	}
	assertSizeConsistent(t, c)
}

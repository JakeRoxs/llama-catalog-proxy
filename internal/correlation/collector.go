package correlation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

// ErrActivityEpochChanged is returned by Refresh when the collection epoch
// changed (via ResetEpoch) while HTTP requests were in flight. The in-flight
// results are discarded and nothing is repopulated under the stale epoch.
var ErrActivityEpochChanged = errors.New("activity epoch changed")

// activityKey scopes a record by backend, collection epoch, and upstream ID.
// The same numeric upstream ID may be reused across backends or epochs; the
// epoch and backend in the key keep those observations distinct. The epoch is
// a collector-local collection scope, not proof of an upstream restart.
type activityKey struct {
	backendID  string
	epoch      int64
	activityID string
}

// dedupKey identifies an upstream activity by backend and upstream ID for
// idempotent replay across repeated Refresh calls.
type dedupKey struct {
	backendID  string
	activityID string
}

// backendInfo holds the configured backend and its parsed URL.
type backendInfo struct {
	config config.Backend
	url    *url.URL
}

// backendJob pairs a configured backend ID with its info so workers process
// backends in a deterministic order.
type backendJob struct {
	id string
	b  *backendInfo
}

// backendResult is the outcome of collecting one backend.
type backendResult struct {
	id      string
	records []*ActivityRecord
	state   BackendState
}

// ActivityCollector collects normalized, bounded activity snapshots from
// llama-swap backends configured with Diagnostics == "llama_swap". It is safe
// for concurrent use and issues no requests on construction. The store is
// keyed by (backendID, collection epoch, upstream activity ID) and bounded by
// count, estimated size, and age (evicted on read). Refresh is serialized by a
// single-unit gate and runs at most four concurrent backend fetches.
type ActivityCollector struct {
	mu                sync.Mutex
	client            *http.Client
	now               func() time.Time
	epoch             int64
	refreshGate       chan struct{}
	backends          map[string]*backendInfo
	store             map[activityKey]*ActivityRecord
	dedup             map[dedupKey]activityKey
	backendState      map[string]BackendState
	modelAllowlist    map[string]bool
	maxRecords        int
	maxRowsPerBackend int
	maxAge            time.Duration
	maxSize           int64
	totalSize         int64
	timeout           time.Duration
	maxResponseBytes  int64
}

// NewActivityCollector returns a collector for the given backends. Only
// backends with Diagnostics == "llama_swap" are collected; all others are
// skipped and make no requests. Construction copies configuration and issues
// no requests. The collection epoch starts at one. Known models (if any) are
// the allowlist used to resolve model labels; unknown models are reported as
// "unknown".
func NewActivityCollector(backends map[string]config.Backend, opts ...Option) *ActivityCollector {
	c := &ActivityCollector{
		client: &http.Client{
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		now:               time.Now,
		epoch:             1,
		refreshGate:       make(chan struct{}, 1),
		backends:          make(map[string]*backendInfo, len(backends)),
		store:             make(map[activityKey]*ActivityRecord),
		dedup:             make(map[dedupKey]activityKey),
		backendState:      make(map[string]BackendState),
		maxRecords:        maxRecords,
		maxRowsPerBackend: maxRowsPerBackend,
		maxAge:            defaultMaxAge,
		maxSize:           defaultMaxSize,
		timeout:           defaultTimeout,
		maxResponseBytes:  maxResponseBytes,
		modelAllowlist:    make(map[string]bool),
	}
	for id, cfg := range backends {
		if !validIdentifier(id) {
			continue
		}
		if cfg.Diagnostics != "llama_swap" {
			// Unknown/disabled backends make no requests.
			c.backendState[id] = BackendState{State: "disabled", Reason: "not_enabled"}
			continue
		}
		u, err := url.Parse(cfg.URL)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.RawQuery != "" || u.Fragment != "" {
			c.backendState[id] = BackendState{State: "unsupported", Reason: "invalid_url"}
			continue
		}
		headers := make(map[string]string, len(cfg.Headers))
		for key, value := range cfg.Headers {
			headers[key] = value
		}
		cfg.Headers = headers
		c.backends[id] = &backendInfo{config: cfg, url: u}
		c.backendState[id] = BackendState{State: "unknown", Reason: "not_collected"}
	}
	for _, opt := range opts {
		opt(c)
	}
	c.maxRecords = min(c.maxRecords, maxRecords)
	c.maxRowsPerBackend = min(c.maxRowsPerBackend, 999)
	c.maxAge = min(c.maxAge, defaultMaxAge)
	c.maxSize = min(c.maxSize, defaultMaxSize)
	c.timeout = min(c.timeout, defaultTimeout)
	c.maxResponseBytes = min(c.maxResponseBytes, maxResponseBytes)
	// Enforce the no-redirect policy on the final client so a client supplied
	// by WithClient cannot bypass redirect refusal.
	clone := *c.client
	clone.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	c.client = &clone
	return c
}

// Epoch returns the current collection epoch. It is a collector-local scope
// counter, bumped by ResetEpoch. It is not proof of an upstream process
// restart; epoch changes are observable only as collection scope.
func (c *ActivityCollector) Epoch() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.epoch
}

// ResetEpoch bumps the collection epoch and clears previous-epoch records and
// dedup. This is a collector-local scope reset: reused upstream IDs become
// re-collectable under the new epoch. It never labels an upstream restart and
// does not invalidate the collector itself.
func (c *ActivityCollector) ResetEpoch() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.epoch++
	c.store = make(map[activityKey]*ActivityRecord)
	c.dedup = make(map[dedupKey]activityKey)
	c.totalSize = 0
	for id := range c.backends {
		c.backendState[id] = BackendState{State: "unknown", Reason: "not_collected"}
	}
}

// Refresh collects activity snapshots from all enabled backends and returns
// the full normalized snapshot (sorted deterministically) after dedup. Calls
// are serialized by a gate: a waiting caller honors context cancellation and
// returns ctx.Err() rather than blocking forever. The epoch is captured before
// any HTTP request; if ResetEpoch bumps the epoch while requests are in
// flight, the stale results are discarded and ErrActivityEpochChanged is
// returned after the workers join, with no repopulation. Records are returned
// as defensive copies. Repeated Refresh calls dedup by (backendID, activityID):
// an activity already observed is not re-stored (idempotent replay). Same
// numeric IDs reused across backend/epoch are distinct keys.
func (c *ActivityCollector) Refresh(ctx context.Context) ([]*ActivityRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Serialize callers. Honor cancellation while waiting for the gate.
	select {
	case c.refreshGate <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-c.refreshGate }()
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Snapshot the epoch and the sorted configured backend jobs under the lock,
	// then release it so collection can proceed without holding the lock across
	// HTTP.
	c.mu.Lock()
	epoch := c.epoch
	jobs := make([]backendJob, 0, len(c.backends))
	for id, b := range c.backends {
		jobs = append(jobs, backendJob{id: id, b: b})
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].id < jobs[j].id })
	c.mu.Unlock()

	// Run the actual bounded worker pool: min(4, len(jobs)) goroutines reading
	// from a bounded job channel and sending to a bounded result channel.
	n := 4
	if n > len(jobs) {
		n = len(jobs)
	}
	jobChan := make(chan backendJob, 4)
	resultChan := make(chan backendResult, 4)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobChan {
				records, state := c.collect(ctx, job.id, job.b)
				resultChan <- backendResult{id: job.id, records: records, state: state}
			}
		}()
	}
	go func() {
		defer close(resultChan)
	feed:
		for _, job := range jobs {
			select {
			case jobChan <- job:
			case <-ctx.Done():
				break feed
			}
		}
		close(jobChan)
		wg.Wait()
	}()

	// Merge each result into the store under the lock. If the epoch changed
	// while requests were in flight, discard everything and report it without
	// repopulating.
	epochChanged := false
	for r := range resultChan {
		c.mu.Lock()
		if c.epoch != epoch {
			epochChanged = true
			c.mu.Unlock()
			continue
		}
		if ctx.Err() != nil {
			c.mu.Unlock()
			continue
		}
		state := r.state
		for _, rec := range r.records {
			rec.Epoch = epoch
			dk := dedupKey{backendID: r.id, activityID: rec.ActivityID}
			if existing, ok := c.dedup[dk]; ok {
				if existing.epoch == epoch {
					// Replay within the same epoch. Detect identity conflict
					// on conflicting timestamps without merging.
					storedKey := activityKey{backendID: r.id, epoch: epoch, activityID: rec.ActivityID}
					stored := c.store[storedKey]
					if stored != nil && !stored.Timestamp.Equal(rec.Timestamp) {
						stored.IdentityConflict = true
						if state.Coverage == "" || state.Coverage == "complete" {
							state.Coverage = "partial"
						}
					}
					continue
				}
				// Observed in an earlier epoch: idempotent replay, keep the
				// first observation (already stored under its epoch).
				continue
			}
			c.store[activityKey{backendID: r.id, epoch: epoch, activityID: rec.ActivityID}] = rec
			c.dedup[dk] = activityKey{backendID: r.id, epoch: epoch, activityID: rec.ActivityID}
			c.totalSize += estimateActivitySize(rec)
		}
		c.backendState[r.id] = state
		// Bound retained and staged rows independently of the number of backends.
		c.evictCountLocked()
		c.evictSizeLocked()
		c.mu.Unlock()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	if epochChanged || c.epoch != epoch {
		c.mu.Unlock()
		return nil, ErrActivityEpochChanged
	}
	// Count and size retention on write.
	c.evictCountLocked()
	c.evictSizeLocked()
	// Return defensive copies after releasing the lock.
	snapshot := c.snapshotLocked()
	c.mu.Unlock()
	return snapshot, nil
}

// collect fetches one page of activity from a backend and normalizes it. It
// never follows redirects, never leaks credentials, and bounds the response
// body and row count. The only allowed upstream path is
// /api/metrics/activity with a bounded limit and page. Complexity is kept low
// by delegating to pure helpers: request building, status classification,
// bounded page decode/validation, and row normalization.
func (c *ActivityCollector) collect(ctx context.Context, id string, b *backendInfo) ([]*ActivityRecord, BackendState) {
	state := BackendState{State: "unknown", Reason: "not_collected", ObservedAt: c.now(), LastAttemptAt: c.now()}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	req, err := c.buildActivityRequest(ctx, b)
	if err != nil {
		state.State, state.Reason = "unsupported", "invalid_request"
		return nil, state
	}
	response, err := c.client.Do(req)
	if err != nil {
		state.State, state.Reason = "unavailable", "request_failed"
		state.HTTPStatus = 0
		return nil, state
	}
	defer response.Body.Close()
	state.HTTPStatus = response.StatusCode
	if st, reason, stop := classifyActivityStatus(response.StatusCode); stop {
		state.State, state.Reason = st, reason
		return nil, state
	}
	page, err := readAndDecodeActivityPage(response.Body, c.maxResponseBytes)
	if err != nil {
		state.State, state.Reason = activityErrorState(err)
		return nil, state
	}
	records, pageState := c.normalizeActivityPage(id, page.Data, c.maxRowsPerBackend, page.Total)
	state.State = pageState.State
	state.Reason = pageState.Reason
	state.Rows = pageState.Rows
	state.Coverage = pageState.Coverage
	state.Truncated = pageState.Truncated
	return records, state
}

// buildActivityRequest builds the fixed, allowlisted request for one backend.
// Only a bounded limit and page=1 are requested. Config headers are applied
// only when not already set on the request; the API key is added only when no
// Authorization or X-Api-Key header is present. Redirects are refused by the
// client's CheckRedirect policy, so this builder never enables following them.
func (c *ActivityCollector) buildActivityRequest(ctx context.Context, b *backendInfo) (*http.Request, error) {
	u := b.url.JoinPath("api/metrics/activity")
	// Fixed allowlist: only a bounded limit and page are requested.
	q := u.Query()
	q.Set("limit", strconv.Itoa(c.maxRowsPerBackend))
	q.Set("page", "1")
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	for key, value := range b.config.Headers {
		if req.Header.Get(key) == "" {
			req.Header.Set(key, value)
		}
	}
	if b.config.APIKey != "" && req.Header.Get("Authorization") == "" && req.Header.Get("X-Api-Key") == "" {
		req.Header.Set("Authorization", "Bearer "+b.config.APIKey)
		req.Header.Set("X-Api-Key", b.config.APIKey)
	}
	return req, nil
}

// classifyActivityStatus maps an HTTP status to the backend state/reason and
// whether collection should stop. A 200 continues; every other status stops
// with a documented state/reason. Redirects are refused (never followed) and
// reported as redirect_refused rather than followed. It delegates to the
// shared classifyBackendStatus, supplying the activity collector's state
// strings.
func classifyActivityStatus(code int) (state, reason string, stop bool) {
	return classifyBackendStatus(code, backendStatusLabels{
		unauthorized: "unauthorized",
		unsupported:  "unsupported",
		unknown:      "unknown",
		unavailable:  "unavailable",
	})
}

var (
	errActivityPageTooLarge    = errors.New("activity response body exceeds the configured byte limit")
	errActivityPageMalformed   = errors.New("activity page data is missing or null")
	errActivityPageInvalidJSON = errors.New("activity page is not valid JSON")
)

// readAndDecodeActivityPage reads a bounded response body, decodes the activity
// page, and validates required fields. The caller is responsible for closing
// the body. It returns a sentinel error for size/malformed/JSON failures so the
// caller can map each to the exact state/reason; other read errors map to the
// generic response_read_failed state.
func readAndDecodeActivityPage(body io.ReadCloser, maxResponseBytes int64) (*activityPage, error) {
	raw, err := io.ReadAll(io.LimitReader(body, maxResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read activity response: %w", err)
	}
	if len(raw) > int(maxResponseBytes) {
		return nil, errActivityPageTooLarge
	}
	var page activityPage
	if err := json.Unmarshal(raw, &page); err != nil {
		// Keep the documented sentinel in the error chain so the caller can
		// map decode failures to the exact invalid_json state/reason.
		return nil, fmt.Errorf("decode activity page: %w", errActivityPageInvalidJSON)
	}
	// Validate required page fields: data must be an array (non-nil). A bare
	// {} or {"data":null} is malformed.
	if page.Data == nil {
		return nil, errActivityPageMalformed
	}
	return &page, nil
}

// activityErrorState maps a read/decode error from readAndDecodeActivityPage to
// the exact backend state/reason. Read failures are unavailable, size and
// schema failures are unknown.
func activityErrorState(err error) (state, reason string) {
	switch {
	case errors.Is(err, errActivityPageTooLarge):
		return "unknown", "response_too_large"
	case errors.Is(err, errActivityPageMalformed):
		return "unknown", "malformed_page"
	case errors.Is(err, errActivityPageInvalidJSON):
		return "unknown", "invalid_json"
	default:
		return "unavailable", "response_read_failed"
	}
}

// normalizeActivityPage bounds rows by the row cap, normalizes each row, and
// assigns the final collected state. It returns the normalized records and the
// BackendState to assign (State/Reason/Rows/Coverage/Truncated). The caller
// preserves HTTPStatus/ObservedAt/LastAttemptAt from its own state.
func (c *ActivityCollector) normalizeActivityPage(backendID string, rows []activityLogEntry, maxRows, pageTotal int) ([]*ActivityRecord, BackendState) {
	// Bound rows by the row cap; detect truncation vs page.total using the
	// pre-truncation row count.
	truncated := len(rows) > maxRows || pageTotal > len(rows)
	if len(rows) > maxRows {
		rows = rows[:maxRows]
	}
	records := make([]*ActivityRecord, 0, len(rows))
	skipped := 0
	metricsInvalid := false
	for _, entry := range rows {
		rec, ok := c.normalizeActivity(backendID, entry)
		if rec == nil {
			skipped++
			continue
		}
		if !ok {
			metricsInvalid = true
		}
		records = append(records, rec)
	}
	state := BackendState{State: "collected", Reason: "ok", Rows: len(records)}
	if skipped > 0 || metricsInvalid {
		state.Coverage = "partial"
	} else if truncated {
		state.Coverage = "truncated"
		state.Truncated = true
	} else {
		state.Coverage = "complete"
	}
	return records, state
}

// evictCountLocked applies count-based retention. Caller must hold the mutex.
func (c *ActivityCollector) evictCountLocked() {
	if c.maxRecords <= 0 {
		return
	}
	if len(c.store) <= c.maxRecords {
		return
	}
	records := sortedStore(c.store)
	toEvict := len(records) - c.maxRecords
	for i := 0; i < toEvict; i++ {
		c.evictRecordLocked(records[i])
	}
}

// evictSizeLocked applies size-based retention. Caller must hold the mutex.
func (c *ActivityCollector) evictSizeLocked() {
	if c.maxSize <= 0 {
		return
	}
	if c.totalSize <= c.maxSize {
		return
	}
	records := sortedStore(c.store)
	for i := 0; i < len(records) && c.totalSize > c.maxSize; i++ {
		c.evictRecordLocked(records[i])
	}
}

// evictAgeLocked applies age-based retention. Caller must hold the mutex.
func (c *ActivityCollector) evictAgeLocked() {
	if c.maxAge <= 0 {
		return
	}
	cutoff := c.now().Add(-c.maxAge)
	for _, r := range c.store {
		if r.Timestamp.Before(cutoff) {
			c.evictRecordLocked(r)
		}
	}
}

// evictRecordLocked removes a record from the store. Caller must hold the
// mutex.
func (c *ActivityCollector) evictRecordLocked(r *ActivityRecord) {
	key := activityKey{backendID: r.BackendID, epoch: r.Epoch, activityID: r.ActivityID}
	delete(c.store, key)
	dk := dedupKey{backendID: r.BackendID, activityID: r.ActivityID}
	if existing, ok := c.dedup[dk]; ok && existing == key {
		delete(c.dedup, dk)
	}
	c.totalSize -= estimateActivitySize(r)
}

// estimateActivitySize estimates the retained size of a record. Pointer
// fields account for the pointer itself (8 bytes) plus the value (8 bytes)
// when non-nil.
func estimateActivitySize(r *ActivityRecord) int64 {
	size := int64(len(r.BackendID) + len(r.ActivityID) + len(r.ModelID) + len(r.Task) + len(r.Route) + len(r.Status) + len(r.RequestIDMapping) + len(r.UpstreamGeneration) + len(r.EpochScope))
	// Nine pointer fields: 8 bytes for each pointer.
	size += 8 * 9
	// Non-nil pointer values: 8 bytes each.
	if r.DurationMS != nil {
		size += 8
	}
	if r.InputTokens != nil {
		size += 8
	}
	if r.OutputTokens != nil {
		size += 8
	}
	if r.CachedTokens != nil {
		size += 8
	}
	if r.DraftTokens != nil {
		size += 8
	}
	if r.DraftAcceptedTokens != nil {
		size += 8
	}
	if r.ReasoningTokens != nil {
		size += 8
	}
	if r.PromptTokensPerSecond != nil {
		size += 8
	}
	if r.TokensPerSecond != nil {
		size += 8
	}
	return size + 64
}

// sortedStore returns store records sorted by timestamp then activity ID.
func sortedStore(store map[activityKey]*ActivityRecord) []*ActivityRecord {
	records := make([]*ActivityRecord, 0, len(store))
	for _, r := range store {
		records = append(records, r)
	}
	sort.Slice(records, func(i, j int) bool {
		return activityBefore(records[i], records[j])
	})
	return records
}

func activityBefore(a, b *ActivityRecord) bool {
	if !a.Timestamp.Equal(b.Timestamp) {
		return a.Timestamp.Before(b.Timestamp)
	}
	if a.BackendID != b.BackendID {
		return a.BackendID < b.BackendID
	}
	if a.Epoch != b.Epoch {
		return a.Epoch < b.Epoch
	}
	return a.ActivityID < b.ActivityID
}

// snapshotLocked returns defensive copies of all stored records, sorted
// deterministically, after applying age-based eviction.
func (c *ActivityCollector) snapshotLocked() []*ActivityRecord {
	c.evictAgeLocked()
	records := make([]*ActivityRecord, 0, len(c.store))
	for _, r := range c.store {
		records = append(records, r.Clone())
	}
	sort.Slice(records, func(i, j int) bool {
		return activityBefore(records[i], records[j])
	})
	return records
}

// Stats returns bounded store statistics. Age-based eviction is applied on
// read so idle entries expire. Returned records and backend states are
// defensive copies.
func (c *ActivityCollector) Stats() map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.evictAgeLocked()
	records := c.snapshotLocked()
	states := make(map[string]BackendState, len(c.backendState))
	for k, v := range c.backendState {
		states[k] = v
	}
	return map[string]any{
		"request_id_mapping":  requestIDMappingUnsupported,
		"upstream_generation": upstreamGenerationUnknown,
		"epoch_scope":         epochScopeCollector,
		"record_count":        len(c.store),
		"max_records":         c.maxRecords,
		"max_age":             c.maxAge.String(),
		"max_size":            c.maxSize,
		"total_size":          c.totalSize,
		"epoch":               c.epoch,
		"records":             records,
		"backend_states":      states,
	}
}

// Snapshot returns defensive copies of all stored activity records, sorted
// deterministically.
func (c *ActivityCollector) Snapshot() []*ActivityRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.snapshotLocked()
}

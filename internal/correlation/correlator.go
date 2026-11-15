// Package correlation provides bounded request-to-activity correlation.
// It links proxy request IDs to llama-swap activity events and retains a
// bounded history with explicit retention policies. Retained records exclude
// prompts, response/reasoning text, credentials, raw logs, and commands.
package correlation

import (
	"bytes"
	"crypto/rand"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/timing"
)

// Clock is the time source used for age-based retention. The default is the
// real clock; tests may inject a deterministic clock.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

var defaultClock = realClock{}

// Finite transport status enum (HTTP outcome, not generation completion).
var transportStatusEnum = map[string]bool{
	"pending":           true,
	"in_progress":       true,
	"completed":         true,
	"failed":            true,
	"cancelled":         true,
	"http_success":      true,
	"http_client_error": true,
	"http_server_error": true,
	"http_other":        true,
	"client_canceled":   true,
	"aborted":           true,
	"transport_error":   true,
}

// Finite finish reason enum.
var finishReasonEnum = map[string]bool{
	"stop":    true,
	"length":  true,
	"timeout": true,
	"error":   true,
}

// Allowed metadata keys with typed/bounded values.
var allowedMetadataKeys = map[string]bool{
	"completion_tokens": true, // int64, nonnegative, bounded
	"duration_ms":       true, // int64, nonnegative, bounded
	"status":            true, // string, in transportStatusEnum
	"finish_reason":     true, // string, in finishReasonEnum
}

// maxMetadataValueSize bounds the byte size of any single metadata value.
const maxMetadataValueSize = 4096

// maxMetadataEntries bounds the number of metadata entries retained per record.
const maxMetadataEntries = 8

// maxIdentifierSize bounds the byte size of any identifier field.
const maxIdentifierSize = 256

// maxQueryLimit caps the number of records returned by a single query.
const maxQueryLimit = 1000

// defaultQueryLimit is the default page size when Limit is 0 or negative.
const defaultQueryLimit = 50

// CorrelationRecord links a proxy request to upstream activity events.
type CorrelationRecord struct {
	RequestID        string            `json:"request_id"`
	BackendID        string            `json:"backend_id"`
	ModelID          string            `json:"model_id"`
	ActivityID       string            `json:"activity_id,omitempty"`
	ActivityTime     time.Time         `json:"activity_time"`
	RequestTime      time.Time         `json:"request_time"`
	Phase            string            `json:"phase,omitempty"`
	Status           string            `json:"status,omitempty"`
	Metadata         map[string]any    `json:"metadata,omitempty"`
	ProxyObservation *ProxyObservation `json:"proxy_observation,omitempty"`
}

// Clone returns a defensive copy of the record. Metadata values are shared
// because only simple allowlisted types are permitted. ProxyObservation is
// deep-copied so caller pointer mutations cannot affect the retained value.
func (r *CorrelationRecord) Clone() *CorrelationRecord {
	clone := &CorrelationRecord{
		RequestID:    r.RequestID,
		BackendID:    r.BackendID,
		ModelID:      r.ModelID,
		ActivityID:   r.ActivityID,
		ActivityTime: r.ActivityTime,
		RequestTime:  r.RequestTime,
		Phase:        r.Phase,
		Status:       r.Status,
	}
	if r.ProxyObservation != nil {
		clone.ProxyObservation = r.ProxyObservation.Clone()
	}
	if r.Metadata != nil {
		clone.Metadata = make(map[string]any, len(r.Metadata))
		for k, v := range r.Metadata {
			clone.Metadata[k] = v
		}
	}
	return clone
}

// QueryFilter supports bounded time/model/backend/status/request-ID filters.
type QueryFilter struct {
	TimeStart time.Time `json:"time_start,omitempty"`
	TimeEnd   time.Time `json:"time_end,omitempty"`
	BackendID string    `json:"backend_id,omitempty"`
	ModelID   string    `json:"model_id,omitempty"`
	Status    string    `json:"status,omitempty"`
	RequestID string    `json:"request_id,omitempty"`
	Limit     int       `json:"limit,omitempty"`
	Offset    int       `json:"offset,omitempty"`
}

// Correlator links request IDs to upstream activity events and retains a
// bounded history with explicit retention policies. It holds mutable state and
// is safe for concurrent use.
type Correlator struct {
	mu             sync.Mutex
	clock          Clock
	records        map[string]*CorrelationRecord
	activityIndex  map[string][]string
	retentionCount int
	retentionAge   time.Duration
	retentionSize  int64
	totalSize      int64

	// Snapshot state for bounded mutation-stable pagination.
	snapshots     map[string]*snapshotState // randomID -> snapshot
	snapshotOrder []string                  // insertion order, oldest first
	snapshotCount int
	snapshotSize  int64
	storeID       string

	// Snapshot configuration.
	snapshotCountCap int
	snapshotSizeCap  int64
	snapshotPageCap  int
	snapshotTTL      time.Duration

	// Cursor authentication.
	entropyReader   io.Reader // injected entropy source (defaults to crypto/rand.Reader)
	storeSecret     []byte    // per-store HMAC secret for cursor authentication
	storeSecretOnce sync.Once // lazy secret initialization
	storeSecretErr  error     // entropy error from lazy init
}

// New returns a Correlator with default bounded retention policies.
func New() *Correlator {
	return newCorrelator(defaultClock, 1000, 24*time.Hour, 10*1024*1024)
}

// NewWithRetention returns a Correlator with custom retention policies.
// Zero or negative values disable the corresponding retention policy.
// If all policies are disabled, default bounded policies are retained.
func NewWithRetention(count int, age time.Duration, size int64) *Correlator {
	if count <= 0 && age <= 0 && size <= 0 {
		return newCorrelator(defaultClock, 1000, 24*time.Hour, 10*1024*1024)
	}
	return newCorrelator(defaultClock, count, age, size)
}

func newCorrelator(clock Clock, count int, age time.Duration, size int64) *Correlator {
	if count < 0 {
		count = 0
	}
	if age < 0 {
		age = 0
	}
	if size < 0 {
		size = 0
	}
	return &Correlator{
		clock:          clock,
		records:        make(map[string]*CorrelationRecord),
		activityIndex:  make(map[string][]string),
		retentionCount: count,
		retentionAge:   age,
		retentionSize:  size,
		snapshots:      make(map[string]*snapshotState),
		// storeID is intentionally not initialized here. It is lazily
		// initialized on first snapshot creation (createSnapshot) so ordinary
		// request_history startup (no paging) never requires cursor entropy
		// and never panics when entropy is unavailable.
		snapshotCountCap: snapshotDefaultCountCap,
		snapshotSizeCap:  snapshotDefaultSizeCap,
		snapshotPageCap:  snapshotDefaultPageCap,
		snapshotTTL:      snapshotDefaultTTL,
		entropyReader:    rand.Reader,
	}
}

// SetClock replaces the clock, useful for deterministic tests. nil restores
// the real/default clock.
func (c *Correlator) SetClock(clock Clock) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if clock == nil {
		c.clock = defaultClock
	} else {
		c.clock = clock
	}
}

// WithRetention sets retention policies and immediately re-applies them.
// If all policies are disabled, default bounded policies are retained.
func (c *Correlator) WithRetention(count int, age time.Duration, size int64) *Correlator {
	c.mu.Lock()
	defer c.mu.Unlock()
	if count < 0 {
		count = 0
	}
	if age < 0 {
		age = 0
	}
	if size < 0 {
		size = 0
	}
	// Retain defaults when all disabled.
	if count <= 0 && age <= 0 && size <= 0 {
		count = 1000
		age = 24 * time.Hour
		size = 10 * 1024 * 1024
	}
	c.retentionCount = count
	c.retentionAge = age
	c.retentionSize = size
	c.evictLocked()
	return c
}

// validIdentifier returns true if s is non-empty and within maxIdentifierSize bytes.
func validIdentifier(s string) bool {
	return s != "" && len(s) <= maxIdentifierSize
}

// isNonnegativeSignedInt returns true if v is a signed integer type with a
// nonnegative value.
func isNonnegativeSignedInt(v any) bool {
	switch vv := v.(type) {
	case int:
		return vv >= 0
	case int8:
		return vv >= 0
	case int16:
		return vv >= 0
	case int32:
		return vv >= 0
	case int64:
		return vv >= 0
	default:
		return false
	}
}

// isAllowedMetadataValue checks that a metadata value is typed and bounded
// for the given key. Unknown keys are rejected by allowedMetadataKeys.
func isAllowedMetadataValue(key string, v any) bool {
	switch key {
	case "completion_tokens", "duration_ms":
		switch v.(type) {
		case int, int8, int16, int32, int64:
			return isNonnegativeSignedInt(v)
		case uint, uint8, uint16, uint32, uint64:
			return true // unsigned is always nonnegative
		default:
			return false
		}
	case "status":
		if s, ok := v.(string); ok {
			return transportStatusEnum[s]
		}
		return false
	case "finish_reason":
		if s, ok := v.(string); ok {
			return finishReasonEnum[s]
		}
		return false
	default:
		return false
	}
}

// sanitizeMetadata retains only allowlisted keys with allowed types and
// bounded sizes. Unknown fields are discarded without retaining their names.
func sanitizeMetadata(metadata map[string]any) map[string]any {
	if metadata == nil {
		return nil
	}
	result := make(map[string]any, maxMetadataEntries)
	for k, v := range metadata {
		if !allowedMetadataKeys[k] {
			continue
		}
		if !isAllowedMetadataValue(k, v) {
			continue
		}
		if len(result) >= maxMetadataEntries {
			continue
		}
		result[k] = v
	}
	return result
}

// RegisterRequest records a request with its ID and backend.
// Invalid request IDs are dropped without storing. backendID and modelID may
// be empty (proxy-only usage, unknown placeholders).
func (c *Correlator) RegisterRequest(requestID, backendID, modelID string, startTime time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !validIdentifier(requestID) {
		return
	}

	existing, ok := c.records[requestID]
	if ok {
		oldSize := estimateRecordSize(existing)
		if startTime.Before(existing.RequestTime) {
			existing.RequestTime = startTime
		}
		if existing.BackendID == "" {
			existing.BackendID = backendID
		}
		if existing.ModelID == "" {
			existing.ModelID = modelID
		}
		newSize := estimateRecordSize(existing)
		c.totalSize += int64(newSize - oldSize)
		c.evictLocked()
		return
	}

	record := &CorrelationRecord{
		RequestID:   requestID,
		BackendID:   backendID,
		ModelID:     modelID,
		RequestTime: startTime,
		Phase:       "request",
		Status:      "pending",
	}
	c.records[requestID] = record
	c.totalSize += int64(estimateRecordSize(record))
	c.evictLocked()
}

// CompleteRequest marks a request as complete with a wall duration and
// finite transport status. It does not synthesize an ActivityID.
func (c *Correlator) CompleteRequest(requestID string, duration time.Duration, status string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !transportStatusEnum[status] {
		return
	}

	record, ok := c.records[requestID]
	if !ok {
		return
	}

	oldSize := estimateRecordSize(record)

	record.Status = status
	if duration >= 0 {
		ms := duration.Milliseconds()
		if record.Metadata == nil {
			record.Metadata = make(map[string]any)
		}
		record.Metadata["duration_ms"] = ms
	}

	newSize := estimateRecordSize(record)
	c.totalSize += int64(newSize - oldSize)
	c.evictLocked()
}

// AttachObservation attaches a validated, bounded timing observation to an
// existing record. It does not create records or synthesize ActivityIDs.
// Unknown or expired request IDs return false. Malformed, incomplete, or
// unfinished summaries are rejected without retaining errors or raw text.
// The observation replaces any existing one (reattach: no growth). The
// estimated record size is adjusted OLD->NEW before retention eviction.
func (c *Correlator) AttachObservation(requestID string, summary timing.Summary) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Age eviction on write so expired IDs are rejected consistently with reads.
	c.evictAgeLocked()

	record, ok := c.records[requestID]
	if !ok {
		// Existing record only; unknown ID returns false without creating one.
		return false
	}

	// Project and validate the summary. Malformed/incomplete/unfinished
	// summaries are rejected without retaining errors or raw text.
	obs := projectSummary(summary)
	if obs == nil {
		return false
	}

	// Adjust estimated record size OLD->NEW before retention eviction.
	oldSize := estimateRecordSize(record)
	record.ProxyObservation = obs.Clone()
	newSize := estimateRecordSize(record)
	c.totalSize += int64(newSize - oldSize)

	c.evictLocked()
	// Fail-open: return whether the request record is still retained. The
	// integration ignores a false return (the request completes normally).
	if _, ok := c.records[requestID]; !ok {
		return false
	}
	return true
}

// CorrelateActivity links an upstream activity event to a request.
// Empty or oversized request/activity IDs are dropped rather than synthesized.
func (c *Correlator) CorrelateActivity(activityID, requestID string, activityTime time.Time, metadata map[string]any) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !validIdentifier(requestID) || !validIdentifier(activityID) {
		return
	}

	record, ok := c.records[requestID]
	if !ok {
		// Activity without matching request; retain as orphan with a
		// non-zero RequestTime so age-based retention does not immediately
		// evict it.
		record = &CorrelationRecord{
			RequestID:    requestID,
			ActivityID:   activityID,
			ActivityTime: activityTime,
			RequestTime:  activityTime,
			Phase:        "activity_only",
			Status:       "unmatched",
		}
		sanitized := sanitizeMetadata(metadata)
		if len(sanitized) > 0 {
			record.Metadata = sanitized
		}
		c.records[requestID] = record
		c.totalSize += int64(estimateRecordSize(record))
		// Index for reverse lookups.
		if activityID != "" {
			c.activityIndex[activityID] = append(c.activityIndex[activityID], requestID)
		}
		c.evictLocked()
		return
	}

	// Remove old activity index association if ActivityID changes.
	if record.ActivityID != "" && record.ActivityID != activityID {
		oldIdx := c.activityIndex[record.ActivityID]
		filtered := make([]string, 0, len(oldIdx))
		for _, id := range oldIdx {
			if id != requestID {
				filtered = append(filtered, id)
			}
		}
		if len(filtered) == 0 {
			delete(c.activityIndex, record.ActivityID)
		} else {
			c.activityIndex[record.ActivityID] = filtered
		}
	}

	oldSize := estimateRecordSize(record)

	record.ActivityID = activityID
	record.ActivityTime = activityTime

	// Sanitize and merge metadata.
	sanitized := sanitizeMetadata(metadata)
	if len(sanitized) > 0 {
		if record.Metadata == nil {
			record.Metadata = make(map[string]any)
		}
		for k, v := range sanitized {
			record.Metadata[k] = v
		}
	}

	// Extract status from sanitized metadata only (validated against enum).
	if s, ok := sanitized["status"]; ok {
		if sStr, ok := s.(string); ok && transportStatusEnum[sStr] {
			record.Status = sStr
		}
	}

	newSize := estimateRecordSize(record)
	c.totalSize += int64(newSize - oldSize)

	// Index activity ID -> request IDs for reverse lookups (dedup).
	if activityID != "" {
		existing := c.activityIndex[activityID]
		found := false
		for _, id := range existing {
			if id == requestID {
				found = true
				break
			}
		}
		if !found {
			c.activityIndex[activityID] = append(existing, requestID)
		}
	}

	c.evictLocked()
}

// defensive copies; mutating them does not affect internal state. Age-based
// eviction is applied on read so idle entries expire.
func (c *Correlator) Query(filter QueryFilter) []*CorrelationRecord {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Age eviction on read so idle entries expire.
	c.evictAgeLocked()

	var result []*CorrelationRecord
	for _, record := range c.records {
		if !matchesFilter(record, filter) {
			continue
		}
		result = append(result, record.Clone())
	}

	// Sort by request time (newest first), then request ID for deterministic tie-break.
	sort.Slice(result, func(i, j int) bool {
		if result[i].RequestTime.Equal(result[j].RequestTime) {
			return result[i].RequestID < result[j].RequestID
		}
		return result[i].RequestTime.After(result[j].RequestTime)
	})

	// Apply pagination with bounded limit and safe offset.
	limit := filter.Limit
	if limit <= 0 {
		limit = defaultQueryLimit
	}
	if limit > maxQueryLimit {
		limit = maxQueryLimit
	}
	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}

	if offset >= len(result) {
		return []*CorrelationRecord{}
	}
	end := offset + limit
	if end < offset { // overflow check
		end = len(result)
	}
	if end > len(result) {
		end = len(result)
	}
	return result[offset:end]
}

// matchesFilter checks if a record matches the query filter.
func matchesFilter(record *CorrelationRecord, filter QueryFilter) bool {
	if !filter.TimeStart.IsZero() && record.RequestTime.Before(filter.TimeStart) {
		return false
	}
	if !filter.TimeEnd.IsZero() && record.RequestTime.After(filter.TimeEnd) {
		return false
	}
	if filter.BackendID != "" && record.BackendID != filter.BackendID {
		return false
	}
	if filter.ModelID != "" && record.ModelID != filter.ModelID {
		return false
	}
	if filter.Status != "" && record.Status != filter.Status {
		return false
	}
	if filter.RequestID != "" && record.RequestID != filter.RequestID {
		return false
	}
	return true
}

// estimateRecordSize estimates the serialized size of a record.
func estimateRecordSize(record *CorrelationRecord) int64 {
	size := int64(len(record.RequestID) + len(record.BackendID) + len(record.ModelID) + len(record.ActivityID) + len(record.Phase) + len(record.Status))
	for k, v := range record.Metadata {
		size += int64(len(k))
		switch vv := v.(type) {
		case string:
			size += int64(len(vv))
		case []byte:
			size += int64(len(vv))
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			size += 8
		case float32, float64:
			size += 8
		case bool:
			size += 1
		case nil:
			size += 4
		default:
			size += 16
		}
	}
	// The ProxyObservation has fixed bounded fields; include its estimated
	// size so retention accounts for attached observations.
	if record.ProxyObservation != nil {
		size += estimatedProxyObservationSize(record.ProxyObservation)
	}
	return size + 64 // overhead
}

// evictLocked applies all retention policies (count, age, size). Caller must
// hold the mutex. Oldest records are evicted first.
func (c *Correlator) evictLocked() {
	c.evictCountLocked()
	c.evictAgeLocked()
	c.evictSizeLocked()
}

// evictCountLocked applies count-based retention. Caller must hold the mutex.
func (c *Correlator) evictCountLocked() {
	if c.retentionCount > 0 && len(c.records) > c.retentionCount {
		records := make([]*CorrelationRecord, 0, len(c.records))
		for _, r := range c.records {
			records = append(records, r)
		}
		sort.Slice(records, func(i, j int) bool {
			if records[i].RequestTime.Equal(records[j].RequestTime) {
				return records[i].RequestID < records[j].RequestID
			}
			return records[i].RequestTime.Before(records[j].RequestTime)
		})
		toEvict := len(records) - c.retentionCount
		for i := 0; i < toEvict; i++ {
			c.evictRecordLocked(records[i])
		}
	}
}

// evictAgeLocked applies age-based retention. Caller must hold the mutex.
func (c *Correlator) evictAgeLocked() {
	if c.retentionAge > 0 {
		cutoff := c.clock.Now().Add(-c.retentionAge)
		for _, r := range c.records {
			if r.RequestTime.Before(cutoff) {
				c.evictRecordLocked(r)
			}
		}
	}
}

// evictSizeLocked applies size-based retention. Caller must hold the mutex.
func (c *Correlator) evictSizeLocked() {
	if c.retentionSize > 0 && c.totalSize > c.retentionSize {
		records := make([]*CorrelationRecord, 0, len(c.records))
		for _, r := range c.records {
			records = append(records, r)
		}
		sort.Slice(records, func(i, j int) bool {
			if records[i].RequestTime.Equal(records[j].RequestTime) {
				return records[i].RequestID < records[j].RequestID
			}
			return records[i].RequestTime.Before(records[j].RequestTime)
		})
		for i := 0; i < len(records) && c.totalSize > c.retentionSize; i++ {
			r := records[i]
			c.evictRecordLocked(r) // subtracts totalSize internally
		}
	}
}

// evictRecordLocked removes a record from the correlator. Caller must hold
// the mutex.
func (c *Correlator) evictRecordLocked(record *CorrelationRecord) {
	delete(c.records, record.RequestID)
	if record.ActivityID != "" {
		idx := c.activityIndex[record.ActivityID]
		filtered := make([]string, 0, len(idx))
		for _, id := range idx {
			if id != record.RequestID {
				filtered = append(filtered, id)
			}
		}
		if len(filtered) == 0 {
			delete(c.activityIndex, record.ActivityID)
		} else {
			c.activityIndex[record.ActivityID] = filtered
		}
	}
	c.totalSize -= int64(estimateRecordSize(record))
}

// GetByRequestID returns a defensive copy of the correlation record for a request ID.
// Age-based eviction is applied on read so idle entries expire.
func (c *Correlator) GetByRequestID(requestID string) (*CorrelationRecord, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.evictAgeLocked()
	r, ok := c.records[requestID]
	if !ok {
		return nil, false
	}
	return r.Clone(), true
}

// GetByActivityID returns defensive copies of correlation records for an activity ID.
// Age-based eviction is applied on read so idle entries expire.
func (c *Correlator) GetByActivityID(activityID string) []*CorrelationRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.evictAgeLocked()

	requestIDs := c.activityIndex[activityID]
	var result []*CorrelationRecord
	for _, id := range requestIDs {
		if r, ok := c.records[id]; ok {
			result = append(result, r.Clone())
		}
	}
	return result
}

// QueryPage returns a single page of correlation records using a bounded,
// mutation-stable snapshot. The first call with an empty cursor freezes a
// sorted, filtered snapshot of defensive copies; subsequent pages are served
// from the immutable snapshot. Expired or evicted cursors return a stable
// error rather than an empty success or live fallback. The filter and page
// size are bound to the cursor; changed filters or page sizes are rejected.
func (c *Correlator) QueryPage(filter QueryFilter, pageReq PageRequest) (PageResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if pageReq.Cursor != "" {
		storeSecret, err := c.ensureStoreSecret()
		if err != nil {
			return PageResponse{}, err
		}
		return c.resumeSnapshot(pageReq.Cursor, pageReq, storeSecret, filter)
	}
	return c.createSnapshot(filter, pageReq, nil)
}

// ensureStoreSecret lazily initializes the per-store HMAC secret. It fails
// closed on entropy failure.
func (c *Correlator) ensureStoreSecret() ([]byte, error) {
	c.storeSecretOnce.Do(func() {
		c.storeSecret, c.storeSecretErr = newStoreSecret(c.entropyReader)
	})
	return c.storeSecret, c.storeSecretErr
}

// createSnapshot freezes a sorted, filtered snapshot of defensive copies and
// returns its first page. Caller must hold the mutex. It rejects oversized
// filtered results instead of silently truncating, and rejects snapshots that
// would exceed the size cap before insertion.
func (c *Correlator) createSnapshot(filter QueryFilter, pageReq PageRequest, storeSecret []byte) (PageResponse, error) {
	// Age eviction on read so idle entries expire (consistent with Query).
	c.evictAgeLocked()

	// Validate the filter before hashing; raw filters are never retained.
	filterDigest, err := filterDigest(filter)
	if err != nil {
		return PageResponse{}, err
	}

	// Filter records, bounding the match count during iteration so memory
	// stays bounded and oversized results are rejected explicitly instead of
	// after unbounded collection.
	pageCap := c.snapshotPageCap
	if pageCap <= 0 {
		pageCap = snapshotDefaultPageCap
	}
	var filtered []*CorrelationRecord
	matchCount := 0
	for _, record := range c.records {
		if !matchesFilter(record, filter) {
			continue
		}
		matchCount++
		if matchCount > pageCap {
			return PageResponse{}, ErrSnapshotTooLarge
		}
		filtered = append(filtered, record)
	}

	// Sort by request time (newest first), then request ID for deterministic tie-break.
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].RequestTime.Equal(filtered[j].RequestTime) {
			return filtered[i].RequestID < filtered[j].RequestID
		}
		return filtered[i].RequestTime.After(filtered[j].RequestTime)
	})

	// Clone records into frozen defensive copies.
	records := make([]*CorrelationRecord, 0, len(filtered))
	for _, r := range filtered {
		records = append(records, r.Clone())
	}

	// No records: return empty page without storing a snapshot.
	if len(records) == 0 {
		return PageResponse{Records: []*CorrelationRecord{}}, nil
	}

	// Determine page size.
	pageSize := pageReq.Limit
	if pageSize <= 0 {
		pageSize = defaultQueryLimit
	}
	if pageSize > maxQueryLimit {
		pageSize = maxQueryLimit
	}

	// If the whole result fits in one page, return it directly without
	// storing a snapshot (no cursor, no entropy consumed, no storage).
	if len(records) <= pageSize {
		return PageResponse{Records: records}, nil
	}

	// Compute snapshot size.
	sizeBytes := int64(0)
	for _, r := range records {
		sizeBytes += estimateRecordSize(r)
	}
	sizeBytes += int64(len(records)) * 32 // per-record snapshot overhead

	// Reject if snapshot exceeds size cap before insertion.
	if c.snapshotSizeCap > 0 && sizeBytes > c.snapshotSizeCap {
		return PageResponse{}, ErrSnapshotTooLarge
	}

	// Lazily initialize the per-store ID on first snapshot creation. This
	// defers entropy consumption to paging time so ordinary (non-paging)
	// startup never requires cursor entropy and never panics when entropy is
	// unavailable. The store ID binds snapshots to a store so cross-store
	// cursors are rejected without leaking store identity.
	if c.storeID == "" {
		id, err := newSnapshotID(c.entropyReader)
		if err != nil {
			return PageResponse{}, err
		}
		c.storeID = id
	}

	storeSecret, err = c.ensureStoreSecret()
	if err != nil {
		return PageResponse{}, err
	}

	// Create snapshot.
	randomID, err := newSnapshotID(c.entropyReader)
	if err != nil {
		return PageResponse{}, err
	}

	// Catch ID collision from injected entropy: do not overwrite or
	// misaccount an existing snapshot.
	if _, exists := c.snapshots[randomID]; exists {
		return PageResponse{}, ErrSnapshotEntropy
	}

	snapshot := &snapshotState{
		randomID:     randomID,
		records:      records,
		pageSize:     pageSize,
		storeID:      c.storeID,
		filterDigest: filterDigest,
		createdAt:    c.clock.Now(),
		sizeBytes:    sizeBytes,
	}

	c.snapshots[randomID] = snapshot
	c.snapshotOrder = append(c.snapshotOrder, randomID)
	c.snapshotCount++
	c.snapshotSize += sizeBytes

	// Evict old snapshots to stay within caps.
	c.evictSnapshotsToCapLocked()

	// Return first page.
	page, err := c.snapshotPage(randomID, 0, pageSize, storeSecret)
	if err != nil {
		// If the page fails, remove the snapshot.
		c.evictSnapshotLocked(randomID)
		return PageResponse{}, err
	}
	return page, nil
}

// resumeSnapshot returns the page at the given offset within an existing
// snapshot. Caller must hold the mutex. It validates the HMAC, expiry,
// store ownership, filter binding, and page size binding.
func (c *Correlator) resumeSnapshot(cursor string, pageReq PageRequest, storeSecret []byte, filter QueryFilter) (PageResponse, error) {
	randomID, offset, err := decodeCursor(storeSecret, cursor)
	if err != nil {
		return PageResponse{}, ErrSnapshotNotFound
	}

	snapshot, ok := c.snapshots[randomID]
	if !ok {
		return PageResponse{}, ErrSnapshotNotFound
	}

	// Check expiry at exact boundary (now >= createdAt + TTL is expired).
	if c.snapshotTTL > 0 {
		if !c.clock.Now().Before(snapshot.createdAt.Add(c.snapshotTTL)) {
			c.evictSnapshotLocked(randomID)
			return PageResponse{}, ErrSnapshotExpired
		}
	}

	// Check store ownership; reject cross-store cursors without leaking store identity.
	if snapshot.storeID != c.storeID {
		return PageResponse{}, ErrSnapshotNotFound
	}

	// Validate filter binding; reject changed filters. The digest is
	// computed over the normalized filter, so any changed filter field
	// (including the legacy Limit/Offset) is rejected.
	boundDigest, err := filterDigest(filter)
	if err != nil {
		return PageResponse{}, ErrSnapshotNotFound
	}
	if !bytes.Equal(boundDigest, snapshot.filterDigest) {
		return PageResponse{}, ErrSnapshotNotFound
	}

	// Validate page size binding; reject changed page sizes.
	if pageReq.Limit != 0 && pageReq.Limit != snapshot.pageSize {
		return PageResponse{}, ErrSnapshotNotFound
	}

	return c.snapshotPage(randomID, offset, snapshot.pageSize, storeSecret)
}

// snapshotPage returns the page at the given offset within a snapshot.
// Caller must hold the mutex and must have already validated the snapshot.
func (c *Correlator) snapshotPage(randomID string, offset, pageSize int, storeSecret []byte) (PageResponse, error) {
	snapshot, ok := c.snapshots[randomID]
	if !ok {
		return PageResponse{}, ErrSnapshotNotFound
	}

	// Clamp offset.
	if offset < 0 {
		offset = 0
	}
	if offset >= len(snapshot.records) {
		return PageResponse{
			Records: []*CorrelationRecord{},
			Cursor:  "",
		}, nil
	}

	end := offset + pageSize
	if end < offset { // overflow check
		end = len(snapshot.records)
	}
	if end > len(snapshot.records) {
		end = len(snapshot.records)
	}

	// Clone page records for defensive output.
	page := make([]*CorrelationRecord, 0, end-offset)
	for _, r := range snapshot.records[offset:end] {
		page = append(page, r.Clone())
	}

	// Determine next cursor (authenticated).
	nextCursor := ""
	if end < len(snapshot.records) {
		cur, err := encodeCursor(storeSecret, []byte(snapshot.randomID), uint64(end))
		if err != nil {
			return PageResponse{}, err
		}
		nextCursor = cur
	}

	return PageResponse{
		Records: page,
		Cursor:  nextCursor,
	}, nil
}

// evictExpiredSnapshotsLocked evicts snapshots whose TTL has expired.
// Caller must hold the mutex.
func (c *Correlator) evictExpiredSnapshotsLocked() {
	if c.snapshotTTL <= 0 {
		return
	}
	now := c.clock.Now()
	var expired []string
	for _, randomID := range c.snapshotOrder {
		snapshot, ok := c.snapshots[randomID]
		if !ok {
			continue
		}
		if !now.Before(snapshot.createdAt.Add(c.snapshotTTL)) {
			expired = append(expired, randomID)
		}
	}
	for _, randomID := range expired {
		c.evictSnapshotLocked(randomID)
	}
}

// evictSnapshotLocked removes a snapshot from the store. Caller must hold the
// mutex.
func (c *Correlator) evictSnapshotLocked(randomID string) {
	snapshot, ok := c.snapshots[randomID]
	if !ok {
		return
	}
	delete(c.snapshots, randomID)
	c.snapshotCount--
	c.snapshotSize -= snapshot.sizeBytes
	for i, id := range c.snapshotOrder {
		if id == randomID {
			c.snapshotOrder = append(c.snapshotOrder[:i], c.snapshotOrder[i+1:]...)
			break
		}
	}
}

// evictSnapshotsToCapLocked evicts oldest snapshots until count and size caps
// are satisfied. Caller must hold the mutex.
func (c *Correlator) evictSnapshotsToCapLocked() {
	// Evict expired first.
	c.evictExpiredSnapshotsLocked()

	// Evict oldest by count cap.
	for c.snapshotCountCap > 0 && c.snapshotCount > c.snapshotCountCap {
		if len(c.snapshotOrder) == 0 {
			break
		}
		c.evictSnapshotLocked(c.snapshotOrder[0])
	}

	// Evict oldest by size cap.
	for c.snapshotSizeCap > 0 && c.snapshotSize > c.snapshotSizeCap {
		if len(c.snapshotOrder) == 0 {
			break
		}
		c.evictSnapshotLocked(c.snapshotOrder[0])
	}
}

// Stats returns correlation statistics. Age-based eviction is applied on read
// so idle entries expire.
func (c *Correlator) Stats() map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.evictAgeLocked()
	// Expire snapshots on reads so idle cursors expire consistently with
	// resume, rather than lingering past their TTL.
	c.evictExpiredSnapshotsLocked()
	return map[string]any{
		"record_count":    len(c.records),
		"activity_count":  len(c.activityIndex),
		"total_size":      c.totalSize,
		"retention_count": c.retentionCount,
		"retention_age":   c.retentionAge.String(),
		"retention_size":  c.retentionSize,
	}
}

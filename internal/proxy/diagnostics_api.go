// Package proxy provides the internal diagnostics API boundary. It is a
// read-only, on-demand query surface protected by a dedicated API key. It
// never routes or proxies to backend inference endpoints.
package proxy

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/catalog"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/correlation"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/diagnostics"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modeldiagnostics"
)

const (
	// diagnosticAPIPath is the reserved legacy diagnostics endpoint.
	diagnosticAPIPath = "/diagnostics"
	// diagnosticModelsPath is the models list subpath.
	diagnosticModelsPath = "/diagnostics/v1/models"
	// diagnosticRequestsPath is the requests list subpath.
	diagnosticRequestsPath = "/diagnostics/v1/requests"
	// diagnosticRequestIDPrefix is the prefix for single-request paths.
	diagnosticRequestIDPrefix = "/diagnostics/v1/requests/"
	// diagnosticSummaryPath is the summary subpath.
	diagnosticSummaryPath = "/diagnostics/v1/summary"
	// diagnosticBackendsPath is the backends capability subpath.
	diagnosticBackendsPath = "/diagnostics/v1/backends"
	diagnosticPeersPath    = "/diagnostics/v1/peers"

	// diagnosticRequestTimeout is the per-request deadline.
	diagnosticRequestTimeout = 10 * time.Second
	// modelsRequestTimeout is the models endpoint deadline; collectors are
	// sequential and each owns a bounded 2s timeout, so 15s bounds the total.
	modelsRequestTimeout = 15 * time.Second
	// diagnosticRateInterval is the minimum interval between diagnostic queries.
	diagnosticRateInterval = time.Second
	// diagnosticSlotsCap is the semaphore capacity for concurrent queries.
	diagnosticSlotsCap = 2
	// maxDiagnosticQueryLimit is the maximum number of models/records returned
	// per query. The snapshot store may hold more (up to modelSnapshotMaxRows).
	maxDiagnosticQueryLimit = 100
	// maxDiagnosticSummaryLimit is the maximum number of records returned by
	// the summary endpoint.
	maxDiagnosticSummaryLimit = 1000
	// defaultDiagnosticQueryLimit is the default page size.
	defaultDiagnosticQueryLimit = 50
	// modelStaleAge is the age after which a cached model catalog is stale.
	modelStaleAge = 5 * time.Minute
	// maxQueryParamLength bounds the byte length of any query parameter value.
	maxQueryParamLength = 256
	// maxCursorLength bounds the byte length of an opaque cursor.
	maxCursorLength = 128
	// maxTimeRange bounds the allowed time range for history queries.
	maxTimeRange = 24 * time.Hour

	// modelSnapshotMaxPages bounds the number of snapshots retained by the
	// per-handler model snapshot pager.
	modelSnapshotMaxPages = 10
	// modelSnapshotMaxSize bounds the estimated total JSON bytes retained by
	// the per-handler model snapshot pager.
	modelSnapshotMaxSize = 4 * 1024 * 1024
	// modelSnapshotMaxRows bounds the number of model snapshots frozen in a
	// single snapshot.
	modelSnapshotMaxRows = 1000
	// modelSnapshotPageSize is the default page size for model snapshots.
	modelSnapshotPageSize = 50
	// modelSnapshotTTL is the time-to-live for a model snapshot.
	modelSnapshotTTL = time.Minute
	// modelSnapshotCursorMaxLen bounds the byte length of a model pager cursor.
	modelSnapshotCursorMaxLen = 256
)

// diagnosticsErr* are the fixed diagnostic response messages duplicated across
// handlers. They are centralized here so the wire response message is defined
// once and referenced everywhere, satisfying go:S1192 without changing any
// response payload or behavior.
const (
	// diagnosticsErrCursorTooLong is returned when a client cursor exceeds the
	// bounded byte length.
	diagnosticsErrCursorTooLong = "cursor too long"
	// diagnosticsErrInvalidFilterValue is returned when a string filter is not
	// valid UTF-8 or exceeds the maximum query parameter length.
	diagnosticsErrInvalidFilterValue = "invalid filter value"
	// diagnosticsErrInvalidLimit is returned when the limit query parameter is
	// not a non-negative integer.
	diagnosticsErrInvalidLimit = "invalid limit"
	// diagnosticsErrDiagnosticsTemporarilyUnavailable is returned when the
	// diagnostics service cannot admit or complete a request.
	diagnosticsErrDiagnosticsTemporarilyUnavailable = "diagnostics temporarily unavailable"
	// diagnosticsErrModelSnapshotFailed is returned when model snapshot
	// collection or pagination fails.
	diagnosticsErrModelSnapshotFailed = "model snapshot failed"
	// diagnosticsErrDuplicateQueryParam is returned when a query parameter is
	// supplied more than once.
	diagnosticsErrDuplicateQueryParam = "duplicate query parameter"
	// diagnosticsErrInvalidStatusFilter is returned when a status filter value
	// is not in the finite accepted set.
	diagnosticsErrInvalidStatusFilter = "invalid status filter"
	// diagnosticsErrUnknownDiagnosticsPath is returned for an unrecognized
	// diagnostics subpath.
	diagnosticsErrUnknownDiagnosticsPath = "unknown diagnostics path"
	// diagnosticsErrDiagnosticsRequestTimedOut is returned when a diagnostics
	// request exceeds its deadline or is canceled.
	diagnosticsErrDiagnosticsRequestTimedOut = "diagnostics request timed out"
)

// errDiagnosticsBusy is returned when the semaphore is full and a new query
// cannot be admitted without waiting.
var errDiagnosticsBusy = errors.New("diagnostics busy")

// serveDiagnosticsAPI handles all diagnostics API requests. It is reserved by
// the /diagnostics and /diagnostics/v1 routes in Handler.serve, so no upstream
// path passthrough occurs for unknown diagnostics subpaths.
func (h *Handler) serveDiagnosticsAPI(w http.ResponseWriter, r *http.Request, info *routeInfo) {
	*info = routeInfo{kind: routeInfoKindLocal}

	// Default-off: if no API key is configured, the endpoint is disabled.
	if h.diagnosticsAPIKey == "" {
		writeDiagnosticsError(w, http.StatusNotFound, "diagnostics disabled")
		return
	}

	// Authentication: constant-time exact Bearer key compare from the
	// Authorization header only. Query params, other headers, and backend
	// inference keys are never accepted as authentication.
	if !h.diagnosticsAuthenticate(r) {
		writeDiagnosticsError(w, http.StatusUnauthorized, "diagnostics authentication required")
		return
	}

	// Method check: only GET is allowed. Checked after auth to avoid method
	// discovery for unauthorized paths.
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeDiagnosticsError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Route the request. Unknown paths return 404 after auth.
	switch {
	case r.URL.Path == diagnosticAPIPath:
		h.serveDiagnosticsBackends(w, r)
	case r.URL.Path == diagnosticModelsPath:
		h.serveDiagnosticsModels(w, r)
	case r.URL.Path == diagnosticRequestsPath:
		h.serveDiagnosticsRequests(w, r)
	case strings.HasPrefix(r.URL.Path, diagnosticRequestIDPrefix):
		id := r.URL.Path[len(diagnosticRequestIDPrefix):]
		if id == "" || strings.Contains(id, "/") {
			writeDiagnosticsError(w, http.StatusNotFound, diagnosticsErrUnknownDiagnosticsPath)
			return
		}
		h.serveDiagnosticsRequestByID(w, r, id)
	case r.URL.Path == diagnosticSummaryPath:
		h.serveDiagnosticsSummary(w, r)
	case r.URL.Path == diagnosticBackendsPath:
		h.serveDiagnosticsBackends(w, r)
	case r.URL.Path == diagnosticPeersPath && h.peerCache != nil:
		h.serveDiagnosticsPeers(w, r)
	default:
		writeDiagnosticsError(w, http.StatusNotFound, diagnosticsErrUnknownDiagnosticsPath)
	}
}

// diagnosticsAuthenticate returns true when the request carries a valid Bearer
// token in the Authorization header that matches the configured diagnostics
// API key using constant-time comparison. Query parameters, other headers, and
// backend inference keys are never accepted.
func (h *Handler) diagnosticsAuthenticate(r *http.Request) bool {
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		return false
	}
	token := strings.TrimPrefix(auth, "Bearer ")
	return hmac.Equal([]byte(token), []byte(h.diagnosticsAPIKey))
}

// diagnoseRateLimit enforces one query per second and a concurrency cap of two.
// It acquires the semaphore slot and the rate-limit mutex, waiting up to the
// context deadline. It returns a release function that must be deferred at the
// endpoint completion so the semaphore slot is held for the entire duration of
// the diagnostic data collection. Returns ctx.Err() on cancellation.
func (h *Handler) diagnoseRateLimit(ctx context.Context) (release func(), err error) {
	releaseSlot, err := h.acquireDiagnosticSlot(ctx)
	if err != nil {
		return nil, err
	}

	// Acquire rate limit mutex.
	h.diagnosticRateMu.Lock()

	// One query per second.
	if !h.lastDiagnosticQuery.IsZero() {
		elapsed := time.Since(h.lastDiagnosticQuery)
		if elapsed < diagnosticRateInterval {
			waitTime := diagnosticRateInterval - elapsed
			timer := time.NewTimer(waitTime)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-ctx.Done():
				h.diagnosticRateMu.Unlock()
				releaseSlot()
				return nil, ctx.Err()
			}
		}
	}
	h.lastDiagnosticQuery = time.Now()

	release = func() {
		h.diagnosticRateMu.Unlock()
		releaseSlot()
	}
	return release, nil
}

// acquireDiagnosticSlot bounds concurrent diagnostics work. Cursor resumes
// use this without the one-per-second refresh rate, because they do not collect
// anything upstream and clients should be able to page a frozen snapshot.
func (h *Handler) acquireDiagnosticSlot(ctx context.Context) (func(), error) {
	select {
	case h.diagnosticSlots <- struct{}{}:
		return func() { <-h.diagnosticSlots }, nil
	default:
		return nil, errDiagnosticsBusy
	}
}

// writeDiagnosticsResponse writes a JSON diagnostics response with
// Cache-Control: no-store to prevent caching of sensitive diagnostics data.
func writeDiagnosticsResponse(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		io.WriteString(w, `{"error":{"message":"response write failed","type":"diagnostics_error"}}`)
	}
}

// writeDiagnosticsError writes a fixed JSON error response without raw error
// details or credential leakage.
func writeDiagnosticsError(w http.ResponseWriter, status int, message string) {
	writeDiagnosticsResponse(w, status, map[string]any{
		"error": map[string]string{"message": message, "type": "diagnostics_error"},
	})
}

// catalogStatus extracts the finite catalog status from a model map. Raw
// strings outside the finite set and nested objects with a finite "value"
// string are accepted; everything else yields an empty string.
func catalogStatus(v any) string {
	switch x := v.(type) {
	case string:
		if validCatalogStatus[x] {
			return x
		}
	case map[string]any:
		if val, ok := x["value"]; ok {
			if s, ok := val.(string); ok && validCatalogStatus[s] {
				return s
			}
		}
	}
	return ""
}

var validCatalogStatus = map[string]bool{
	"loaded":   true,
	"unloaded": true,
}

// validModelStatus is the finite exact set of model lifecycle status values
// accepted by the models status filter. It is derived from
// ModelSnapshot.Lifecycle.CurrentState: loaded/unloaded (catalog observation)
// and ready/starting/stopping/unknown (lifecycle observation). Public ID
// collisions across backends cannot bleed because each typed snapshot carries
// its own current state.
var validModelStatus = map[string]bool{
	"loaded":   true,
	"unloaded": true,
	"ready":    true,
	"starting": true,
	"stopping": true,
	"unknown":  true,
}

// validTransportStatus reports whether s is one of the finite transport status
// values accepted by the requests endpoint.
func validTransportStatus(s string) bool {
	switch s {
	case "http_success", "http_client_error", "http_server_error", "http_other",
		"client_canceled", "transport_error", "aborted":
		return true
	}
	return false
}

// validRequestID reports whether id is a proxy-generated request ID: 32
// lowercase hex digits or the nonempty req-<digits> fallback. No length cap
// is applied to the req-<digits> fallback.
func validRequestID(id string) bool {
	return validLowerHex32(id) || validReqDigits(id)
}

// validLowerHex32 reports whether id is exactly 32 lowercase hex digits.
// Uppercase hex and any other length are rejected.
func validLowerHex32(id string) bool {
	if len(id) != 32 {
		return false
	}
	for i := 0; i < 32; i++ {
		c := id[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

// validReqDigits reports whether id is the nonempty req-<digits> fallback:
// the prefix req- followed by one or more ASCII digits, with no length cap.
func validReqDigits(id string) bool {
	if !strings.HasPrefix(id, "req-") {
		return false
	}
	rest := id[4:]
	if rest == "" {
		return false
	}
	for i := 0; i < len(rest); i++ {
		if rest[i] < '0' || rest[i] > '9' {
			return false
		}
	}
	return true
}

// rejectDuplicateQueryParams returns an error if any of the named query
// parameters carry more than one value. Query parameters must be unique;
// duplicates are rejected before any data access.
func rejectDuplicateQueryParams(values url.Values, names ...string) error {
	for _, name := range names {
		if len(values[name]) > 1 {
			return errors.New("duplicate query parameter")
		}
	}
	return nil
}

// parseDiagnosticsTimeRange parses and validates the since/until query
// parameters. Missing since defaults to now-24h; missing until defaults to
// now. Future bounds, reversed ranges, and ranges exceeding maxTimeRange are
// rejected. The error message is suitable for a 400 response.
func parseDiagnosticsTimeRange(sinceParam, untilParam string, now time.Time) (time.Time, time.Time, error) {
	var since, until time.Time
	if sinceParam != "" {
		if err := since.UnmarshalText([]byte(sinceParam)); err != nil {
			return time.Time{}, time.Time{}, errors.New("invalid since")
		}
		if since.After(now) {
			return time.Time{}, time.Time{}, errors.New("since in future")
		}
	} else {
		since = now.Add(-maxTimeRange)
	}
	if untilParam != "" {
		if err := until.UnmarshalText([]byte(untilParam)); err != nil {
			return time.Time{}, time.Time{}, errors.New("invalid until")
		}
		if until.After(now) {
			return time.Time{}, time.Time{}, errors.New("until in future")
		}
	} else {
		until = now
	}
	if until.Sub(since) > maxTimeRange {
		return time.Time{}, time.Time{}, errors.New("time range exceeds 24h")
	}
	if since.After(until) {
		return time.Time{}, time.Time{}, errors.New("start after end")
	}
	return since, until, nil
}

// modelFilterDigest returns the SHA256 digest of a normalized model filter. It
// validates the filter before hashing: string fields must be valid UTF-8 and no
// longer than 256 bytes. Raw filters are never retained.
func modelFilterDigest(backend, model, status string, limit int) []byte {
	var buf []byte
	buf = appendDigestStr(buf, backend)
	buf = appendDigestStr(buf, model)
	buf = appendDigestStr(buf, status)
	buf = appendDigestInt(buf, int64(limit))
	h := sha256.New()
	h.Write(buf)
	return h.Sum(nil)
}

func appendDigestStr(buf []byte, s string) []byte {
	buf = appendDigestInt(buf, int64(len(s)))
	buf = append(buf, s...)
	return buf
}

func appendDigestInt(buf []byte, v int64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(v))
	return append(buf, b[:]...)
}

// modelSnapshotSnapshot is a frozen, immutable snapshot of filtered model
// snapshots used by the per-handler model snapshot pager. Records are
// defensive copies so later collector refreshes cannot mutate the page.
type modelSnapshotSnapshot struct {
	randomID     string
	models       []*modeldiagnostics.ModelSnapshot
	pageSize     int
	filterDigest []byte
	createdAt    time.Time
	coverage     string
	sizeBytes    int64
}

// modelSnapshotPageStore is a bounded per-handler immutable snapshot pager for
// model diagnostics. It stores at most modelSnapshotMaxPages snapshots with a
// total estimated size of modelSnapshotMaxSize bytes. Each snapshot is frozen
// at creation time and evicted by TTL or count/size caps. Cursors are opaque
// HMAC-authenticated bindings of snapshot ID, offset, and filter digest; raw
// filters and IDs are never retained.
type modelSnapshotPageStore struct {
	mu         sync.Mutex
	snapshots  map[string]*modelSnapshotSnapshot
	order      []string
	count      int
	size       int64
	secret     []byte
	secretOnce sync.Once
	secretErr  error
	now        func() time.Time
}

// newModelSnapshotPageStore returns a pager with default bounds.
func newModelSnapshotPageStore() *modelSnapshotPageStore {
	return &modelSnapshotPageStore{
		snapshots: make(map[string]*modelSnapshotSnapshot),
		now:       time.Now,
	}
}

// ensureSecret lazily initializes the per-store HMAC secret. It fails closed on
// entropy failure.
func (s *modelSnapshotPageStore) ensureSecret() ([]byte, error) {
	s.secretOnce.Do(func() {
		buf := make([]byte, 32)
		if _, err := io.ReadFull(rand.Reader, buf); err != nil {
			s.secretErr = errors.New("snapshot entropy failure")
			return
		}
		s.secret = buf
	})
	return s.secret, s.secretErr
}

// createSnapshot freezes a sorted snapshot of defensive copies and returns its
// first page. Caller must hold the mutex. It rejects oversized results instead
// of silently truncating.
func (s *modelSnapshotPageStore) createSnapshot(models []*modeldiagnostics.ModelSnapshot, pageSize int, filterDigest []byte) (modelSnapshotPageResponse, error) {
	// Age eviction on read so idle entries expire.
	s.evictExpiredSnapshotsLocked()

	// Compute exact JSON size of the typed snapshots before insertion so the
	// size budget is enforced against actual serialized data, not a guess.
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, m := range models {
		if err := enc.Encode(m); err != nil {
			return modelSnapshotPageResponse{}, err
		}
	}
	sizeBytes := int64(buf.Len())

	// Reject if snapshot exceeds size cap before insertion.
	if s.size+sizeBytes > modelSnapshotMaxSize {
		return modelSnapshotPageResponse{}, errors.New("snapshot too large")
	}

	// Bound row count.
	if len(models) > modelSnapshotMaxRows {
		return modelSnapshotPageResponse{}, errors.New("snapshot too large")
	}

	// Lazily initialize the per-store secret.
	secret, err := s.ensureSecret()
	if err != nil {
		return modelSnapshotPageResponse{}, err
	}

	// Create a crypto-random snapshot ID. Fail closed on entropy failure.
	randomID := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, randomID); err != nil {
		return modelSnapshotPageResponse{}, errors.New("snapshot entropy failure")
	}

	// Catch ID collision.
	if _, exists := s.snapshots[string(randomID)]; exists {
		return modelSnapshotPageResponse{}, errors.New("snapshot entropy failure")
	}

	snapshot := &modelSnapshotSnapshot{
		randomID:     string(randomID),
		models:       append([]*modeldiagnostics.ModelSnapshot(nil), models...),
		pageSize:     pageSize,
		filterDigest: filterDigest,
		createdAt:    s.now(),
		sizeBytes:    sizeBytes,
	}

	s.snapshots[snapshot.randomID] = snapshot
	s.order = append(s.order, snapshot.randomID)
	s.count++
	s.size += sizeBytes

	// Evict old snapshots to stay within caps.
	s.evictSnapshotsToCapLocked()

	// Return first page.
	page, err := s.snapshotPage(snapshot.randomID, 0, pageSize, secret)
	if err != nil {
		s.evictSnapshotLocked(snapshot.randomID)
		return modelSnapshotPageResponse{}, err
	}
	return page, nil
}

// resumeSnapshot returns the page at the given offset within an existing
// snapshot. Caller must hold the mutex. It validates the HMAC, expiry, store
// ownership, filter binding, and page size binding.
func (s *modelSnapshotPageStore) resumeSnapshot(cursor string, pageSize int, filterDigest []byte) (modelSnapshotPageResponse, error) {
	secret, err := s.ensureSecret()
	if err != nil {
		return modelSnapshotPageResponse{}, err
	}

	randomID, offset, err := decodeModelCursor(secret, cursor)
	if err != nil {
		return modelSnapshotPageResponse{}, errSnapshotNotFound
	}

	snapshot, ok := s.snapshots[randomID]
	if !ok {
		return modelSnapshotPageResponse{}, errSnapshotNotFound
	}

	// Check expiry at exact boundary (now >= createdAt + TTL is expired).
	if modelSnapshotTTL > 0 {
		if !s.now().Before(snapshot.createdAt.Add(modelSnapshotTTL)) {
			s.evictSnapshotLocked(randomID)
			return modelSnapshotPageResponse{}, errSnapshotExpired
		}
	}

	// Validate filter binding; reject changed filters.
	if !hmac.Equal(filterDigest, snapshot.filterDigest) {
		return modelSnapshotPageResponse{}, errSnapshotNotFound
	}

	// Validate page size binding; reject changed page sizes.
	if pageSize != 0 && pageSize != snapshot.pageSize {
		return modelSnapshotPageResponse{}, errSnapshotNotFound
	}

	return s.snapshotPage(randomID, offset, snapshot.pageSize, secret)
}

// snapshotPage returns the page at the given offset within a snapshot.
// Caller must hold the mutex and must have already validated the snapshot.
func (s *modelSnapshotPageStore) snapshotPage(randomID string, offset, pageSize int, secret []byte) (modelSnapshotPageResponse, error) {
	snapshot, ok := s.snapshots[randomID]
	if !ok {
		return modelSnapshotPageResponse{}, errSnapshotNotFound
	}

	// Clamp offset.
	if offset < 0 {
		offset = 0
	}
	if offset >= len(snapshot.models) {
		return modelSnapshotPageResponse{
			Records:  []*modeldiagnostics.ModelSnapshot{},
			Cursor:   "",
			Coverage: snapshot.coverage,
		}, nil
	}

	end := offset + pageSize
	if end < offset {
		end = len(snapshot.models)
	}
	if end > len(snapshot.models) {
		end = len(snapshot.models)
	}

	// Clone page models for defensive output.
	page := make([]*modeldiagnostics.ModelSnapshot, 0, end-offset)
	for _, m := range snapshot.models[offset:end] {
		clone := cloneModelSnapshot(m)
		if clone == nil {
			return modelSnapshotPageResponse{}, errors.New("model snapshot clone failed")
		}
		page = append(page, clone)
	}

	// Determine next cursor (authenticated).
	nextCursor := ""
	if end < len(snapshot.models) {
		nextCursor = encodeModelCursor(secret, []byte(snapshot.randomID), uint64(end))
	}

	return modelSnapshotPageResponse{
		Records:     page,
		Cursor:      nextCursor,
		GeneratedAt: snapshot.createdAt,
		Coverage:    snapshot.coverage,
	}, nil
}

// evictExpiredSnapshotsLocked evicts snapshots whose TTL has expired.
// Caller must hold the mutex.
func (s *modelSnapshotPageStore) evictExpiredSnapshotsLocked() {
	if modelSnapshotTTL <= 0 {
		return
	}
	now := s.now()
	var expired []string
	for _, randomID := range s.order {
		snapshot, ok := s.snapshots[randomID]
		if !ok {
			continue
		}
		if !now.Before(snapshot.createdAt.Add(modelSnapshotTTL)) {
			expired = append(expired, randomID)
		}
	}
	for _, randomID := range expired {
		s.evictSnapshotLocked(randomID)
	}
}

// evictSnapshotLocked removes a snapshot from the store. Caller must hold the
// mutex.
func (s *modelSnapshotPageStore) evictSnapshotLocked(randomID string) {
	snapshot, ok := s.snapshots[randomID]
	if !ok {
		return
	}
	delete(s.snapshots, randomID)
	s.count--
	s.size -= snapshot.sizeBytes
	for i, id := range s.order {
		if id == randomID {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
}

// evictSnapshotsToCapLocked evicts oldest snapshots until count and size caps
// are satisfied. Caller must hold the mutex.
func (s *modelSnapshotPageStore) evictSnapshotsToCapLocked() {
	// Evict expired first.
	s.evictExpiredSnapshotsLocked()

	// Evict oldest by count cap.
	for modelSnapshotMaxPages > 0 && s.count > modelSnapshotMaxPages {
		if len(s.order) == 0 {
			break
		}
		s.evictSnapshotLocked(s.order[0])
	}

	// Evict oldest by size cap.
	for modelSnapshotMaxSize > 0 && s.size > modelSnapshotMaxSize {
		if len(s.order) == 0 {
			break
		}
		s.evictSnapshotLocked(s.order[0])
	}
}

// modelSnapshotPageResponse is the response for a single page of model
// snapshots.
type modelSnapshotPageResponse struct {
	// Records are defensive copies of the page's snapshots. Empty pages
	// encode as empty arrays, not null.
	Records []*modeldiagnostics.ModelSnapshot `json:"records"`
	// Cursor is the opaque cursor for the next page, or empty if this is
	// the last page.
	Cursor string `json:"cursor,omitempty"`
	// GeneratedAt is the snapshot's creation time, captured once at snapshot
	// creation in modelSnapshotSnapshot.createdAt. Every page (first and
	// resumed) returns the same value so pagination preserves the initial
	// generated_at rather than emitting time.Now() on each request.
	GeneratedAt time.Time `json:"generated_at,omitempty"`
	// Coverage is the snapshot's coverage, captured once at snapshot
	// creation. Every page (first and resumed) returns the same value.
	Coverage string `json:"coverage,omitempty"`
}

var (
	errSnapshotNotFound = errors.New("snapshot not found")
	errSnapshotExpired  = errors.New("snapshot expired")
)

// encodeModelCursor returns an opaque, authenticated cursor binding a snapshot
// random ID, page offset, and store secret. The cursor uses a fixed-width
// binary layout (16-byte ID + uint64 offset + 32-byte MAC) so that
// crypto-random IDs containing NUL bytes cannot be split apart. It is
// unforgeable: any change to the offset or random ID invalidates the HMAC.
func encodeModelCursor(secret []byte, randomID []byte, offset uint64) string {
	if len(randomID) != 16 {
		return ""
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write(randomID)
	var off [8]byte
	binary.BigEndian.PutUint64(off[:], offset)
	mac.Write(off[:])
	sum := mac.Sum(nil)

	var buf [16 + 8 + 32]byte
	copy(buf[:], randomID)
	copy(buf[16:], off[:])
	copy(buf[16+8:], sum)
	return base64.RawURLEncoding.EncodeToString(buf[:])
}

// decodeModelCursor parses and authenticates an opaque cursor. It rejects
// malformed, forged, overlong, or tampered cursors. Any change to the offset
// or random ID invalidates the HMAC.
func decodeModelCursor(secret []byte, cursor string) (randomID string, offset int, err error) {
	if cursor == "" {
		return "", 0, errSnapshotNotFound
	}
	if len(cursor) > modelSnapshotCursorMaxLen {
		return "", 0, errSnapshotNotFound
	}

	// Strict base64 decode; any alphabet or length anomaly is rejection.
	expectedLen := base64.RawURLEncoding.EncodedLen(16 + 8 + 32)
	if len(cursor) != expectedLen {
		return "", 0, errSnapshotNotFound
	}
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(cursor)
	if err != nil {
		return "", 0, errSnapshotNotFound
	}
	if len(decoded) != 16+8+32 {
		return "", 0, errSnapshotNotFound
	}

	randomID = string(decoded[:16])
	offsetVal := binary.BigEndian.Uint64(decoded[16 : 16+8])
	if offsetVal > uint64(math.MaxInt) {
		return "", 0, errSnapshotNotFound
	}
	offset = int(offsetVal)

	// Verify HMAC over randomID + offset bytes.
	mac := hmac.New(sha256.New, secret)
	mac.Write(decoded[:16+8])
	expectedMAC := mac.Sum(nil)
	if !hmac.Equal(expectedMAC, decoded[16+8:]) {
		return "", 0, errSnapshotNotFound
	}

	return randomID, offset, nil
}

// serveDiagnosticsBackends returns the protected backends capability list for
// the legacy /diagnostics and /diagnostics/v1/backends endpoints.
func (h *Handler) serveDiagnosticsBackends(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), diagnosticRequestTimeout)
	defer cancel()

	if h.diagnostics == nil {
		writeDiagnosticsError(w, http.StatusServiceUnavailable, "diagnostics not configured")
		return
	}

	release, err := h.diagnoseRateLimit(ctx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			writeDiagnosticsError(w, http.StatusGatewayTimeout, diagnosticsErrDiagnosticsRequestTimedOut)
			return
		}
		writeDiagnosticsError(w, http.StatusServiceUnavailable, diagnosticsErrDiagnosticsTemporarilyUnavailable)
		return
	}
	defer release()

	snapshots, err := h.diagnostics.All(ctx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			writeDiagnosticsError(w, http.StatusGatewayTimeout, diagnosticsErrDiagnosticsRequestTimedOut)
			return
		}
		writeDiagnosticsError(w, http.StatusServiceUnavailable, "diagnostics snapshot failed")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	writeDiagnosticsResponse(w, http.StatusOK, map[string]any{"backends": snapshots})
}

// diagnosticError carries an HTTP status and a fixed response message for a
// diagnostics API failure. It never leaks request details, raw errors, or
// credentials. Helpers return it so the caller owns the response mapping.
type diagnosticError struct {
	status  int
	message string
}

func (e *diagnosticError) Error() string { return e.message }

// modelsQueryArgs holds the validated and normalized query arguments for the
// /diagnostics/v1/models endpoint.
type modelsQueryArgs struct {
	limit   int
	cursor  string
	backend string
	model   string
	status  string
}

// diagnosticsQueryArgs holds the validated and normalized query arguments
// shared by the /diagnostics/v1/requests and /diagnostics/v1/summary
// endpoints.
type diagnosticsQueryArgs struct {
	since     time.Time
	until     time.Time
	backend   string
	status    string
	requestID string
	cursor    string
	limit     int
}

// diagnosticsQueryPolicy captures endpoint-specific defaults/caps/zero-limit
// behavior for the requests and summary query parsers and page helpers.
type diagnosticsQueryPolicy struct {
	// defaultLimit is the default page size when limit is omitted.
	defaultLimit int
	// maxLimit is the maximum allowed page size.
	maxLimit int
	// zeroAsEmpty reports whether a limit of zero is accepted as an empty
	// page rather than defaulted to the page size.
	zeroAsEmpty bool
	// includeCursor reports whether the cursor is passed through to QueryPage.
	includeCursor bool
	// tooLargeStatus is the HTTP status returned when the QueryPage result
	// exceeds the cap (400 for requests, 413 for summary).
	tooLargeStatus int
	// checkTooLargeFirst reports whether ErrSnapshotTooLarge is checked before
	// ErrSnapshotExpired/ErrSnapshotNotFound, preserving each endpoint's
	// existing error-check order.
	checkTooLargeFirst bool
}

var (
	// diagnosticsRequestsPolicy is the endpoint-specific policy for the
	// /diagnostics/v1/requests endpoint: default limit 50, max 100, zero
	// accepted as an empty page, cursor passed to QueryPage, too-large maps to
	// 400, and expired/not-found is checked before too-large.
	diagnosticsRequestsPolicy = diagnosticsQueryPolicy{
		defaultLimit:       defaultDiagnosticQueryLimit,
		maxLimit:           maxDiagnosticQueryLimit,
		zeroAsEmpty:        true,
		includeCursor:      true,
		tooLargeStatus:     http.StatusBadRequest,
		checkTooLargeFirst: false,
	}
	// diagnosticsSummaryPolicy is the endpoint-specific policy for the
	// /diagnostics/v1/summary endpoint: default limit 1000, max 1000, zero
	// accepted as an empty page, no cursor passed to QueryPage, too-large maps
	// to 413, and too-large is checked before expired/not-found.
	diagnosticsSummaryPolicy = diagnosticsQueryPolicy{
		defaultLimit:       maxDiagnosticSummaryLimit,
		maxLimit:           maxDiagnosticSummaryLimit,
		zeroAsEmpty:        true,
		includeCursor:      false,
		tooLargeStatus:     http.StatusRequestEntityTooLarge,
		checkTooLargeFirst: true,
	}
)

// isContextTimeout reports whether err is a context deadline exceeded or
// cancellation error.
func isContextTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return errors.Is(err, context.Canceled)
}

// validateCursorLength rejects a cursor that exceeds the bounded byte length.
func validateCursorLength(cursor string) *diagnosticError {
	if cursor == "" {
		return nil
	}
	if len(cursor) > modelSnapshotCursorMaxLen {
		return &diagnosticError{status: http.StatusBadRequest, message: diagnosticsErrCursorTooLong}
	}
	return nil
}

// validQueryFilterValue reports whether a nonempty query filter value is valid
// UTF-8 and no longer than the maximum query parameter length.
func validQueryFilterValue(v string) bool {
	if len(v) > maxQueryParamLength {
		return false
	}
	return utf8.ValidString(v)
}

// validateQueryFilters validates the string query filters. It returns a
// diagnosticError when any nonempty filter is invalid UTF-8 or too long.
func validateQueryFilters(backend, model, status string) *diagnosticError {
	for _, v := range []string{backend, model, status} {
		if v != "" {
			if !validQueryFilterValue(v) {
				return &diagnosticError{status: http.StatusBadRequest, message: diagnosticsErrInvalidFilterValue}
			}
		}
	}
	return nil
}

// validateStatusFilter reports whether the status filter is in the finite
// accepted set derived from typed ModelSnapshot.Lifecycle.CurrentState.
func validateStatusFilter(statusFilter string) *diagnosticError {
	if statusFilter == "" {
		return nil
	}
	if !validModelStatus[statusFilter] {
		return &diagnosticError{status: http.StatusBadRequest, message: diagnosticsErrInvalidStatusFilter}
	}
	return nil
}

// parseQueryLimit parses the limit query parameter, clamping to the allowed
// range and defaulting to the page size.
func parseQueryLimit(limitParam string) (int, *diagnosticError) {
	limit := modelSnapshotPageSize
	if limitParam == "" {
		return limit, nil
	}
	l, err := strconv.Atoi(limitParam)
	if err != nil {
		return 0, &diagnosticError{status: http.StatusBadRequest, message: diagnosticsErrInvalidLimit}
	}
	if l < 0 {
		return 0, &diagnosticError{status: http.StatusBadRequest, message: diagnosticsErrInvalidLimit}
	}
	if l > maxDiagnosticQueryLimit {
		l = maxDiagnosticQueryLimit
	}
	if l == 0 {
		l = modelSnapshotPageSize
	}
	return l, nil
}

// validateDiagnosticsModelsQuery validates and normalizes the query
// parameters for the /diagnostics/v1/models endpoint. It performs no data
// refresh. On success it returns the normalized arguments. On failure it
// returns a diagnosticError the caller maps to a response.
func (h *Handler) validateDiagnosticsModelsQuery(r *http.Request) (modelsQueryArgs, *diagnosticError) {
	backendFilter := r.URL.Query().Get("backend")
	modelFilter := r.URL.Query().Get("model")
	statusFilter := r.URL.Query().Get("status")
	cursor := r.URL.Query().Get("cursor")
	offset := r.URL.Query().Get("offset")
	limitParam := r.URL.Query().Get("limit")

	// Reject duplicate query parameters.
	if err := rejectDuplicateQueryParams(r.URL.Query(), "backend", "model", "status", "cursor", "offset", "limit"); err != nil {
		return modelsQueryArgs{}, &diagnosticError{status: http.StatusBadRequest, message: diagnosticsErrDuplicateQueryParam}
	}

	// Reject offset: the opaque cursor is the only pagination mechanism.
	if offset != "" {
		return modelsQueryArgs{}, &diagnosticError{status: http.StatusBadRequest, message: "offset unsupported"}
	}

	// Validate cursor length.
	if diagErr := validateCursorLength(cursor); diagErr != nil {
		return modelsQueryArgs{}, diagErr
	}

	// Validate string filters (bounded valid UTF-8).
	if diagErr := validateQueryFilters(backendFilter, modelFilter, statusFilter); diagErr != nil {
		return modelsQueryArgs{}, diagErr
	}

	// Validate status filter against the finite exact set derived from
	// typed ModelSnapshot.Lifecycle.CurrentState.
	if diagErr := validateStatusFilter(statusFilter); diagErr != nil {
		return modelsQueryArgs{}, diagErr
	}

	// Validate backend filter against configured backends.
	if diagErr := h.validateBackendFilter(backendFilter); diagErr != nil {
		return modelsQueryArgs{}, diagErr
	}

	// Validate limit.
	limit, err := parseQueryLimit(limitParam)
	if err != nil {
		return modelsQueryArgs{}, err
	}

	return modelsQueryArgs{limit: limit, cursor: cursor, backend: backendFilter, model: modelFilter, status: statusFilter}, nil
}

// parseDiagnosticsQuery is the shared query parser for the requests and
// summary endpoints. It performs the common validations once, delegating
// endpoint-specific defaults/caps/zero-limit behavior to the policy. It
// performs no data refresh. On success it returns the normalized arguments.
// On failure it returns a diagnosticError the caller maps to a response.
func (h *Handler) parseDiagnosticsQuery(r *http.Request, policy diagnosticsQueryPolicy) (diagnosticsQueryArgs, *diagnosticError) {
	values := r.URL.Query()

	// Reject duplicate query parameters.
	if err := rejectDuplicateQueryParams(values, "since", "until", "backend", "model", "status", "requestid", "cursor", "limit"); err != nil {
		return diagnosticsQueryArgs{}, &diagnosticError{status: http.StatusBadRequest, message: diagnosticsErrDuplicateQueryParam}
	}

	// Model filter on proxy request records cannot be attributed because the
	// correlation record ModelID is intentionally blank. A nonempty model
	// filter returns explicit 501.
	if modelFilter := values.Get("model"); modelFilter != "" {
		return diagnosticsQueryArgs{}, &diagnosticError{status: http.StatusNotImplemented, message: "model_attribution_unsupported"}
	}

	// Derive effective time bounds. Missing bounds default to a 24h window
	// ending at now. Reject any future bound and any window > 24h.
	since, until, err := parseDiagnosticsTimeRange(values.Get("since"), values.Get("until"), time.Now())
	if err != nil {
		return diagnosticsQueryArgs{}, &diagnosticError{status: http.StatusBadRequest, message: err.Error()}
	}

	// Validate string filters (bounded valid UTF-8).
	backendFilter := values.Get("backend")
	statusFilter := values.Get("status")
	requestIDFilter := values.Get("requestid")
	if backendFilter != "" && (len(backendFilter) > maxQueryParamLength || !utf8.ValidString(backendFilter)) {
		return diagnosticsQueryArgs{}, &diagnosticError{status: http.StatusBadRequest, message: diagnosticsErrInvalidFilterValue}
	}
	if statusFilter != "" && (len(statusFilter) > maxQueryParamLength || !utf8.ValidString(statusFilter)) {
		return diagnosticsQueryArgs{}, &diagnosticError{status: http.StatusBadRequest, message: diagnosticsErrInvalidFilterValue}
	}
	if requestIDFilter != "" && (len(requestIDFilter) > maxQueryParamLength || !utf8.ValidString(requestIDFilter)) {
		return diagnosticsQueryArgs{}, &diagnosticError{status: http.StatusBadRequest, message: diagnosticsErrInvalidFilterValue}
	}

	// Validate cursor length.
	cursor := values.Get("cursor")
	if cursor != "" && len(cursor) > maxCursorLength {
		return diagnosticsQueryArgs{}, &diagnosticError{status: http.StatusBadRequest, message: diagnosticsErrCursorTooLong}
	}

	// Validate limit.
	limit, diagErr := parseDiagnosticsQueryLimit(values.Get("limit"), policy)
	if diagErr != nil {
		return diagnosticsQueryArgs{}, diagErr
	}

	// Validate backend filter: must be a configured backend.
	if backendFilter != "" {
		if _, ok := h.proxies[backendFilter]; !ok {
			return diagnosticsQueryArgs{}, &diagnosticError{status: http.StatusBadRequest, message: "unknown backend filter"}
		}
	}

	// Validate status filter: finite union of Correlation transport enums.
	if statusFilter != "" && !validTransportStatus(statusFilter) {
		return diagnosticsQueryArgs{}, &diagnosticError{status: http.StatusBadRequest, message: diagnosticsErrInvalidStatusFilter}
	}

	// Validate requestID filter format/bounds.
	if requestIDFilter != "" && !validRequestID(requestIDFilter) {
		return diagnosticsQueryArgs{}, &diagnosticError{status: http.StatusBadRequest, message: "invalid request id filter"}
	}

	return diagnosticsQueryArgs{
		since:     since,
		until:     until,
		backend:   backendFilter,
		status:    statusFilter,
		requestID: requestIDFilter,
		cursor:    cursor,
		limit:     limit,
	}, nil
}

// parseDiagnosticsQueryLimit parses the limit query parameter, clamping to the
// configured max. The default and zero handling are policy-driven.
func parseDiagnosticsQueryLimit(limitParam string, policy diagnosticsQueryPolicy) (int, *diagnosticError) {
	limit := policy.defaultLimit
	if limitParam == "" {
		return limit, nil
	}
	l, err := strconv.Atoi(limitParam)
	if err != nil || l < 0 {
		return 0, &diagnosticError{status: http.StatusBadRequest, message: diagnosticsErrInvalidLimit}
	}
	if l > policy.maxLimit {
		l = policy.maxLimit
	}
	if l == 0 && !policy.zeroAsEmpty {
		l = policy.defaultLimit
	}
	return l, nil
}

// parseDiagnosticsRequestsQuery validates and normalizes the query
// parameters for the /diagnostics/v1/requests endpoint. It performs no data
// refresh. On success it returns the normalized arguments. On failure it
// returns a diagnosticError the caller maps to a response. The model filter
// is attributed 501 because proxy-only records carry no model identity.
func (h *Handler) parseDiagnosticsRequestsQuery(r *http.Request) (diagnosticsQueryArgs, *diagnosticError) {
	return h.parseDiagnosticsQuery(r, diagnosticsRequestsPolicy)
}

// validateBackendFilter reports whether the backend filter names a
// configured backend.
func (h *Handler) validateBackendFilter(backendFilter string) *diagnosticError {
	if backendFilter == "" {
		return nil
	}
	if _, ok := h.proxies[backendFilter]; !ok {
		return &diagnosticError{status: http.StatusBadRequest, message: "invalid backend filter"}
	}
	return nil
}

// serveDiagnosticsModels returns the unified, sanitized model diagnostics
// snapshots. It uses the cached catalog source (BackendModels) only and never
// issues upstream /v1/models or /show requests. Model snapshots are produced
// via modeldiagnostics.Service SnapshotWithSources, combining catalog metadata
// with backend diagnostics, activity, alias, GPU, and lifecycle observations.
// Results are paged through an immutable snapshot store; the cursor replaces
// offset.
func (h *Handler) serveDiagnosticsModels(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), modelsRequestTimeout)
	defer cancel()

	if h.diagnostics == nil {
		writeDiagnosticsError(w, http.StatusServiceUnavailable, "diagnostics not configured")
		return
	}
	if h.modelDiagnostics == nil {
		writeDiagnosticsError(w, http.StatusServiceUnavailable, "model diagnostics not configured")
		return
	}

	// Validate query parameters first: no data refresh on invalid query.
	args, err := h.validateDiagnosticsModelsQuery(r)
	if err != nil {
		writeDiagnosticsError(w, err.status, err.message)
		return
	}

	// Admit the request. Cursor resumes bypass the per-second refresh rate
	// limit but still consume a semaphore slot.
	var release func()
	var limErr error
	if args.cursor != "" {
		release, limErr = h.acquireDiagnosticSlot(ctx)
	} else {
		release, limErr = h.diagnoseRateLimit(ctx)
	}
	if limErr != nil {
		if h.handleDiagnosticsLimiterError(w, limErr) {
			return
		}
	}
	defer release()

	// Early cursor-resume branch: no collectors, no upstream work.
	if args.cursor != "" {
		h.resumeDiagnosticsModelsPage(w, args)
		return
	}

	// Collect diagnostics inputs (catalog + collectors + snapshot projection).
	inputs, err := h.collectDiagnosticsModelInputs(ctx, args)
	if err != nil {
		writeDiagnosticsError(w, err.status, err.message)
		return
	}

	// Filter typed snapshots by backend/model/status.
	filtered := filterDiagnosticsModels(inputs.modelSnaps, inputs.modelFilterIDs, args.backend, args.model, args.status)

	// Unknown model filter: return 400 without echoing the raw ID.
	if diagErr := unknownModelFilterError(args.model, filtered, inputs.modelSnaps, inputs.modelFilterIDs); diagErr != nil {
		writeDiagnosticsError(w, diagErr.status, diagErr.message)
		return
	}

	// Coverage from cached catalog and diagnostics capability state.
	coverage := h.calculateDiagnosticsModelCoverage(inputs.backendModels, inputs.snapshots)

	// Page (direct return when the result fits, otherwise snapshot store).
	h.pageDiagnosticsModels(w, filtered, inputs.resolvedModelID, coverage, args)
}

// resolveModelFilterIDs resolves the model filter against the cached catalog,
// returning the set of canonical public IDs and, when exactly one matches,
// the resolved model ID.
func resolveModelFilterIDs(backendModels []catalog.BackendModel, backendFilter, modelFilter string) (map[string]struct{}, string) {
	modelFilterIDs := map[string]struct{}{}
	if modelFilter != "" {
		modelFilterIDs = catalogModelFilterIDs(backendModels, backendFilter, modelFilter)
	}
	resolvedModelID := ""
	if len(modelFilterIDs) == 1 {
		for id := range modelFilterIDs {
			resolvedModelID = id
		}
	}
	return modelFilterIDs, resolvedModelID
}

// modelFilterMatched reports whether any collected snapshot carries a public
// ID present in the model filter ID set.
func modelFilterMatched(modelSnaps []*modeldiagnostics.ModelSnapshot, modelFilterIDs map[string]struct{}) bool {
	for _, snap := range modelSnaps {
		if _, ok := modelFilterIDs[snap.Identity.PublicID]; ok {
			return true
		}
	}
	return false
}

// filterDiagnosticsModels filters typed model snapshots by backend, model,
// and status. Public ID collisions across backends cannot bleed because each
// snapshot carries its own lifecycle state.
func filterDiagnosticsModels(modelSnaps []*modeldiagnostics.ModelSnapshot, modelFilterIDs map[string]struct{}, backendFilter, modelFilter, statusFilter string) []*modeldiagnostics.ModelSnapshot {
	filtered := make([]*modeldiagnostics.ModelSnapshot, 0)
	for _, snap := range modelSnaps {
		if backendFilter != "" {
			if snap.Identity.BackendID != backendFilter {
				continue
			}
		}
		if modelFilter != "" {
			if _, ok := modelFilterIDs[snap.Identity.PublicID]; !ok {
				continue
			}
		}
		if statusFilter != "" {
			if snap.Lifecycle.CurrentState != statusFilter {
				continue
			}
		}
		filtered = append(filtered, snap)
	}
	return filtered
}

// unknownModelFilterError returns a diagnosticError when the model filter is
// nonempty but no collected snapshot matches any filtered model ID.
func unknownModelFilterError(modelFilter string, filtered []*modeldiagnostics.ModelSnapshot, modelSnaps []*modeldiagnostics.ModelSnapshot, modelFilterIDs map[string]struct{}) *diagnosticError {
	if modelFilter == "" {
		return nil
	}
	if len(filtered) != 0 {
		return nil
	}
	if modelFilterMatched(modelSnaps, modelFilterIDs) {
		return nil
	}
	return &diagnosticError{status: http.StatusBadRequest, message: "model not found"}
}

// handleDiagnosticsLimiterError maps a limiter or semaphore error to a
// diagnostics response. It returns true when a response was written.
func (h *Handler) handleDiagnosticsLimiterError(w http.ResponseWriter, err error) bool {
	if isContextTimeout(err) {
		writeDiagnosticsError(w, http.StatusGatewayTimeout, diagnosticsErrDiagnosticsRequestTimedOut)
		return true
	}
	if err == errDiagnosticsBusy {
		writeDiagnosticsError(w, http.StatusTooManyRequests, diagnosticsErrDiagnosticsTemporarilyUnavailable)
		return true
	}
	writeDiagnosticsError(w, http.StatusServiceUnavailable, diagnosticsErrDiagnosticsTemporarilyUnavailable)
	return true
}

// mapDiagnosticsServiceError maps a diagnostics service error to a
// diagnosticError. Context cancellations become 504; all other failures
// become 503 with a generic message.
func mapDiagnosticsServiceError(err error) *diagnosticError {
	if isContextTimeout(err) {
		return &diagnosticError{status: http.StatusGatewayTimeout, message: diagnosticsErrDiagnosticsRequestTimedOut}
	}
	return &diagnosticError{status: http.StatusServiceUnavailable, message: "diagnostics snapshot failed"}
}

// snapshotErrorFromErr maps a SnapshotWithSources error to a diagnosticError.
// Context cancellations become 504; all other failures become 503 with the
// model snapshot failed message.
func snapshotErrorFromErr(err error) *diagnosticError {
	if isContextTimeout(err) {
		return &diagnosticError{status: http.StatusGatewayTimeout, message: diagnosticsErrDiagnosticsRequestTimedOut}
	}
	return &diagnosticError{status: http.StatusServiceUnavailable, message: diagnosticsErrModelSnapshotFailed}
}

// diagnosticsModelInputs holds the bounded source inputs for model snapshot
// production.
type diagnosticsModelInputs struct {
	backendModels   []catalog.BackendModel
	snapshots       []diagnostics.Snapshot
	modelSnaps      []*modeldiagnostics.ModelSnapshot
	modelFilterIDs  map[string]struct{}
	resolvedModelID string
}

// collectDiagnosticsModelInputs collects diagnostics snapshots and the
// collector outputs, then builds typed model snapshots via
// SnapshotWithSources. It resolves model aliases against the cached catalog.
// All collector errors are mapped to diagnostics errors.
func (h *Handler) collectDiagnosticsModelInputs(ctx context.Context, args modelsQueryArgs) (*diagnosticsModelInputs, *diagnosticError) {
	// Get cached catalog models only. No upstream /v1/models or /show calls.
	backendModels := h.catalog.BackendModels()

	// Resolve model aliases against the cached backend catalog before any
	// collection. Alias values are used only for matching.
	modelFilterIDs, resolvedModelID := resolveModelFilterIDs(backendModels, args.backend, args.model)

	// Collect diagnostics snapshots (read-only, on-demand).
	snapshots, err := h.diagnostics.All(ctx)
	if err != nil {
		return nil, mapDiagnosticsServiceError(err)
	}

	var epoch int64 = 1
	var activities []*correlation.ActivityRecord
	var backendStates map[string]correlation.BackendState
	if h.activityCollector != nil {
		act, bs, collectorEpoch, diagErr := h.collectActivityInputs(ctx)
		if diagErr != nil {
			return nil, diagErr
		}
		activities = act
		backendStates = bs
		epoch = collectorEpoch
	}

	var identities []correlation.ActivityModelIdentity
	if h.aliasCollector != nil {
		var diagErr *diagnosticError
		identities, diagErr = h.collectAliasInputs(ctx, epoch, activities)
		if diagErr != nil {
			return nil, diagErr
		}
	}

	var deviceAssociations []correlation.ActivityDeviceAssociation
	var gpuReport *correlation.GPUReport
	if h.gpuCollector != nil {
		var diagErr *diagnosticError
		deviceAssociations, gpuReport, diagErr = h.collectGPUInputs(ctx, epoch, activities)
		if diagErr != nil {
			return nil, diagErr
		}
	}

	var lifecycleReport *correlation.LifecycleReport
	if h.lifecycleCollector != nil {
		var diagErr *diagnosticError
		lifecycleReport, diagErr = h.collectLifecycleInputs(ctx, epoch)
		if diagErr != nil {
			return nil, diagErr
		}
	}

	// Build bounded source inputs. SnapshotWithSources validates bounds before
	// any allocation and returns an error on violation.
	mdBackendModels := backendModelsToMD(backendModels)
	mdSnapshots := snapshotsToPointers(snapshots)
	sources := modeldiagnostics.SnapshotSources{
		BackendModels:         mdBackendModels,
		BackendSnapshots:      mdSnapshots,
		Activities:            activities,
		Identities:            identities,
		BackendStates:         backendStates,
		GPUReport:             gpuReport,
		DeviceAssociations:    deviceAssociations,
		LifecycleReport:       lifecycleReport,
		LifecycleAssociations: nil,
	}

	modelSnaps, err := h.modelDiagnostics.SnapshotWithSources(ctx, mdBackendModels, mdSnapshots, sources)
	if err != nil {
		return nil, snapshotErrorFromErr(err)
	}

	return &diagnosticsModelInputs{
		backendModels:   backendModels,
		snapshots:       snapshots,
		modelSnaps:      modelSnaps,
		modelFilterIDs:  modelFilterIDs,
		resolvedModelID: resolvedModelID,
	}, nil
}

// collectActivityInputs refreshes activity snapshots. When no activity
// collector is configured it returns empty inputs and epoch 1.
func (h *Handler) collectActivityInputs(ctx context.Context) ([]*correlation.ActivityRecord, map[string]correlation.BackendState, int64, *diagnosticError) {
	if h.activityCollector == nil {
		return nil, nil, 1, nil
	}
	actRecords, err := h.activityCollector.Refresh(ctx)
	if err != nil {
		if isContextTimeout(err) {
			return nil, nil, 0, &diagnosticError{status: http.StatusGatewayTimeout, message: diagnosticsErrDiagnosticsRequestTimedOut}
		}
		return nil, nil, 0, &diagnosticError{status: http.StatusServiceUnavailable, message: "activity collection failed"}
	}
	var backendStates map[string]correlation.BackendState
	stats := h.activityCollector.Stats()
	if bs, ok := stats["backend_states"].(map[string]correlation.BackendState); ok {
		backendStates = bs
	}
	return actRecords, backendStates, h.activityCollector.Epoch(), nil
}

// collectAliasInputs collects alias edges and resolves activity identities.
func (h *Handler) collectAliasInputs(ctx context.Context, epoch int64, activities []*correlation.ActivityRecord) ([]correlation.ActivityModelIdentity, *diagnosticError) {
	if h.aliasCollector == nil {
		return nil, nil
	}
	report, err := h.aliasCollector.Collect(ctx, epoch)
	if err != nil {
		if isContextTimeout(err) {
			return nil, &diagnosticError{status: http.StatusGatewayTimeout, message: diagnosticsErrDiagnosticsRequestTimedOut}
		}
		return nil, &diagnosticError{status: http.StatusServiceUnavailable, message: "alias collection failed"}
	}
	identities, err := correlation.ResolveActivityAliases(activities, report)
	if err != nil {
		return nil, &diagnosticError{status: http.StatusServiceUnavailable, message: "alias resolution failed"}
	}
	return identities, nil
}

// collectGPUInputs collects GPU samples and associates them with activities
// using the default association window.
func (h *Handler) collectGPUInputs(ctx context.Context, epoch int64, activities []*correlation.ActivityRecord) ([]correlation.ActivityDeviceAssociation, *correlation.GPUReport, *diagnosticError) {
	if h.gpuCollector == nil {
		return nil, nil, nil
	}
	report, err := h.gpuCollector.Collect(ctx, epoch)
	if err != nil {
		if isContextTimeout(err) {
			return nil, nil, &diagnosticError{status: http.StatusGatewayTimeout, message: diagnosticsErrDiagnosticsRequestTimedOut}
		}
		return nil, nil, &diagnosticError{status: http.StatusServiceUnavailable, message: "GPU collection failed"}
	}
	gpuReport := &report
	var samples []correlation.TimedDeviceSample
	for _, s := range report.Samples {
		samples = append(samples, correlation.TimedDeviceSample{
			BackendID: s.BackendID,
			Epoch:     s.Epoch,
			DeviceID:  s.DeviceID,
			Timestamp: s.Timestamp,
		})
	}
	assocs, err := correlation.AssociateActivities(activities, samples, correlation.DefaultAssociationWindow())
	if err != nil {
		return nil, nil, &diagnosticError{status: http.StatusServiceUnavailable, message: "GPU association failed"}
	}
	return assocs, gpuReport, nil
}

// collectLifecycleInputs collects lifecycle snapshots.
func (h *Handler) collectLifecycleInputs(ctx context.Context, epoch int64) (*correlation.LifecycleReport, *diagnosticError) {
	if h.lifecycleCollector == nil {
		return nil, nil
	}
	report, err := h.lifecycleCollector.Collect(ctx, epoch)
	if err != nil {
		if isContextTimeout(err) {
			return nil, &diagnosticError{status: http.StatusGatewayTimeout, message: diagnosticsErrDiagnosticsRequestTimedOut}
		}
		return nil, &diagnosticError{status: http.StatusServiceUnavailable, message: "lifecycle collection failed"}
	}
	return &report, nil
}

// calculateDiagnosticsModelCoverage determines the coverage string from the
// cached catalog and diagnostics capability state.
func (h *Handler) calculateDiagnosticsModelCoverage(backendModels []catalog.BackendModel, snapshots []diagnostics.Snapshot) string {
	cacheCoverage := h.cacheCoverage(backendModels)
	if cacheCoverage == "unknown" {
		return "unknown"
	}
	if cacheCoverage == "partial" {
		return "partial"
	}
	if h.capabilityCoverage(backendModels, snapshots) == "partial" {
		return "partial"
	}
	return "complete"
}

// cacheCoverage reports unknown when no configured backend has a cached
// catalog, partial when any configured backend lacks a cache, else complete.
func (h *Handler) cacheCoverage(backendModels []catalog.BackendModel) string {
	configured := make(map[string]bool, len(h.proxies))
	for id := range h.proxies {
		configured[id] = true
	}
	fetched := make(map[string]bool, len(backendModels))
	for _, bm := range backendModels {
		if !bm.FetchedAt.IsZero() {
			fetched[bm.BackendID] = true
		}
	}
	unfetched := 0
	for backendID := range configured {
		if !fetched[backendID] {
			unfetched++
		}
	}
	if unfetched == 0 {
		return "complete"
	}
	if unfetched == len(configured) {
		return "unknown"
	}
	return "partial"
}

// backendDowngradesCoverage reports whether a fetched, configured backend
// downgrades coverage to partial due to a stale or non-expected diagnostics
// capability state, or no cached model entries. Disabled and Unsupported
// states are expected.
func (h *Handler) backendDowngradesCoverage(bm catalog.BackendModel, snapByBackend map[string]diagnostics.Snapshot) bool {
	if time.Since(bm.FetchedAt) > modelStaleAge {
		return true
	}
	snap, ok := snapByBackend[bm.BackendID]
	if !ok {
		return true
	}
	for _, obs := range snap.Capabilities {
		switch obs.State {
		case diagnostics.Disabled, diagnostics.Unsupported:
			// Expected: backend does not expose this capability.
		case diagnostics.Unavailable, diagnostics.Unauthorized, diagnostics.Unknown:
			return true
		}
		if obs.Stale {
			return true
		}
	}
	return len(bm.Models) == 0
}

// capabilityCoverage returns partial when any fetched, configured backend has
// a stale or non-expected diagnostics capability state, or no cached model
// entries. Disabled and Unsupported states are expected. Otherwise complete.
func (h *Handler) capabilityCoverage(backendModels []catalog.BackendModel, snapshots []diagnostics.Snapshot) string {
	configured := make(map[string]bool, len(h.proxies))
	for id := range h.proxies {
		configured[id] = true
	}
	snapByBackend := make(map[string]diagnostics.Snapshot, len(snapshots))
	for _, snap := range snapshots {
		snapByBackend[snap.Backend] = snap
	}
	for _, bm := range backendModels {
		if !configured[bm.BackendID] {
			continue
		}
		if bm.FetchedAt.IsZero() {
			continue
		}
		if h.backendDowngradesCoverage(bm, snapByBackend) {
			return "partial"
		}
	}
	return "complete"
}

// pageDiagnosticsModels writes the models response, paging through the
// immutable snapshot store when the result exceeds the page size.
func (h *Handler) pageDiagnosticsModels(w http.ResponseWriter, filtered []*modeldiagnostics.ModelSnapshot, resolvedModelID string, coverage string, args modelsQueryArgs) {
	// Direct return when result fits in one page (no cursor).
	if len(filtered) <= args.limit {
		if filtered == nil {
			filtered = []*modeldiagnostics.ModelSnapshot{}
		}
		body := map[string]any{
			"generated_at": time.Now().Format(time.RFC3339),
			"coverage":     coverage,
			"models":       filtered,
		}
		if resolvedModelID != "" {
			body["resolved_model_id"] = resolvedModelID
		}
		writeDiagnosticsResponse(w, http.StatusOK, body)
		return
	}

	// Otherwise, page through the immutable snapshot store.
	if h.modelSnapshotPages == nil {
		writeDiagnosticsError(w, http.StatusServiceUnavailable, "model snapshot pager not configured")
		return
	}

	filterDigest := modelFilterDigest(args.backend, args.model, args.status, args.limit)

	h.pagerMu.Lock()
	var page modelSnapshotPageResponse
	page, err := h.modelSnapshotPages.createSnapshot(filtered, args.limit, filterDigest)
	h.pagerMu.Unlock()

	if err != nil {
		if errors.Is(err, errSnapshotExpired) {
			writeDiagnosticsError(w, http.StatusGone, "snapshot expired")
			return
		}
		if errors.Is(err, errSnapshotNotFound) {
			writeDiagnosticsError(w, http.StatusNotFound, "snapshot not found")
			return
		}
		writeDiagnosticsError(w, http.StatusServiceUnavailable, diagnosticsErrModelSnapshotFailed)
		return
	}

	body := map[string]any{
		"generated_at": page.GeneratedAt.Format(time.RFC3339),
		"coverage":     coverage,
		"models":       page.Records,
		"cursor":       page.Cursor,
	}
	if resolvedModelID != "" {
		body["resolved_model_id"] = resolvedModelID
	}
	writeDiagnosticsResponse(w, http.StatusOK, body)
}

// resumeDiagnosticsModelsPage resumes a frozen snapshot page from the cursor.
// It performs no collectors or network access, so it works during an outage.
func (h *Handler) resumeDiagnosticsModelsPage(w http.ResponseWriter, args modelsQueryArgs) {
	if h.modelSnapshotPages == nil {
		writeDiagnosticsError(w, http.StatusServiceUnavailable, "model snapshot pager not configured")
		return
	}
	filterDigest := modelFilterDigest(args.backend, args.model, args.status, args.limit)
	h.pagerMu.Lock()
	page, err := h.modelSnapshotPages.resumeSnapshot(args.cursor, args.limit, filterDigest)
	h.pagerMu.Unlock()
	if err != nil {
		if errors.Is(err, errSnapshotExpired) {
			writeDiagnosticsError(w, http.StatusGone, "snapshot expired")
			return
		}
		if errors.Is(err, errSnapshotNotFound) {
			writeDiagnosticsError(w, http.StatusNotFound, "snapshot not found")
			return
		}
		writeDiagnosticsError(w, http.StatusServiceUnavailable, diagnosticsErrModelSnapshotFailed)
		return
	}
	writeDiagnosticsResponse(w, http.StatusOK, map[string]any{
		"generated_at": page.GeneratedAt.Format(time.RFC3339),
		"coverage":     page.Coverage,
		"models":       page.Records,
		"cursor":       page.Cursor,
	})
}

// snapByBackend returns true if a diagnostics snapshot exists for the backend.
func snapByBackend(snapshots []diagnostics.Snapshot, backendID string) bool {
	for _, snap := range snapshots {
		if snap.Backend == backendID {
			return true
		}
	}
	return false
}

// cloneModelSnapshot returns a deep defensive copy of a model snapshot using
// bounded JSON marshaling. This ensures the serialized size is exact and the
// returned snapshot cannot alias the frozen store.
func cloneModelSnapshot(m *modeldiagnostics.ModelSnapshot) *modeldiagnostics.ModelSnapshot {
	data, err := json.Marshal(m)
	if err != nil {
		return nil
	}
	var clone *modeldiagnostics.ModelSnapshot
	if err := json.Unmarshal(data, &clone); err != nil {
		return nil
	}
	return clone
}

// catalogModelPublicIDForFilter returns the canonical public ID (prefix +
// local ID) of a catalog model row that matches the model filter, or false
// when the row does not match. Alias rows, missing/empty IDs, and
// nonmatching prefix-qualified/public/local/alias IDs are rejected. The
// backend parameter supplies the prefix used to build the public ID; backend
// filtering remains the caller's responsibility.
func catalogModelPublicIDForFilter(backend catalog.BackendModel, model map[string]any, filter string) (string, bool) {
	// Skip alias rows: canonical rows already carry their aliases.
	if catalog.IsAliasModelRow(model) {
		return "", false
	}
	localID, _ := model["id"].(string)
	if localID == "" {
		return "", false
	}
	publicID := backend.Prefix + localID
	if publicID == filter || localID == filter || catalogModelAliasMatches(model, backend.Prefix, filter) {
		return publicID, true
	}
	return "", false
}

// catalogModelFilterIDs maps a cached catalog ID or alias to canonical public
// model IDs. Alias metadata is consumed only for exact matching and is never
// returned to the caller.
func catalogModelFilterIDs(backendModels []catalog.BackendModel, backendFilter, modelFilter string) map[string]struct{} {
	ids := make(map[string]struct{})
	for _, bm := range backendModels {
		if backendFilter != "" && bm.BackendID != backendFilter {
			continue
		}
		for _, m := range bm.Models {
			if publicID, ok := catalogModelPublicIDForFilter(bm, m, modelFilter); ok {
				ids[publicID] = struct{}{}
			}
		}
	}
	return ids
}

func catalogModelAliasMatches(model map[string]any, prefix, modelFilter string) bool {
	for _, alias := range catalog.ModelAliases(model) {
		if modelFilter == alias || modelFilter == prefix+alias {
			return true
		}
	}
	return false
}

// backendModelsToMD converts catalog.BackendModel (cached catalog) to
// modeldiagnostics.BackendModel (diagnostics projection input). FetchedAt maps
// to ObservedAt.
func backendModelsToMD(bms []catalog.BackendModel) []modeldiagnostics.BackendModel {
	out := make([]modeldiagnostics.BackendModel, 0, len(bms))
	for _, bm := range bms {
		models := make([]map[string]any, 0, len(bm.Models))
		for _, m := range bm.Models {
			models = append(models, m)
		}
		out = append(out, modeldiagnostics.BackendModel{
			BackendID:  bm.BackendID,
			Prefix:     bm.Prefix,
			Models:     models,
			ObservedAt: bm.FetchedAt,
		})
	}
	return out
}

// snapshotsToPointers converts a slice of diagnostics.Snapshot values to a
// slice of pointers, as required by SnapshotWithSources.
func snapshotsToPointers(snaps []diagnostics.Snapshot) []*diagnostics.Snapshot {
	out := make([]*diagnostics.Snapshot, 0, len(snaps))
	for i := range snaps {
		out = append(out, &snaps[i])
	}
	return out
}

// serveDiagnosticsRequests returns a cursor-paged list of proxy-only request
// history records. Requires request_history to be enabled. It is a thin
// orchestrator: history-disabled is checked before query parsing, query
// validation is delegated to parseDiagnosticsRequestsQuery (no data refresh
// on invalid query), and rate limiting/semaphore/QueryPage is delegated to
// queryDiagnosticsHistory.
func (h *Handler) serveDiagnosticsRequests(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), diagnosticRequestTimeout)
	defer cancel()

	// If history is nil, return 503 history_disabled. Checked before query
	// parsing so an invalid query never triggers a history refresh.
	if h.history == nil {
		writeDiagnosticsError(w, http.StatusServiceUnavailable, "history_disabled")
		return
	}

	// Validate query parameters first: no data refresh on invalid query.
	args, diagErr := h.parseDiagnosticsRequestsQuery(r)
	if diagErr != nil {
		writeDiagnosticsError(w, diagErr.status, diagErr.message)
		return
	}

	// Query history through the rate limiter and semaphore.
	page, diagErr := h.queryDiagnosticsHistory(ctx, args)
	if diagErr != nil {
		writeDiagnosticsError(w, diagErr.status, diagErr.message)
		return
	}

	response := map[string]any{
		"records": page.Records,
	}
	if page.Cursor != "" {
		response["cursor"] = page.Cursor
	}
	writeDiagnosticsResponse(w, http.StatusOK, response)
}

// queryDiagnosticsPage admits the request through the rate limiter and
// semaphore, then queries the correlator for a stable, mutation-free page. It
// maps rate-limit and query errors to diagnosticErrors. includeCursor controls
// whether the cursor is passed to QueryPage. tooLargeStatus is the HTTP status
// returned when the result exceeds the cap. checkTooLargeFirst controls the
// error-check order: when true, ErrSnapshotTooLarge is checked before
// ErrSnapshotExpired/ErrSnapshotNotFound. The caller owns the response
// mapping.
func (h *Handler) queryDiagnosticsPage(ctx context.Context, args diagnosticsQueryArgs, includeCursor bool, tooLargeStatus int, checkTooLargeFirst bool) (*correlation.PageResponse, *diagnosticError) {
	// Rate limit and semaphore. Context timeout/cancellation maps to 504;
	// semaphore unavailability maps to 503.
	release, err := h.diagnoseRateLimit(ctx)
	if err != nil {
		if isContextTimeout(err) {
			return nil, &diagnosticError{status: http.StatusGatewayTimeout, message: diagnosticsErrDiagnosticsRequestTimedOut}
		}
		return nil, &diagnosticError{status: http.StatusServiceUnavailable, message: diagnosticsErrDiagnosticsTemporarilyUnavailable}
	}
	defer release()

	// Build filter and query the correlator for a stable, mutation-free page.
	filter := correlation.QueryFilter{
		TimeStart: args.since,
		TimeEnd:   args.until,
		BackendID: args.backend,
		// ModelID is intentionally blank: proxy-only records do not carry
		// model identity, so a model filter is not attributable.
		ModelID:   "",
		Status:    args.status,
		RequestID: args.requestID,
	}
	pageReq := correlation.PageRequest{Limit: args.limit}
	if includeCursor {
		pageReq.Cursor = args.cursor
	}
	page, err := h.history.QueryPage(filter, pageReq)
	if err != nil {
		if checkTooLargeFirst {
			if errors.Is(err, correlation.ErrSnapshotTooLarge) {
				return nil, &diagnosticError{status: tooLargeStatus, message: "too_large"}
			}
			if errors.Is(err, correlation.ErrSnapshotExpired) || errors.Is(err, correlation.ErrSnapshotNotFound) {
				return nil, &diagnosticError{status: http.StatusServiceUnavailable, message: "snapshot expired or not found"}
			}
		} else {
			if errors.Is(err, correlation.ErrSnapshotExpired) || errors.Is(err, correlation.ErrSnapshotNotFound) {
				return nil, &diagnosticError{status: http.StatusServiceUnavailable, message: "snapshot expired or not found"}
			}
			if errors.Is(err, correlation.ErrSnapshotTooLarge) {
				return nil, &diagnosticError{status: tooLargeStatus, message: "too_large"}
			}
		}
		return nil, &diagnosticError{status: http.StatusServiceUnavailable, message: "query failed"}
	}
	return &page, nil
}

// queryDiagnosticsHistory admits the request through the rate limiter and
// semaphore, then queries the correlator for a stable, mutation-free page. It
// maps rate-limit and query errors to diagnosticErrors. The caller owns the
// response mapping.
func (h *Handler) queryDiagnosticsHistory(ctx context.Context, args diagnosticsQueryArgs) (*correlation.PageResponse, *diagnosticError) {
	return h.queryDiagnosticsPage(ctx, args, diagnosticsRequestsPolicy.includeCursor, diagnosticsRequestsPolicy.tooLargeStatus, diagnosticsRequestsPolicy.checkTooLargeFirst)
}

// serveDiagnosticsRequestByID returns a single proxy-only request history
// record by exact bounded path segment. The request ID must match the
// proxy-generated format (32 lowercase hex or bounded req-<digits> fallback).
func (h *Handler) serveDiagnosticsRequestByID(w http.ResponseWriter, r *http.Request, id string) {
	ctx, cancel := context.WithTimeout(r.Context(), diagnosticRequestTimeout)
	defer cancel()

	// Validate exact bounded path segment: 32 lowercase hex or req-<digits>
	// fallback. No raw ID is echoed in errors.
	if !validRequestID(id) {
		writeDiagnosticsError(w, http.StatusNotFound, diagnosticsErrUnknownDiagnosticsPath)
		return
	}

	// If history is nil, return 503 history_disabled.
	if h.history == nil {
		writeDiagnosticsError(w, http.StatusServiceUnavailable, "history_disabled")
		return
	}

	// Rate limit and semaphore.
	release, err := h.diagnoseRateLimit(ctx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			writeDiagnosticsError(w, http.StatusGatewayTimeout, diagnosticsErrDiagnosticsRequestTimedOut)
			return
		}
		writeDiagnosticsError(w, http.StatusServiceUnavailable, diagnosticsErrDiagnosticsTemporarilyUnavailable)
		return
	}
	defer release()

	record, ok := h.history.GetByRequestID(id)
	if !ok {
		writeDiagnosticsError(w, http.StatusNotFound, "request not found")
		return
	}

	writeDiagnosticsResponse(w, http.StatusOK, record)
}

// serveDiagnosticsSummary aggregates proxy-only request history: count,
// outcomes by HTTP transport status, and duration from ProxyObservation.
// Pages through a single bounded QueryPage; exceeds cap returns too_large. It
// is a thin orchestrator: history-disabled is checked before query parsing,
// query validation is delegated to parseDiagnosticsSummaryQuery (no data
// refresh on invalid query), and rate limiting/semaphore/QueryPage is
// delegated to queryDiagnosticsSummary.
func (h *Handler) serveDiagnosticsSummary(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), diagnosticRequestTimeout)
	defer cancel()

	// If history is nil, return 503 history_disabled. Checked before query
	// parsing so an invalid query never triggers a history refresh.
	if h.history == nil {
		writeDiagnosticsError(w, http.StatusServiceUnavailable, "history_disabled")
		return
	}

	// Validate query parameters first: no data refresh on invalid query.
	args, diagErr := h.parseDiagnosticsSummaryQuery(r)
	if diagErr != nil {
		writeDiagnosticsError(w, diagErr.status, diagErr.message)
		return
	}

	// Query history through the rate limiter and semaphore.
	page, diagErr := h.queryDiagnosticsSummary(ctx, args)
	if diagErr != nil {
		writeDiagnosticsError(w, diagErr.status, diagErr.message)
		return
	}

	writeDiagnosticsResponse(w, http.StatusOK, summarizeDiagnosticsRecords(page.Records))
}

// parseDiagnosticsSummaryQuery validates and normalizes the query parameters
// for the /diagnostics/v1/summary endpoint. It performs no data refresh. On
// success it returns the normalized arguments. On failure it returns a
// diagnosticError the caller maps to a response.
func (h *Handler) parseDiagnosticsSummaryQuery(r *http.Request) (diagnosticsQueryArgs, *diagnosticError) {
	return h.parseDiagnosticsQuery(r, diagnosticsSummaryPolicy)
}

// queryDiagnosticsSummary admits the request through the rate limiter and
// semaphore, then queries the correlator for a stable, mutation-free page. It
// maps rate-limit and query errors to diagnosticErrors. The caller owns the
// response mapping.
func (h *Handler) queryDiagnosticsSummary(ctx context.Context, args diagnosticsQueryArgs) (*correlation.PageResponse, *diagnosticError) {
	return h.queryDiagnosticsPage(ctx, args, diagnosticsSummaryPolicy.includeCursor, diagnosticsSummaryPolicy.tooLargeStatus, diagnosticsSummaryPolicy.checkTooLargeFirst)
}

// summarizeDiagnosticsRecords aggregates count/outcomes/generation_outcomes/
// duration mean/p50/sample count from typed ProxyObservation only. HTTP 200 is
// never inferred as generation success. duration_ms carries sample_count even
// when there are no durations and mean/p50 only when samples exist.
func summarizeDiagnosticsRecords(records []*correlation.CorrelationRecord) map[string]any {
	count := len(records)
	outcomes := make(map[string]int)
	generationOutcomes := make(map[string]int)
	var durations []float64
	durationSampleCount := 0
	for _, rec := range records {
		outcomes[rec.Status]++
		if rec.ProxyObservation != nil {
			if rec.ProxyObservation.GenerationOutcome != "" {
				generationOutcomes[rec.ProxyObservation.GenerationOutcome]++
			}
			if rec.ProxyObservation.TotalDurationMs != nil {
				durations = append(durations, float64(*rec.ProxyObservation.TotalDurationMs))
				durationSampleCount++
			}
		}
	}

	var durationMS map[string]any
	if len(durations) > 0 {
		sort.Float64s(durations)
		mean := sumFloat64(durations) / float64(len(durations))
		p50Idx := int(math.Ceil(0.5*float64(len(durations)))) - 1
		if p50Idx < 0 {
			p50Idx = 0
		}
		durationMS = map[string]any{
			"mean":         mean,
			"p50":          durations[p50Idx],
			"sample_count": durationSampleCount,
		}
	} else {
		durationMS = map[string]any{
			"sample_count": durationSampleCount,
		}
	}

	return map[string]any{
		"count":                 count,
		"outcomes":              outcomes,
		"generation_outcomes":   generationOutcomes,
		"duration_ms":           durationMS,
		"duration_sample_count": durationSampleCount,
	}
}

func sumFloat64(values []float64) float64 {
	var sum float64
	for _, v := range values {
		sum += v
	}
	return sum
}

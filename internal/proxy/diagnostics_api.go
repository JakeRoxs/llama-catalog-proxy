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
	backendFilter := r.URL.Query().Get("backend")
	modelFilter := r.URL.Query().Get("model")
	statusFilter := r.URL.Query().Get("status")
	cursor := r.URL.Query().Get("cursor")
	offset := r.URL.Query().Get("offset")
	limitParam := r.URL.Query().Get("limit")

	// Reject duplicate query parameters.
	if err := rejectDuplicateQueryParams(r.URL.Query(), "backend", "model", "status", "cursor", "offset", "limit"); err != nil {
		writeDiagnosticsError(w, http.StatusBadRequest, diagnosticsErrDuplicateQueryParam)
		return
	}

	// Reject offset: the opaque cursor is the only pagination mechanism.
	if offset != "" {
		writeDiagnosticsError(w, http.StatusBadRequest, "offset unsupported")
		return
	}

	// Validate cursor length.
	if cursor != "" && len(cursor) > modelSnapshotCursorMaxLen {
		writeDiagnosticsError(w, http.StatusBadRequest, diagnosticsErrCursorTooLong)
		return
	}

	// Validate string filters (bounded valid UTF-8).
	for _, v := range []string{backendFilter, modelFilter, statusFilter} {
		if v != "" {
			if len(v) > maxQueryParamLength || !utf8.ValidString(v) {
				writeDiagnosticsError(w, http.StatusBadRequest, diagnosticsErrInvalidFilterValue)
				return
			}
		}
	}

	// Validate status filter against the finite exact set derived from
	// typed ModelSnapshot.Lifecycle.CurrentState.
	if statusFilter != "" && !validModelStatus[statusFilter] {
		writeDiagnosticsError(w, http.StatusBadRequest, diagnosticsErrInvalidStatusFilter)
		return
	}

	// Validate backend filter against configured backends.
	if backendFilter != "" {
		if _, ok := h.proxies[backendFilter]; !ok {
			writeDiagnosticsError(w, http.StatusBadRequest, "invalid backend filter")
			return
		}
	}

	// Validate limit.
	limit := modelSnapshotPageSize
	if limitParam != "" {
		l, err := strconv.Atoi(limitParam)
		if err != nil || l < 0 {
			writeDiagnosticsError(w, http.StatusBadRequest, diagnosticsErrInvalidLimit)
			return
		}
		if l > maxDiagnosticQueryLimit {
			l = maxDiagnosticQueryLimit
		}
		if l == 0 {
			l = modelSnapshotPageSize
		}
		limit = l
	}

	// Bound all requests. Cursor resumes have a concurrency cap but deliberately
	// bypass the collector refresh rate because they perform no upstream work.
	var release func()
	var err error
	if cursor != "" {
		release, err = h.acquireDiagnosticSlot(ctx)
	} else {
		release, err = h.diagnoseRateLimit(ctx)
	}
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			writeDiagnosticsError(w, http.StatusGatewayTimeout, diagnosticsErrDiagnosticsRequestTimedOut)
			return
		}
		if err == errDiagnosticsBusy {
			writeDiagnosticsError(w, http.StatusTooManyRequests, diagnosticsErrDiagnosticsTemporarilyUnavailable)
			return
		}
		writeDiagnosticsError(w, http.StatusServiceUnavailable, diagnosticsErrDiagnosticsTemporarilyUnavailable)
		return
	}
	defer release()

	// Early cursor-resume branch: resume from the stored frozen snapshot
	// only. No collectors, network, or refresh on continuation. This
	// guarantees frozen cursor pagination never hits upstream even during
	// an outage and preserves the initial generated_at and coverage.
	if cursor != "" {
		if h.modelSnapshotPages == nil {
			writeDiagnosticsError(w, http.StatusServiceUnavailable, "model snapshot pager not configured")
			return
		}
		filterDigest := modelFilterDigest(backendFilter, modelFilter, statusFilter, limit)
		h.pagerMu.Lock()
		page, err := h.modelSnapshotPages.resumeSnapshot(cursor, limit, filterDigest)
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
		return
	}

	// Get cached catalog models only. No upstream /v1/models or /show calls.
	backendModels := h.catalog.BackendModels()

	// Resolve model aliases against the cached backend catalog before any
	// collection. Alias values are used only for matching; snapshots continue to
	// expose canonical identities and never return raw alias metadata.
	modelFilterIDs := map[string]struct{}{}
	if modelFilter != "" {
		modelFilterIDs = catalogModelFilterIDs(backendModels, backendFilter, modelFilter)
		if len(modelFilterIDs) == 0 {
			writeDiagnosticsError(w, http.StatusBadRequest, "model not found")
			return
		}
	}
	resolvedModelID := ""
	if len(modelFilterIDs) == 1 {
		for id := range modelFilterIDs {
			resolvedModelID = id
		}
	}

	// Collect diagnostics snapshots (read-only, on-demand).
	snapshots, err := h.diagnostics.All(ctx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			writeDiagnosticsError(w, http.StatusGatewayTimeout, diagnosticsErrDiagnosticsRequestTimedOut)
			return
		}
		writeDiagnosticsError(w, http.StatusServiceUnavailable, "diagnostics snapshot failed")
		return
	}

	// Collect activity snapshots. The epoch is captured by Refresh and reused
	// by all subsequent collectors so the collection scope is consistent.
	var activities []*correlation.ActivityRecord
	var backendStates map[string]correlation.BackendState
	if h.activityCollector != nil {
		actRecords, err := h.activityCollector.Refresh(ctx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				writeDiagnosticsError(w, http.StatusGatewayTimeout, diagnosticsErrDiagnosticsRequestTimedOut)
				return
			}
			writeDiagnosticsError(w, http.StatusServiceUnavailable, "activity collection failed")
			return
		}
		activities = actRecords
		stats := h.activityCollector.Stats()
		if bs, ok := stats["backend_states"].(map[string]correlation.BackendState); ok {
			backendStates = bs
		}
	}

	epoch := int64(1)
	if h.activityCollector != nil {
		epoch = h.activityCollector.Epoch()
	}

	// Collect alias edges and resolve activity identities.
	var identities []correlation.ActivityModelIdentity
	if h.aliasCollector != nil {
		report, err := h.aliasCollector.Collect(ctx, epoch)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				writeDiagnosticsError(w, http.StatusGatewayTimeout, diagnosticsErrDiagnosticsRequestTimedOut)
				return
			}
			writeDiagnosticsError(w, http.StatusServiceUnavailable, "alias collection failed")
			return
		}
		identities, err = correlation.ResolveActivityAliases(activities, report)
		if err != nil {
			writeDiagnosticsError(w, http.StatusServiceUnavailable, "alias resolution failed")
			return
		}
	}

	// Collect GPU samples and associate with activities using the default
	// association window.
	var deviceAssociations []correlation.ActivityDeviceAssociation
	var gpuReport *correlation.GPUReport
	if h.gpuCollector != nil {
		report, err := h.gpuCollector.Collect(ctx, epoch)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				writeDiagnosticsError(w, http.StatusGatewayTimeout, diagnosticsErrDiagnosticsRequestTimedOut)
				return
			}
			writeDiagnosticsError(w, http.StatusServiceUnavailable, "GPU collection failed")
			return
		}
		gpuReport = &report
		// Convert GPUSample to TimedDeviceSample for association.
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
			writeDiagnosticsError(w, http.StatusServiceUnavailable, "GPU association failed")
			return
		}
		deviceAssociations = assocs
	}

	// Collect lifecycle snapshots and link current lifecycle to catalog models
	// via the modeldiagnostics projection.
	var lifecycleReport *correlation.LifecycleReport
	if h.lifecycleCollector != nil {
		report, err := h.lifecycleCollector.Collect(ctx, epoch)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				writeDiagnosticsError(w, http.StatusGatewayTimeout, diagnosticsErrDiagnosticsRequestTimedOut)
				return
			}
			writeDiagnosticsError(w, http.StatusServiceUnavailable, "lifecycle collection failed")
			return
		}
		lifecycleReport = &report
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
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			writeDiagnosticsError(w, http.StatusGatewayTimeout, diagnosticsErrDiagnosticsRequestTimedOut)
			return
		}
		writeDiagnosticsError(w, http.StatusServiceUnavailable, diagnosticsErrModelSnapshotFailed)
		return
	}

	// Apply filters against typed snapshots. No raw catalog maps are returned.
	// Status filter is based on each typed ModelSnapshot.Lifecycle.CurrentState
	// (loaded/unloaded/ready/starting/stopping/unknown); public ID collisions
	// across backends cannot bleed because each snapshot carries its own state.
	filtered := make([]*modeldiagnostics.ModelSnapshot, 0)
	for _, snap := range modelSnaps {
		if backendFilter != "" && snap.Identity.BackendID != backendFilter {
			continue
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

	// Unknown model filter: return 400 without echoing the raw ID.
	if modelFilter != "" && len(filtered) == 0 {
		found := false
		for _, snap := range modelSnaps {
			if _, ok := modelFilterIDs[snap.Identity.PublicID]; ok {
				found = true
				break
			}
		}
		if !found {
			writeDiagnosticsError(w, http.StatusBadRequest, "model not found")
			return
		}
	}

	// Determine coverage. Coverage is unknown only when no configured backend
	// has a cached catalog (all FetchedAt zero). It is partial when any
	// configured backend lacks a cache, any fetched backend's diagnostics
	// capability state is stale/unavailable/unauthorized/unknown, or any
	// configured backend has no cached model entry. Disabled capability state
	// is expected and is not a failure.
	coverage := "complete"
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
	// Any configured backend without a cache downgrades coverage.
	anyUnfetched := false
	for id := range configured {
		if !fetched[id] {
			anyUnfetched = true
			break
		}
	}
	if anyUnfetched {
		allUnfetched := true
		for id := range configured {
			if fetched[id] {
				allUnfetched = false
				break
			}
		}
		if allUnfetched {
			coverage = "unknown"
		} else {
			coverage = "partial"
		}
	}
	// For fetched backends, check diagnostics capability states and model
	// entries. Disabled is expected; stale/unavailable/unauthorized/unknown
	// downgrade to partial. A backend with no cached model entry also
	// downgrades to partial.
	snapByBackend := make(map[string]diagnostics.Snapshot, len(snapshots))
	for _, snap := range snapshots {
		snapByBackend[snap.Backend] = snap
	}
	for _, bm := range backendModels {
		if !configured[bm.BackendID] || bm.FetchedAt.IsZero() {
			continue
		}
		// Per-backend cached fetch older than modelStaleAge is stale.
		if time.Since(bm.FetchedAt) > modelStaleAge {
			coverage = "partial"
		}
		snap, ok := snapByBackend[bm.BackendID]
		if !ok {
			coverage = "partial"
			continue
		}
		for _, obs := range snap.Capabilities {
			switch obs.State {
			case diagnostics.Disabled:
				// Expected: backend does not expose this capability.
			case diagnostics.Unsupported:
				// Expected: backend does not expose this capability.
			case diagnostics.Unavailable, diagnostics.Unauthorized, diagnostics.Unknown:
				coverage = "partial"
			}
			if obs.Stale {
				coverage = "partial"
			}
		}
		if len(bm.Models) == 0 {
			coverage = "partial"
		}
	}

	// Direct return when result fits in one page (no cursor).
	if len(filtered) <= limit {
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

	filterDigest := modelFilterDigest(backendFilter, modelFilter, statusFilter, limit)

	h.pagerMu.Lock()
	var page modelSnapshotPageResponse
	page, err = h.modelSnapshotPages.createSnapshot(filtered, limit, filterDigest)
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
		"coverage":     page.Coverage,
		"models":       page.Records,
		"cursor":       page.Cursor,
	}
	if resolvedModelID != "" {
		body["resolved_model_id"] = resolvedModelID
	}
	writeDiagnosticsResponse(w, http.StatusOK, body)
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
// history records. Requires request_history to be enabled.
func (h *Handler) serveDiagnosticsRequests(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), diagnosticRequestTimeout)
	defer cancel()

	// If history is nil, return 503 history_disabled.
	if h.history == nil {
		writeDiagnosticsError(w, http.StatusServiceUnavailable, "history_disabled")
		return
	}

	// Validate query parameters first: no data refresh on invalid query.
	sinceParam := r.URL.Query().Get("since")
	untilParam := r.URL.Query().Get("until")
	backendFilter := r.URL.Query().Get("backend")
	modelFilter := r.URL.Query().Get("model")
	statusFilter := r.URL.Query().Get("status")
	requestIDFilter := r.URL.Query().Get("requestid")
	cursor := r.URL.Query().Get("cursor")
	limitParam := r.URL.Query().Get("limit")

	// Reject duplicate query parameters.
	if err := rejectDuplicateQueryParams(r.URL.Query(), "since", "until", "backend", "model", "status", "requestid", "cursor", "limit"); err != nil {
		writeDiagnosticsError(w, http.StatusBadRequest, diagnosticsErrDuplicateQueryParam)
		return
	}

	// Model filter on proxy request records cannot be attributed because the
	// correlation record ModelID is intentionally blank (the upstream activity
	// carries no X-Request-ID). A nonempty model filter returns explicit 501.
	if modelFilter != "" {
		writeDiagnosticsError(w, http.StatusNotImplemented, "model_attribution_unsupported")
		return
	}

	// Derive effective time bounds. Missing bounds default to a 24h window
	// ending at now. Reject any future bound and any window > 24h. The
	// normalized effective times are used for the query and any digest.
	since, until, err := parseDiagnosticsTimeRange(sinceParam, untilParam, time.Now())
	if err != nil {
		writeDiagnosticsError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Validate string filters (bounded valid UTF-8).
	for _, v := range []string{backendFilter, statusFilter, requestIDFilter} {
		if v != "" {
			if len(v) > maxQueryParamLength || !utf8.ValidString(v) {
				writeDiagnosticsError(w, http.StatusBadRequest, diagnosticsErrInvalidFilterValue)
				return
			}
		}
	}

	// Validate cursor length.
	if cursor != "" && len(cursor) > maxCursorLength {
		writeDiagnosticsError(w, http.StatusBadRequest, diagnosticsErrCursorTooLong)
		return
	}

	// Validate limit.
	limit := defaultDiagnosticQueryLimit
	if limitParam != "" {
		l, err := strconv.Atoi(limitParam)
		if err != nil || l < 0 {
			writeDiagnosticsError(w, http.StatusBadRequest, diagnosticsErrInvalidLimit)
			return
		}
		if l > maxDiagnosticQueryLimit {
			l = maxDiagnosticQueryLimit
		}
		limit = l
	}

	// Validate backend filter: must be a configured backend.
	if backendFilter != "" {
		if _, ok := h.proxies[backendFilter]; !ok {
			writeDiagnosticsError(w, http.StatusBadRequest, "unknown backend filter")
			return
		}
	}

	// Validate status filter: finite union of Correlation transport enums.
	if statusFilter != "" && !validTransportStatus(statusFilter) {
		writeDiagnosticsError(w, http.StatusBadRequest, diagnosticsErrInvalidStatusFilter)
		return
	}

	// Validate requestID filter format/bounds.
	if requestIDFilter != "" && !validRequestID(requestIDFilter) {
		writeDiagnosticsError(w, http.StatusBadRequest, "invalid request id filter")
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

	// Build filter and query the correlator for a stable, mutation-free page.
	filter := correlation.QueryFilter{
		TimeStart: since,
		TimeEnd:   until,
		BackendID: backendFilter,
		// ModelID is intentionally blank: proxy-only records do not carry
		// model identity, so a model filter is not attributable.
		ModelID:   "",
		Status:    statusFilter,
		RequestID: requestIDFilter,
	}
	pageReq := correlation.PageRequest{Cursor: cursor, Limit: limit}
	page, err := h.history.QueryPage(filter, pageReq)
	if err != nil {
		if errors.Is(err, correlation.ErrSnapshotExpired) || errors.Is(err, correlation.ErrSnapshotNotFound) {
			writeDiagnosticsError(w, http.StatusServiceUnavailable, "snapshot expired or not found")
			return
		}
		if errors.Is(err, correlation.ErrSnapshotTooLarge) {
			writeDiagnosticsError(w, http.StatusBadRequest, "too_large")
			return
		}
		writeDiagnosticsError(w, http.StatusServiceUnavailable, "query failed")
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
// Pages through a single bounded QueryPage; exceeds cap returns too_large.
func (h *Handler) serveDiagnosticsSummary(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), diagnosticRequestTimeout)
	defer cancel()

	// If history is nil, return 503 history_disabled.
	if h.history == nil {
		writeDiagnosticsError(w, http.StatusServiceUnavailable, "history_disabled")
		return
	}

	// Validate query parameters first: no data refresh on invalid query.
	sinceParam := r.URL.Query().Get("since")
	untilParam := r.URL.Query().Get("until")
	backendFilter := r.URL.Query().Get("backend")
	modelFilter := r.URL.Query().Get("model")
	statusFilter := r.URL.Query().Get("status")
	requestIDFilter := r.URL.Query().Get("requestid")
	cursor := r.URL.Query().Get("cursor")
	limitParam := r.URL.Query().Get("limit")

	// Reject duplicate query parameters.
	if err := rejectDuplicateQueryParams(r.URL.Query(), "since", "until", "backend", "model", "status", "requestid", "cursor", "limit"); err != nil {
		writeDiagnosticsError(w, http.StatusBadRequest, diagnosticsErrDuplicateQueryParam)
		return
	}

	// Model filter on proxy request records cannot be attributed because the
	// correlation record ModelID is intentionally blank.
	if modelFilter != "" {
		writeDiagnosticsError(w, http.StatusNotImplemented, "model_attribution_unsupported")
		return
	}

	// Parse and validate time range. Missing since defaults to now-24h,
	// missing until defaults to now. Future bounds, reversed ranges, and
	// ranges exceeding 24h are rejected.
	now := time.Now()
	since, until, err := parseDiagnosticsTimeRange(sinceParam, untilParam, now)
	if err != nil {
		writeDiagnosticsError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Validate string filters (bounded valid UTF-8).
	for _, v := range []string{backendFilter, statusFilter, requestIDFilter} {
		if v != "" {
			if len(v) > maxQueryParamLength || !utf8.ValidString(v) {
				writeDiagnosticsError(w, http.StatusBadRequest, diagnosticsErrInvalidFilterValue)
				return
			}
		}
	}

	// Validate cursor length.
	if cursor != "" && len(cursor) > maxCursorLength {
		writeDiagnosticsError(w, http.StatusBadRequest, diagnosticsErrCursorTooLong)
		return
	}

	// Validate limit.
	limit := maxDiagnosticSummaryLimit
	if limitParam != "" {
		l, err := strconv.Atoi(limitParam)
		if err != nil || l < 0 {
			writeDiagnosticsError(w, http.StatusBadRequest, diagnosticsErrInvalidLimit)
			return
		}
		if l > maxDiagnosticSummaryLimit {
			l = maxDiagnosticSummaryLimit
		}
		limit = l
	}

	// Validate backend filter: must be a configured backend.
	if backendFilter != "" {
		if _, ok := h.proxies[backendFilter]; !ok {
			writeDiagnosticsError(w, http.StatusBadRequest, "unknown backend filter")
			return
		}
	}

	// Validate status filter: finite union of Correlation transport enums.
	if statusFilter != "" && !validTransportStatus(statusFilter) {
		writeDiagnosticsError(w, http.StatusBadRequest, diagnosticsErrInvalidStatusFilter)
		return
	}

	// Validate requestID filter format/bounds.
	if requestIDFilter != "" && !validRequestID(requestIDFilter) {
		writeDiagnosticsError(w, http.StatusBadRequest, "invalid request id filter")
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

	// Single bounded QueryPage with filters; ErrSnapshotTooLarge means more
	// than cap.
	filter := correlation.QueryFilter{
		TimeStart: since,
		TimeEnd:   until,
		BackendID: backendFilter,
		// ModelID is intentionally blank: proxy-only records do not carry
		// model identity, so a model filter is not attributable.
		ModelID:   "",
		Status:    statusFilter,
		RequestID: requestIDFilter,
	}
	pageReq := correlation.PageRequest{Limit: limit}
	page, err := h.history.QueryPage(filter, pageReq)
	if err != nil {
		if errors.Is(err, correlation.ErrSnapshotTooLarge) {
			writeDiagnosticsError(w, http.StatusRequestEntityTooLarge, "too_large")
			return
		}
		if errors.Is(err, correlation.ErrSnapshotExpired) || errors.Is(err, correlation.ErrSnapshotNotFound) {
			writeDiagnosticsError(w, http.StatusServiceUnavailable, "snapshot expired or not found")
			return
		}
		writeDiagnosticsError(w, http.StatusServiceUnavailable, "query failed")
		return
	}

	// Aggregate count/outcomes/duration via only allowlisted typed
	// ProxyObservation. Distinguish HTTP transport status (record.Status)
	// from generation outcome (ProxyObservation.GenerationOutcome) and
	// duration sample count. HTTP 200 is never inferred as generation
	// success.
	count := len(page.Records)
	outcomes := make(map[string]int)
	generationOutcomes := make(map[string]int)
	var durations []float64
	durationSampleCount := 0
	for _, rec := range page.Records {
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

	writeDiagnosticsResponse(w, http.StatusOK, map[string]any{
		"count":                 count,
		"outcomes":              outcomes,
		"generation_outcomes":   generationOutcomes,
		"duration_ms":           durationMS,
		"duration_sample_count": durationSampleCount,
	})
}

func sumFloat64(values []float64) float64 {
	var sum float64
	for _, v := range values {
		sum += v
	}
	return sum
}

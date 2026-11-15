// Package correlation provides bounded, normalized collection of backend
// activity from llama-swap upstreams. It is independent of the request
// correlation store (correlator.go) and never merges activity records with
// proxy request records. Correlation with proxy request IDs is explicitly
// unsupported: the llama-swap ActivityLogEntry carries no X-Request-ID, so
// activity cannot be joined to a proxy request, and concurrent same-model
// requests are never merged.
package correlation

import (
	"math"
	"net/http"
	"strconv"
	"time"
)

// Collection budget constants.
const (
	// maxRecords bounds the total number of activity records retained.
	maxRecords = 1000
	// maxRowsPerBackend bounds the activity rows collected from one backend
	// per Refresh. The upstream /api/metrics/activity API accepts limits
	// between 1 and 999.
	maxRowsPerBackend = 100
	// maxResponseBytes bounds the size of a single backend response body.
	maxResponseBytes = 1 << 20
	// defaultTimeout bounds a single backend collection request.
	defaultTimeout = 2 * time.Second
	// defaultMaxAge is the age-based retention horizon. Records older than
	// this are evicted on read so stale data never looks fresh.
	defaultMaxAge = 24 * time.Hour
	// defaultMaxSize bounds the estimated retained size in bytes.
	defaultMaxSize = 10 * 1024 * 1024
)

// Explicit serialized collection facts. These appear on every record so the
// collection contract is self-documenting rather than comment-only.
const (
	// requestIDMappingUnsupported is the serialized value of
	// ActivityRecord.RequestIDMapping. llama-swap ActivityLogEntry carries no
	// X-Request-ID, so activity cannot be mapped to a proxy request.
	requestIDMappingUnsupported = "unsupported"
	// upstreamGenerationUnknown is the serialized value of
	// ActivityRecord.UpstreamGeneration. The upstream generation/restart is
	// not observable from activity rows.
	upstreamGenerationUnknown = "unknown"
	// epochScopeCollector is the serialized value of
	// ActivityRecord.EpochScope. The epoch is a collector-local collection
	// scope, not proof of an upstream process restart.
	epochScopeCollector = "collector"
)

// sentinelMissing is the documented upstream sentinel meaning "not reported".
// It is preserved as nil (missing), never as rejection or zero.
const sentinelMissing = -1

// saneInt64Bound is the maximum acceptable value for integer metrics.
const saneInt64Bound = math.MaxInt64

// saneRateBound is the maximum acceptable value for per-second rates.
const saneRateBound = 1e9

// allowedTasks is the fixed verified enum for the ActivityLogEntry.Task
// field. Values outside the allowlist are omitted (never attributed).
var allowedTasks = map[string]bool{
	"chat":             true,
	"embedding":        true,
	"rerank":           true,
	"token_count":      true,
	"image_generation": true,
	"image_edit":       true,
	"speech":           true,
	"transcription":    true,
	"decision":         true,
	"audio":            true,
	"realtime":         true,
}

// allowedRoutes is the fixed verified enum for the ActivityLogEntry.Route
// field ("local" or "peer"). Values outside the allowlist are omitted.
var allowedRoutes = map[string]bool{
	"local": true,
	"peer":  true,
}

// BackendState reports the outcome of one collection round for a backend. It
// never leaks upstream error bodies or credentials.
type BackendState struct {
	State         string    `json:"state"`
	Reason        string    `json:"reason,omitempty"`
	HTTPStatus    int       `json:"http_status,omitempty"`
	ObservedAt    time.Time `json:"observed_at,omitzero"`
	LastAttemptAt time.Time `json:"last_attempt_at,omitzero"`
	Rows          int       `json:"rows,omitempty"`
	// Coverage reports whether the returned rows were complete, partially
	// skipped (invalid entries), or truncated by the row cap / page total.
	Coverage string `json:"coverage,omitempty"`
	// Truncated is true when rows were dropped by the row cap or when
	// page.total indicates more history exists than was returned.
	Truncated bool `json:"truncated,omitempty"`
}

// ActivityRecord is a normalized, safe snapshot of one upstream activity row.
// Raw upstream metadata, logs, errors, commands, body text, source paths and
// peer labels are not copied. Documented numeric token/duration metrics are
// preserved as optional pointers (-1 means missing; zero is distinct when
// the source preserves field presence). Input/output zero is ambiguous because
// the upstream omits its internal usage-found flag, so those zero values stay
// unknown. The ModelID is resolved against the caller-supplied allowlist; unknown models
// are reported as "unknown" and never attributed. Task/Route are retained
// only when they belong to the fixed verified enum allowlists. IdentityConflict
// is true when a duplicate (backend, epoch, activity ID) with a conflicting
// timestamp was observed in the same collection epoch and not merged.
type ActivityRecord struct {
	BackendID  string    `json:"backend_id"`
	Epoch      int64     `json:"epoch"`
	ActivityID string    `json:"activity_id"` // upstream store-assigned ID, not a synthetic RequestID
	Timestamp  time.Time `json:"timestamp"`
	ModelID    string    `json:"model_id"` // allowlisted identity or "unknown"
	// ReportedModelRef is an opaque SHA256-derived reference of the raw model
	// label under this backend. It is computed from the source entry.Model
	// before the raw label is discarded, so alias resolution can match
	// activities to the catalog without storing names. The raw label is never
	// retained.
	ReportedModelRef   string `json:"reported_model_ref,omitempty"`
	Task               string `json:"task,omitempty"`
	Route              string `json:"route,omitempty"`
	Status             string `json:"status,omitempty"` // transport status derived from RespStatusCode
	IdentityConflict   bool   `json:"identity_conflict,omitempty"`
	RequestIDMapping   string `json:"request_id_mapping,omitempty"`
	UpstreamGeneration string `json:"upstream_generation,omitempty"`
	EpochScope         string `json:"epoch_scope,omitempty"`
	// DurationMS is the activity duration in milliseconds. -1 sentinel is
	// missing (nil). Explicit zero is a distinct pointer to zero.
	DurationMS *int64 `json:"duration_ms,omitempty"`
	// InputTokens is the number of input tokens. -1 sentinel is missing.
	InputTokens *int64 `json:"input_tokens,omitempty"`
	// OutputTokens is the number of output tokens. -1 sentinel is missing.
	OutputTokens *int64 `json:"output_tokens,omitempty"`
	// CachedTokens is the number of cached tokens. -1 sentinel is missing.
	CachedTokens *int64 `json:"cache_tokens,omitempty"`
	// DraftTokens is the number of draft tokens. -1 sentinel is missing.
	DraftTokens *int64 `json:"draft_tokens,omitempty"`
	// DraftAcceptedTokens is the number of accepted draft tokens. -1 sentinel is missing.
	DraftAcceptedTokens *int64 `json:"draft_acc_tokens,omitempty"`
	// ReasoningTokens is the number of reasoning tokens. -1 sentinel is missing.
	ReasoningTokens *int64 `json:"reasoning_tokens,omitempty"`
	// PromptTokensPerSecond is the prompt token rate. -1 sentinel is missing.
	PromptTokensPerSecond *float64 `json:"prompt_per_second,omitempty"`
	// TokensPerSecond is the token rate. -1 sentinel is missing.
	TokensPerSecond *float64 `json:"tokens_per_second,omitempty"`
}

// Clone returns a defensive copy of the record. All metric pointers are
// deep-copied so the clone never aliases the original.
func (r *ActivityRecord) Clone() *ActivityRecord {
	rec := &ActivityRecord{
		BackendID:          r.BackendID,
		Epoch:              r.Epoch,
		ActivityID:         r.ActivityID,
		Timestamp:          r.Timestamp,
		ModelID:            r.ModelID,
		ReportedModelRef:   r.ReportedModelRef,
		Task:               r.Task,
		Route:              r.Route,
		Status:             r.Status,
		IdentityConflict:   r.IdentityConflict,
		RequestIDMapping:   r.RequestIDMapping,
		UpstreamGeneration: r.UpstreamGeneration,
		EpochScope:         r.EpochScope,
	}
	// Deep copy metric pointers so the clone never aliases the original.
	if r.DurationMS != nil {
		v := *r.DurationMS
		rec.DurationMS = &v
	}
	if r.InputTokens != nil {
		v := *r.InputTokens
		rec.InputTokens = &v
	}
	if r.OutputTokens != nil {
		v := *r.OutputTokens
		rec.OutputTokens = &v
	}
	if r.CachedTokens != nil {
		v := *r.CachedTokens
		rec.CachedTokens = &v
	}
	if r.DraftTokens != nil {
		v := *r.DraftTokens
		rec.DraftTokens = &v
	}
	if r.DraftAcceptedTokens != nil {
		v := *r.DraftAcceptedTokens
		rec.DraftAcceptedTokens = &v
	}
	if r.ReasoningTokens != nil {
		v := *r.ReasoningTokens
		rec.ReasoningTokens = &v
	}
	if r.PromptTokensPerSecond != nil {
		v := *r.PromptTokensPerSecond
		rec.PromptTokensPerSecond = &v
	}
	if r.TokensPerSecond != nil {
		v := *r.TokensPerSecond
		rec.TokensPerSecond = &v
	}
	return rec
}

// metricInt64 converts an upstream int64 metric to a pointer. The -1 sentinel
// becomes nil (missing). Explicit zero becomes a pointer to zero. Other
// negative or out-of-bounds values become nil and malformed is set to true,
// indicating the row has partial coverage.
func metricInt64(value int64, malformed *bool) *int64 {
	if value == sentinelMissing {
		return nil
	}
	if value < 0 || value > saneInt64Bound {
		*malformed = true
		return nil
	}
	v := value
	return &v
}

// metricFloat64 converts an upstream float64 metric to a pointer. NaN/Inf
// become nil and malformed is set to true. The -1 sentinel becomes nil
// (missing). Explicit zero becomes a pointer to zero. Other negative or
// out-of-bounds values become nil and malformed is set to true.
func metricFloat64(value float64, malformed *bool) *float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		*malformed = true
		return nil
	}
	if value == float64(sentinelMissing) {
		return nil
	}
	if value < 0 || value > saneRateBound {
		*malformed = true
		return nil
	}
	v := value
	return &v
}

// transportStatus derives a finite transport status from an HTTP status code.
func transportStatus(code int) string {
	switch {
	case code >= 200 && code < 300:
		return "http_success"
	case code >= 300 && code < 400:
		return "http_other"
	case code >= 400 && code < 500:
		return "http_client_error"
	case code >= 500:
		return "http_server_error"
	default:
		return "transport_error"
	}
}

// activityPage mirrors the real llama-swap /api/metrics/activity response
// contract: {"data":[...],"page":1,"limit":N,"total":M,"total_pages":K}.
type activityPage struct {
	Data       []activityLogEntry `json:"data"`
	Page       int                `json:"page"`
	Limit      int                `json:"limit"`
	Total      int                `json:"total"`
	TotalPages int                `json:"total_pages"`
}

// activityLogEntry mirrors the real llama-swap store.ActivityLogEntry schema.
// Only the fields retained in ActivityRecord are decoded; the rest are
// ignored and never copied to output.
type activityLogEntry struct {
	ID              int                  `json:"id"`
	Timestamp       time.Time            `json:"timestamp"`
	Src             string               `json:"src"`
	Model           string               `json:"model"`
	Task            string               `json:"task,omitempty"`
	Route           string               `json:"route,omitempty"`
	PeerID          string               `json:"peer_id,omitempty"`
	ReqPath         string               `json:"req_path"`
	RespContentType string               `json:"resp_content_type"`
	RespStatusCode  int                  `json:"resp_status_code"`
	Tokens          activityTokenMetrics `json:"tokens"`
	DurationMs      *int                 `json:"duration_ms,omitempty"`
	CaptureID       string               `json:"capture_id,omitempty"`
	HasCapture      bool                 `json:"has_capture"`
	ErrorMsg        string               `json:"error_msg,omitempty"`
	Metadata        map[string]string    `json:"metadata,omitempty"`
}

type activityTokenMetrics struct {
	CachedTokens    int     `json:"cache_tokens"`
	DraftTokens     int     `json:"draft_tokens"`
	DraftAccTokens  int     `json:"draft_acc_tokens"`
	InputTokens     int     `json:"input_tokens"`
	OutputTokens    int     `json:"output_tokens"`
	ReasoningTokens int     `json:"reasoning_tokens"`
	PromptPerSecond float64 `json:"prompt_per_second"`
	TokensPerSecond float64 `json:"tokens_per_second"`
}

// Option configures an ActivityCollector.
type Option func(*ActivityCollector)

// WithKnownModels registers the allowlisted model identities. Activities
// whose model is not in the allowlist are reported as "unknown" and are never
// attributed to a known identity.
func WithKnownModels(models ...string) Option {
	return func(c *ActivityCollector) {
		if c.modelAllowlist == nil {
			c.modelAllowlist = make(map[string]bool, len(models))
		}
		for _, m := range models {
			if validIdentifier(m) {
				c.modelAllowlist[m] = true
			}
		}
	}
}

// WithMaxRecords bounds the total number of activity records retained. Zero
// or negative uses the default.
func WithMaxRecords(n int) Option {
	return func(c *ActivityCollector) {
		if n > 0 {
			c.maxRecords = n
		}
	}
}

// WithMaxRowsPerBackend bounds the number of activity rows collected from one
// backend per Refresh. Must be between 1 and 999 (the upstream API limit).
func WithMaxRowsPerBackend(n int) Option {
	return func(c *ActivityCollector) {
		if n > 0 && n <= 999 {
			c.maxRowsPerBackend = n
		}
	}
}

// WithMaxAge sets the age-based retention horizon. Records older than this
// are evicted on read.
func WithMaxAge(d time.Duration) Option {
	return func(c *ActivityCollector) {
		if d > 0 {
			c.maxAge = d
		}
	}
}

// WithMaxSize bounds the estimated retained size in bytes. Zero or negative
// uses the default.
func WithMaxSize(n int64) Option {
	return func(c *ActivityCollector) {
		if n > 0 {
			c.maxSize = n
		}
	}
}

// WithTimeout bounds a single backend collection request.
func WithTimeout(d time.Duration) Option {
	return func(c *ActivityCollector) {
		if d > 0 {
			c.timeout = d
		}
	}
}

// WithMaxResponseBytes bounds the size of a single backend response body.
func WithMaxResponseBytes(n int64) Option {
	return func(c *ActivityCollector) {
		if n > 0 {
			c.maxResponseBytes = n
		}
	}
}

// WithClient replaces the HTTP client, useful for tests. The no-redirect
// policy is always enforced on the resulting client so a custom client cannot
// bypass redirect refusal.
func WithClient(client *http.Client) Option {
	return func(c *ActivityCollector) {
		if client != nil {
			c.client = client
		}
	}
}

// WithNow replaces the clock, useful for deterministic tests. nil restores
// the real clock.
func WithNow(now func() time.Time) Option {
	return func(c *ActivityCollector) {
		if now != nil {
			c.now = now
		}
	}
}

// normalizeActivity converts one upstream activity log entry into a
// normalized safe record. Invalid entries (ID <= 0, missing timestamp,
// out-of-range status code) are rejected (nil) rather than fabricated into
// records. The model is resolved against the allowlist; unknown models are
// reported as "unknown". Task/Route are retained only when in the fixed
// verified enum allowlists. Raw upstream metadata, logs, errors, commands,
// body text, source paths, peers, and token metrics are not copied.
//
// Metric fields are preserved as optional pointers. The documented -1
// sentinel becomes nil (missing), never rejection or zero. InputTokens and
// OutputTokens use zero as a decoder default when the source omits them and
// expose no presence marker, so zero for those two fields stays unknown.
// Other counters retain explicit zero when the source uses -1 for missing.
// Invalid negative, NaN/Inf, or out-of-bounds values become nil and the row
// is marked as having partial metric coverage.
//
// The source entry.Model is a declared identity (the model the activity was
// for), not request proof. Before discarding it, an opaque
// ReportedModelRef is computed from (backendID, entry.Model); this enables
// alias ref matching without storing names. The raw label itself is never
// retained. ModelID remains "unknown" unless the model is on the explicit
// allowlist.
func (c *ActivityCollector) normalizeActivity(backendID string, entry activityLogEntry) (*ActivityRecord, bool) {
	if entry.ID <= 0 {
		return nil, false
	}
	if entry.Timestamp.IsZero() {
		return nil, false
	}
	if entry.RespStatusCode < 0 || entry.RespStatusCode > 599 {
		return nil, false
	}
	rec := &ActivityRecord{
		BackendID:          backendID,
		ActivityID:         strconv.Itoa(entry.ID),
		Timestamp:          entry.Timestamp,
		ModelID:            "unknown",
		Status:             transportStatus(entry.RespStatusCode),
		RequestIDMapping:   requestIDMappingUnsupported,
		UpstreamGeneration: upstreamGenerationUnknown,
		EpochScope:         epochScopeCollector,
	}
	// Compute the reported model reference from the raw model label before
	// discarding it. The reference is an opaque hash; the raw label is never
	// retained. This holds for both allowlisted and unknown models so alias
	// resolution can match activities to the catalog without storing names.
	if ref, ok := ModelReference(backendID, entry.Model); ok {
		rec.ReportedModelRef = ref
	}
	if c.modelAllowlist[entry.Model] {
		rec.ModelID = entry.Model
	}
	if allowedTasks[entry.Task] {
		rec.Task = entry.Task
	}
	if allowedRoutes[entry.Route] {
		rec.Route = entry.Route
	}
	// ActivityLogEntry omits Usage.Found. A fresh upstream row has zero-valued
	// token/rate fields when usage extraction did not succeed, except
	// ReasoningTokens=-1. In successful Usage.toUsage results, optional metrics
	// default to -1. Use that sentinel/nonzero values as evidence that usage
	// fields were populated; otherwise zeros are ambiguous and stay unknown.
	var malformed bool
	tokens := entry.Tokens
	usageEvidence := tokens.InputTokens != 0 || tokens.OutputTokens != 0 ||
		tokens.CachedTokens != 0 || tokens.DraftTokens != 0 || tokens.DraftAccTokens != 0 ||
		tokens.PromptPerSecond != 0 || tokens.TokensPerSecond != 0 || tokens.ReasoningTokens != -1
	if entry.DurationMs != nil {
		rec.DurationMS = metricInt64(int64(*entry.DurationMs), &malformed)
	}
	// The upstream stores these two API counts as plain int fields. Its usage
	// extractor defaults absent input/output counts to zero and does not retain
	// the internal Usage.Found bit, so zero is ambiguous in the activity API.
	if usageEvidence {
		if entry.Tokens.InputTokens != 0 {
			rec.InputTokens = metricInt64(int64(entry.Tokens.InputTokens), &malformed)
		}
		if entry.Tokens.OutputTokens != 0 {
			rec.OutputTokens = metricInt64(int64(entry.Tokens.OutputTokens), &malformed)
		}
		rec.CachedTokens = metricInt64(int64(entry.Tokens.CachedTokens), &malformed)
		rec.DraftTokens = metricInt64(int64(entry.Tokens.DraftTokens), &malformed)
		rec.DraftAcceptedTokens = metricInt64(int64(entry.Tokens.DraftAccTokens), &malformed)
		rec.ReasoningTokens = metricInt64(int64(entry.Tokens.ReasoningTokens), &malformed)
		rec.PromptTokensPerSecond = metricFloat64(entry.Tokens.PromptPerSecond, &malformed)
		rec.TokensPerSecond = metricFloat64(entry.Tokens.TokensPerSecond, &malformed)
	}
	return rec, !malformed
}

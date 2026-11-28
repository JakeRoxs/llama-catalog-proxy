// Package correlation provides bounded, normalized collection of backend
// alias identity evidence from llama-swap /v1/models listings. It maps
// reported model references (opaque hashes of raw model labels) to canonical
// model references using typed rows from the listing. It is independent of
// the activity and lifecycle collectors and never merges with request records.
// The collector issues no requests on construction and is safe for concurrent
// use.
package correlation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

// Collection budget constants.
const (
	// aliasMaxEdgesGlobal bounds the total number of alias edges returned
	// across all backends in a single Collect call.
	aliasMaxEdgesGlobal = 4096
	// aliasMaxRowsPerBackend bounds the number of /v1/models rows collected
	// from one backend per Collect call.
	aliasMaxRowsPerBackend = 1000
	// aliasMaxResponseBytes bounds the size of a single backend response body.
	aliasMaxResponseBytes = 1 << 20
	// aliasDefaultTimeout bounds a single backend collection request.
	aliasDefaultTimeout = 2 * time.Second
	// aliasPath is the only upstream path collected: GET /v1/models.
	aliasPath = "/v1/models"
	// aliasNumWorkers is the number of concurrent backend fetches.
	aliasNumWorkers = 4
	// aliasMaxActivities bounds the number of activities ResolveActivityAliases
	// can process in one call.
	aliasMaxActivities = 1000
)

// Backend-level finite collection states.
const (
	aliasEnabledState      = "collected"
	aliasDisabledState     = "disabled"
	aliasUnsupportedState  = "unsupported"
	aliasUnauthorizedState = "unauthorized"
	aliasUnavailableState  = "unavailable"
	aliasUnknownState      = "unknown"
)

// Edge-level resolution states.
// The serialized value for an exact resolution is "catalog_resolved"; the
// word "exact" is avoided because it collides with request-join terminology.
const (
	aliasStateExact     = "catalog_resolved"
	aliasStateAmbiguous = "ambiguous"
	aliasStateUnknown   = "unknown"
)

// Serialized provenance/attribution facts for activity identities resolved
// from the alias catalog.
const (
	// activityProvenanceCatalogDeclared states that the identity was resolved
	// from the catalog of declared reported identities, not runtime weights.
	activityProvenanceCatalogDeclared = "catalog_declared"
)

// Explicit serialized collection facts. These appear on every report so the
// collection contract is self-documenting rather than comment-only.
const (
	aliasUpstreamGenerationUnknown   = "unknown"
	aliasRequestIDMappingUnsupported = "unsupported"
	aliasEpochScopeCollector         = "collector"
	// aliasProvenanceCatalogDeclared states that the catalog is a provenance
	// catalog of reported identities, not runtime weights.
	aliasProvenanceCatalogDeclared = "declared_not_runtime_weights"
	aliasSourceCatalogMetadata     = "catalog_metadata"
)

// modelReferencePrefix is the opaque prefix for serialized SHA256-derived
// model references. It is used verbatim by ModelReference and isModelReference
// so the serialized reference format stays byte-identical across the package.
const modelReferencePrefix = "model-"

// ModelReference returns an opaque SHA256-derived reference for a reported
// model ID under a backend. The digest is SHA256(backendID + NUL + reportedID)
// and the result is the lowercase hex of the first 16 bytes prefixed with
// "model-". The raw ID is never retained; only the hash is returned. Invalid
// (empty, overlong, or non-UTF-8) IDs return false.
func ModelReference(backendID, reportedID string) (string, bool) {
	if !validIdentifier(backendID) || !validIdentifier(reportedID) {
		return "", false
	}
	if !utf8.ValidString(backendID) || !utf8.ValidString(reportedID) {
		return "", false
	}
	h := sha256.New()
	h.Write([]byte(backendID))
	h.Write([]byte("\x00"))
	h.Write([]byte(reportedID))
	digest := h.Sum(nil)
	return modelReferencePrefix + hex.EncodeToString(digest[:16]), true
}

// aliasBackendInfo holds the configured backend and its parsed URL.
type aliasBackendInfo struct {
	config config.Backend
	url    *url.URL
}

// aliasBackendJob pairs a configured backend ID with its info so workers
// process backends in a deterministic order.
type aliasBackendJob struct {
	id string
	b  *aliasBackendInfo
}

// aliasBackendResult is the outcome of collecting one backend.
type aliasBackendResult struct {
	id    string
	edges []AliasEdge
	state AliasBackendState
}

// AliasBackendState reports the outcome of one collection round for a backend.
// It never leaks upstream error bodies or credentials.
type AliasBackendState struct {
	State         string    `json:"state"`
	Reason        string    `json:"reason,omitempty"`
	HTTPStatus    int       `json:"http_status,omitempty"`
	ObservedAt    time.Time `json:"observed_at,omitzero"`
	LastAttemptAt time.Time `json:"last_attempt_at,omitzero"`
	Rows          int       `json:"rows,omitempty"`
	Coverage      string    `json:"coverage,omitempty"`
	Truncated     bool      `json:"truncated,omitempty"`
}

// AliasEdge is a typed, bounded edge from a reported reference to its
// canonical reference. The ref and canonical_ref are opaque SHA256-derived
// references; raw model IDs are never retained.
type AliasEdge struct {
	BackendID    string `json:"backend_id"`
	Epoch        int64  `json:"epoch"`
	Ref          string `json:"ref"`
	CanonicalRef string `json:"canonical_ref,omitempty"`
	State        string `json:"state"` // exact, ambiguous, unknown
}

// AliasReport is the typed fixed-schema result of one Collect call. It
// contains per-backend states and edges, not a map of upstream raw JSON. The
// attribution fields are always serialized so the collection contract is
// self-documenting.
type AliasReport struct {
	BackendStates map[string]AliasBackendState `json:"backend_states"`
	Edges         []AliasEdge                  `json:"edges"`
	// Attribution limits: structural constraints of the collection method.
	UpstreamGenerationUnknown   string `json:"upstream_generation,omitempty"`
	RequestIDMappingUnsupported string `json:"request_id_mapping,omitempty"`
	EpochScopeCollector         string `json:"epoch_scope,omitempty"`
	ProvenanceCatalog           string `json:"provenance_catalog,omitempty"`
	// Coverage of the global collection.
	Coverage  string `json:"coverage,omitempty"` // complete, partial, truncated, unknown
	Truncated bool   `json:"truncated,omitempty"`
	// Global bounds.
	TotalEdges         int `json:"total_edges,omitempty"`
	MaxEdges           int `json:"max_edges,omitempty"`
	MaxEdgesPerBackend int `json:"max_edges_per_backend,omitempty"`
}

// ActivityModelIdentity is the resolved identity of one activity's reported
// model reference. It never exposes the raw ModelID; only the opaque
// ReportedModelRef and, when unique, the CanonicalRef are reported.
type ActivityModelIdentity struct {
	BackendID        string `json:"backend_id"`
	Epoch            int64  `json:"epoch"`
	ActivityID       string `json:"activity_id"`
	ReportedModelRef string `json:"reported_model_ref,omitempty"`
	CanonicalRef     string `json:"canonical_ref,omitempty"`
	State            string `json:"state"`  // catalog_resolved, ambiguous, unknown
	Source           string `json:"source"` // catalog_metadata
	// Attribution: always serialized so the collection contract is
	// self-documenting rather than comment-only.
	Provenance         string `json:"provenance,omitempty"`
	RequestIDMapping   string `json:"request_id_mapping,omitempty"`
	UpstreamGeneration string `json:"upstream_generation,omitempty"`
}

// aliasModelPage mirrors the /v1/models list envelope: {"data":[...]}.
// Only ID and meta.llamaswap.* are decoded; Name, Description, and all other
// metadata keys are ignored and never copied to output.
type aliasModelPage struct {
	Data []aliasRow `json:"data"`
}

// aliasRow is the private production decode type. Only ID and
// meta.llamaswap.type/modelID/aliases are decoded; Name, Description, and
// all other metadata keys are ignored and never copied to output.
type aliasRow struct {
	ID   string    `json:"id"`
	Meta aliasMeta `json:"meta,omitempty"`
}

type aliasMeta struct {
	LLaMaSwap aliasLLaMaSwap `json:"llamaswap,omitempty"`
}

type aliasLLaMaSwap struct {
	Type    string   `json:"type,omitempty"`
	ModelID string   `json:"modelID,omitempty"`
	Aliases []string `json:"aliases,omitempty"`
}

// NewAliasCollector returns a collector for the given backends. Only
// backends with Diagnostics == "llama_swap" are collected; all others are
// skipped and make no requests. Construction copies configuration and issues
// no requests. Unknown/disabled backends make no requests and are reported
// disabled. Construction validates the configured URL (absolute http/https,
// no query/fragment) and rejects invalid URLs as unsupported.
func NewAliasCollector(backends map[string]config.Backend) *AliasCollector {
	c := &AliasCollector{
		client: &http.Client{
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		now:               time.Now,
		backends:          make(map[string]*aliasBackendInfo, len(backends)),
		backendStates:     make(map[string]AliasBackendState, len(backends)),
		refreshGate:       make(chan struct{}, 1),
		timeout:           aliasDefaultTimeout,
		maxResponseBytes:  aliasMaxResponseBytes,
		maxEdges:          aliasMaxEdgesGlobal,
		maxRowsPerBackend: aliasMaxRowsPerBackend,
	}
	for id, cfg := range backends {
		if !validIdentifier(id) {
			continue
		}
		if cfg.Diagnostics != "llama_swap" {
			// Unknown/disabled backends make no requests.
			c.backendStates[id] = AliasBackendState{State: aliasDisabledState, Reason: "not_enabled"}
			continue
		}
		u, err := url.Parse(cfg.URL)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.RawQuery != "" || u.Fragment != "" {
			c.backendStates[id] = AliasBackendState{State: aliasUnsupportedState, Reason: "invalid_url"}
			continue
		}
		headers := make(map[string]string, len(cfg.Headers))
		for key, value := range cfg.Headers {
			headers[key] = value
		}
		cfg.Headers = headers
		c.backends[id] = &aliasBackendInfo{config: cfg, url: u}
		c.backendStates[id] = AliasBackendState{State: aliasUnknownState, Reason: "not_collected", ObservedAt: c.now(), LastAttemptAt: c.now()}
	}
	return c
}

// AliasCollector collects normalized, bounded alias edges from llama-swap
// backends configured with Diagnostics == "llama_swap". It is safe for
// concurrent use and issues no requests on construction. The report is
// bounded by edge count (global 4096), response size, and timeout. Collect is
// serialized by a single-unit gate and runs at most four concurrent backend
// fetches.
type AliasCollector struct {
	mu                sync.Mutex
	client            *http.Client
	now               func() time.Time
	backends          map[string]*aliasBackendInfo
	backendStates     map[string]AliasBackendState
	refreshGate       chan struct{}
	timeout           time.Duration
	maxResponseBytes  int64
	maxEdges          int
	maxRowsPerBackend int
}

// Collect collects alias edges from all enabled backends and returns the
// normalized report. The epoch is caller-supplied and must be positive; it is
// a collector-local collection scope, not proof of an upstream process
// restart. Calls are serialized by a gate: a waiting caller honors context
// cancellation and returns ctx.Err() rather than blocking forever. The epoch
// is captured before any HTTP request. On context cancellation, no partial
// success is returned.
func (c *AliasCollector) Collect(ctx context.Context, epoch int64) (AliasReport, error) {
	if epoch <= 0 {
		return AliasReport{}, errors.New("epoch must be positive")
	}
	if err := ctx.Err(); err != nil {
		return AliasReport{}, err
	}
	// Serialize callers. Honor cancellation while waiting for the gate.
	select {
	case c.refreshGate <- struct{}{}:
	case <-ctx.Done():
		return AliasReport{}, ctx.Err()
	}
	defer func() { <-c.refreshGate }()
	if err := ctx.Err(); err != nil {
		return AliasReport{}, err
	}

	jobs := c.snapshotBackendJobs()
	results := c.runBackendWorkers(ctx, jobs, epoch)

	states := make(map[string]AliasBackendState, len(c.backendStates))
	for id, state := range c.backendStates {
		states[id] = state
	}
	edges := make([]AliasEdge, 0, c.maxEdges)
	states, edges, globalTruncated := c.mergeAliasResults(states, edges, results)

	// Honor parent cancellation: never return partial success on cancel.
	if err := ctx.Err(); err != nil {
		return AliasReport{}, err
	}

	report := AliasReport{
		BackendStates:               states,
		Edges:                       edges,
		UpstreamGenerationUnknown:   aliasUpstreamGenerationUnknown,
		RequestIDMappingUnsupported: aliasRequestIDMappingUnsupported,
		EpochScopeCollector:         aliasEpochScopeCollector,
		ProvenanceCatalog:           aliasProvenanceCatalogDeclared,
		MaxEdges:                    aliasMaxEdgesGlobal,
		MaxEdgesPerBackend:          aliasMaxRowsPerBackend,
	}

	coverage, truncated := determineCoverage(states, globalTruncated)
	report.Coverage = coverage
	report.Truncated = truncated
	report.TotalEdges = len(report.Edges)
	return report, nil
}

// snapshotBackendJobs returns the configured backends as sorted jobs for
// deterministic collection order. It holds the lock only for the snapshot.
func (c *AliasCollector) snapshotBackendJobs() []aliasBackendJob {
	c.mu.Lock()
	jobs := make([]aliasBackendJob, 0, len(c.backends))
	for id, b := range c.backends {
		jobs = append(jobs, aliasBackendJob{id: id, b: b})
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].id < jobs[j].id })
	c.mu.Unlock()
	return jobs
}

// runBackendWorkers runs at most aliasNumWorkers concurrent fetches. It feeds
// jobs into a bounded channel, honors context cancellation, and returns a
// result channel that is closed once all workers have finished.
func (c *AliasCollector) runBackendWorkers(ctx context.Context, jobs []aliasBackendJob, epoch int64) <-chan aliasBackendResult {
	n := aliasNumWorkers
	if n > len(jobs) {
		n = len(jobs)
	}
	jobChan := make(chan aliasBackendJob, 4)
	resultChan := make(chan aliasBackendResult, 4)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobChan {
				edges, state := c.collect(ctx, job.id, job.b, epoch)
				resultChan <- aliasBackendResult{id: job.id, edges: edges, state: state}
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
	return resultChan
}

// mergeAliasResults drains the result channel, merging each backend's state
// and edges into the report. Edges are sorted deterministically after each
// backend's edges are appended; the global edge cap is applied after every
// append. It returns the merged states, edges, and whether the global cap was
// reached.
func (c *AliasCollector) mergeAliasResults(states map[string]AliasBackendState, edges []AliasEdge, results <-chan aliasBackendResult) (map[string]AliasBackendState, []AliasEdge, bool) {
	globalTruncated := false
	for r := range results {
		states[r.id] = r.state
		edges = append(edges, r.edges...)
		sort.Slice(edges, func(i, j int) bool {
			if edges[i].BackendID != edges[j].BackendID {
				return edges[i].BackendID < edges[j].BackendID
			}
			if edges[i].Epoch != edges[j].Epoch {
				return edges[i].Epoch < edges[j].Epoch
			}
			return edges[i].Ref < edges[j].Ref
		})
		if len(edges) > c.maxEdges {
			clear(edges[c.maxEdges:])
			edges = edges[:c.maxEdges]
			globalTruncated = true
		}
	}
	return states, edges, globalTruncated
}

// determineCoverage computes the coverage string and truncated flag from
// per-backend states and the global truncation flag.
func determineCoverage(states map[string]AliasBackendState, globalTruncated bool) (coverage string, truncated bool) {
	coverage = "complete"
	truncated = globalTruncated
	for _, st := range states {
		if st.State != aliasEnabledState && st.State != aliasDisabledState {
			coverage = "unknown"
		}
		switch st.Coverage {
		case "unknown":
			coverage = "unknown"
		case "truncated":
			truncated = true
			if coverage != "unknown" {
				coverage = "partial"
			}
		case "partial":
			if coverage != "unknown" {
				coverage = "partial"
			}
		}
	}
	if truncated {
		coverage = "truncated"
	}
	return coverage, truncated
}

// collect fetches one page of /v1/models from a backend and normalizes it.
// It never follows redirects, never leaks credentials, and bounds the response
// body and row count. The only allowed upstream path is GET /v1/models.
func (c *AliasCollector) collect(ctx context.Context, id string, b *aliasBackendInfo, epoch int64) ([]AliasEdge, AliasBackendState) {
	state := c.newAliasBackendState()
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	page, state, ok := c.fetchAliasPage(ctx, b, state)
	if !ok {
		return nil, state
	}
	edges, truncated := c.normalizeAliasPage(id, epoch, page)
	state.State, state.Reason = aliasEnabledState, "ok"
	state.Rows = min(len(page.Data), c.maxRowsPerBackend)
	state.Truncated = truncated
	state.Coverage = "complete"
	if truncated {
		state.Coverage = "truncated"
	}
	return edges, state
}

func (c *AliasCollector) newAliasBackendState() AliasBackendState {
	return AliasBackendState{
		State: aliasUnknownState, Reason: "not_collected",
		ObservedAt: c.now(), LastAttemptAt: c.now(),
	}
}

func (c *AliasCollector) fetchAliasPage(ctx context.Context, backend *aliasBackendInfo, state AliasBackendState) (aliasModelPage, AliasBackendState, bool) {
	request, err := newBackendJSONRequest(ctx, backend.url.JoinPath(aliasPath).String(), backend.config.Headers, backend.config.APIKey)
	if err != nil {
		state.State, state.Reason = aliasUnsupportedState, "invalid_request"
		return aliasModelPage{}, state, false
	}
	response, err := c.client.Do(request)
	if err != nil {
		state.State, state.Reason = aliasUnavailableState, "request_failed"
		state.HTTPStatus = 0
		return aliasModelPage{}, state, false
	}
	defer response.Body.Close()
	state.HTTPStatus = response.StatusCode
	if state, ok := classifyAliasResponseStatus(response.StatusCode, state); !ok {
		return aliasModelPage{}, state, false
	}
	return c.decodeAliasResponse(response.Body, state)
}

func classifyAliasResponseStatus(statusCode int, state AliasBackendState) (AliasBackendState, bool) {
	switch {
	case statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden:
		state.State, state.Reason = aliasUnauthorizedState, "authorization_required"
	case statusCode == http.StatusNotFound || statusCode == http.StatusMethodNotAllowed:
		state.State, state.Reason = aliasUnsupportedState, "not_available"
	case statusCode >= 300 && statusCode < 400:
		state.State, state.Reason = aliasUnknownState, "redirect_refused"
	case statusCode != http.StatusOK:
		state.State, state.Reason = aliasUnavailableState, "http_error"
	default:
		return state, true
	}
	return state, false
}

func (c *AliasCollector) decodeAliasResponse(bodyReader io.Reader, state AliasBackendState) (aliasModelPage, AliasBackendState, bool) {
	body, err := io.ReadAll(io.LimitReader(bodyReader, c.maxResponseBytes+1))
	if err != nil {
		state.State, state.Reason = aliasUnavailableState, "response_read_failed"
		return aliasModelPage{}, state, false
	}
	if len(body) > int(c.maxResponseBytes) {
		state.State, state.Reason = aliasUnknownState, "response_too_large"
		return aliasModelPage{}, state, false
	}
	var page aliasModelPage
	if err := json.Unmarshal(body, &page); err != nil {
		state.State, state.Reason = aliasUnknownState, "invalid_json"
		return aliasModelPage{}, state, false
	}
	if page.Data == nil {
		state.State, state.Reason = aliasUnknownState, "malformed_page"
		return aliasModelPage{}, state, false
	}
	return page, state, true
}

func (c *AliasCollector) normalizeAliasPage(backendID string, epoch int64, page aliasModelPage) ([]AliasEdge, bool) {
	rows := page.Data
	truncated := len(rows) > c.maxRowsPerBackend
	if truncated {
		rows = rows[:c.maxRowsPerBackend]
	}
	edges, edgeLimitReached := buildEdges(backendID, rows, epoch)
	truncated = truncated || edgeLimitReached
	if truncated {
		// A clipped catalog may hide a later conflicting declaration. Preserve
		// ambiguity, but do not advertise a unique mapping from an incomplete graph.
		for i := range edges {
			edges[i] = markIncompleteAliasEdge(edges[i])
		}
	}
	return edges, truncated
}

func markIncompleteAliasEdge(edge AliasEdge) AliasEdge {
	if edge.State == aliasStateExact {
		edge.State = aliasStateUnknown
		edge.CanonicalRef = ""
	}
	return edge
}

// buildEdges converts one backend's /v1/models rows into typed alias edges.
// The catalog is built from typed rows: model rows (type=model) declare a
// canonical self edge and list aliases; alias rows (type=alias) declare an
// alias -> canonical mapping via modelID. Alias chains are not inferred:
// only direct edges are produced. Conflicting aliases and canonical
// collisions yield ambiguous edges (no winner is picked). An alias resolves
// only to an explicit type=model canonical; if the target is itself an alias
// or absent, the edge is unknown. Rows with missing or unknown types, or
// invalid IDs, yield unknown finite states.
//
// The number of distinct refs is bounded during iteration to
// aliasMaxEdgesGlobal so a large response cannot allocate an unbounded map.
// If the bound is reached, truncated is true and remaining rows are not
// inspected.
func buildEdges(backendID string, rows []aliasRow, epoch int64) ([]AliasEdge, bool) {
	builder := newAliasEdgeBuilder(backendID)
	for _, row := range rows {
		if builder.processRow(row) {
			break
		}
	}
	edges := buildAliasEdges(backendID, epoch, builder.rowRefs, builder.modelRefs, builder.aliasTargets)
	return edges, builder.truncated
}

type aliasEdgeBuilder struct {
	backendID    string
	aliasTargets map[string]map[string]struct{}
	modelRefs    map[string]struct{}
	rowRefs      map[string]struct{}
	refSeen      map[string]struct{}
	truncated    bool
}

func newAliasEdgeBuilder(backendID string) *aliasEdgeBuilder {
	return &aliasEdgeBuilder{
		backendID:    backendID,
		aliasTargets: make(map[string]map[string]struct{}),
		modelRefs:    make(map[string]struct{}),
		rowRefs:      make(map[string]struct{}),
		refSeen:      make(map[string]struct{}),
	}
}

// addRef registers a distinct ref, bounding the total count so a large
// response cannot allocate an unbounded map. It returns true if the ref was
// registered (or already seen), false if the cap was reached and the ref was
// not inspected.
func addRef(refSeen map[string]struct{}, ref string, truncated *bool) bool {
	if _, ok := refSeen[ref]; ok {
		return true
	}
	if len(refSeen) >= aliasMaxEdgesGlobal {
		*truncated = true
		return false
	}
	refSeen[ref] = struct{}{}
	return true
}

// registerAliasTarget records that aliasRef points to targetRef, capping the
// number of targets at 2 so a third target flips the edge to ambiguous.
func registerAliasTarget(aliasTargets map[string]map[string]struct{}, aliasRef, targetRef string) {
	if aliasTargets[aliasRef] == nil {
		aliasTargets[aliasRef] = make(map[string]struct{})
	}
	if len(aliasTargets[aliasRef]) < 2 {
		aliasTargets[aliasRef][targetRef] = struct{}{}
	}
}

// processAliasRow processes one /v1/models row, updating the catalog maps. It
// returns true if the ref bound was reached and remaining rows must not be
// inspected.
func (b *aliasEdgeBuilder) processRow(row aliasRow) bool {
	ref, ok := ModelReference(b.backendID, row.ID)
	if !ok {
		// Invalid or overlong ID: never retained.
		return false
	}
	if !addRef(b.refSeen, ref, &b.truncated) {
		return true // cap reached
	}
	b.rowRefs[ref] = struct{}{}
	switch row.Meta.LLaMaSwap.Type {
	case "model":
		return b.processCanonicalRow(ref, row.Meta.LLaMaSwap.Aliases)
	case "alias":
		return b.processAliasMapping(ref, row.Meta.LLaMaSwap.ModelID)
	}
	// Other/missing types produce no edge; the ref is unknown below.
	return false
}

func (b *aliasEdgeBuilder) processCanonicalRow(ref string, aliases []string) bool {
	b.modelRefs[ref] = struct{}{}
	for _, alias := range aliases {
		aliasRef, ok := ModelReference(b.backendID, alias)
		if !ok {
			continue
		}
		if !addRef(b.refSeen, aliasRef, &b.truncated) {
			return true
		}
		b.rowRefs[aliasRef] = struct{}{}
		registerAliasTarget(b.aliasTargets, aliasRef, ref)
	}
	return false
}

func (b *aliasEdgeBuilder) processAliasMapping(ref, modelID string) bool {
	if modelID == "" {
		return false
	}
	canonicalRef, ok := ModelReference(b.backendID, modelID)
	if !ok {
		return false
	}
	if !addRef(b.refSeen, canonicalRef, &b.truncated) {
		return true
	}
	registerAliasTarget(b.aliasTargets, ref, canonicalRef)
	return false
}

// buildAliasEdges builds one edge per distinct ref and sorts them
// deterministically by (Ref, CanonicalRef).
func buildAliasEdges(backendID string, epoch int64, rowRefs, modelRefs map[string]struct{}, aliasTargets map[string]map[string]struct{}) []AliasEdge {
	edges := make([]AliasEdge, 0, len(rowRefs))
	// Row IDs and aliases explicitly declared on canonical rows produce edges.
	for ref := range rowRefs {
		_, isModel := modelRefs[ref]
		edges = append(edges, edgeForRef(backendID, epoch, ref, aliasTargets[ref], isModel, modelRefs))
	}

	sort.Slice(edges, func(i, j int) bool {
		if edges[i].Ref != edges[j].Ref {
			return edges[i].Ref < edges[j].Ref
		}
		return edges[i].CanonicalRef < edges[j].CanonicalRef
	})
	return edges
}

// edgeForRef builds one edge for a ref given its candidate canonical targets
// and whether it is an explicit model.
func edgeForRef(backendID string, epoch int64, ref string, targets map[string]struct{}, isModel bool, modelRefs map[string]struct{}) AliasEdge {
	// Build candidate canonical targets.
	candidates := make(map[string]struct{})
	if isModel {
		// Explicit model: a self edge is always a candidate.
		candidates[ref] = struct{}{}
	}
	for t := range targets {
		candidates[t] = struct{}{}
	}
	// A model ref that is also declared as an alias to a different target
	// collides with its self edge: ambiguous, no winner picked.
	if isModel && len(candidates) >= 2 {
		return AliasEdge{BackendID: backendID, Epoch: epoch, Ref: ref, State: aliasStateAmbiguous}
	}
	if isModel {
		// Explicit model: exact self edge to itself.
		return AliasEdge{BackendID: backendID, Epoch: epoch, Ref: ref, CanonicalRef: ref, State: aliasStateExact}
	}
	// Non-model ref (alias): resolve only to an explicit type=model canonical.
	switch len(candidates) {
	case 0:
		// No valid canonical target: unknown finite state, no guess.
		return AliasEdge{BackendID: backendID, Epoch: epoch, Ref: ref, State: aliasStateUnknown}
	case 1:
		// Exactly one canonical target: exact only if it is a known canonical
		// (explicit type=model row). If the target is itself an alias or
		// absent, no chain inference is made: unknown.
		canonicalRef := ""
		for t := range candidates {
			canonicalRef = t
			break
		}
		if _, ok := modelRefs[canonicalRef]; ok {
			return AliasEdge{BackendID: backendID, Epoch: epoch, Ref: ref, CanonicalRef: canonicalRef, State: aliasStateExact}
		}
		return AliasEdge{BackendID: backendID, Epoch: epoch, Ref: ref, State: aliasStateUnknown}
	default:
		// Multiple canonical targets: ambiguous, never pick a winner.
		return AliasEdge{BackendID: backendID, Epoch: epoch, Ref: ref, State: aliasStateAmbiguous}
	}
}

// ResolveActivityAliases resolves each activity's ReportedModelRef against the
// alias catalog. It is pure: it does not mutate the activities or the report.
// The lookup is scoped by backend ID, collection epoch, and reported model
// reference. Activities are bounded to 1000 per call; the report's edges are
// bounded to 4096 before the lookup is built. The raw ModelID is never
// emitted; only the opaque ReportedModelRef and, when unique, the
// CanonicalRef are reported. An activity with an IdentityConflict is
// reported unknown (conflicting observations are never merged).
func ResolveActivityAliases(activities []*ActivityRecord, report AliasReport) ([]ActivityModelIdentity, error) {
	if len(activities) > aliasMaxActivities {
		return nil, errors.New("activity count exceeds bound")
	}
	edges := report.Edges
	if len(edges) > aliasMaxEdgesGlobal {
		return nil, errors.New("alias edge count exceeds bound")
	}
	if err := validateAliasEdges(edges); err != nil {
		return nil, err
	}
	if err := validateAliasActivities(activities); err != nil {
		return nil, err
	}
	lookup := buildAliasLookup(edges)

	identities := make([]ActivityModelIdentity, 0, len(activities))
	for _, a := range activities {
		if a == nil {
			continue
		}
		identities = append(identities, resolveAliasActivity(a, lookup))
	}
	return identities, nil
}

// validateAliasEdges validates each edge in the report. It returns a finite
// error (without caller data) if any edge is malformed.
func validateAliasEdges(edges []AliasEdge) error {
	for _, e := range edges {
		if !validIdentifier(e.BackendID) || e.Epoch <= 0 || !isModelReference(e.Ref) ||
			(e.CanonicalRef != "" && !isModelReference(e.CanonicalRef)) {
			return errors.New("invalid alias evidence")
		}
	}
	return nil
}

// validateAliasActivities validates each activity's identity fields. Nil
// activities are skipped. It returns a finite error if any activity is
// malformed.
func validateAliasActivities(activities []*ActivityRecord) error {
	for _, a := range activities {
		if a != nil && (!validIdentifier(a.BackendID) || !validIdentifier(a.ActivityID) || a.Epoch <= 0 ||
			(a.ReportedModelRef != "" && !isModelReference(a.ReportedModelRef))) {
			return errors.New("invalid activity identity")
		}
	}
	return nil
}

// buildAliasLookup builds a lookup keyed by (backendID, epoch, ref). Duplicate
// refs in the same scope are ambiguous: no winner is picked.
func buildAliasLookup(edges []AliasEdge) map[string]AliasEdge {
	lookup := make(map[string]AliasEdge, len(edges))
	for _, e := range edges {
		key := aliasEdgeKey(e.BackendID, e.Epoch, e.Ref)
		if existing, ok := lookup[key]; ok {
			lookup[key] = AliasEdge{
				BackendID: existing.BackendID,
				Epoch:     existing.Epoch,
				Ref:       existing.Ref,
				State:     aliasStateAmbiguous,
			}
		} else {
			lookup[key] = e
		}
	}
	return lookup
}

// resolveAliasActivity resolves one activity against the lookup, returning its
// identity. Conflicting identities are reported unknown.
func resolveAliasActivity(a *ActivityRecord, lookup map[string]AliasEdge) ActivityModelIdentity {
	identity := ActivityModelIdentity{
		BackendID:          a.BackendID,
		Epoch:              a.Epoch,
		ActivityID:         a.ActivityID,
		ReportedModelRef:   a.ReportedModelRef,
		Source:             aliasSourceCatalogMetadata,
		Provenance:         activityProvenanceCatalogDeclared,
		RequestIDMapping:   aliasRequestIDMappingUnsupported,
		UpstreamGeneration: upstreamGenerationUnknown,
	}
	// A conflicting (duplicate) activity identity is never merged; it is
	// reported unknown rather than attributed to a catalog ref.
	if a.IdentityConflict {
		identity.State = aliasStateUnknown
		return identity
	}
	key := aliasEdgeKey(a.BackendID, a.Epoch, a.ReportedModelRef)
	edge, ok := lookup[key]
	if !ok {
		// No catalog edge for this ref: unknown finite state.
		identity.State = aliasStateUnknown
		return identity
	}
	identity.State = edge.State
	if edge.State == aliasStateExact && edge.CanonicalRef != "" {
		identity.CanonicalRef = edge.CanonicalRef
	}
	// Ambiguous or unknown: canonical_ref is omitted.
	return identity
}
func isModelReference(ref string) bool {
	if len(ref) != len(modelReferencePrefix)+32 || !strings.HasPrefix(ref, modelReferencePrefix) {
		return false
	}
	for _, c := range ref[len(modelReferencePrefix):] {
		if !(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// aliasEdgeKey builds a deterministic lookup key for a (backendID, epoch, ref)
// triple.
func aliasEdgeKey(backendID string, epoch int64, ref string) string {
	return backendID + "\x00" + strconv.FormatInt(epoch, 10) + "\x00" + ref
}

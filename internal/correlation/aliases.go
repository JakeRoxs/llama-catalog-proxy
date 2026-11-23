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

	// Snapshot the backend jobs under the lock, then release it so collection
	// can proceed without holding the lock across HTTP.
	c.mu.Lock()
	jobs := make([]aliasBackendJob, 0, len(c.backends))
	for id, b := range c.backends {
		jobs = append(jobs, aliasBackendJob{id: id, b: b})
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].id < jobs[j].id })
	c.mu.Unlock()

	// Run the bounded worker pool: at most 4 goroutines reading from a bounded
	// job channel and sending to a bounded result channel. No goroutine is
	// dedicated to a backend.
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

	// Merge each result into the report. Edges are sorted deterministically.
	// The global edge cap is applied after draining all results.
	states := make(map[string]AliasBackendState, len(c.backendStates))
	for id, state := range c.backendStates {
		states[id] = state
	}
	edges := make([]AliasEdge, 0, c.maxEdges)
	globalTruncated := false
	for r := range resultChan {
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

	// Determine coverage from per-backend states and global truncation.
	coverage := "complete"
	truncated := globalTruncated
	for _, st := range report.BackendStates {
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
	report.Coverage = coverage
	report.Truncated = truncated
	report.TotalEdges = len(report.Edges)
	return report, nil
}

// collect fetches one page of /v1/models from a backend and normalizes it.
// It never follows redirects, never leaks credentials, and bounds the response
// body and row count. The only allowed upstream path is GET /v1/models.
func (c *AliasCollector) collect(ctx context.Context, id string, b *aliasBackendInfo, epoch int64) ([]AliasEdge, AliasBackendState) {
	state := AliasBackendState{
		State:         aliasUnknownState,
		Reason:        "not_collected",
		ObservedAt:    c.now(),
		LastAttemptAt: c.now(),
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	u := b.url.JoinPath(aliasPath)
	req, err := newBackendJSONRequest(ctx, u.String(), b.config.Headers, b.config.APIKey)
	if err != nil {
		state.State, state.Reason = aliasUnsupportedState, "invalid_request"
		return nil, state
	}
	response, err := c.client.Do(req)
	if err != nil {
		state.State, state.Reason = aliasUnavailableState, "request_failed"
		state.HTTPStatus = 0
		return nil, state
	}
	defer response.Body.Close()
	state.HTTPStatus = response.StatusCode

	// Check HTTP status codes first. 401/403, 404/405, 3xx, and other
	// non-200 statuses are returned without parsing the body.
	switch {
	case response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden:
		state.State, state.Reason = aliasUnauthorizedState, "authorization_required"
		return nil, state
	case response.StatusCode == http.StatusNotFound || response.StatusCode == http.StatusMethodNotAllowed:
		state.State, state.Reason = aliasUnsupportedState, "not_available"
		return nil, state
	case response.StatusCode >= 300 && response.StatusCode < 400:
		state.State, state.Reason = aliasUnknownState, "redirect_refused"
		return nil, state
	case response.StatusCode != http.StatusOK:
		state.State, state.Reason = aliasUnavailableState, "http_error"
		return nil, state
	}

	body, err := io.ReadAll(io.LimitReader(response.Body, c.maxResponseBytes+1))
	if err != nil {
		state.State, state.Reason = aliasUnavailableState, "response_read_failed"
		return nil, state
	}
	if len(body) > int(c.maxResponseBytes) {
		state.State, state.Reason = aliasUnknownState, "response_too_large"
		return nil, state
	}

	var page aliasModelPage
	if err := json.Unmarshal(body, &page); err != nil {
		state.State, state.Reason = aliasUnknownState, "invalid_json"
		return nil, state
	}
	// Validate required page field: data must be an array (non-nil). A bare
	// {} or {"data":null} is malformed.
	if page.Data == nil {
		state.State, state.Reason = aliasUnknownState, "malformed_page"
		return nil, state
	}

	// Bound rows per backend; detect truncation vs the row cap.
	rows := page.Data
	rowsTruncated := len(rows) > c.maxRowsPerBackend
	if len(rows) > c.maxRowsPerBackend {
		rows = rows[:c.maxRowsPerBackend]
	}

	edges, edgesTruncated := buildEdges(id, rows, epoch)
	state.State = aliasEnabledState
	state.Reason = "ok"
	state.Rows = len(rows)
	// Row-cap truncation and edge-cap truncation both signal a partial
	// collection; neither may hide conflicts (each ref's edge is complete).
	truncated := rowsTruncated || edgesTruncated
	if truncated {
		// A clipped catalog can hide a later conflicting declaration. Preserve
		// ambiguity, but never advertise a unique mapping from an incomplete graph.
		for i := range edges {
			if edges[i].State == aliasStateExact {
				edges[i].State = aliasStateUnknown
				edges[i].CanonicalRef = ""
			}
		}
		state.Coverage = "truncated"
		state.Truncated = true
	} else {
		state.Coverage = "complete"
	}
	return edges, state
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
	// aliasTargets maps a ref to the set of canonical refs it is declared to
	// point at. modelRefs tracks refs that are explicit models (self edges).
	// rowRefs tracks refs that appear as row IDs and therefore produce an edge.
	aliasTargets := make(map[string]map[string]struct{})
	modelRefs := make(map[string]struct{})
	rowRefs := make(map[string]struct{})
	// refSeen bounds every distinct ref (row IDs, aliases, canonical targets)
	// so a large response cannot allocate an unbounded map.
	refSeen := make(map[string]struct{})
	truncated := false

	addRef := func(ref string) bool {
		if _, ok := refSeen[ref]; ok {
			return true
		}
		if len(refSeen) >= aliasMaxEdgesGlobal {
			truncated = true
			return false
		}
		refSeen[ref] = struct{}{}
		return true
	}

	for _, row := range rows {
		ref, ok := ModelReference(backendID, row.ID)
		if !ok {
			// Invalid or overlong ID: never retained.
			continue
		}
		if !addRef(ref) {
			break
		}
		rowRefs[ref] = struct{}{}
		rowType := row.Meta.LLaMaSwap.Type
		modelID := row.Meta.LLaMaSwap.ModelID
		aliases := row.Meta.LLaMaSwap.Aliases

		if rowType == "model" {
			// Explicit canonical model: a self edge is always a candidate.
			modelRefs[ref] = struct{}{}
			// Canonical metadata lists aliases even when the source suppresses
			// separate alias rows (IncludeAliasesInList=false).
			for _, alias := range aliases {
				aliasRef, ok := ModelReference(backendID, alias)
				if !ok {
					continue
				}
				if !addRef(aliasRef) {
					break
				}
				rowRefs[aliasRef] = struct{}{}
				if aliasTargets[aliasRef] == nil {
					aliasTargets[aliasRef] = make(map[string]struct{})
				}
				if len(aliasTargets[aliasRef]) < 2 {
					aliasTargets[aliasRef][ref] = struct{}{}
				}
			}
		} else if rowType == "alias" {
			// Explicit alias -> canonical mapping via modelID.
			if modelID == "" {
				continue
			}
			canonicalRef, ok := ModelReference(backendID, modelID)
			if !ok {
				continue
			}
			if !addRef(canonicalRef) {
				break
			}
			if aliasTargets[ref] == nil {
				aliasTargets[ref] = make(map[string]struct{})
			}
			if len(aliasTargets[ref]) < 2 {
				aliasTargets[ref][canonicalRef] = struct{}{}
			}
		}
		// Other/missing types produce no edge; the ref is unknown below.
	}

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
	return edges, truncated
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
	// Bound the reported edges before allocating the lookup.
	edges := report.Edges
	if len(edges) > aliasMaxEdgesGlobal {
		return nil, errors.New("alias edge count exceeds bound")
	}
	for _, e := range edges {
		if !validIdentifier(e.BackendID) || e.Epoch <= 0 || !isModelReference(e.Ref) ||
			(e.CanonicalRef != "" && !isModelReference(e.CanonicalRef)) {
			return nil, errors.New("invalid alias evidence")
		}
	}
	for _, a := range activities {
		if a != nil && (!validIdentifier(a.BackendID) || !validIdentifier(a.ActivityID) || a.Epoch <= 0 ||
			(a.ReportedModelRef != "" && !isModelReference(a.ReportedModelRef))) {
			return nil, errors.New("invalid activity identity")
		}
	}
	// Build a lookup keyed by (backendID, epoch, ref). Duplicate refs in the
	// same scope are ambiguous: no winner is picked.
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

	identities := make([]ActivityModelIdentity, 0, len(activities))
	for _, a := range activities {
		if a == nil {
			continue
		}
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
			identities = append(identities, identity)
			continue
		}
		key := aliasEdgeKey(a.BackendID, a.Epoch, a.ReportedModelRef)
		edge, ok := lookup[key]
		if !ok {
			// No catalog edge for this ref: unknown finite state.
			identity.State = aliasStateUnknown
		} else {
			identity.State = edge.State
			if edge.State == aliasStateExact && edge.CanonicalRef != "" {
				identity.CanonicalRef = edge.CanonicalRef
			}
			// Ambiguous or unknown: canonical_ref is omitted.
		}
		identities = append(identities, identity)
	}
	return identities, nil
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

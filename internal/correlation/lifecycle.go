// Package correlation provides bounded, normalized collection of backend
// lifecycle (readiness) snapshots from llama-swap backends configured with
// Diagnostics == "llama_swap". It reads the read-only, finite GET /running
// endpoint (/home/jake/repos/llama-swap/internal/server/api.go:336
// handleRunning; registered GET /running at server.go:488). That endpoint
// never calls ServeHTTP, Process.Start, a healthcheck, or touches TTL, so it
// is non-mutating and safe to poll. It is safe for concurrent use and issues
// no requests on construction.
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
	"sync"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

// Collection budget constants.
const (
	// lifecycleMaxModelsGlobal bounds the total number of lifecycle models
	// returned across all backends in a single Collect call.
	lifecycleMaxModelsGlobal = 1000
	// lifecycleMaxModelsPerBackend bounds the number of lifecycle models
	// collected from one backend per Collect call.
	lifecycleMaxModelsPerBackend = 100
	// lifecycleMaxResponseBytes bounds the size of a single backend response body.
	lifecycleMaxResponseBytes = 1 << 20
	// lifecycleDefaultTimeout bounds a single backend collection request.
	lifecycleDefaultTimeout = 2 * time.Second
	// lifecyclePath is the only upstream path collected: GET /running.
	lifecyclePath = "/running"
)

// Backend-level finite collection states.
const (
	lifecycleEnabledState      = "enabled"
	lifecycleDisabledState     = "disabled"
	lifecycleUnsupportedState  = "unsupported"
	lifecycleUnauthorizedState = "unauthorized"
	lifecycleUnavailableState  = "unavailable"
	lifecycleUnknownState      = "unknown"
)

// Model-level strict source finite enum. Values mirror the source
// ProcessState enum (process.go:14-22) as observed through /running.
// /running lists only non-stopped processes, so the observed states are
// ready, starting, and stopping. Absent models are not reported; we never
// infer "unloaded" from absence.
const (
	lifecycleStateReady    = "ready"
	lifecycleStateStarting = "starting"
	lifecycleStateStopping = "stopping"
	lifecycleStateUnknown  = "unknown"
)

// Explicit serialized collection facts. These appear on every report/model so
// the collection contract is self-documenting rather than comment-only.
const (
	// lifecycleUpstreamGenerationUnknown is the serialized value of
	// LifecycleModel.UpstreamGeneration. /running carries no upstream
	// generation/restart identifier.
	lifecycleUpstreamGenerationUnknown = "unknown"
	// lifecycleAliasResolutionUnsupported is the serialized value of
	// LifecycleModel.AliasResolution. /running reports the configured model ID
	// only; alias resolution is not performed by the collector.
	lifecycleAliasResolutionUnsupported = "unsupported"
	// lifecycleRequestIDMappingUnsupported is the serialized value of
	// LifecycleModel.RequestIDMapping. /running carries no X-Request-ID, so
	// lifecycle observations cannot be mapped to a proxy request.
	lifecycleRequestIDMappingUnsupported = "unsupported"
	// lifecycleModelExecutionAttributionUnverified is the serialized value of
	// LifecycleModel.ModelExecutionAttribution. One readiness observation does
	// not prove that the backend actually executed a model or booted.
	lifecycleModelExecutionAttributionUnverified = "unverified"
)

// lifecycleBackendInfo holds the configured backend and its parsed URL.
type lifecycleBackendInfo struct {
	config config.Backend
	url    *url.URL
}

// lifecycleBackendJob pairs a configured backend ID with its info so workers
// process backends in a deterministic order.
type lifecycleBackendJob struct {
	id string
	b  *lifecycleBackendInfo
}

// lifecycleBackendResult is the outcome of collecting one backend.
type lifecycleBackendResult struct {
	id     string
	models []LifecycleModel
	state  LifecycleBackendState
}

// LifecycleReport is the typed fixed-schema result of one Collect call. It
// contains per-backend states and models, not a map of upstream raw JSON. The
// attribution fields are always serialized so the collection contract is
// self-documenting.
type LifecycleReport struct {
	BackendStates  map[string]LifecycleBackendState `json:"backend_states"`
	Models         []LifecycleModel                 `json:"models"`
	SnapshotScope  string                           `json:"snapshot_scope"`
	AbsenceState   string                           `json:"absence_state"`
	TransitionTime string                           `json:"transition_time"`
	// Attribution limits: structural constraints of the collection method.
	UpstreamGenerationUnknown           string `json:"upstream_generation,omitempty"`
	AliasResolutionUnsupported          string `json:"alias_resolution,omitempty"`
	RequestIDMappingUnsupported         string `json:"request_id_mapping,omitempty"`
	ModelExecutionAttributionUnverified string `json:"model_execution_attribution,omitempty"`
	// Coverage of the global collection.
	Coverage  string `json:"coverage,omitempty"` // complete, partial, truncated, unknown
	Truncated bool   `json:"truncated,omitempty"`
	// Global bounds.
	TotalModels         int `json:"total_models,omitempty"`
	MaxModels           int `json:"max_models,omitempty"`
	MaxModelsPerBackend int `json:"max_models_per_backend,omitempty"`
}

// LifecycleBackendState reports the outcome of one collection round for a
// backend. It never leaks upstream error bodies or credentials.
type LifecycleBackendState struct {
	State         string    `json:"state"`
	Reason        string    `json:"reason,omitempty"`
	HTTPStatus    int       `json:"http_status,omitempty"`
	ObservedAt    time.Time `json:"observed_at,omitzero"`
	LastAttemptAt time.Time `json:"last_attempt_at,omitzero"`
	Rows          int       `json:"rows,omitempty"`
	Coverage      string    `json:"coverage,omitempty"`
	Truncated     bool      `json:"truncated,omitempty"`
}

// LifecycleModel is a normalized, safe snapshot of one /running row.
// Raw upstream metadata (Cmd, Proxy, TTL, Name, Description) and credentials
// are never copied. ModelRef is an opaque SHA256 of the source canonical ID
// (using ModelReference's backend/NUL/model namespace); it is a reported-ID reference, not verified
// alias resolution or weights identity. ReadySince is a pointer set only when
// the source actually exposes a positive ready time; /running does not expose
// it, so it is nil here. No derived start/queue/coldstart times are computed.
// ObservedAt is the local collection time.
type LifecycleModel struct {
	BackendID  string     `json:"backend_id"`
	Epoch      int64      `json:"epoch"`
	ModelRef   string     `json:"model_ref"`             // opaque SHA256 of source canonical ID
	State      string     `json:"state"`                 // ready, starting, stopping, unknown
	ReadySince *time.Time `json:"ready_since,omitempty"` // source-positive only; nil when not exposed
	ObservedAt time.Time  `json:"observed_at"`
	// Attribution: always serialized so the collection contract is self-documenting.
	UpstreamGeneration        string `json:"upstream_generation,omitempty"`
	AliasResolution           string `json:"alias_resolution,omitempty"`
	RequestIDMapping          string `json:"request_id_mapping,omitempty"`
	ModelExecutionAttribution string `json:"model_execution_attribution,omitempty"`
}

// Clone returns a defensive copy of the model. time.Time has no Clone
// method, so the ready time is copied by value.
func (m *LifecycleModel) Clone() *LifecycleModel {
	var readySince *time.Time
	if m.ReadySince != nil {
		t := *m.ReadySince
		readySince = &t
	}
	return &LifecycleModel{
		BackendID:                 m.BackendID,
		Epoch:                     m.Epoch,
		ModelRef:                  m.ModelRef,
		State:                     m.State,
		ReadySince:                readySince,
		ObservedAt:                m.ObservedAt,
		UpstreamGeneration:        m.UpstreamGeneration,
		AliasResolution:           m.AliasResolution,
		RequestIDMapping:          m.RequestIDMapping,
		ModelExecutionAttribution: m.ModelExecutionAttribution,
	}
}

// lifecycleRunningPage mirrors the real /running response contract:
// {"running":[{"model","state","cmd","proxy","ttl","name","description"},...]}.
// Only model and state are decoded; cmd/proxy/ttl/name/description and any
// unknown JSON fields are ignored and never copied to output.
type lifecycleRunningPage struct {
	Running []lifecycleRunningModel `json:"running"`
}

// lifecycleRunningModel is the private production decode type. Pointer fields
// distinguish absent from explicit zero. Raw upstream metadata is never
// decoded.
type lifecycleRunningModel struct {
	Model string `json:"model"`
	State string `json:"state"`
}

// LifecycleCollector collects normalized, bounded lifecycle snapshots from
// llama-swap backends configured with Diagnostics == "llama_swap". It is safe
// for concurrent use and issues no requests on construction. The report is
// bounded by model count (global 1000, per backend 100), response size, and
// timeout. Collect is serialized by a single-unit gate and runs at most four
// concurrent backend fetches.
type LifecycleCollector struct {
	mu                  sync.Mutex
	client              *http.Client
	now                 func() time.Time
	backends            map[string]*lifecycleBackendInfo
	backendStates       map[string]LifecycleBackendState
	refreshGate         chan struct{}
	timeout             time.Duration
	maxResponseBytes    int64
	maxModels           int
	maxModelsPerBackend int
}

// NewLifecycleCollector returns a collector for the given backends. Only
// backends with Diagnostics == "llama_swap" are collected; all others are
// skipped and make no requests. Construction copies configuration and issues
// no requests. Unknown/disabled backends make no requests and are reported
// disabled. Construction validates the configured URL (absolute http/https,
// no query/fragment) and rejects invalid URLs as unsupported.
func NewLifecycleCollector(backends map[string]config.Backend) *LifecycleCollector {
	c := &LifecycleCollector{
		client: &http.Client{
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		now:                 time.Now,
		backends:            make(map[string]*lifecycleBackendInfo, len(backends)),
		backendStates:       make(map[string]LifecycleBackendState, len(backends)),
		refreshGate:         make(chan struct{}, 1),
		timeout:             lifecycleDefaultTimeout,
		maxResponseBytes:    lifecycleMaxResponseBytes,
		maxModels:           lifecycleMaxModelsGlobal,
		maxModelsPerBackend: lifecycleMaxModelsPerBackend,
	}
	for id, cfg := range backends {
		if !validIdentifier(id) {
			continue
		}
		if cfg.Diagnostics != "llama_swap" {
			// Unknown/disabled backends make no requests.
			c.backendStates[id] = LifecycleBackendState{State: lifecycleDisabledState, Reason: "not_enabled"}
			continue
		}
		u, err := url.Parse(cfg.URL)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.RawQuery != "" || u.Fragment != "" {
			c.backendStates[id] = LifecycleBackendState{State: lifecycleUnsupportedState, Reason: "invalid_url"}
			continue
		}
		headers := make(map[string]string, len(cfg.Headers))
		for key, value := range cfg.Headers {
			headers[key] = value
		}
		cfg.Headers = headers
		c.backends[id] = &lifecycleBackendInfo{config: cfg, url: u}
		c.backendStates[id] = LifecycleBackendState{State: lifecycleUnknownState, Reason: "not_collected", ObservedAt: c.now(), LastAttemptAt: c.now()}
	}
	return c
}

// Collect collects lifecycle snapshots from all enabled backends and returns
// the normalized report. The epoch is caller-supplied and must be positive;
// it is a collector-local collection scope, not proof of an upstream process
// restart. Calls are serialized by a gate: a waiting caller honors context
// cancellation and returns ctx.Err() rather than blocking forever. The epoch
// is captured before any HTTP request.
func (c *LifecycleCollector) Collect(ctx context.Context, epoch int64) (LifecycleReport, error) {
	release, err := c.collectLifecycleGate(ctx, epoch)
	if err != nil {
		return LifecycleReport{}, err
	}
	defer release()

	jobs := c.snapshotBackendJobs()
	results := c.runBackendWorkers(ctx, jobs, epoch)

	states, models, globalTruncated := c.mergeLifecycleResults(ctx, results)
	if err := ctx.Err(); err != nil {
		return LifecycleReport{}, err
	}

	coverage, truncated := computeLifecycleCoverage(states, globalTruncated)
	return buildLifecycleReport(states, models, coverage, truncated), nil
}

// collectLifecycleGate validates the epoch and context, then serializes the
// caller behind the single-unit refresh gate. It returns a release function
// that the caller must defer. A canceled context returns ctx.Err without
// acquiring the gate; if the context is canceled after the gate is acquired,
// the gate is released before returning the error.
func (c *LifecycleCollector) collectLifecycleGate(ctx context.Context, epoch int64) (func(), error) {
	if epoch <= 0 {
		return nil, errors.New("epoch must be positive")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Serialize callers. Honor cancellation while waiting for the gate.
	select {
	case c.refreshGate <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		<-c.refreshGate
		return nil, ctx.Err()
	}
	return func() { <-c.refreshGate }, nil
}

// snapshotBackendJobs copies the configured backends under the lock and
// returns them sorted by backend ID. The lock is released before returning so
// that HTTP collection never runs while holding the mutex.
func (c *LifecycleCollector) snapshotBackendJobs() []lifecycleBackendJob {
	c.mu.Lock()
	jobs := make([]lifecycleBackendJob, 0, len(c.backends))
	for id, b := range c.backends {
		jobs = append(jobs, lifecycleBackendJob{id: id, b: b})
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].id < jobs[j].id })
	c.mu.Unlock()
	return jobs
}

// runBackendWorkers runs at most four goroutines that each collect one backend
// from a bounded job channel and send results to a bounded result channel. The
// feeder pushes all jobs, stopping on context cancellation, then closes the job
// channel and waits for the workers before closing the result channel. The
// caller must drain the returned channel to EOF.
func (c *LifecycleCollector) runBackendWorkers(ctx context.Context, jobs []lifecycleBackendJob, epoch int64) <-chan lifecycleBackendResult {
	n := 4
	if n > len(jobs) {
		n = len(jobs)
	}
	jobChan := make(chan lifecycleBackendJob, 4)
	resultChan := make(chan lifecycleBackendResult, 4)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobChan {
				models, state := c.collect(ctx, job.id, job.b, epoch)
				resultChan <- lifecycleBackendResult{id: job.id, models: models, state: state}
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

// mergeLifecycleResults drains the result channel, merging per-backend states
// and models. Models are sorted by backend ID then ModelRef after each result,
// and the global cap is applied after each drain. It returns the merged
// states, the merged models, and whether the global cap truncated the model
// list.
func (c *LifecycleCollector) mergeLifecycleResults(ctx context.Context, results <-chan lifecycleBackendResult) (map[string]LifecycleBackendState, []LifecycleModel, bool) {
	states := make(map[string]LifecycleBackendState, len(c.backendStates))
	for id, state := range c.backendStates {
		states[id] = state
	}
	models := make([]LifecycleModel, 0, c.maxModels)
	globalTruncated := false
	for r := range results {
		states[r.id] = r.state
		models = append(models, r.models...)
		sort.Slice(models, func(i, j int) bool {
			if models[i].BackendID != models[j].BackendID {
				return models[i].BackendID < models[j].BackendID
			}
			return models[i].ModelRef < models[j].ModelRef
		})
		if len(models) > c.maxModels {
			clear(models[c.maxModels:])
			models = models[:c.maxModels]
			globalTruncated = true
		}
	}
	return states, models, globalTruncated
}

// computeLifecycleCoverage determines the coverage string and truncated flag
// from the per-backend states and the global truncation flag. Precedence:
// unknown outranks partial; global truncation overrides to truncated.
func computeLifecycleCoverage(states map[string]LifecycleBackendState, globalTruncated bool) (string, bool) {
	coverage := "complete"
	truncated := globalTruncated
	for _, st := range states {
		if st.State != lifecycleEnabledState && st.State != lifecycleDisabledState {
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

// buildLifecycleReport assembles the fixed-schema report from the merged
// collection state. TotalModels is the count of returned models.
func buildLifecycleReport(states map[string]LifecycleBackendState, models []LifecycleModel, coverage string, truncated bool) LifecycleReport {
	return LifecycleReport{
		BackendStates:                       states,
		Models:                              models,
		SnapshotScope:                       "non_stopped_local_processes",
		AbsenceState:                        "unknown",
		TransitionTime:                      "unavailable",
		UpstreamGenerationUnknown:           lifecycleUpstreamGenerationUnknown,
		AliasResolutionUnsupported:          lifecycleAliasResolutionUnsupported,
		RequestIDMappingUnsupported:         lifecycleRequestIDMappingUnsupported,
		ModelExecutionAttributionUnverified: lifecycleModelExecutionAttributionUnverified,
		MaxModels:                           lifecycleMaxModelsGlobal,
		MaxModelsPerBackend:                 lifecycleMaxModelsPerBackend,
		Coverage:                            coverage,
		Truncated:                           truncated,
		TotalModels:                         len(models),
	}
}

// lifecycleCollectionFailure centralizes the terminal collection-failure
// assignment: it sets the backend state/reason and returns the state for the
// caller to return. It preserves HTTPStatus (and ObservedAt/LastAttemptAt)
// that the caller set before the failure.
func lifecycleCollectionFailure(st LifecycleBackendState, stateCode, reason string) LifecycleBackendState {
	st.State = stateCode
	st.Reason = reason
	return st
}

// collect fetches one page of lifecycle from a backend and normalizes it. It
// never follows redirects, never leaks credentials, and bounds the response
// body and model count. The only allowed upstream path is GET /running.
// Complexity is kept low by delegating to pure helpers: request building,
// status classification, bounded page decode/validation, and row normalization.
func (c *LifecycleCollector) collect(ctx context.Context, id string, b *lifecycleBackendInfo, epoch int64) ([]LifecycleModel, LifecycleBackendState) {
	state := LifecycleBackendState{
		State:         lifecycleUnknownState,
		Reason:        "not_collected",
		ObservedAt:    c.now(),
		LastAttemptAt: c.now(),
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	req, err := buildLifecycleRequest(ctx, b)
	if err != nil {
		return nil, lifecycleCollectionFailure(state, lifecycleUnsupportedState, "invalid_request")
	}
	response, err := c.client.Do(req)
	if err != nil {
		state.HTTPStatus = 0
		return nil, lifecycleCollectionFailure(state, lifecycleUnavailableState, "request_failed")
	}
	defer response.Body.Close()
	state.HTTPStatus = response.StatusCode
	if st, reason, stop := classifyLifecycleStatus(response.StatusCode); stop {
		return nil, lifecycleCollectionFailure(state, st, reason)
	}
	page, err := readAndDecodeLifecyclePage(response.Body, c.maxResponseBytes)
	if err != nil {
		stateCode, reason := lifecycleErrorState(err)
		return nil, lifecycleCollectionFailure(state, stateCode, reason)
	}
	models, pageState := c.normalizeLifecyclePage(id, page.Running, c.maxModelsPerBackend, epoch)
	state.State = pageState.State
	state.Reason = pageState.Reason
	state.Rows = pageState.Rows
	state.Coverage = pageState.Coverage
	state.Truncated = pageState.Truncated
	return models, state
}

// buildLifecycleRequest builds the fixed, allowlisted request for one backend.
// Only GET /running is requested. Config headers are applied only when not
// already set on the request; the API key is added only when no Authorization
// or X-Api-Key header is present. Redirects are refused by the client's
// CheckRedirect policy, so this builder never enables following them.
func buildLifecycleRequest(ctx context.Context, b *lifecycleBackendInfo) (*http.Request, error) {
	u := b.url.JoinPath(lifecyclePath)
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

// classifyLifecycleStatus maps an HTTP status to the backend state/reason and
// whether collection should stop. A 200 continues; every other status stops
// with a documented state/reason. Redirects are refused (never followed) and
// reported as redirect_refused rather than followed. It delegates to the
// shared classifyBackendStatus, supplying the lifecycle collector's state
// constants.
func classifyLifecycleStatus(code int) (state, reason string, stop bool) {
	return classifyBackendStatus(code, backendStatusLabels{
		unauthorized: lifecycleUnauthorizedState,
		unsupported:  lifecycleUnsupportedState,
		unknown:      lifecycleUnknownState,
		unavailable:  lifecycleUnavailableState,
	})
}

var (
	errLifecyclePageTooLarge    = errors.New("lifecycle response body exceeds the configured byte limit")
	errLifecyclePageMalformed   = errors.New("lifecycle page running is missing or null")
	errLifecyclePageInvalidJSON = errors.New("lifecycle page is not valid JSON")
)

// readAndDecodeLifecyclePage reads a bounded response body, decodes the
// /running page, and validates required fields. The caller is responsible for
// closing the body. It returns a sentinel error for size/malformed/JSON
// failures so the caller can map each to the exact state/reason; other read
// errors map to the generic response_read_failed state.
func readAndDecodeLifecyclePage(body io.ReadCloser, maxResponseBytes int64) (*lifecycleRunningPage, error) {
	raw, err := io.ReadAll(io.LimitReader(body, maxResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read lifecycle response: %w", err)
	}
	if len(raw) > int(maxResponseBytes) {
		return nil, errLifecyclePageTooLarge
	}
	var page lifecycleRunningPage
	if err := json.Unmarshal(raw, &page); err != nil {
		return nil, fmt.Errorf("decode lifecycle page: %w", errLifecyclePageInvalidJSON)
	}
	// Validate required page fields: running must be an array (non-nil). A bare
	// {} or {"running":null} is malformed.
	if page.Running == nil {
		return nil, errLifecyclePageMalformed
	}
	return &page, nil
}

// lifecycleErrorState maps a read/decode error from readAndDecodeLifecyclePage
// to the exact backend state/reason. Read failures are unavailable, size and
// schema failures are unknown.
func lifecycleErrorState(err error) (state, reason string) {
	switch {
	case errors.Is(err, errLifecyclePageTooLarge):
		return lifecycleUnknownState, "response_too_large"
	case errors.Is(err, errLifecyclePageMalformed):
		return lifecycleUnknownState, "malformed_page"
	case errors.Is(err, errLifecyclePageInvalidJSON):
		return lifecycleUnknownState, "invalid_json"
	default:
		return lifecycleUnavailableState, "response_read_failed"
	}
}

// normalizeLifecyclePage bounds rows by the per-backend cap, normalizes each
// row, and assigns the final collected state. It returns the normalized
// models and the BackendState to assign (State/Reason/Rows/Coverage/Truncated).
// The caller preserves HTTPStatus/ObservedAt/LastAttemptAt from its own state.
func (c *LifecycleCollector) normalizeLifecyclePage(id string, entries []lifecycleRunningModel, maxModelsPerBackend int, epoch int64) ([]LifecycleModel, LifecycleBackendState) {
	models := make([]LifecycleModel, 0, maxModelsPerBackend)
	truncated := false
	for _, entry := range entries {
		if len(models) >= maxModelsPerBackend {
			truncated = true
			break
		}
		m := c.normalizeRunningModel(id, entry, epoch)
		if m == nil {
			continue
		}
		models = append(models, *m)
	}
	state := LifecycleBackendState{State: lifecycleEnabledState, Reason: "ok", Rows: len(models)}
	if truncated {
		state.Coverage = "truncated"
		state.Truncated = true
	} else {
		state.Coverage = "complete"
	}
	return models, state
}

// normalizeRunningModel converts one upstream /running row into a normalized
// safe model. Invalid entries (empty model ID) are rejected (nil) rather than
// fabricated. The model is reported by opaque ModelRef (SHA256 of source
// canonical ID); the raw ID, name, alias, description, Cmd, Proxy, TTL, and
// paths are never output. State is mapped strictly from the source finite enum;
// unrecognized state strings are reported unknown and never guessed.
// ReadySince is nil because /running does not expose a ready time; only
// source-positive values are set.
func (c *LifecycleCollector) normalizeRunningModel(backendID string, entry lifecycleRunningModel, epoch int64) *LifecycleModel {
	if _, ok := ModelReference(backendID, entry.Model); !ok {
		return nil
	}
	state := lifecycleStateUnknown
	switch entry.State {
	case "ready":
		state = lifecycleStateReady
	case "starting":
		state = lifecycleStateStarting
	case "stopping":
		state = lifecycleStateStopping
	}
	// ModelRef uses the shared opaque backend/model namespace so independently
	// collected catalog evidence can be compared without retaining names. It is not verified alias
	// resolution or weights identity.
	modelRef := modelRef(backendID, entry.Model)
	m := &LifecycleModel{
		BackendID:                 backendID,
		Epoch:                     epoch,
		ModelRef:                  modelRef,
		State:                     state,
		ReadySince:                nil, // /running does not expose a ready time.
		ObservedAt:                c.now(),
		UpstreamGeneration:        lifecycleUpstreamGenerationUnknown,
		AliasResolution:           lifecycleAliasResolutionUnsupported,
		RequestIDMapping:          lifecycleRequestIDMappingUnsupported,
		ModelExecutionAttribution: lifecycleModelExecutionAttributionUnverified,
	}
	return m
}

// modelRef returns an opaque SHA256 digest of the configured backend ID and the
// source canonical model ID (first 16 bytes, lowercase hex). The raw model ID,
// name, alias, description, Cmd, Proxy, TTL, paths, and credentials are never
// used.
func modelRef(backendID, modelID string) string {
	ref, _ := ModelReference(backendID, modelID)
	return ref
}

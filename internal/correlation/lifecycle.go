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
	BackendStates map[string]LifecycleBackendState `json:"backend_states"`
	Models        []LifecycleModel                 `json:"models"`
	SnapshotScope string                          `json:"snapshot_scope"`
	AbsenceState  string                          `json:"absence_state"`
	TransitionTime string                         `json:"transition_time"`
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
	if epoch <= 0 {
		return LifecycleReport{}, errors.New("epoch must be positive")
	}
	if err := ctx.Err(); err != nil {
		return LifecycleReport{}, err
	}
	// Serialize callers. Honor cancellation while waiting for the gate.
	select {
	case c.refreshGate <- struct{}{}:
	case <-ctx.Done():
		return LifecycleReport{}, ctx.Err()
	}
	defer func() { <-c.refreshGate }()
	if err := ctx.Err(); err != nil {
		return LifecycleReport{}, err
	}

	// Snapshot the backend jobs under the lock, then release it so collection
	// can proceed without holding the lock across HTTP.
	c.mu.Lock()
	jobs := make([]lifecycleBackendJob, 0, len(c.backends))
	for id, b := range c.backends {
		jobs = append(jobs, lifecycleBackendJob{id: id, b: b})
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].id < jobs[j].id })
	c.mu.Unlock()

	// Run the bounded worker pool: at most 4 goroutines reading from a bounded
	// job channel and sending to a bounded result channel. No goroutine is
	// dedicated to a backend.
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

	// Merge each result into the report. Models are sorted deterministically
	// by backend ID then ModelRef. Global count cap applied after draining.
	states := make(map[string]LifecycleBackendState, len(c.backendStates))
	for id, state := range c.backendStates {
		states[id] = state
	}
	models := make([]LifecycleModel, 0, c.maxModels)
	globalTruncated := false
	for r := range resultChan {
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
	// Honor parent cancellation: never return partial success on cancel.
	if err := ctx.Err(); err != nil {
		return LifecycleReport{}, err
	}

	report := LifecycleReport{
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
	}

	// Determine coverage from per-backend states and global truncation.
	coverage := "complete"
	truncated := globalTruncated
	for _, st := range report.BackendStates {
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
	report.Coverage = coverage
	report.Truncated = truncated
	report.TotalModels = len(report.Models)
	return report, nil
}

// collect fetches one page of lifecycle from a backend and normalizes it. It
// never follows redirects, never leaks credentials, and bounds the response
// body and model count. The only allowed upstream path is GET /running.
func (c *LifecycleCollector) collect(ctx context.Context, id string, b *lifecycleBackendInfo, epoch int64) ([]LifecycleModel, LifecycleBackendState) {
	state := LifecycleBackendState{
		State:         lifecycleUnknownState,
		Reason:        "not_collected",
		ObservedAt:    c.now(),
		LastAttemptAt: c.now(),
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	u := b.url.JoinPath(lifecyclePath)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		state.State, state.Reason = lifecycleUnsupportedState, "invalid_request"
		return nil, state
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
	response, err := c.client.Do(req)
	if err != nil {
		state.State, state.Reason = lifecycleUnavailableState, "request_failed"
		state.HTTPStatus = 0
		return nil, state
	}
	defer response.Body.Close()
	state.HTTPStatus = response.StatusCode

	// Check HTTP status codes first. 401/403, 404/405, 3xx, and other
	// non-200 statuses are returned without parsing the body.
	switch {
	case response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden:
		state.State, state.Reason = lifecycleUnauthorizedState, "authorization_required"
		return nil, state
	case response.StatusCode == http.StatusNotFound || response.StatusCode == http.StatusMethodNotAllowed:
		state.State, state.Reason = lifecycleUnsupportedState, "not_available"
		return nil, state
	case response.StatusCode >= 300 && response.StatusCode < 400:
		state.State, state.Reason = lifecycleUnknownState, "redirect_refused"
		return nil, state
	case response.StatusCode != http.StatusOK:
		state.State, state.Reason = lifecycleUnavailableState, "http_error"
		return nil, state
	}

	body, err := io.ReadAll(io.LimitReader(response.Body, c.maxResponseBytes+1))
	if err != nil {
		state.State, state.Reason = lifecycleUnavailableState, "response_read_failed"
		return nil, state
	}
	if len(body) > int(c.maxResponseBytes) {
		state.State, state.Reason = lifecycleUnknownState, "response_too_large"
		return nil, state
	}

	var page lifecycleRunningPage
	if err := json.Unmarshal(body, &page); err != nil {
		state.State, state.Reason = lifecycleUnknownState, "invalid_json"
		return nil, state
	}
	// Validate required page field: running must be an array (non-nil). A bare
	// {} or {"running":null} is malformed.
	if page.Running == nil {
		state.State, state.Reason = lifecycleUnknownState, "malformed_page"
		return nil, state
	}

	models := make([]LifecycleModel, 0, c.maxModelsPerBackend)
	truncated := false
	for _, entry := range page.Running {
		if len(models) >= c.maxModelsPerBackend {
			truncated = true
			break
		}
		m := c.normalizeRunningModel(id, entry, epoch)
		if m == nil {
			continue
		}
		models = append(models, *m)
	}
	state.State = lifecycleEnabledState
	state.Reason = "ok"
	state.Rows = len(models)
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

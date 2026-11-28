// Package correlation provides bounded, normalized collection of GPU metadata
// from llama-swap backends configured with Diagnostics == "llama_swap".
package correlation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

// Collection budget constants.
const (
	// gpuMaxSamplesGlobal bounds the total number of GPU samples returned across
	// all backends in a single Collect call.
	gpuMaxSamplesGlobal = 4096
	// gpuMaxDevicesPerBackend bounds the number of distinct GPU devices collected
	// from one backend per Collect call.
	gpuMaxDevicesPerBackend = 8
	// gpuMaxRecordsPerBackend bounds the number of records collected from one
	// backend per Collect call.
	gpuMaxRecordsPerBackend = 4096
	// gpuMaxResponseBytes bounds the size of a single backend response body.
	gpuMaxResponseBytes = 1 << 20
	// gpuDefaultTimeout bounds a single backend collection request.
	gpuDefaultTimeout = 2 * time.Second
)

// Finite collection states.
const (
	gpuEnabledState      = "enabled"
	gpuDisabledState     = "disabled"
	gpuUnsupportedState  = "unsupported"
	gpuUnauthorizedState = "unauthorized"
	gpuUnavailableState  = "unavailable"
	gpuUnknownState      = "unknown"
)

// Explicit serialized collection facts. These appear on every report so the
// collection contract is self-documenting rather than comment-only.
const (
	// gpuRequestIDMappingUnsupported is the serialized value of
	// GPUReport.RequestIDMapping. The /api/performance endpoint carries no
	// X-Request-ID, so GPU samples cannot be mapped to a proxy request.
	gpuRequestIDMappingUnsupported = "unsupported"
	// gpuUpstreamGenerationUnknown is the serialized value of
	// GPUReport.UpstreamGeneration. The upstream generation/restart is not
	// observable from the performance endpoint.
	gpuUpstreamGenerationUnknown = "unknown"
	// gpuEpochScopeCollector is the serialized value of
	// GPUReport.EpochScope. The epoch is a caller-supplied collection scope,
	// not proof of an upstream process restart.
	gpuEpochScopeCollector = "collector"
)

// GPUReport is the typed fixed-schema result of one Collect call. It contains
// per-backend states and samples, not a map of upstream raw JSON. The
// attribution fields are always serialized so the collection contract is
// self-documenting.
type GPUReport struct {
	BackendStates map[string]GPUBackendState `json:"backend_states"`
	Samples       []GPUSample                `json:"samples"`
	// Attribution limits: structural constraints of the collection method.
	SharedDeviceOnly            bool   `json:"shared_device_only"`
	ExecutionDeviceUnverified   bool   `json:"execution_device_unverified"`
	ResourceScope               string `json:"resource_scope"`
	LifecycleState              string `json:"lifecycle_state"`
	EpochScope                  string `json:"epoch_scope"`
	ClockAlignmentUnverified    bool   `json:"clock_alignment_unverified,omitempty"`
	UpstreamGenerationUnknown   string `json:"upstream_generation,omitempty"`
	RequestIDMappingUnsupported string `json:"request_id_mapping,omitempty"`
	// Coverage of the global collection.
	Coverage  string `json:"coverage,omitempty"` // complete, partial, truncated, unknown
	Truncated bool   `json:"truncated,omitempty"`
	// Global bounds.
	TotalSamples int `json:"total_samples,omitempty"`
	MaxSamples   int `json:"max_samples,omitempty"`
	MaxDevices   int `json:"max_devices,omitempty"`
}

// GPUBackendState reports the outcome of one collection round for a backend.
// It never leaks upstream error bodies or credentials.
type GPUBackendState struct {
	State         string    `json:"state"`
	Reason        string    `json:"reason,omitempty"`
	HTTPStatus    int       `json:"http_status,omitempty"`
	ObservedAt    time.Time `json:"observed_at,omitzero"`
	LastAttemptAt time.Time `json:"last_attempt_at,omitzero"`
	Devices       int       `json:"devices,omitempty"`
	Samples       int       `json:"samples,omitempty"`
	Coverage      string    `json:"coverage,omitempty"`
	Truncated     bool      `json:"truncated,omitempty"`
}

// GPUSample is a normalized, safe snapshot of one GPU observation.
// Raw upstream metadata (name, uuid, driver, PCI address), logs, errors,
// body text, source paths, and secrets are not copied. DeviceID is a
// canonical opaque identifier derived from a hash of the bounded source
// device index, never the raw name/UUID/PCI address.
type GPUSample struct {
	BackendID  string    `json:"backend_id"`
	Epoch      int64     `json:"epoch"`
	DeviceID   string    `json:"device_id"`   // 'gpu-' + hash digest
	Timestamp  time.Time `json:"timestamp"`   // source timestamp from the performance endpoint
	ObservedAt time.Time `json:"observed_at"` // local collection time
	// Optional numeric values with units, positively verified from the source.
	// Explicit zero values are reported as non-nil pointers; absent/unknown
	// values are nil. Units: GpuUtilPct/MemUtilPct are percent; MemUsedMB/MemTotalMB are MB.
	GpuUtilPct *float64 `json:"gpu_util_pct,omitempty"` // percent
	MemUtilPct *float64 `json:"mem_util_pct,omitempty"` // percent
	MemUsedMB  *int     `json:"mem_used_mb,omitempty"`  // MB
	MemTotalMB *int     `json:"mem_total_mb,omitempty"` // MB
}

// gpuBackendInfo holds the configured backend and its parsed URL.
type gpuBackendInfo struct {
	config config.Backend
	url    *url.URL
}

// gpuBackendJob pairs a configured backend ID with its info so workers process
// backends in a deterministic order.
type gpuBackendJob struct {
	id string
	b  *gpuBackendInfo
}

// gpuBackendResult is the outcome of collecting one backend.
type gpuBackendResult struct {
	id      string
	samples []GPUSample
	state   GPUBackendState
}

// gpuPerformancePage mirrors the real llama-swap /api/performance response
// contract: {"enabled": bool, "sys_stats": [...], "gpu_stats": [...]}.
// sys_stats is ignored and never decoded or copied to output.
type gpuPerformancePage struct {
	Enabled  *bool          `json:"enabled"`
	GPUStats *[]wireGpuStat `json:"gpu_stats"`
}

// wireGpuStat is the private production decode type. Pointer fields distinguish
// absent (nil) from explicit zero. Raw upstream metadata (name, uuid, driver,
// PCI address) is never decoded.
// Units: GpuUtilPct/MemUtilPct are percent; MemUsedMB/MemTotalMB are MB.
type wireGpuStat struct {
	ID         *int      `json:"id"`
	Timestamp  time.Time `json:"timestamp"`
	GpuUtilPct *float64  `json:"gpu_util_pct"`
	MemUtilPct *float64  `json:"mem_util_pct"`
	MemUsedMB  *int      `json:"mem_used_mb"`
	MemTotalMB *int      `json:"mem_total_mb"`
}

// GpuStat mirrors the real llama-swap perf.GpuStat schema
// (/home/jake/repos/llama-swap/internal/perf/types.go:5-19). It is retained as
// a public fixture type for tests; production decoding uses wireGpuStat.
// Units: GpuUtilPct/MemUtilPct/FanSpeedPct are percent; MemUsedMB/MemTotalMB
// are MB; TempC/VramTempC are Celsius; PowerDrawW is Watts.
type GpuStat struct {
	Timestamp   time.Time `json:"timestamp"`
	ID          int       `json:"id"`
	Name        string    `json:"name"`
	UUID        string    `json:"uuid"`
	TempC       int       `json:"temp_c"`
	VramTempC   int       `json:"vram_temp_c"`
	GpuUtilPct  float64   `json:"gpu_util_pct"`
	MemUtilPct  float64   `json:"mem_util_pct"`
	MemUsedMB   int       `json:"mem_used_mb"`
	MemTotalMB  int       `json:"mem_total_mb"`
	FanSpeedPct float64   `json:"fan_speed_pct"`
	PowerDrawW  float64   `json:"power_draw_w"`
}

// GPUCollector collects normalized, bounded GPU metadata snapshots from
// llama-swap backends configured with Diagnostics == "llama_swap". It is safe
// for concurrent use and issues no requests on construction. The report is
// bounded by sample count, device count, record count, response size, and
// timeout. Collect is serialized by a single-unit gate and runs at most four
// concurrent backend fetches.
type GPUCollector struct {
	mu               sync.Mutex
	client           *http.Client
	now              func() time.Time
	backends         map[string]*gpuBackendInfo
	backendStates    map[string]GPUBackendState
	refreshGate      chan struct{}
	timeout          time.Duration
	maxResponseBytes int64
}

// NewGPUCollector returns a collector for the given backends. Only
// backends with Diagnostics == "llama_swap" are collected; all others are
// skipped and make no requests. Construction copies configuration and issues
// no requests.
func NewGPUCollector(backends map[string]config.Backend) *GPUCollector {
	c := &GPUCollector{
		client: &http.Client{
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		now:              time.Now,
		backends:         make(map[string]*gpuBackendInfo, len(backends)),
		backendStates:    make(map[string]GPUBackendState, len(backends)),
		refreshGate:      make(chan struct{}, 1),
		timeout:          gpuDefaultTimeout,
		maxResponseBytes: gpuMaxResponseBytes,
	}
	for id, cfg := range backends {
		if !validIdentifier(id) {
			continue
		}
		if cfg.Diagnostics != "llama_swap" {
			// Unknown/disabled backends make no requests.
			c.backendStates[id] = GPUBackendState{State: gpuDisabledState, Reason: "not_enabled"}
			continue
		}
		u, err := url.Parse(cfg.URL)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.RawQuery != "" || u.Fragment != "" {
			c.backendStates[id] = GPUBackendState{State: gpuUnsupportedState, Reason: "invalid_url"}
			continue
		}
		headers := make(map[string]string, len(cfg.Headers))
		for key, value := range cfg.Headers {
			headers[key] = value
		}
		cfg.Headers = headers
		c.backends[id] = &gpuBackendInfo{config: cfg, url: u}
		c.backendStates[id] = GPUBackendState{State: gpuUnknownState, Reason: "not_collected", ObservedAt: c.now(), LastAttemptAt: c.now()}
	}
	return c
}

// Collect collects GPU metadata snapshots from all enabled backends and
// returns the normalized report. The epoch is caller-supplied and must be
// positive; it is a collector-local collection scope, not proof of an upstream
// restart. Calls are serialized by a gate: a waiting caller honors context
// cancellation and returns ctx.Err() rather than blocking forever.
//
// Collect is gate/context orchestration plus post-drain cancellation: it
// validates the epoch and context, serializes callers through the refresh
// gate, snapshots the sorted backend jobs, runs the bounded worker pool, and
// drains results to EOF through mergeResults. After the drain it honors parent
// cancellation and never returns a partial report on cancel.
func (c *GPUCollector) Collect(ctx context.Context, epoch int64) (GPUReport, error) {
	if epoch <= 0 {
		return GPUReport{}, errors.New("epoch must be positive")
	}
	if err := ctx.Err(); err != nil {
		return GPUReport{}, err
	}
	// Serialize callers. Honor cancellation while waiting for the gate.
	select {
	case c.refreshGate <- struct{}{}:
	case <-ctx.Done():
		return GPUReport{}, ctx.Err()
	}
	defer func() { <-c.refreshGate }()
	if err := ctx.Err(); err != nil {
		return GPUReport{}, err
	}

	// Snapshot the enabled backend jobs under the lock, then release it so
	// collection can proceed without holding the lock across HTTP.
	jobs := c.snapshotJobs()

	// Run the bounded worker pool and drain results to EOF. The feeder respects
	// context cancellation and stops feeding; workers drain the job channel to
	// completion; the result channel closes only after all workers have drained,
	// so there is no goroutine leak even when the parent context is canceled
	// mid-collection.
	resultChan := c.runWorkers(ctx, epoch, jobs)

	// Merge bounded batches into the global top-K window, re-sorting by
	// (BackendID, Timestamp, DeviceID) and truncating to the global max after
	// every result. The states map is a defensive copy of the initial states.
	samples, states, globalTruncated := mergeResults(c.backendStates, resultChan)

	// After draining workers, honor parent cancellation: never return partial
	// success on cancel.
	if err := ctx.Err(); err != nil {
		return GPUReport{}, err
	}

	return finalizeReport(states, samples, globalTruncated), nil
}

// snapshotJobs copies all enabled backend jobs under the collector lock and
// returns them sorted by backend ID. The caller must not hold c.mu.
func (c *GPUCollector) snapshotJobs() []gpuBackendJob {
	c.mu.Lock()
	jobs := make([]gpuBackendJob, 0, len(c.backends))
	for id, b := range c.backends {
		jobs = append(jobs, gpuBackendJob{id: id, b: b})
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].id < jobs[j].id })
	c.mu.Unlock()
	return jobs
}

// runWorkers starts at most four workers that fetch each job and send results
// to the returned channel. The feeder respects ctx cancellation and stops
// feeding; workers drain the job channel to completion. The result channel is
// closed only after all workers have drained, so the caller ranges it to EOF.
func (c *GPUCollector) runWorkers(ctx context.Context, epoch int64, jobs []gpuBackendJob) <-chan gpuBackendResult {
	n := 4
	if n > len(jobs) {
		n = len(jobs)
	}
	jobChan := make(chan gpuBackendJob, 4)
	resultChan := make(chan gpuBackendResult, 4)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobChan {
				samples, state := c.collect(ctx, job.id, job.b, epoch)
				resultChan <- gpuBackendResult{id: job.id, samples: samples, state: state}
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

// mergeResults merges backend results into a bounded sample slice and a copy
// of the backend state map. For each result it updates the state, appends
// samples, re-sorts by (BackendID, Timestamp, DeviceID), and truncates to the
// global max after every result. It ranges the result channel to EOF (the
// caller of runWorkers guarantees the channel closes only after workers drain).
func mergeResults(initialStates map[string]GPUBackendState, resultChan <-chan gpuBackendResult) ([]GPUSample, map[string]GPUBackendState, bool) {
	states := make(map[string]GPUBackendState, len(initialStates))
	for id, state := range initialStates {
		states[id] = state
	}
	samples := make([]GPUSample, 0, gpuMaxSamplesGlobal)
	globalTruncated := false
	for r := range resultChan {
		states[r.id] = r.state
		samples = append(samples, r.samples...)
		sort.Slice(samples, func(i, j int) bool {
			if samples[i].BackendID != samples[j].BackendID {
				return samples[i].BackendID < samples[j].BackendID
			}
			if !samples[i].Timestamp.Equal(samples[j].Timestamp) {
				return samples[i].Timestamp.Before(samples[j].Timestamp)
			}
			return samples[i].DeviceID < samples[j].DeviceID
		})
		if len(samples) > gpuMaxSamplesGlobal {
			clear(samples[gpuMaxSamplesGlobal:])
			samples = samples[:gpuMaxSamplesGlobal]
			globalTruncated = true
		}
	}
	return samples, states, globalTruncated
}

// finalizeReport assembles the GPUReport from merged states and samples,
// computing coverage and truncation flags from per-backend states and global
// truncation.
func finalizeReport(states map[string]GPUBackendState, samples []GPUSample, globalTruncated bool) GPUReport {
	report := GPUReport{
		BackendStates:               states,
		Samples:                     samples,
		SharedDeviceOnly:            true,
		ExecutionDeviceUnverified:   true,
		ResourceScope:               "backend_host",
		LifecycleState:              "unknown",
		EpochScope:                  gpuEpochScopeCollector,
		ClockAlignmentUnverified:    true,
		UpstreamGenerationUnknown:   gpuUpstreamGenerationUnknown,
		RequestIDMappingUnsupported: gpuRequestIDMappingUnsupported,
		MaxSamples:                  gpuMaxSamplesGlobal,
		MaxDevices:                  gpuMaxDevicesPerBackend,
	}

	// Determine coverage from per-backend states and global truncation.
	coverage := "complete"
	truncated := globalTruncated
	for _, st := range report.BackendStates {
		if st.State != gpuEnabledState && st.State != gpuDisabledState {
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
	report.TotalSamples = len(report.Samples)
	return report
}

// buildRequest creates the authenticated GET request for /api/performance.
// It sets Accept, copies configured headers (only when absent), and adds
// API-key authentication when no Authorization/X-Api-Key header is present.
func (c *GPUCollector) buildRequest(ctx context.Context, b *gpuBackendInfo) (*http.Request, error) {
	u := b.url.JoinPath("api/performance")
	return newBackendJSONRequest(ctx, u.String(), b.config.Headers, b.config.APIKey)
}

// classifyStatus maps an HTTP status code to a GPU backend state and reason.
// Returns continueProcessing=true for 200 and 503 (continue reading the body).
// For all other non-success statuses it returns the state/reason and
// continueProcessing=false so the caller can return early without parsing.
func classifyStatus(statusCode int) (state string, reason string, continueProcessing bool) {
	switch {
	case statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden:
		return gpuUnauthorizedState, "authorization_required", false
	case statusCode == http.StatusNotFound || statusCode == http.StatusMethodNotAllowed:
		return gpuUnsupportedState, "not_available", false
	case statusCode >= 300 && statusCode < 400:
		return gpuUnknownState, "redirect_refused", false
	case statusCode != http.StatusOK && statusCode != http.StatusServiceUnavailable:
		return gpuUnavailableState, "http_error", false
	default:
		return "", "", true
	}
}

// collect fetches GPU stats from one backend and normalizes them. It never
// follows redirects, never leaks credentials, and bounds the response body,
// device count, and record count. The only allowed upstream path is
// /api/performance.
func (c *GPUCollector) collect(ctx context.Context, id string, b *gpuBackendInfo, epoch int64) ([]GPUSample, GPUBackendState) {
	state := GPUBackendState{
		State:         gpuUnknownState,
		Reason:        "not_collected",
		ObservedAt:    c.now(),
		LastAttemptAt: c.now(),
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := c.buildRequest(ctx, b)
	if err != nil {
		state.State, state.Reason = gpuUnsupportedState, "invalid_request"
		return nil, state
	}
	response, err := c.client.Do(req)
	if err != nil {
		state.State, state.Reason = gpuUnavailableState, "request_failed"
		state.HTTPStatus = 0
		return nil, state
	}
	defer response.Body.Close()
	state.HTTPStatus = response.StatusCode
	st, reason, cont := classifyStatus(response.StatusCode)
	if !cont {
		state.State, state.Reason = st, reason
		return nil, state
	}

	// For 200 and 503: read body (bounded) and check the enabled field.
	// disabled for enabled:false even 503.
	body, err := io.ReadAll(io.LimitReader(response.Body, c.maxResponseBytes+1))
	if err != nil {
		state.State, state.Reason = gpuUnavailableState, "response_read_failed"
		return nil, state
	}
	if len(body) > int(c.maxResponseBytes) {
		state.State, state.Reason = gpuUnknownState, "response_too_large"
		return nil, state
	}

	var page gpuPerformancePage
	if err := json.Unmarshal(body, &page); err != nil {
		state.State, state.Reason = gpuUnknownState, "invalid_json"
		return nil, state
	}

	// Check the enabled field. disabled for enabled:false even 503.
	if page.Enabled != nil && !*page.Enabled {
		state.State = gpuDisabledState
		state.Reason = "monitoring_disabled"
		return nil, state
	}
	if page.Enabled == nil {
		state.State, state.Reason = gpuUnknownState, "missing_enabled"
		state.Coverage = "partial"
		return nil, state
	}
	// enabled == true. A 503 with monitoring enabled means the backend is
	// unavailable (not enabled).
	if response.StatusCode == http.StatusServiceUnavailable {
		state.State, state.Reason = gpuUnavailableState, "service_unavailable"
		return nil, state
	}

	// gpu_stats presence: missing (or null) => unknown/partial; explicit []
	// => legitimate empty.
	if page.GPUStats == nil {
		state.State = gpuUnknownState
		state.Reason = "missing_gpu_stats"
		state.Coverage = "partial"
		return nil, state
	}

	samples, deviceCount, truncated, coverage := c.normalizeGPUStats(id, *page.GPUStats, epoch)
	state.State = gpuEnabledState
	state.Reason = "ok"
	state.Devices = deviceCount
	state.Samples = len(samples)
	state.Coverage = coverage
	state.Truncated = truncated
	return samples, state
}

// appendNormalizedGPUStat normalizes one wire stat and appends it to samples
// when valid, not a duplicate, and within device caps. It returns whether the
// sample was appended, whether the row was invalid, and whether a new device
// beyond the cap was encountered (truncation). The devices map is updated for
// newly accepted devices. A new device beyond the cap sets truncated=true but
// does not append, so already-accepted devices keep collecting time points.
func (c *GPUCollector) appendNormalizedGPUStat(
	samples *[]GPUSample,
	devices map[string]bool,
	seen map[string]bool,
	w wireGpuStat,
	backendID string,
	epoch int64,
) (appended bool, invalid bool, truncated bool) {
	s := c.normalizeWireStat(backendID, w)
	if s == nil {
		return false, true, false
	}
	if !devices[s.DeviceID] {
		if len(devices) >= gpuMaxDevicesPerBackend {
			return false, false, true
		}
		devices[s.DeviceID] = true
	}
	key := fmt.Sprintf("%s:%v", s.DeviceID, s.Timestamp)
	if seen[key] {
		return false, false, false
	}
	seen[key] = true
	sample := *s
	sample.Epoch = epoch
	*samples = append(*samples, sample)
	return true, false, false
}

// coverageFor returns the coverage string from truncation and invalid flags.
// Truncated takes precedence over invalid.
func coverageFor(truncated bool, anyInvalid bool) string {
	if truncated {
		return "truncated"
	}
	if anyInvalid {
		return "partial"
	}
	return "complete"
}

// normalizeGPUStats converts one backend's wire stats into normalized samples.
// Distinct device indexes are capped at gpuMaxDevicesPerBackend; many time
// points per device are allowed, up to gpuMaxRecordsPerBackend records.
// Duplicate device/time rows are deduped. Samples are returned sorted by
// timestamp then device ID for stable, deterministic output.
func (c *GPUCollector) normalizeGPUStats(backendID string, stats []wireGpuStat, epoch int64) ([]GPUSample, int, bool, string) {
	samples := make([]GPUSample, 0, gpuMaxRecordsPerBackend)
	devices := make(map[string]bool)
	seen := make(map[string]bool)
	truncated := false
	collected := 0
	anyInvalid := false
	for _, w := range stats {
		if collected >= gpuMaxRecordsPerBackend {
			truncated = true
			break
		}
		appended, invalid, devTruncated := c.appendNormalizedGPUStat(&samples, devices, seen, w, backendID, epoch)
		if invalid {
			anyInvalid = true
		}
		if devTruncated {
			truncated = true
		}
		if appended {
			collected++
		}
	}
	sort.SliceStable(samples, func(i, j int) bool {
		if samples[i].Timestamp.Equal(samples[j].Timestamp) {
			return samples[i].DeviceID < samples[j].DeviceID
		}
		return samples[i].Timestamp.Before(samples[j].Timestamp)
	})
	coverage := coverageFor(truncated, anyInvalid)
	return samples, len(devices), truncated, coverage
}

// copyGPUPercent copies a utilization percentage into a non-nil pointer when it
// is a finite value in [0, 100]; explicit zero is preserved as a non-nil zero.
// NaN, Inf, negative, or > 100 values yield nil. The row is still retained;
// only the field is dropped.
func copyGPUPercent(v float64) *float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 || v > 100 {
		return nil
	}
	p := new(float64)
	*p = v
	return p
}

// copyNonnegativeInt copies a nonnegative integer into a non-nil pointer;
// explicit zero is preserved as a non-nil zero. Negative values yield nil.
func copyNonnegativeInt(v int) *int {
	if v < 0 {
		return nil
	}
	p := new(int)
	*p = v
	return p
}

// normalizeWireStat converts one upstream wire stat into a normalized safe
// sample. Raw upstream metadata (name, uuid, driver, PCI address) is not
// copied. DeviceID is a canonical opaque identifier derived from a hash of
// the bounded source device index. Utilization/memory values are reported as
// optional pointers with units; explicit zero values are preserved as
// non-nil pointers, absent/unknown values are nil, and out-of-range values
// are nil while the row is retained. Invalid measurements drop only their
// field; the row is never rejected for an out-of-range value.
func (c *GPUCollector) normalizeWireStat(backendID string, w wireGpuStat) *GPUSample {
	if w.ID == nil || *w.ID < 0 {
		return nil
	}
	if w.Timestamp.IsZero() {
		return nil
	}
	sample := &GPUSample{
		BackendID:  backendID,
		DeviceID:   deviceID(backendID, *w.ID),
		Timestamp:  w.Timestamp,
		ObservedAt: c.now(),
	}
	if w.GpuUtilPct != nil {
		sample.GpuUtilPct = copyGPUPercent(*w.GpuUtilPct)
	}
	if w.MemUtilPct != nil {
		sample.MemUtilPct = copyGPUPercent(*w.MemUtilPct)
	}
	if w.MemUsedMB != nil {
		sample.MemUsedMB = copyNonnegativeInt(*w.MemUsedMB)
	}
	if w.MemTotalMB != nil {
		sample.MemTotalMB = copyNonnegativeInt(*w.MemTotalMB)
	}
	return sample
}

// deviceID returns a canonical opaque device identifier 'gpu-' + digest
// derived from a SHA256 of the configured backend ID and the bounded source
// device index (first 16 bytes, lowercase hex). The raw name, UUID, driver,
// and PCI address are never used.
func deviceID(backendID string, gpuStatID int) string {
	h := sha256.New()
	h.Write([]byte(backendID))
	h.Write([]byte(":"))
	h.Write([]byte(strconv.Itoa(gpuStatID)))
	digest := h.Sum(nil)
	return "gpu-" + hex.EncodeToString(digest[:16])
}

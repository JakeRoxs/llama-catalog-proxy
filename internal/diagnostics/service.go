// Package diagnostics provides opt-in, on-demand capability observations. It
// never calls model upstreams, captures, logs, or lifecycle-management APIs.
package diagnostics

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

type State string

const (
	Supported    State = "supported"
	Unsupported  State = "unsupported"
	Disabled     State = "disabled"
	Unavailable  State = "unavailable"
	Unauthorized State = "unauthorized"
	Unknown      State = "unknown"

	maxResponseBytes = 1 << 20
)

// Only finite llama-swap metadata APIs are allowed. In particular, GET requests
// under /upstream can load models and must not be used for discovery.
var operations = []struct{ name, path string }{
	{"version", "api/version"},
	{"models", "v1/models"},
	{"activity", "api/metrics/activity"},
	{"stats", "api/metrics/stats"},
	{"performance", "api/performance"},
	{"hardware", "api/hardware"},
}

type Build struct {
	Version string `json:"version,omitempty"`
	Commit  string `json:"commit,omitempty"`
	Date    string `json:"date,omitempty"`
}

type Observation struct {
	State            State     `json:"state"`
	Source           string    `json:"source"`
	ObservedAt       time.Time `json:"observed_at,omitzero"`
	FreshUntil       time.Time `json:"fresh_until,omitzero"`
	ExpiresAt        time.Time `json:"expires_at,omitzero"`
	LastAttemptAt    time.Time `json:"last_attempt_at,omitzero"`
	LastAttemptState State     `json:"last_attempt_state,omitempty"`
	HTTPStatus       int       `json:"http_status,omitempty"`
	Reason           string    `json:"reason,omitempty"`
	Stale            bool      `json:"stale"`
	Build            *Build    `json:"build,omitempty"`
}

type Snapshot struct {
	Backend      string                 `json:"backend"`
	Adapter      string                 `json:"adapter,omitempty"`
	GeneratedAt  time.Time              `json:"generated_at"`
	RetryAt      time.Time              `json:"retry_at,omitzero"`
	Capabilities map[string]Observation `json:"capabilities"`
}

type backend struct {
	config config.Backend
	url    *url.URL
	mu     sync.Mutex
	busy   chan struct{}
	cache  map[string]Observation
	next   time.Time
	fails  uint
}

type Service struct {
	backends map[string]*backend
	client   *http.Client
	limit    chan struct{}
	now      func() time.Time
	timeout  time.Duration
	ttl      time.Duration
	maxAge   time.Duration
}

// New copies configuration. Construction does not issue requests or start any
// goroutines. A fresh service must be constructed after configuration changes.
func New(backends map[string]config.Backend) (*Service, error) {
	s := &Service{
		backends: make(map[string]*backend, len(backends)),
		client: &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}},
		limit: make(chan struct{}, 4), now: time.Now,
		timeout: 2 * time.Second, ttl: 30 * time.Second, maxAge: 5 * time.Minute,
	}
	for id, cfg := range backends {
		if cfg.Diagnostics != "" && cfg.Diagnostics != "llama_swap" {
			return nil, errors.New("unsupported diagnostics adapter")
		}
		u, err := url.Parse(cfg.URL)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.RawQuery != "" || u.Fragment != "" {
			return nil, errors.New("invalid diagnostics backend URL")
		}
		headers := make(map[string]string, len(cfg.Headers))
		for key, value := range cfg.Headers {
			headers[key] = value
		}
		cfg.Headers = headers
		s.backends[id] = &backend{config: cfg, url: u, cache: make(map[string]Observation)}
	}
	return s, nil
}

// All returns deterministically ordered snapshots using at most four workers.
func (s *Service) All(ctx context.Context) ([]Snapshot, error) {
	ids := make([]string, 0, len(s.backends))
	for id := range s.backends {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	results := make([]Snapshot, len(ids))
	jobs := make(chan int, len(ids))
	for i := range ids {
		jobs <- i
	}
	close(jobs)
	var wg sync.WaitGroup
	for range min(4, len(ids)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				results[i], _ = s.Snapshot(ctx, ids[i])
			}
		}()
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

// snapshotMode describes the decision returned by beginSnapshotRefresh.
type snapshotMode int

const (
	// snapshotCached means the backend is disabled or still within its
	// backoff/TTL window; the caller should snapshot the cache. beginSnapshotRefresh
	// returns with b.mu held for this mode.
	snapshotCached snapshotMode = iota
	// snapshotWait means another goroutine is refreshing; the caller should wait
	// on the returned busy channel. beginSnapshotRefresh releases b.mu.
	snapshotWait
	// snapshotOwner means no refresh is in flight; the caller should probe and
	// commit. beginSnapshotRefresh creates b.busy and releases b.mu.
	snapshotOwner
)

// Snapshot coalesces concurrent refreshes. Canceled callers do not poison the
// cache; a waiting caller may retry with its own context after a canceled owner.
func (s *Service) Snapshot(ctx context.Context, id string) (Snapshot, error) {
	b, ok := s.backends[id]
	if !ok {
		return Snapshot{}, errors.New("unknown diagnostics backend")
	}
	for {
		if err := ctx.Err(); err != nil {
			return Snapshot{}, err
		}
		result, retry, err := s.refreshOnce(ctx, id, b)
		if err != nil {
			return Snapshot{}, err
		}
		if !retry {
			return result, nil
		}
	}
}

// refreshOnce performs a single snapshot refresh decision. Entry and exit are
// with b.mu not held. For snapshotCached beginSnapshotRefresh returns with b.mu
// held; the caller snapshots the cache and unlocks exactly once before return.
// For snapshotWait the lock is already released and the busy channel captured;
// the caller waits and reports retry so the top-level loop re-decides. For
// snapshotOwner the caller probes all operations outside the lock and commits
// through finishSnapshotRefresh. A non-retryable terminal error is returned as-is.
func (s *Service) refreshOnce(ctx context.Context, id string, b *backend) (Snapshot, bool, error) {
	mode, busy := s.beginSnapshotRefresh(b)
	switch mode {
	case snapshotCached:
		return s.snapshotCachedPath(id, b)
	case snapshotWait:
		return s.snapshotWaitPath(ctx, busy)
	case snapshotOwner:
		return s.snapshotOwnerPath(ctx, id, b)
	}
	return Snapshot{}, false, errors.New("unknown snapshot refresh mode")
}

// snapshotCachedPath snapshots the cache and releases the b.mu lock held by
// beginSnapshotRefresh on entry.
func (s *Service) snapshotCachedPath(id string, b *backend) (Snapshot, bool, error) {
	result := s.snapshot(id, b)
	b.mu.Unlock()
	return result, false, nil
}

// snapshotWaitPath waits for an in-flight refresh and reports retry so the
// caller re-decides. b.mu is not held on entry.
func (s *Service) snapshotWaitPath(ctx context.Context, busy chan struct{}) (Snapshot, bool, error) {
	if err := waitForSnapshotRefresh(ctx, busy); err != nil {
		return Snapshot{}, false, err
	}
	return Snapshot{}, true, nil
}

// snapshotOwnerPath probes all operations outside the lock and commits the
// result. b.mu is not held on entry.
func (s *Service) snapshotOwnerPath(ctx context.Context, id string, b *backend) (Snapshot, bool, error) {
	observations := make(map[string]Observation, len(operations))
	for _, op := range operations {
		observations[op.name] = s.probe(ctx, b, op.name, op.path)
	}
	result, err := s.finishSnapshotRefresh(ctx, id, b, observations)
	if err != nil {
		return Snapshot{}, false, err
	}
	return result, false, nil
}

// beginSnapshotRefresh decides, under b.mu, whether the caller should use the
// cached snapshot, wait for an in-flight refresh, or become the owner. For
// snapshotCached b.mu is held on return (the caller unlocks after snapshotting);
// for snapshotWait and snapshotOwner b.mu is released and b.busy is captured or
// created. The owner must probe before calling finishSnapshotRefresh.
func (s *Service) beginSnapshotRefresh(b *backend) (snapshotMode, chan struct{}) {
	b.mu.Lock()
	if b.config.Diagnostics == "" || s.now().Before(b.next) {
		return snapshotCached, nil
	}
	if b.busy != nil {
		busy := b.busy
		b.mu.Unlock()
		return snapshotWait, busy
	}
	b.busy = make(chan struct{})
	b.mu.Unlock()
	return snapshotOwner, nil
}

// waitForSnapshotRefresh waits for an in-flight refresh to release its busy
// channel. It returns ctx.Err() when the caller is canceled before the refresh
// completes, so waiting callers never block past their own context.
func waitForSnapshotRefresh(ctx context.Context, busy chan struct{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-busy:
		return nil
	}
}

// isFailedState reports whether a probe observation counts toward the backend
// failure budget and backoff.
func isFailedState(state State) bool {
	return state == Unavailable || state == Unknown || state == Unauthorized
}

// preserveFreshCache reports whether a failed observation should be merged into
// a fresh cached Supported/Disabled observation instead of replacing it.
func preserveFreshCache(current, previous Observation, now time.Time) bool {
	return (current.State == Unavailable || current.State == Unknown) &&
		(previous.State == Supported || previous.State == Disabled) &&
		now.Before(previous.ExpiresAt)
}

// finishSnapshotRefresh commits probe observations under the backend lock,
// closes the busy channel, and snapshots the cache. The caller is the owner;
// b.mu is not held on entry and is released before return. When the context is
// canceled, observations are not applied but the busy channel is still released
// and the context error is returned.
func (s *Service) finishSnapshotRefresh(ctx context.Context, id string, b *backend, observations map[string]Observation) (Snapshot, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		close(b.busy)
		b.busy = nil
		return Snapshot{}, err
	}
	failed := s.mergeObservations(b, observations, s.now())
	s.commitBackoff(b, failed)
	result := s.snapshot(id, b)
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	return result, nil
}

// mergeObservations merges probe observations into the cache and reports
// whether any observation is a failed state. b.mu is held on entry and remains
// held on exit.
func (s *Service) mergeObservations(b *backend, observations map[string]Observation, now time.Time) bool {
	failed := false
	for name, current := range observations {
		failed = failed || isFailedState(current.State)
		previous := b.cache[name]
		if preserveFreshCache(current, previous, now) {
			previous.LastAttemptAt, previous.LastAttemptState = current.LastAttemptAt, current.State
			previous.HTTPStatus, previous.Reason = current.HTTPStatus, current.Reason
			b.cache[name] = previous
		} else {
			b.cache[name] = current
		}
	}
	return failed
}

// commitBackoff records the failure count and backoff, then releases the busy
// channel. b.mu is held on entry and remains held on exit.
func (s *Service) commitBackoff(b *backend, failed bool) {
	if failed {
		b.fails = min(b.fails+1, 7)
		b.next = s.now().Add(min(time.Second<<uint(b.fails-1), time.Minute))
	} else {
		b.fails = 0
		b.next = s.now().Add(s.ttl)
	}
	close(b.busy)
	b.busy = nil
}

// Called with the backend lock held; returned values never alias cached data.
func (s *Service) snapshot(id string, b *backend) Snapshot {
	now := s.now()
	result := Snapshot{Backend: id, Adapter: b.config.Diagnostics, GeneratedAt: now,
		RetryAt: b.next, Capabilities: make(map[string]Observation, len(operations))}
	for _, op := range operations {
		observation, exists := b.cache[op.name]
		if !exists {
			observation = Observation{State: Disabled, Source: "/" + op.path, Reason: "not_enabled"}
		} else {
			observation.Stale = !now.Before(observation.FreshUntil)
			if !now.Before(observation.ExpiresAt) {
				observation.State, observation.Reason, observation.Build = Unknown, "expired", nil
			}
		}
		if observation.Build != nil {
			build := *observation.Build
			observation.Build = &build
		}
		result.Capabilities[op.name] = observation
	}
	return result
}

func (s *Service) probe(ctx context.Context, b *backend, name, path string) Observation {
	now := s.now()
	result := Observation{State: Unavailable, Source: "/" + path, ObservedAt: now,
		LastAttemptAt: now, FreshUntil: now.Add(s.ttl), ExpiresAt: now.Add(s.maxAge)}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	if !s.acquireProbeSlot(ctx) {
		result.Reason = "timeout_or_cancellation"
		return result
	}
	defer func() { <-s.limit }()
	s.probeHTTP(ctx, b, name, path, &result)
	return result
}

// probeHTTP performs the request and applies the status and body results to
// the observation. The caller must hold a concurrency slot. When the HTTP
// status is terminal, the body is not read.
func (s *Service) probeHTTP(ctx context.Context, b *backend, name, path string, result *Observation) {
	req, err := buildProbeRequest(ctx, b.url, path, name, b.config)
	if err != nil {
		result.State, result.Reason = Unknown, "invalid_request"
		return
	}
	response, err := s.client.Do(req)
	if err != nil {
		result.Reason = "request_failed"
		return
	}
	defer response.Body.Close()
	result.HTTPStatus = response.StatusCode
	if applyProbeStatus(result, name, response) {
		return
	}
	applyProbeBody(result, name, response)
}

// acquireProbeSlot acquires the concurrency slot. It returns false when the
// context is canceled before a slot is available; the caller must not release
// a slot it did not acquire.
func (s *Service) acquireProbeSlot(ctx context.Context) bool {
	select {
	case s.limit <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

// applyProbeStatus applies the HTTP status classification to the observation.
// It returns true when the observation is terminal and the body must not be
// read. A zero State means leave the current state unchanged.
func applyProbeStatus(result *Observation, name string, response *http.Response) bool {
	state, reason, stop := classifyProbeStatus(name, response.StatusCode)
	if !stop {
		return false
	}
	result.Reason = reason
	if state != "" {
		result.State = state
	}
	return true
}

// applyProbeBody reads the response body, recognizes it, and applies the final
// state/reason rules to the observation.
func applyProbeBody(result *Observation, name string, response *http.Response) {
	object, state, reason, stop := readProbeBody(response.Body)
	if stop {
		result.Reason = reason
		if state != "" {
			result.State = state
		}
		return
	}
	result.State, result.Build = recognize(name, object)
	if response.StatusCode != http.StatusOK && result.State != Disabled {
		result.State, result.Build, result.Reason = Unavailable, nil, "http_error"
	}
	if result.State == Unknown {
		result.Reason = "unrecognized_response"
	}
}

// buildProbeRequest builds the GET request used to probe a backend operation.
// Backend headers are applied first, the APIKey fallback is added only when
// neither Authorization nor X-Api-Key is present, and Accept is always set.
func buildProbeRequest(ctx context.Context, base *url.URL, path, name string, cfg config.Backend) (*http.Request, error) {
	u := base.JoinPath(path)
	if name == "activity" {
		u.RawQuery = "limit=1"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	for key, value := range cfg.Headers {
		req.Header.Set(key, value)
	}
	if cfg.APIKey != "" && req.Header.Get("Authorization") == "" && req.Header.Get("X-Api-Key") == "" {
		req.Header.Set("Authorization", "Bearer "+cfg.APIKey)
		req.Header.Set("X-Api-Key", cfg.APIKey)
	}
	req.Header.Set("Accept", "application/json")
	return req, nil
}

// classifyProbeStatus maps an HTTP status to an observation state and reason.
// It returns stop=true when the caller should apply the result and return. The
// performance endpoint tolerates 503 as an enabled-but-unavailable signal. A
// zero State means leave the current state unchanged.
func classifyProbeStatus(name string, status int) (State, string, bool) {
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return Unauthorized, "authorization_required", true
	case status == http.StatusNotFound || status == http.StatusMethodNotAllowed:
		return Unsupported, "", true
	case status >= 300 && status < 400:
		return Unknown, "redirect_refused", true
	case status != http.StatusOK && !(name == "performance" && status == http.StatusServiceUnavailable):
		return "", "http_error", true
	}
	return "", "", false
}

// readProbeBody reads at most maxResponseBytes+1 bytes, rejects oversized
// bodies, and decodes a JSON object. It returns stop=true when the caller
// should apply the result and return. A zero State means leave the current
// state unchanged.
func readProbeBody(body io.ReadCloser) (map[string]json.RawMessage, State, string, bool) {
	raw, err := io.ReadAll(io.LimitReader(body, maxResponseBytes+1))
	if err != nil {
		return nil, "", "response_read_failed", true
	}
	if len(raw) > maxResponseBytes {
		return nil, Unknown, "response_too_large", true
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil || object == nil {
		return nil, Unknown, "invalid_json", true
	}
	return object, "", "", false
}

var (
	versionPattern = regexp.MustCompile(`^(v?[0-9]+\.[0-9]+\.[0-9]+([-+][A-Za-z0-9._-]+)?|dev|unknown)$`)
	commitPattern  = regexp.MustCompile(`^([0-9a-fA-F]{7,64}|unknown)$`)
)

func recognize(name string, object map[string]json.RawMessage) (State, *Build) {
	switch name {
	case "version":
		return recognizeVersion(object)
	case "models":
		return recognizeModels(object)
	case "activity":
		return recognizeActivity(object)
	case "stats":
		return recognizeStats(object)
	case "performance":
		return recognizePerformance(object)
	case "hardware":
		return recognizeHardware(object)
	}
	return Unknown, nil
}

// recognizeVersion validates and builds the version observation. A missing or
// unparseable version yields Unknown; a valid version yields Supported with the
// parsed build metadata (unset fields remain nil when they do not validate).
func recognizeVersion(object map[string]json.RawMessage) (State, *Build) {
	var raw struct {
		Version, Commit string
		BuildDate       string `json:"build_date"`
	}
	encoded, _ := json.Marshal(object)
	if json.Unmarshal(encoded, &raw) != nil || raw.Version == "" {
		return Unknown, nil
	}
	build := &Build{}
	if versionPattern.MatchString(raw.Version) && len(raw.Version) <= 96 {
		build.Version = raw.Version
	}
	if commitPattern.MatchString(raw.Commit) {
		build.Commit = raw.Commit
	}
	if date, err := time.Parse(time.RFC3339, raw.BuildDate); err == nil {
		build.Date = date.Format(time.RFC3339)
	}
	return Supported, build
}

// recognizeModels reports Supported when the response is a JSON array under data.
func recognizeModels(object map[string]json.RawMessage) (State, *Build) {
	if isArray(object["data"]) {
		return Supported, nil
	}
	return Unknown, nil
}

// recognizeActivity reports Supported when data is an array and page/total are counts.
func recognizeActivity(object map[string]json.RawMessage) (State, *Build) {
	if isArray(object["data"]) && isCount(object["page"]) && isCount(object["total"]) {
		return Supported, nil
	}
	return Unknown, nil
}

// recognizeStats reports Supported when all three totals are counts.
func recognizeStats(object map[string]json.RawMessage) (State, *Build) {
	if isCount(object["total_requests"]) && isCount(object["total_input_tokens"]) && isCount(object["total_output_tokens"]) {
		return Supported, nil
	}
	return Unknown, nil
}

// recognizePerformance reports Disabled only for an explicit enabled:false, and
// Supported when enabled is true and sys_stats/gpu_stats are arrays. Any other
// input (missing or unparseable enabled) yields Unknown.
func recognizePerformance(object map[string]json.RawMessage) (State, *Build) {
	var enabled *bool
	if value, exists := object["enabled"]; exists && json.Unmarshal(value, &enabled) == nil && enabled != nil {
		if !*enabled {
			return Disabled, nil
		}
		if isArray(object["sys_stats"]) && isArray(object["gpu_stats"]) {
			return Supported, nil
		}
	}
	return Unknown, nil
}

// recognizeHardware reports Supported when schema_version is a count and
// accelerators is an array.
func recognizeHardware(object map[string]json.RawMessage) (State, *Build) {
	if isCount(object["schema_version"]) && isArray(object["accelerators"]) {
		return Supported, nil
	}
	return Unknown, nil
}

func isArray(value json.RawMessage) bool {
	return strings.HasPrefix(strings.TrimSpace(string(value)), "[")
}

func isCount(value json.RawMessage) bool {
	var count uint64
	return len(value) > 0 && string(value) != "null" && json.Unmarshal(value, &count) == nil
}

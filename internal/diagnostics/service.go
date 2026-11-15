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
		b.mu.Lock()
		if b.config.Diagnostics == "" || s.now().Before(b.next) {
			result := s.snapshot(id, b)
			b.mu.Unlock()
			return result, nil
		}
		if b.busy != nil {
			busy := b.busy
			b.mu.Unlock()
			select {
			case <-ctx.Done():
				return Snapshot{}, ctx.Err()
			case <-busy:
				continue
			}
		}
		b.busy = make(chan struct{})
		b.mu.Unlock()
		observations := make(map[string]Observation, len(operations))
		for _, op := range operations {
			observations[op.name] = s.probe(ctx, b, op.name, op.path)
		}
		b.mu.Lock()
		if ctx.Err() == nil {
			failed := false
			for name, current := range observations {
				bad := current.State == Unavailable || current.State == Unknown || current.State == Unauthorized
				failed = failed || bad
				previous := b.cache[name]
				if (current.State == Unavailable || current.State == Unknown) &&
					(previous.State == Supported || previous.State == Disabled) && s.now().Before(previous.ExpiresAt) {
					previous.LastAttemptAt, previous.LastAttemptState = current.LastAttemptAt, current.State
					previous.HTTPStatus, previous.Reason = current.HTTPStatus, current.Reason
					b.cache[name] = previous
				} else {
					b.cache[name] = current
				}
			}
			if failed {
				b.fails = min(b.fails+1, 7)
				b.next = s.now().Add(min(time.Second<<uint(b.fails-1), time.Minute))
			} else {
				b.fails = 0
				b.next = s.now().Add(s.ttl)
			}
		}
		close(b.busy)
		b.busy = nil
		result := s.snapshot(id, b)
		b.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return Snapshot{}, err
		}
		return result, nil
	}
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
	select {
	case s.limit <- struct{}{}:
		defer func() { <-s.limit }()
	case <-ctx.Done():
		result.Reason = "timeout_or_cancellation"
		return result
	}
	u := b.url.JoinPath(path)
	if name == "activity" {
		u.RawQuery = "limit=1"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		result.State, result.Reason = Unknown, "invalid_request"
		return result
	}
	for key, value := range b.config.Headers {
		req.Header.Set(key, value)
	}
	if b.config.APIKey != "" && req.Header.Get("Authorization") == "" && req.Header.Get("X-Api-Key") == "" {
		req.Header.Set("Authorization", "Bearer "+b.config.APIKey)
		req.Header.Set("X-Api-Key", b.config.APIKey)
	}
	req.Header.Set("Accept", "application/json")
	response, err := s.client.Do(req)
	if err != nil {
		result.Reason = "request_failed"
		return result
	}
	defer response.Body.Close()
	result.HTTPStatus = response.StatusCode
	switch {
	case response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden:
		result.State, result.Reason = Unauthorized, "authorization_required"
		return result
	case response.StatusCode == http.StatusNotFound || response.StatusCode == http.StatusMethodNotAllowed:
		result.State = Unsupported
		return result
	case response.StatusCode >= 300 && response.StatusCode < 400:
		result.State, result.Reason = Unknown, "redirect_refused"
		return result
	case response.StatusCode != http.StatusOK && !(name == "performance" && response.StatusCode == http.StatusServiceUnavailable):
		result.Reason = "http_error"
		return result
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		result.Reason = "response_read_failed"
		return result
	}
	if len(body) > maxResponseBytes {
		result.State, result.Reason = Unknown, "response_too_large"
		return result
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(body, &object) != nil || object == nil {
		result.State, result.Reason = Unknown, "invalid_json"
		return result
	}
	result.State, result.Build = recognize(name, object)
	if response.StatusCode != http.StatusOK && result.State != Disabled {
		result.State, result.Build, result.Reason = Unavailable, nil, "http_error"
	}
	if result.State == Unknown {
		result.Reason = "unrecognized_response"
	}
	return result
}

var (
	versionPattern = regexp.MustCompile(`^(v?[0-9]+\.[0-9]+\.[0-9]+([-+][A-Za-z0-9._-]+)?|dev|unknown)$`)
	commitPattern  = regexp.MustCompile(`^([0-9a-fA-F]{7,64}|unknown)$`)
)

func recognize(name string, object map[string]json.RawMessage) (State, *Build) {
	var valid bool
	switch name {
	case "version":
		var raw struct {
			Version, Commit string
			BuildDate       string `json:"build_date"`
		}
		encoded, _ := json.Marshal(object)
		valid = json.Unmarshal(encoded, &raw) == nil && raw.Version != ""
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
		if valid {
			return Supported, build
		}
	case "models":
		valid = isArray(object["data"])
	case "activity":
		valid = isArray(object["data"]) && isCount(object["page"]) && isCount(object["total"])
	case "stats":
		valid = isCount(object["total_requests"]) && isCount(object["total_input_tokens"]) && isCount(object["total_output_tokens"])
	case "performance":
		var enabled *bool
		if value, exists := object["enabled"]; exists && json.Unmarshal(value, &enabled) == nil && enabled != nil {
			if !*enabled {
				return Disabled, nil
			}
			valid = isArray(object["sys_stats"]) && isArray(object["gpu_stats"])
		}
	case "hardware":
		valid = isCount(object["schema_version"]) && isArray(object["accelerators"])
	}
	if valid {
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

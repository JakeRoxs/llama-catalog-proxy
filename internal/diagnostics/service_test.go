package diagnostics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

var validBodies = map[string]string{
	"/api/version":          `{"version":"v1.2.3","commit":"abcdef1234","build_date":"2026-11-11T00:00:00Z"}`,
	"/v1/models":            `{"data":[]}`,
	"/api/metrics/activity": `{"data":[],"page":1,"total":0}`,
	"/api/metrics/stats":    `{"total_requests":0,"total_input_tokens":0,"total_output_tokens":0}`,
	"/api/performance":      `{"enabled":true,"sys_stats":[],"gpu_stats":[]}`,
	"/api/hardware":         `{"schema_version":1,"accelerators":[]}`,
}

func newTestService(t *testing.T, server *httptest.Server) *Service {
	t.Helper()
	s, err := New(map[string]config.Backend{"test": {URL: server.URL, Diagnostics: "llama_swap"}})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func getSnapshot(t *testing.T, s *Service) Snapshot {
	t.Helper()
	value, err := s.Snapshot(context.Background(), "test")
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func writeValid(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if body, ok := validBodies[r.URL.Path]; ok {
		fmt.Fprint(w, body)
	} else {
		http.Error(w, "unexpected operation", http.StatusBadRequest)
	}
}

func TestOptInAndConstructionNeverIssueRequests(t *testing.T) {
	var count atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { count.Add(1); writeValid(w, r) }))
	defer server.Close()
	s, err := New(map[string]config.Backend{"off": {URL: server.URL}, "on": {URL: server.URL, Diagnostics: "llama_swap"}})
	if err != nil {
		t.Fatal(err)
	}
	if count.Load() != 0 {
		t.Fatal("construction made requests")
	}
	off, err := s.Snapshot(context.Background(), "off")
	if err != nil {
		t.Fatal(err)
	}
	for _, observation := range off.Capabilities {
		if observation.State != Disabled || observation.Reason != "not_enabled" || !observation.ObservedAt.IsZero() {
			t.Fatalf("disabled observation = %#v", observation)
		}
	}
	if count.Load() != 0 {
		t.Fatal("disabled backend made requests")
	}
	if _, err := s.Snapshot(context.Background(), "on"); err != nil {
		t.Fatal(err)
	}
	if count.Load() != int32(len(operations)) {
		t.Fatalf("request count = %d", count.Load())
	}
}

func TestAllowedOperationsCredentialsAndBasePath(t *testing.T) {
	seen := make(map[string]bool)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.Header.Get("Authorization") != "Bearer sentinel-key" ||
			r.Header.Get("X-Api-Key") != "sentinel-key" || r.Header.Get("X-Custom") != "sentinel-header" {
			t.Error("method or configured credentials not applied")
		}
		path := strings.TrimPrefix(r.URL.Path, "/prefix")
		if _, ok := validBodies[path]; !ok {
			t.Errorf("unsafe/unexpected path %s", r.URL.Path)
		}
		if path == "/api/metrics/activity" && r.URL.RawQuery != "limit=1" {
			t.Error("activity query is not bounded")
		}
		if path != "/api/metrics/activity" && r.URL.RawQuery != "" {
			t.Error("unexpected query")
		}
		seen[path] = true
		r.URL.Path = path
		writeValid(w, r)
	}))
	defer server.Close()
	s, err := New(map[string]config.Backend{"test": {URL: server.URL + "/prefix", Diagnostics: "llama_swap",
		APIKey: "sentinel-key", Headers: map[string]string{"X-Custom": "sentinel-header"}}})
	if err != nil {
		t.Fatal(err)
	}
	result := getSnapshot(t, s)
	for name, observation := range result.Capabilities {
		if observation.State != Supported || observation.Stale || observation.ObservedAt.IsZero() {
			t.Errorf("%s = %#v", name, observation)
		}
	}
	if len(seen) != len(operations) {
		t.Fatalf("probed %d paths", len(seen))
	}
	if result.Capabilities["version"].Build.Version != "v1.2.3" {
		t.Fatal("missing sanitized build")
	}
	encoded, _ := json.Marshal(result)
	for _, secret := range []string{server.URL, "sentinel-key", "sentinel-header"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatal("snapshot leaked backend secret or address")
		}
	}
}

func TestConfiguredAuthorizationTakesPrecedence(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer explicit" || r.Header.Get("X-Api-Key") != "" {
			t.Error("overrode configured auth")
		}
		writeValid(w, r)
	}))
	defer server.Close()
	headers := map[string]string{"Authorization": "Bearer explicit"}
	s, err := New(map[string]config.Backend{"test": {URL: server.URL, Diagnostics: "llama_swap", APIKey: "fallback", Headers: headers}})
	if err != nil {
		t.Fatal(err)
	}
	headers["Authorization"] = "changed"
	getSnapshot(t, s)
}

func TestCapabilityStatesAndResponseRedaction(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/version":
			fmt.Fprint(w, `{"version":"SENTINEL-PRIVATE","commit":"SENTINEL-PRIVATE","build_date":"SENTINEL-PRIVATE","cmd":"SENTINEL-PRIVATE"}`)
		case "/v1/models":
			w.WriteHeader(404)
			fmt.Fprint(w, "SENTINEL-PRIVATE")
		case "/api/metrics/activity":
			w.WriteHeader(401)
			fmt.Fprint(w, "SENTINEL-PRIVATE")
		case "/api/metrics/stats":
			w.WriteHeader(500)
			fmt.Fprint(w, "SENTINEL-PRIVATE")
		case "/api/performance":
			w.WriteHeader(503)
			fmt.Fprint(w, `{"enabled":false,"secret":"SENTINEL-PRIVATE"}`)
		case "/api/hardware":
			fmt.Fprint(w, `{"cmd":"SENTINEL-PRIVATE","prompt":"SENTINEL-PRIVATE"}`)
		default:
			t.Error("unexpected path")
		}
	}))
	defer server.Close()
	result := getSnapshot(t, newTestService(t, server))
	want := map[string]State{"version": Supported, "models": Unsupported, "activity": Unauthorized,
		"stats": Unavailable, "performance": Disabled, "hardware": Unknown}
	for name, state := range want {
		if result.Capabilities[name].State != state {
			t.Errorf("%s state = %s", name, result.Capabilities[name].State)
		}
	}
	encoded, _ := json.Marshal(result)
	if strings.Contains(string(encoded), "SENTINEL-PRIVATE") {
		t.Fatal("response fields leaked")
	}
}

func TestRedirectsCannotReachUpstreamsOrLeakCredentials(t *testing.T) {
	var reached atomic.Int32
	tripwire := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached.Add(1) }))
	defer tripwire.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, tripwire.URL+"/upstream/model/props", http.StatusFound)
	}))
	defer server.Close()
	result := getSnapshot(t, newTestService(t, server))
	for _, observation := range result.Capabilities {
		if observation.State != Unknown || observation.Reason != "redirect_refused" {
			t.Fatal("redirect was not refused")
		}
	}
	if reached.Load() != 0 {
		t.Fatal("followed redirect to a lifecycle-sensitive path")
	}
}

func TestMalformedAndOversizedResponses(t *testing.T) {
	for _, body := range []string{"<html>login</html>", `null`, `[]`, `{"data":[]} trailing`, strings.Repeat("x", maxResponseBytes+1)} {
		t.Run(fmt.Sprint(len(body)), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body) }))
			defer server.Close()
			result := getSnapshot(t, newTestService(t, server))
			for _, observation := range result.Capabilities {
				if observation.State != Unknown {
					t.Fatalf("state = %s", observation.State)
				}
			}
		})
	}
	for _, raw := range []string{`{"enabled":null}`, `{"enabled":"false"}`, `{"enabled":true}`} {
		var object map[string]json.RawMessage
		_ = json.Unmarshal([]byte(raw), &object)
		if state, _ := recognize("performance", object); state != Unknown {
			t.Fatalf("accepted malformed performance: %s", raw)
		}
	}
}

func TestCacheBackoffExpiryRecoveryAndCopies(t *testing.T) {
	var count atomic.Int32
	var fail atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		if fail.Load() {
			w.WriteHeader(500)
			return
		}
		writeValid(w, r)
	}))
	defer server.Close()
	s := newTestService(t, server)
	start := time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)
	now := start
	s.now = func() time.Time { return now }
	first := getSnapshot(t, s)
	first.Capabilities["version"].Build.Version = "mutated"
	delete(first.Capabilities, "models")
	cached := getSnapshot(t, s)
	if cached.Capabilities["version"].Build.Version != "v1.2.3" || len(cached.Capabilities) != len(operations) {
		t.Fatal("cache aliases returned data")
	}
	if count.Load() != int32(len(operations)) {
		t.Fatal("fresh cache re-probed")
	}
	fail.Store(true)
	for index, seconds := range []int{31, 32, 34, 38, 46, 62, 94, 154, 214, 274} {
		now = start.Add(time.Duration(seconds) * time.Second)
		stale := getSnapshot(t, s).Capabilities["version"]
		if stale.State != Supported || !stale.Stale || stale.LastAttemptState != Unavailable {
			t.Fatalf("stale = %#v", stale)
		}
		calls := count.Load()
		getSnapshot(t, s)
		if count.Load() != calls {
			t.Fatal("backoff ignored")
		}
		if calls != int32((index+2)*len(operations)) {
			t.Fatal("unexpected probe count")
		}
	}
	now = start.Add(301 * time.Second)
	expired := getSnapshot(t, s).Capabilities["version"]
	if expired.State != Unknown || expired.Reason != "expired" || expired.Build != nil {
		t.Fatalf("expired = %#v", expired)
	}
	now = start.Add(335 * time.Second)
	fail.Store(false)
	recovered := getSnapshot(t, s)
	if recovered.Capabilities["version"].State != Supported || recovered.Capabilities["version"].Stale {
		t.Fatal("did not recover")
	}
	if !recovered.RetryAt.Equal(now.Add(s.ttl)) {
		t.Fatal("success did not reset backoff")
	}
}

func TestConcurrentRefreshCoalescesAndWaitingCancellationIsIndependent(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var count atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if count.Add(1) == 1 {
			close(started)
			<-release
		}
		writeValid(w, r)
	}))
	defer server.Close()
	s := newTestService(t, server)
	done := make(chan error, 20)
	go func() { _, err := s.Snapshot(context.Background(), "test"); done <- err }()
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Snapshot(ctx, "test"); !errors.Is(err, context.Canceled) {
		t.Fatal("ignored cancellation")
	}
	for range 19 {
		go func() { _, err := s.Snapshot(context.Background(), "test"); done <- err }()
	}
	close(release)
	for range 20 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if count.Load() != int32(len(operations)) {
		t.Fatalf("refresh not coalesced: %d requests", count.Load())
	}
}

func TestAllHasBoundedConcurrencyAndDeterministicOrder(t *testing.T) {
	var active, maximum atomic.Int32
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := active.Add(1)
		defer active.Add(-1)
		for old := maximum.Load(); n > old; old = maximum.Load() {
			if maximum.CompareAndSwap(old, n) {
				break
			}
		}
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		writeValid(w, r)
	}))
	defer server.Close()
	backends := make(map[string]config.Backend)
	for i := range 8 {
		backends[fmt.Sprintf("backend-%d", i)] = config.Backend{URL: server.URL, Diagnostics: "llama_swap"}
	}
	s, err := New(backends)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var result []Snapshot
	var allErr error
	done := make(chan struct{})
	go func() { result, allErr = s.All(ctx); close(done) }()
	for range 4 {
		select {
		case <-started:
		case <-ctx.Done():
			close(release)
			t.Fatal("workers did not start")
		}
	}
	close(release)
	<-done
	if allErr != nil {
		t.Fatal(allErr)
	}
	if maximum.Load() != 4 {
		t.Fatalf("max requests = %d", maximum.Load())
	}
	for i, snapshot := range result {
		if snapshot.Backend != fmt.Sprintf("backend-%d", i) {
			t.Fatal("unstable ordering")
		}
	}
}

func TestTimeoutAndCanceledOwnerDoNotPoisonCache(t *testing.T) {
	started := make(chan struct{}, 1)
	var block atomic.Bool
	block.Store(true)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if block.Load() {
			select {
			case started <- struct{}{}:
			default:
			}
			<-r.Context().Done()
			return
		}
		writeValid(w, r)
	}))
	defer server.Close()
	s := newTestService(t, server)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := s.Snapshot(ctx, "test"); done <- err }()
	<-started
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
	if len(s.backends["test"].cache) != 0 {
		t.Fatal("canceled owner updated cache")
	}
	s.timeout = 10 * time.Millisecond
	failed := getSnapshot(t, s)
	for _, observation := range failed.Capabilities {
		if observation.State != Unavailable {
			t.Fatal("timeout not unavailable")
		}
	}
	block.Store(false)
	s.backends["test"].next = time.Time{}
	if getSnapshot(t, s).Capabilities["version"].State != Supported {
		t.Fatal("timeout prevented recovery")
	}
}

func TestInvalidConfigurationErrorsDoNotEchoValues(t *testing.T) {
	for _, cfg := range []config.Backend{{URL: "http://user:SENTINEL@%zz"}, {URL: "file:///SENTINEL"},
		{URL: "http://example.invalid/?key=SENTINEL"}, {URL: "http://example.invalid", Diagnostics: "SENTINEL"}} {
		_, err := New(map[string]config.Backend{"test": cfg})
		if err == nil || strings.Contains(err.Error(), "SENTINEL") {
			t.Fatal("invalid configuration accepted or echoed")
		}
	}
}

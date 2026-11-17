package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/correlation"
)

func TestAdminOverviewRequiresLocalViewRoleAndRedactsBackendFields(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet || req.URL.Path != "/v1/models" {
			http.Error(w, "unexpected backend request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"udms/model-a","object":"model","owned_by":"udms","api_key":"never-return"}]}`))
	}))
	defer backend.Close()
	logger := discardLogger()
	proxy, err := New(config.Config{
		Listen: "0.0.0.0:8080", LogLevel: "INFO", DefaultBackend: "local",
		Backends:       map[string]config.Backend{"local": {URL: backend.URL}},
		ModelsCacheTTL: time.Minute, RequestTimeout: time.Second, HealthTimeout: time.Second,
		CatalogTimeout: time.Second, MaxJSONBody: 1 << 20,
	}, logger, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler := proxy.(*Handler)
	now := time.Now()
	handler.peerCache = &peerCache{entries: make(map[string]peerCacheEntry)}
	handler.peerCache.update("bigboi", nil, "private-upstream-detail", now)
	auth, err := newAdminAuth(&config.ManagementUIConfig{
		IssuerURL: "https://identity.example", ClientID: "catalog", ClientSecret: strings.Repeat("c", 32),
		RedirectURL: "https://catalog.example/admin/auth/callback", RoleGroups: map[string][]string{"view": {"viewers"}},
	}, logger, nil)
	if err != nil {
		t.Fatal(err)
	}
	auth.sessions["view-session"] = adminSession{subject: "local-user", roles: []string{"view"}, csrf: "csrf",
		lastSeen: now, expiresAt: now.Add(time.Hour)}
	auth.sessions["other-session"] = adminSession{subject: "other-user", roles: []string{"operate"}, csrf: "csrf",
		lastSeen: now, expiresAt: now.Add(time.Hour)}
	reloader := &Reloader{logger: logger, adminAuth: auth}
	reloader.state.Store(&reloaderState{handler: handler, peer: &config.PeerConfig{NodeID: "udms"}, peerCache: handler.peerCache})

	unauthenticated := httptest.NewRequest(http.MethodGet, "https://catalog.example/admin/api/v1/overview", nil)
	unauthenticated.Header.Set("X-Forwarded-User", "forged-admin")
	unauthenticatedResponse := httptest.NewRecorder()
	reloader.ServeHTTP(unauthenticatedResponse, unauthenticated)
	if unauthenticatedResponse.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated dashboard status = %d", unauthenticatedResponse.Code)
	}

	forbidden := httptest.NewRequest(http.MethodGet, "https://catalog.example/admin/api/v1/overview", nil)
	forbidden.AddCookie(&http.Cookie{Name: adminSessionCookie, Value: "other-session"})
	forbiddenResponse := httptest.NewRecorder()
	reloader.ServeHTTP(forbiddenResponse, forbidden)
	if forbiddenResponse.Code != http.StatusForbidden {
		t.Fatalf("non-view role dashboard status = %d", forbiddenResponse.Code)
	}

	request := httptest.NewRequest(http.MethodGet, "https://catalog.example/admin/api/v1/overview", nil)
	request.AddCookie(&http.Cookie{Name: adminSessionCookie, Value: "view-session"})
	response := httptest.NewRecorder()
	reloader.ServeHTTP(response, request)
	var result struct {
		NodeID              string `json:"node_id"`
		RequestSummaryState string `json:"request_summary_state"`
		Models              []struct {
			ID string `json:"id"`
		} `json:"models"`
		Peers []struct {
			NodeID string `json:"node_id"`
			Status string `json:"status"`
		} `json:"peers"`
	}
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &result) != nil {
		t.Fatalf("dashboard response = %d %s", response.Code, response.Body.String())
	}
	if result.NodeID != "udms" || result.RequestSummaryState != "not_configured" || len(result.Models) != 1 || result.Models[0].ID != "udms/model-a" ||
		len(result.Peers) != 1 || result.Peers[0].NodeID != "bigboi" || result.Peers[0].Status != "unavailable" {
		t.Fatalf("unexpected safe dashboard summary: %s", response.Body.String())
	}
	if strings.Contains(response.Body.String(), "never-return") || strings.Contains(response.Body.String(), "private-upstream-detail") {
		t.Fatalf("dashboard exposed sensitive backend/cache details: %s", response.Body.String())
	}
}

func TestAdminActivityRequiresViewAndReturnsSanitizedRecentEvents(t *testing.T) {
	now := time.Now()
	history := correlation.New()
	history.RegisterRequest("0123456789abcdef0123456789abcdef", "local", "", now.Add(-time.Minute))
	history.CompleteRequest("0123456789abcdef0123456789abcdef", 25*time.Millisecond, "http_success")
	logger := discardLogger()
	admin, err := newAdminAuth(&config.ManagementUIConfig{
		IssuerURL: "https://identity.example", ClientID: "catalog", ClientSecret: strings.Repeat("c", 32),
		RedirectURL: "https://catalog.example/admin/auth/callback", RoleGroups: map[string][]string{"view": {"viewers"}},
	}, logger, nil)
	if err != nil {
		t.Fatal(err)
	}
	admin.sessions["view-session"] = adminSession{subject: "local-user", roles: []string{"view"}, csrf: "csrf",
		lastSeen: now, expiresAt: now.Add(time.Hour)}
	handler := &Handler{logger: logger, history: history}
	reloader := &Reloader{logger: logger, adminAuth: admin}
	reloader.state.Store(&reloaderState{handler: handler, peerCache: &peerCache{entries: map[string]peerCacheEntry{}}})

	unauthenticated := httptest.NewRequest(http.MethodGet, "https://catalog.example/admin/api/v1/activity", nil)
	unauthenticatedResponse := httptest.NewRecorder()
	reloader.ServeHTTP(unauthenticatedResponse, unauthenticated)
	if unauthenticatedResponse.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated activity status = %d", unauthenticatedResponse.Code)
	}

	request := httptest.NewRequest(http.MethodGet, "https://catalog.example/admin/api/v1/activity", nil)
	request.AddCookie(&http.Cookie{Name: adminSessionCookie, Value: "view-session"})
	response := httptest.NewRecorder()
	reloader.ServeHTTP(response, request)
	var result struct {
		State  string `json:"state"`
		Events []struct {
			BackendID  string   `json:"backend_id"`
			Status     string   `json:"status"`
			DurationMS *float64 `json:"duration_ms"`
		} `json:"events"`
	}
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &result) != nil {
		t.Fatalf("activity response = %d %s", response.Code, response.Body.String())
	}
	if result.State != "available" || len(result.Events) != 1 || result.Events[0].BackendID != "local" ||
		result.Events[0].Status != "http_success" || result.Events[0].DurationMS == nil || *result.Events[0].DurationMS != 25 {
		t.Fatalf("unexpected activity summary: %s", response.Body.String())
	}
	for _, forbidden := range []string{"request_id", "activity_id", "metadata", "prompt", "reasoning", "0123456789abcdef"} {
		if strings.Contains(response.Body.String(), forbidden) {
			t.Fatalf("activity BFF leaked %q: %s", forbidden, response.Body.String())
		}
	}
}

func TestAdminOverviewUsesConfiguredBoundedRequestSummary(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	defer backend.Close()

	logger := discardLogger()
	httpHandler, err := New(config.Config{
		Listen: "0.0.0.0:8080", LogLevel: "INFO", DefaultBackend: "local",
		Backends:       map[string]config.Backend{"local": {URL: backend.URL}},
		ModelsCacheTTL: time.Minute, RequestTimeout: time.Second, HealthTimeout: time.Second,
		CatalogTimeout: time.Second, MaxJSONBody: 1 << 20, RequestHistory: true,
		DiagnosticsAPIKey: strings.Repeat("d", 32),
	}, logger, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler := httpHandler.(*Handler)
	now := time.Now()
	handler.history.RegisterRequest("0123456789abcdef0123456789abcdef", "local", "", now.Add(-time.Minute))
	handler.history.CompleteRequest("0123456789abcdef0123456789abcdef", 25*time.Millisecond, "http_success")

	auth, err := newAdminAuth(&config.ManagementUIConfig{
		IssuerURL: "https://identity.example", ClientID: "catalog", ClientSecret: strings.Repeat("c", 32),
		RedirectURL: "https://catalog.example/admin/auth/callback", RoleGroups: map[string][]string{"view": {"viewers"}},
	}, logger, nil)
	if err != nil {
		t.Fatal(err)
	}
	auth.sessions["view-session"] = adminSession{subject: "local-user", roles: []string{"view"}, csrf: "csrf",
		lastSeen: now, expiresAt: now.Add(time.Hour)}
	reloader := &Reloader{logger: logger, adminAuth: auth}
	reloader.state.Store(&reloaderState{handler: handler})

	request := httptest.NewRequest(http.MethodGet, "https://catalog.example/admin/api/v1/overview", nil)
	request.AddCookie(&http.Cookie{Name: adminSessionCookie, Value: "view-session"})
	response := httptest.NewRecorder()
	reloader.ServeHTTP(response, request)
	var result struct {
		RequestSummaryState string `json:"request_summary_state"`
		RequestSummary      struct {
			Count    int            `json:"count"`
			Outcomes map[string]int `json:"outcomes"`
		} `json:"request_summary"`
	}
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &result) != nil {
		t.Fatalf("dashboard response = %d %s", response.Code, response.Body.String())
	}
	if result.RequestSummaryState != "available" || result.RequestSummary.Count != 1 || result.RequestSummary.Outcomes["http_success"] != 1 {
		t.Fatalf("configured request summary = %s", response.Body.String())
	}
	if strings.Contains(response.Body.String(), "0123456789abcdef") || strings.Contains(response.Body.String(), "request_id") {
		t.Fatalf("dashboard exposed request identity: %s", response.Body.String())
	}
}

package proxy

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
)

type managementRoundTripper func(*http.Request) (*http.Response, error)

func (roundTrip managementRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return roundTrip(req)
}

func TestAdminManagementBFFRequiresConfigureModelsRoleAndCSRF(t *testing.T) {
	logger := discardLogger()
	auth, err := newAdminAuth(&config.ManagementUIConfig{
		IssuerURL: "https://identity.example", ClientID: "catalog", ClientSecret: strings.Repeat("c", 32),
		RedirectURL: "https://catalog.example/admin/auth/callback",
		RoleGroups:  map[string][]string{"view": {"viewers"}, "configure_models": {"model-operators"}},
	}, logger, nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	auth.sessions["viewer"] = adminSession{subject: "viewer", roles: []string{"view"}, csrf: "viewer-csrf", lastSeen: now, expiresAt: now.Add(time.Hour)}
	auth.sessions["operator"] = adminSession{subject: "operator", roles: []string{"configure_models"}, csrf: "operator-csrf", lastSeen: now, expiresAt: now.Add(time.Hour)}
	audit := newTestAuditStore(t, "node-a")
	reloader := &Reloader{logger: logger, adminAuth: auth, auditStore: audit, managementSlots: make(chan struct{}, 1)}
	reloader.state.Store(&reloaderState{
		peer:       &config.PeerConfig{NodeID: "node-a", Peers: map[string]config.PeerGrant{}},
		management: &config.ManagementConfig{DelegationToken: strings.Repeat("d", 32)},
	})
	body := `{"owner":{"node_id":"node-b","proxy_instance_id":"` + strings.Repeat("a", 32) + `","config_generation":1},"backend_id":"backend-a","model_id":"model-a","revision":"revision","settings":{}}`
	request := func(session, csrf string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "https://catalog.example/admin/api/v1/management/proposals", strings.NewReader(body))
		req.AddCookie(&http.Cookie{Name: adminSessionCookie, Value: session})
		req.Header.Set("Origin", "https://catalog.example")
		req.Header.Set("Content-Type", "application/json")
		if csrf != "" {
			req.Header.Set(adminCSRFHeader, csrf)
		}
		response := httptest.NewRecorder()
		reloader.ServeHTTP(response, req)
		return response
	}
	if response := request("viewer", "viewer-csrf"); response.Code != http.StatusForbidden {
		t.Fatalf("view-only management status = %d", response.Code)
	}
	if response := request("operator", ""); response.Code != http.StatusForbidden {
		t.Fatalf("missing-CSRF management status = %d", response.Code)
	}
	if response := request("operator", "operator-csrf"); response.Code != http.StatusBadGateway {
		t.Fatalf("authorized BFF should reach peer lookup, got %d %s", response.Code, response.Body.String())
	}
	events, err := audit.ListRecent(20)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, event := range events {
		if event.Action == "settings.proposal.create" && event.PrincipalType == "oidc" &&
			event.VerifiedUserSubject == "operator" && event.UserIdentityState == "verified_local" && event.Outcome == "unknown" {
			found = true
		}
	}
	if !found {
		t.Fatalf("local OIDC identity/outcome missing from audit: %+v", events)
	}
}

func TestAdminSettingsBFFUsesPeerCredentialAndReturnsTypedSnapshot(t *testing.T) {
	logger := discardLogger()
	auth, err := newAdminAuth(&config.ManagementUIConfig{
		IssuerURL: "https://identity.example", ClientID: "catalog", ClientSecret: strings.Repeat("c", 32),
		RedirectURL: "https://catalog.example/admin/auth/callback", RoleGroups: map[string][]string{"configure_models": {"model-operators"}},
	}, logger, nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	auth.sessions["operator"] = adminSession{subject: "operator", roles: []string{"configure_models"}, csrf: "operator-csrf", lastSeen: now, expiresAt: now.Add(time.Hour)}
	snapshot := modelconfig.SettingsSnapshot{
		TargetID: "target-a", ModelID: "repo/model-a", Revision: strings.Repeat("a", 64),
		Settings:   modelconfig.CommandSettings{ContextSize: func() *int32 { value := int32(4096); return &value }()},
		Validation: "yaml_syntax_and_supported_command_only",
	}
	remoteBody, err := json.Marshal(map[string]any{
		"owner":    modelconfig.DelegatedOwner{NodeID: "owner-b", ProxyInstanceID: strings.Repeat("b", 32), ConfigGeneration: 2},
		"snapshot": snapshot,
	})
	if err != nil {
		t.Fatal(err)
	}
	previewBody, err := json.Marshal(map[string]any{
		"owner": modelconfig.DelegatedOwner{NodeID: "owner-b", ProxyInstanceID: strings.Repeat("b", 32), ConfigGeneration: 2},
		"preview": modelconfig.Preview{TargetID: "target-a", ModelID: "repo/model-a", Revision: snapshot.Revision,
			Changes:    []modelconfig.SettingChange{{Setting: "context", Before: "2048", After: "4096"}},
			Validation: "yaml_syntax_and_supported_command_only"},
	})
	if err != nil {
		t.Fatal(err)
	}
	remoteGrant := modelconfig.OwnerGrant{
		ID: strings.Repeat("c", 32), RequesterNodeID: "node-a", Action: "manage_settings",
		BackendID: "backend-a", TargetID: "target-a", ModelIDs: []string{"repo/model-a"},
		CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour), ApprovedBy: "oidc_admin",
	}
	grantsBody, err := json.Marshal(map[string]any{
		"owner":  modelconfig.DelegatedOwner{NodeID: "owner-b", ProxyInstanceID: strings.Repeat("b", 32), ConfigGeneration: 2},
		"grants": []modelconfig.OwnerGrant{remoteGrant},
	})
	if err != nil {
		t.Fatal(err)
	}
	ownerPeerClient := &peerClient{id: "owner-b", origin: "https://owner-b.example", token: strings.Repeat("p", 32), routeManagement: true,
		client: &http.Client{Transport: managementRoundTripper(func(req *http.Request) (*http.Response, error) {
			if req.Header.Get("Authorization") != "Bearer "+strings.Repeat("p", 32) ||
				req.Header.Get("Cookie") != "" {
				t.Errorf("unsafe outbound BFF request: %s %s headers=%v", req.Method, req.URL.Path, req.Header)
			}
			body := remoteBody
			if req.URL.Path == peerSettingsGrantsPath {
				body = grantsBody
			} else if req.URL.Path == peerSettingsPreviewPath {
				body = previewBody
			} else if req.URL.Path != peerSettingsSnapshotPath {
				return nil, errors.New("unexpected peer management path")
			}
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}},
				Body: io.NopCloser(strings.NewReader(string(body)))}, nil
		})}}
	audit := newTestAuditStore(t, "node-a")
	reloader := &Reloader{logger: logger, adminAuth: auth, auditStore: audit, managementSlots: make(chan struct{}, 1)}
	reloader.state.Store(&reloaderState{
		peer:       &config.PeerConfig{NodeID: "node-a", Peers: map[string]config.PeerGrant{}},
		management: &config.ManagementConfig{}, peerClients: map[string]*peerClient{"owner-b": ownerPeerClient},
	})
	grantRequest := httptest.NewRequest(http.MethodGet, "https://catalog.example/admin/api/v1/management/grants?owner_node_id=owner-b", nil)
	grantRequest.AddCookie(&http.Cookie{Name: adminSessionCookie, Value: "operator"})
	grantResponse := httptest.NewRecorder()
	reloader.ServeHTTP(grantResponse, grantRequest)
	if grantResponse.Code != http.StatusOK || !strings.Contains(grantResponse.Body.String(), remoteGrant.ID) {
		t.Fatalf("typed owner grant list = %d %s", grantResponse.Code, grantResponse.Body.String())
	}
	body := `{"owner_node_id":"owner-b","backend_id":"backend-a","target_id":"target-a","model_id":"repo/model-a"}`
	req := httptest.NewRequest(http.MethodPost, "https://catalog.example/admin/api/v1/management/settings/snapshot", strings.NewReader(body))
	req.AddCookie(&http.Cookie{Name: adminSessionCookie, Value: "operator"})
	req.Header.Set("Origin", "https://catalog.example")
	req.Header.Set(adminCSRFHeader, "operator-csrf")
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	reloader.ServeHTTP(response, req)
	var result struct {
		Snapshot modelconfig.SettingsSnapshot `json:"snapshot"`
	}
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &result) != nil ||
		result.Snapshot.ModelID != snapshot.ModelID || result.Snapshot.Settings.ContextSize == nil || *result.Snapshot.Settings.ContextSize != 4096 {
		t.Fatalf("typed settings snapshot = %d %s", response.Code, response.Body.String())
	}
	previewRequest := httptest.NewRequest(http.MethodPost, "https://catalog.example/admin/api/v1/management/settings/preview",
		strings.NewReader(`{"owner_node_id":"owner-b","backend_id":"backend-a","target_id":"target-a","model_id":"repo/model-a","settings":{"context_size":4096}}`))
	previewRequest.AddCookie(&http.Cookie{Name: adminSessionCookie, Value: "operator"})
	previewRequest.Header.Set("Origin", "https://catalog.example")
	previewRequest.Header.Set(adminCSRFHeader, "operator-csrf")
	previewRequest.Header.Set("Content-Type", "application/json")
	previewResponse := httptest.NewRecorder()
	reloader.ServeHTTP(previewResponse, previewRequest)
	if previewResponse.Code != http.StatusOK || !strings.Contains(previewResponse.Body.String(), `"setting":"context"`) ||
		strings.Contains(previewResponse.Body.String(), "cmd:") {
		t.Fatalf("sanitized typed settings preview = %d %s", previewResponse.Code, previewResponse.Body.String())
	}
}

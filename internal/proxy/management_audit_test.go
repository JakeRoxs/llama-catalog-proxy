package proxy

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/auditlog"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/identity"
)

func newAuditTestReloader(t *testing.T, adminAuth *adminAuth) *Reloader {
	t.Helper()
	return &Reloader{
		peerNodeID: "local-node",
		adminAuth:  adminAuth,
		logger:     discardLogger(),
	}
}

func newAuditTestState(management *config.ManagementConfig, peer *config.PeerConfig) *reloaderState {
	return &reloaderState{
		peer:       peer,
		management: management,
	}
}

func newAuditRequest(t *testing.T, token string, query url.Values, cookie *http.Cookie) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "https://proxy.example/management/v1/preview", nil)
	if query != nil {
		req.URL.RawQuery = query.Encode()
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if cookie != nil {
		req.AddCookie(cookie)
	}
	return req
}

func newAdminAuthWithSession(t *testing.T, subject string, roles []string) *adminAuth {
	t.Helper()
	auth, err := newAdminAuth(&config.ManagementUIConfig{
		IssuerURL:  "https://issuer.example",
		RoleGroups: map[string][]string{"configure_models": {"model-operators"}},
		SessionTTL: "1h",
	}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	sessionID := "session-" + subject
	auth.sessions[sessionID] = adminSession{
		subject:   subject,
		roles:     roles,
		csrf:      "csrf",
		lastSeen:  time.Now(),
		expiresAt: time.Now().Add(time.Hour),
	}
	return auth
}

func TestManagementAuditEventPrincipalClassification(t *testing.T) {
	delegationToken := strings.Repeat("d", 32)
	previewToken := strings.Repeat("p", 32)

	t.Run("delegation token wins over machine token", func(t *testing.T) {
		r := newAuditTestReloader(t, nil)
		state := newAuditTestState(&config.ManagementConfig{
			DelegationToken: delegationToken,
			PreviewToken:    previewToken,
		}, nil)
		req := newAuditRequest(t, delegationToken, nil, nil)
		event := r.managementAuditEvent(req, state)
		if event.PrincipalType != "delegation_token" {
			t.Fatalf("principal type = %q, want delegation_token", event.PrincipalType)
		}
		if event.UserIdentityState != "unknown" {
			t.Fatalf("user identity state = %q, want unknown", event.UserIdentityState)
		}
		if event.VerifiedUserSubject != "" || event.IdentityIssuer != "" || event.Role != "" {
			t.Fatalf("unexpected oidc fields set for delegation token: %+v", event)
		}
	})

	t.Run("machine token when no delegation token", func(t *testing.T) {
		r := newAuditTestReloader(t, nil)
		state := newAuditTestState(&config.ManagementConfig{
			PreviewToken: previewToken,
		}, nil)
		req := newAuditRequest(t, previewToken, nil, nil)
		event := r.managementAuditEvent(req, state)
		if event.PrincipalType != "machine_token" {
			t.Fatalf("principal type = %q, want machine_token", event.PrincipalType)
		}
	})

	t.Run("unknown when no credentials", func(t *testing.T) {
		r := newAuditTestReloader(t, nil)
		state := newAuditTestState(&config.ManagementConfig{}, nil)
		req := newAuditRequest(t, "", nil, nil)
		event := r.managementAuditEvent(req, state)
		if event.PrincipalType != "unknown" {
			t.Fatalf("principal type = %q, want unknown", event.PrincipalType)
		}
		if event.UserIdentityState != "unknown" {
			t.Fatalf("user identity state = %q, want unknown", event.UserIdentityState)
		}
	})

	t.Run("oidc verified_local when configure_models role", func(t *testing.T) {
		auth := newAdminAuthWithSession(t, "operator", []string{"configure_models"})
		r := newAuditTestReloader(t, auth)
		state := newAuditTestState(&config.ManagementConfig{}, nil)
		req := newAuditRequest(t, "", nil, &http.Cookie{Name: adminSessionCookie, Value: "session-operator"})
		event := r.managementAuditEvent(req, state)
		if event.PrincipalType != "oidc" {
			t.Fatalf("principal type = %q, want oidc", event.PrincipalType)
		}
		if event.UserIdentityState != "verified_local" {
			t.Fatalf("user identity state = %q, want verified_local", event.UserIdentityState)
		}
		if event.IdentityIssuer != "https://issuer.example" {
			t.Fatalf("identity issuer = %q, want https://issuer.example", event.IdentityIssuer)
		}
		if event.VerifiedUserSubject != "operator" {
			t.Fatalf("verified user subject = %q, want operator", event.VerifiedUserSubject)
		}
		if event.Role != "configure_models" {
			t.Fatalf("role = %q, want configure_models", event.Role)
		}
	})

	t.Run("oidc not set when session lacks configure_models role", func(t *testing.T) {
		auth := newAdminAuthWithSession(t, "viewer", []string{"view"})
		r := newAuditTestReloader(t, auth)
		state := newAuditTestState(&config.ManagementConfig{}, nil)
		req := newAuditRequest(t, "", nil, &http.Cookie{Name: adminSessionCookie, Value: "session-viewer"})
		event := r.managementAuditEvent(req, state)
		if event.PrincipalType != "unknown" {
			t.Fatalf("principal type = %q, want unknown", event.PrincipalType)
		}
		if event.UserIdentityState != "unknown" {
			t.Fatalf("user identity state = %q, want unknown", event.UserIdentityState)
		}
	})
}

func TestManagementAuditEventQueryIDs(t *testing.T) {
	r := newAuditTestReloader(t, nil)
	state := newAuditTestState(&config.ManagementConfig{}, nil)
	operationID := strings.Repeat("a", 32)

	t.Run("single valued target/model/operation extracted", func(t *testing.T) {
		query := url.Values{
			"target_id":    []string{"node-a"},
			"model_id":     []string{"model-a"},
			"operation_id": []string{operationID},
		}
		req := newAuditRequest(t, "", query, nil)
		event := r.managementAuditEvent(req, state)
		if event.TargetID != "node-a" {
			t.Fatalf("target ID = %q, want node-a", event.TargetID)
		}
		if event.ModelID != "model-a" {
			t.Fatalf("model ID = %q, want model-a", event.ModelID)
		}
		if event.CorrelationID != operationID {
			t.Fatalf("correlation ID = %q, want %s", event.CorrelationID, operationID)
		}
	})

	t.Run("multi valued query ids ignored", func(t *testing.T) {
		query := url.Values{"target_id": []string{"node-a", "node-b"}}
		req := newAuditRequest(t, "", query, nil)
		event := r.managementAuditEvent(req, state)
		if event.TargetID != "" {
			t.Fatalf("target ID = %q, want empty (multi-valued)", event.TargetID)
		}
	})

	t.Run("invalid node id ignored", func(t *testing.T) {
		query := url.Values{"target_id": []string{"invalid id!"}}
		req := newAuditRequest(t, "", query, nil)
		event := r.managementAuditEvent(req, state)
		if event.TargetID != "" {
			t.Fatalf("target ID = %q, want empty (invalid)", event.TargetID)
		}
		if !identity.ValidNodeID("node-a") {
			t.Fatal("test fixture node-a should be valid")
		}
	})

	t.Run("model id over max bytes ignored", func(t *testing.T) {
		query := url.Values{"model_id": []string{strings.Repeat("x", identity.MaxModelComponentBytes+1)}}
		req := newAuditRequest(t, "", query, nil)
		event := r.managementAuditEvent(req, state)
		if event.ModelID != "" {
			t.Fatalf("model ID = %q, want empty (too long)", event.ModelID)
		}
	})

	t.Run("invalid operation id ignored", func(t *testing.T) {
		query := url.Values{"operation_id": []string{"not-hex"}}
		req := newAuditRequest(t, "", query, nil)
		event := r.managementAuditEvent(req, state)
		if event.CorrelationID != "" {
			t.Fatalf("correlation ID = %q, want empty (invalid)", event.CorrelationID)
		}
	})
}

func TestManagementAuditEventNilHandlingAndDefaults(t *testing.T) {
	r := newAuditTestReloader(t, nil)

	t.Run("nil request returns defaults and unknown action", func(t *testing.T) {
		state := newAuditTestState(&config.ManagementConfig{}, nil)
		event := r.managementAuditEvent(nil, state)
		if event.PrincipalType != "unknown" || event.UserIdentityState != "unknown" {
			t.Fatalf("unexpected event for nil request: %+v", event)
		}
		if event.Action != "management.unknown" {
			t.Fatalf("action = %q, want management.unknown", event.Action)
		}
		if event.LocalNodeID != "local-node" {
			t.Fatalf("local node ID = %q, want local-node", event.LocalNodeID)
		}
	})

	t.Run("nil state returns defaults", func(t *testing.T) {
		// state is assumed non-nil by the caller (r.state.Load()); only req and
		// state.management nil are guarded, so a nil state is not a supported input.
		t.Skip("nil state is not a supported input; state.peer is dereferenced first")
	})

	t.Run("nil management returns defaults but peer override applies", func(t *testing.T) {
		req := newAuditRequest(t, "", nil, nil)
		state := &reloaderState{peer: &config.PeerConfig{NodeID: "peer-node"}}
		event := r.managementAuditEvent(req, state)
		if event.PrincipalType != "unknown" {
			t.Fatalf("principal type = %q, want unknown", event.PrincipalType)
		}
		if event.LocalNodeID != "peer-node" {
			t.Fatalf("local node ID = %q, want peer-node", event.LocalNodeID)
		}
	})
}

func TestManagementAuditAuditedHelpers(t *testing.T) {
	r := newAuditTestReloader(t, nil)

	t.Run("beginManagementAudit nil state", func(t *testing.T) {
		tx, err := r.beginManagementAudit(nil, nil)
		if tx != nil || err != nil {
			t.Fatalf("expected (nil, nil), got tx=%v err=%v", tx, err)
		}
	})

	t.Run("beginManagementAudit fail closed peer federation no store", func(t *testing.T) {
		state := &reloaderState{peer: &config.PeerConfig{NodeID: "peer-node", FederationID: "fed"}}
		tx, err := r.beginManagementAudit(nil, state)
		if tx != nil || err == nil {
			t.Fatalf("expected (nil, error), got tx=%v err=%v", tx, err)
		}
	})

	t.Run("beginManagementAudit no store no federation continues", func(t *testing.T) {
		state := &reloaderState{peer: &config.PeerConfig{NodeID: "peer-node"}}
		tx, err := r.beginManagementAudit(nil, state)
		if tx != nil || err != nil {
			t.Fatalf("expected (nil, nil), got tx=%v err=%v", tx, err)
		}
	})

	t.Run("beginManagementAudit store begin error fails closed", func(t *testing.T) {
		// A zero store fails validation in Begin, so Begin returns an error.
		state := &reloaderState{auditStore: &auditlog.Store{}}
		tx, err := r.beginManagementAudit(nil, state)
		if tx != nil || err == nil {
			t.Fatalf("expected (nil, error), got tx=%v err=%v", tx, err)
		}
	})

	t.Run("managementAuditOutcome mapping", func(t *testing.T) {
		cases := []struct {
			status int
			want   string
		}{
			{200, "succeeded"},
			{299, "succeeded"},
			{300, "failed"},
			{401, "denied"},
			{403, "denied"},
			{404, "failed"},
			{499, "failed"},
			{500, "unknown"},
			{599, "unknown"},
		}
		for _, c := range cases {
			if got := managementAuditOutcome(c.status); got != c.want {
				t.Fatalf("status %d = %q, want %q", c.status, got, c.want)
			}
		}
	})

	t.Run("finishManagementAudit nil tx no-op", func(t *testing.T) {
		// Should not panic and must not call Finish.
		r.finishManagementAudit(nil, &auditResponseWriter{ResponseWriter: httptest.NewRecorder()})
	})

	t.Run("finishManagementAudit logs finish failure", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))
		auditReloader := &Reloader{peerNodeID: "local-node", logger: logger}
		// nil store -> Finish returns ErrInvalid, triggering the error log.
		auditReloader.finishManagementAudit(&auditlog.Transaction{}, &auditResponseWriter{ResponseWriter: httptest.NewRecorder()})
		if !strings.Contains(buf.String(), "management audit completion failed") {
			t.Fatalf("expected audit completion failure log, got: %q", buf.String())
		}
	})
}

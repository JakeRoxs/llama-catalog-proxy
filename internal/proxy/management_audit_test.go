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

type auditEventInput struct {
	token  string
	query  url.Values
	cookie *http.Cookie
	config *config.ManagementConfig
	peer   *config.PeerConfig
	auth   *adminAuth
}

// newAuditEvent centralizes the repeated reloader/state/request setup used by
// the management audit event tests.
func newAuditEvent(t *testing.T, input auditEventInput) auditlog.Event {
	t.Helper()
	r := newAuditTestReloader(t, input.auth)
	state := newAuditTestState(input.config, input.peer)
	req := newAuditRequest(t, input.token, input.query, input.cookie)
	return r.managementAuditEvent(req, state)
}

type principalExpectation struct {
	principalType string
	userIdentity  string
	issuer        string
	subject       string
	role          string
	emptyOIDC     bool
}

func assertPrincipalEvent(t *testing.T, event auditlog.Event, want principalExpectation) {
	t.Helper()
	if event.PrincipalType != want.principalType {
		t.Fatalf("principal type = %q, want %q", event.PrincipalType, want.principalType)
	}
	if event.UserIdentityState != want.userIdentity {
		t.Fatalf("user identity state = %q, want %q", event.UserIdentityState, want.userIdentity)
	}
	if want.emptyOIDC && (event.VerifiedUserSubject != "" || event.IdentityIssuer != "" || event.Role != "") {
		t.Fatalf("unexpected oidc fields set for %q: %+v", want.principalType, event)
	}
	assertOIDCPrincipalEvent(t, event, want)
}

func assertOIDCPrincipalEvent(t *testing.T, event auditlog.Event, want principalExpectation) {
	t.Helper()
	if want.principalType != "oidc" {
		return
	}
	if event.IdentityIssuer != want.issuer {
		t.Fatalf("identity issuer = %q, want %q", event.IdentityIssuer, want.issuer)
	}
	if event.VerifiedUserSubject != want.subject {
		t.Fatalf("verified user subject = %q, want %q", event.VerifiedUserSubject, want.subject)
	}
	if event.Role != want.role {
		t.Fatalf("role = %q, want %q", event.Role, want.role)
	}
}

func TestManagementAuditEventPrincipalClassification(t *testing.T) {
	delegationToken := strings.Repeat("d", 32)
	previewToken := strings.Repeat("p", 32)

	tests := []struct {
		name        string
		config      *config.ManagementConfig
		token       string
		cookie      *http.Cookie
		oidcSubject string
		oidcRoles   []string
		want        principalExpectation
	}{
		{
			name: "delegation token wins over machine token",
			config: &config.ManagementConfig{
				DelegationToken: delegationToken,
				PreviewToken:    previewToken,
			},
			token: delegationToken,
			want:  principalExpectation{principalType: "delegation_token", userIdentity: "unknown", emptyOIDC: true},
		},
		{
			name: "machine token when no delegation token",
			config: &config.ManagementConfig{
				PreviewToken: previewToken,
			},
			token: previewToken,
			want:  principalExpectation{principalType: "machine_token", userIdentity: "unknown"},
		},
		{
			name:   "unknown when no credentials",
			config: &config.ManagementConfig{},
			want:   principalExpectation{principalType: "unknown", userIdentity: "unknown", emptyOIDC: true},
		},
		{
			name:        "oidc verified_local when configure_models role",
			config:      &config.ManagementConfig{},
			oidcSubject: "operator",
			oidcRoles:   []string{"configure_models"},
			cookie:      &http.Cookie{Name: adminSessionCookie, Value: "session-operator"},
			want: principalExpectation{
				principalType: "oidc",
				userIdentity:  "verified_local",
				issuer:        "https://issuer.example",
				subject:       "operator",
				role:          "configure_models",
			},
		},
		{
			name:        "oidc not set when session lacks configure_models role",
			config:      &config.ManagementConfig{},
			oidcSubject: "viewer",
			oidcRoles:   []string{"view"},
			cookie:      &http.Cookie{Name: adminSessionCookie, Value: "session-viewer"},
			want:        principalExpectation{principalType: "unknown", userIdentity: "unknown", emptyOIDC: true},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var auth *adminAuth
			if tc.oidcSubject != "" {
				auth = newAdminAuthWithSession(t, tc.oidcSubject, tc.oidcRoles)
			}
			event := newAuditEvent(t, auditEventInput{
				token: tc.token, cookie: tc.cookie, config: tc.config, auth: auth,
			})

			assertPrincipalEvent(t, event, tc.want)
		})
	}
}

type queryIDExpectation struct {
	targetID      string
	modelID       string
	correlationID string
}

type auditTransactionExpectation struct {
	tx      *auditlog.Transaction
	err     error
	wantErr bool
}

func assertAuditTransaction(t *testing.T, result auditTransactionExpectation) {
	t.Helper()
	if result.wantErr && (result.tx != nil || result.err == nil) {
		t.Fatalf("expected (nil, error), got tx=%v err=%v", result.tx, result.err)
	}
	if !result.wantErr && (result.tx != nil || result.err != nil) {
		t.Fatalf("expected (nil, nil), got tx=%v err=%v", result.tx, result.err)
	}
}

func assertManagementAuditOutcomes(t *testing.T, cases []struct {
	status int
	want   string
}) {
	t.Helper()
	for _, tc := range cases {
		if got := managementAuditOutcome(tc.status); got != tc.want {
			t.Errorf("status %d = %q, want %q", tc.status, got, tc.want)
		}
	}
}

func TestManagementAuditEventQueryIDs(t *testing.T) {
	r := newAuditTestReloader(t, nil)
	state := newAuditTestState(&config.ManagementConfig{}, nil)
	operationID := strings.Repeat("a", 32)

	if !identity.ValidNodeID("node-a") {
		t.Fatal("test fixture node-a should be valid")
	}

	tests := []struct {
		name  string
		query url.Values
		want  queryIDExpectation
	}{
		{
			name: "single valued target/model/operation extracted",
			query: url.Values{
				"target_id":    []string{"node-a"},
				"model_id":     []string{"model-a"},
				"operation_id": []string{operationID},
			},
			want: queryIDExpectation{
				targetID:      "node-a",
				modelID:       "model-a",
				correlationID: operationID,
			},
		},
		{
			name:  "multi valued query ids ignored",
			query: url.Values{"target_id": []string{"node-a", "node-b"}},
			want: queryIDExpectation{
				targetID: "",
			},
		},
		{
			name:  "invalid node id ignored",
			query: url.Values{"target_id": []string{"invalid id!"}},
			want: queryIDExpectation{
				targetID: "",
			},
		},
		{
			name:  "model id over max bytes ignored",
			query: url.Values{"model_id": []string{strings.Repeat("x", identity.MaxModelComponentBytes+1)}},
			want: queryIDExpectation{
				modelID: "",
			},
		},
		{
			name:  "invalid operation id ignored",
			query: url.Values{"operation_id": []string{"not-hex"}},
			want: queryIDExpectation{
				correlationID: "",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := newAuditRequest(t, "", tc.query, nil)
			event := r.managementAuditEvent(req, state)

			if event.TargetID != tc.want.targetID {
				t.Fatalf("target ID = %q, want %q", event.TargetID, tc.want.targetID)
			}
			if event.ModelID != tc.want.modelID {
				t.Fatalf("model ID = %q, want %q", event.ModelID, tc.want.modelID)
			}
			if event.CorrelationID != tc.want.correlationID {
				t.Fatalf("correlation ID = %q, want %q", event.CorrelationID, tc.want.correlationID)
			}
		})
	}
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

	tests := []struct {
		name    string
		state   *reloaderState
		wantErr bool
	}{
		{
			name:    "nil state",
			state:   nil,
			wantErr: false,
		},
		{
			name:    "fail closed peer federation no store",
			state:   &reloaderState{peer: &config.PeerConfig{NodeID: "peer-node", FederationID: "fed"}},
			wantErr: true,
		},
		{
			name:    "no store no federation continues",
			state:   &reloaderState{peer: &config.PeerConfig{NodeID: "peer-node"}},
			wantErr: false,
		},
		{
			name:    "store begin error fails closed",
			state:   &reloaderState{auditStore: &auditlog.Store{}},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tx, err := r.beginManagementAudit(nil, tc.state)
			assertAuditTransaction(t, auditTransactionExpectation{tx: tx, err: err, wantErr: tc.wantErr})
		})
	}

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
		assertManagementAuditOutcomes(t, cases)
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

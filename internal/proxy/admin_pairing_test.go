package proxy

import (
	"crypto/x509"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/federation"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
)

func TestAdminPairingInvitationRequiresRoleAndCSRFAndNeverListsCode(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := federation.OpenPairingStore(directory, "lab.example", "node-a")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	audit := newTestAuditStore(t, "node-a")
	now := time.Now()
	logger := discardLogger()
	auth, err := newAdminAuth(&config.ManagementUIConfig{
		IssuerURL: "https://identity.example", ClientID: "catalog", ClientSecret: strings.Repeat("c", 32),
		RedirectURL: "https://catalog.example/admin/auth/callback",
		RoleGroups:  map[string][]string{"view": {"viewers"}, "administer_federation": {"federation-admins"}},
	}, logger, nil)
	if err != nil {
		t.Fatal(err)
	}
	auth.sessions["view-session"] = adminSession{subject: "viewer", roles: []string{"view"}, csrf: "view-csrf", lastSeen: now, expiresAt: now.Add(time.Hour)}
	auth.sessions["admin-session"] = adminSession{subject: "operator", roles: []string{"administer_federation"}, csrf: "admin-csrf", lastSeen: now, expiresAt: now.Add(time.Hour)}
	reloader := &Reloader{logger: logger, adminAuth: auth, auditStore: audit}
	reloader.state.Store(&reloaderState{peer: &config.PeerConfig{NodeID: "node-a", FederationID: "lab.example"}, pairingStore: store, auditStore: audit})

	post := func(session, csrf string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "https://catalog.example/admin/api/v1/federation/invitations", strings.NewReader(`{"node_id":"node-b"}`))
		req.AddCookie(&http.Cookie{Name: adminSessionCookie, Value: session})
		req.Header.Set("Origin", "https://catalog.example")
		if csrf != "" {
			req.Header.Set(adminCSRFHeader, csrf)
		}
		req.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		reloader.ServeHTTP(response, req)
		return response
	}
	if response := post("view-session", "view-csrf"); response.Code != http.StatusForbidden {
		t.Fatalf("view role invitation status = %d", response.Code)
	}
	if response := post("admin-session", ""); response.Code != http.StatusForbidden {
		t.Fatalf("missing CSRF invitation status = %d", response.Code)
	}
	created := post("admin-session", "admin-csrf")
	if created.Code != http.StatusCreated {
		t.Fatalf("create invitation = %d %s", created.Code, created.Body.String())
	}
	var invite federation.Invitation
	if err := json.Unmarshal(created.Body.Bytes(), &invite); err != nil || invite.Code == "" || invite.NodeID != "node-b" {
		t.Fatalf("created invitation payload = %+v, %v", invite, err)
	}
	list := httptest.NewRequest(http.MethodGet, "https://catalog.example/admin/api/v1/federation/invitations", nil)
	list.AddCookie(&http.Cookie{Name: adminSessionCookie, Value: "admin-session"})
	listed := httptest.NewRecorder()
	reloader.ServeHTTP(listed, list)
	if listed.Code != http.StatusOK || strings.Contains(listed.Body.String(), invite.Code) || !strings.Contains(listed.Body.String(), "node-b") {
		t.Fatalf("invitation list exposed code or omitted target: %d %s", listed.Code, listed.Body.String())
	}
	events, err := audit.ListRecent(20)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, event := range events {
		if event.Action == "pairing.invitation.create" && event.PrincipalType == "oidc" &&
			event.VerifiedUserSubject == "operator" && event.TargetID == "node-b" && event.Outcome == "succeeded" {
			found = true
		}
	}
	auditBytes, _ := json.Marshal(events)
	if !found || strings.Contains(string(auditBytes), invite.Code) {
		t.Fatalf("verified local actor missing or invitation secret was audited: %+v", events)
	}
	auditRequest := httptest.NewRequest(http.MethodGet, "https://catalog.example/admin/api/v1/audit/events?limit=20", nil)
	auditRequest.AddCookie(&http.Cookie{Name: adminSessionCookie, Value: "admin-session"})
	auditResponse := httptest.NewRecorder()
	reloader.ServeHTTP(auditResponse, auditRequest)
	if auditResponse.Code != http.StatusOK || !strings.Contains(auditResponse.Body.String(), "verified_user_subject") || strings.Contains(auditResponse.Body.String(), invite.Code) {
		t.Fatalf("protected audit response = %d %s", auditResponse.Code, auditResponse.Body.String())
	}
}

func TestPairingJoinUsesOneTimeInvitationAndPersistsBothMemberships(t *testing.T) {
	ca, caKey, roots := newPeerIdentityTestCA(t)
	ownerCertificate, _ := newPeerIdentityTestPair(t, ca, caKey, "lab.example", "node-a")
	joiningCertificate, _ := newPeerIdentityTestPair(t, ca, caKey, "lab.example", "node-b")
	crls := []*x509.RevocationList{newPeerIdentityTestCRL(t, ca, caKey)}
	ownerDirectory, joiningDirectory := t.TempDir(), t.TempDir()
	for _, directory := range []string{ownerDirectory, joiningDirectory} {
		if err := os.Chmod(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	ownerStore, err := federation.OpenPairingStore(ownerDirectory, "lab.example", "node-a")
	if err != nil {
		t.Fatal(err)
	}
	defer ownerStore.Close()
	joiningStore, err := federation.OpenPairingStore(joiningDirectory, "lab.example", "node-b")
	if err != nil {
		t.Fatal(err)
	}
	defer joiningStore.Close()
	ownerAudit := newTestAuditStore(t, "node-a")
	joiningAudit := newTestAuditStore(t, "node-b")
	invite, err := ownerStore.CreateInvitation("node-b", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	owner := &Reloader{peerSlots: make(chan struct{}, 8), auditStore: ownerAudit}
	owner.state.Store(&reloaderState{
		peer:        &config.PeerConfig{NodeID: "node-a", FederationID: "lab.example"},
		certificate: &ownerCertificate, peerClientCAs: roots, peerClientCRLs: crls, pairingStore: ownerStore, auditStore: ownerAudit,
	})
	server := httptest.NewUnstartedServer(owner.PeerHandler())
	server.TLS = owner.PeerTLSConfig()
	server.StartTLS()
	defer server.Close()

	logger := discardLogger()
	auth, err := newAdminAuth(&config.ManagementUIConfig{
		IssuerURL: "https://identity.example", ClientID: "catalog", ClientSecret: strings.Repeat("c", 32),
		RedirectURL: "https://catalog.example/admin/auth/callback", RoleGroups: map[string][]string{"administer_federation": {"federation-admins"}},
	}, logger, nil)
	if err != nil {
		t.Fatal(err)
	}
	auth.sessions["admin-session"] = adminSession{subject: "operator", roles: []string{"administer_federation"}, csrf: "admin-csrf", lastSeen: time.Now(), expiresAt: time.Now().Add(time.Hour)}
	joining := &Reloader{logger: logger, adminAuth: auth, auditStore: joiningAudit}
	joining.state.Store(&reloaderState{
		peer:        &config.PeerConfig{NodeID: "node-b", FederationID: "lab.example"},
		certificate: &joiningCertificate, peerClientCAs: roots, peerClientCRLs: crls, pairingStore: joiningStore, auditStore: joiningAudit,
	})
	requestJoin := func(code string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "https://catalog.example/admin/api/v1/federation/join",
			strings.NewReader(`{"url":"`+server.URL+`","invitation_code":"`+code+`"}`))
		req.AddCookie(&http.Cookie{Name: adminSessionCookie, Value: "admin-session"})
		req.Header.Set("Origin", "https://catalog.example")
		req.Header.Set(adminCSRFHeader, "admin-csrf")
		req.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		joining.ServeHTTP(response, req)
		return response
	}
	if rejected := requestJoin(strings.Repeat("x", 43)); rejected.Code != http.StatusBadGateway {
		t.Fatalf("invalid invitation response = %d %s", rejected.Code, rejected.Body.String())
	}
	ownerMembers, ownerErr := ownerStore.ListMembers()
	joiningMembers, joiningErr := joiningStore.ListMembers()
	if ownerErr != nil || joiningErr != nil || len(ownerMembers) != 0 || len(joiningMembers) != 0 {
		t.Fatal("failed invitation handshake changed membership")
	}
	joined := requestJoin(invite.Code)
	if joined.Code != http.StatusOK || !strings.Contains(joined.Body.String(), `"node_id":"node-a"`) || strings.Contains(joined.Body.String(), "scopes") {
		t.Fatalf("web join = %d %s", joined.Code, joined.Body.String())
	}
	if !ownerStore.IsMember("node-b") || !joiningStore.IsMember("node-a") {
		t.Fatal("pairing did not persist reciprocal identity memberships")
	}
	if retry := requestJoin(invite.Code); retry.Code != http.StatusOK {
		t.Fatalf("lost-response retry was not idempotent: %d %s", retry.Code, retry.Body.String())
	}
	ownerMembers, ownerErr = ownerStore.ListMembers()
	joiningMembers, joiningErr = joiningStore.ListMembers()
	if ownerErr != nil || joiningErr != nil || len(ownerMembers) != 1 || len(joiningMembers) != 1 {
		t.Fatal("invitation replay created duplicate membership")
	}
}

func TestAdminOwnerGrantRequiresStaticCeilingAndCreatesTargetModelGrant(t *testing.T) {
	root := t.TempDir()
	membershipDir := filepath.Join(root, "membership")
	grantDir := filepath.Join(root, "grants")
	targetDir := filepath.Join(root, "target")
	for _, directory := range []string{membershipDir, grantDir, targetDir} {
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(targetDir, "config.yaml"), []byte("models:\n  model:\n    cmd: llama-server -c 1024\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := federation.OpenPairingStore(membershipDir, "lab.example", "owner")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.AddMember("node-b", strings.Repeat("a", 64), time.Now()); err != nil {
		t.Fatal(err)
	}
	audit := newTestAuditStore(t, "owner")
	logger := discardLogger()
	auth, err := newAdminAuth(&config.ManagementUIConfig{
		IssuerURL: "https://identity.example", ClientID: "catalog", ClientSecret: strings.Repeat("c", 32),
		RedirectURL: "https://catalog.example/admin/auth/callback", RoleGroups: map[string][]string{"administer_federation": {"federation-admins"}},
	}, logger, nil)
	if err != nil {
		t.Fatal(err)
	}
	auth.sessions["admin-session"] = adminSession{subject: "operator", roles: []string{"administer_federation"}, csrf: "admin-csrf", lastSeen: time.Now(), expiresAt: time.Now().Add(time.Hour)}
	management := &config.ManagementConfig{
		Targets:        map[string]config.ManagementTarget{"target-a": {Directory: targetDir, Filename: "config.yaml"}},
		DelegatedStore: &config.DelegatedStoreConfig{ProposalDirectory: filepath.Join(root, "proposals"), OperationDirectory: filepath.Join(root, "operations"), GrantDirectory: grantDir},
	}
	peer := &config.PeerConfig{
		NodeID: "owner", FederationID: "lab.example", LocalBackends: []string{"backend-a"},
		ManagementTargets: map[string]string{"backend-a": "target-a"},
		Peers:             map[string]config.PeerGrant{"node-b": {Token: strings.Repeat("b", 32), ManageSettings: true}},
	}
	reloader := &Reloader{logger: logger, adminAuth: auth, auditStore: audit}
	reloader.state.Store(&reloaderState{peer: peer, management: management, pairingStore: store, auditStore: audit})
	create := func(requester string, confirm bool) *httptest.ResponseRecorder {
		confirmation := "false"
		if confirm {
			confirmation = "true"
		}
		body := `{"requester_node_id":"` + requester + `","backend_id":"backend-a","model_ids":["model"],"expires_in":"24h","confirm_grant":` + confirmation + `}`
		req := httptest.NewRequest(http.MethodPost, "https://catalog.example/admin/api/v1/federation/grants", strings.NewReader(body))
		req.AddCookie(&http.Cookie{Name: adminSessionCookie, Value: "admin-session"})
		req.Header.Set("Origin", "https://catalog.example")
		req.Header.Set(adminCSRFHeader, "admin-csrf")
		req.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		reloader.ServeHTTP(response, req)
		return response
	}
	if response := create("node-b", false); response.Code != http.StatusBadRequest {
		t.Fatalf("missing explicit grant confirmation status = %d", response.Code)
	}
	if response := create("node-c", true); response.Code != http.StatusForbidden {
		t.Fatalf("unpaired owner grant status = %d %s", response.Code, response.Body.String())
	}
	created := create("node-b", true)
	if created.Code != http.StatusCreated {
		t.Fatalf("create owner grant = %d %s", created.Code, created.Body.String())
	}
	var grant modelconfig.OwnerGrant
	if err := json.Unmarshal(created.Body.Bytes(), &grant); err != nil || grant.RequesterNodeID != "node-b" || grant.TargetID != "target-a" {
		t.Fatalf("owner grant response = %+v, %v", grant, err)
	}
	if allowed, err := modelconfig.OwnerGrantAuthorizes(grantDir, "node-b", "manage_settings", "backend-a", "target-a", "model"); err != nil || !allowed {
		t.Fatalf("authorized target-model grant = %v, %v", allowed, err)
	}
	if allowed, err := modelconfig.OwnerGrantAuthorizes(grantDir, "node-b", "manage_settings", "backend-a", "target-a", "other-model"); err != nil || allowed {
		t.Fatalf("grant broadened to an unapproved model = %v, %v", allowed, err)
	}
	listRequest := httptest.NewRequest(http.MethodGet, "https://catalog.example/admin/api/v1/federation/grants", nil)
	listRequest.AddCookie(&http.Cookie{Name: adminSessionCookie, Value: "admin-session"})
	listResponse := httptest.NewRecorder()
	reloader.ServeHTTP(listResponse, listRequest)
	if listResponse.Code != http.StatusOK || !strings.Contains(listResponse.Body.String(), grant.ID) || strings.Contains(listResponse.Body.String(), "delegation_token") {
		t.Fatalf("owner grant list response = %d %s", listResponse.Code, listResponse.Body.String())
	}
	revokeRequest := httptest.NewRequest(http.MethodDelete, "https://catalog.example/admin/api/v1/federation/grants/"+grant.ID, nil)
	revokeRequest.AddCookie(&http.Cookie{Name: adminSessionCookie, Value: "admin-session"})
	revokeRequest.Header.Set("Origin", "https://catalog.example")
	revokeRequest.Header.Set(adminCSRFHeader, "admin-csrf")
	revokeResponse := httptest.NewRecorder()
	reloader.ServeHTTP(revokeResponse, revokeRequest)
	if revokeResponse.Code != http.StatusNoContent {
		t.Fatalf("owner grant revoke = %d %s", revokeResponse.Code, revokeResponse.Body.String())
	}
	if allowed, err := modelconfig.OwnerGrantAuthorizes(grantDir, "node-b", "manage_settings", "backend-a", "target-a", "model"); err != nil || allowed {
		t.Fatalf("revoked owner grant remained authorized = %v, %v", allowed, err)
	}
	peer.Peers["node-b"] = config.PeerGrant{Token: strings.Repeat("b", 32), ManageSettings: false}
	if response := create("node-b", true); response.Code != http.StatusForbidden {
		t.Fatalf("grant bypassed the static ManageSettings ceiling: %d %s", response.Code, response.Body.String())
	}
}

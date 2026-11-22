//go:build linux

package proxy

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/auditlog"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/federation"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
)

// testPeerManagementTargetDirectories creates the scratch directories used by
// the peer management target test and returns them keyed by purpose.
func testPeerManagementTargetDirectories(t *testing.T, root string) map[string]string {
	t.Helper()
	dirs := map[string]string{}
	for _, name := range []string{"membership", "grants", "target", "proposals", "operations"} {
		dirs[name] = filepath.Join(root, name)
		if err := os.Mkdir(dirs[name], 0o700); err != nil {
			t.Fatal(err)
		}
	}
	return dirs
}

// requestForPeer issues an authenticated request against the peer server and
// returns the response. The requester identity is a forged admin bearer token
// over a client certificate, so the handler must rely on mTLS peer identity.
func requestForPeer(t *testing.T, client *http.Client, serverURL string, method, path string, body []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, serverURL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+strings.Repeat("b", 32))
	req.Header.Set("X-Authenticated-User", "forged-admin")
	req.Header.Set("Content-Type", "application/json")
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

// assertOwnerGrantsResponse verifies the owner-scoped grant list includes the
// target model and excludes the target secret.
func assertOwnerGrantsResponse(t *testing.T, resp *http.Response) {
	t.Helper()
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil || resp.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("model-a")) || bytes.Contains(body, []byte("do-not-return")) {
		t.Fatalf("owner-scoped grant response = %d %s, %v", resp.StatusCode, body, err)
	}
}

// assertSettingsSnapshot verifies the typed settings snapshot is owner-scoped,
// carries the expected context size, and excludes the target secret.
func assertSettingsSnapshot(t *testing.T, resp *http.Response) {
	t.Helper()
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	var result struct {
		Owner    modelconfig.DelegatedOwner   `json:"owner"`
		Snapshot modelconfig.SettingsSnapshot `json:"snapshot"`
	}
	if err != nil || resp.StatusCode != http.StatusOK || json.Unmarshal(body, &result) != nil ||
		result.Owner.NodeID != "owner" || result.Snapshot.Settings.ContextSize == nil ||
		*result.Snapshot.Settings.ContextSize != 1024 || bytes.Contains(body, []byte("do-not-return")) {
		t.Fatalf("typed settings snapshot = %d %s, %v", resp.StatusCode, body, err)
	}
}

// assertSettingsPreview verifies the typed settings preview is sanitized:
// it exposes the requested setting name and command, but excludes the target
// secret and the original command.
func assertSettingsPreview(t *testing.T, resp *http.Response) {
	t.Helper()
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil || resp.StatusCode != http.StatusOK || !bytes.Contains(body, []byte(`"setting":"context"`)) ||
		bytes.Contains(body, []byte("do-not-return")) || bytes.Contains(body, []byte("llama-server -c 1024")) {
		t.Fatalf("sanitized typed settings preview = %d %s, %v", resp.StatusCode, body, err)
	}
}

// assertRevocationDenied verifies that a revoked owner grant no longer exposes
// target settings through the settings snapshot endpoint.
func assertRevocationDenied(t *testing.T, resp *http.Response) {
	t.Helper()
	_ = resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		t.Fatal("revoked owner grant still exposed target settings")
	}
}

// assertAuditUnknownPrincipal verifies the audit trail keeps the remote human
// requester explicitly unknown (peer_node principal, unknown identity state,
// empty verified subject) for the settings snapshot read action.
func assertAuditUnknownPrincipal(t *testing.T, events []auditlog.Event) {
	t.Helper()
	foundUnknown := false
	for _, event := range events {
		if event.Action == "settings.snapshot.read" && event.PrincipalType == "peer_node" &&
			event.RequesterNodeID == "node-b" && event.UserIdentityState == "unknown" && event.VerifiedUserSubject == "" {
			foundUnknown = true
		}
	}
	if !foundUnknown {
		t.Fatalf("remote human attribution was not kept explicitly unknown: %+v", events)
	}
}

func TestPeerSettingsGrantSnapshotAndPreviewAreOwnerScopedAndSanitized(t *testing.T) {
	ca, caKey, roots := newPeerIdentityTestCA(t)
	ownerCertificate, _ := newPeerIdentityTestPair(t, ca, caKey, "lab.example", "owner")
	clientCertificate, clientLeaf := newPeerIdentityTestPair(t, ca, caKey, "lab.example", "node-b")
	crls := []*x509.RevocationList{newPeerIdentityTestCRL(t, ca, caKey)}
	root := t.TempDir()
	dirs := testPeerManagementTargetDirectories(t, root)
	const targetYAML = "models:\n  model-a:\n    cmd: llama-server -c 1024\nsecret: do-not-return\n"
	if err := os.WriteFile(filepath.Join(dirs["target"], "config.yaml"), []byte(targetYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	members, err := federation.OpenPairingStore(dirs["membership"], "lab.example", "owner")
	if err != nil {
		t.Fatal(err)
	}
	defer members.Close()
	if _, err := members.AddMember("node-b", peerCertificateFingerprint(clientLeaf), time.Now()); err != nil {
		t.Fatal(err)
	}
	grant := modelconfig.OwnerGrant{
		ID: "0123456789abcdef0123456789abcdef", RequesterNodeID: "node-b", Action: "manage_settings",
		BackendID: "backend-a", TargetID: "target-a", ModelIDs: []string{"model-a"},
		CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour), ApprovedBy: "oidc_admin",
	}
	if err := modelconfig.CreateOwnerGrant(dirs["grants"], grant); err != nil {
		t.Fatal(err)
	}
	audit := newTestAuditStore(t, "owner")
	owner := &Reloader{peerSlots: make(chan struct{}, 8), instanceID: strings.Repeat("a", 32), auditStore: audit}
	owner.state.Store(&reloaderState{
		peer: &config.PeerConfig{
			NodeID: "owner", FederationID: "lab.example", LocalBackends: []string{"backend-a"},
			ManagementTargets: map[string]string{"backend-a": "target-a"},
			Peers:             map[string]config.PeerGrant{"node-b": {Token: strings.Repeat("b", 32), ManageSettings: true}},
		},
		management: &config.ManagementConfig{
			Targets:        map[string]config.ManagementTarget{"target-a": {Directory: dirs["target"], Filename: "config.yaml"}},
			DelegatedStore: &config.DelegatedStoreConfig{GrantDirectory: dirs["grants"], ProposalDirectory: dirs["proposals"], OperationDirectory: dirs["operations"]},
		},
		generation: 2, certificate: &ownerCertificate, peerClientCAs: roots, peerClientCRLs: crls, pairingStore: members, auditStore: audit,
	})
	server := httptest.NewUnstartedServer(owner.PeerHandler())
	server.TLS = owner.PeerTLSConfig()
	server.StartTLS()
	defer server.Close()
	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, Certificates: []tls.Certificate{clientCertificate}}}}
	defer client.CloseIdleConnections()
	grantsResponse := requestForPeer(t, client, server.URL, http.MethodGet, peerSettingsGrantsPath, nil)
	assertOwnerGrantsResponse(t, grantsResponse)
	input := delegatedSettingsTargetRequest{BackendID: "backend-a", TargetID: "target-a", ModelID: "model-a"}
	snapshotRequest, _ := json.Marshal(input)
	snapshotResponse := requestForPeer(t, client, server.URL, http.MethodPost, peerSettingsSnapshotPath, snapshotRequest)
	assertSettingsSnapshot(t, snapshotResponse)
	previewInput := delegatedSettingsPreviewRequest{BackendID: "backend-a", TargetID: "target-a", ModelID: "model-a",
		Settings: modelconfig.CommandSettings{ContextSize: func() *int32 { value := int32(4096); return &value }()}}
	previewRequest, _ := json.Marshal(previewInput)
	previewResponse := requestForPeer(t, client, server.URL, http.MethodPost, peerSettingsPreviewPath, previewRequest)
	assertSettingsPreview(t, previewResponse)
	if err := modelconfig.RevokeOwnerGrant(dirs["grants"], grant.ID); err != nil {
		t.Fatal(err)
	}
	denied := requestForPeer(t, client, server.URL, http.MethodPost, peerSettingsSnapshotPath, snapshotRequest)
	_ = denied.Body.Close()
	assertRevocationDenied(t, denied)
	events, err := audit.ListRecent(20)
	if err != nil {
		t.Fatal(err)
	}
	assertAuditUnknownPrincipal(t, events)
}

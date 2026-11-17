//go:build linux

package proxy

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
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

func TestDelegatedOwnerGrantIntersectionAndLegacyBoundary(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	grant := modelconfig.OwnerGrant{
		ID: "0123456789abcdef0123456789abcdef", RequesterNodeID: "node-b", Action: "manage_settings",
		BackendID: "backend-a", TargetID: "target-a", ModelIDs: []string{"model-a"},
		CreatedAt: now, ExpiresAt: now.Add(time.Hour), ApprovedBy: "oidc_admin",
	}
	if err := modelconfig.CreateOwnerGrant(directory, grant); err != nil {
		t.Fatal(err)
	}
	state := &reloaderState{
		peer: &config.PeerConfig{FederationID: "lab.example"},
		management: &config.ManagementConfig{DelegatedStore: &config.DelegatedStoreConfig{
			GrantDirectory: directory, ProposalDirectory: filepath.Join(directory, "proposals"), OperationDirectory: filepath.Join(directory, "operations"),
		}},
	}
	if !delegatedOwnerGrantAllows(state, "node-b", "backend-a", "target-a", "model-a") {
		t.Fatal("valid target/model grant rejected")
	}
	if delegatedOwnerGrantAllows(state, "node-b", "backend-a", "target-a", "model-b") ||
		delegatedOwnerGrantAllows(state, "node-c", "backend-a", "target-a", "model-a") ||
		delegatedOwnerGrantAllows(state, "node-b", "backend-a", "target-b", "model-a") {
		t.Fatal("owner grant broadened beyond requester/target/model")
	}
	state.peer.FederationID = ""
	if !delegatedOwnerGrantAllows(state, "node-b", "backend-a", "target-b", "model-b") {
		t.Fatal("legacy bearer-only behavior changed by the mTLS owner grant layer")
	}
}

func TestCreateDelegatedProposalRechecksDurableGrant(t *testing.T) {
	ca, caKey, roots := newPeerIdentityTestCA(t)
	_, clientLeaf := newPeerIdentityTestPair(t, ca, caKey, "lab.example", "node-b")
	crls := []*x509.RevocationList{newPeerIdentityTestCRL(t, ca, caKey)}
	root := t.TempDir()
	directories := map[string]string{}
	for _, name := range []string{"membership", "grants", "proposals", "operations", "proposal-store", "backups", "target"} {
		directory := filepath.Join(root, name)
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
		directories[name] = directory
	}
	if err := os.WriteFile(filepath.Join(directories["target"], "config.yaml"), []byte("models:\n  model-a:\n    cmd: llama-server -c 1024\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	members, err := federation.OpenPairingStore(directories["membership"], "lab.example", "owner")
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
	if err := modelconfig.CreateOwnerGrant(directories["grants"], grant); err != nil {
		t.Fatal(err)
	}
	audit := newTestAuditStore(t, "owner")
	peer := &config.PeerConfig{
		NodeID: "owner", FederationID: "lab.example", LocalBackends: []string{"backend-a"},
		ManagementTargets: map[string]string{"backend-a": "target-a"},
		Peers:             map[string]config.PeerGrant{"node-b": {Token: strings.Repeat("b", 32), ManageSettings: true}},
	}
	management := &config.ManagementConfig{
		WriteToken:    strings.Repeat("w", 32),
		ProposalStore: &config.ProposalStoreConfig{Directory: directories["proposal-store"], Token: strings.Repeat("p", 32)},
		BackupStore:   &config.BackupStoreConfig{Directory: directories["backups"], Token: strings.Repeat("c", 32), MaxBackups: 4},
		Targets:       map[string]config.ManagementTarget{"target-a": {Directory: directories["target"], Filename: "config.yaml"}},
		DelegatedStore: &config.DelegatedStoreConfig{
			ProposalDirectory: directories["proposals"], OperationDirectory: directories["operations"], GrantDirectory: directories["grants"],
		},
	}
	state := &reloaderState{
		peer: peer, management: management, generation: 1, pairingStore: members, peerClientCAs: roots, peerClientCRLs: crls, auditStore: audit,
	}
	r := &Reloader{instanceID: strings.Repeat("a", 32), logger: discardLogger(), auditStore: audit}
	r.state.Store(state)
	size := int32(2048)
	preview, err := modelconfig.PreviewTarget(directories["target"], "config.yaml", "target-a", "model-a", modelconfig.CommandSettings{ContextSize: &size})
	if err != nil {
		t.Fatal(err)
	}
	input := delegatedSettingsRequest{
		Owner:   modelconfig.DelegatedOwner{NodeID: "owner", ProxyInstanceID: r.instanceID, ConfigGeneration: 1},
		Backend: "backend-a", Model: "model-a", Revision: preview.Revision, Settings: modelconfig.CommandSettings{ContextSize: &size},
	}
	request := httptest.NewRequest(http.MethodPost, "https://peer.example/peer/v1/management/settings/proposals", nil)
	request.Header.Set("Authorization", "Bearer "+strings.Repeat("b", 32))
	request.TLS = &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{clientLeaf}, VerifiedChains: [][]*x509.Certificate{{clientLeaf, ca}},
	}
	proposal, err := r.createDelegatedSettingsProposal(request, state, input, time.Hour)
	if err != nil {
		t.Fatalf("approved target/model grant rejected: %v", err)
	}
	if err := modelconfig.RevokeOwnerGrant(directories["grants"], grant.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := r.createDelegatedSettingsProposal(request, state, input, time.Hour); !errors.Is(err, errDelegationUnauthorized) {
		t.Fatalf("revoked owner grant still authorized proposal: %v", err)
	}
	if _, _, err := r.applyDelegatedSettings(request, state, delegatedSettingsApply{
		Backend: "backend-a", ProposalID: proposal.Proposal.ID, OperationID: strings.Repeat("d", 32),
		ReviewRevision: proposal.ReviewRevision, AcknowledgeReview: true,
		AcknowledgeDisruption: true, AcknowledgeExclusiveWindow: true,
	}); !errors.Is(err, errDelegationUnauthorized) {
		t.Fatalf("revoked owner grant still authorized previously reviewed proposal: %v", err)
	}
	events, err := audit.ListRecent(20)
	if err != nil {
		t.Fatal(err)
	}
	foundUnknown := false
	for _, event := range events {
		if event.Action == "settings.proposal.create" && event.PrincipalType == "peer_node" &&
			event.RequesterNodeID == "node-b" && event.UserIdentityState == "unknown" && event.VerifiedUserSubject == "" {
			foundUnknown = true
		}
	}
	if !foundUnknown {
		t.Fatalf("remote user attribution was not preserved as unknown: %+v", events)
	}
}

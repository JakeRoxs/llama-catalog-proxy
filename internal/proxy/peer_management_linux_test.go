//go:build linux

package proxy

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
)

func TestCreateDelegatedSettingsProposal(t *testing.T) {
	targetDir, store, operations := t.TempDir(), t.TempDir(), t.TempDir()
	if err := os.Chmod(store, 0o700); err != nil {
		t.Fatal(err)
	}
	aliasDir := t.TempDir()
	targetAlias, storeAlias := filepath.Join(aliasDir, "target"), filepath.Join(aliasDir, "store")
	if err := os.Symlink(targetDir, targetAlias); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(store, storeAlias); err != nil {
		t.Fatal(err)
	}
	source := "secret: confidential\nmodels: {model: {cmd: 'llama-server -m /private/model.gguf -c 1024'}}\n"
	targetPath := filepath.Join(targetDir, "config.yaml")
	if err := os.WriteFile(targetPath, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	size := int32(4096)
	settings := modelconfig.CommandSettings{ContextSize: &size}
	preview, err := modelconfig.PreviewTarget(targetDir, "config.yaml", "swap", "model", settings)
	if err != nil {
		t.Fatal(err)
	}
	token := strings.Repeat("p", 32)
	state := &reloaderState{
		generation: 1,
		peer: &config.PeerConfig{
			NodeID: "owner", LocalBackends: []string{"backend"},
			ManagementTargets: map[string]string{"backend": "swap"},
			Peers:             map[string]config.PeerGrant{"requester": {Token: token, ManageSettings: true}},
		},
		management: &config.ManagementConfig{
			DelegatedStore: &config.DelegatedStoreConfig{ProposalDirectory: store, OperationDirectory: operations},
			Targets: map[string]config.ManagementTarget{
				"swap": {Directory: targetDir, Filename: "config.yaml"},
			},
		},
	}
	r := &Reloader{instanceID: strings.Repeat("a", 32)}
	r.state.Store(state)
	request := httptest.NewRequest(http.MethodPost, "https://peer.example/peer/v1/management", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	input := delegatedSettingsRequest{
		Owner:   modelconfig.DelegatedOwner{NodeID: "owner", ProxyInstanceID: r.instanceID, ConfigGeneration: 1},
		Backend: "backend", Model: "model", Revision: preview.Revision, Settings: settings,
	}
	proposal, err := r.createDelegatedSettingsProposal(request, state, input, 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	assertDelegatedProposalOwnership(t, proposal)
	assertDelegatedProposalPersisted(t, store, proposal.Proposal.ID, input.Owner, proposal.Proposal.CreatedAt)
	assertDelegatedProposalNoLeak(t, proposal, token)
	assertTargetUnchanged(t, targetPath, source)
	assertAllDelegatedProposalRejections(t, state, r, request, input, store, proposal, token, targetDir, targetAlias, storeAlias)
	r.state.Store(&reloaderState{})
	if _, err := r.createDelegatedSettingsProposal(request, state, input, 15*time.Minute); !errors.Is(err, errDelegationChanged) {
		t.Fatalf("stale configuration accepted: %v", err)
	}
	r.state.Store(state)
	r.stopped = true
	if _, err := r.createDelegatedSettingsProposal(request, state, input, 15*time.Minute); !errors.Is(err, errDelegationChanged) {
		t.Fatalf("stopped owner accepted proposal: %v", err)
	}
}

func assertDelegatedProposalOwnership(t *testing.T, proposal modelconfig.DelegatedProposal) {
	if proposal.RequesterNode != "requester" || proposal.Proposal.TargetID != "swap" || proposal.Proposal.ModelID != "model" {
		t.Fatalf("unexpected owner binding: %#v", proposal)
	}
}

func assertDelegatedProposalPersisted(t *testing.T, store string, id string, owner modelconfig.DelegatedOwner, createdAt time.Time) {
	stored, err := modelconfig.LoadDelegatedProposal(store, id)
	if err != nil || modelconfig.ValidateDelegatedProposal(stored, owner, "requester", "backend", "swap", createdAt) != nil {
		t.Fatalf("owner review was not persisted correctly: %v", err)
	}
}

func assertDelegatedProposalNoLeak(t *testing.T, proposal modelconfig.DelegatedProposal, token string) {
	data, err := json.Marshal(proposal)
	if err != nil || strings.Contains(string(data), "confidential") || strings.Contains(string(data), "/private/") || strings.Contains(string(data), token) {
		t.Fatalf("proposal leaked source or credential: %v", err)
	}
}

func assertTargetUnchanged(t *testing.T, path, source string) {
	got, err := os.ReadFile(path)
	if err != nil || string(got) != source {
		t.Fatalf("creating review changed the target: %v", err)
	}
}

func assertAllDelegatedProposalRejections(t *testing.T, state *reloaderState, r *Reloader, request *http.Request, input delegatedSettingsRequest, store string, proposal modelconfig.DelegatedProposal, token, targetDir, targetAlias, storeAlias string) {
	for _, tt := range []struct {
		name   string
		change func(*reloaderState, *http.Request, *delegatedSettingsRequest) string
		want   error
	}{
		{"missing TLS", func(_ *reloaderState, req *http.Request, _ *delegatedSettingsRequest) string {
			req.TLS = nil
			return store
		}, errDelegationUnauthorized},
		{"ungranted peer", func(s *reloaderState, _ *http.Request, _ *delegatedSettingsRequest) string {
			s.peer.Peers["requester"] = config.PeerGrant{Token: token, CatalogRead: true}
			return store
		}, errDelegationUnauthorized},
		{"wrong token", func(_ *reloaderState, req *http.Request, _ *delegatedSettingsRequest) string {
			req.Header.Set("Authorization", "Bearer "+strings.Repeat("x", 32))
			return store
		}, errDelegationUnauthorized},
		{"owner restarted", func(_ *reloaderState, _ *http.Request, in *delegatedSettingsRequest) string {
			in.Owner.ProxyInstanceID = strings.Repeat("b", 32)
			return store
		}, errDelegationChanged},
		{"source changed", func(_ *reloaderState, _ *http.Request, in *delegatedSettingsRequest) string {
			in.Revision = strings.Repeat("b", 64)
			return store
		}, errDelegationChanged},
		{"unowned backend", func(s *reloaderState, _ *http.Request, _ *delegatedSettingsRequest) string {
			s.peer.LocalBackends = nil
			return store
		}, errDelegationTarget},
		{"mapping removed", func(s *reloaderState, _ *http.Request, _ *delegatedSettingsRequest) string {
			delete(s.peer.ManagementTargets, "backend")
			return store
		}, errDelegationTarget},
		{"missing target", func(s *reloaderState, _ *http.Request, _ *delegatedSettingsRequest) string {
			delete(s.management.Targets, "swap")
			return store
		}, errDelegationTarget},
		{"delegated stores disabled", func(s *reloaderState, _ *http.Request, _ *delegatedSettingsRequest) string {
			s.management.DelegatedStore = nil
			return store
		}, modelconfig.ErrProposalStore},
		{"store aliases target", func(_ *reloaderState, _ *http.Request, _ *delegatedSettingsRequest) string {
			return targetDir
		}, modelconfig.ErrProposalStore},
		{"store symlinks to target", func(_ *reloaderState, _ *http.Request, _ *delegatedSettingsRequest) string {
			return targetAlias
		}, modelconfig.ErrProposalStore},
		{"store aliases local proposals", func(s *reloaderState, _ *http.Request, _ *delegatedSettingsRequest) string {
			s.management.ProposalStore = &config.ProposalStoreConfig{Directory: storeAlias}
			return store
		}, modelconfig.ErrProposalStore},
		{"store aliases backups", func(s *reloaderState, _ *http.Request, _ *delegatedSettingsRequest) string {
			s.management.BackupStore = &config.BackupStoreConfig{Directory: storeAlias}
			return store
		}, modelconfig.ErrProposalStore},
		{"store aliases ingestion", func(s *reloaderState, _ *http.Request, _ *delegatedSettingsRequest) string {
			s.management.Ingestion = &config.IngestionConfig{Directory: storeAlias, RecordDirectory: t.TempDir()}
			return store
		}, modelconfig.ErrProposalStore},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := *state
			s.peer = copyPeerConfig(state.peer)
			s.management = copyManagementConfig(state.management)
			req := request.Clone(request.Context())
			in := input
			r.state.Store(&s)
			directory := tt.change(&s, req, &in)
			if s.management.DelegatedStore != nil {
				s.management.DelegatedStore.ProposalDirectory = directory
			}
			_, err := r.createDelegatedSettingsProposal(req, &s, in, 15*time.Minute)
			if !errors.Is(err, tt.want) {
				t.Fatalf("got %v, want %v", err, tt.want)
			}
			ids, err := modelconfig.ListDelegatedProposals(store)
			if err != nil || len(ids) != 1 || ids[0] != proposal.Proposal.ID {
				t.Fatalf("rejected action modified store: %v, %v", ids, err)
			}
		})
	}
}

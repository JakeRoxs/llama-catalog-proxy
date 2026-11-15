//go:build linux

package proxy

import (
	"bytes"
	"context"
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

type delegatedApplyFixture struct {
	r          *Reloader
	state      *reloaderState
	request    *http.Request
	proposals  string
	operations string
	target     string
	source     []byte
	input      delegatedSettingsApply
}

func newDelegatedApplyFixture(t *testing.T) delegatedApplyFixture {
	t.Helper()
	target, proposals, operations, localProposals, backups := t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir()
	for _, directory := range []string{proposals, operations, localProposals, backups} {
		if err := os.Chmod(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	source := []byte("secret: confidential\nmodels: {model: {cmd: 'llama-server -m /private/model.gguf -c 1024'}}\n")
	if err := os.WriteFile(filepath.Join(target, "config.yaml"), source, 0o600); err != nil {
		t.Fatal(err)
	}
	size := int32(4096)
	settings := modelconfig.CommandSettings{ContextSize: &size}
	preview, err := modelconfig.PreviewTarget(target, "config.yaml", "swap", "model", settings)
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
			WriteToken:     strings.Repeat("w", 32),
			DelegatedStore: &config.DelegatedStoreConfig{ProposalDirectory: proposals, OperationDirectory: operations},
			ProposalStore:  &config.ProposalStoreConfig{Directory: localProposals},
			BackupStore:    &config.BackupStoreConfig{Directory: backups, Token: strings.Repeat("b", 32), MaxBackups: 2},
			Targets:        map[string]config.ManagementTarget{"swap": {Directory: target, Filename: "config.yaml"}},
		},
	}
	r := &Reloader{instanceID: strings.Repeat("a", 32), peerNodeID: "owner"}
	r.state.Store(state)
	req := httptest.NewRequest(http.MethodPost, "https://peer.example/peer/v1/management", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	proposal, err := r.createDelegatedSettingsProposal(req, state, delegatedSettingsRequest{
		Owner:   modelconfig.DelegatedOwner{NodeID: "owner", ProxyInstanceID: r.instanceID, ConfigGeneration: 1},
		Backend: "backend", Model: "model", Revision: preview.Revision, Settings: settings,
	}, 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return delegatedApplyFixture{
		r: r, state: state, request: req, proposals: proposals, operations: operations, target: target, source: source,
		input: delegatedSettingsApply{
			Backend: "backend", ProposalID: proposal.Proposal.ID, OperationID: strings.Repeat("e", 32),
			ReviewRevision: proposal.ReviewRevision, AcknowledgeReview: true, AcknowledgeDisruption: true,
			AcknowledgeExclusiveWindow: true,
		},
	}
}

func (f delegatedApplyFixture) apply() (modelconfig.DelegatedOperation, bool, error) {
	return f.r.applyDelegatedSettings(f.request, f.state, f.input)
}

func TestApplyDelegatedSettingsAuthorizedAndReplay(t *testing.T) {
	f := newDelegatedApplyFixture(t)
	record, reserved, err := f.apply()
	if err != nil || !reserved || record.Outcome != "written" || record.RequesterNode != "requester" ||
		record.CallerScope != "unknown" || record.BackendID != "backend" || record.TargetID != "swap" {
		t.Fatalf("apply = %+v, %v, %v", record, reserved, err)
	}
	data, err := json.Marshal(record)
	if err != nil || bytes.Contains(data, []byte("confidential")) || bytes.Contains(data, []byte("/private/")) ||
		bytes.Contains(data, []byte(strings.Repeat("p", 32))) || bytes.Contains(data, []byte(f.target)) {
		t.Fatalf("operation record exposed secrets or target path: %v", err)
	}
	written, err := os.ReadFile(filepath.Join(f.target, "config.yaml"))
	if err != nil || bytes.Equal(written, f.source) {
		t.Fatalf("target not updated: %v", err)
	}
	record, reserved, err = f.apply()
	if err != nil || reserved || record.Outcome != "written" {
		t.Fatalf("replay = %+v, %v, %v", record, reserved, err)
	}
	f.state.peer.Peers["requester"] = config.PeerGrant{Token: strings.Repeat("p", 32), CatalogRead: true}
	if _, reserved, err := f.apply(); !errors.Is(err, errDelegationUnauthorized) || reserved {
		t.Fatalf("revoked peer could retry: %v, %v", reserved, err)
	}
	after, err := os.ReadFile(filepath.Join(f.target, "config.yaml"))
	if err != nil || !bytes.Equal(after, written) {
		t.Fatalf("replay or revocation changed target: %v", err)
	}
}

func TestApplyDelegatedSettingsWithSeparateOptionalStores(t *testing.T) {
	f := newDelegatedApplyFixture(t)
	f.state.management.Ingestion = &config.IngestionConfig{Directory: t.TempDir(), RecordDirectory: t.TempDir()}
	f.r.benchmark = &benchmarkRuntime{config: config.BenchmarkConfig{ReportDirectory: t.TempDir()}}
	if err := os.Chmod(f.state.management.Ingestion.Directory, 0o700); err != nil {
		t.Fatal(err)
	}
	record, reserved, err := f.apply()
	if err != nil || !reserved || record.Outcome != "written" {
		t.Fatalf("separate optional stores rejected: %+v, %v, %v", record, reserved, err)
	}
}

func TestApplyDelegatedSettingsRejectsUnsafeRequests(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*testing.T, *delegatedApplyFixture)
		want   error
	}{
		{"missing TLS", func(_ *testing.T, f *delegatedApplyFixture) { f.request.TLS = nil }, errDelegationUnauthorized},
		{"revoked scope", func(_ *testing.T, f *delegatedApplyFixture) {
			f.state.peer.Peers["requester"] = config.PeerGrant{Token: strings.Repeat("p", 32), CatalogRead: true}
		}, errDelegationUnauthorized},
		{"other authorized peer", func(_ *testing.T, f *delegatedApplyFixture) {
			token := strings.Repeat("q", 32)
			f.state.peer.Peers["other"] = config.PeerGrant{Token: token, ManageSettings: true}
			f.request.Header.Set("Authorization", "Bearer "+token)
		}, modelconfig.ErrDelegatedProposalBinding},
		{"missing review acknowledgment", func(_ *testing.T, f *delegatedApplyFixture) { f.input.AcknowledgeReview = false }, errDelegationAcknowledgment},
		{"missing disruption acknowledgment", func(_ *testing.T, f *delegatedApplyFixture) { f.input.AcknowledgeDisruption = false }, errDelegationAcknowledgment},
		{"missing exclusive window", func(_ *testing.T, f *delegatedApplyFixture) { f.input.AcknowledgeExclusiveWindow = false }, errDelegationAcknowledgment},
		{"local writes disabled", func(_ *testing.T, f *delegatedApplyFixture) { f.state.management.WriteToken = "" }, modelconfig.ErrTargetWrite},
		{"backup disabled", func(_ *testing.T, f *delegatedApplyFixture) { f.state.management.BackupStore = nil }, modelconfig.ErrTargetWrite},
		{"delegated stores disabled", func(_ *testing.T, f *delegatedApplyFixture) { f.state.management.DelegatedStore = nil }, modelconfig.ErrProposalStore},
		{"backend no longer owned", func(_ *testing.T, f *delegatedApplyFixture) { f.state.peer.LocalBackends = nil }, errDelegationTarget},
		{"mapping removed", func(_ *testing.T, f *delegatedApplyFixture) { delete(f.state.peer.ManagementTargets, "backend") }, errDelegationTarget},
		{"target remapped", func(t *testing.T, f *delegatedApplyFixture) {
			f.state.peer.ManagementTargets["backend"] = "other"
			f.state.management.Targets["other"] = config.ManagementTarget{Directory: t.TempDir(), Filename: "config.yaml"}
		}, modelconfig.ErrDelegatedProposalBinding},
		{"owner restarted", func(_ *testing.T, f *delegatedApplyFixture) { f.r.instanceID = strings.Repeat("b", 32) }, modelconfig.ErrDelegatedProposalBinding},
		{"generation changed", func(_ *testing.T, f *delegatedApplyFixture) { f.state.generation++ }, modelconfig.ErrDelegatedProposalBinding},
		{"review changed", func(_ *testing.T, f *delegatedApplyFixture) { f.input.ReviewRevision = strings.Repeat("b", 64) }, modelconfig.ErrDelegatedProposalBinding},
		{"stores identical", func(_ *testing.T, f *delegatedApplyFixture) { f.operations = f.proposals }, modelconfig.ErrProposalStore},
		{"store symlink aliases review", func(t *testing.T, f *delegatedApplyFixture) {
			alias := filepath.Join(t.TempDir(), "alias")
			if err := os.Symlink(f.proposals, alias); err != nil {
				t.Fatal(err)
			}
			f.operations = alias
		}, modelconfig.ErrProposalStore},
		{"operation aliases target", func(_ *testing.T, f *delegatedApplyFixture) { f.operations = f.target }, modelconfig.ErrProposalStore},
		{"operation aliases backups", func(_ *testing.T, f *delegatedApplyFixture) {
			f.operations = f.state.management.BackupStore.Directory
		}, modelconfig.ErrProposalStore},
		{"operation nested inside review", func(t *testing.T, f *delegatedApplyFixture) {
			f.operations = filepath.Join(f.proposals, "nested")
			if err := os.Mkdir(f.operations, 0o700); err != nil {
				t.Fatal(err)
			}
		}, modelconfig.ErrProposalStore},
		{"backup aliases local proposals", func(_ *testing.T, f *delegatedApplyFixture) {
			f.state.management.BackupStore.Directory = f.state.management.ProposalStore.Directory
		}, modelconfig.ErrProposalStore},
		{"backup aliases target", func(_ *testing.T, f *delegatedApplyFixture) {
			f.state.management.BackupStore.Directory = f.target
		}, modelconfig.ErrProposalStore},
		{"benchmark aliases backup", func(_ *testing.T, f *delegatedApplyFixture) {
			f.r.benchmark = &benchmarkRuntime{config: config.BenchmarkConfig{ReportDirectory: f.state.management.BackupStore.Directory}}
		}, modelconfig.ErrProposalStore},
		{"operation aliases benchmark", func(_ *testing.T, f *delegatedApplyFixture) {
			f.r.benchmark = &benchmarkRuntime{config: config.BenchmarkConfig{ReportDirectory: f.operations}}
		}, modelconfig.ErrProposalStore},
		{"ingestion aliases backup", func(t *testing.T, f *delegatedApplyFixture) {
			f.state.management.Ingestion = &config.IngestionConfig{
				Directory: f.state.management.BackupStore.Directory, RecordDirectory: t.TempDir(),
			}
		}, modelconfig.ErrProposalStore},
		{"operation aliases ingestion records", func(t *testing.T, f *delegatedApplyFixture) {
			f.state.management.Ingestion = &config.IngestionConfig{Directory: t.TempDir(), RecordDirectory: f.operations}
		}, modelconfig.ErrProposalStore},
		{"cancelled request", func(_ *testing.T, f *delegatedApplyFixture) {
			ctx, cancel := context.WithCancel(f.request.Context())
			cancel()
			f.request = f.request.WithContext(ctx)
		}, context.Canceled},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newDelegatedApplyFixture(t)
			originalOperations := f.operations
			tt.change(t, &f)
			if f.state.management.DelegatedStore != nil {
				f.state.management.DelegatedStore.OperationDirectory = f.operations
			}
			if _, reserved, err := f.apply(); !errors.Is(err, tt.want) || reserved {
				t.Fatalf("apply = %v, %v; want %v", reserved, err, tt.want)
			}
			if _, err := modelconfig.LoadDelegatedOperation(originalOperations, f.input.ProposalID); !errors.Is(err, modelconfig.ErrProposalNotFound) {
				t.Fatalf("rejected action reserved an operation: %v", err)
			}
			got, err := os.ReadFile(filepath.Join(f.target, "config.yaml"))
			if err != nil || !bytes.Equal(got, f.source) {
				t.Fatalf("rejected action changed target: %v", err)
			}
		})
	}
}

func TestApplyDelegatedSettingsRecordsStaleTarget(t *testing.T) {
	f := newDelegatedApplyFixture(t)
	path := filepath.Join(f.target, "config.yaml")
	external := append(append([]byte(nil), f.source...), []byte("# external change\n")...)
	if err := os.WriteFile(path, external, 0o600); err != nil {
		t.Fatal(err)
	}
	record, reserved, err := f.apply()
	if !errors.Is(err, modelconfig.ErrProposalStale) || !reserved || record.Outcome != "not_written" {
		t.Fatalf("stale apply = %+v, %v, %v", record, reserved, err)
	}
	stored, err := modelconfig.LoadDelegatedOperation(f.operations, f.input.ProposalID)
	if err != nil || stored.Outcome != "not_written" {
		t.Fatalf("stale outcome not persisted: %+v, %v", stored, err)
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, external) {
		t.Fatalf("stale apply changed target: %v", err)
	}
}

func TestApplyDelegatedSettingsRechecksStateUnderLifecycleLock(t *testing.T) {
	f := newDelegatedApplyFixture(t)
	f.r.lifecycleMu.Lock()
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		_, _, err := f.apply()
		done <- err
	}()
	<-started
	reloaded := *f.state
	reloaded.peer = copyPeerConfig(f.state.peer)
	reloaded.peer.Peers["requester"] = config.PeerGrant{Token: strings.Repeat("p", 32), CatalogRead: true}
	f.r.state.Store(&reloaded)
	f.r.lifecycleMu.Unlock()
	if err := <-done; !errors.Is(err, errDelegationChanged) {
		t.Fatalf("stale authorization accepted: %v", err)
	}
	if _, err := modelconfig.LoadDelegatedOperation(f.operations, f.input.ProposalID); !errors.Is(err, modelconfig.ErrProposalNotFound) {
		t.Fatalf("stale action reserved an operation: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(f.target, "config.yaml"))
	if err != nil || !bytes.Equal(got, f.source) {
		t.Fatalf("stale action changed target: %v", err)
	}
}

//go:build linux

package proxy

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
)

func reserveDelegatedFixture(t *testing.T, f delegatedApplyFixture) {
	t.Helper()
	proposal, err := modelconfig.LoadDelegatedProposal(f.proposals, f.input.ProposalID)
	if err != nil {
		t.Fatal(err)
	}
	if _, reserved, err := modelconfig.ReserveDelegatedOperation(f.operations, proposal, f.input.OperationID, time.Now()); err != nil || !reserved {
		t.Fatalf("reserve = %v, %v", reserved, err)
	}
}

func ownerRequest(f delegatedApplyFixture) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "https://owner.example/management/v1/operations", nil)
	req.Header.Set("Authorization", "Bearer "+f.state.management.BackupStore.Token)
	return req
}

func TestPeerDelegatedOperationAuthorizesOriginAndSurvivesRestart(t *testing.T) {
	f := newDelegatedApplyFixture(t)
	reserveDelegatedFixture(t, f)
	lookup := func(req *http.Request, operationID string) (modelconfig.DelegatedOperation, error) {
		return f.r.peerDelegatedOperation(req, f.state, f.input.ProposalID, operationID)
	}
	record, err := lookup(f.request, f.input.OperationID)
	if err != nil || record.Outcome != "unknown" || record.RequesterNode != "requester" {
		t.Fatalf("lookup = %+v, %v", record, err)
	}
	f.r.instanceID = strings.Repeat("b", 32)
	f.state.generation++
	f.state.peer.LocalBackends = nil
	if _, err := lookup(f.request, f.input.OperationID); err != nil {
		t.Fatalf("historical record inaccessible after restart and ownership change: %v", err)
	}
	if _, err := lookup(f.request, strings.Repeat("f", 32)); !errors.Is(err, modelconfig.ErrProposalNotFound) {
		t.Fatalf("wrong operation revealed a record: %v", err)
	}
	token := strings.Repeat("q", 32)
	f.state.peer.Peers["other"] = config.PeerGrant{Token: token, ManageSettings: true}
	other := f.request.Clone(f.request.Context())
	other.Header.Set("Authorization", "Bearer "+token)
	if record, err := lookup(other, f.input.OperationID); !errors.Is(err, modelconfig.ErrProposalNotFound) || record.OperationID != "" {
		t.Fatalf("another peer read the operation: %+v, %v", record, err)
	}
	f.state.peer.NodeID = "different-owner"
	if _, err := lookup(f.request, f.input.OperationID); !errors.Is(err, modelconfig.ErrProposalNotFound) {
		t.Fatalf("foreign owner record was exposed: %v", err)
	}
	f.state.peer.NodeID = "owner"
	f.state.peer.Peers["requester"] = config.PeerGrant{Token: strings.Repeat("p", 32), CatalogRead: true}
	if _, err := lookup(f.request, f.input.OperationID); !errors.Is(err, errDelegationUnauthorized) {
		t.Fatalf("revoked peer read the operation: %v", err)
	}
}

func TestOwnerReviewRejectsKnownOutcome(t *testing.T) {
	f := newDelegatedApplyFixture(t)
	if record, reserved, err := f.apply(); err != nil || !reserved || record.Outcome != "written" {
		t.Fatalf("apply = %+v, %v, %v", record, reserved, err)
	}
	record, err := f.r.ownerDelegatedOperation(ownerRequest(f), f.state, f.input.ProposalID, f.input.OperationID, true)
	if !errors.Is(err, modelconfig.ErrDelegatedOperationConflict) || record.OperationID != "" {
		t.Fatalf("known outcome was reviewed as unknown: %+v, %v", record, err)
	}
}

func TestOwnerDelegatedOperationReviewRequiresOwnerAuthorization(t *testing.T) {
	f := newDelegatedApplyFixture(t)
	reserveDelegatedFixture(t, f)
	owner := ownerRequest(f)
	access := func(req *http.Request, review bool) (modelconfig.DelegatedOperation, error) {
		return f.r.ownerDelegatedOperation(req, f.state, f.input.ProposalID, f.input.OperationID, review)
	}
	if record, err := access(f.request, true); !errors.Is(err, errDelegationUnauthorized) || record.OperationID != "" {
		t.Fatalf("peer authorized owner review: %+v, %v", record, err)
	}
	plaintext := owner.Clone(owner.Context())
	plaintext.TLS = nil
	if _, err := access(plaintext, true); !errors.Is(err, errDelegationUnauthorized) {
		t.Fatalf("plaintext owner review accepted: %v", err)
	}
	duplicate := owner.Clone(owner.Context())
	duplicate.Header.Add("Authorization", "Bearer "+f.state.management.BackupStore.Token)
	if _, err := access(duplicate, true); !errors.Is(err, errDelegationUnauthorized) {
		t.Fatalf("ambiguous owner credential accepted: %v", err)
	}
	record, err := access(owner, false)
	if err != nil || record.Outcome != "unknown" || record.ReviewedAt != nil {
		t.Fatalf("owner inspection = %+v, %v", record, err)
	}
	// Review remains available when both federation and local writes are disabled.
	f.state.peer = nil
	f.state.management.WriteToken = ""
	f.r.instanceID = strings.Repeat("b", 32)
	reviewed, err := access(owner, true)
	if err != nil || reviewed.Outcome != "unknown" || reviewed.ReviewedAt == nil {
		t.Fatalf("owner review = %+v, %v", reviewed, err)
	}
	again, err := access(owner, true)
	if err != nil || again.ReviewedAt == nil || !again.ReviewedAt.Equal(*reviewed.ReviewedAt) {
		t.Fatalf("review is not idempotent: %+v, %v", again, err)
	}
	proposal, err := modelconfig.LoadDelegatedProposal(f.proposals, f.input.ProposalID)
	if err != nil {
		t.Fatal(err)
	}
	if record, reserved, err := modelconfig.ReserveDelegatedOperation(f.operations, proposal, f.input.OperationID, time.Now()); err != nil || reserved || record.Outcome != "unknown" {
		t.Fatalf("review authorized retry: %+v, %v, %v", record, reserved, err)
	}
	got, err := os.ReadFile(filepath.Join(f.target, "config.yaml"))
	if err != nil || !bytes.Equal(got, f.source) {
		t.Fatalf("review changed target: %v", err)
	}
}

func TestOwnerReviewRejectsForeignOperationAndLiveExecution(t *testing.T) {
	f := newDelegatedApplyFixture(t)
	reserveDelegatedFixture(t, f)
	owner := ownerRequest(f)
	access := func(operationID string, review bool) (modelconfig.DelegatedOperation, error) {
		return f.r.ownerDelegatedOperation(owner, f.state, f.input.ProposalID, operationID, review)
	}
	if _, err := access(strings.Repeat("f", 32), true); !errors.Is(err, modelconfig.ErrProposalNotFound) {
		t.Fatalf("wrong operation was reviewed: %v", err)
	}
	f.r.peerNodeID, f.state.peer.NodeID = "other", "other"
	if _, err := access(f.input.OperationID, true); !errors.Is(err, modelconfig.ErrProposalNotFound) {
		t.Fatalf("foreign owner record was reviewed: %v", err)
	}
	f.r.peerNodeID, f.state.peer.NodeID = "owner", "owner"
	dir, err := os.Open(f.operations)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	if err := syscall.Flock(int(dir.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	if _, err := access(f.input.OperationID, true); !errors.Is(err, modelconfig.ErrProposalStore) {
		t.Fatalf("review bypassed active operation lock: %v", err)
	}
	if err := syscall.Flock(int(dir.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	record, err := access(f.input.OperationID, false)
	if err != nil || record.ReviewedAt != nil {
		t.Fatalf("rejected review modified record: %+v, %v", record, err)
	}
}

func TestOperationAccessRejectsStaleStateAndStoreAliases(t *testing.T) {
	f := newDelegatedApplyFixture(t)
	reserveDelegatedFixture(t, f)
	owner := ownerRequest(f)
	f.state.management.DelegatedStore.OperationDirectory = f.proposals
	if _, err := f.r.peerDelegatedOperation(f.request, f.state, f.input.ProposalID, f.input.OperationID); !errors.Is(err, modelconfig.ErrProposalStore) {
		t.Fatalf("peer lookup accepted store alias: %v", err)
	}
	if _, err := f.r.ownerDelegatedOperation(owner, f.state, f.input.ProposalID, f.input.OperationID, true); !errors.Is(err, modelconfig.ErrProposalStore) {
		t.Fatalf("owner review accepted store alias: %v", err)
	}
	f.state.management.DelegatedStore.OperationDirectory = f.operations
	stores := f.state.management.DelegatedStore
	f.state.management.DelegatedStore = nil
	if _, err := f.r.peerDelegatedOperation(f.request, f.state, f.input.ProposalID, f.input.OperationID); !errors.Is(err, modelconfig.ErrProposalStore) {
		t.Fatalf("peer lookup accepted unconfigured stores: %v", err)
	}
	if _, err := f.r.ownerDelegatedOperation(owner, f.state, f.input.ProposalID, f.input.OperationID, true); !errors.Is(err, modelconfig.ErrProposalStore) {
		t.Fatalf("owner review accepted unconfigured stores: %v", err)
	}
	f.state.management.DelegatedStore = stores
	reloaded := *f.state
	f.r.state.Store(&reloaded)
	if _, err := f.r.peerDelegatedOperation(f.request, f.state, f.input.ProposalID, f.input.OperationID); !errors.Is(err, errDelegationChanged) {
		t.Fatalf("peer lookup accepted stale state: %v", err)
	}
	if _, err := f.r.ownerDelegatedOperation(owner, f.state, f.input.ProposalID, f.input.OperationID, true); !errors.Is(err, errDelegationChanged) {
		t.Fatalf("owner review accepted stale state: %v", err)
	}
}

//go:build linux

package modelconfig

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func recoveryQuery(owner DelegatedOwner, proposal DelegatedProposal) DelegatedProposalRecoveryQuery {
	return DelegatedProposalRecoveryQuery{
		Owner: owner, RequesterNode: proposal.RequesterNode, BackendID: proposal.BackendID,
		TargetID: proposal.Proposal.TargetID, ModelID: proposal.Proposal.ModelID,
		Revision: proposal.Proposal.Revision, Settings: proposal.Proposal.Settings,
	}
}

func TestRecoverDelegatedProposalMatchesTypedRequestAndPersistsAudit(t *testing.T) {
	store := privateTestDir(t)
	owner, proposal := delegatedFixture(t)
	if err := SaveDelegatedProposal(store, proposal); err != nil {
		t.Fatal(err)
	}
	query := recoveryQuery(owner, proposal)
	now := proposal.Proposal.CreatedAt.Add(time.Minute)
	recovered, err := RecoverDelegatedProposal(store, query, "manage_settings", now)
	if err != nil || recovered.Proposal.ID != proposal.Proposal.ID {
		t.Fatalf("recovery = %q, %v", recovered.Proposal.ID, err)
	}
	if len(recovered.RecoveryAudit) != 1 {
		t.Fatalf("recovery audit = %+v", recovered.RecoveryAudit)
	}
	event := recovered.RecoveryAudit[0]
	if !event.At.Equal(now) || event.RequesterNode != query.RequesterNode ||
		event.CredentialScope != "manage_settings" || event.OwnerNode != owner.NodeID ||
		event.TargetID != query.TargetID || event.BackendID != query.BackendID ||
		event.ModelID != query.ModelID || event.SourceRevision != query.Revision ||
		event.ReviewRevision != proposal.ReviewRevision || event.Outcome != "recovered" {
		t.Fatalf("recovery audit omitted scope or binding: %+v", event)
	}
	stored, err := LoadDelegatedProposal(store, proposal.Proposal.ID)
	if err != nil || len(stored.RecoveryAudit) != 1 || stored.RecoveryAudit[0] != event {
		t.Fatalf("persisted recovery audit = %+v, %v", stored.RecoveryAudit, err)
	}
}

func TestRecoverDelegatedProposalRequiresExactRequesterAndSettings(t *testing.T) {
	store := privateTestDir(t)
	owner, proposal := delegatedFixture(t)
	if err := SaveDelegatedProposal(store, proposal); err != nil {
		t.Fatal(err)
	}
	query := recoveryQuery(owner, proposal)
	tests := []struct {
		name   string
		change func(*DelegatedProposalRecoveryQuery)
	}{
		{"requester", func(q *DelegatedProposalRecoveryQuery) { q.RequesterNode = "another-requester" }},
		{"backend", func(q *DelegatedProposalRecoveryQuery) { q.BackendID = "another-backend" }},
		{"target", func(q *DelegatedProposalRecoveryQuery) { q.TargetID = "another-target" }},
		{"model", func(q *DelegatedProposalRecoveryQuery) { q.ModelID = "another-model" }},
		{"revision", func(q *DelegatedProposalRecoveryQuery) { q.Revision = strings.Repeat("b", 64) }},
		{"settings", func(q *DelegatedProposalRecoveryQuery) { value := int32(8192); q.Settings.ContextSize = &value }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := query
			test.change(&candidate)
			if _, err := RecoverDelegatedProposal(store, candidate, "manage_settings", proposal.Proposal.CreatedAt); !errors.Is(err, ErrProposalNotFound) {
				t.Fatalf("mismatched query error = %v", err)
			}
		})
	}
	if _, err := RecoverDelegatedProposal(store, query, "catalog_read", proposal.Proposal.CreatedAt); !errors.Is(err, ErrProposalInvalid) {
		t.Fatalf("wrong scope error = %v", err)
	}
	stored, err := LoadDelegatedProposal(store, proposal.Proposal.ID)
	if err != nil || len(stored.RecoveryAudit) != 0 {
		t.Fatalf("failed lookup recorded an unsupported identity: %+v, %v", stored.RecoveryAudit, err)
	}
}

func TestRecoverDelegatedProposalRejectsAmbiguousMatches(t *testing.T) {
	store := privateTestDir(t)
	owner, first := delegatedFixture(t)
	second := first
	second.Proposal.ID = strings.Repeat("b", 32)
	revision, err := delegatedReviewRevision(second)
	if err != nil {
		t.Fatal(err)
	}
	second.ReviewRevision = revision
	for _, proposal := range []DelegatedProposal{first, second} {
		if err := SaveDelegatedProposal(store, proposal); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := RecoverDelegatedProposal(store, recoveryQuery(owner, first), "manage_settings", first.Proposal.CreatedAt); !errors.Is(err, ErrDelegatedProposalAmbiguous) {
		t.Fatalf("ambiguous recovery error = %v", err)
	}
	for _, id := range []string{first.Proposal.ID, second.Proposal.ID} {
		stored, err := LoadDelegatedProposal(store, id)
		if err != nil || len(stored.RecoveryAudit) != 1 || stored.RecoveryAudit[0].Outcome != "ambiguous" {
			t.Fatalf("ambiguous match %s audit = %+v, %v", id, stored.RecoveryAudit, err)
		}
	}
}

func TestRecoverDelegatedProposalExpiryAndAuditBound(t *testing.T) {
	store := privateTestDir(t)
	owner, proposal := delegatedFixture(t)
	if err := SaveDelegatedProposal(store, proposal); err != nil {
		t.Fatal(err)
	}
	query := recoveryQuery(owner, proposal)
	for i := 0; i < MaxDelegatedProposalRecoveryAudit+2; i++ {
		now := proposal.ExpiresAt.Add(time.Duration(i) * time.Second)
		recovered, err := RecoverDelegatedProposal(store, query, "manage_settings", now)
		if err != nil || recovered.Proposal.ID != proposal.Proposal.ID {
			t.Fatalf("expired recovery %d = %q, %v", i, recovered.Proposal.ID, err)
		}
		if recovered.RecoveryAudit[len(recovered.RecoveryAudit)-1].Outcome != "expired" {
			t.Fatalf("expiry was not represented: %+v", recovered.RecoveryAudit)
		}
	}
	stored, err := LoadDelegatedProposal(store, proposal.Proposal.ID)
	if err != nil || len(stored.RecoveryAudit) != MaxDelegatedProposalRecoveryAudit {
		t.Fatalf("audit bound = %d, %v", len(stored.RecoveryAudit), err)
	}
	wantFirst := proposal.ExpiresAt.Add(2 * time.Second)
	if !stored.RecoveryAudit[0].At.Equal(wantFirst) {
		t.Fatalf("old audit entries were not bounded: first=%s want=%s", stored.RecoveryAudit[0].At, wantFirst)
	}
}

//go:build linux

package modelconfig

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func cleanupProposal(t *testing.T, proposal DelegatedProposal, id string) DelegatedProposal {
	t.Helper()
	proposal.Proposal.ID = id
	revision, err := delegatedReviewRevision(proposal)
	if err != nil {
		t.Fatal(err)
	}
	proposal.ReviewRevision = revision
	return proposal
}

func privateTestDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestExpiredOrphanedDelegatedProposalEligibilityAndBoundary(t *testing.T) {
	proposals, operations := privateTestDir(t), privateTestDir(t)
	_, proposal := delegatedFixture(t)
	if err := SaveDelegatedProposal(proposals, proposal); err != nil {
		t.Fatal(err)
	}
	for _, now := range []time.Time{proposal.ExpiresAt.Add(-time.Nanosecond), proposal.Proposal.CreatedAt} {
		candidates, err := ListExpiredOrphanedDelegatedProposals(proposals, operations, now)
		if err != nil || len(candidates) != 0 {
			t.Fatalf("proposal eligible at %s: %#v, %v", now, candidates, err)
		}
	}
	candidates, err := ListExpiredOrphanedDelegatedProposals(proposals, operations, proposal.ExpiresAt)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("proposal not eligible at expiry: %#v, %v", candidates, err)
	}
	candidate := candidates[0]
	if candidate.ProposalID != proposal.Proposal.ID || candidate.RequesterNode != proposal.RequesterNode ||
		candidate.BackendID != proposal.BackendID || candidate.ModelID != proposal.Proposal.ModelID ||
		!candidate.ExpiresAt.Equal(proposal.ExpiresAt) {
		t.Fatalf("cleanup candidate omitted or inferred stored identity: %+v", candidate)
	}
}

func TestCleanupExpiredOrphanedDelegatedProposalIsExplicitAndIdempotent(t *testing.T) {
	proposals, operations := privateTestDir(t), privateTestDir(t)
	_, proposal := delegatedFixture(t)
	if err := SaveDelegatedProposal(proposals, proposal); err != nil {
		t.Fatal(err)
	}
	if _, err := CleanupExpiredOrphanedDelegatedProposals(proposals, operations, []string{proposal.Proposal.ID}, proposal.ExpiresAt.Add(-time.Nanosecond)); !errors.Is(err, ErrDelegatedProposalCleanup) {
		t.Fatalf("unexpired proposal cleanup error = %v", err)
	}
	removed, err := CleanupExpiredOrphanedDelegatedProposals(proposals, operations, []string{proposal.Proposal.ID}, proposal.ExpiresAt)
	if err != nil || len(removed) != 1 || removed[0] != proposal.Proposal.ID {
		t.Fatalf("cleanup = %v, %v", removed, err)
	}
	removed, err = CleanupExpiredOrphanedDelegatedProposals(proposals, operations, []string{proposal.Proposal.ID}, proposal.ExpiresAt)
	if err != nil || len(removed) != 0 {
		t.Fatalf("repeated cleanup = %v, %v", removed, err)
	}
	if _, err := LoadDelegatedProposal(proposals, proposal.Proposal.ID); !errors.Is(err, ErrProposalNotFound) {
		t.Fatalf("cleaned proposal still present: %v", err)
	}
}

func TestRecoveryAuditSurvivesCleanupAndRemainsBoundedAfterRestart(t *testing.T) {
	proposals, operations := privateTestDir(t), privateTestDir(t)
	owner, base := delegatedFixture(t)
	const proposalCount = MaxDelegatedProposalRecoveryAuditRecords/MaxDelegatedProposalRecoveryAudit + 2
	for i := range proposalCount {
		proposal := cleanupProposal(t, base, fmt.Sprintf("%032x", i+1))
		if err := SaveDelegatedProposal(proposals, proposal); err != nil {
			t.Fatalf("save proposal %d: %v", i, err)
		}
		query := recoveryQuery(owner, proposal)
		for j := range MaxDelegatedProposalRecoveryAudit {
			if _, err := RecoverDelegatedProposal(proposals, query, DelegatedSettingsCredentialScope,
				proposal.ExpiresAt.Add(time.Duration(j)*time.Second)); err != nil {
				t.Fatalf("recover proposal %d event %d: %v", i, j, err)
			}
		}
		if removed, err := CleanupExpiredOrphanedDelegatedProposals(proposals, operations,
			[]string{proposal.Proposal.ID}, proposal.ExpiresAt.Add(MaxDelegatedProposalRecoveryAudit*time.Second)); err != nil || len(removed) != 1 {
			t.Fatalf("cleanup proposal %d: %v, %v", i, removed, err)
		}
	}

	var records []DelegatedProposalRecoveryAudit
	err := withProposalStore(proposals, func(root *os.Root, _ *os.File) error {
		var err error
		records, err = readDelegatedProposalRecoveryAudit(root, delegatedProposalAuditFilename)
		return err
	})
	if err != nil || len(records) != MaxDelegatedProposalRecoveryAuditRecords {
		t.Fatalf("bounded audit records = %d, %v", len(records), err)
	}
	wantFirst := fmt.Sprintf("%032x", proposalCount-MaxDelegatedProposalRecoveryAuditRecords/MaxDelegatedProposalRecoveryAudit+1)
	if records[0].ProposalID != wantFirst || records[len(records)-1].ProposalID != fmt.Sprintf("%032x", proposalCount) {
		t.Fatalf("audit retention did not keep newest events: first=%s last=%s", records[0].ProposalID, records[len(records)-1].ProposalID)
	}
	// Reopen the directory-backed store and verify the durable audit survives a process restart.
	var afterRestart []DelegatedProposalRecoveryAudit
	err = withProposalStore(proposals, func(root *os.Root, _ *os.File) error {
		var err error
		afterRestart, err = readDelegatedProposalRecoveryAudit(root, delegatedProposalAuditFilename)
		return err
	})
	if err != nil || len(afterRestart) != len(records) || afterRestart[0] != records[0] || afterRestart[len(afterRestart)-1] != records[len(records)-1] {
		t.Fatalf("audit after restart = %d records, %v", len(afterRestart), err)
	}
	if _, err := json.Marshal(afterRestart); err != nil {
		t.Fatalf("persisted audit is not serializable: %v", err)
	}
}

func TestCleanupFailsClosedOnCorruptRecoveryAudit(t *testing.T) {
	proposals, operations := privateTestDir(t), privateTestDir(t)
	_, proposal := delegatedFixture(t)
	if err := SaveDelegatedProposal(proposals, proposal); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(proposals, delegatedProposalAuditFilename), []byte(`{"broken":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ListDelegatedProposals(proposals); !errors.Is(err, ErrProposalStore) {
		t.Fatalf("corrupt audit was not detected by proposal inventory: %v", err)
	}
	if err := withProposalStore(proposals, func(root *os.Root, _ *os.File) error {
		_, err := readDelegatedProposalRecoveryAudit(root, delegatedProposalAuditFilename)
		return err
	}); !errors.Is(err, ErrProposalStore) {
		t.Fatalf("direct read accepted corrupt audit: %v", err)
	}
	if removed, err := CleanupExpiredOrphanedDelegatedProposals(proposals, operations,
		[]string{proposal.Proposal.ID}, proposal.ExpiresAt); !errors.Is(err, ErrProposalStore) || len(removed) != 0 {
		t.Fatalf("cleanup accepted corrupt audit store: removed=%v err=%v", removed, err)
	}
	if _, err := LoadDelegatedProposal(proposals, proposal.Proposal.ID); err != nil {
		t.Fatalf("corrupt audit caused proposal removal: %v", err)
	}
}

func TestCleanupExpiredDelegatedProposalFreesBoundedStoreSlot(t *testing.T) {
	proposals, operations := privateTestDir(t), privateTestDir(t)
	_, base := delegatedFixture(t)
	for i := range MaxProposals {
		proposal := cleanupProposal(t, base, fmt.Sprintf("%032x", i+1))
		if err := SaveDelegatedProposal(proposals, proposal); err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
	}
	newProposal := cleanupProposal(t, base, strings.Repeat("f", 32))
	if err := SaveDelegatedProposal(proposals, newProposal); !errors.Is(err, ErrProposalStoreFull) {
		t.Fatalf("full store accepted proposal: %v", err)
	}
	firstID := fmt.Sprintf("%032x", 1)
	if removed, err := CleanupExpiredOrphanedDelegatedProposals(proposals, operations, []string{firstID}, base.ExpiresAt); err != nil || len(removed) != 1 {
		t.Fatalf("cleanup did not free one slot: %v, %v", removed, err)
	}
	if err := SaveDelegatedProposal(proposals, newProposal); err != nil {
		t.Fatalf("store remained full after explicit cleanup: %v", err)
	}
}

func TestCleanupFailsClosedWhileOperationStoreIsLocked(t *testing.T) {
	proposals, operations := privateTestDir(t), privateTestDir(t)
	_, proposal := delegatedFixture(t)
	if err := SaveDelegatedProposal(proposals, proposal); err != nil {
		t.Fatal(err)
	}
	operationDir, err := os.Open(operations)
	if err != nil {
		t.Fatal(err)
	}
	defer operationDir.Close()
	if err := syscall.Flock(int(operationDir.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	defer syscall.Flock(int(operationDir.Fd()), syscall.LOCK_UN)
	if _, err := CleanupExpiredOrphanedDelegatedProposals(proposals, operations,
		[]string{proposal.Proposal.ID}, proposal.ExpiresAt); !errors.Is(err, ErrProposalStore) {
		t.Fatalf("cleanup ignored active operation-store lock: %v", err)
	}
	if _, err := LoadDelegatedProposal(proposals, proposal.Proposal.ID); err != nil {
		t.Fatalf("cleanup under active operation lock removed proposal: %v", err)
	}
}

func TestCleanupDoesNotTouchOperationLedgerOrOperationBackedProposal(t *testing.T) {
	proposals, operations := privateTestDir(t), privateTestDir(t)
	_, base := delegatedFixture(t)
	operationProposal := cleanupProposal(t, base, strings.Repeat("a", 32))
	orphanProposal := cleanupProposal(t, base, strings.Repeat("b", 32))
	for _, proposal := range []DelegatedProposal{operationProposal, orphanProposal} {
		if err := SaveDelegatedProposal(proposals, proposal); err != nil {
			t.Fatal(err)
		}
	}
	operationID := strings.Repeat("e", 32)
	if _, reserved, err := ReserveDelegatedOperation(operations, operationProposal, operationID, operationProposal.Proposal.CreatedAt); err != nil || !reserved {
		t.Fatalf("reserve operation = %v, %v", reserved, err)
	}
	before := readPrivateDirectory(t, operations)
	candidates, err := ListExpiredOrphanedDelegatedProposals(proposals, operations, base.ExpiresAt)
	if err != nil || len(candidates) != 1 || candidates[0].ProposalID != orphanProposal.Proposal.ID {
		t.Fatalf("operation-backed proposal was eligible: %#v, %v", candidates, err)
	}
	if removed, err := CleanupExpiredOrphanedDelegatedProposals(proposals, operations,
		[]string{operationProposal.Proposal.ID, orphanProposal.Proposal.ID}, base.ExpiresAt); !errors.Is(err, ErrDelegatedProposalCleanup) || len(removed) != 0 {
		t.Fatalf("mixed cleanup was not rejected atomically: %v, %v", removed, err)
	}
	if removed, err := CleanupExpiredOrphanedDelegatedProposals(proposals, operations,
		[]string{orphanProposal.Proposal.ID}, base.ExpiresAt); err != nil || len(removed) != 1 {
		t.Fatalf("orphan cleanup = %v, %v", removed, err)
	}
	after := readPrivateDirectory(t, operations)
	if len(before) != len(after) {
		t.Fatalf("cleanup changed operation ledger entries: before=%v after=%v", before, after)
	}
	for name, data := range before {
		if string(after[name]) != string(data) {
			t.Fatalf("cleanup modified operation ledger file %s", name)
		}
	}
	record, err := LoadDelegatedOperation(operations, operationProposal.Proposal.ID)
	if err != nil || record.OperationID != operationID || record.Outcome != "unknown" {
		t.Fatalf("unknown operation recovery was changed: %+v, %v", record, err)
	}
}

func readPrivateDirectory(t *testing.T, directory string) map[string][]byte {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	files := make(map[string][]byte, len(entries))
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		files[entry.Name()] = data
	}
	return files
}

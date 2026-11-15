//go:build linux

package modelconfig

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func delegatedMutationFixture(t *testing.T) (string, string, string, string, []byte, DelegatedProposal, DelegatedApplyBinding) {
	t.Helper()
	target, backup, source, local := mutationFixture(t)
	proposals, operations := t.TempDir(), t.TempDir()
	for _, directory := range []string{proposals, operations} {
		if err := os.Chmod(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	owner := DelegatedOwner{NodeID: "owner", ProxyInstanceID: strings.Repeat("a", 32), ConfigGeneration: 1}
	proposal, err := NewDelegatedProposal(owner, "requester", "backend", local, 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := SaveDelegatedProposal(proposals, proposal); err != nil {
		t.Fatal(err)
	}
	binding := DelegatedApplyBinding{
		Owner: owner, RequesterNode: "requester", BackendID: "backend",
		TargetID: local.TargetID, ReviewRevision: proposal.ReviewRevision,
	}
	return target, backup, proposals, operations, source, proposal, binding
}

func TestApplyStoredDelegatedProposalAndReplay(t *testing.T) {
	target, backup, proposals, operations, source, proposal, binding := delegatedMutationFixture(t)
	id := strings.Repeat("e", 32)
	apply := func(operationID string) (DelegatedOperation, bool, error) {
		return ApplyStoredDelegatedProposal(proposals, operations, proposal.Proposal.ID, operationID,
			target, "config.yaml", backup, 2, binding)
	}
	record, reserved, err := apply(id)
	if err != nil || !reserved || record.Outcome != "written" || record.FinishedAt == nil {
		t.Fatalf("apply = %+v, %v, %v", record, reserved, err)
	}
	path := filepath.Join(target, "config.yaml")
	written, err := os.ReadFile(path)
	if err != nil || bytes.Equal(written, source) {
		t.Fatalf("target not updated: %v", err)
	}
	replayed, reserved, err := apply(id)
	if err != nil || reserved || replayed.Outcome != "written" {
		t.Fatalf("replay = %+v, %v, %v", replayed, reserved, err)
	}
	if _, reserved, err := apply(strings.Repeat("f", 32)); !errors.Is(err, ErrDelegatedOperationConflict) || reserved {
		t.Fatalf("different operation bypassed replay protection: %v, %v", reserved, err)
	}
	backups, err := os.ReadDir(backup)
	if err != nil || len(backups) != 1 {
		t.Fatalf("replay created extra backups: %d, %v", len(backups), err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(after, written) {
		t.Fatalf("replay changed target: %v", err)
	}
}

func TestApplyStoredDelegatedProposalRejectsChangedBindingAndStaleTarget(t *testing.T) {
	target, backup, proposals, operations, source, proposal, binding := delegatedMutationFixture(t)
	id := strings.Repeat("e", 32)
	changed := binding
	changed.ReviewRevision = strings.Repeat("b", 64)
	if _, reserved, err := ApplyStoredDelegatedProposal(proposals, operations, proposal.Proposal.ID, id,
		target, "config.yaml", backup, 2, changed); !errors.Is(err, ErrDelegatedProposalBinding) || reserved {
		t.Fatalf("different review accepted: %v, %v", reserved, err)
	}
	changed = binding
	changed.RequesterNode = "other"
	if _, reserved, err := ApplyStoredDelegatedProposal(proposals, operations, proposal.Proposal.ID, id,
		target, "config.yaml", backup, 2, changed); !errors.Is(err, ErrDelegatedProposalBinding) || reserved {
		t.Fatalf("different requester accepted: %v, %v", reserved, err)
	}
	path := filepath.Join(target, "config.yaml")
	external := append(source, []byte("# externally changed\n")...)
	if err := os.WriteFile(path, external, 0o640); err != nil {
		t.Fatal(err)
	}
	record, reserved, err := ApplyStoredDelegatedProposal(proposals, operations, proposal.Proposal.ID, id,
		target, "config.yaml", backup, 2, binding)
	if !errors.Is(err, ErrProposalStale) || !reserved || record.Outcome != "not_written" {
		t.Fatalf("stale apply = %+v, %v, %v", record, reserved, err)
	}
	if err := os.WriteFile(path, source, 0o640); err != nil {
		t.Fatal(err)
	}
	replay, reserved, err := ApplyStoredDelegatedProposal(proposals, operations, proposal.Proposal.ID, id,
		target, "config.yaml", backup, 2, binding)
	if err != nil || reserved || replay.Outcome != "not_written" {
		t.Fatalf("stale retry = %+v, %v, %v", replay, reserved, err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(after, source) {
		t.Fatalf("retry changed target: %v", err)
	}
}

func TestDelegatedExecutionLocksOutReview(t *testing.T) {
	target, backup, proposals, operations, _, proposal, binding := delegatedMutationFixture(t)
	id := strings.Repeat("e", 32)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	var record DelegatedOperation
	var reserved bool
	var applyErr, reviewErr error
	go func() {
		defer wg.Done()
		<-start
		record, reserved, applyErr = ApplyStoredDelegatedProposal(proposals, operations, proposal.Proposal.ID, id,
			target, "config.yaml", backup, 2, binding)
	}()
	go func() {
		defer wg.Done()
		<-start
		_, reviewErr = ReviewUnknownDelegatedOperation(operations, proposal.Proposal.ID, id, proposal.Owner.NodeID, time.Now())
	}()
	close(start)
	wg.Wait()
	if errors.Is(applyErr, ErrProposalStore) && !reserved {
		record, reserved, applyErr = ApplyStoredDelegatedProposal(proposals, operations, proposal.Proposal.ID, id,
			target, "config.yaml", backup, 2, binding)
	}
	if applyErr != nil || !reserved || record.Outcome != "written" {
		t.Fatalf("apply = %+v, %v, %v", record, reserved, applyErr)
	}
	if reviewErr == nil {
		t.Fatal("review succeeded while execution was active or after a known outcome")
	}
	stored, err := LoadDelegatedOperation(operations, proposal.Proposal.ID)
	if err != nil || stored.Outcome != "written" || stored.ReviewedAt != nil {
		t.Fatalf("review affected execution: %+v, %v", stored, err)
	}
}

func TestDelegatedInterruptedReservationNeverExecutesAgain(t *testing.T) {
	target, backup, proposals, operations, source, proposal, binding := delegatedMutationFixture(t)
	id := strings.Repeat("e", 32)
	if _, reserved, err := ReserveDelegatedOperation(operations, proposal, id, time.Now()); err != nil || !reserved {
		t.Fatalf("reserve = %v, %v", reserved, err)
	}
	record, reserved, err := ApplyStoredDelegatedProposal(proposals, operations, proposal.Proposal.ID, id,
		target, "config.yaml", backup, 2, binding)
	if err != nil || reserved || record.Outcome != "unknown" {
		t.Fatalf("interrupted retry = %+v, %v, %v", record, reserved, err)
	}
	got, err := os.ReadFile(filepath.Join(target, "config.yaml"))
	if err != nil || !bytes.Equal(got, source) {
		t.Fatalf("interrupted retry changed target: %v", err)
	}
	if err := DeleteDelegatedProposal(proposals, proposal.Proposal.ID); err != nil {
		t.Fatal(err)
	}
	if _, reserved, err := ApplyStoredDelegatedProposal(proposals, operations, proposal.Proposal.ID, id,
		target, "config.yaml", backup, 2, binding); !errors.Is(err, ErrProposalNotFound) || reserved {
		t.Fatalf("deleted review accepted: %v, %v", reserved, err)
	}
}

func TestDelegatedLiveOperationCannotBeReviewedOrPruned(t *testing.T) {
	_, _, _, operations, _, proposal, _ := delegatedMutationFixture(t)
	id := strings.Repeat("e", 32)
	if _, reserved, err := ReserveDelegatedOperation(operations, proposal, id, time.Now()); err != nil || !reserved {
		t.Fatalf("reserve = %v, %v", reserved, err)
	}
	err := withProposalStore(operations, func(_ *os.Root, _ *os.File) error {
		if _, err := ReviewUnknownDelegatedOperation(operations, proposal.Proposal.ID, id, proposal.Owner.NodeID, time.Now()); !errors.Is(err, ErrProposalStore) {
			t.Fatalf("review bypassed active operation lock: %v", err)
		}
		if _, err := PruneDelegatedOperations(operations, proposal.ExpiresAt.Add(DelegatedOperationRetention)); !errors.Is(err, ErrProposalStore) {
			t.Fatalf("pruning bypassed active operation lock: %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	record, err := LoadDelegatedOperation(operations, proposal.Proposal.ID)
	if err != nil || record.Outcome != "unknown" || record.ReviewedAt != nil {
		t.Fatalf("active reservation changed: %+v, %v", record, err)
	}
}

func TestDelegatedCheckRunsAgainBeforeReplacement(t *testing.T) {
	target, backup, source, proposal := mutationFixture(t)
	calls := 0
	result, err := applyReviewedProposalChecked(target, "config.yaml", proposal.TargetID, backup, 2, proposal, func() error {
		calls++
		if calls == 2 {
			return ErrDelegatedProposalExpired
		}
		return nil
	})
	if !errors.Is(err, ErrDelegatedProposalExpired) || result.Written || calls != 2 {
		t.Fatalf("expiry recheck = %+v, %v, calls=%d", result, err, calls)
	}
	got, err := os.ReadFile(filepath.Join(target, "config.yaml"))
	if err != nil || !bytes.Equal(got, source) {
		t.Fatalf("expired proposal changed target: %v", err)
	}
	backups, err := os.ReadDir(backup)
	if err != nil || len(backups) != 0 {
		t.Fatalf("expired proposal left a backup: %v", err)
	}
}

//go:build linux

package modelconfig

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// tempDelegatedDir returns a private temporary directory for delegated
// operation tests, preserving the 0o700 directory permission requirement.
func tempDelegatedDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}

// mustReserveDelegatedOperation reserves a delegated operation and fails the
// test if the reservation fails or does not reserve. It returns the reserved
// record.
func mustReserveDelegatedOperation(t *testing.T, dir string, proposal DelegatedProposal, operationID string, now time.Time) DelegatedOperation {
	t.Helper()
	record, reserved, err := ReserveDelegatedOperation(dir, proposal, operationID, now)
	if err != nil || !reserved {
		t.Fatalf("reserve = %v, %v", reserved, err)
	}
	return record
}

func TestDelegatedOperationReservationAndReplay(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	_, proposal := delegatedFixture(t)
	id := strings.Repeat("e", 32)
	now := proposal.Proposal.CreatedAt.Add(time.Second)
	record, reserved, err := ReserveDelegatedOperation(dir, proposal, id, now)
	if err != nil || !reserved || record.Outcome != "unknown" || record.CallerScope != "unknown" {
		t.Fatalf("reservation = %#v, %v, %v", record, reserved, err)
	}
	path := filepath.Join(dir, delegatedOperationFilename(proposal.Proposal.ID))
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("operation permissions = %v, %v", info, err)
	}
	data, err := os.ReadFile(path)
	if err != nil || strings.Contains(string(data), "config.yaml") || strings.Contains(string(data), "directory") {
		t.Fatalf("operation exposed target path or could not be read: %v", err)
	}
	afterRestart, err := LoadDelegatedOperation(dir, proposal.Proposal.ID)
	if err != nil || afterRestart.OperationID != id || afterRestart.Outcome != "unknown" {
		t.Fatalf("interrupted outcome = %#v, %v", afterRestart, err)
	}
	replayed, reserved, err := ReserveDelegatedOperation(dir, proposal, id, proposal.ExpiresAt.Add(time.Hour))
	if err != nil || reserved || replayed.OperationID != id || replayed.Outcome != "unknown" {
		t.Fatalf("replay = %#v, %v, %v", replayed, reserved, err)
	}
	if _, _, err := ReserveDelegatedOperation(dir, proposal, strings.Repeat("f", 32), now); !errors.Is(err, ErrDelegatedOperationConflict) {
		t.Fatalf("different operation ID bypassed proposal protection: %v", err)
	}
	other := proposal
	other.Proposal.ID = strings.Repeat("a", 32)
	other.ReviewRevision, err = delegatedReviewRevision(other)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReserveDelegatedOperation(dir, other, id, now); !errors.Is(err, ErrDelegatedOperationConflict) {
		t.Fatalf("operation ID reused for another proposal: %v", err)
	}
	changed := proposal
	changed.RequesterNode = "other"
	changed.ReviewRevision, err = delegatedReviewRevision(changed)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReserveDelegatedOperation(dir, changed, id, now); !errors.Is(err, ErrDelegatedOperationConflict) {
		t.Fatalf("changed proposal binding accepted: %v", err)
	}
}

func TestDelegatedOperationRejectsExpiredAndCorruptRecords(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	_, proposal := delegatedFixture(t)
	id := strings.Repeat("e", 32)
	if _, _, err := ReserveDelegatedOperation(dir, proposal, id, proposal.ExpiresAt); !errors.Is(err, ErrDelegatedProposalExpired) {
		t.Fatalf("expired proposal reserved: %v", err)
	}
	if _, _, err := ReserveDelegatedOperation(dir, proposal, id, proposal.Proposal.CreatedAt.Add(-time.Nanosecond)); !errors.Is(err, ErrDelegatedProposalExpired) {
		t.Fatalf("not-yet-valid proposal reserved: %v", err)
	}
	if _, _, err := ReserveDelegatedOperation(dir, proposal, "../invalid", proposal.Proposal.CreatedAt); !errors.Is(err, ErrProposalInvalid) {
		t.Fatalf("invalid operation identity accepted: %v", err)
	}
	if _, err := LoadDelegatedOperation(dir, proposal.Proposal.ID); !errors.Is(err, ErrProposalNotFound) {
		t.Fatalf("expired proposal left a record: %v", err)
	}
	if _, reserved, err := ReserveDelegatedOperation(dir, proposal, id, proposal.Proposal.CreatedAt); err != nil || !reserved {
		t.Fatalf("valid reservation failed: %v", err)
	}
	path := filepath.Join(dir, delegatedOperationFilename(proposal.Proposal.ID))
	if err := os.WriteFile(path, []byte(`{"outcome":"written"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReserveDelegatedOperation(dir, proposal, id, proposal.Proposal.CreatedAt); !errors.Is(err, ErrProposalStore) {
		t.Fatalf("corrupt record allowed retry: %v", err)
	}
}

func TestDelegatedOperationConcurrentReservation(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	_, proposal := delegatedFixture(t)
	id := strings.Repeat("e", 32)
	const workers = 16
	type result struct {
		reserved bool
		err      error
	}
	results := make(chan result, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, reserved, err := ReserveDelegatedOperation(dir, proposal, id, proposal.Proposal.CreatedAt)
			results <- result{reserved, err}
		}()
	}
	wg.Wait()
	close(results)
	successes := 0
	for result := range results {
		if result.reserved {
			successes++
		}
		if result.err != nil && !errors.Is(result.err, ErrProposalStore) {
			t.Fatalf("unexpected concurrent error: %v", result.err)
		}
	}
	if successes != 1 {
		t.Fatalf("operation reserved %d times", successes)
	}
	if _, reserved, err := ReserveDelegatedOperation(dir, proposal, id, proposal.Proposal.CreatedAt); err != nil || reserved {
		t.Fatalf("retry after contention = %v, %v", reserved, err)
	}
}

func TestDelegatedOperationTerminalOutcomeAndRetention(t *testing.T) {
	t.Run("terminal outcome and idempotency", func(t *testing.T) {
		dir := tempDelegatedDir(t)
		owner, proposal := delegatedFixture(t)
		id, operationID := proposal.Proposal.ID, strings.Repeat("e", 32)
		started := proposal.Proposal.CreatedAt
		mustReserveDelegatedOperation(t, dir, proposal, operationID, started)
		written := MutationResult{Written: true, Durable: true}
		if _, err := FinishDelegatedOperation(dir, id, operationID, strings.Repeat("b", 32), written, nil, started); !errors.Is(err, ErrDelegatedOperationConflict) {
			t.Fatalf("restarted owner completed old operation: %v", err)
		}
		finishedAt := started.Add(time.Second)
		finished, err := FinishDelegatedOperation(dir, id, operationID, owner.ProxyInstanceID, written, nil, finishedAt)
		if err != nil || finished.Outcome != "written" || finished.FinishedAt == nil || !finished.FinishedAt.Equal(finishedAt) {
			t.Fatalf("finish = %#v, %v", finished, err)
		}
		replayed, reserved, err := ReserveDelegatedOperation(dir, proposal, operationID, proposal.ExpiresAt)
		if err != nil || reserved || replayed.Outcome != "written" {
			t.Fatalf("replay did not preserve outcome: %#v, %v, %v", replayed, reserved, err)
		}
		if _, err := FinishDelegatedOperation(dir, id, operationID, owner.ProxyInstanceID, written, nil, finishedAt.Add(time.Second)); err != nil {
			t.Fatalf("identical finish was not idempotent: %v", err)
		}
		if _, err := FinishDelegatedOperation(dir, id, operationID, owner.ProxyInstanceID, MutationResult{}, ErrTargetWrite, finishedAt); !errors.Is(err, ErrDelegatedOperationConflict) {
			t.Fatalf("terminal outcome changed: %v", err)
		}
	})

	t.Run("retention boundary", func(t *testing.T) {
		dir := tempDelegatedDir(t)
		owner, proposal := delegatedFixture(t)
		id, operationID := proposal.Proposal.ID, strings.Repeat("e", 32)
		started := proposal.Proposal.CreatedAt
		mustReserveDelegatedOperation(t, dir, proposal, operationID, started)
		finishedAt := started.Add(time.Second)
		if _, err := FinishDelegatedOperation(dir, id, operationID, owner.ProxyInstanceID, MutationResult{Written: true, Durable: true}, nil, finishedAt); err != nil {
			t.Fatalf("finish = %v", err)
		}
		cutoff := proposal.ExpiresAt.Add(DelegatedOperationRetention)
		if removed, err := PruneDelegatedOperations(dir, cutoff.Add(-time.Nanosecond)); err != nil || removed != 0 {
			t.Fatalf("record pruned prematurely: %d, %v", removed, err)
		}
		if removed, err := PruneDelegatedOperations(dir, cutoff); err != nil || removed != 1 {
			t.Fatalf("record not pruned at retention boundary: %d, %v", removed, err)
		}
		if _, err := LoadDelegatedOperation(dir, id); !errors.Is(err, ErrProposalNotFound) {
			t.Fatalf("terminal record retained after pruning: %v", err)
		}
	})

	t.Run("clock rollback", func(t *testing.T) {
		dir := tempDelegatedDir(t)
		owner, proposal := delegatedFixture(t)
		id, operationID := proposal.Proposal.ID, strings.Repeat("e", 32)
		started := proposal.Proposal.CreatedAt
		mustReserveDelegatedOperation(t, dir, proposal, operationID, started)
		finishedAt := started.Add(time.Second)
		if _, err := FinishDelegatedOperation(dir, id, operationID, owner.ProxyInstanceID, MutationResult{Written: true, Durable: true}, nil, finishedAt); err != nil {
			t.Fatalf("finish = %v", err)
		}
		cutoff := proposal.ExpiresAt.Add(DelegatedOperationRetention)
		if removed, err := PruneDelegatedOperations(dir, cutoff); err != nil || removed != 1 {
			t.Fatalf("record not pruned at retention boundary: %d, %v", removed, err)
		}
		if _, err := LoadDelegatedOperation(dir, id); !errors.Is(err, ErrProposalNotFound) {
			t.Fatalf("terminal record retained after pruning: %v", err)
		}
		if _, reserved, err := ReserveDelegatedOperation(dir, proposal, operationID, started.Add(time.Second)); !errors.Is(err, ErrDelegatedClockRollback) || reserved {
			t.Fatalf("clock rollback reopened expired proposal: %v, %v", reserved, err)
		}
		if removed, err := PruneDelegatedOperations(dir, cutoff.Add(-time.Nanosecond)); !errors.Is(err, ErrDelegatedClockRollback) || removed != 0 {
			t.Fatalf("prune accepted clock rollback: %d, %v", removed, err)
		}
	})
}

func TestDelegatedMutationOutcome(t *testing.T) {
	tests := []struct {
		name    string
		result  MutationResult
		err     error
		outcome string
		invalid bool
	}{
		{"durable write", MutationResult{Written: true, Durable: true}, nil, "written", false},
		{"write with later failure", MutationResult{Written: true, Durable: true}, ErrTargetCleanup, "written", false},
		{"failure without write", MutationResult{}, ErrTargetWrite, "not_written", false},
		{"uncertain rename", MutationResult{}, ErrTargetRenameUncertain, "unknown", false},
		{"uncertain durability", MutationResult{Written: true}, ErrTargetCommitUncertain, "unknown", false},
		{"unconfirmed write", MutationResult{Written: true}, nil, "unknown", false},
		{"unknown result", MutationResult{OutcomeUnknown: true}, ErrTargetWrite, "unknown", false},
		{"inconsistent durability", MutationResult{Durable: true}, nil, "", true},
		{"missing outcome", MutationResult{}, nil, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := delegatedMutationOutcome(tt.result, tt.err)
			if got != tt.outcome || tt.invalid != errors.Is(err, ErrProposalInvalid) {
				t.Fatalf("outcome = %q, %v", got, err)
			}
		})
	}
}

func TestDelegatedUnknownRequiresReviewBeforePruning(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	owner, proposal := delegatedFixture(t)
	id, operationID := proposal.Proposal.ID, strings.Repeat("e", 32)
	if _, reserved, err := ReserveDelegatedOperation(dir, proposal, operationID, proposal.Proposal.CreatedAt); err != nil || !reserved {
		t.Fatalf("reserve = %v, %v", reserved, err)
	}
	cutoff := proposal.ExpiresAt.Add(DelegatedOperationRetention)
	if removed, err := PruneDelegatedOperations(dir, cutoff); err != nil || removed != 0 {
		t.Fatalf("unreviewed unknown removed: %d, %v", removed, err)
	}
	review, err := ReviewUnknownDelegatedOperation(dir, id, operationID, proposal.Owner.NodeID, cutoff)
	if err != nil || review.Outcome != "unknown" || review.ReviewedAt == nil {
		t.Fatalf("review = %#v, %v", review, err)
	}
	if _, reserved, err := ReserveDelegatedOperation(dir, proposal, operationID, cutoff); err != nil || reserved {
		t.Fatalf("review authorized retry: %v, %v", reserved, err)
	}
	if _, err := FinishDelegatedOperation(dir, id, operationID, owner.ProxyInstanceID, MutationResult{Written: true, Durable: true}, nil, cutoff); !errors.Is(err, ErrDelegatedOperationConflict) {
		t.Fatalf("reviewed unknown changed to written: %v", err)
	}
	if removed, err := PruneDelegatedOperations(dir, cutoff.Add(-time.Nanosecond)); err != nil || removed != 0 {
		t.Fatalf("future review was treated as complete: %d, %v", removed, err)
	}
	if removed, err := PruneDelegatedOperations(dir, cutoff); err != nil || removed != 1 {
		t.Fatalf("reviewed unknown not pruned: %d, %v", removed, err)
	}
	if _, reserved, err := ReserveDelegatedOperation(dir, proposal, operationID, proposal.Proposal.CreatedAt); !errors.Is(err, ErrDelegatedClockRollback) || reserved {
		t.Fatalf("review and pruning allowed retry: %v, %v", reserved, err)
	}
}

func TestDelegatedUncertainMutationStaysUnknown(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	owner, proposal := delegatedFixture(t)
	id, operationID := proposal.Proposal.ID, strings.Repeat("e", 32)
	now := proposal.Proposal.CreatedAt
	if _, reserved, err := ReserveDelegatedOperation(dir, proposal, operationID, now); err != nil || !reserved {
		t.Fatalf("reserve = %v, %v", reserved, err)
	}
	result := MutationResult{Written: true}
	record, err := FinishDelegatedOperation(dir, id, operationID, owner.ProxyInstanceID, result, ErrTargetCommitUncertain, now)
	if err != nil || record.Outcome != "unknown" || record.FinishedAt == nil {
		t.Fatalf("uncertain mutation misreported: %#v, %v", record, err)
	}
	cutoff := proposal.ExpiresAt.Add(DelegatedOperationRetention)
	if removed, err := PruneDelegatedOperations(dir, cutoff); err != nil || removed != 0 {
		t.Fatalf("unreviewed uncertain outcome removed: %d, %v", removed, err)
	}
	if _, err := ReviewUnknownDelegatedOperation(dir, id, operationID, owner.NodeID, cutoff); err != nil {
		t.Fatal(err)
	}
	if removed, err := PruneDelegatedOperations(dir, cutoff); err != nil || removed != 1 {
		t.Fatalf("reviewed uncertain outcome not removed: %d, %v", removed, err)
	}
}

func TestDelegatedOperationConcurrentFinishes(t *testing.T) {
	t.Run("concurrent finishes record exactly one terminal outcome", func(t *testing.T) {
		dir := tempDelegatedDir(t)
		owner, proposal := delegatedFixture(t)
		id, operationID := proposal.Proposal.ID, strings.Repeat("e", 32)
		now := proposal.Proposal.CreatedAt
		mustReserveDelegatedOperation(t, dir, proposal, operationID, now)
		type result struct {
			record DelegatedOperation
			err    error
		}
		results := make(chan result, 2)
		var wg sync.WaitGroup
		for _, written := range []bool{false, true} {
			wg.Add(1)
			go func(written bool) {
				defer wg.Done()
				mutation, mutationErr := MutationResult{}, ErrTargetWrite
				if written {
					mutation, mutationErr = MutationResult{Written: true, Durable: true}, nil
				}
				record, err := FinishDelegatedOperation(dir, id, operationID, owner.ProxyInstanceID, mutation, mutationErr, now)
				results <- result{record, err}
			}(written)
		}
		wg.Wait()
		close(results)
		successes := 0
		for result := range results {
			if result.err == nil {
				successes++
			} else if !errors.Is(result.err, ErrProposalStore) && !errors.Is(result.err, ErrDelegatedOperationConflict) {
				t.Fatalf("unexpected finish error: %v", result.err)
			}
		}
		if successes != 1 {
			t.Fatalf("terminal outcome recorded %d times", successes)
		}
		record, err := LoadDelegatedOperation(dir, id)
		if err != nil || record.FinishedAt == nil || record.Outcome == "unknown" {
			t.Fatalf("terminal outcome missing: %#v, %v", record, err)
		}
	})

	t.Run("conflicting retry does not replace terminal outcome", func(t *testing.T) {
		dir := tempDelegatedDir(t)
		owner, proposal := delegatedFixture(t)
		id, operationID := proposal.Proposal.ID, strings.Repeat("e", 32)
		now := proposal.Proposal.CreatedAt
		mustReserveDelegatedOperation(t, dir, proposal, operationID, now)
		if _, err := FinishDelegatedOperation(dir, id, operationID, owner.ProxyInstanceID, MutationResult{Written: true, Durable: true}, nil, now); err != nil {
			t.Fatalf("finish = %v", err)
		}
		record, err := LoadDelegatedOperation(dir, id)
		if err != nil || record.FinishedAt == nil || record.Outcome == "unknown" {
			t.Fatalf("terminal outcome missing: %#v, %v", record, err)
		}
		other, otherErr := MutationResult{}, ErrTargetWrite
		if record.Outcome == "not_written" {
			other, otherErr = MutationResult{Written: true, Durable: true}, nil
		}
		if _, err := FinishDelegatedOperation(dir, id, operationID, owner.ProxyInstanceID, other, otherErr, now); !errors.Is(err, ErrDelegatedOperationConflict) {
			t.Fatalf("conflicting retry replaced outcome: %v", err)
		}
	})
}

func TestDelegatedReservationPrunesEligibleOutcomes(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	owner, first := delegatedFixture(t)
	firstOp := strings.Repeat("e", 32)
	if _, reserved, err := ReserveDelegatedOperation(dir, first, firstOp, first.Proposal.CreatedAt); err != nil || !reserved {
		t.Fatalf("reserve = %v, %v", reserved, err)
	}
	if _, err := FinishDelegatedOperation(dir, first.Proposal.ID, firstOp, owner.ProxyInstanceID, MutationResult{}, ErrTargetWrite, first.Proposal.CreatedAt); err != nil {
		t.Fatal(err)
	}
	now := first.ExpiresAt.Add(DelegatedOperationRetention)
	next := first
	next.Proposal.ID = strings.Repeat("f", 32)
	next.Proposal.CreatedAt = now
	next.ExpiresAt = now.Add(time.Minute)
	var err error
	next.ReviewRevision, err = delegatedReviewRevision(next)
	if err != nil {
		t.Fatal(err)
	}
	if _, reserved, err := ReserveDelegatedOperation(dir, next, strings.Repeat("d", 32), now); err != nil || !reserved {
		t.Fatalf("new reservation after retention = %v, %v", reserved, err)
	}
	if _, err := LoadDelegatedOperation(dir, first.Proposal.ID); !errors.Is(err, ErrProposalNotFound) {
		t.Fatalf("eligible record not pruned: %v", err)
	}
	if _, reserved, err := ReserveDelegatedOperation(dir, first, firstOp, first.Proposal.CreatedAt); !errors.Is(err, ErrDelegatedClockRollback) || reserved {
		t.Fatalf("clock rollback reopened auto-pruned operation: %v, %v", reserved, err)
	}
}

func TestDelegatedOperationRecoversInterruptedReplacement(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	_, proposal := delegatedFixture(t)
	id, operationID := proposal.Proposal.ID, strings.Repeat("e", 32)
	if _, reserved, err := ReserveDelegatedOperation(dir, proposal, operationID, proposal.Proposal.CreatedAt); err != nil || !reserved {
		t.Fatalf("reserve = %v, %v", reserved, err)
	}
	temp := filepath.Join(dir, ".operation-"+strings.Repeat("f", 32)+".tmp")
	if err := os.WriteFile(temp, []byte(`{"outcome":`), 0o600); err != nil {
		t.Fatal(err)
	}
	record, err := LoadDelegatedOperation(dir, id)
	if err != nil || record.Outcome != "unknown" {
		t.Fatalf("interrupted replacement lost unknown outcome: %#v, %v", record, err)
	}
	if _, err := os.Lstat(temp); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("interrupted temporary record retained: %v", err)
	}
	if err := os.Remove(filepath.Join(dir, operationClockFilename)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReserveDelegatedOperation(dir, proposal, operationID, proposal.Proposal.CreatedAt); !errors.Is(err, ErrProposalStore) {
		t.Fatalf("missing clock accepted alongside records: %v", err)
	}
}

func TestDelegatedOperationStoreFullFailsClosed(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	_, proposal := delegatedFixture(t)
	opID := strings.Repeat("e", 32)
	record, reserved, err := ReserveDelegatedOperation(dir, proposal, opID, proposal.Proposal.CreatedAt)
	if err != nil || !reserved {
		t.Fatalf("first reservation failed: %v", err)
	}
	for i := 0; i < MaxProposals-1; i++ {
		record.ProposalID = fmt.Sprintf("%032x", i)
		record.OperationID = fmt.Sprintf("%032x", i+MaxProposals)
		if record.ProposalID == proposal.Proposal.ID || record.OperationID == opID {
			t.Fatal("test identity collision")
		}
		data, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, delegatedOperationFilename(record.ProposalID)), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	other := proposal
	other.Proposal.ID = strings.Repeat("f", 32)
	other.ReviewRevision, err = delegatedReviewRevision(other)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReserveDelegatedOperation(dir, other, strings.Repeat("d", 32), proposal.Proposal.CreatedAt); !errors.Is(err, ErrProposalStoreFull) {
		t.Fatalf("full store accepted a new operation: %v", err)
	}
	if _, reserved, err := ReserveDelegatedOperation(dir, proposal, opID, proposal.Proposal.CreatedAt); err != nil || reserved {
		t.Fatalf("full store did not preserve replay record: %v, %v", reserved, err)
	}
}

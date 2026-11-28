//go:build linux

package modelconfig

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"time"
)

const delegatedOperationPrefix = "operation-"

func delegatedOperationFilename(proposalID string) string {
	return delegatedOperationPrefix + proposalID + ".json"
}

// ReserveDelegatedOperation durably reserves a single operation per proposal.
// The caller must pass an owner-controlled proposal, enforce authorization,
// and recheck the proposal under the appropriate locks before any side effect.
// A false reservation is a replay and must never execute again.
func ReserveDelegatedOperation(directory string, proposal DelegatedProposal, operationID string, now time.Time) (DelegatedOperation, bool, error) {
	if !validDelegatedProposal(proposal) || !validProposalID(operationID) || now.IsZero() {
		return DelegatedOperation{}, false, ErrProposalInvalid
	}
	var result DelegatedOperation
	var reserved bool
	err := withProposalStore(directory, func(root *os.Root, dir *os.File) error {
		var err error
		result, reserved, err = reserveDelegatedOperation(root, dir, proposal, operationID, now)
		return err
	})
	if err != nil {
		return DelegatedOperation{}, false, err
	}
	return result, reserved, nil
}

func reserveDelegatedOperation(root *os.Root, dir *os.File, proposal DelegatedProposal, operationID string, now time.Time) (DelegatedOperation, bool, error) {
	ids, clock, err := operationInventory(root, dir)
	if err != nil {
		return DelegatedOperation{}, false, err
	}
	records := make([]DelegatedOperation, 0, len(ids))
	for _, id := range ids {
		existing, err := readDelegatedOperation(root, id)
		if err != nil {
			return DelegatedOperation{}, false, err
		}
		records = append(records, existing)
		if existing.ProposalID == proposal.Proposal.ID {
			if existing.OperationID != operationID || !operationMatchesProposal(existing, proposal) {
				return DelegatedOperation{}, false, ErrDelegatedOperationConflict
			}
			return existing, false, nil
		}
		if existing.OperationID == operationID {
			return DelegatedOperation{}, false, ErrDelegatedOperationConflict
		}
	}
	if err := ValidateDelegatedProposal(proposal, proposal.Owner, proposal.RequesterNode, proposal.BackendID, proposal.Proposal.TargetID, now); err != nil {
		return DelegatedOperation{}, false, err
	}
	if !clock.IsZero() && now.Before(clock) {
		return DelegatedOperation{}, false, ErrDelegatedClockRollback
	}
	if clock.IsZero() {
		if err := writeOperationClock(root, dir, now); err != nil {
			return DelegatedOperation{}, false, err
		}
	}
	removed, err := pruneDelegatedOperations(root, dir, records, now)
	if err != nil {
		return DelegatedOperation{}, false, err
	}
	if len(ids)-removed >= MaxProposals {
		return DelegatedOperation{}, false, ErrProposalStoreFull
	}
	result := newDelegatedOperation(proposal, operationID, now)
	data, err := json.Marshal(result)
	if err != nil || len(data) > maxProposalBytes || !validDelegatedOperation(result) {
		return DelegatedOperation{}, false, ErrProposalStore
	}
	if err := writePrivateRecord(root, dir, delegatedOperationFilename(result.ProposalID), data, ErrProposalStore); err != nil {
		return DelegatedOperation{}, false, err
	}
	return result, true, nil
}

// LoadDelegatedOperation is not permission to inspect a record. An unfinished
// operation remains unknown after interruption.
func LoadDelegatedOperation(directory, proposalID string) (DelegatedOperation, error) {
	if !validProposalID(proposalID) {
		return DelegatedOperation{}, ErrProposalNotFound
	}
	var result DelegatedOperation
	err := withProposalStore(directory, func(root *os.Root, dir *os.File) error {
		if _, _, err := operationInventory(root, dir); err != nil {
			return err
		}
		var err error
		result, err = readDelegatedOperation(root, proposalID)
		return err
	})
	if err != nil {
		return DelegatedOperation{}, err
	}
	return result, nil
}

func readDelegatedOperation(root *os.Root, proposalID string) (DelegatedOperation, error) {
	data, err := readPrivateProposalFile(root, delegatedOperationFilename(proposalID))
	if err != nil {
		return DelegatedOperation{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var record DelegatedOperation
	if err := decoder.Decode(&record); err != nil || record.ProposalID != proposalID || !validDelegatedOperation(record) {
		return DelegatedOperation{}, ErrProposalStore
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return DelegatedOperation{}, ErrProposalStore
	}
	return record, nil
}

// FinishDelegatedOperation records the local mutation result from the owner
// instance that reserved it. It does not verify effective runtime settings.
func FinishDelegatedOperation(directory, proposalID, operationID, ownerInstance string, mutation MutationResult, mutationErr error, now time.Time) (DelegatedOperation, error) {
	if !validProposalID(proposalID) || !validProposalID(operationID) || now.IsZero() {
		return DelegatedOperation{}, ErrProposalInvalid
	}
	outcome, err := delegatedMutationOutcome(mutation, mutationErr)
	if err != nil {
		return DelegatedOperation{}, err
	}
	return updateDelegatedOperation(directory, proposalID, now, finishDelegatedOperation(operationID, ownerInstance, outcome, now))
}

func finishDelegatedOperation(operationID, ownerInstance, outcome string, now time.Time) func(*DelegatedOperation) error {
	return func(record *DelegatedOperation) error {
		if record.OperationID != operationID || record.Owner.ProxyInstanceID != ownerInstance {
			return ErrDelegatedOperationConflict
		}
		if record.FinishedAt != nil {
			if record.Outcome == outcome {
				return nil
			}
			return ErrDelegatedOperationConflict
		}
		if record.ReviewedAt != nil || now.Before(record.AcceptedAt) {
			return ErrDelegatedOperationConflict
		}
		finished := now.UTC()
		record.FinishedAt, record.Outcome = &finished, outcome
		return nil
	}
}

func delegatedMutationOutcome(result MutationResult, err error) (string, error) {
	if result.OutcomeUnknown || errors.Is(err, ErrTargetRenameUncertain) ||
		errors.Is(err, ErrTargetCommitUncertain) || result.Written && !result.Durable {
		return "unknown", nil
	}
	if result.Durable && !result.Written {
		return "", ErrProposalInvalid
	}
	if result.Written {
		return "written", nil
	}
	if err != nil {
		return "not_written", nil
	}
	return "", ErrProposalInvalid
}

// ReviewUnknownDelegatedOperation records that an operator reviewed an unknown
// outcome; it neither resolves the outcome nor authorizes a retry. The caller
// must authorize the review and exclude any still-running execution.
func ReviewUnknownDelegatedOperation(directory, proposalID, operationID, ownerNode string, now time.Time) (DelegatedOperation, error) {
	if !validProposalID(proposalID) || !validProposalID(operationID) || now.IsZero() {
		return DelegatedOperation{}, ErrProposalInvalid
	}
	return updateDelegatedOperation(directory, proposalID, now, func(record *DelegatedOperation) error {
		if record.Owner.NodeID != ownerNode || record.OperationID != operationID {
			return ErrProposalNotFound
		}
		if record.Outcome != "unknown" {
			return ErrDelegatedOperationConflict
		}
		if record.ReviewedAt != nil {
			return nil
		}
		if now.Before(record.AcceptedAt) || record.FinishedAt != nil && now.Before(*record.FinishedAt) {
			return ErrDelegatedOperationConflict
		}
		reviewed := now.UTC()
		record.ReviewedAt = &reviewed
		return nil
	})
}

func updateDelegatedOperation(directory, proposalID string, now time.Time, update func(*DelegatedOperation) error) (DelegatedOperation, error) {
	var result DelegatedOperation
	err := withProposalStore(directory, func(root *os.Root, dir *os.File) error {
		_, clock, err := operationInventory(root, dir)
		if err != nil {
			return err
		}
		if !clock.IsZero() && now.Before(clock) {
			return ErrDelegatedClockRollback
		}
		result, err = updateDelegatedOperationLocked(root, dir, proposalID, update)
		return err
	})
	if err != nil {
		return DelegatedOperation{}, err
	}
	return result, nil
}

func updateDelegatedOperationLocked(root *os.Root, dir *os.File, proposalID string, update func(*DelegatedOperation) error) (DelegatedOperation, error) {
	result, err := readDelegatedOperation(root, proposalID)
	if err != nil {
		return DelegatedOperation{}, err
	}
	previous := result
	if err := update(&result); err != nil {
		return DelegatedOperation{}, err
	}
	if result == previous {
		return result, nil
	}
	data, err := json.Marshal(result)
	if err != nil || !validDelegatedOperation(result) {
		return DelegatedOperation{}, ErrProposalStore
	}
	if err := replaceOperationRecord(root, dir, delegatedOperationFilename(proposalID), data); err != nil {
		return DelegatedOperation{}, err
	}
	return result, nil
}

// PruneDelegatedOperations removes eligible terminal records only after seven
// days from proposal expiry. Unknown outcomes require explicit review.
func PruneDelegatedOperations(directory string, now time.Time) (int, error) {
	if now.IsZero() {
		return 0, ErrProposalInvalid
	}
	var removed int
	err := withProposalStore(directory, func(root *os.Root, dir *os.File) error {
		ids, clock, err := operationInventory(root, dir)
		if err != nil {
			return err
		}
		if !clock.IsZero() && now.Before(clock) {
			return ErrDelegatedClockRollback
		}
		records := make([]DelegatedOperation, 0, len(ids))
		for _, id := range ids {
			record, err := readDelegatedOperation(root, id)
			if err != nil {
				return err
			}
			records = append(records, record)
		}
		removed, err = pruneDelegatedOperations(root, dir, records, now)
		return err
	})
	return removed, err
}

func pruneDelegatedOperations(root *os.Root, dir *os.File, records []DelegatedOperation, now time.Time) (int, error) {
	var expired []string
	for _, record := range records {
		if (record.Outcome != "unknown" || record.ReviewedAt != nil) &&
			(record.FinishedAt == nil || !now.Before(*record.FinishedAt)) &&
			(record.ReviewedAt == nil || !now.Before(*record.ReviewedAt)) &&
			!now.Before(record.ProposalExpiresAt.Add(DelegatedOperationRetention)) {
			expired = append(expired, record.ProposalID)
		}
	}
	if len(expired) == 0 {
		return 0, nil
	}
	// Advance the durable lower time bound before removing replay protection.
	if err := writeOperationClock(root, dir, now); err != nil {
		return 0, err
	}
	for _, id := range expired {
		if err := root.Remove(delegatedOperationFilename(id)); err != nil {
			return 0, ErrProposalStore
		}
	}
	if err := dir.Sync(); err != nil {
		return 0, ErrProposalStore
	}
	return len(expired), nil
}

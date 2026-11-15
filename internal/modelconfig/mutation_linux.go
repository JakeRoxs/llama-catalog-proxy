//go:build linux

package modelconfig

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"time"
)

type mutationResult = MutationResult

// ApplyStoredProposal keeps the proposal locked until the target write finishes.
// The caller must enforce authorization, directory separation, and an exclusive edit window.
func ApplyStoredProposal(proposalDirectory, proposalID, directory, filename, targetID, backupDirectory string, limit int) (MutationResult, error) {
	var result MutationResult
	err := withProposalStore(proposalDirectory, func(root *os.Root, _ *os.File) error {
		proposal, err := readProposal(root, proposalID)
		if err != nil {
			return err
		}
		result, err = applyReviewedProposal(directory, filename, targetID, backupDirectory, limit, proposal)
		return err
	})
	return result, err
}

// ApplyStoredDelegatedProposal holds both private-store locks through execution
// and outcome persistence. The caller must enforce current authorization,
// directory separation, and an exclusive edit window under its lifecycle lock.
// A false reservation is a replay and never invokes the mutation.
// If outcome persistence fails, the returned reservation remains unknown and
// the error must be surfaced; callers must not retry the mutation.
func ApplyStoredDelegatedProposal(proposalDirectory, operationDirectory, proposalID, operationID, directory, filename, backupDirectory string, limit int, binding DelegatedApplyBinding) (DelegatedOperation, bool, error) {
	if !validProposalID(proposalID) || !validProposalID(operationID) {
		return DelegatedOperation{}, false, ErrProposalInvalid
	}
	var result DelegatedOperation
	var reserved bool
	err := withProposalStore(proposalDirectory, func(proposals *os.Root, _ *os.File) error {
		proposal, err := readDelegatedProposal(proposals, proposalID)
		if err != nil {
			return err
		}
		if proposal.ReviewRevision != binding.ReviewRevision {
			return ErrDelegatedProposalBinding
		}
		return withProposalStore(operationDirectory, func(operations *os.Root, dir *os.File) error {
			now := time.Now()
			if err := ValidateDelegatedProposal(proposal, binding.Owner, binding.RequesterNode, binding.BackendID, binding.TargetID, now); err != nil {
				return err
			}
			result, reserved, err = reserveDelegatedOperation(operations, dir, proposal, operationID, now)
			if err != nil || !reserved {
				return err
			}
			check := func() error {
				now := time.Now()
				if now.Before(result.AcceptedAt) {
					return ErrDelegatedClockRollback
				}
				return ValidateDelegatedProposal(proposal, binding.Owner, binding.RequesterNode, binding.BackendID, binding.TargetID, now)
			}
			mutation, mutationErr := applyReviewedProposalChecked(directory, filename, binding.TargetID, backupDirectory, limit, proposal.Proposal, check)
			outcome, outcomeErr := delegatedMutationOutcome(mutation, mutationErr)
			if outcomeErr != nil {
				return errors.Join(mutationErr, outcomeErr)
			}
			finished, finishErr := updateDelegatedOperationLocked(operations, dir, proposalID,
				finishDelegatedOperation(operationID, binding.Owner.ProxyInstanceID, outcome, time.Now()))
			if finishErr != nil {
				return errors.Join(mutationErr, finishErr)
			}
			result = finished
			return mutationErr
		})
	})
	return result, reserved, err
}

// ApplyStoredCreationProposal holds the proposal and target locks through
// replacement. The caller must enforce authorization, directory separation,
// an exclusive edit window, and the artifact binding's lifetime.
func ApplyStoredCreationProposal(proposalDirectory, proposalID, directory, filename, targetID, backupDirectory string, limit int, artifact CreationArtifactBinding) (MutationResult, error) {
	if artifact.Recheck == nil {
		return MutationResult{}, ErrProposalArtifact
	}
	var result MutationResult
	err := withProposalStore(proposalDirectory, func(root *os.Root, _ *os.File) error {
		record, err := readCreationProposal(root, proposalID)
		if err != nil {
			return err
		}
		if record.Proposal.ArtifactRecordID != artifact.RecordID {
			return ErrProposalArtifact
		}
		return withTargetWrite(directory, filename, func(target *targetWrite) error {
			preview, err := revalidateCreationRecord(directory, filename, targetID, artifact.Revision, artifact.ModelPath, record, target.source)
			if err != nil {
				return err
			}
			backup, err := newBackupRecord(targetID, record.Proposal.TargetFingerprint, record.Proposal.ModelID,
				record.Proposal.ID, target.source, preview.candidate)
			if err != nil {
				return err
			}
			result, err = commitWithBackup(backupDirectory, limit, func(*os.Root) (backupRecord, []byte, error) {
				return backup, preview.candidate, nil
			}, func(candidate []byte) (bool, error) {
				if err := artifact.Recheck(); err != nil {
					return false, err
				}
				return target.replace(candidate)
			})
			return err
		})
	})
	return result, err
}

// RollbackStoredBackup rechecks the backup and target while both write locks are held.
// The caller must enforce authorization, directory separation, and an exclusive edit window.
func RollbackStoredBackup(directory, filename, targetID, backupDirectory, backupID string, limit int) (MutationResult, error) {
	return rollbackBackup(directory, filename, targetID, backupDirectory, backupID, limit)
}

// These internal operations require an authorized caller to enforce an exclusive
// edit window; the directory lock cannot prevent uncooperative writers.
func applyReviewedProposal(directory, filename, targetID, backupDirectory string, limit int, proposal Proposal) (mutationResult, error) {
	return applyReviewedProposalChecked(directory, filename, targetID, backupDirectory, limit, proposal, nil)
}

func applyReviewedProposalChecked(directory, filename, targetID, backupDirectory string, limit int, proposal Proposal, check func() error) (mutationResult, error) {
	var result mutationResult
	err := withTargetWrite(directory, filename, func(target *targetWrite) error {
		if check != nil {
			if err := check(); err != nil {
				return err
			}
		}
		record, err := prepareBackup(directory, filename, targetID, proposal, target.source)
		if err != nil {
			return err
		}
		preview, err := PreviewDocument(targetID, proposal.ModelID, target.source, proposal.Settings)
		if err != nil {
			return err
		}
		if bytes.Equal(target.source, preview.candidate) {
			return errTargetNoChange
		}
		result, err = commitWithBackup(backupDirectory, limit, func(*os.Root) (backupRecord, []byte, error) {
			return record, preview.candidate, nil
		}, func(candidate []byte) (bool, error) {
			if check != nil {
				if err := check(); err != nil {
					return false, err
				}
			}
			return target.replace(candidate)
		})
		return err
	})
	return result, err
}

func rollbackBackup(directory, filename, targetID, backupDirectory, backupID string, limit int) (mutationResult, error) {
	var result mutationResult
	err := withTargetWrite(directory, filename, func(target *targetWrite) error {
		var err error
		result, err = commitWithBackup(backupDirectory, limit, func(root *os.Root) (backupRecord, []byte, error) {
			record, err := readBackup(root, backupID)
			if err != nil {
				return backupRecord{}, nil, err
			}
			candidate, err := prepareRollback(directory, filename, targetID, record, target.source)
			if err != nil {
				return backupRecord{}, nil, err
			}
			if bytes.Equal(target.source, candidate) {
				return backupRecord{}, nil, errTargetNoChange
			}
			id := make([]byte, 16)
			if _, err := rand.Read(id); err != nil {
				return backupRecord{}, nil, errBackupStore
			}
			reverse := backupRecord{
				Info: backupInfo{
					ID: hex.EncodeToString(id), CreatedAt: time.Now().UTC(),
					TargetID: record.Info.TargetID, TargetFingerprint: record.Info.TargetFingerprint,
					ModelID: record.Info.ModelID, ProposalID: record.Info.ProposalID,
					SourceRevision: record.Info.WrittenRevision, WrittenRevision: record.Info.SourceRevision,
				},
				Source: append([]byte(nil), target.source...),
			}
			return reverse, candidate, nil
		}, target.replace)
		return err
	})
	return result, err
}

// Keep existing backups until the target is durably replaced. A post-rename
// error retains all records, even above the limit, for explicit recovery.
func commitWithBackup(directory string, limit int, prepare func(*os.Root) (backupRecord, []byte, error), replace func([]byte) (bool, error)) (mutationResult, error) {
	var result mutationResult
	err := withPrivateStore(directory, errBackupStore, func(root *os.Root, dir *os.File) error {
		backups, err := backupInventory(root, dir, limit)
		if err != nil {
			return err
		}
		record, candidate, err := prepare(root)
		if err != nil {
			return err
		}
		if !validBackup(record) || sourceRevision(candidate) != record.Info.WrittenRevision {
			return errBackupStore
		}
		result.BackupID = record.Info.ID
		if err := writeBackup(root, dir, record); err != nil {
			return err
		}
		result.Written, err = replace(candidate)
		if !result.Written {
			if errors.Is(err, ErrTargetRenameUncertain) {
				result.OutcomeUnknown = true
				result.Revision = record.Info.WrittenRevision
				return err
			}
			if err == nil {
				err = errTargetWrite
			}
			if removeErr := root.Remove(backupFilename(record.Info.ID)); removeErr != nil {
				return errors.Join(err, errBackupStore)
			}
			if syncErr := dir.Sync(); syncErr != nil {
				return errors.Join(err, errBackupStore)
			}
			result.BackupID = ""
			return err
		}
		result.Revision = record.Info.WrittenRevision
		if err != nil {
			return err
		}
		result.Durable = true
		return rotateBackup(root, dir, backups, limit)
	})
	return result, err
}

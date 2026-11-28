//go:build linux

package modelconfig

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func mutationFixture(t *testing.T) (string, string, []byte, Proposal) {
	t.Helper()
	targetDir, backupDir := t.TempDir(), t.TempDir()
	if err := os.Chmod(backupDir, 0o700); err != nil {
		t.Fatal(err)
	}
	source := []byte("# confidential\nmodels: {m: {cmd: 'llama-server -c 1024'}}\n")
	if err := os.WriteFile(filepath.Join(targetDir, "config.yaml"), source, 0o640); err != nil {
		t.Fatal(err)
	}
	size := int32(4096)
	settings := CommandSettings{ContextSize: &size}
	preview, err := PreviewDocument("local", "m", source, settings)
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := NewProposal(targetDir, "config.yaml", preview, settings)
	if err != nil {
		t.Fatal(err)
	}
	return targetDir, backupDir, source, proposal
}

func TestReviewedMutationAndReversibleRollback(t *testing.T) {
	targetDir, backupDir, source, proposal := mutationFixture(t)
	path := filepath.Join(targetDir, "config.yaml")
	ctx := mutationCheck{t, targetDir, path, source, backupDir, proposal}
	applied, current := verifyMutationApplied(ctx)
	verifyRollbackAndReverse(ctx, applied, current)
}

// mutationCheck groups the target, source, and backup context shared by the
// mutation verification helpers so verifyRollbackAndReverse does not thread
// eight separate arguments (Sonar go:S107: too many parameters).
type mutationCheck struct {
	t         *testing.T
	targetDir string
	path      string
	source    []byte
	backupDir string
	proposal  Proposal
}

// verifyMutationApplied applies the reviewed proposal and verifies the applied
// result, the on-disk content, and the backup contents. It returns the applied
// result and the post-apply content for later reverse-rollback comparisons.
func verifyMutationApplied(ctx mutationCheck) (MutationResult, []byte) {
	t := ctx.t
	t.Helper()
	applied, err := applyReviewedProposal(ctx.targetDir, "config.yaml", "local", ctx.backupDir, 1, ctx.proposal)
	if err != nil || !applied.Written || !applied.Durable || applied.BackupID == "" {
		t.Fatalf("apply = %+v, %v", applied, err)
	}
	current, err := os.ReadFile(ctx.path)
	if err != nil || bytes.Equal(current, ctx.source) || sourceRevision(current) != applied.Revision {
		t.Fatalf("apply content: %v", err)
	}
	backup, err := loadBackup(ctx.backupDir, applied.BackupID)
	if err != nil || !bytes.Equal(backup.Source, ctx.source) {
		t.Fatalf("apply backup: %v", err)
	}
	return applied, current
}

// verifyRollbackAndReverse verifies the stale replay rejection, the rollback,
// backup retention, the reverse backup, the reverse rollback, and the target
// permissions. Source bytes, revisions, backup retention, rollback order, and
// mode checks are preserved verbatim.
func verifyRollbackAndReverse(ctx mutationCheck, applied MutationResult, current []byte) {
	t := ctx.t
	t.Helper()
	if _, err := applyReviewedProposal(ctx.targetDir, "config.yaml", "local", ctx.backupDir, 1, ctx.proposal); !errors.Is(err, ErrProposalStale) {
		t.Fatalf("replayed proposal: %v", err)
	}
	rolledBack, err := rollbackBackup(ctx.targetDir, "config.yaml", "local", ctx.backupDir, applied.BackupID, 1)
	if err != nil || !rolledBack.Written || !rolledBack.Durable || rolledBack.Revision != sourceRevision(ctx.source) {
		t.Fatalf("rollback = %+v, %v", rolledBack, err)
	}
	restored, err := os.ReadFile(ctx.path)
	if err != nil || !bytes.Equal(restored, ctx.source) {
		t.Fatalf("rollback content: %v", err)
	}
	if _, err := loadBackup(ctx.backupDir, applied.BackupID); !errors.Is(err, errBackupStore) {
		t.Fatalf("oldest backup was not rotated: %v", err)
	}
	reverse, err := loadBackup(ctx.backupDir, rolledBack.BackupID)
	if err != nil || !bytes.Equal(reverse.Source, current) {
		t.Fatalf("rollback did not back up replaced config: %v", err)
	}
	undone, err := rollbackBackup(ctx.targetDir, "config.yaml", "local", ctx.backupDir, rolledBack.BackupID, 1)
	if err != nil || !undone.Written || !undone.Durable {
		t.Fatalf("reverse rollback = %+v, %v", undone, err)
	}
	got, err := os.ReadFile(ctx.path)
	if err != nil || !bytes.Equal(got, current) {
		t.Fatalf("reverse rollback content: %v", err)
	}
	info, err := os.Stat(ctx.path)
	if err != nil || info.Mode().Perm() != 0o640 {
		t.Fatalf("target permissions: %v", err)
	}
}

func TestReviewedMutationRejectsStaleAndCorruptState(t *testing.T) {
	targetDir, backupDir, source, proposal := mutationFixture(t)
	path := filepath.Join(targetDir, "config.yaml")
	if err := os.WriteFile(path, append(source, []byte("# external\n")...), 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := applyReviewedProposal(targetDir, "config.yaml", "local", backupDir, 1, proposal); !errors.Is(err, ErrProposalStale) {
		t.Fatalf("stale proposal accepted: %v", err)
	}
	if err := os.WriteFile(path, source, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(backupDir, "unexpected"), []byte("invalid"), 0o600); err != nil {
		t.Fatal(err)
	}
	if result, err := applyReviewedProposal(targetDir, "config.yaml", "local", backupDir, 1, proposal); !errors.Is(err, errBackupStore) || result.Written {
		t.Fatalf("corrupt store accepted: %+v, %v", result, err)
	}
	current, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(current, source) {
		t.Fatalf("target changed on failure: %v", err)
	}
}

func TestRollbackRejectsChangedTargetWithoutRotating(t *testing.T) {
	targetDir, backupDir, _, proposal := mutationFixture(t)
	applied, err := applyReviewedProposal(targetDir, "config.yaml", "local", backupDir, 1, proposal)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(targetDir, "config.yaml")
	external := []byte("external change")
	if err := os.WriteFile(path, external, 0o640); err != nil {
		t.Fatal(err)
	}
	if result, err := rollbackBackup(targetDir, "config.yaml", "local", backupDir, applied.BackupID, 1); !errors.Is(err, errRollbackStale) || result.Written {
		t.Fatalf("stale rollback accepted: %+v, %v", result, err)
	}
	if _, err := loadBackup(backupDir, applied.BackupID); err != nil {
		t.Fatalf("backup lost after stale rollback: %v", err)
	}
	current, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(current, external) {
		t.Fatalf("external change lost: %v", err)
	}
}

func TestReviewedMutationRequiresTargetLock(t *testing.T) {
	targetDir, backupDir, source, proposal := mutationFixture(t)
	err := withTargetWrite(targetDir, "config.yaml", func(*targetWrite) error {
		result, err := applyReviewedProposal(targetDir, "config.yaml", "local", backupDir, 1, proposal)
		if !errors.Is(err, errTargetWrite) || result.Written {
			t.Fatalf("concurrent apply = %+v, %v", result, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	current, err := os.ReadFile(filepath.Join(targetDir, "config.yaml"))
	if err != nil || !bytes.Equal(current, source) {
		t.Fatalf("target changed under lock: %v", err)
	}
	entries, err := os.ReadDir(backupDir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("backup created under lock: %v", err)
	}
}

func TestStoredProposalMustExistAndHoldItsStoreLock(t *testing.T) {
	targetDir, backupDir, source, proposal := mutationFixture(t)
	proposalDir := t.TempDir()
	if err := os.Chmod(proposalDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := SaveProposal(proposalDir, proposal); err != nil {
		t.Fatal(err)
	}
	if err := withProposalStore(proposalDir, func(_ *os.Root, _ *os.File) error {
		result, err := ApplyStoredProposal(proposalDir, proposal.ID, targetDir, "config.yaml", "local", backupDir, 1)
		if !errors.Is(err, ErrProposalStore) || result.Written {
			t.Fatalf("apply ignored proposal lock: %+v, %v", result, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := DeleteProposal(proposalDir, proposal.ID); err != nil {
		t.Fatal(err)
	}
	result, err := ApplyStoredProposal(proposalDir, proposal.ID, targetDir, "config.yaml", "local", backupDir, 1)
	if !errors.Is(err, ErrProposalNotFound) || result.Written {
		t.Fatalf("deleted proposal applied: %+v, %v", result, err)
	}
	current, err := os.ReadFile(filepath.Join(targetDir, "config.yaml"))
	if err != nil || !bytes.Equal(current, source) {
		t.Fatalf("target changed without proposal: %v", err)
	}
}

func TestReviewedMutationRejectsNoChange(t *testing.T) {
	targetDir, backupDir, source, _ := mutationFixture(t)
	size := int32(1024)
	settings := CommandSettings{ContextSize: &size}
	preview, err := PreviewDocument("local", "m", source, settings)
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := NewProposal(targetDir, "config.yaml", preview, settings)
	if err != nil {
		t.Fatal(err)
	}
	result, err := applyReviewedProposal(targetDir, "config.yaml", "local", backupDir, 1, proposal)
	if !errors.Is(err, errTargetNoChange) || result.Written {
		t.Fatalf("no-op apply = %+v, %v", result, err)
	}
	entries, err := os.ReadDir(backupDir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("no-op created backup: %v", err)
	}
}

func TestBackupMutationRechecksTargetAfterStaging(t *testing.T) {
	targetDir, backupDir, _, proposal := mutationFixture(t)
	path := filepath.Join(targetDir, "config.yaml")
	external := []byte("external change")
	err := withTargetWrite(targetDir, "config.yaml", func(target *targetWrite) error {
		record, err := prepareBackup(targetDir, "config.yaml", "local", proposal, target.source)
		if err != nil {
			return err
		}
		preview, err := PreviewDocument("local", "m", target.source, proposal.Settings)
		if err != nil {
			return err
		}
		result, err := commitWithBackup(backupDir, 1, func(*os.Root) (backupRecord, []byte, error) {
			return record, preview.candidate, nil
		}, func(candidate []byte) (bool, error) {
			if err := os.WriteFile(path, external, 0o640); err != nil {
				t.Fatal(err)
			}
			return target.replace(candidate)
		})
		if !errors.Is(err, errTargetChanged) || result.Written || result.BackupID != "" {
			t.Fatalf("changed target accepted: %+v, %v", result, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	current, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(current, external) {
		t.Fatalf("external change lost: %v", err)
	}
	entries, err := os.ReadDir(backupDir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("staged backup not removed: %v", err)
	}
}

// backupMutationFixture builds the shared backup fixture used by
// TestBackupRotationFailureReportsDurableWrite and runBackupMutationCase: a
// temp directory with the initial backup stored, plus the proposal and the
// candidate for the next mutation. Extracting this shared setup removes the
// duplicated 17-line block reported by the duplicated-lines rule.
func backupMutationFixture(t *testing.T) (string, Proposal, backupRecord, backupRecord, []byte) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	source := []byte("models: {m: {cmd: 'llama-server -c 1024'}}")
	proposal, old := backupFixture(t, source)
	_, next := backupFixture(t, source)
	if err := saveBackup(dir, old, 1); err != nil {
		t.Fatal(err)
	}
	preview, err := PreviewDocument("local", "m", source, proposal.Settings)
	if err != nil {
		t.Fatal(err)
	}
	return dir, proposal, old, next, preview.candidate
}

func TestBackupRotationFailureReportsDurableWrite(t *testing.T) {
	dir, _, old, next, candidate := backupMutationFixture(t)
	result, err := commitWithBackup(dir, 1, func(*os.Root) (backupRecord, []byte, error) {
		return next, candidate, nil
	}, func([]byte) (bool, error) {
		oldPath := filepath.Join(dir, backupFilename(old.Info.ID))
		if err := os.Remove(oldPath); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(oldPath, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(oldPath, "block-removal"), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		return true, nil
	})
	if !errors.Is(err, errBackupStore) || !result.Written || !result.Durable || result.BackupID != next.Info.ID {
		t.Fatalf("rotation failure = %+v, %v", result, err)
	}
	if _, err := loadBackup(dir, next.Info.ID); err != nil {
		t.Fatalf("new backup lost: %v", err)
	}
}

// backupMutationCase is a single row of the TestBackupMutationFailureAndUncertainCommit
// matrix. Every field and table value is preserved verbatim from the original.
type backupMutationCase struct {
	name        string
	written     bool
	replaceErr  error
	wantOld     bool
	wantNew     bool
	wantDurable bool
	wantUnknown bool
}

func TestBackupMutationFailureAndUncertainCommit(t *testing.T) {
	for _, tc := range []backupMutationCase{
		{name: "before rename", replaceErr: errTargetChanged, wantOld: true},
		{name: "uncertain durability", written: true, replaceErr: errTargetCommitUncertain, wantOld: true, wantNew: true},
		{name: "uncertain rename", replaceErr: ErrTargetRenameUncertain, wantOld: true, wantNew: true, wantUnknown: true},
		{name: "durable commit", written: true, wantNew: true, wantDurable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runBackupMutationCase(t, tc)
		})
	}
}

// runBackupMutationCase executes one matrix case of
// TestBackupMutationFailureAndUncertainCommit. The fixture setup,
// commitWithBackup invocation, result-field assertions, backup-retention
// decisions, backup-ID check, and over-limit fail-closed check are preserved
// verbatim.
func runBackupMutationCase(t *testing.T, tc backupMutationCase) {
	t.Helper()
	dir, _, old, next, candidate := backupMutationFixture(t)
	result, err := commitWithBackup(dir, 1, func(*os.Root) (backupRecord, []byte, error) {
		return next, candidate, nil
	}, func([]byte) (bool, error) {
		return tc.written, tc.replaceErr
	})
	if !errors.Is(err, tc.replaceErr) || result.Written != tc.written || result.Durable != tc.wantDurable ||
		result.OutcomeUnknown != tc.wantUnknown {
		t.Fatalf("mutation = %+v, %v", result, err)
	}
	_, oldErr := loadBackup(dir, old.Info.ID)
	_, newErr := loadBackup(dir, next.Info.ID)
	if (oldErr == nil) != tc.wantOld || (newErr == nil) != tc.wantNew {
		t.Fatalf("backup retention: old=%v new=%v", oldErr, newErr)
	}
	if tc.wantNew && result.BackupID != next.Info.ID || !tc.wantNew && result.BackupID != "" {
		t.Fatalf("wrong backup ID: %+v", result)
	}
	if tc.written && tc.replaceErr != nil {
		if err := saveBackup(dir, old, 1); !errors.Is(err, errBackupStore) {
			t.Fatalf("over-limit store did not fail closed: %v", err)
		}
	}
}

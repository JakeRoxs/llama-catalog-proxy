//go:build linux

package modelconfig

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCreationMutationAndRollback(t *testing.T) {
	for _, mode := range []string{"explicit", "template"} {
		t.Run(mode, func(t *testing.T) {
			target, store, backup, path, source, proposal, preview, recordID, artifactRevision := newCreationFixture(t, mode)
			checks := 0
			binding := CreationArtifactBinding{RecordID: recordID, Revision: artifactRevision, ModelPath: "/models/new.gguf",
				Recheck: func() error {
					checks++
					return nil
				}}
			applied := applyCreationProposal(t, store, target, backup, proposal.ID, binding, &checks)
			assertAppliedContent(t, path, preview.candidate, applied.Revision, source)
			assertReplayRejected(t, store, target, backup, proposal.ID, binding)
			assertRollbackRestores(t, target, backup, applied.BackupID, source, path)
		})
	}
}

// newCreationFixture secures the store/backup dirs, writes the original config,
// builds the preview and saves the proposal for the given mode.
func newCreationFixture(t *testing.T, mode string) (target, store, backup, path string, source []byte, proposal CreationProposal, preview CreationPreview, recordID, artifactRevision string) {
	target, store, backup = t.TempDir(), t.TempDir(), t.TempDir()
	for _, dir := range []string{store, backup} {
		if err := os.Chmod(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	source = []byte("# confidential\nmodels:\n  old: {cmd: 'llama-server --port ${PORT} -m /models/old.gguf'}\n")
	path = filepath.Join(target, "config.yaml")
	if err := os.WriteFile(path, source, 0o640); err != nil {
		t.Fatal(err)
	}
	input, template := CreationCommandInput{}, "old"
	if mode == "explicit" {
		input, template = CreationCommandInput{Executable: "llama-server"}, ""
	}
	previewInput := input
	previewInput.ModelPath = "/models/new.gguf"
	preview, err := PreviewCreationDocument("local", "new", template, source, previewInput)
	if err != nil {
		t.Fatal(err)
	}
	preview.Artifact = "regular_file_when_checked"
	recordID, artifactRevision = "ingest-"+strings.Repeat("a", 32), strings.Repeat("b", 64)
	proposal, err = SaveCreationProposal(store, target, "config.yaml", preview, recordID, artifactRevision, input)
	if err != nil {
		t.Fatal(err)
	}
	return
}

// applyCreationProposal applies the stored proposal and asserts a durable write
// with exactly one artifact recheck.
func applyCreationProposal(t *testing.T, store, target, backup, proposalID string, binding CreationArtifactBinding, checks *int) MutationResult {
	applied, err := ApplyStoredCreationProposal(CreationProposalApplyRequest{
		ProposalDirectory: store,
		ProposalID:        proposalID,
		Target: MutationTarget{
			Directory: target,
			Filename:  "config.yaml",
		},
		TargetID:        "local",
		BackupDirectory: backup,
		Limit:           2,
		Artifact:        binding,
	})
	if err != nil || !applied.Written || !applied.Durable || applied.BackupID == "" || *checks != 1 {
		t.Fatalf("apply = %+v, checks=%d, error=%v", applied, *checks, err)
	}
	return applied
}

// assertAppliedContent verifies the target holds the preview candidate bytes
// and the durable source revision.
func assertAppliedContent(t *testing.T, path string, candidate []byte, revision string, source []byte) {
	current, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(current, candidate) || sourceRevision(current) != revision {
		t.Fatalf("unexpected applied content: %v", err)
	}
}

// assertReplayRejected verifies replaying the proposal is rejected as stale.
func assertReplayRejected(t *testing.T, store, target, backup, proposalID string, binding CreationArtifactBinding) {
	if _, err := ApplyStoredCreationProposal(CreationProposalApplyRequest{
		ProposalDirectory: store,
		ProposalID:        proposalID,
		Target: MutationTarget{
			Directory: target,
			Filename:  "config.yaml",
		},
		TargetID:        "local",
		BackupDirectory: backup,
		Limit:           2,
		Artifact:        binding,
	}); !errors.Is(err, ErrProposalStale) {
		t.Fatalf("replayed proposal accepted: %v", err)
	}
}

// assertRollbackRestores verifies rollback restores the exact source bytes,
// revision, and 0o640 permissions.
func assertRollbackRestores(t *testing.T, target, backup, backupID string, source []byte, path string) {
	rolledBack, err := RollbackStoredBackup(target, "config.yaml", "local", backup, backupID, 2)
	if err != nil || !rolledBack.Written || !rolledBack.Durable || rolledBack.Revision != sourceRevision(source) {
		t.Fatalf("rollback = %+v, %v", rolledBack, err)
	}
	restored, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(restored, source) {
		t.Fatalf("rollback changed original source: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o640 {
		t.Fatalf("target permissions changed: %v", err)
	}
}

func TestCreationMutationRejectsArtifactChangeAfterBackup(t *testing.T) {
	target, store, backup := t.TempDir(), t.TempDir(), t.TempDir()
	for _, dir := range []string{store, backup} {
		if err := os.Chmod(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	source := []byte("models:\n  old: {cmd: 'llama-server --port ${PORT} -m /models/old.gguf'}\n")
	path := filepath.Join(target, "config.yaml")
	if err := os.WriteFile(path, source, 0o600); err != nil {
		t.Fatal(err)
	}
	input := CreationCommandInput{Executable: "llama-server"}
	previewInput := input
	previewInput.ModelPath = "/models/new.gguf"
	preview, err := PreviewCreationDocument("local", "new", "", source, previewInput)
	if err != nil {
		t.Fatal(err)
	}
	preview.Artifact = "regular_file_when_checked"
	recordID, revision := "ingest-"+strings.Repeat("a", 32), strings.Repeat("b", 64)
	proposal, err := SaveCreationProposal(store, target, "config.yaml", preview, recordID, revision, input)
	if err != nil {
		t.Fatal(err)
	}
	binding := CreationArtifactBinding{RecordID: recordID, Revision: revision, ModelPath: "/models/new.gguf",
		Recheck: func() error {
			entries, err := os.ReadDir(backup)
			if err != nil || len(entries) != 1 {
				t.Fatalf("artifact recheck did not occur after backup: %v %v", entries, err)
			}
			return ErrProposalArtifact
		}}
	result, err := ApplyStoredCreationProposal(CreationProposalApplyRequest{
		ProposalDirectory: store,
		ProposalID:        proposal.ID,
		Target: MutationTarget{
			Directory: target,
			Filename:  "config.yaml",
		},
		TargetID:        "local",
		BackupDirectory: backup,
		Limit:           2,
		Artifact:        binding,
	})
	if !errors.Is(err, ErrProposalArtifact) || result.Written || result.BackupID != "" {
		t.Fatalf("changed artifact applied: %+v, %v", result, err)
	}
	entries, err := os.ReadDir(backup)
	if err != nil || len(entries) != 0 {
		t.Fatalf("failed apply left backup: %v %v", entries, err)
	}
	current, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(current, source) {
		t.Fatalf("failed apply changed target: %v", err)
	}
	binding.Recheck = nil
	if _, err := ApplyStoredCreationProposal(CreationProposalApplyRequest{
		ProposalDirectory: store,
		ProposalID:        proposal.ID,
		Target: MutationTarget{
			Directory: target,
			Filename:  "config.yaml",
		},
		TargetID:        "local",
		BackupDirectory: backup,
		Limit:           2,
		Artifact:        binding,
	}); !errors.Is(err, ErrProposalArtifact) {
		t.Fatalf("missing recheck accepted: %v", err)
	}
}

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
			target, store, backup := t.TempDir(), t.TempDir(), t.TempDir()
			for _, dir := range []string{store, backup} {
				if err := os.Chmod(dir, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			source := []byte("# confidential\nmodels:\n  old: {cmd: 'llama-server --port ${PORT} -m /models/old.gguf'}\n")
			path := filepath.Join(target, "config.yaml")
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
			recordID, artifactRevision := "ingest-"+strings.Repeat("a", 32), strings.Repeat("b", 64)
			proposal, err := SaveCreationProposal(store, target, "config.yaml", preview, recordID, artifactRevision, input)
			if err != nil {
				t.Fatal(err)
			}
			checks := 0
			binding := CreationArtifactBinding{RecordID: recordID, Revision: artifactRevision, ModelPath: "/models/new.gguf",
				Recheck: func() error {
					checks++
					return nil
				}}
			applied, err := ApplyStoredCreationProposal(store, proposal.ID, target, "config.yaml", "local", backup, 2, binding)
			if err != nil || !applied.Written || !applied.Durable || applied.BackupID == "" || checks != 1 {
				t.Fatalf("apply = %+v, checks=%d, error=%v", applied, checks, err)
			}
			current, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(current, preview.candidate) || sourceRevision(current) != applied.Revision {
				t.Fatalf("unexpected applied content: %v", err)
			}
			if _, err := ApplyStoredCreationProposal(store, proposal.ID, target, "config.yaml", "local", backup, 2, binding); !errors.Is(err, ErrProposalStale) {
				t.Fatalf("replayed proposal accepted: %v", err)
			}
			rolledBack, err := RollbackStoredBackup(target, "config.yaml", "local", backup, applied.BackupID, 2)
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
		})
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
	result, err := ApplyStoredCreationProposal(store, proposal.ID, target, "config.yaml", "local", backup, 2, binding)
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
	if _, err := ApplyStoredCreationProposal(store, proposal.ID, target, "config.yaml", "local", backup, 2, binding); !errors.Is(err, ErrProposalArtifact) {
		t.Fatalf("missing recheck accepted: %v", err)
	}
}

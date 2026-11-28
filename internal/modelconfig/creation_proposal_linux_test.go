//go:build linux

package modelconfig

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// proposalFixture holds the shared state built for one mode iteration of the
// persistence and revalidation test. Each field is an exact input or outcome
// preserved from the original single test.
type proposalFixture struct {
	store, target    string
	source           string
	preview          CreationPreview
	proposal         CreationProposal
	artifactID       string
	artifactRevision string
}

// TestCreationProposalPersistsAndRevalidates exercises both creation modes
// (explicit and template) and orchestrates the focused helpers below.
func TestCreationProposalPersistsAndRevalidates(t *testing.T) {
	for _, mode := range []string{"explicit", "template"} {
		t.Run(mode, func(t *testing.T) {
			fx := newProposalFixture(t, mode)
			assertProposalSecrecy(t, fx)
			assertRevalidationFailureMatrix(t, fx)
			assertProposalListAndDelete(t, fx)
		})
	}
}

// newProposalFixture builds the temp store/target, writes the config source,
// constructs the mode-specific input, previews and persists the proposal.
func newProposalFixture(t *testing.T, mode string) proposalFixture {
	store, target := t.TempDir(), t.TempDir()
	if err := os.Chmod(store, 0o700); err != nil {
		t.Fatal(err)
	}
	source := "secret: confidential\nmodels:\n  old:\n    cmd: llama-server --port ${PORT} -m /private/old.gguf -ngl 20\n    env: [CUDA_VISIBLE_DEVICES=0]\n"
	if err := os.WriteFile(filepath.Join(target, "config.yaml"), []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	input := CreationCommandInput{}
	template := "old"
	if mode == "explicit" {
		ctx := int32(4096)
		input = CreationCommandInput{
			Executable: "/private/bin/llama-server", Settings: CommandSettings{ContextSize: &ctx},
		}
		template = ""
	}
	previewInput := input
	previewInput.ModelPath = "/models/new.gguf"
	preview, err := PreviewCreationTarget(target, "config.yaml", "local", "new", template, previewInput)
	if err != nil {
		t.Fatal(err)
	}
	preview.Artifact = "regular_file_when_checked"
	artifactID, artifactRevision := "ingest-"+strings.Repeat("a", 32), strings.Repeat("b", 64)
	proposal, err := SaveCreationProposal(store, target, "config.yaml", preview, artifactID, artifactRevision, input)
	if err != nil {
		t.Fatal(err)
	}
	return proposalFixture{
		store: store, target: target, source: source, preview: preview, proposal: proposal,
		artifactID: artifactID, artifactRevision: artifactRevision,
	}
}

// assertProposalSecrecy verifies the persisted proposal reloads with the exact
// identity and artifact record, is exposed through the shared store as a
// CreationProposal, and leaks no private input in its JSON or on disk.
func assertProposalSecrecy(t *testing.T, fx proposalFixture) {
	loaded, err := LoadCreationProposal(fx.store, fx.proposal.ID)
	if err != nil || loaded.ID != fx.proposal.ID || loaded.ArtifactRecordID != fx.artifactID {
		t.Fatalf("proposal reload failed: %#v %v", loaded, err)
	}
	generic, err := LoadStoredProposal(fx.store, fx.proposal.ID)
	if _, ok := generic.(CreationProposal); err != nil || !ok {
		t.Fatalf("shared store returned wrong proposal type: %#v %v", generic, err)
	}
	public, err := json.Marshal(loaded)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"confidential", "/private", "/models", "llama-server"} {
		if strings.Contains(string(public), secret) {
			t.Fatalf("proposal leaked private input: %s", public)
		}
	}
	disk, err := os.ReadFile(filepath.Join(fx.store, proposalFilename(fx.proposal.ID)))
	if err != nil || strings.Contains(string(disk), "confidential") ||
		strings.Contains(string(disk), "/models/") || strings.Contains(string(disk), "/private/old.gguf") {
		t.Fatalf("stored source or candidate configuration: %v", err)
	}
}

// assertRevalidationFailureMatrix verifies the happy revalidation path then the
// failure matrix for changed artifact, changed model path, changed target,
// missing target path, and stale config.
func assertRevalidationFailureMatrix(t *testing.T, fx proposalFixture) {
	verified, err := RevalidateStoredCreationProposal(fx.store, fx.proposal.ID, fx.target, "config.yaml", "local", fx.artifactRevision, "/models/new.gguf")
	if err != nil || string(verified.candidate) != string(fx.preview.candidate) {
		t.Fatalf("proposal verification failed: %v", err)
	}
	if _, err := RevalidateStoredCreationProposal(fx.store, fx.proposal.ID, fx.target, "config.yaml", "local", strings.Repeat("c", 64), "/models/new.gguf"); !errors.Is(err, ErrProposalArtifact) {
		t.Fatalf("changed artifact accepted: %v", err)
	}
	if _, err := RevalidateStoredCreationProposal(fx.store, fx.proposal.ID, fx.target, "config.yaml", "local", fx.artifactRevision, "/models/other.gguf"); !errors.Is(err, ErrProposalReview) {
		t.Fatalf("changed path accepted: %v", err)
	}
	if _, err := RevalidateStoredCreationProposal(fx.store, fx.proposal.ID, fx.target, "config.yaml", "other", fx.artifactRevision, "/models/new.gguf"); !errors.Is(err, ErrProposalTarget) {
		t.Fatalf("changed target accepted: %v", err)
	}
	if _, err := RevalidateStoredCreationProposal(fx.store, fx.proposal.ID, fx.target, "missing.yaml", "local", fx.artifactRevision, "/models/new.gguf"); !errors.Is(err, ErrProposalTarget) {
		t.Fatalf("changed target path not detected before reading: %v", err)
	}
	if err := os.WriteFile(filepath.Join(fx.target, "config.yaml"), []byte(fx.source+"# changed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := RevalidateStoredCreationProposal(fx.store, fx.proposal.ID, fx.target, "config.yaml", "local", fx.artifactRevision, "/models/new.gguf"); !errors.Is(err, ErrProposalStale) {
		t.Fatalf("stale config accepted: %v", err)
	}
}

// assertProposalListAndDelete verifies the proposal is listed in the shared
// store, can be deleted, and is no longer loadable afterwards.
func assertProposalListAndDelete(t *testing.T, fx proposalFixture) {
	ids, err := ListProposals(fx.store)
	if err != nil || len(ids) != 1 || ids[0] != fx.proposal.ID {
		t.Fatalf("creation proposal omitted from shared store: %v %v", ids, err)
	}
	if err := DeleteProposal(fx.store, fx.proposal.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCreationProposal(fx.store, fx.proposal.ID); !errors.Is(err, ErrProposalNotFound) {
		t.Fatalf("deleted proposal remained: %v", err)
	}
}

func TestCreationProposalRejectsModifiedPrivateExecutable(t *testing.T) {
	source := []byte("models:\n  old: {cmd: 'llama-server --port ${PORT} -m /models/old.gguf'}\n")
	input := CreationCommandInput{Executable: "/private/llama-server"}
	previewInput := input
	previewInput.ModelPath = "/models/new.gguf"
	preview, err := PreviewCreationDocument("local", "new", "", source, previewInput)
	if err != nil {
		t.Fatal(err)
	}
	preview.Artifact = "regular_file_when_checked"
	revision := strings.Repeat("b", 64)
	record, err := newCreationProposalRecord("/target", "config.yaml", preview, "ingest-"+strings.Repeat("a", 32), revision, input)
	if err != nil {
		t.Fatal(err)
	}
	record.Executable = "/different/llama-server"
	if _, err := revalidateCreationRecord("/target", "config.yaml", "local", revision, "/models/new.gguf", record, source); !errors.Is(err, ErrProposalReview) {
		t.Fatalf("changed executable accepted: %v", err)
	}
}

func TestCreationProposalRejectsModifiedReview(t *testing.T) {
	source := []byte("models:\n  old: {cmd: 'llama-server --port ${PORT} -m /models/old.gguf'}\n")
	preview, err := PreviewCreationDocument("local", "new", "old", source, CreationCommandInput{ModelPath: "/models/new.gguf"})
	if err != nil {
		t.Fatal(err)
	}
	preview.Artifact = "regular_file_when_checked"
	revision := strings.Repeat("b", 64)
	record, err := newCreationProposalRecord("/target", "config.yaml", preview, "ingest-"+strings.Repeat("a", 32), revision, CreationCommandInput{})
	if err != nil {
		t.Fatal(err)
	}
	record.Proposal.CandidateRevision = strings.Repeat("c", 64)
	if _, err := revalidateCreationRecord("/target", "config.yaml", "local", revision, "/models/new.gguf", record, source); !errors.Is(err, ErrProposalReview) {
		t.Fatalf("changed candidate revision accepted: %v", err)
	}
}

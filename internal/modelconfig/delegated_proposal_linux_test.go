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

func TestDelegatedProposalStorePersistenceAndIsolation(t *testing.T) {
	dir, owner, proposal, id := setupDelegatedProposalStore(t)
	assertDelegatedRecordPrivate(t, dir, id)
	assertDelegatedBindingAndExpiry(t, dir, id, owner, proposal)
	assertDelegatedListAndDuplicate(t, dir, id, proposal)
	assertDelegatedIsolationFromLocalAPIs(t, dir, id, proposal)
	assertDelegatedDeleteAndInvalidID(t, dir, id)
	assertDelegatedDirectoryMixingRejected(t, dir, id, proposal)
}

// setupDelegatedProposalStore creates a private store directory, saves a
// delegated proposal, and returns the directory, owner, proposal, and id.
func setupDelegatedProposalStore(t *testing.T) (string, DelegatedOwner, DelegatedProposal, string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	owner, proposal := delegatedFixture(t)
	id := proposal.Proposal.ID
	if err := SaveDelegatedProposal(dir, proposal); err != nil {
		t.Fatal(err)
	}
	return dir, owner, proposal, id
}

// assertDelegatedRecordPrivate checks the persisted record is private and does
// not leak the target path or directory name.
func assertDelegatedRecordPrivate(t *testing.T, dir, id string) {
	t.Helper()
	path := filepath.Join(dir, delegatedProposalFilename(id))
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("private record permissions: %v %v", info, err)
	}
	data, err := os.ReadFile(path)
	if err != nil || strings.Contains(string(data), "config.yaml") || strings.Contains(string(data), "directory") {
		t.Fatalf("record includes target path or cannot be read: %v", err)
	}
}

// assertDelegatedBindingAndExpiry checks the persisted proposal validates
// against the original owner and is invalidated by owner restart or expiry.
func assertDelegatedBindingAndExpiry(t *testing.T, dir, id string, owner DelegatedOwner, proposal DelegatedProposal) {
	t.Helper()
	restored, err := LoadDelegatedProposal(dir, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateDelegatedProposal(restored, owner, "requester", "backend", "target", proposal.Proposal.CreatedAt); err != nil {
		t.Fatalf("persisted binding rejected: %v", err)
	}
	restarted := owner
	restarted.ProxyInstanceID = strings.Repeat("b", 32)
	if err := ValidateDelegatedProposal(restored, restarted, "requester", "backend", "target", proposal.Proposal.CreatedAt); !errors.Is(err, ErrDelegatedProposalBinding) {
		t.Fatalf("owner restart did not invalidate review: %v", err)
	}
	if err := ValidateDelegatedProposal(restored, owner, "requester", "backend", "target", proposal.ExpiresAt); !errors.Is(err, ErrDelegatedProposalExpired) {
		t.Fatalf("expired persisted review accepted: %v", err)
	}
}

// assertDelegatedListAndDuplicate checks listing returns the proposal and a
// duplicate save is rejected.
func assertDelegatedListAndDuplicate(t *testing.T, dir, id string, proposal DelegatedProposal) {
	t.Helper()
	ids, err := ListDelegatedProposals(dir)
	if err != nil || len(ids) != 1 || ids[0] != id {
		t.Fatalf("list = %v, %v", ids, err)
	}
	if err := SaveDelegatedProposal(dir, proposal); !errors.Is(err, ErrProposalStore) {
		t.Fatalf("duplicate record overwritten: %v", err)
	}
}

// assertDelegatedIsolationFromLocalAPIs checks local proposal APIs cannot read,
// apply, delete, or save into a delegated-only directory.
func assertDelegatedIsolationFromLocalAPIs(t *testing.T, dir, id string, proposal DelegatedProposal) {
	t.Helper()
	if _, err := LoadStoredProposal(dir, id); !errors.Is(err, ErrProposalNotFound) {
		t.Fatalf("delegated review exposed as a local proposal: %v", err)
	}
	if _, err := ApplyStoredProposal(dir, id, "", "", "", "", 1); !errors.Is(err, ErrProposalNotFound) {
		t.Fatalf("delegated review accepted by local apply: %v", err)
	}
	if err := DeleteProposal(dir, id); !errors.Is(err, ErrProposalNotFound) {
		t.Fatalf("local API deleted delegated review: %v", err)
	}
	if err := SaveProposal(dir, proposal.Proposal); !errors.Is(err, ErrProposalStore) {
		t.Fatalf("mixed local/delegated directory accepted: %v", err)
	}
}

// assertDelegatedDeleteAndInvalidID checks deletion succeeds and invalid IDs
// are rejected.
func assertDelegatedDeleteAndInvalidID(t *testing.T, dir, id string) {
	t.Helper()
	if err := DeleteDelegatedProposal(dir, id); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadDelegatedProposal(dir, id); !errors.Is(err, ErrProposalNotFound) {
		t.Fatalf("deleted review still available: %v", err)
	}
	if err := DeleteDelegatedProposal(dir, "../config.yaml"); !errors.Is(err, ErrProposalNotFound) {
		t.Fatalf("invalid ID accepted: %v", err)
	}
}

// assertDelegatedDirectoryMixingRejected checks a local proposal in the same
// directory is not treated as a delegated review.
func assertDelegatedDirectoryMixingRejected(t *testing.T, dir, id string, proposal DelegatedProposal) {
	t.Helper()
	if err := SaveProposal(dir, proposal.Proposal); err != nil {
		t.Fatal(err)
	}
	if err := SaveDelegatedProposal(dir, proposal); !errors.Is(err, ErrProposalStore) {
		t.Fatalf("mixed delegated/local directory accepted: %v", err)
	}
	if _, err := LoadDelegatedProposal(dir, id); !errors.Is(err, ErrProposalNotFound) {
		t.Fatalf("local proposal exposed as delegated review: %v", err)
	}
	if err := DeleteDelegatedProposal(dir, id); !errors.Is(err, ErrProposalNotFound) {
		t.Fatalf("delegated API deleted local proposal: %v", err)
	}
}

func TestDelegatedProposalStoreRejectsInvalidRecords(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	_, proposal := delegatedFixture(t)
	id := proposal.Proposal.ID
	path := filepath.Join(dir, delegatedProposalFilename(id))
	encoded, err := json.Marshal(proposal)
	if err != nil {
		t.Fatal(err)
	}
	tampered := proposal
	tampered.RequesterNode = "other"
	tamperedData, err := json.Marshal(tampered)
	if err != nil {
		t.Fatal(err)
	}
	otherID := proposal
	otherID.Proposal.ID = strings.Repeat("b", 32)
	otherID.ReviewRevision, err = delegatedReviewRevision(otherID)
	if err != nil {
		t.Fatal(err)
	}
	otherData, err := json.Marshal(otherID)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name string
		data []byte
	}{
		{"tampered binding", tamperedData},
		{"wrong ID", otherData},
		{"unknown field", []byte(strings.TrimSuffix(string(encoded), "}") + `,"extra":true}`)},
		{"trailing JSON", append(append([]byte(nil), encoded...), []byte(` {}`)...)},
		{"oversized", []byte(strings.Repeat("x", maxProposalBytes+1))},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := os.WriteFile(path, tt.data, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadDelegatedProposal(dir, id); !errors.Is(err, ErrProposalStore) {
				t.Fatalf("invalid record accepted: %v", err)
			}
		})
	}
	if err := os.WriteFile(path, encoded, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadDelegatedProposal(dir, id); !errors.Is(err, ErrProposalStore) {
		t.Fatalf("insecure record accepted: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(t.TempDir(), "target"), path); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadDelegatedProposal(dir, id); !errors.Is(err, ErrProposalStore) {
		t.Fatalf("symlink accepted: %v", err)
	}
	if err := SaveDelegatedProposal(t.TempDir(), tampered); !errors.Is(err, ErrProposalInvalid) {
		t.Fatalf("invalid binding saved: %v", err)
	}
}

func TestDelegatedProposalStoreBounds(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	_, proposal := delegatedFixture(t)
	for i := range MaxProposals {
		id := strings.Repeat("0", 28) + hexID(i)
		if err := os.WriteFile(filepath.Join(dir, delegatedProposalFilename(id)), []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := SaveDelegatedProposal(dir, proposal); !errors.Is(err, ErrProposalStoreFull) {
		t.Fatalf("full store accepted proposal: %v", err)
	}
	ids, err := ListDelegatedProposals(dir)
	if err != nil || len(ids) != MaxProposals {
		t.Fatalf("full store lost records: %d, %v", len(ids), err)
	}
}

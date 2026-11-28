//go:build linux

package modelconfig

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestProposalStorePersistsUntilDeleted(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	data := []byte("secret: confidential\nmodels: {m: {cmd: 'llama-server -m /private/model.gguf -c 1024'}}")
	size := int32(4096)
	settings := CommandSettings{ContextSize: &size}
	preview, err := PreviewDocument("local", "m", data, settings)
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := NewProposal("/target", "config.yaml", preview, settings)
	if err != nil {
		t.Fatal(err)
	}
	if err := SaveProposal(dir, proposal); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, proposalFilename(proposal.ID))
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("proposal file permissions: %v %v", info, err)
	}
	disk, err := os.ReadFile(path)
	if err != nil || strings.Contains(string(disk), "confidential") || strings.Contains(string(disk), "/private/") {
		t.Fatalf("proposal leaked source or could not be read: %v", err)
	}
	got, err := LoadProposal(dir, proposal.ID)
	if err != nil {
		t.Fatal(err)
	}
	generic, err := LoadStoredProposal(dir, proposal.ID)
	if _, ok := generic.(Proposal); err != nil || !ok {
		t.Fatalf("shared store returned wrong proposal type: %#v %v", generic, err)
	}
	expectedJSON, _ := json.Marshal(proposal)
	actualJSON, _ := json.Marshal(got)
	if string(expectedJSON) != string(actualJSON) {
		t.Fatalf("proposal changed across reads: %s", actualJSON)
	}
	ids, err := ListProposals(dir)
	if err != nil || len(ids) != 1 || ids[0] != proposal.ID {
		t.Fatalf("list = %v, %v", ids, err)
	}
	if err := DeleteProposal(dir, proposal.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadProposal(dir, proposal.ID); !errors.Is(err, ErrProposalNotFound) {
		t.Fatalf("deleted proposal still available: %v", err)
	}
	if err := DeleteProposal(dir, "../config.yaml"); !errors.Is(err, ErrProposalNotFound) {
		t.Fatalf("invalid ID accepted: %v", err)
	}
	if err := os.WriteFile(path, []byte(`{"id":"`+proposal.ID+`"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadProposal(dir, proposal.ID); !errors.Is(err, ErrProposalStore) {
		t.Fatalf("incomplete record accepted: %v", err)
	}
	if err := DeleteProposal(dir, proposal.ID); err != nil {
		t.Fatal(err)
	}
}

func TestRevalidateProposalTarget(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	source := []byte("# secret: confidential\nmodels: {m: {cmd: 'llama-server -c 1024'}}\n")
	if err := os.WriteFile(path, source, 0o600); err != nil {
		t.Fatal(err)
	}
	size := int32(4096)
	settings := CommandSettings{ContextSize: &size}
	preview, err := PreviewTarget(dir, "config.yaml", "local", "m", settings)
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := NewProposal(dir, "config.yaml", preview, settings)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := RevalidateProposalTarget(dir, "config.yaml", "local", proposal)
	if err != nil || string(verified.candidate) != strings.Replace(string(source), "-c 1024", "-c 4096", 1) {
		t.Fatalf("revalidation failed: %v", err)
	}
	for _, test := range []struct {
		directory string
		filename  string
		targetID  string
	}{
		{t.TempDir(), "config.yaml", "local"},
		{dir, "other.yaml", "local"},
		{dir, "config.yaml", "other"},
	} {
		if _, err := RevalidateProposalTarget(test.directory, test.filename, test.targetID, proposal); !errors.Is(err, ErrProposalTarget) {
			t.Fatalf("changed target accepted: %v", err)
		}
	}
	tampered := proposal
	tampered.Changes = append([]SettingChange(nil), proposal.Changes...)
	tampered.Changes[0].Before = "512"
	if _, err := RevalidateProposalTarget(dir, "config.yaml", "local", tampered); !errors.Is(err, ErrProposalReview) {
		t.Fatalf("changed review accepted: %v", err)
	}
	tampered.Changes[0].After = "bad"
	if _, err := RevalidateProposalTarget(dir, "config.yaml", "local", tampered); !errors.Is(err, ErrProposalInvalid) {
		t.Fatalf("invalid proposal accepted: %v", err)
	}
	if err := os.WriteFile(path, append(append([]byte(nil), source...), []byte("# external edit\n")...), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := RevalidateProposalTarget(dir, "config.yaml", "local", proposal); !errors.Is(err, ErrProposalStale) {
		t.Fatalf("stale source accepted: %v", err)
	}
	current, err := os.ReadFile(path)
	if err != nil || string(current) != string(source)+"# external edit\n" {
		t.Fatalf("revalidation modified the target: %v", err)
	}
}

func TestProposalStoreBoundaries(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := ListProposals(dir); !errors.Is(err, ErrProposalStore) {
		t.Fatalf("insecure directory accepted: %v", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	id := strings.Repeat("a", 32)
	if err := os.Symlink(filepath.Join(t.TempDir(), "secret"), filepath.Join(dir, proposalFilename(id))); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadProposal(dir, id); !errors.Is(err, ErrProposalStore) {
		t.Fatalf("symlink accepted: %v", err)
	}
	if err := DeleteProposal(dir, id); err != nil {
		t.Fatal(err)
	}
	linked := filepath.Join(t.TempDir(), "other")
	if err := os.WriteFile(linked, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(linked, filepath.Join(dir, proposalFilename(id))); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadProposal(dir, id); !errors.Is(err, ErrProposalStore) {
		t.Fatalf("hard-linked proposal accepted: %v", err)
	}
	if err := DeleteProposal(dir, id); err != nil {
		t.Fatal(err)
	}
	size := int32(32)
	settings := CommandSettings{ContextSize: &size}
	preview, err := PreviewDocument("local", "m", []byte("models: {m: {cmd: 'llama-server -c 16'}}"), settings)
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewProposal("/target", "config.yaml", preview, settings)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < MaxProposals; i++ {
		p.ID = strings.Repeat("0", 28) + hexID(i)
		if err := SaveProposal(dir, p); err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
	}
	p.ID = strings.Repeat("f", 32)
	if err := SaveProposal(dir, p); !errors.Is(err, ErrProposalStoreFull) {
		t.Fatalf("full store accepted proposal: %v", err)
	}
	ids, err := ListProposals(dir)
	if err != nil || len(ids) != MaxProposals {
		t.Fatalf("full store lost proposals: %d, %v", len(ids), err)
	}
}

func TestProposalStoreConcurrentSavesRemainConsistent(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	size := int32(32)
	settings := CommandSettings{ContextSize: &size}
	preview, err := PreviewDocument("local", "m", []byte("models: {m: {cmd: 'llama-server -c 16'}}"), settings)
	if err != nil {
		t.Fatal(err)
	}
	const workers = 12
	results := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, err := NewProposal("/target", "config.yaml", preview, settings)
			if err == nil {
				err = SaveProposal(dir, p)
			}
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		} else if !errors.Is(err, ErrProposalStore) {
			t.Fatalf("unexpected save error: %v", err)
		}
	}
	ids, err := ListProposals(dir)
	if err != nil || successes == 0 || len(ids) != successes {
		t.Fatalf("saved=%d listed=%d error=%v", successes, len(ids), err)
	}
	for _, id := range ids {
		if _, err := LoadProposal(dir, id); err != nil {
			t.Fatalf("concurrent save produced invalid record: %v", err)
		}
	}
}

func hexID(n int) string {
	const digits = "0123456789abcdef"
	return string([]byte{digits[(n>>12)&15], digits[(n>>8)&15], digits[(n>>4)&15], digits[n&15]})
}

//go:build linux

package modelconfig

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func backupFixture(t *testing.T, source []byte) (Proposal, backupRecord) {
	t.Helper()
	size := int32(4096)
	settings := CommandSettings{ContextSize: &size}
	preview, err := PreviewDocument("local", "m", source, settings)
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := NewProposal("/target", "config.yaml", preview, settings)
	if err != nil {
		t.Fatal(err)
	}
	record, err := prepareBackup("/target", "config.yaml", "local", proposal, source)
	if err != nil {
		t.Fatal(err)
	}
	return proposal, record
}

func TestBackupRetentionAndRollbackPreconditions(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	source := []byte("# secret: confidential\nmodels: {m: {cmd: 'llama-server -c 1024'}}\n")
	proposal, first := backupFixture(t, source)
	first.Info.CreatedAt = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	second := first
	second.Info.ID = strings.Repeat("b", 32)
	second.Info.CreatedAt = first.Info.CreatedAt.Add(time.Minute)
	third := first
	third.Info.ID = strings.Repeat("c", 32)
	third.Info.CreatedAt = second.Info.CreatedAt.Add(time.Minute)
	for _, record := range []backupRecord{first, second, third} {
		if err := saveBackup(dir, record, 2); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := loadBackup(dir, first.Info.ID); !errors.Is(err, errBackupStore) {
		t.Fatalf("oldest backup was not rotated: %v", err)
	}
	assertRetainedBackups(t, dir, source, second.Info, third.Info)
	preview, err := PreviewDocument("local", "m", source, proposal.Settings)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := prepareRollback("/target", "config.yaml", "local", third, preview.candidate)
	if err != nil || string(restored) != string(source) {
		t.Fatalf("rollback source = %q, %v", restored, err)
	}
	restored[0] = 'x'
	if string(third.Source) != string(source) {
		t.Fatal("rollback result changed retained backup")
	}
	if _, err := prepareRollback("/target", "config.yaml", "local", third, source); !errors.Is(err, errRollbackStale) {
		t.Fatalf("changed current config accepted: %v", err)
	}
	if _, err := prepareRollback("/other", "config.yaml", "local", third, preview.candidate); !errors.Is(err, ErrProposalTarget) {
		t.Fatalf("changed target accepted: %v", err)
	}
}

// assertRetainedBackups verifies that the two newest backups survive rotation,
// are intact, are permission-restricted, and do not leak source metadata in
// their serialized info. Extracting this phase keeps the parent test's
// cognitive complexity under the Sonar threshold.
func assertRetainedBackups(t *testing.T, dir string, source []byte, second, third backupInfo) {
	t.Helper()
	for _, info := range []backupInfo{second, third} {
		loaded, err := loadBackup(dir, info.ID)
		if err != nil || string(loaded.Source) != string(source) || loaded.Info != info {
			t.Fatalf("retained backup could not be restored: %v", err)
		}
		path := filepath.Join(dir, backupFilename(info.ID))
		stat, err := os.Stat(path)
		if err != nil || stat.Mode().Perm() != 0o600 {
			t.Fatalf("backup permissions: %v %v", stat, err)
		}
		public, err := json.Marshal(loaded.Info)
		if err != nil || strings.Contains(string(public), "confidential") {
			t.Fatalf("backup metadata exposed source: %s %v", public, err)
		}
	}
}

func TestBackupFailsClosedBeforeRotation(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	source := []byte("models: {m: {cmd: 'llama-server -c 1024'}}\n")
	proposal, first := backupFixture(t, source)
	if err := saveBackup(dir, first, 1); err != nil {
		t.Fatal(err)
	}
	_, next := backupFixture(t, source)
	changed := append(append([]byte(nil), source...), []byte("# modified\n")...)
	if _, err := prepareBackup("/target", "config.yaml", "local", proposal, changed); !errors.Is(err, ErrProposalStale) {
		t.Fatalf("stale proposal accepted: %v", err)
	}
	if _, err := prepareBackup("/target", "other.yaml", "local", proposal, source); !errors.Is(err, ErrProposalTarget) {
		t.Fatalf("wrong target accepted: %v", err)
	}
	tampered := proposal
	tampered.Changes = append([]SettingChange(nil), proposal.Changes...)
	tampered.Changes[0].Before = "512"
	if _, err := prepareBackup("/target", "config.yaml", "local", tampered, source); !errors.Is(err, ErrProposalReview) {
		t.Fatalf("changed review accepted: %v", err)
	}
	path := filepath.Join(dir, backupFilename(first.Info.ID))
	if err := os.WriteFile(path, []byte(`{"broken":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := saveBackup(dir, next, 1); !errors.Is(err, errBackupStore) {
		t.Fatalf("corrupt backup accepted during rotation: %v", err)
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("corrupt prior backup removed: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(dir, backupFilename(next.Info.ID))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("new backup created despite failure: %v", err)
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := saveBackup(dir, next, 1); !errors.Is(err, errBackupStore) {
		t.Fatalf("insecure backup directory accepted: %v", err)
	}
}

func TestBackupRejectsUnsafeRecords(t *testing.T) {
	dir, other := t.TempDir(), t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	_, backup := backupFixture(t, []byte("models: {m: {cmd: 'llama-server -c 1024'}}"))
	target := filepath.Join(other, "secret")
	if err := os.WriteFile(target, []byte("private"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, backupFilename(backup.Info.ID))
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := loadBackup(dir, backup.Info.ID); !errors.Is(err, errBackupStore) {
		t.Fatalf("symlink accepted: %v", err)
	}
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := loadBackup(dir, backup.Info.ID); !errors.Is(err, errBackupStore) {
		t.Fatalf("hard link accepted: %v", err)
	}
	if data, err := os.ReadFile(target); err != nil || string(data) != "private" {
		t.Fatalf("unsafe target was modified: %v", err)
	}
}

func TestBackupKeepsOldRecordWhenNewWriteFails(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can write to a non-writable directory")
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	source := []byte("models: {m: {cmd: 'llama-server -c 1024'}}")
	_, first := backupFixture(t, source)
	_, next := backupFixture(t, source)
	if err := saveBackup(dir, first, 1); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	if err := saveBackup(dir, next, 1); !errors.Is(err, errBackupStore) {
		t.Fatalf("write unexpectedly succeeded: %v", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := loadBackup(dir, first.Info.ID); err != nil {
		t.Fatalf("old backup lost after write failure: %v", err)
	}
}

func TestBackupConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	source := []byte("models: {m: {cmd: 'llama-server -c 1024'}}")
	_, initial := backupFixture(t, source)
	const workers = 12
	results := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		backup := initial
		backup.Info.ID = strings.Repeat("0", 28) + hexID(i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- saveBackup(dir, backup, maxBackupLimit)
		}()
	}
	wg.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		} else if !errors.Is(err, errBackupStore) {
			t.Fatalf("unexpected backup error: %v", err)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil || successes == 0 || len(entries) != successes {
		t.Fatalf("saved=%d entries=%d error=%v", successes, len(entries), err)
	}
	for _, entry := range entries {
		id := strings.TrimSuffix(strings.TrimPrefix(entry.Name(), "backup-"), ".json")
		if _, err := loadBackup(dir, id); err != nil {
			t.Fatalf("concurrent write produced invalid backup: %v", err)
		}
	}
}

func TestBackupReadableWithRestrictiveUmask(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	_, backup := backupFixture(t, []byte("models: {m: {cmd: 'llama-server -c 1024'}}"))
	previous := syscall.Umask(0o777)
	defer syscall.Umask(previous)
	if err := saveBackup(dir, backup, 1); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(dir, backupFilename(backup.Info.ID)))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("unexpected backup permissions: %v %v", info, err)
	}
	if _, err := loadBackup(dir, backup.Info.ID); err != nil {
		t.Fatalf("backup is not readable under a restrictive umask: %v", err)
	}
}

func TestBackupSupportsMaximumConfigSize(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}

	prefix := "models: {m: {cmd: 'llama-server -c 1024'}}\n#"
	source := []byte(prefix + strings.Repeat("x", maxConfigBytes-len(prefix)))
	_, record := backupFixture(t, source)
	if err := saveBackup(dir, record, 1); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadBackup(dir, record.Info.ID)
	if err != nil || string(loaded.Source) != string(source) {
		t.Fatalf("maximum-sized backup failed: %v", err)
	}
}

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

func TestBackupRecoveryListsInvalidAndOverLimitWithoutSource(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	source := []byte("# confidential\nmodels: {m: {cmd: 'llama-server -c 1024'}}")
	_, valid := backupFixture(t, source)
	if err := saveBackup(dir, valid, 1); err != nil {
		t.Fatal(err)
	}
	invalidID := strings.Repeat("f", 32)
	if err := os.WriteFile(filepath.Join(dir, backupFilename(invalidID)), []byte("confidential malformed backup"), 0o600); err != nil {
		t.Fatal(err)
	}
	summaries, err := ListBackups(dir)
	if err != nil || len(summaries) != 2 {
		t.Fatalf("list over-limit store: %v", err)
	}
	var validSeen, invalidSeen bool
	for _, summary := range summaries {
		switch summary.ID {
		case valid.Info.ID:
			validSeen = summary.Status == "valid" && summary.Info != nil && *summary.Info == valid.Info
		case invalidID:
			invalidSeen = summary.Status == "unreadable_or_invalid" && summary.Info == nil
		}
	}
	data, err := json.Marshal(summaries)
	if err != nil || strings.Contains(string(data), "confidential") || !validSeen || !invalidSeen {
		t.Fatalf("invalid or unredacted listing: %v", err)
	}
	info, err := ReadBackupInfo(dir, valid.Info.ID)
	if err != nil || info != valid.Info {
		t.Fatalf("read metadata: %v", err)
	}
	if _, err := ReadBackupInfo(dir, invalidID); !errors.Is(err, ErrBackupStore) {
		t.Fatalf("invalid record accepted: %v", err)
	}
	if err := DeleteBackup(dir, invalidID); err != nil {
		t.Fatalf("remove invalid record: %v", err)
	}
	if _, err := ReadBackupInfo(dir, invalidID); !errors.Is(err, ErrBackupNotFound) {
		t.Fatalf("deleted record exists: %v", err)
	}
	if _, err := loadBackup(dir, valid.Info.ID); err != nil {
		t.Fatalf("valid backup lost during recovery: %v", err)
	}
}

func TestBackupRecoveryRejectsUnsafeNamesAndProtectsLinkTargets(t *testing.T) {
	dir, other := t.TempDir(), t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	external := filepath.Join(other, "private")
	if err := os.WriteFile(external, []byte("private"), 0o600); err != nil {
		t.Fatal(err)
	}
	id := strings.Repeat("a", 32)
	path := filepath.Join(dir, backupFilename(id))
	if err := os.Symlink(external, path); err != nil {
		t.Fatal(err)
	}
	list, err := ListBackups(dir)
	if err != nil || len(list) != 1 || list[0].Status != "unreadable_or_invalid" {
		t.Fatalf("unsafe link listing: %v", err)
	}
	if err := DeleteBackup(dir, id); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(external)
	if err != nil || string(data) != "private" {
		t.Fatalf("link target changed: %v", err)
	}
	if err := os.Link(external, path); err != nil {
		t.Fatal(err)
	}
	if err := DeleteBackup(dir, id); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(external); err != nil || string(data) != "private" {
		t.Fatalf("hard-link target changed: %v", err)
	}
	if err := DeleteBackup(dir, "../private"); !errors.Is(err, ErrBackupNotFound) {
		t.Fatalf("unsafe ID accepted: %v", err)
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := DeleteBackup(dir, id); !errors.Is(err, ErrBackupStore) {
		t.Fatalf("directory removed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "unexpected-secret"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ListBackups(dir); !errors.Is(err, ErrBackupStore) {
		t.Fatalf("unexpected store entry accepted: %v", err)
	}
}

func TestBackupRecoveryRespectsStoreLock(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	_, backup := backupFixture(t, []byte("models: {m: {cmd: 'llama-server -c 1024'}}"))
	if err := saveBackup(dir, backup, 1); err != nil {
		t.Fatal(err)
	}
	err := withPrivateStore(dir, ErrBackupStore, func(_ *os.Root, _ *os.File) error {
		if _, err := ListBackups(dir); !errors.Is(err, ErrBackupStore) {
			t.Fatalf("list ignored store lock: %v", err)
		}
		if err := DeleteBackup(dir, backup.Info.ID); !errors.Is(err, ErrBackupStore) {
			t.Fatalf("delete ignored store lock: %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loadBackup(dir, backup.Info.ID); err != nil {
		t.Fatalf("locked backup was removed: %v", err)
	}
}

func TestBackupRecoveryBoundsStoreListing(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < MaxBackups+2; i++ {
		id := strings.Repeat("0", 28) + hexID(i)
		if err := os.WriteFile(filepath.Join(dir, backupFilename(id)), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := ListBackups(dir); !errors.Is(err, ErrBackupStore) {
		t.Fatalf("oversized store listing accepted: %v", err)
	}
	id := strings.Repeat("0", 28) + hexID(MaxBackups+1)
	if err := DeleteBackup(dir, id); err != nil {
		t.Fatal(err)
	}
	list, err := ListBackups(dir)
	if err != nil || len(list) != MaxBackups+1 {
		t.Fatalf("recovered oversized store: %d, %v", len(list), err)
	}
}

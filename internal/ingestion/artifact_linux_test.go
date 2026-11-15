//go:build linux

package ingestion

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func recordedArtifactManager(t *testing.T) (*JobManager, string, string, []byte) {
	t.Helper()
	models := t.TempDir()
	manager, err := NewPersistentJobManager(context.Background(), "", privateRecordDirectory(t), 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := manager.Stop(); err != nil {
			t.Error(err)
		}
	})
	data := ggufHeader(3, 0, 0)
	manager.run = func(ctx context.Context, options StageOptions, _ time.Duration, _ string) (StageResult, error) {
		return StageNewArtifact(ctx, options, bytes.NewReader(data))
	}
	job, err := manager.Start(stageOptions(models), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	manager.wg.Wait()
	status, err := manager.Status(job.ID)
	if err != nil || status.State != "succeeded" {
		t.Fatalf("fixture ingestion failed: %#v, %v", status, err)
	}
	return manager, models, job.ID, data
}

func TestCheckRecordedArtifactObservesWithoutClaimingIdentity(t *testing.T) {
	manager, models, id, data := recordedArtifactManager(t)
	first, err := manager.CheckRecordedArtifact(models, id)
	if err != nil {
		t.Fatal(err)
	}
	if first.RecordID != id || first.Bytes != int64(len(data)) || first.Presence != "regular_file_when_checked" ||
		first.RecordMatch != "unknown" || first.Integrity != "not_verified" || first.BackendAccess != "unknown" {
		t.Fatalf("unexpected evidence: %#v", first)
	}
	again, err := manager.CheckRecordedArtifact(models, id)
	if err != nil || first.Revision != again.Revision {
		t.Fatalf("unchanged observation differed: %#v, %v", again, err)
	}
	encoded, err := json.Marshal(first)
	if err != nil || strings.Contains(string(encoded), models) {
		t.Fatalf("evidence disclosed directory: %s, %v", encoded, err)
	}
	path := filepath.Join(models, first.Source.Filename)
	replacement := filepath.Join(models, "replacement")
	if err := os.WriteFile(replacement, bytes.Repeat([]byte{1}, len(data)), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, path); err != nil {
		t.Fatal(err)
	}
	changed, err := manager.CheckRecordedArtifact(models, id)
	if err != nil || changed.Revision == first.Revision || changed.RecordMatch != "unknown" {
		t.Fatalf("replacement was not distinguishable: %#v, %v", changed, err)
	}
	if err := os.WriteFile(path, []byte("different size"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.CheckRecordedArtifact(models, id); !errors.Is(err, ErrArtifactChanged) {
		t.Fatalf("size mismatch accepted: %v", err)
	}
}

func TestCheckRecordedArtifactRejectsUnsafeObjects(t *testing.T) {
	for _, kind := range []string{"missing", "symlink", "fifo", "hardlink", "writable", "directory symlink"} {
		t.Run(kind, func(t *testing.T) {
			manager, models, id, _ := recordedArtifactManager(t)
			path := filepath.Join(models, "model.gguf")
			switch kind {
			case "missing":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			case "symlink", "fifo":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if kind == "symlink" {
					if err := os.Symlink(filepath.Join(t.TempDir(), "missing"), path); err != nil {
						t.Fatal(err)
					}
				} else if err := unix.Mkfifo(path, 0o600); err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				if err := os.Link(path, filepath.Join(models, "other")); err != nil {
					t.Fatal(err)
				}
			case "writable":
				if err := os.Chmod(path, 0o660); err != nil {
					t.Fatal(err)
				}
			case "directory symlink":
				alias := filepath.Join(t.TempDir(), "alias")
				if err := os.Symlink(models, alias); err != nil {
					t.Fatal(err)
				}
				models = alias
			}
			if _, err := manager.CheckRecordedArtifact(models, id); err == nil {
				t.Fatal("unsafe artifact accepted")
			}
		})
	}
}

func TestCheckRecordedArtifactRejectsFailedRecord(t *testing.T) {
	models := t.TempDir()
	manager, err := NewPersistentJobManager(context.Background(), "", privateRecordDirectory(t), 4)
	if err != nil {
		t.Fatal(err)
	}

	defer manager.Stop()
	job := recordJob(1)
	if err := manager.store.Begin(job); err != nil {
		t.Fatal(err)
	}
	if err := manager.store.Finish(finishRecord(job)); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.CheckRecordedArtifact(models, job.ID); !errors.Is(err, ErrArtifactRecord) {
		t.Fatalf("failed record accepted: %v", err)
	}
}

func TestCheckRecordedArtifactRejectsBusyDirectory(t *testing.T) {
	manager, models, id, _ := recordedArtifactManager(t)
	dir, err := os.Open(models)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	if err := unix.Flock(int(dir.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	defer unix.Flock(int(dir.Fd()), unix.LOCK_UN)
	if _, err := manager.CheckRecordedArtifact(models, id); !errors.Is(err, ErrArtifactBusy) {
		t.Fatalf("busy directory accepted: %v", err)
	}
}

func TestCheckRecordedArtifactRejectsEvictedRecord(t *testing.T) {
	manager, models, id, _ := recordedArtifactManager(t)
	for n := 1; n <= 4; n++ {
		job := recordJob(n)
		job.StartedAt = time.Now().Add(time.Duration(n) * time.Hour)
		if err := manager.store.Begin(job); err != nil {
			t.Fatal(err)
		}

		if err := manager.store.Finish(finishRecord(job)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := manager.CheckRecordedArtifact(models, id); !errors.Is(err, ErrRecordNotFound) {
		t.Fatalf("evicted record accepted: %v", err)
	}
}

func TestWithRecordedArtifactHoldsDirectoryLockAndRechecks(t *testing.T) {
	manager, models, id, data := recordedArtifactManager(t)
	err := manager.WithRecordedArtifact(models, id, func(first ArtifactEvidence, recheck func() (ArtifactEvidence, error)) error {
		dir, err := os.Open(models)
		if err != nil {
			return err
		}
		defer dir.Close()
		if err := unix.Flock(int(dir.Fd()), unix.LOCK_EX|unix.LOCK_NB); err == nil {
			_ = unix.Flock(int(dir.Fd()), unix.LOCK_UN)
			t.Fatal("artifact operation did not retain the directory lock")
		}
		again, err := recheck()
		if err != nil || again.Revision != first.Revision {
			t.Fatalf("stable artifact recheck failed: %#v %v", again, err)
		}
		replacement := filepath.Join(models, "replacement")
		if err := os.WriteFile(replacement, bytes.Repeat([]byte{1}, len(data)), 0o600); err != nil {
			return err
		}
		if err := os.Rename(replacement, filepath.Join(models, first.Source.Filename)); err != nil {
			return err
		}
		changed, err := recheck()
		if err != nil || changed.Revision == first.Revision {
			t.Fatalf("recheck missed a same-size replacement: %#v %v", changed, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

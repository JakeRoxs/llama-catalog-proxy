//go:build linux

package ingestion

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAdoptExistingArtifactPersistsLocalEvidence(t *testing.T) {
	dir, records := t.TempDir(), privateRecordDirectory(t)
	data := ggufHeader(3, 0, 0)
	path := filepath.Join(dir, "model.gguf")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	source, err := LocalSource("model.gguf")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewPersistentJobManager(context.Background(), "", records, 4)
	if err != nil {
		t.Fatal(err)
	}
	options := StageOptions{Directory: dir, Source: source, MaxBytes: int64(len(data)), FileMode: 0o600,
		AdoptExisting: true, AllowAdoption: true}
	job, err := manager.Start(options, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	manager.wg.Wait()
	status, err := manager.Status(job.ID)
	if err != nil || status.State != "succeeded" || status.Operation != "adopt" ||
		status.Source.Kind != "local" || status.Source.Repository != "" || status.Source.Revision != "" ||
		status.Result.Publication != PublicationExisting || status.Result.Durability != "not_applicable" ||
		status.Result.Integrity != "calculated_unverified" || status.Result.Metadata == nil {
		t.Fatalf("adoption = %+v, %v", status, err)
	}
	evidence, err := manager.CheckRecordedArtifact(dir, job.ID)
	if err != nil || evidence.Source.Kind != "local" || evidence.RecordMatch != "unknown" || evidence.Integrity != "not_verified" {
		t.Fatalf("adopted evidence = %+v, %v", evidence, err)
	}
	if err := manager.Stop(); err != nil {
		t.Fatal(err)
	}
	manager, err = NewPersistentJobManager(context.Background(), "", records, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Stop()
	record, err := manager.Record(job.ID)
	if err != nil || record.Job.Operation != "adopt" || record.Job.Source.Kind != "local" {
		t.Fatalf("adoption not persisted: %+v, %v", record, err)
	}
	contents, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(contents, data) {
		t.Fatalf("adoption modified artifact: %v", err)
	}
}

func TestAdoptRejectsUnsafeOrInvalidFiles(t *testing.T) {
	for _, kind := range []string{"missing", "symlink", "hardlink", "writable", "invalid", "too_large", "unauthorized"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "model.gguf")
			data := ggufHeader(3, 0, 0)
			if kind == "invalid" {
				data = []byte("not GGUF")
			}
			if kind != "missing" {
				if err := os.WriteFile(path, data, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			switch kind {
			case "symlink":
				other := filepath.Join(dir, "other")
				if err := os.Rename(path, other); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(other, path); err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				if err := os.Link(path, filepath.Join(dir, "other")); err != nil {
					t.Fatal(err)
				}
			case "writable":
				if err := os.Chmod(path, 0o660); err != nil {
					t.Fatal(err)
				}
			}
			source, _ := LocalSource("model.gguf")
			options := StageOptions{Directory: dir, Source: source, MaxBytes: 1024, FileMode: 0o600, AdoptExisting: true, AllowAdoption: true}
			if kind == "too_large" {
				options.MaxBytes = int64(len(data) - 1)
			}
			if kind == "unauthorized" {
				options.AllowAdoption = false
			}
			result, err := adoptArtifact(context.Background(), options, time.Minute)
			if err == nil || result.Publication != PublicationNotPublished {
				t.Fatalf("unsafe adoption accepted: %+v %v", result, err)
			}
		})
	}
	for _, name := range []string{"../model.gguf", "model-00001-of-00002.gguf", "/model.gguf"} {
		if _, err := LocalSource(name); err == nil {
			t.Fatalf("invalid local filename accepted: %q", name)
		}
	}
}

func TestReplacementRequiresConsentAndPublishesOnlyCompleteArtifact(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "model.gguf")
	old := []byte("old model")
	if err := os.WriteFile(path, old, 0o600); err != nil {
		t.Fatal(err)
	}
	options := stageOptions(dir)
	options.ReplaceExisting = true
	data := ggufHeader(3, 0, 0)
	if _, err := StageNewArtifact(context.Background(), options, bytes.NewReader(data)); !errors.Is(err, ErrStageInvalid) {
		t.Fatalf("replacement without consent accepted: %v", err)
	}
	options.AllowReplacement = true
	if _, err := StageNewArtifact(context.Background(), options, bytes.NewReader(data)); !errors.Is(err, ErrStageInvalid) {
		t.Fatalf("replacement without exclusive window accepted: %v", err)
	}
	options.AcknowledgeExclusiveWindow = true
	if result, err := StageNewArtifact(context.Background(), options, bytes.NewReader([]byte("invalid"))); err == nil || result.Publication != PublicationNotPublished {
		t.Fatalf("invalid replacement accepted: %+v %v", result, err)
	}
	unchanged, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(unchanged, old) {
		t.Fatalf("failed replacement changed existing file: %v", err)
	}
	result, err := StageNewArtifact(context.Background(), options, bytes.NewReader(data))
	if err != nil || result.Publication != PublicationPublished || result.Durability != "confirmed" {
		t.Fatalf("replacement failed: %+v %v", result, err)
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("replacement content = %x, %v", got, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("replacement left temporary file: %v %v", entries, err)
	}
}

func TestReplacementJobPersistsPublication(t *testing.T) {
	dir, records := t.TempDir(), privateRecordDirectory(t)
	path := filepath.Join(dir, "model.gguf")
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	data := ggufHeader(3, 0, 0)
	manager, err := NewPersistentJobManager(context.Background(), "", records, 4)
	if err != nil {
		t.Fatal(err)
	}
	manager.run = func(ctx context.Context, options StageOptions, _ time.Duration, _ string) (StageResult, error) {
		return StageNewArtifact(ctx, options, bytes.NewReader(data))
	}
	options := stageOptions(dir)
	options.ReplaceExisting, options.AllowReplacement, options.AcknowledgeExclusiveWindow = true, true, true
	job, err := manager.Start(options, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	manager.wg.Wait()
	record, err := manager.Record(job.ID)
	if err != nil || record.Job.State != "succeeded" || record.Job.Operation != "replace" ||
		record.Job.Result.Publication != PublicationPublished || record.Job.Result.Durability != "confirmed" {
		t.Fatalf("replacement record = %+v %v", record, err)
	}
	if _, err := manager.CheckRecordedArtifact(dir, job.ID); err != nil {
		t.Fatalf("replacement cannot be reviewed: %v", err)
	}
	if err := manager.Stop(); err != nil {
		t.Fatal(err)
	}
	manager, err = NewPersistentJobManager(context.Background(), "", records, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Stop()
	if record, err = manager.Record(job.ID); err != nil || record.Job.Operation != "replace" {
		t.Fatalf("replacement record not persisted: %+v %v", record, err)
	}
}

func TestReplacementDoesNotPublishOnCancellationOrUnsafeDestination(t *testing.T) {
	for _, kind := range []string{"missing", "symlink", "hardlink", "canceled", "canceled_during_copy"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "model.gguf")
			if kind != "missing" {
				if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			switch kind {
			case "symlink":
				other := filepath.Join(dir, "other")
				if err := os.Rename(path, other); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(other, path); err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				if err := os.Link(path, filepath.Join(dir, "other")); err != nil {
					t.Fatal(err)
				}
			}
			options := stageOptions(dir)
			options.ReplaceExisting, options.AllowReplacement, options.AcknowledgeExclusiveWindow = true, true, true
			ctx := context.Background()
			var reader io.Reader = bytes.NewReader(ggufHeader(3, 0, 0))
			if kind == "canceled" || kind == "canceled_during_copy" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				defer cancel()
				if kind == "canceled" {
					cancel()
				} else {
					reader = &cancelingReader{data: ggufHeader(3, 0, 0), cancel: cancel}
				}
			}
			result, err := StageNewArtifact(ctx, options, reader)
			if err == nil || result.Publication != PublicationNotPublished {
				t.Fatalf("unsafe replacement accepted: %+v %v", result, err)
			}
			if kind != "missing" {
				data, err := os.ReadFile(path)
				if err != nil || string(data) != "old" {
					t.Fatalf("existing file modified: %q %v", data, err)
				}
			}
		})
	}
}

func TestReplacementRejectsChangedDestination(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "model.gguf")
	if err := os.WriteFile(path, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	options := stageOptions(dir)
	options.ReplaceExisting, options.AllowReplacement, options.AcknowledgeExclusiveWindow = true, true, true
	reader := &waitingReader{started: make(chan struct{}), release: make(chan struct{}), data: bytes.NewReader(ggufHeader(3, 0, 0))}
	type outcome struct {
		result StageResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := StageNewArtifact(context.Background(), options, reader)
		done <- outcome{result, err}
	}()
	<-reader.started
	other := filepath.Join(dir, "new-file")
	if err := os.WriteFile(other, []byte("changed"), 0o600); err != nil {
		close(reader.release)
		<-done
		t.Fatal(err)
	}
	if err := os.Rename(other, path); err != nil {
		close(reader.release)
		<-done
		t.Fatal(err)
	}
	close(reader.release)
	got := <-done
	if !errors.Is(got.err, ErrArtifactChanged) || got.result.Publication != PublicationNotPublished {
		t.Fatalf("changed destination accepted: %+v %v", got.result, got.err)
	}
	current, err := os.ReadFile(path)
	if err != nil || string(current) != "changed" {
		t.Fatalf("changed destination overwritten: %q %v", current, err)
	}
}

//go:build linux

package ingestion

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func privateRecordDirectory(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func recordJob(n int) JobStatus {
	return JobStatus{
		ID: fmt.Sprintf("ingest-%032x", n), State: "running",
		Source:    Source{Repository: "owner/repo", Filename: "model.gguf", Revision: "main"},
		StartedAt: time.Date(2026, 1, 1, 0, 0, n, 0, time.UTC),
		Result:    StageResult{Publication: PublicationUnknown, Durability: "unknown"},
	}
}

func finishRecord(job JobStatus) Record {
	job.State = "failed"
	job.CompletedAt = job.StartedAt.Add(time.Second)
	job.Result = StageResult{Publication: PublicationNotPublished, Durability: "not_applicable"}
	job.ErrorCodes = []string{"destination_exists"}
	return Record{Version: 1, Job: job}
}

func TestRecordStoreRecoversInterruptedJobAndRequiresAcknowledgment(t *testing.T) {
	dir := privateRecordDirectory(t)
	store, err := openRecordStore(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	job := recordJob(1)
	if err := store.Begin(job); err != nil {
		t.Fatal(err)
	}
	if _, err := openRecordStore(dir, 2); !errors.Is(err, ErrRecordStore) {
		t.Fatalf("concurrent store accepted: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = openRecordStore(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	record, err := store.Get(job.ID)
	if err != nil || record.Job.State != "interrupted" || !record.ReviewRequired || record.Acknowledged ||
		record.Job.Result.Publication != PublicationUnknown || record.Job.Source != job.Source {
		t.Fatalf("recovered = %+v, %v", record, err)
	}
	if err := store.Begin(recordJob(2)); !errors.Is(err, ErrRecordReview) {
		t.Fatalf("started before review: %v", err)
	}
	record, err = store.Acknowledge(job.ID)
	if err != nil || !record.Acknowledged || record.Job.Result.Publication != PublicationUnknown {
		t.Fatalf("acknowledged = %+v, %v", record, err)
	}
	if err := store.Begin(recordJob(2)); err != nil {
		t.Fatal(err)
	}
	if err := store.Finish(finishRecord(recordJob(2))); err != nil {
		t.Fatal(err)
	}
	if err := store.Begin(recordJob(3)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(job.ID); err != nil {
		t.Fatalf("interrupted record was automatically evicted: %v", err)
	}
	if _, err := store.Get(recordJob(2).ID); !errors.Is(err, ErrRecordNotFound) {
		t.Fatalf("oldest completed record retained: %v", err)
	}
}

func TestRecordStoreEvictsOldestCompletedOnly(t *testing.T) {
	dir := privateRecordDirectory(t)
	store, err := openRecordStore(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for n := 1; n <= 3; n++ {
		job := recordJob(n)
		if err := store.Begin(job); err != nil {
			t.Fatal(err)
		}
		if err := store.Finish(finishRecord(job)); err != nil {
			t.Fatal(err)
		}
	}
	records, err := store.List()
	if err != nil || len(records) != 2 || records[0].Job.ID != recordJob(2).ID || records[1].Job.ID != recordJob(3).ID {
		t.Fatalf("retained records = %+v, %v", records, err)
	}
	if _, err := store.Get(recordJob(1).ID); !errors.Is(err, ErrRecordNotFound) {
		t.Fatalf("oldest record retained: %v", err)
	}
}

func TestRecordStoreRejectsUnsafeEntriesAndPermissions(t *testing.T) {
	t.Run("directory permissions", func(t *testing.T) {
		dir := privateRecordDirectory(t)
		if err := os.Chmod(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if _, err := openRecordStore(dir, 10); !errors.Is(err, ErrRecordStore) {
			t.Fatalf("unsafe directory accepted: %v", err)
		}
	})
	t.Run("unexpected temporary record", func(t *testing.T) {
		dir := privateRecordDirectory(t)
		if err := os.WriteFile(filepath.Join(dir, ".record-interrupted.tmp"), []byte("partial"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := openRecordStore(dir, 10); !errors.Is(err, ErrRecordStore) {
			t.Fatalf("unexpected file accepted: %v", err)
		}
	})
	t.Run("symlink record", func(t *testing.T) {
		dir := privateRecordDirectory(t)
		if err := os.Symlink(filepath.Join(t.TempDir(), "missing"), filepath.Join(dir, recordJob(1).ID+".json")); err != nil {
			t.Fatal(err)
		}
		if _, err := openRecordStore(dir, 10); !errors.Is(err, ErrRecordStore) {
			t.Fatalf("symlink record accepted: %v", err)
		}
	})
}

func TestRecordStoreDoesNotEvictAcknowledgedInterruption(t *testing.T) {
	dir := privateRecordDirectory(t)
	store, err := openRecordStore(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	job := recordJob(1)
	if err := store.Begin(job); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = openRecordStore(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.Acknowledge(job.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.Begin(recordJob(2)); !errors.Is(err, ErrRecordStoreFull) {
		t.Fatalf("interrupted record was evicted: %v", err)
	}
}

type failingFinishStore struct{ recordStore }

func (s failingFinishStore) Finish(Record) error { return ErrRecordStore }

func TestPersistentJobPreservesPublishedOutcomeWhenRecordWriteFails(t *testing.T) {
	dir := privateRecordDirectory(t)
	manager, err := NewPersistentJobManager(context.Background(), "fixture-token", dir, 10)
	if err != nil {
		t.Fatal(err)
	}
	manager.store = failingFinishStore{manager.store}
	manager.run = func(context.Context, StageOptions, time.Duration, string) (StageResult, error) {
		return StageResult{Publication: PublicationPublished, Durability: "confirmed", Bytes: 24,
			SHA256: strings.Repeat("a", 64), Integrity: "calculated_unverified", Metadata: &GGUFMetadata{Version: 3}}, nil
	}
	options := stageOptions(t.TempDir())
	job, err := manager.Start(options, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	manager.wg.Wait()
	status, err := manager.Status(job.ID)
	if err != nil || status.State != "failed" || status.Result.Publication != PublicationPublished ||
		!slices.Contains(status.ErrorCodes, "provenance_write_failed") {
		t.Fatalf("status = %+v, error = %v", status, err)
	}
	if _, err := manager.Start(options, time.Minute); !errors.Is(err, ErrRecordReview) {
		t.Fatalf("started after persistence failure: %v", err)
	}
	if err := manager.Stop(); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewPersistentJobManager(context.Background(), "", dir, 10)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Stop()
	record, err := restarted.Record(job.ID)
	if err != nil || record.Job.State != "interrupted" || record.Job.Result.Publication != PublicationUnknown || !record.ReviewRequired {
		t.Fatalf("restart claimed a known outcome: %+v, %v", record, err)
	}
}

func TestPersistentJobRequiresReviewForCleanupFailure(t *testing.T) {
	dir := privateRecordDirectory(t)
	manager, err := NewPersistentJobManager(context.Background(), "fixture-secret", dir, 10)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Stop()
	manager.run = func(context.Context, StageOptions, time.Duration, string) (StageResult, error) {
		return StageResult{Publication: PublicationNotPublished, Durability: "not_applicable"}, ErrStageCleanup
	}
	options := stageOptions(t.TempDir())
	job, err := manager.Start(options, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	manager.wg.Wait()
	record, err := manager.Record(job.ID)
	if err != nil || !record.ReviewRequired || record.Acknowledged || !slices.Contains(record.Job.ErrorCodes, "cleanup_failed") {
		t.Fatalf("record = %+v, error = %v", record, err)
	}
	data, err := os.ReadFile(filepath.Join(dir, job.ID+".json"))
	if err != nil || strings.Contains(string(data), "fixture-secret") {
		t.Fatalf("record missing or contained credential: %v", err)
	}
	if _, err := manager.Start(options, time.Minute); !errors.Is(err, ErrRecordReview) {
		t.Fatalf("started before cleanup review: %v", err)
	}
	record, err = manager.Acknowledge(job.ID)
	if err != nil || !record.Acknowledged || record.Job.Result.Publication != PublicationNotPublished {
		t.Fatalf("acknowledged = %+v, %v", record, err)
	}
}

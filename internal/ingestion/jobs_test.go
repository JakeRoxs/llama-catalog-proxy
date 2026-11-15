package ingestion

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

func TestJobManagerBoundsJobsAndReportsCancellation(t *testing.T) {
	manager, err := NewJobManager(context.Background(), "fixture-token")
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	manager.run = func(ctx context.Context, _ StageOptions, _ time.Duration, _ string) (StageResult, error) {
		close(started)
		<-ctx.Done()
		return StageResult{Publication: PublicationNotPublished}, errors.Join(ctx.Err(), ErrStageCleanup)
	}
	options := StageOptions{Directory: t.TempDir(), Source: Source{Repository: "owner/repo", Filename: "model.gguf", Revision: "main"},
		MaxBytes: 1024, MaxDirectoryBytes: 1024, MinFreeBytes: 1, FileMode: 0o600, AllowDownload: true}
	job, err := manager.Start(options, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	<-started
	if job.Result.Publication != PublicationUnknown {
		t.Fatalf("active publication = %q", job.Result.Publication)
	}
	if _, err := manager.Start(options, time.Minute); !errors.Is(err, ErrJobActive) {
		t.Fatalf("second start = %v", err)
	}
	if err := manager.Cancel(job.ID); err != nil {
		t.Fatal(err)
	}
	manager.wg.Wait()
	status, err := manager.Status(job.ID)
	if err != nil || status.State != "canceled" || status.Result.Publication != PublicationNotPublished ||
		!slices.Contains(status.ErrorCodes, "canceled") || !slices.Contains(status.ErrorCodes, "cleanup_failed") {
		t.Fatalf("status = %+v, error = %v", status, err)
	}
	if err := manager.Cancel(job.ID); !errors.Is(err, ErrJobFinished) {
		t.Fatalf("cancel finished job = %v", err)
	}
	status.ErrorCodes[0] = "modified"
	again, err := manager.Current()
	if err != nil || slices.Contains(again.ErrorCodes, "modified") {
		t.Fatalf("status snapshot was mutable: %+v, %v", again, err)
	}
	manager.Stop()
	if _, err := manager.Start(options, time.Minute); !errors.Is(err, ErrJobStopped) {
		t.Fatalf("start after stop = %v", err)
	}
}

func TestJobManagerPreservesPublicationWhenCancellationRaces(t *testing.T) {
	for _, tt := range []struct {
		name        string
		publication PublicationState
		err         error
		state       string
		code        string
	}{
		{"published successfully", PublicationPublished, nil, "succeeded", ""},
		{"published with durability error", PublicationPublished, errors.Join(context.Canceled, ErrStageDurability), "failed", "durability_uncertain"},
		{"unknown publication", PublicationUnknown, errors.Join(context.Canceled, ErrStageCommitUnknown), "failed", "publication_unknown"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			manager, err := NewJobManager(context.Background(), "")
			if err != nil {
				t.Fatal(err)
			}
			started := make(chan struct{})
			manager.run = func(ctx context.Context, _ StageOptions, _ time.Duration, _ string) (StageResult, error) {
				close(started)
				<-ctx.Done()
				name := "fixture"
				return StageResult{Publication: tt.publication, Metadata: &GGUFMetadata{Name: &name}}, tt.err
			}
			options := StageOptions{Directory: t.TempDir(), Source: Source{Repository: "owner/repo", Filename: "model.gguf", Revision: "main"},
				MaxBytes: 1024, MaxDirectoryBytes: 1024, MinFreeBytes: 1, FileMode: 0o600, AllowDownload: true}
			job, err := manager.Start(options, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			<-started
			if err := manager.Cancel(job.ID); err != nil {
				t.Fatal(err)
			}
			manager.wg.Wait()
			status, err := manager.Status(job.ID)
			if err != nil || status.State != tt.state || status.Result.Publication != tt.publication ||
				(tt.code != "" && !slices.Contains(status.ErrorCodes, tt.code)) {
				t.Fatalf("status = %+v, error = %v", status, err)
			}
			*status.Result.Metadata.Name = "modified"
			again, err := manager.Status(job.ID)
			if err != nil || *again.Result.Metadata.Name != "fixture" {
				t.Fatalf("metadata snapshot was mutable: %+v, %v", again, err)
			}
			manager.Stop()
		})
	}
}

func TestJobManagerValidatesAuthorization(t *testing.T) {
	manager, err := NewJobManager(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Stop()
	options := StageOptions{Directory: t.TempDir(), Source: Source{Repository: "owner/repo", Filename: "model.gguf", Revision: "main"},
		MaxBytes: 1024, MaxDirectoryBytes: 1024, MinFreeBytes: 1, FileMode: 0o600}
	if _, err := manager.Start(options, time.Minute); !errors.Is(err, ErrStageInvalid) {
		t.Fatalf("unauthorized start = %v", err)
	}
	options.AllowDownload = true
	for _, invalid := range []StageOptions{
		{Directory: options.Directory, Source: options.Source, MaxBytes: 1024, MinFreeBytes: 1, FileMode: 0o600, AllowDownload: true},
		{Directory: options.Directory, Source: options.Source, MaxBytes: 1024, MaxDirectoryBytes: 1024, FileMode: 0o600, AllowDownload: true},
	} {
		if _, err := manager.Start(invalid, time.Minute); !errors.Is(err, ErrStageInvalid) {
			t.Fatalf("missing disk limit accepted: %v", err)
		}
	}
	if _, err := manager.Start(options, 0); !errors.Is(err, ErrStageInvalid) {
		t.Fatalf("unbounded start = %v", err)
	}
}

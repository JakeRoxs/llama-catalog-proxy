//go:build linux

package ingestion

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func stageOptions(directory string) StageOptions {
	return StageOptions{
		Directory: directory, Source: Source{Repository: "owner/repo", Filename: "model.gguf", Revision: "main"},
		MaxBytes: 1024, MaxDirectoryBytes: 1 << 30, MinFreeBytes: 1, FileMode: 0o600, AllowDownload: true,
	}
}

func TestStageNewArtifactPublishesOnlyCompleteInspectedData(t *testing.T) {
	dir := t.TempDir()
	data := ggufString(ggufKey(ggufHeader(3, 0, 1), "general.architecture", 8), "llama")
	options := stageOptions(dir)
	options.MaxBytes = int64(len(data))
	options.FileMode = 0o640
	result, err := StageNewArtifact(context.Background(), options, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	if result.Publication != PublicationPublished || result.Durability != "confirmed" ||
		result.Bytes != int64(len(data)) || result.SHA256 != hex.EncodeToString(sum[:]) ||
		result.Integrity != "calculated_unverified" || result.Metadata == nil ||
		result.Metadata.Architecture == nil || *result.Metadata.Architecture != "llama" {
		t.Fatalf("result = %+v", result)
	}
	path := filepath.Join(dir, options.Source.Filename)
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("published bytes = %x, error = %v", got, err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o640 {
		t.Fatalf("published mode = %v, error = %v", info, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("temporary artifact remains: %v, %v", entries, err)
	}
}

func TestStageDirectoryQuotaCountsNestedFilesAndStagedBytes(t *testing.T) {
	dir := t.TempDir()
	nested := filepath.Join(dir, "nested")
	if err := os.Mkdir(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(nested, ".existing"), []byte("12345"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(nested, ".existing"), filepath.Join(dir, "linked")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(dir, "outside")); err != nil {
		t.Fatal(err)
	}
	data := ggufHeader(3, 0, 0)
	options := stageOptions(dir)
	options.MaxDirectoryBytes = int64(10 + len(data) - 1)
	result, err := StageNewArtifact(context.Background(), options,
		io.MultiReader(bytes.NewReader(data[:12]), bytes.NewReader(data[12:])))
	if !errors.Is(err, ErrStageQuota) || result.Publication != PublicationNotPublished {
		t.Fatalf("quota failure = %+v, %v", result, err)
	}
	if _, err := os.Lstat(filepath.Join(dir, options.Source.Filename)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("quota failure published an artifact: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 3 {
		t.Fatalf("quota failure left temporary files: %v, %v", entries, err)
	}
	options.MaxDirectoryBytes++
	result, err = StageNewArtifact(context.Background(), options, bytes.NewReader(data))
	if err != nil || result.Publication != PublicationPublished {
		t.Fatalf("exact quota boundary = %+v, %v", result, err)
	}
	options.Source.Filename = "another.gguf"
	result, err = StageNewArtifact(context.Background(), options, bytes.NewReader(data))
	if !errors.Is(err, ErrStageQuota) || result.Publication != PublicationNotPublished {
		t.Fatalf("cumulative quota = %+v, %v", result, err)
	}
}

func TestStageDirectoryQuotaFailsClosedOnExcessiveDepth(t *testing.T) {
	dir := t.TempDir()
	path := dir
	for range maxQuotaDepth + 1 {
		path = filepath.Join(path, "d")
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	options := stageOptions(dir)
	options.MaxDirectoryBytes = 1024
	result, err := StageNewArtifact(context.Background(), options, errorReader{err: errors.New("must not read")})
	if !errors.Is(err, ErrStageDiskCheck) || result.Publication != PublicationNotPublished {
		t.Fatalf("deep directory check = %+v, %v", result, err)
	}
}

func TestReplacementQuotaIncludesOldAndStagedArtifact(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "model.gguf")
	old := []byte("old model")
	if err := os.WriteFile(path, old, 0o600); err != nil {
		t.Fatal(err)
	}
	data := ggufHeader(3, 0, 0)
	options := stageOptions(dir)
	options.ReplaceExisting, options.AllowReplacement, options.AcknowledgeExclusiveWindow = true, true, true
	options.MaxDirectoryBytes = int64(len(old) + len(data) - 1)
	result, err := StageNewArtifact(context.Background(), options, bytes.NewReader(data))
	if !errors.Is(err, ErrStageQuota) || result.Publication != PublicationNotPublished {
		t.Fatalf("replacement quota = %+v, %v", result, err)
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, old) {
		t.Fatalf("quota failure changed old file: %q, %v", got, err)
	}
	options.MaxDirectoryBytes++
	result, err = StageNewArtifact(context.Background(), options, bytes.NewReader(data))
	if err != nil || result.Publication != PublicationPublished {
		t.Fatalf("exact replacement boundary = %+v, %v", result, err)
	}
}

func TestStageRejectsInsufficientFreeSpaceBeforeReading(t *testing.T) {
	options := stageOptions(t.TempDir())
	options.MinFreeBytes = math.MaxInt64
	result, err := StageNewArtifact(context.Background(), options, errorReader{err: errors.New("must not read")})
	if !errors.Is(err, ErrStageFreeSpace) || result.Publication != PublicationNotPublished {
		t.Fatalf("free-space failure = %+v, %v", result, err)
	}
	entries, err := os.ReadDir(options.Directory)
	if err != nil || len(entries) != 0 {
		t.Fatalf("free-space failure left files: %v, %v", entries, err)
	}
}

func TestFreeSpaceReserveUsesAvailableWholeBlocks(t *testing.T) {
	for _, tt := range []struct {
		reserve, additional int64
		want                bool
	}{
		{4096, 4096, true},
		{4096, 4097, false},
		{4097, 0, true},
		{4097, 1, false},
		{math.MaxInt64, math.MaxInt64, false},
	} {
		if got := freeSpaceAvailable(2, 4096, tt.reserve, tt.additional); got != tt.want {
			t.Errorf("reserve %d, additional %d: got %v, want %v", tt.reserve, tt.additional, got, tt.want)
		}
	}
}

func TestStageNewArtifactRejectsUnsafeOrIncompleteInput(t *testing.T) {
	inputErr := errors.New("fixture-sensitive-url")
	data := ggufHeader(3, 0, 0)
	for _, test := range []struct {
		name   string
		setup  func(*testing.T, *StageOptions) (context.Context, io.Reader)
		expect error
	}{
		{"consent", func(_ *testing.T, o *StageOptions) (context.Context, io.Reader) {
			o.AllowDownload = false
			return context.Background(), bytes.NewReader(data)
		}, ErrStageInvalid},
		{"bad source", func(_ *testing.T, o *StageOptions) (context.Context, io.Reader) {
			o.Source.Filename = "../model.gguf"
			return context.Background(), bytes.NewReader(data)
		}, ErrStageInvalid},
		{"unsafe permissions", func(_ *testing.T, o *StageOptions) (context.Context, io.Reader) {
			o.FileMode = 0o666
			return context.Background(), bytes.NewReader(data)
		}, ErrStageInvalid},
		{"missing quota", func(_ *testing.T, o *StageOptions) (context.Context, io.Reader) {
			o.MaxDirectoryBytes = 0
			return context.Background(), bytes.NewReader(data)
		}, ErrStageInvalid},
		{"missing reserve", func(_ *testing.T, o *StageOptions) (context.Context, io.Reader) {
			o.MinFreeBytes = 0
			return context.Background(), bytes.NewReader(data)
		}, ErrStageInvalid},
		{"directory permissions", func(t *testing.T, o *StageOptions) (context.Context, io.Reader) {
			if err := os.Chmod(o.Directory, 0o777); err != nil {
				t.Fatal(err)
			}
			return context.Background(), bytes.NewReader(data)
		}, ErrStageDirectory},
		{"too large", func(_ *testing.T, o *StageOptions) (context.Context, io.Reader) {
			o.MaxBytes = int64(len(data) - 1)
			return context.Background(), bytes.NewReader(data)
		}, ErrStageTooLarge},
		{"empty", func(_ *testing.T, _ *StageOptions) (context.Context, io.Reader) {
			return context.Background(), bytes.NewReader(nil)
		}, ErrStageEmpty},
		{"invalid gguf", func(_ *testing.T, _ *StageOptions) (context.Context, io.Reader) {
			return context.Background(), bytes.NewReader([]byte("not a model"))
		}, ErrInvalidGGUF},
		{"read failure", func(_ *testing.T, _ *StageOptions) (context.Context, io.Reader) {
			return context.Background(), io.MultiReader(bytes.NewReader(data), errorReader{err: inputErr})
		}, ErrStageIO},
		{"canceled", func(_ *testing.T, _ *StageOptions) (context.Context, io.Reader) {
			ctx, cancel := context.WithCancel(context.Background())
			return ctx, &cancelingReader{data: data, cancel: cancel}
		}, context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			options := stageOptions(dir)
			ctx, reader := test.setup(t, &options)
			result, err := StageNewArtifact(ctx, options, reader)
			if !errors.Is(err, test.expect) || result.Publication != PublicationNotPublished {
				t.Fatalf("result = %+v, error = %v; want %v", result, err, test.expect)
			}
			if strings.Contains(err.Error(), inputErr.Error()) {
				t.Fatal("staging error exposed a source error")
			}
			entries, err := os.ReadDir(dir)
			if err != nil || len(entries) != 0 {
				t.Fatalf("staging left files: %v, %v", entries, err)
			}
		})
	}
}

type cancelingReader struct {
	data   []byte
	cancel context.CancelFunc
}

func (r *cancelingReader) Read(buffer []byte) (int, error) {
	n := copy(buffer, r.data)
	r.data = r.data[n:]
	r.cancel()
	return n, nil
}

func TestStageNewArtifactNeverReplacesExistingFileOrSymlink(t *testing.T) {
	for _, symlink := range []bool{false, true} {
		t.Run(map[bool]string{false: "file", true: "symlink"}[symlink], func(t *testing.T) {
			dir := t.TempDir()
			options := stageOptions(dir)
			target := filepath.Join(dir, options.Source.Filename)
			other := filepath.Join(t.TempDir(), "existing")
			if err := os.WriteFile(other, []byte("existing"), 0o600); err != nil {
				t.Fatal(err)
			}
			var err error
			if symlink {
				err = os.Symlink(other, target)
			} else {
				err = os.WriteFile(target, []byte("existing"), 0o600)
			}
			if err != nil {
				t.Fatal(err)
			}
			result, err := StageNewArtifact(context.Background(), options, errorReader{err: errors.New("must not read")})
			if !errors.Is(err, ErrStageExists) || result.Publication != PublicationNotPublished {
				t.Fatalf("result = %+v, error = %v", result, err)
			}
			got, err := os.ReadFile(target)
			if err != nil || string(got) != "existing" {
				t.Fatalf("existing file changed: %q, %v", got, err)
			}
		})
	}
}

func TestStageNewArtifactRejectsSymlinkDirectory(t *testing.T) {
	dir := t.TempDir()
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(dir, alias); err != nil {
		t.Fatal(err)
	}
	result, err := StageNewArtifact(context.Background(), stageOptions(alias), bytes.NewReader(ggufHeader(3, 0, 0)))
	if !errors.Is(err, ErrStageDirectory) || result.Publication != PublicationNotPublished {
		t.Fatalf("result = %+v, error = %v", result, err)
	}
}

type waitingReader struct {
	started chan struct{}
	release chan struct{}
	data    *bytes.Reader
}

func (r *waitingReader) Read(data []byte) (int, error) {
	select {
	case <-r.started:
	default:
		close(r.started)
		<-r.release
	}
	return r.data.Read(data)
}

func TestStageNewArtifactRejectsConcurrentWriter(t *testing.T) {
	options := stageOptions(t.TempDir())
	reader := &waitingReader{started: make(chan struct{}), release: make(chan struct{}), data: bytes.NewReader(ggufHeader(3, 0, 0))}
	done := make(chan error, 1)
	go func() {
		_, err := StageNewArtifact(context.Background(), options, reader)
		done <- err
	}()
	<-reader.started
	other := options
	other.Source.Filename = "other.gguf"
	_, err := StageNewArtifact(context.Background(), other, bytes.NewReader(ggufHeader(3, 0, 0)))
	close(reader.release)
	if first := <-done; first != nil {
		t.Fatal(first)
	}
	if !errors.Is(err, ErrStageDirectory) {
		t.Fatalf("concurrent staging error = %v", err)
	}
}

func TestStageNewArtifactRejectsDestinationCreatedDuringStaging(t *testing.T) {
	dir := t.TempDir()
	options := stageOptions(dir)
	reader := &waitingReader{started: make(chan struct{}), release: make(chan struct{}), data: bytes.NewReader(ggufHeader(3, 0, 0))}
	type publication struct {
		result StageResult
		err    error
	}
	done := make(chan publication, 1)
	go func() {
		result, err := StageNewArtifact(context.Background(), options, reader)
		done <- publication{result: result, err: err}
	}()
	<-reader.started
	path := filepath.Join(dir, options.Source.Filename)
	writeErr := os.WriteFile(path, []byte("existing"), 0o600)
	close(reader.release)
	outcome := <-done
	if writeErr != nil {
		t.Fatal(writeErr)
	}
	if !errors.Is(outcome.err, ErrStageExists) || !errors.Is(outcome.err, ErrStageCommitUnknown) ||
		outcome.result.Publication != PublicationUnknown {
		t.Fatalf("publication = %+v, error = %v", outcome.result, outcome.err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != "existing" {
		t.Fatalf("existing file changed: %q, %v", got, err)
	}
}

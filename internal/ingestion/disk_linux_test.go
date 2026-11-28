//go:build linux

package ingestion

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func artifactDirectoryForTest(t *testing.T, dir string) *os.File {
	t.Helper()
	d, err := os.Open(dir)
	if err != nil {
		t.Fatalf("open directory: %v", err)
	}
	return d
}

func writeFile(t *testing.T, dir, name string, size int64) {
	t.Helper()
	data := make([]byte, size)
	if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
}

func TestArtifactDirectoryBytesSumsRegularFiles(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "a.bin", 100)
	writeFile(t, dir, "b.bin", 200)
	sub := filepath.Join(dir, "sub")
	if err := os.Mkdir(sub, 0o700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, sub, "c.bin", 50)

	d := artifactDirectoryForTest(t, dir)
	defer d.Close()
	total, err := artifactDirectoryBytes(context.Background(), d, 1<<30)
	if err != nil {
		t.Fatalf("artifactDirectoryBytes = %v", err)
	}
	if total != 350 {
		t.Fatalf("total = %d, want 350", total)
	}
}

func TestArtifactDirectoryBytesExceedsLimitReturnsQuota(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "a.bin", 100)

	d := artifactDirectoryForTest(t, dir)
	defer d.Close()
	if _, err := artifactDirectoryBytes(context.Background(), d, 50); !errors.Is(err, ErrStageQuota) {
		t.Fatalf("expected ErrStageQuota, got %v", err)
	}
}

func TestArtifactDirectoryBytesExactLimitIsAllowed(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "a.bin", 100)

	d := artifactDirectoryForTest(t, dir)
	defer d.Close()
	if _, err := artifactDirectoryBytes(context.Background(), d, 100); err != nil {
		t.Fatalf("exact limit should be allowed, got %v", err)
	}
}

func TestArtifactDirectoryBytesCountsNestedFiles(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "a.bin", 100)
	sub := filepath.Join(dir, "sub")
	if err := os.Mkdir(sub, 0o700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, sub, "deep.bin", 75)
	sub2 := filepath.Join(sub, "sub2")
	if err := os.Mkdir(sub2, 0o700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, sub2, "deeper.bin", 25)

	d := artifactDirectoryForTest(t, dir)
	defer d.Close()
	total, err := artifactDirectoryBytes(context.Background(), d, 1<<30)
	if err != nil {
		t.Fatalf("artifactDirectoryBytes = %v", err)
	}
	if total != 200 {
		t.Fatalf("total = %d, want 200", total)
	}
}

func TestArtifactDirectoryBytesSkipsSymlinks(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "real.bin", 100)
	target := filepath.Join(t.TempDir(), "target")
	if err := os.WriteFile(target, []byte("target"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(dir, "link.bin")); err != nil {
		t.Fatal(err)
	}

	d := artifactDirectoryForTest(t, dir)
	defer d.Close()
	total, err := artifactDirectoryBytes(context.Background(), d, 1<<30)
	if err != nil {
		t.Fatalf("artifactDirectoryBytes = %v", err)
	}
	if total != 100 {
		t.Fatalf("total = %d, want 100 (symlink not counted)", total)
	}
}

func TestArtifactDirectoryBytesExcessiveDepthReturnsDiskCheck(t *testing.T) {
	dir := t.TempDir()
	path := dir
	for range maxQuotaDepth + 1 {
		path = filepath.Join(path, "d")
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	writeFile(t, path, "leaf.bin", 10)

	d := artifactDirectoryForTest(t, dir)
	defer d.Close()
	if _, err := artifactDirectoryBytes(context.Background(), d, 1<<30); !errors.Is(err, ErrStageDiskCheck) {
		t.Fatalf("expected ErrStageDiskCheck for excessive depth, got %v", err)
	}
}

func TestArtifactDirectoryBytesHonorsContextCancellation(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "a.bin", 100)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	d := artifactDirectoryForTest(t, dir)
	defer d.Close()
	if _, err := artifactDirectoryBytes(ctx, d, 1<<30); err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

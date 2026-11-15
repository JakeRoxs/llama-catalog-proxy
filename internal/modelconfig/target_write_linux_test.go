//go:build linux

package modelconfig

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

func TestTargetWritePreservesContentAndMetadata(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	source := []byte("# secret\nmodels: {m: {cmd: 'llama-server -c 1024'}}\n")
	candidate := []byte(strings.Replace(string(source), "-c 1024", "-c 4096", 1))
	if err := os.WriteFile(path, source, 0o640); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := withTargetWrite(dir, "config.yaml", func(target *targetWrite) error {
		if !bytes.Equal(target.source, source) {
			t.Fatal("snapshot differs from target")
		}
		written, err := target.replace(candidate)
		if !written || err != nil {
			t.Fatalf("replace = %v, %v", written, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	oldStat, newStat := before.Sys().(*syscall.Stat_t), after.Sys().(*syscall.Stat_t)
	if before.Mode().Perm() != after.Mode().Perm() || oldStat.Uid != newStat.Uid || oldStat.Gid != newStat.Gid {
		t.Fatal("replacement changed target permissions or ownership")
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, candidate) {
		t.Fatalf("replacement differs: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 || entries[0].Name() != "config.yaml" {
		t.Fatalf("temporary files remain: %v, %v", entries, err)
	}
}

func TestTargetWritePreservesExtendedAttributes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("before"), 0o600); err != nil {
		t.Fatal(err)
	}
	value := []byte{0, 1, 2, 255}
	if err := unix.Setxattr(path, "user.catalog-test", value, 0); err != nil {
		if errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EOPNOTSUPP) {
			t.Skip("filesystem does not support user xattrs")
		}
		t.Fatal(err)
	}
	if err := withTargetWrite(dir, "config.yaml", func(target *targetWrite) error {
		_, err := target.replace([]byte("after"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(value))
	n, err := unix.Getxattr(path, "user.catalog-test", got)
	if err != nil || n != len(value) || !bytes.Equal(got, value) {
		t.Fatalf("xattr was not preserved: %v", err)
	}
}

func TestTargetWriteRejectsStaleAndUnsafeTargets(t *testing.T) {
	dir, other := t.TempDir(), t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := withTargetWrite(dir, "config.yaml", func(target *targetWrite) error {
		if changed, err := target.replace([]byte("original")); changed || !errors.Is(err, errTargetNoChange) {
			t.Fatalf("unchanged write = %v, %v", changed, err)
		}
		if err := os.WriteFile(path, []byte("external"), 0o600); err != nil {
			t.Fatal(err)
		}
		if changed, err := target.replace([]byte("new")); changed || !errors.Is(err, errTargetChanged) {
			t.Fatalf("stale write = %v, %v", changed, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != "external" {
		t.Fatalf("external change lost: %v", err)
	}
	link := filepath.Join(dir, "link.yaml")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	hard := filepath.Join(other, "hard.yaml")
	if err := os.Link(path, hard); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"link.yaml", "config.yaml", "../config.yaml"} {
		if err := withTargetWrite(dir, name, func(*targetWrite) error {
			t.Fatal("unsafe target reached callback")
			return nil
		}); !errors.Is(err, errTargetWrite) {
			t.Fatalf("unsafe target accepted: %v", err)
		}
	}
}

func TestTargetWriteLocksDirectory(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"a", "b"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("source"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := withTargetWrite(dir, "a", func(*targetWrite) error {
		err := withTargetWrite(dir, "b", func(*targetWrite) error {
			t.Fatal("concurrent writer acquired directory lock")
			return nil
		})
		if !errors.Is(err, errTargetWrite) {
			t.Fatalf("concurrent writer error: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTargetWriteLockAcrossProcesses(t *testing.T) {
	const childVar = "CATALOG_PROXY_TARGET_LOCK_TEST"
	if directory := os.Getenv(childVar); directory != "" {
		err := withTargetWrite(directory, "config.yaml", func(*targetWrite) error {
			t.Fatal("child acquired the parent's lock")
			return nil
		})
		if !errors.Is(err, errTargetWrite) {
			t.Fatalf("child lock error: %v", err)
		}
		return
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte("source"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := withTargetWrite(dir, "config.yaml", func(*targetWrite) error {
		cmd := exec.Command(os.Args[0], "-test.run=^TestTargetWriteLockAcrossProcesses$")
		cmd.Env = append(os.Environ(), childVar+"="+dir)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child process failed: %v: %s", err, output)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTargetWriteRejectsMetadataChanges(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := withTargetWrite(dir, "config.yaml", func(target *targetWrite) error {
		if err := os.Chmod(path, 0o640); err != nil {
			t.Fatal(err)
		}
		if changed, err := target.replace([]byte("new")); changed || !errors.Is(err, errTargetChanged) {
			t.Fatalf("changed metadata accepted: %v, %v", changed, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o640 {
		t.Fatalf("external metadata change lost: %v, %v", info, err)
	}
}

func TestTargetWriteRejectsSpecialPermissions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600|os.ModeSetuid); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeSetuid == 0 {
		t.Skip("filesystem does not retain special mode bits")
	}
	if err := withTargetWrite(dir, "config.yaml", func(*targetWrite) error {
		t.Fatal("special-permission file reached callback")
		return nil
	}); !errors.Is(err, errTargetWrite) {
		t.Fatalf("special-permission file accepted: %v", err)
	}
}

func TestTargetWriteFailureKeepsOriginal(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can write to a non-writable directory")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := withTargetWrite(dir, "config.yaml", func(target *targetWrite) error {
		if err := os.Chmod(dir, 0o500); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := os.Chmod(dir, 0o700); err != nil {
				t.Error(err)
			}
		}()
		changed, err := target.replace([]byte("new"))
		if changed || !errors.Is(err, errTargetWrite) {
			t.Fatalf("failed write = %v, %v", changed, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "original" {
		t.Fatalf("write failure changed source: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("write failure left temporary files: %v %v", entries, err)
	}
}

func TestTargetWriteIsAtomicForReaders(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	first := bytes.Repeat([]byte("a"), 64<<10)
	second := bytes.Repeat([]byte("b"), 64<<10)
	if err := os.WriteFile(path, first, 0o600); err != nil {
		t.Fatal(err)
	}
	var stopped atomic.Bool
	results := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stopped.Load() {
			data, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(data, first) && !bytes.Equal(data, second) {
				results <- errors.New("reader observed a partial or unavailable target")
				return
			}
		}
	}()
	defer func() { stopped.Store(true); wg.Wait() }()
	for i := 0; i < 10; i++ {
		candidate := second
		if i%2 == 1 {
			candidate = first
		}
		if err := withTargetWrite(dir, "config.yaml", func(target *targetWrite) error {
			_, err := target.replace(candidate)
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	stopped.Store(true)
	wg.Wait()
	select {
	case err := <-results:
		t.Fatal(err)
	default:
	}
}

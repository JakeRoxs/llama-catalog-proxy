//go:build linux

package modelconfig

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestPreviewTargetOnlyReadsConfiguredRegularFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	data := []byte("models:\n  model:\n    cmd: llama-server -c 1024\n")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	size := int32(4096)
	preview, err := PreviewTarget(dir, "config.yaml", "local", "model", CommandSettings{ContextSize: &size})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(preview.candidate), "-c 4096") {
		t.Fatal("preview did not use configured file")
	}
	current, err := os.ReadFile(path)
	if err != nil || string(current) != string(data) {
		t.Fatalf("preview changed source file: %v", err)
	}
	for _, file := range []string{"../config.yaml", ".", "..", "sub/file.yaml", `sub\file.yaml`} {
		if _, err := PreviewTarget(dir, file, "local", "model", CommandSettings{ContextSize: &size}); err == nil {
			t.Fatalf("accepted unsafe filename %q", file)
		}
	}
}

func TestPreviewTargetRejectsSymlinksAndSpecialFiles(t *testing.T) {
	dir, other := t.TempDir(), t.TempDir()
	path := filepath.Join(other, "secret.yaml")
	if err := os.WriteFile(path, []byte("models: {}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(path, filepath.Join(dir, "link.yaml")); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(dir, "fifo.yaml"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "large.yaml"), []byte(strings.Repeat("x", maxConfigBytes+1)), 0o600); err != nil {
		t.Fatal(err)
	}
	size := int32(1)
	for _, name := range []string{"link.yaml", "fifo.yaml", "large.yaml", "missing.yaml"} {
		t.Run(name, func(t *testing.T) {
			if _, err := PreviewTarget(dir, name, "local", "model", CommandSettings{ContextSize: &size}); err == nil {
				t.Fatal("accepted unsafe target")
			}
			if _, err := ReadSettingsTarget(dir, name, "local", "model"); err == nil {
				t.Fatal("read accepted unsafe target")
			}
		})
	}
}

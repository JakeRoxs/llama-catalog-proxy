//go:build linux

package proxy

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

func TestDelegatedStoresRemainPinnedAcrossReload(t *testing.T) {
	cert, key, _ := peerTestCertificate(t)
	target, backup, proposals, operations, replacement := t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir()
	path := configPath(t)
	write := func(store *config.DelegatedStoreConfig) {
		t.Helper()
		content := fmt.Sprintf(`default_backend: local
backends: {local: {url: http://localhost:9292}}
management:
  listen: "127.0.0.1:9294"
  tls_cert_file: %q
  tls_key_file: %q
  preview_token: %q
  backup_store: {directory: %q, token: %q, max_backups: 2}
  targets:
    swap: {directory: %q, filename: config.yaml}
`, cert, key, strings.Repeat("m", 32), backup, strings.Repeat("b", 32), target)
		if store != nil {
			content += fmt.Sprintf("  delegated_store: {proposal_directory: %q, operation_directory: %q}\n",
				store.ProposalDirectory, store.OperationDirectory)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	original := &config.DelegatedStoreConfig{ProposalDirectory: proposals, OperationDirectory: operations}
	write(original)
	r, returned, err := NewReloader(path, discardLogger(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	returned.Management.DelegatedStore.OperationDirectory = replacement
	if r.state.Load().management.DelegatedStore.OperationDirectory != operations ||
		r.delegatedStores.OperationDirectory != operations {
		t.Fatal("returned configuration modified pinned stores")
	}
	changed := &config.DelegatedStoreConfig{ProposalDirectory: proposals, OperationDirectory: replacement}
	write(changed)
	old := r.state.Load()
	if err := r.reload(); err == nil || r.state.Load() != old {
		t.Fatalf("changed store accepted on reload: %v", err)
	}
	write(nil)
	if err := r.reload(); err != nil || r.state.Load().management.DelegatedStore != nil {
		t.Fatalf("disabling delegated stores failed: %v", err)
	}
	write(changed)
	if err := r.reload(); err == nil {
		t.Fatal("changing the store after disable was accepted")
	}
	write(original)
	if err := r.reload(); err != nil || r.state.Load().management.DelegatedStore.OperationDirectory != operations {
		t.Fatalf("restoring pinned stores failed: %v", err)
	}
}

func TestEnablingDelegatedStoresRequiresRestart(t *testing.T) {
	cert, key, _ := peerTestCertificate(t)
	target, backup, proposals, operations := t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir()
	path := configPath(t)
	content := fmt.Sprintf(`default_backend: local
backends: {local: {url: http://localhost:9292}}
management:
  listen: "127.0.0.1:9294"
  tls_cert_file: %q
  tls_key_file: %q
  preview_token: %q
  backup_store: {directory: %q, token: %q, max_backups: 2}
  targets: {swap: {directory: %q, filename: config.yaml}}
`, cert, key, strings.Repeat("m", 32), backup, strings.Repeat("b", 32), target)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	r, _, err := NewReloader(path, discardLogger(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	content += fmt.Sprintf("  delegated_store: {proposal_directory: %q, operation_directory: %q}\n", proposals, operations)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := r.reload(); err == nil || r.state.Load().management.DelegatedStore != nil {
		t.Fatalf("new stores accepted without restart: %v", err)
	}
}

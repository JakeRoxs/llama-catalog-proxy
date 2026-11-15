//go:build linux

package proxy

import (
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
)

func TestBackupRecoveryHTTPS(t *testing.T) {
	targetDir, proposalDir, backupDir := t.TempDir(), t.TempDir(), t.TempDir()
	for _, directory := range []string{proposalDir, backupDir} {
		if err := os.Chmod(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	source := []byte("# confidential\nmodels: {m: {cmd: 'llama-server -c 1024'}}\n")
	targetPath := filepath.Join(targetDir, "config.yaml")
	if err := os.WriteFile(targetPath, source, 0o600); err != nil {
		t.Fatal(err)
	}
	size := int32(4096)
	settings := modelconfig.CommandSettings{ContextSize: &size}
	preview, err := modelconfig.PreviewDocument("local", "m", source, settings)
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := modelconfig.NewProposal(targetDir, "config.yaml", preview, settings)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(strings.Replace(string(source), "-c 1024", "-c 4096", 1)))
	id, invalidID := strings.Repeat("a", 32), strings.Repeat("b", 32)
	info := modelconfig.BackupInfo{
		ID: id, CreatedAt: time.Now().UTC(), TargetID: "local", TargetFingerprint: proposal.TargetFingerprint,
		ModelID: "m", ProposalID: proposal.ID, SourceRevision: preview.Revision,
		WrittenRevision: hex.EncodeToString(digest[:]),
	}
	record, err := json.Marshal(struct {
		Info   modelconfig.BackupInfo `json:"info"`
		Source []byte                 `json:"source"`
	}{Info: info, Source: source})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(backupDir, "backup-"+id+".json"), record, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(backupDir, "backup-"+invalidID+".json"), []byte("confidential"), 0o600); err != nil {
		t.Fatal(err)
	}

	certPath, keyPath, roots := peerTestCertificate(t)
	previewToken, proposalToken := strings.Repeat("m", 32), strings.Repeat("p", 32)
	backupToken, newToken := strings.Repeat("r", 32), strings.Repeat("n", 32)
	currentDir, currentToken, enabled := backupDir, backupToken, true
	path := configPath(t)
	writeConfig := func() {
		t.Helper()
		content := fmt.Sprintf(`default_backend: local
backends: {local: {url: http://localhost:9292}}
management:
  listen: "127.0.0.1:9294"
  tls_cert_file: %q
  tls_key_file: %q
  preview_token: %q
  proposal_store: {directory: %q, token: %q}
  targets:
    local: {directory: %q, filename: config.yaml}
`, certPath, keyPath, previewToken, proposalDir, proposalToken, targetDir)
		if enabled {
			content += fmt.Sprintf("  backup_store: {directory: %q, token: %q, max_backups: 1}\n", currentDir, currentToken)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeConfig()
	r, cfg, err := NewReloader(path, discardLogger(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	cfg.Management.BackupStore.Token = newToken
	cfg.Management.BackupStore.Directory = targetDir
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: r.ManagementHandler(), TLSConfig: r.ManagementTLSConfig()}
	done := make(chan error, 1)
	go func() { done <- server.ServeTLS(listener, "", "") }()
	t.Cleanup(func() { _ = server.Close(); <-done })
	client := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}}}
	defer client.CloseIdleConnections()
	request := func(method, urlPath, token, body string) (int, string) {
		t.Helper()
		req, err := http.NewRequest(method, "https://"+listener.Addr().String()+urlPath, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		if response.Header.Get("Cache-Control") != "no-store" {
			t.Fatal("missing no-store header")
		}
		return response.StatusCode, string(data)
	}
	const base = "/management/v1/backups"
	for _, token := range []string{"", previewToken, proposalToken, newToken} {
		if status, _ := request(http.MethodGet, base, token, ""); status != http.StatusUnauthorized {
			t.Fatalf("wrong credential accepted: %d", status)
		}
	}
	if status, _ := request(http.MethodDelete, base+"/"+id, proposalToken, ""); status != http.StatusUnauthorized {
		t.Fatalf("proposal credential could delete backup: %d", status)
	}
	if status, _ := request(http.MethodGet, "/management/v1/proposals", backupToken, ""); status != http.StatusUnauthorized {
		t.Fatalf("backup credential could read proposals: %d", status)
	}
	status, body := request(http.MethodGet, base, backupToken, "")
	if status != http.StatusOK || !strings.Contains(body, `"status":"valid"`) ||
		!strings.Contains(body, `"status":"unreadable_or_invalid"`) || !strings.Contains(body, invalidID) {
		t.Fatalf("list backups = %d %s", status, body)
	}
	for _, secret := range []string{"confidential", targetDir, backupDir, previewToken, proposalToken, backupToken} {
		if strings.Contains(body, secret) {
			t.Fatal("backup listing exposed source, path, or credential")
		}
	}
	if status, body := request(http.MethodGet, base+"/"+id, backupToken, ""); status != http.StatusOK ||
		!strings.Contains(body, preview.Revision) || strings.Contains(body, "confidential") {
		t.Fatalf("backup metadata = %d %s", status, body)
	}
	if status, _ := request(http.MethodGet, base+"/"+invalidID, backupToken, ""); status != http.StatusServiceUnavailable {
		t.Fatalf("invalid backup metadata status: %d", status)
	}
	if status, _ := request(http.MethodDelete, base+"/"+invalidID, backupToken, "{}"); status != http.StatusBadRequest {
		t.Fatalf("backup deletion accepted body: %d", status)
	}
	if status, _ := request(http.MethodDelete, base+"/"+invalidID, backupToken, ""); status != http.StatusNoContent {
		t.Fatalf("invalid backup deletion status: %d", status)
	}
	if status, _ := request(http.MethodGet, base+"/"+invalidID, backupToken, ""); status != http.StatusNotFound {
		t.Fatalf("deleted backup status: %d", status)
	}
	if status, _ := request(http.MethodGet, base+"?raw=1", backupToken, ""); status != http.StatusNotFound {
		t.Fatalf("query accepted: %d", status)
	}
	plain := httptest.NewRecorder()
	r.ManagementHandler().ServeHTTP(plain, httptest.NewRequest(http.MethodGet, base, nil))
	if plain.Code != http.StatusForbidden {
		t.Fatalf("plaintext status: %d", plain.Code)
	}
	public := httptest.NewRecorder()
	r.ServeHTTP(public, httptest.NewRequest(http.MethodGet, base, nil))
	if public.Code != http.StatusNotFound {
		t.Fatalf("public listener status: %d", public.Code)
	}
	alias := filepath.Join(t.TempDir(), "target-alias")
	if err := os.Symlink(targetDir, alias); err != nil {
		t.Fatal(err)
	}
	currentDir = alias
	writeConfig()
	if err := r.reload(); err != nil {
		t.Fatal(err)
	}
	if status, _ := request(http.MethodDelete, base+"/"+id, backupToken, ""); status != http.StatusServiceUnavailable {
		t.Fatalf("aliased target directory accepted: %d", status)
	}
	proposalAlias := filepath.Join(t.TempDir(), "proposal-alias")
	if err := os.Symlink(proposalDir, proposalAlias); err != nil {
		t.Fatal(err)
	}
	currentDir = proposalAlias
	writeConfig()
	if err := r.reload(); err != nil {
		t.Fatal(err)
	}
	if status, _ := request(http.MethodGet, base, backupToken, ""); status != http.StatusServiceUnavailable {
		t.Fatalf("aliased proposal directory accepted by backup API: %d", status)
	}
	if status, _ := request(http.MethodGet, "/management/v1/proposals", proposalToken, ""); status != http.StatusServiceUnavailable {
		t.Fatalf("aliased backup directory accepted by proposal API: %d", status)
	}
	currentDir, currentToken = backupDir, newToken
	writeConfig()
	if err := r.reload(); err != nil {
		t.Fatal(err)
	}
	if status, _ := request(http.MethodGet, base, backupToken, ""); status != http.StatusUnauthorized {
		t.Fatalf("old token accepted after reload: %d", status)
	}
	if status, _ := request(http.MethodGet, base+"/"+id, newToken, ""); status != http.StatusOK {
		t.Fatalf("new token rejected: %d", status)
	}
	enabled = false
	writeConfig()
	if err := r.reload(); err != nil {
		t.Fatal(err)
	}
	if status, _ := request(http.MethodGet, base, newToken, ""); status != http.StatusUnauthorized {
		t.Fatalf("disabled backup store accessible: %d", status)
	}
	got, err := os.ReadFile(targetPath)
	if err != nil || string(got) != string(source) {
		t.Fatalf("backup operations modified target: %v", err)
	}
}

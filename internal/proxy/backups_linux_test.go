//go:build linux

package proxy

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
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

// backupRecoveryState holds the shared state for the TestBackupRecoveryHTTPS
// phases. Each phase is a top-level helper below the cognitive-complexity
// threshold; the struct keeps the setup, config writer, server, and request
// helper behavior intact while reusing backupConfigTemplate.
type backupRecoveryState struct {
	t                                                  *testing.T
	targetDir, proposalDir, backupDir, targetPath      string
	source                                             []byte
	preview                                            modelconfig.Preview
	proposal                                           modelconfig.Proposal
	id, invalidID                                      string
	certPath, keyPath                                  string
	roots                                              *x509.CertPool
	previewToken, proposalToken, backupToken, newToken string
	currentDir, currentToken                           string
	enabled                                            bool
	path                                               string
	r                                                  *Reloader
	listener                                           net.Listener
	client                                             *http.Client
}

// setupBackupRecovery builds the fixture directories, source file, preview,
// proposal, backup records, certificates, tokens, and config path. It is
// linear with only shallow conditionals, keeping cognitive complexity low.
func setupBackupRecovery(t *testing.T) *backupRecoveryState {
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
	return &backupRecoveryState{
		t:             t,
		targetDir:     targetDir,
		proposalDir:   proposalDir,
		backupDir:     backupDir,
		targetPath:    targetPath,
		source:        source,
		preview:       preview,
		proposal:      proposal,
		id:            id,
		invalidID:     invalidID,
		certPath:      certPath,
		keyPath:       keyPath,
		roots:         roots,
		previewToken:  previewToken,
		proposalToken: proposalToken,
		backupToken:   backupToken,
		newToken:      newToken,
		currentDir:    currentDir,
		currentToken:  currentToken,
		enabled:       enabled,
		path:          path,
	}
}

// writeConfig preserves the original closure behavior: it writes the shared
// backupConfigTemplate and appends the conditional backup_store line.
func (s *backupRecoveryState) writeConfig() {
	s.t.Helper()
	content := backupConfigTemplate(s.certPath, s.keyPath, s.previewToken, s.proposalDir, s.proposalToken, s.targetDir)
	if s.enabled {
		content += fmt.Sprintf("  backup_store: {directory: %q, token: %q, max_backups: 1}\n", s.currentDir, s.currentToken)
	}
	if err := os.WriteFile(s.path, []byte(content), 0o600); err != nil {
		s.t.Fatal(err)
	}
}

// startBackupServer preserves the original server startup behavior: write
// config, create the reloader, override the backup store token/directory,
// listen on TLS, and register cleanup.
func (s *backupRecoveryState) startBackupServer() {
	s.writeConfig()
	r, cfg, err := NewReloader(s.path, discardLogger(), nil, nil, nil, nil, nil)
	if err != nil {
		s.t.Fatal(err)
	}
	s.r = r
	cfg.Management.BackupStore.Token = s.newToken
	cfg.Management.BackupStore.Directory = s.targetDir
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		s.t.Fatal(err)
	}
	server := &http.Server{Handler: r.ManagementHandler(), TLSConfig: r.ManagementTLSConfig()}
	done := make(chan error, 1)
	go func() { done <- server.ServeTLS(listener, "", "") }()
	s.t.Cleanup(func() { _ = server.Close(); <-done })
	s.client = &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: s.roots}}}
	s.listener = listener
}

// request preserves the original closure behavior: build an authorized HTTPS
// request, assert the no-store header, and return status and body.
func (s *backupRecoveryState) request(method, urlPath, token, body string) (int, string) {
	s.t.Helper()
	req, err := http.NewRequest(method, "https://"+s.listener.Addr().String()+urlPath, strings.NewReader(body))
	if err != nil {
		s.t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := s.client.Do(req)
	if err != nil {
		s.t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		s.t.Fatal(err)
	}
	if response.Header.Get("Cache-Control") != "no-store" {
		s.t.Fatal("missing no-store header")
	}
	return response.StatusCode, string(data)
}

// assertCredentialGuards verifies that wrong or insufficient credentials are
// rejected for backup listing, deletion, and proposal access.
func assertCredentialGuards(s *backupRecoveryState) {
	const base = "/management/v1/backups"
	for _, token := range []string{"", s.previewToken, s.proposalToken, s.newToken} {
		if status, _ := s.request(http.MethodGet, base, token, ""); status != http.StatusUnauthorized {
			s.t.Fatalf("wrong credential accepted: %d", status)
		}
	}
	if status, _ := s.request(http.MethodDelete, base+"/"+s.id, s.proposalToken, ""); status != http.StatusUnauthorized {
		s.t.Fatalf("proposal credential could delete backup: %d", status)
	}
	if status, _ := s.request(http.MethodGet, "/management/v1/proposals", s.backupToken, ""); status != http.StatusUnauthorized {
		s.t.Fatalf("backup credential could read proposals: %d", status)
	}
}

// assertListBackups verifies the backup listing contains valid and
// unreadable entries plus the invalid backup id, and that no secrets leak.
func assertListBackups(s *backupRecoveryState) {
	const base = "/management/v1/backups"
	status, body := s.request(http.MethodGet, base, s.backupToken, "")
	if status != http.StatusOK || !strings.Contains(body, `"status":"valid"`) ||
		!strings.Contains(body, `"status":"unreadable_or_invalid"`) || !strings.Contains(body, s.invalidID) {
		s.t.Fatalf("list backups = %d %s", status, body)
	}
	for _, secret := range []string{"confidential", s.targetDir, s.backupDir, s.previewToken, s.proposalToken, s.backupToken} {
		if strings.Contains(body, secret) {
			s.t.Fatal("backup listing exposed source, path, or credential")
		}
	}
}

// assertMetadataAndInvalidBackup verifies valid backup metadata, the 503 for
// an unknown backup, and the 400/204/404 deletion sequence for an invalid id.
func assertMetadataAndInvalidBackup(s *backupRecoveryState) {
	const base = "/management/v1/backups"
	if status, body := s.request(http.MethodGet, base+"/"+s.id, s.backupToken, ""); status != http.StatusOK ||
		!strings.Contains(body, s.preview.Revision) || strings.Contains(body, "confidential") {
		s.t.Fatalf("backup metadata = %d %s", status, body)
	}
	if status, _ := s.request(http.MethodGet, base+"/"+s.invalidID, s.backupToken, ""); status != http.StatusServiceUnavailable {
		s.t.Fatalf("invalid backup metadata status: %d", status)
	}
	if status, _ := s.request(http.MethodDelete, base+"/"+s.invalidID, s.backupToken, "{}"); status != http.StatusBadRequest {
		s.t.Fatalf("backup deletion accepted body: %d", status)
	}
	if status, _ := s.request(http.MethodDelete, base+"/"+s.invalidID, s.backupToken, ""); status != http.StatusNoContent {
		s.t.Fatalf("invalid backup deletion status: %d", status)
	}
	if status, _ := s.request(http.MethodGet, base+"/"+s.invalidID, s.backupToken, ""); status != http.StatusNotFound {
		s.t.Fatalf("deleted backup status: %d", status)
	}
}

// assertListenerAndQueryGuards verifies the raw=1 query is rejected, plaintext
// is forbidden, and the public listener returns 404.
func assertListenerAndQueryGuards(s *backupRecoveryState) {
	const base = "/management/v1/backups"
	if status, _ := s.request(http.MethodGet, base+"?raw=1", s.backupToken, ""); status != http.StatusNotFound {
		s.t.Fatalf("query accepted: %d", status)
	}
	plain := httptest.NewRecorder()
	s.r.ManagementHandler().ServeHTTP(plain, httptest.NewRequest(http.MethodGet, base, nil))
	if plain.Code != http.StatusForbidden {
		s.t.Fatalf("plaintext status: %d", plain.Code)
	}
	public := httptest.NewRecorder()
	s.r.ServeHTTP(public, httptest.NewRequest(http.MethodGet, base, nil))
	if public.Code != http.StatusNotFound {
		s.t.Fatalf("public listener status: %d", public.Code)
	}
}

// assertAliasReloadRejection verifies that reloading with a symlinked backup
// target or proposal directory is rejected by the backup and proposal APIs.
func assertAliasReloadRejection(s *backupRecoveryState) {
	const base = "/management/v1/backups"
	alias := filepath.Join(s.t.TempDir(), "target-alias")
	if err := os.Symlink(s.targetDir, alias); err != nil {
		s.t.Fatal(err)
	}
	s.currentDir = alias
	s.writeConfig()
	if err := s.r.reload(); err != nil {
		s.t.Fatal(err)
	}
	if status, _ := s.request(http.MethodDelete, base+"/"+s.id, s.backupToken, ""); status != http.StatusServiceUnavailable {
		s.t.Fatalf("aliased target directory accepted: %d", status)
	}
	proposalAlias := filepath.Join(s.t.TempDir(), "proposal-alias")
	if err := os.Symlink(s.proposalDir, proposalAlias); err != nil {
		s.t.Fatal(err)
	}
	s.currentDir = proposalAlias
	s.writeConfig()
	if err := s.r.reload(); err != nil {
		s.t.Fatal(err)
	}
	if status, _ := s.request(http.MethodGet, base, s.backupToken, ""); status != http.StatusServiceUnavailable {
		s.t.Fatalf("aliased proposal directory accepted by backup API: %d", status)
	}
	if status, _ := s.request(http.MethodGet, "/management/v1/proposals", s.proposalToken, ""); status != http.StatusServiceUnavailable {
		s.t.Fatalf("aliased backup directory accepted by proposal API: %d", status)
	}
}

// assertTokenRotationAndDisable verifies token rotation after reload, that the
// disabled backup store is inaccessible, and that the target file is unchanged.
func assertTokenRotationAndDisable(s *backupRecoveryState) {
	const base = "/management/v1/backups"
	s.currentDir, s.currentToken = s.backupDir, s.newToken
	s.writeConfig()
	if err := s.r.reload(); err != nil {
		s.t.Fatal(err)
	}
	if status, _ := s.request(http.MethodGet, base, s.backupToken, ""); status != http.StatusUnauthorized {
		s.t.Fatalf("old token accepted after reload: %d", status)
	}
	if status, _ := s.request(http.MethodGet, base+"/"+s.id, s.newToken, ""); status != http.StatusOK {
		s.t.Fatalf("new token rejected: %d", status)
	}
	s.enabled = false
	s.writeConfig()
	if err := s.r.reload(); err != nil {
		s.t.Fatal(err)
	}
	if status, _ := s.request(http.MethodGet, base, s.newToken, ""); status != http.StatusUnauthorized {
		s.t.Fatalf("disabled backup store accessible: %d", status)
	}
	got, err := os.ReadFile(s.targetPath)
	if err != nil || string(got) != string(s.source) {
		s.t.Fatalf("backup operations modified target: %v", err)
	}
}

func TestBackupRecoveryHTTPS(t *testing.T) {
	state := setupBackupRecovery(t)
	state.startBackupServer()
	defer state.r.Stop()
	assertCredentialGuards(state)
	assertListBackups(state)
	assertMetadataAndInvalidBackup(state)
	assertListenerAndQueryGuards(state)
	assertAliasReloadRejection(state)
	assertTokenRotationAndDisable(state)
}

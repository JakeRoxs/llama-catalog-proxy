//go:build linux

package proxy

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
)

func TestMutationOutcomeReportsUncertainAndPostWriteFailures(t *testing.T) {
	tests := []struct {
		name       string
		result     modelconfig.MutationResult
		err        error
		status     string
		durability string
		errorCode  string
		httpCode   int
	}{
		{"success", modelconfig.MutationResult{Written: true, Durable: true, BackupID: "id", Revision: "revision"}, nil, "written", "confirmed", "", 200},
		{"missing outcome", modelconfig.MutationResult{}, nil, "not_written", "not_applicable", "unavailable", 503},
		{"pre-write conflict", modelconfig.MutationResult{}, modelconfig.ErrProposalStale, "not_written", "not_applicable", "conflict", 409},
		{"rename uncertain", modelconfig.MutationResult{OutcomeUnknown: true, BackupID: "id"}, modelconfig.ErrTargetRenameUncertain, "unknown", "unknown", "rename_outcome_unknown", 503},
		{"sync uncertain", modelconfig.MutationResult{Written: true, BackupID: "id"}, modelconfig.ErrTargetCommitUncertain, "written", "uncertain", "directory_sync_failed", 503},
		{"rotation failed", modelconfig.MutationResult{Written: true, Durable: true, BackupID: "id"}, modelconfig.ErrBackupStore, "written", "confirmed", "backup_store_unavailable", 503},
		{"backup cleanup failed", modelconfig.MutationResult{BackupID: "id"}, errors.Join(modelconfig.ErrTargetChanged, modelconfig.ErrBackupStore), "not_written", "not_applicable", "backup_store_unavailable", 503},
		{"cleanup and rename", modelconfig.MutationResult{OutcomeUnknown: true, BackupID: "id"}, errors.Join(modelconfig.ErrTargetRenameUncertain, modelconfig.ErrTargetCleanup), "unknown", "unknown", "temporary_cleanup_failed", 503},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response, code := mutationOutcome(test.result, test.err)
			if code != test.httpCode || response.Status != test.status || response.Durability != test.durability ||
				response.Error != test.errorCode || response.Observed != "unknown" || response.Effective != "unknown" {
				t.Fatalf("outcome = %+v, %d", response, code)
			}
		})
	}
}

// managementWritesHarness holds the shared fixture for the TestManagementWritesHTTPS
// phases. Each phase is a focused top-level helper below the cognitive-complexity
// threshold; the struct keeps the setup, config writer, server, and request
// helper behavior intact while reusing backupConfigTemplate.
type managementWritesHarness struct {
	t *testing.T

	targetDir, proposalDir, backupDir, targetPath    string
	source                                           []byte
	certPath, keyPath                                string
	roots                                            *x509.CertPool
	previewToken, proposalToken, backupToken         string
	writeToken, newToken                             string
	currentToken, currentBackupDir, currentTargetDir string
	enabled                                          bool
	path                                             string
	r                                                *Reloader
	listener                                         net.Listener
	client                                           *http.Client

	proposalID       string
	snapshotRevision string
	appliedBackupID  string
	writtenContent   []byte
}

func newManagementWritesHarness(t *testing.T) *managementWritesHarness {
	h := &managementWritesHarness{t: t}
	h.setupDirectoriesAndSource()
	h.setupCertificatesAndTokens()
	h.setupConfig()
	h.startServerAndClient()
	return h
}

func (h *managementWritesHarness) setupDirectoriesAndSource() {
	h.t.Helper()
	h.targetDir, h.proposalDir, h.backupDir = h.t.TempDir(), h.t.TempDir(), h.t.TempDir()
	for _, directory := range []string{h.proposalDir, h.backupDir} {
		if err := os.Chmod(directory, 0o700); err != nil {
			h.t.Fatal(err)
		}
	}
	h.source = []byte("# confidential\nmodels: {m: {cmd: 'llama-server -c 1024'}}\n")
	h.targetPath = filepath.Join(h.targetDir, "config.yaml")
	if err := os.WriteFile(h.targetPath, h.source, 0o640); err != nil {
		h.t.Fatal(err)
	}
}

func (h *managementWritesHarness) setupCertificatesAndTokens() {
	h.t.Helper()
	h.certPath, h.keyPath, h.roots = peerTestCertificate(h.t)
	h.previewToken, h.proposalToken = strings.Repeat("m", 32), strings.Repeat("p", 32)
	h.backupToken, h.writeToken, h.newToken = strings.Repeat("b", 32), strings.Repeat("w", 32), strings.Repeat("n", 32)
	h.currentToken, h.currentBackupDir, h.currentTargetDir, h.enabled = h.writeToken, h.backupDir, h.targetDir, true
	h.path = configPath(h.t)
}

func (h *managementWritesHarness) writeConfig() {
	h.t.Helper()
	content := backupConfigTemplate(h.certPath, h.keyPath, h.previewToken, h.proposalDir, h.proposalToken, h.currentTargetDir)
	content += fmt.Sprintf("  backup_store: {directory: %q, token: %q, max_backups: 1}\n", h.currentBackupDir, h.backupToken)
	if h.enabled {
		content += fmt.Sprintf("  write_token: %q\n", h.currentToken)
	}
	if err := os.WriteFile(h.path, []byte(content), 0o600); err != nil {
		h.t.Fatal(err)
	}
}

func (h *managementWritesHarness) setupConfig() {
	h.t.Helper()
	h.writeConfig()
	r, cfg, err := NewReloader(h.path, discardLogger(), nil, nil, nil, nil, nil)
	if err != nil {
		h.t.Fatal(err)
	}
	h.r = r
	cfg.Management.WriteToken = h.newToken
	h.t.Cleanup(h.r.Stop)
}

func (h *managementWritesHarness) startServerAndClient() {
	h.t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		h.t.Fatal(err)
	}
	server := &http.Server{Handler: h.r.ManagementHandler(), TLSConfig: h.r.ManagementTLSConfig()}
	done := make(chan error, 1)
	go func() { done <- server.ServeTLS(listener, "", "") }()
	h.t.Cleanup(func() { _ = server.Close(); <-done })
	h.listener = listener
	h.client = &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: h.roots}}}
	h.t.Cleanup(h.client.CloseIdleConnections)
}

func (h *managementWritesHarness) request(method, urlPath, token, body string) (int, string, error) {
	h.t.Helper()
	req, err := http.NewRequest(method, "https://"+h.listener.Addr().String()+urlPath, strings.NewReader(body))
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := h.client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if response.Header.Get("Cache-Control") != "no-store" {
		return 0, "", errors.New("missing no-store header")
	}
	return response.StatusCode, string(data), err
}

func (h *managementWritesHarness) mustRequest(method, urlPath, token, body string) (int, string) {
	h.t.Helper()
	code, response, err := h.request(method, urlPath, token, body)
	if err != nil {
		h.t.Fatal(err)
	}
	return code, response
}

// Phase helpers -----------------------------------------------------------

func runSettingsAndProposalPhase(h *managementWritesHarness) {
	h.t.Helper()
	settings := `{"target_id":"local","model_id":"m"}`
	status, body := h.mustRequest(http.MethodPost, "/management/v1/settings", h.previewToken, settings)
	var snapshot struct {
		Revision string `json:"revision"`
	}
	if status != http.StatusOK || json.Unmarshal([]byte(body), &snapshot) != nil || len(snapshot.Revision) != 64 {
		h.t.Fatalf("settings = %d %s", status, body)
	}
	h.snapshotRevision = snapshot.Revision
	proposalRequest := fmt.Sprintf(`{"target_id":"local","model_id":"m","revision":%q,"settings":{"context_size":4096}}`, h.snapshotRevision)
	status, body = h.mustRequest(http.MethodPost, "/management/v1/proposals", h.proposalToken, proposalRequest)
	var proposal struct {
		ID string `json:"id"`
	}
	if status != http.StatusCreated || json.Unmarshal([]byte(body), &proposal) != nil || len(proposal.ID) != 32 {
		h.t.Fatalf("proposal = %d %s", status, body)
	}
	h.proposalID = proposal.ID
}

func runCredentialAndGuardPhase(h *managementWritesHarness) {
	h.t.Helper()
	apply := "/management/v1/proposals/" + h.proposalID + "/apply"
	acks := `{"acknowledge_review":true,"acknowledge_disruption":true,"acknowledge_exclusive_window":true}`
	for _, token := range []string{"", h.previewToken, h.proposalToken, h.backupToken, h.newToken} {
		if code, _ := h.mustRequest(http.MethodPost, apply, token, acks); code != http.StatusUnauthorized {
			h.t.Fatalf("wrong apply credential accepted: %d", code)
		}
	}
	if code, _ := h.mustRequest(http.MethodGet, "/management/v1/backups", h.writeToken, ""); code != http.StatusUnauthorized {
		h.t.Fatalf("write credential granted backup management: %d", code)
	}
	for _, invalid := range []string{
		`{}`, `{"acknowledge_disruption":true}`, `{"acknowledge_exclusive_window":true}`,
		`{"acknowledge_disruption":true,"acknowledge_exclusive_window":true}`,
		`{"acknowledge_review":true,"acknowledge_disruption":false,"acknowledge_exclusive_window":true}`,
		`{"acknowledge_review":true,"acknowledge_disruption":true,"ACKNOWLEDGE_DISRUPTION":true,"acknowledge_exclusive_window":true}`,
		`{"acknowledge_review":true,"acknowledge_disruption":true,"acknowledge_exclusive_window":true,"extra":1}`,
	} {
		if code, _ := h.mustRequest(http.MethodPost, apply, h.writeToken, invalid); code != http.StatusBadRequest {
			h.t.Fatalf("invalid acknowledgements accepted: %d", code)
		}
	}
	if code, _ := h.mustRequest(http.MethodPost, apply, h.writeToken, strings.Repeat(" ", maxManagementRequest+1)); code != http.StatusRequestEntityTooLarge {
		h.t.Fatalf("oversized request accepted: %d", code)
	}
	if got, err := os.ReadFile(h.targetPath); err != nil || string(got) != string(h.source) {
		h.t.Fatalf("unauthorized request changed target: %v", err)
	}
}

func runAliasAndReboundPhase(h *managementWritesHarness) {
	h.t.Helper()
	apply := "/management/v1/proposals/" + h.proposalID + "/apply"
	acks := `{"acknowledge_review":true,"acknowledge_disruption":true,"acknowledge_exclusive_window":true}`

	alias := filepath.Join(h.t.TempDir(), "target-alias")
	if err := os.Symlink(h.targetDir, alias); err != nil {
		h.t.Fatal(err)
	}
	h.currentBackupDir = alias
	h.writeConfig()
	if err := h.r.reload(); err != nil {
		h.t.Fatal(err)
	}
	if code, _ := h.mustRequest(http.MethodPost, apply, h.writeToken, acks); code != http.StatusServiceUnavailable {
		h.t.Fatalf("aliased backup store accepted: %d", code)
	}

	otherTarget := h.t.TempDir()
	if err := os.WriteFile(filepath.Join(otherTarget, "config.yaml"), h.source, 0o640); err != nil {
		h.t.Fatal(err)
	}
	h.currentBackupDir, h.currentTargetDir = h.backupDir, otherTarget
	h.writeConfig()
	if err := h.r.reload(); err != nil {
		h.t.Fatal(err)
	}
	if code, _ := h.mustRequest(http.MethodPost, apply, h.writeToken, acks); code != http.StatusConflict {
		h.t.Fatalf("rebound proposal target accepted: %d", code)
	}
	if got, err := os.ReadFile(filepath.Join(otherTarget, "config.yaml")); err != nil || string(got) != string(h.source) {
		h.t.Fatalf("rebound target was changed: %v", err)
	}
	h.currentTargetDir = h.targetDir
	h.writeConfig()
	if err := h.r.reload(); err != nil {
		h.t.Fatal(err)
	}
}

func runRevocationPhase(h *managementWritesHarness) {
	h.t.Helper()
	apply := "/management/v1/proposals/" + h.proposalID + "/apply"
	acks := `{"acknowledge_review":true,"acknowledge_disruption":true,"acknowledge_exclusive_window":true}`

	type requestResult struct {
		code int
		body string
		err  error
	}
	h.r.lifecycleMu.Lock()
	waiting := make(chan requestResult, 1)
	go func() {
		code, body, err := h.request(http.MethodPost, apply, h.writeToken, acks)
		waiting <- requestResult{code, body, err}
	}()
	deadline := time.Now().Add(2 * time.Second)
	for len(h.r.managementSlots) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(h.r.managementSlots) == 0 {
		h.r.lifecycleMu.Unlock()
		h.t.Fatal("request did not reach the configuration lock")
	}
	oldState := h.r.state.Load()
	changedState := *oldState
	h.r.state.Store(&changedState)
	h.r.lifecycleMu.Unlock()
	pending := <-waiting
	h.r.lifecycleMu.Lock()
	h.r.state.Store(oldState)
	h.r.lifecycleMu.Unlock()
	if pending.err != nil || pending.code != http.StatusServiceUnavailable {
		h.t.Fatalf("revoked in-flight configuration accepted: %d %v", pending.code, pending.err)
	}
	if got, err := os.ReadFile(h.targetPath); err != nil || string(got) != string(h.source) {
		h.t.Fatalf("in-flight revocation changed target: %v", err)
	}
}

func runConcurrencyPhase(h *managementWritesHarness) {
	h.t.Helper()
	apply := "/management/v1/proposals/" + h.proposalID + "/apply"
	acks := `{"acknowledge_review":true,"acknowledge_disruption":true,"acknowledge_exclusive_window":true}`

	type requestResult struct {
		code int
		body string
		err  error
	}
	results := make(chan requestResult, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			code, body, err := h.request(http.MethodPost, apply, h.writeToken, acks)
			results <- requestResult{code, body, err}
		}()
	}
	wg.Wait()
	close(results)
	var applied mutationResponse
	successes, conflicts := 0, 0
	for result := range results {
		if result.err != nil {
			h.t.Fatal(result.err)
		}
		switch result.code {
		case http.StatusOK:
			successes++
			if err := json.Unmarshal([]byte(result.body), &applied); err != nil {
				h.t.Fatal(err)
			}
		case http.StatusConflict:
			conflicts++
		default:
			h.t.Fatalf("concurrent apply = %d %s", result.code, result.body)
		}
		for _, secret := range []string{"confidential", h.targetDir, h.backupDir, h.proposalDir, h.writeToken} {
			if strings.Contains(result.body, secret) {
				h.t.Fatal("mutation response exposed source, path, or credential")
			}
		}
	}
	if successes != 1 || conflicts != 1 || applied.Status != "written" || applied.Durability != "confirmed" ||
		applied.Observed != "unknown" || applied.Effective != "unknown" || len(applied.BackupID) != 32 {
		h.t.Fatalf("concurrent apply: successes=%d conflicts=%d result=%+v", successes, conflicts, applied)
	}
	written, err := os.ReadFile(h.targetPath)
	if err != nil || string(written) != strings.Replace(string(h.source), "-c 1024", "-c 4096", 1) {
		h.t.Fatalf("applied content: %v", err)
	}
	h.appliedBackupID = applied.BackupID
	h.writtenContent = written
}

func runBackupAndRollbackPhase(h *managementWritesHarness) {
	h.t.Helper()
	if code, body := h.mustRequest(http.MethodGet, "/management/v1/backups/"+h.appliedBackupID, h.backupToken, ""); code != http.StatusOK ||
		!strings.Contains(body, h.snapshotRevision) || strings.Contains(body, "confidential") {
		h.t.Fatalf("backup metadata = %d %s", code, body)
	}
	rollback := "/management/v1/backups/" + h.appliedBackupID + "/rollback"
	acks := `{"acknowledge_review":true,"acknowledge_disruption":true,"acknowledge_exclusive_window":true}`
	if code, _ := h.mustRequest(http.MethodPost, rollback, h.backupToken, acks); code != http.StatusUnauthorized {
		h.t.Fatalf("backup credential granted rollback: %d", code)
	}
	if code, _ := h.mustRequest(http.MethodPost, rollback, h.writeToken, "{}"); code != http.StatusBadRequest {
		h.t.Fatalf("rollback accepted no acknowledgements: %d", code)
	}
	if err := os.WriteFile(h.targetPath, append(h.writtenContent, []byte("# external\n")...), 0o640); err != nil {
		h.t.Fatal(err)
	}
	if code, _ := h.mustRequest(http.MethodPost, rollback, h.writeToken, acks); code != http.StatusConflict {
		h.t.Fatalf("stale rollback accepted: %d", code)
	}
	if err := os.WriteFile(h.targetPath, h.writtenContent, 0o640); err != nil {
		h.t.Fatal(err)
	}
	status, body := h.mustRequest(http.MethodPost, rollback, h.writeToken, acks)
	var rolledBack mutationResponse
	if status != http.StatusOK || json.Unmarshal([]byte(body), &rolledBack) != nil ||
		rolledBack.Status != "written" || rolledBack.Durability != "confirmed" || len(rolledBack.BackupID) != 32 {
		h.t.Fatalf("rollback = %d %s", status, body)
	}
	if got, err := os.ReadFile(h.targetPath); err != nil || string(got) != string(h.source) {
		h.t.Fatalf("rollback content: %v", err)
	}
	if code, _ := h.mustRequest(http.MethodGet, "/management/v1/backups/"+h.appliedBackupID, h.backupToken, ""); code != http.StatusNotFound {
		h.t.Fatalf("oldest backup was not rotated: %d", code)
	}
	reverse := "/management/v1/backups/" + rolledBack.BackupID + "/rollback"
	if code, body := h.mustRequest(http.MethodPost, reverse, h.writeToken, acks); code != http.StatusOK {
		h.t.Fatalf("reverse rollback = %d %s", code, body)
	}
	if got, err := os.ReadFile(h.targetPath); err != nil || string(got) != string(h.writtenContent) {
		h.t.Fatalf("reverse rollback content: %v", err)
	}
	if info, err := os.Stat(h.targetPath); err != nil || info.Mode().Perm() != 0o640 {
		h.t.Fatalf("target permissions: %v", err)
	}
}

func runListenerAndTokenPhase(h *managementWritesHarness) {
	h.t.Helper()
	apply := "/management/v1/proposals/" + h.proposalID + "/apply"
	acks := `{"acknowledge_review":true,"acknowledge_disruption":true,"acknowledge_exclusive_window":true}`

	plain := httptest.NewRecorder()
	h.r.ManagementHandler().ServeHTTP(plain, httptest.NewRequest(http.MethodPost, apply, strings.NewReader(acks)))
	if plain.Code != http.StatusForbidden {
		h.t.Fatalf("plaintext write status: %d", plain.Code)
	}
	public := httptest.NewRecorder()
	h.r.ServeHTTP(public, httptest.NewRequest(http.MethodPost, apply, strings.NewReader(acks)))
	if public.Code != http.StatusNotFound {
		h.t.Fatalf("public write status: %d", public.Code)
	}
	h.currentToken = h.newToken
	h.writeConfig()
	if err := h.r.reload(); err != nil {
		h.t.Fatal(err)
	}
	if code, _ := h.mustRequest(http.MethodPost, apply, h.writeToken, acks); code != http.StatusUnauthorized {
		h.t.Fatalf("revoked write token accepted: %d", code)
	}
	if code, _ := h.mustRequest(http.MethodGet, apply, h.newToken, ""); code != http.StatusMethodNotAllowed {
		h.t.Fatalf("new write token rejected: %d", code)
	}
	h.enabled = false
	h.writeConfig()
	if err := h.r.reload(); err != nil {
		h.t.Fatal(err)
	}
	if code, _ := h.mustRequest(http.MethodPost, apply, h.newToken, acks); code != http.StatusUnauthorized {
		h.t.Fatalf("disabled write token accepted: %d", code)
	}
}

func TestManagementWritesHTTPS(t *testing.T) {
	h := newManagementWritesHarness(t)
	runSettingsAndProposalPhase(h)
	runCredentialAndGuardPhase(h)
	runAliasAndReboundPhase(h)
	runRevocationPhase(h)
	runConcurrencyPhase(h)
	runBackupAndRollbackPhase(h)
	runListenerAndTokenPhase(h)
}

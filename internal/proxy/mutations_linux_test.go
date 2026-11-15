//go:build linux

package proxy

import (
	"crypto/tls"
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

func TestManagementWritesHTTPS(t *testing.T) {
	targetDir, proposalDir, backupDir := t.TempDir(), t.TempDir(), t.TempDir()
	for _, directory := range []string{proposalDir, backupDir} {
		if err := os.Chmod(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	source := "# confidential\nmodels: {m: {cmd: 'llama-server -c 1024'}}\n"
	targetPath := filepath.Join(targetDir, "config.yaml")
	if err := os.WriteFile(targetPath, []byte(source), 0o640); err != nil {
		t.Fatal(err)
	}
	certPath, keyPath, roots := peerTestCertificate(t)
	previewToken, proposalToken := strings.Repeat("m", 32), strings.Repeat("p", 32)
	backupToken, writeToken, newToken := strings.Repeat("b", 32), strings.Repeat("w", 32), strings.Repeat("n", 32)
	currentToken, currentBackupDir, currentTargetDir, enabled := writeToken, backupDir, targetDir, true
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
  backup_store: {directory: %q, token: %q, max_backups: 1}
  targets:
    local: {directory: %q, filename: config.yaml}
`, certPath, keyPath, previewToken, proposalDir, proposalToken, currentBackupDir, backupToken, currentTargetDir)
		if enabled {
			content += fmt.Sprintf("  write_token: %q\n", currentToken)
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
	cfg.Management.WriteToken = newToken
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: r.ManagementHandler(), TLSConfig: r.ManagementTLSConfig()}
	done := make(chan error, 1)
	go func() { done <- server.ServeTLS(listener, "", "") }()
	t.Cleanup(func() { _ = server.Close(); <-done })
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}}}
	defer client.CloseIdleConnections()
	request := func(method, urlPath, token, body string) (int, string, error) {
		req, err := http.NewRequest(method, "https://"+listener.Addr().String()+urlPath, strings.NewReader(body))
		if err != nil {
			return 0, "", err
		}
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		response, err := client.Do(req)
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
	mustRequest := func(method, urlPath, token, body string) (int, string) {
		t.Helper()
		code, response, err := request(method, urlPath, token, body)
		if err != nil {
			t.Fatal(err)
		}
		return code, response
	}
	settings := `{"target_id":"local","model_id":"m"}`
	status, body := mustRequest(http.MethodPost, "/management/v1/settings", previewToken, settings)
	var snapshot struct {
		Revision string `json:"revision"`
	}
	if status != http.StatusOK || json.Unmarshal([]byte(body), &snapshot) != nil || len(snapshot.Revision) != 64 {
		t.Fatalf("settings = %d %s", status, body)
	}
	proposalRequest := fmt.Sprintf(`{"target_id":"local","model_id":"m","revision":%q,"settings":{"context_size":4096}}`, snapshot.Revision)
	status, body = mustRequest(http.MethodPost, "/management/v1/proposals", proposalToken, proposalRequest)
	var proposal struct {
		ID string `json:"id"`
	}
	if status != http.StatusCreated || json.Unmarshal([]byte(body), &proposal) != nil || len(proposal.ID) != 32 {
		t.Fatalf("proposal = %d %s", status, body)
	}
	apply := "/management/v1/proposals/" + proposal.ID + "/apply"
	acks := `{"acknowledge_review":true,"acknowledge_disruption":true,"acknowledge_exclusive_window":true}`
	for _, token := range []string{"", previewToken, proposalToken, backupToken, newToken} {
		if code, _ := mustRequest(http.MethodPost, apply, token, acks); code != http.StatusUnauthorized {
			t.Fatalf("wrong apply credential accepted: %d", code)
		}
	}
	if code, _ := mustRequest(http.MethodGet, "/management/v1/backups", writeToken, ""); code != http.StatusUnauthorized {
		t.Fatalf("write credential granted backup management: %d", code)
	}
	for _, invalid := range []string{
		`{}`, `{"acknowledge_disruption":true}`, `{"acknowledge_exclusive_window":true}`,
		`{"acknowledge_disruption":true,"acknowledge_exclusive_window":true}`,
		`{"acknowledge_review":true,"acknowledge_disruption":false,"acknowledge_exclusive_window":true}`,
		`{"acknowledge_review":true,"acknowledge_disruption":true,"ACKNOWLEDGE_DISRUPTION":true,"acknowledge_exclusive_window":true}`,
		`{"acknowledge_review":true,"acknowledge_disruption":true,"acknowledge_exclusive_window":true,"extra":1}`,
	} {
		if code, _ := mustRequest(http.MethodPost, apply, writeToken, invalid); code != http.StatusBadRequest {
			t.Fatalf("invalid acknowledgements accepted: %d", code)
		}
	}
	if code, _ := mustRequest(http.MethodPost, apply, writeToken, strings.Repeat(" ", maxManagementRequest+1)); code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized request accepted: %d", code)
	}
	if got, err := os.ReadFile(targetPath); err != nil || string(got) != source {
		t.Fatalf("unauthorized request changed target: %v", err)
	}
	alias := filepath.Join(t.TempDir(), "target-alias")
	if err := os.Symlink(targetDir, alias); err != nil {
		t.Fatal(err)
	}
	currentBackupDir = alias
	writeConfig()
	if err := r.reload(); err != nil {
		t.Fatal(err)
	}
	if code, _ := mustRequest(http.MethodPost, apply, writeToken, acks); code != http.StatusServiceUnavailable {
		t.Fatalf("aliased backup store accepted: %d", code)
	}
	otherTarget := t.TempDir()
	if err := os.WriteFile(filepath.Join(otherTarget, "config.yaml"), []byte(source), 0o640); err != nil {
		t.Fatal(err)
	}
	currentBackupDir, currentTargetDir = backupDir, otherTarget
	writeConfig()
	if err := r.reload(); err != nil {
		t.Fatal(err)
	}
	if code, _ := mustRequest(http.MethodPost, apply, writeToken, acks); code != http.StatusConflict {
		t.Fatalf("rebound proposal target accepted: %d", code)
	}
	if got, err := os.ReadFile(filepath.Join(otherTarget, "config.yaml")); err != nil || string(got) != source {
		t.Fatalf("rebound target was changed: %v", err)
	}
	currentTargetDir = targetDir
	writeConfig()
	if err := r.reload(); err != nil {
		t.Fatal(err)
	}

	type requestResult struct {
		code int
		body string
		err  error
	}
	r.lifecycleMu.Lock()
	waiting := make(chan requestResult, 1)
	go func() {
		code, body, err := request(http.MethodPost, apply, writeToken, acks)
		waiting <- requestResult{code, body, err}
	}()
	deadline := time.Now().Add(2 * time.Second)
	for len(r.managementSlots) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(r.managementSlots) == 0 {
		r.lifecycleMu.Unlock()
		t.Fatal("request did not reach the configuration lock")
	}
	oldState := r.state.Load()
	changedState := *oldState
	r.state.Store(&changedState)
	r.lifecycleMu.Unlock()
	pending := <-waiting
	r.lifecycleMu.Lock()
	r.state.Store(oldState)
	r.lifecycleMu.Unlock()
	if pending.err != nil || pending.code != http.StatusServiceUnavailable {
		t.Fatalf("revoked in-flight configuration accepted: %d %v", pending.code, pending.err)
	}
	if got, err := os.ReadFile(targetPath); err != nil || string(got) != source {
		t.Fatalf("in-flight revocation changed target: %v", err)
	}

	results := make(chan requestResult, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			code, body, err := request(http.MethodPost, apply, writeToken, acks)
			results <- requestResult{code, body, err}
		}()
	}
	wg.Wait()
	close(results)
	var applied mutationResponse
	successes, conflicts := 0, 0
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		switch result.code {
		case http.StatusOK:
			successes++
			if err := json.Unmarshal([]byte(result.body), &applied); err != nil {
				t.Fatal(err)
			}
		case http.StatusConflict:
			conflicts++
		default:
			t.Fatalf("concurrent apply = %d %s", result.code, result.body)
		}
		for _, secret := range []string{"confidential", targetDir, backupDir, proposalDir, writeToken} {
			if strings.Contains(result.body, secret) {
				t.Fatal("mutation response exposed source, path, or credential")
			}
		}
	}
	if successes != 1 || conflicts != 1 || applied.Status != "written" || applied.Durability != "confirmed" ||
		applied.Observed != "unknown" || applied.Effective != "unknown" || len(applied.BackupID) != 32 {
		t.Fatalf("concurrent apply: successes=%d conflicts=%d result=%+v", successes, conflicts, applied)
	}
	written, err := os.ReadFile(targetPath)
	if err != nil || string(written) != strings.Replace(source, "-c 1024", "-c 4096", 1) {
		t.Fatalf("applied content: %v", err)
	}
	if code, body := mustRequest(http.MethodGet, "/management/v1/backups/"+applied.BackupID, backupToken, ""); code != http.StatusOK ||
		!strings.Contains(body, snapshot.Revision) || strings.Contains(body, "confidential") {
		t.Fatalf("backup metadata = %d %s", code, body)
	}
	rollback := "/management/v1/backups/" + applied.BackupID + "/rollback"
	if code, _ := mustRequest(http.MethodPost, rollback, backupToken, acks); code != http.StatusUnauthorized {
		t.Fatalf("backup credential granted rollback: %d", code)
	}
	if code, _ := mustRequest(http.MethodPost, rollback, writeToken, "{}"); code != http.StatusBadRequest {
		t.Fatalf("rollback accepted no acknowledgements: %d", code)
	}
	if err := os.WriteFile(targetPath, append(written, []byte("# external\n")...), 0o640); err != nil {
		t.Fatal(err)
	}
	if code, _ := mustRequest(http.MethodPost, rollback, writeToken, acks); code != http.StatusConflict {
		t.Fatalf("stale rollback accepted: %d", code)
	}
	if err := os.WriteFile(targetPath, written, 0o640); err != nil {
		t.Fatal(err)
	}
	status, body = mustRequest(http.MethodPost, rollback, writeToken, acks)
	var rolledBack mutationResponse
	if status != http.StatusOK || json.Unmarshal([]byte(body), &rolledBack) != nil ||
		rolledBack.Status != "written" || rolledBack.Durability != "confirmed" || len(rolledBack.BackupID) != 32 {
		t.Fatalf("rollback = %d %s", status, body)
	}
	if got, err := os.ReadFile(targetPath); err != nil || string(got) != source {
		t.Fatalf("rollback content: %v", err)
	}
	if code, _ := mustRequest(http.MethodGet, "/management/v1/backups/"+applied.BackupID, backupToken, ""); code != http.StatusNotFound {
		t.Fatalf("oldest backup was not rotated: %d", code)
	}
	reverse := "/management/v1/backups/" + rolledBack.BackupID + "/rollback"
	if code, body := mustRequest(http.MethodPost, reverse, writeToken, acks); code != http.StatusOK {
		t.Fatalf("reverse rollback = %d %s", code, body)
	}
	if got, err := os.ReadFile(targetPath); err != nil || string(got) != string(written) {
		t.Fatalf("reverse rollback content: %v", err)
	}
	if info, err := os.Stat(targetPath); err != nil || info.Mode().Perm() != 0o640 {
		t.Fatalf("target permissions: %v", err)
	}
	plain := httptest.NewRecorder()
	r.ManagementHandler().ServeHTTP(plain, httptest.NewRequest(http.MethodPost, apply, strings.NewReader(acks)))
	if plain.Code != http.StatusForbidden {
		t.Fatalf("plaintext write status: %d", plain.Code)
	}
	public := httptest.NewRecorder()
	r.ServeHTTP(public, httptest.NewRequest(http.MethodPost, apply, strings.NewReader(acks)))
	if public.Code != http.StatusNotFound {
		t.Fatalf("public write status: %d", public.Code)
	}
	currentToken = newToken
	writeConfig()
	if err := r.reload(); err != nil {
		t.Fatal(err)
	}
	if code, _ := mustRequest(http.MethodPost, apply, writeToken, acks); code != http.StatusUnauthorized {
		t.Fatalf("revoked write token accepted: %d", code)
	}
	if code, _ := mustRequest(http.MethodGet, apply, newToken, ""); code != http.StatusMethodNotAllowed {
		t.Fatalf("new write token rejected: %d", code)
	}
	enabled = false
	writeConfig()
	if err := r.reload(); err != nil {
		t.Fatal(err)
	}
	if code, _ := mustRequest(http.MethodPost, apply, newToken, acks); code != http.StatusUnauthorized {
		t.Fatalf("disabled write token accepted: %d", code)
	}
}

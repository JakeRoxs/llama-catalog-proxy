//go:build linux

package proxy

import (
	"crypto/tls"
	"crypto/x509"
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

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

func TestProposalStoreRejectsTargetDirectoryAliases(t *testing.T) {
	target, other := t.TempDir(), t.TempDir()
	link := filepath.Join(other, "target-alias")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	targets := map[string]config.ManagementTarget{"local": {Directory: target, Filename: "config.yaml"}}
	if separateProposalDirectory(link, targets) || !separateProposalDirectory(other, targets) {
		t.Fatal("proposal store directory separation failed")
	}
}

func TestManagementHTTPSPreviewAndReload(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "config.yaml")
	source := "secret: confidential\nmodels:\n  example:\n    cmd: llama-server -m /private/model.gguf -c 1024\n"
	if err := os.WriteFile(target, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := os.Symlink(target, filepath.Join(dir, "link.yaml")); err != nil {
		t.Fatal(err)
	}

	certPath, keyPath, roots := peerTestCertificate(t)
	token, newToken := strings.Repeat("m", 32), strings.Repeat("n", 32)
	proposalToken, newProposalToken := strings.Repeat("p", 32), strings.Repeat("q", 32)
	delegationToken, newDelegationToken := strings.Repeat("l", 32), strings.Repeat("r", 32)
	storeDir := t.TempDir()
	if err := os.Chmod(storeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := configPath(t)
	writeConfig := func(credential, listen, certificate string, enabled bool) {
		t.Helper()
		content := "default_backend: local\nbackends: {local: {url: http://localhost:9292}}\n"
		if enabled {
			content += fmt.Sprintf(`management:
  listen: %q
  tls_cert_file: %q
  tls_key_file: %q
  preview_token: %q
  delegation_token: %q
  proposal_store: {directory: %q, token: %q}
  targets:
    local: {directory: %q, filename: config.yaml}
    link: {directory: %q, filename: link.yaml}
`, listen, certificate, keyPath, credential, delegationToken, storeDir, proposalToken, dir, dir)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeConfig(token, "127.0.0.1:9294", certPath, true)
	r, cfg, err := NewReloader(path, discardLogger(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	cfg.Management.PreviewToken = newToken
	cfg.Management.DelegationToken = newDelegationToken
	cfg.Management.ProposalStore.Token = newProposalToken
	cfg.Management.Targets["local"] = cfg.Management.Targets["link"]
	delegationRequest := httptest.NewRequest(http.MethodPost, "https://proxy.example/management/v1/delegation", nil)
	delegationRequest.Header.Set("Authorization", "Bearer "+delegationToken)
	oldState := r.state.Load()
	if !r.delegatedCallerAuthorized(delegationRequest, oldState) {
		t.Fatal("configured delegated caller not retained in snapshot")
	}

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
	endpoint := "https://" + listener.Addr().String() + "/management/v1/preview"
	untrusted := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: x509.NewCertPool()}}}
	defer untrusted.CloseIdleConnections()
	if response, err := untrusted.Get(endpoint); err == nil {
		_ = response.Body.Close()
		t.Fatal("untrusted certificate accepted")
	}
	valid := `{"target_id":"local","model_id":"example","settings":{"context_size":4096}}`
	requestMethod := func(method, path, credential, body string) (int, string) {
		t.Helper()
		req, err := http.NewRequest(method, "https://"+listener.Addr().String()+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		if credential != "" {
			req.Header.Set("Authorization", "Bearer "+credential)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if resp.Header.Get("Cache-Control") != "no-store" {
			t.Fatal("missing no-store header")
		}
		return resp.StatusCode, string(data)
	}
	requestTo := func(path, credential, body string) (int, string) {
		t.Helper()
		return requestMethod(http.MethodPost, path, credential, body)
	}
	request := func(credential, body string) (int, string) {
		t.Helper()
		return requestTo("/management/v1/preview", credential, body)
	}
	for _, invalid := range []string{"", newToken, strings.Repeat("x", 32)} {
		if status, _ := request(invalid, valid); status != http.StatusUnauthorized {
			t.Fatalf("invalid credential status = %d", status)
		}
	}
	status, body := request(token, valid)
	if status != http.StatusOK || !strings.Contains(body, `"before":"1024"`) ||
		!strings.Contains(body, `"after":"4096"`) || !strings.Contains(body, `"revision":`) ||
		!strings.Contains(body, `"validation":"yaml_syntax_and_supported_command_only"`) {
		t.Fatalf("preview = %d %s", status, body)
	}
	for _, secret := range []string{"confidential", "/private/model.gguf", dir, token} {
		if strings.Contains(body, secret) {
			t.Fatal("preview exposed source data or credential")
		}
	}
	settingsBody := `{"target_id":"local","model_id":"example"}`
	readStatus, readBody := requestTo("/management/v1/settings", token, settingsBody)
	if readStatus != http.StatusOK || !strings.Contains(readBody, `"settings":{"context_size":1024}`) ||
		!strings.Contains(readBody, `"revision":`) {
		t.Fatalf("settings = %d %s", readStatus, readBody)
	}
	for _, secret := range []string{"confidential", "/private/model.gguf", dir, token} {
		if strings.Contains(readBody, secret) {
			t.Fatal("settings exposed source data or credential")
		}
	}
	for _, test := range []struct {
		token string
		body  string
		want  int
	}{
		{"", settingsBody, http.StatusUnauthorized},
		{newToken, settingsBody, http.StatusUnauthorized},
		{token, strings.Replace(settingsBody, `"local"`, `"missing"`, 1), http.StatusNotFound},
		{token, strings.Replace(settingsBody, `"local"`, `"link"`, 1), http.StatusBadRequest},
		{token, valid, http.StatusBadRequest},
		{token, `{"target_id":"local","TARGET_ID":"link","model_id":"example"}`, http.StatusBadRequest},
	} {
		status, body := requestTo("/management/v1/settings", test.token, test.body)
		if status != test.want || strings.Contains(body, dir) || strings.Contains(body, "confidential") {
			t.Fatalf("settings request: status=%d want=%d body=%s", status, test.want, body)
		}
	}
	var snapshot struct {
		Revision string `json:"revision"`
	}
	if err := json.Unmarshal([]byte(readBody), &snapshot); err != nil || len(snapshot.Revision) != 64 {
		t.Fatalf("invalid settings revision: %v", err)
	}
	proposalPath := "/management/v1/proposals"
	proposalBody := fmt.Sprintf(`{"target_id":"local","model_id":"example","revision":%q,"settings":{"context_size":4096}}`, snapshot.Revision)
	if status, _ := requestTo(proposalPath, token, proposalBody); status != http.StatusUnauthorized {
		t.Fatalf("preview credential can create proposal: %d", status)
	}
	if status, _ := requestTo(proposalPath, newProposalToken, proposalBody); status != http.StatusUnauthorized {
		t.Fatalf("modified returned config changed proposal token: %d", status)
	}
	if status, _ := requestTo("/management/v1/settings", proposalToken, settingsBody); status != http.StatusUnauthorized {
		t.Fatalf("proposal credential can read settings endpoint: %d", status)
	}
	if err := os.WriteFile(target, []byte(source+"# external edit\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if status, _ := requestTo(proposalPath, proposalToken, proposalBody); status != http.StatusConflict {
		t.Fatalf("stale proposal accepted: %d", status)
	}
	if err := os.WriteFile(target, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	status, body = requestTo(proposalPath, proposalToken, proposalBody)
	if status != http.StatusCreated {
		t.Fatalf("create proposal = %d %s", status, body)
	}
	var proposal struct {
		ID       string `json:"id"`
		Revision string `json:"revision"`
	}
	if err := json.Unmarshal([]byte(body), &proposal); err != nil || len(proposal.ID) != 32 || proposal.Revision != snapshot.Revision {
		t.Fatalf("invalid created proposal: %s %v", body, err)
	}
	for _, secret := range []string{"confidential", "/private/model.gguf", dir, storeDir, token, proposalToken} {
		if strings.Contains(body, secret) {
			t.Fatal("proposal exposed source data or credential")
		}
	}
	if status, body := requestMethod(http.MethodGet, proposalPath, proposalToken, ""); status != http.StatusOK || !strings.Contains(body, proposal.ID) {
		t.Fatalf("list proposals = %d %s", status, body)
	}
	if status, body := requestMethod(http.MethodGet, proposalPath+"/"+proposal.ID, proposalToken, ""); status != http.StatusOK || !strings.Contains(body, snapshot.Revision) {
		t.Fatalf("retrieve proposal = %d %s", status, body)
	}
	verifyPath := proposalPath + "/" + proposal.ID + "/verify"
	if status, _ := requestTo(verifyPath, token, ""); status != http.StatusUnauthorized {
		t.Fatalf("preview credential can verify proposal: %d", status)
	}
	if status, _ := requestTo(verifyPath, proposalToken, "{}"); status != http.StatusBadRequest {
		t.Fatalf("verification accepted a body: %d", status)
	}
	if status, body := requestTo(verifyPath, proposalToken, ""); status != http.StatusOK ||
		!strings.Contains(body, `"status":"matched_when_checked"`) || !strings.Contains(body, snapshot.Revision) {
		t.Fatalf("verify proposal = %d %s", status, body)
	}
	if err := os.WriteFile(target, []byte(source+"# later edit\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if status, _ := requestTo(verifyPath, proposalToken, ""); status != http.StatusConflict {
		t.Fatalf("stale proposal verification status = %d", status)
	}
	if err := os.WriteFile(target, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	restarted, _, err := NewReloader(path, discardLogger(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	restartedRequest := httptest.NewRequest(http.MethodGet, "https://management"+proposalPath+"/"+proposal.ID, nil)
	restartedRequest.Header.Set("Authorization", "Bearer "+proposalToken)
	restartedResponse := httptest.NewRecorder()
	restarted.ManagementHandler().ServeHTTP(restartedResponse, restartedRequest)
	if restartedResponse.Code != http.StatusOK || !strings.Contains(restartedResponse.Body.String(), proposal.ID) {
		t.Fatalf("restarted listener could not retrieve proposal: %d", restartedResponse.Code)
	}
	restarted.Stop()
	cases := []struct {
		body string
		want int
	}{
		{`{"target_id":"local","TARGET_ID":"link","model_id":"example","settings":{"context_size":4096}}`, http.StatusBadRequest},
		{`{"target_id":"local","model_id":"example","settings":{"context_size":4096,"Context_Size":1024}}`, http.StatusBadRequest},
		{`{"target_id":"local","model_id":"example","settings":{"unknown":1}}`, http.StatusBadRequest},
		{valid + `{}`, http.StatusBadRequest},
		{strings.Repeat(" ", maxManagementRequest+1), http.StatusRequestEntityTooLarge},
		{strings.Replace(valid, `"local"`, `"missing"`, 1), http.StatusNotFound},
		{strings.Replace(valid, `"local"`, `"link"`, 1), http.StatusBadRequest},
		{strings.Replace(valid, `"example"`, `"missing"`, 1), http.StatusBadRequest},
		{strings.Replace(valid, `"context_size":4096`, `"fit":true`, 1), http.StatusBadRequest},
	}
	for _, test := range cases {
		status, body := request(token, test.body)
		if status != test.want || strings.Contains(body, dir) || strings.Contains(body, "confidential") {
			t.Fatalf("invalid request: status=%d want=%d body=%s", status, test.want, body)
		}
	}
	plain := httptest.NewRecorder()
	r.ManagementHandler().ServeHTTP(plain, httptest.NewRequest(http.MethodPost, "/management/v1/preview", strings.NewReader(valid)))
	if plain.Code != http.StatusForbidden {
		t.Fatalf("plaintext handler status = %d", plain.Code)
	}
	public := httptest.NewRecorder()
	r.ServeHTTP(public, httptest.NewRequest(http.MethodPost, "/management/v1/preview", strings.NewReader(valid)))
	if public.Code != http.StatusNotFound {
		t.Fatalf("client listener status = %d", public.Code)
	}
	proposalToken = newProposalToken
	delegationToken = newDelegationToken
	writeConfig(newToken, "127.0.0.1:9294", certPath, true)
	if err := r.reload(); err != nil {
		t.Fatal(err)
	}
	if r.delegatedCallerAuthorized(delegationRequest, oldState) || r.delegatedCallerAuthorized(delegationRequest, r.state.Load()) {
		t.Fatal("reload did not revoke delegated caller")
	}
	delegationRequest.Header.Set("Authorization", "Bearer "+newDelegationToken)
	if !r.delegatedCallerAuthorized(delegationRequest, r.state.Load()) {
		t.Fatal("reloaded delegated caller rejected")
	}
	if status, _ := request(token, valid); status != http.StatusUnauthorized {
		t.Fatalf("revoked credential status = %d", status)
	}
	if status, _ := request(newToken, valid); status != http.StatusOK {
		t.Fatalf("new credential status = %d", status)
	}
	if status, _ := requestTo("/management/v1/settings", token, settingsBody); status != http.StatusUnauthorized {
		t.Fatalf("revoked settings credential status = %d", status)
	}
	if status, _ := requestTo("/management/v1/settings", newToken, settingsBody); status != http.StatusOK {
		t.Fatalf("new settings credential status = %d", status)
	}
	if status, _ := requestMethod(http.MethodGet, proposalPath+"/"+proposal.ID, strings.Repeat("p", 32), ""); status != http.StatusUnauthorized {
		t.Fatalf("revoked proposal credential status = %d", status)
	}
	if status, _ := requestMethod(http.MethodDelete, proposalPath+"/"+proposal.ID, newProposalToken, ""); status != http.StatusNoContent {
		t.Fatalf("delete proposal status = %d", status)
	}
	if status, _ := requestMethod(http.MethodGet, proposalPath+"/"+proposal.ID, newProposalToken, ""); status != http.StatusNotFound {
		t.Fatalf("deleted proposal still available: %d", status)
	}
	writeConfig(newToken, "127.0.0.1:9295", certPath, true)
	if err := r.reload(); err == nil {
		t.Fatal("changed listener address accepted without restart")
	}
	writeConfig(newToken, "127.0.0.1:9294", filepath.Join(dir, "missing.pem"), true)
	if err := r.reload(); err == nil {
		t.Fatal("invalid replacement certificate accepted")
	}
	if status, _ := request(newToken, valid); status != http.StatusOK {
		t.Fatalf("failed reload changed active policy: %d", status)
	}
	writeConfig(newToken, "", "", false)
	if err := r.reload(); err != nil {
		t.Fatal(err)
	}
	if status, _ := request(newToken, valid); status != http.StatusUnauthorized {
		t.Fatalf("disabled listener status = %d", status)
	}
	got, err := os.ReadFile(target)
	if err != nil || string(got) != source {
		t.Fatalf("preview modified target: %v", err)
	}
}

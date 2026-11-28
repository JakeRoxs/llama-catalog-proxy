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

const proposalPath = "/management/v1/proposals"

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

func assertNoSecretLeak(t *testing.T, body string, secrets []string, what string) {
	t.Helper()
	for _, secret := range secrets {
		if strings.Contains(body, secret) {
			t.Fatalf("%s exposed source data or credential", what)
		}
	}
}

// managementHTTPSHarness is the cohesive fixture for TestManagementHTTPSPreviewAndReload.
// It owns the temp target, symlink, cert/TLS setup, config, reloader, TLS server,
// HTTP clients, and request helpers. Each test phase is a focused top-level helper
// that the main test orchestrates, keeping every helper below cognitive complexity 15.
type managementHTTPSHarness struct {
	t *testing.T

	dir      string
	target   string
	source   string
	certPath string
	keyPath  string
	roots    *x509.CertPool

	token              string
	newToken           string
	proposalToken      string
	newProposalToken   string
	delegationToken    string
	newDelegationToken string

	storeDir string
	path     string

	r *Reloader

	listener net.Listener
	server   *http.Server
	done     chan error

	client    *http.Client
	untrusted *http.Client
	endpoint  string

	oldState          *reloaderState
	delegationRequest *http.Request

	valid        string
	settingsBody string
	revision     string
	proposalID   string
}

func newManagementHTTPSHarness(t *testing.T) *managementHTTPSHarness {
	t.Helper()
	h := &managementHTTPSHarness{t: t}
	h.setupTargetAndCert()
	h.setupConfigAndReloader()
	h.setupTLSAndClients()
	h.defineRequestHelpers()
	return h
}

func (h *managementHTTPSHarness) setupTargetAndCert() {
	h.dir = h.t.TempDir()
	h.target = filepath.Join(h.dir, "config.yaml")
	h.source = "secret: confidential\nmodels:\n  example:\n    cmd: llama-server -m /private/model.gguf -c 1024\n"
	if err := os.WriteFile(h.target, []byte(h.source), 0o600); err != nil {
		h.t.Fatal(err)
	}
	if err := os.Symlink(h.target, filepath.Join(h.dir, "link.yaml")); err != nil {
		h.t.Fatal(err)
	}
	h.certPath, h.keyPath, h.roots = peerTestCertificate(h.t)
}

func (h *managementHTTPSHarness) setupConfigAndReloader() {
	h.token, h.newToken = strings.Repeat("m", 32), strings.Repeat("n", 32)
	h.proposalToken, h.newProposalToken = strings.Repeat("p", 32), strings.Repeat("q", 32)
	h.delegationToken, h.newDelegationToken = strings.Repeat("l", 32), strings.Repeat("r", 32)
	h.storeDir = h.t.TempDir()
	if err := os.Chmod(h.storeDir, 0o700); err != nil {
		h.t.Fatal(err)
	}
	h.path = configPath(h.t)
	h.writeConfig(h.token, "127.0.0.1:9294", h.certPath, true)
	var cfg config.Config
	var err error
	h.r, cfg, err = NewReloader(h.path, discardLogger(), nil, nil, nil, nil, nil)
	if err != nil {
		h.t.Fatal(err)
	}
	cfg.Management.PreviewToken = h.newToken
	cfg.Management.DelegationToken = h.newDelegationToken
	cfg.Management.ProposalStore.Token = h.newProposalToken
	cfg.Management.Targets["local"] = cfg.Management.Targets["link"]
	h.delegationRequest = httptest.NewRequest(http.MethodPost, "https://proxy.example/management/v1/delegation", nil)
	h.delegationRequest.Header.Set("Authorization", "Bearer "+h.delegationToken)
	h.oldState = h.r.state.Load()
	if !h.r.delegatedCallerAuthorized(h.delegationRequest, h.oldState) {
		h.t.Fatal("configured delegated caller not retained in snapshot")
	}
	h.t.Cleanup(h.r.Stop)
}

func (h *managementHTTPSHarness) setupTLSAndClients() {
	var err error
	h.listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		h.t.Fatal(err)
	}
	h.server = &http.Server{Handler: h.r.ManagementHandler(), TLSConfig: h.r.ManagementTLSConfig()}
	h.done = make(chan error, 1)
	go func() { h.done <- h.server.ServeTLS(h.listener, "", "") }()
	h.t.Cleanup(func() { _ = h.server.Close(); <-h.done })
	h.client = &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: h.roots}}}
	h.t.Cleanup(h.client.CloseIdleConnections)
	h.untrusted = &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: x509.NewCertPool()}}}
	h.t.Cleanup(h.untrusted.CloseIdleConnections)
	h.endpoint = "https://" + h.listener.Addr().String() + "/management/v1/preview"
	if response, err := h.untrusted.Get(h.endpoint); err == nil {
		_ = response.Body.Close()
		h.t.Fatal("untrusted certificate accepted")
	}
}

func (h *managementHTTPSHarness) defineRequestHelpers() {
	h.valid = `{"target_id":"local","model_id":"example","settings":{"context_size":4096}}`
	h.settingsBody = `{"target_id":"local","model_id":"example"}`
}

func (h *managementHTTPSHarness) writeConfig(credential, listen, certificate string, enabled bool) {
	h.t.Helper()
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
`, listen, certificate, h.keyPath, credential, h.delegationToken, h.storeDir, h.proposalToken, h.dir, h.dir)
	}
	if err := os.WriteFile(h.path, []byte(content), 0o600); err != nil {
		h.t.Fatal(err)
	}
}

func (h *managementHTTPSHarness) requestMethod(method, path, credential, body string) (int, string) {
	h.t.Helper()
	req, err := http.NewRequest(method, "https://"+h.listener.Addr().String()+path, strings.NewReader(body))
	if err != nil {
		h.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if credential != "" {
		req.Header.Set("Authorization", "Bearer "+credential)
	}
	resp, err := h.client.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		h.t.Fatal(err)
	}
	if resp.Header.Get("Cache-Control") != "no-store" {
		h.t.Fatal("missing no-store header")
	}
	return resp.StatusCode, string(data)
}

func (h *managementHTTPSHarness) requestTo(path, credential, body string) (int, string) {
	h.t.Helper()
	return h.requestMethod(http.MethodPost, path, credential, body)
}

func (h *managementHTTPSHarness) request(credential, body string) (int, string) {
	h.t.Helper()
	return h.requestTo("/management/v1/preview", credential, body)
}

// Phase helpers -----------------------------------------------------------

func TestManagementHTTPSPreviewAndReload(t *testing.T) {
	h := newManagementHTTPSHarness(t)
	runPreviewAndSettingsPhase(h)
	runProposalLifecyclePhase(h)
	runReloadAndRotationPhase(h)
}

func runPreviewAndSettingsPhase(h *managementHTTPSHarness) {
	h.t.Helper()
	assertInvalidPreviewCredentials(h)
	assertValidPreview(h)
	readBody := assertSettingsRead(h)
	assertSettingsMatrix(h)
	parseSettingsRevision(h, readBody)
}

func assertInvalidPreviewCredentials(h *managementHTTPSHarness) {
	h.t.Helper()
	for _, invalid := range []string{"", h.newToken, strings.Repeat("x", 32)} {
		if status, _ := h.request(invalid, h.valid); status != http.StatusUnauthorized {
			h.t.Fatalf("invalid credential status = %d", status)
		}
	}
}

func assertValidPreview(h *managementHTTPSHarness) {
	h.t.Helper()
	status, body := h.request(h.token, h.valid)
	if status != http.StatusOK || !strings.Contains(body, `"before":"1024"`) ||
		!strings.Contains(body, `"after":"4096"`) || !strings.Contains(body, `"revision":`) ||
		!strings.Contains(body, `"validation":"yaml_syntax_and_supported_command_only"`) {
		h.t.Fatalf("preview = %d %s", status, body)
	}
	assertNoSecretLeak(h.t, body, []string{"confidential", "/private/model.gguf", h.dir, h.token}, "preview")
}

func assertSettingsRead(h *managementHTTPSHarness) string {
	h.t.Helper()
	readStatus, readBody := h.requestTo("/management/v1/settings", h.token, h.settingsBody)
	if readStatus != http.StatusOK || !strings.Contains(readBody, `"settings":{"context_size":1024}`) ||
		!strings.Contains(readBody, `"revision":`) {
		h.t.Fatalf("settings = %d %s", readStatus, readBody)
	}
	assertNoSecretLeak(h.t, readBody, []string{"confidential", "/private/model.gguf", h.dir, h.token}, "settings")
	return readBody
}

func assertSettingsMatrix(h *managementHTTPSHarness) {
	h.t.Helper()
	for _, test := range []struct {
		token string
		body  string
		want  int
	}{
		{"", h.settingsBody, http.StatusUnauthorized},
		{h.newToken, h.settingsBody, http.StatusUnauthorized},
		{h.token, strings.Replace(h.settingsBody, `"local"`, `"missing"`, 1), http.StatusNotFound},
		{h.token, strings.Replace(h.settingsBody, `"local"`, `"link"`, 1), http.StatusBadRequest},
		{h.token, h.valid, http.StatusBadRequest},
		{h.token, `{"target_id":"local","TARGET_ID":"link","model_id":"example"}`, http.StatusBadRequest},
	} {
		status, body := h.requestTo("/management/v1/settings", test.token, test.body)
		if status != test.want || strings.Contains(body, h.dir) || strings.Contains(body, "confidential") {
			h.t.Fatalf("settings request: status=%d want=%d body=%s", status, test.want, body)
		}
	}
}

func parseSettingsRevision(h *managementHTTPSHarness, readBody string) {
	h.t.Helper()
	var snapshot struct {
		Revision string `json:"revision"`
	}
	if err := json.Unmarshal([]byte(readBody), &snapshot); err != nil || len(snapshot.Revision) != 64 {
		h.t.Fatalf("invalid settings revision: %v", err)
	}
	h.revision = snapshot.Revision
}

func runProposalLifecyclePhase(h *managementHTTPSHarness) {
	h.t.Helper()
	proposalBody := fmt.Sprintf(`{"target_id":"local","model_id":"example","revision":%q,"settings":{"context_size":4096}}`, h.revision)
	assertPreviewAndNewProposalTokenCannotCreateProposal(h, proposalBody)
	assertProposalTokenCannotReadSettings(h)
	assertStaleProposalRejected(h, proposalBody)
	proposalID := assertCreateProposal(h, proposalBody)
	h.proposalID = proposalID
	assertListAndGetProposal(h, proposalID)
	assertVerifyProposalFlow(h, proposalID)
}

func assertPreviewAndNewProposalTokenCannotCreateProposal(h *managementHTTPSHarness, proposalBody string) {
	h.t.Helper()
	if status, _ := h.requestTo(proposalPath, h.token, proposalBody); status != http.StatusUnauthorized {
		h.t.Fatalf("preview credential can create proposal: %d", status)
	}
	if status, _ := h.requestTo(proposalPath, h.newProposalToken, proposalBody); status != http.StatusUnauthorized {
		h.t.Fatalf("modified returned config changed proposal token: %d", status)
	}
}

func assertProposalTokenCannotReadSettings(h *managementHTTPSHarness) {
	h.t.Helper()
	if status, _ := h.requestTo("/management/v1/settings", h.proposalToken, h.settingsBody); status != http.StatusUnauthorized {
		h.t.Fatalf("proposal credential can read settings endpoint: %d", status)
	}
}

func assertStaleProposalRejected(h *managementHTTPSHarness, proposalBody string) {
	h.t.Helper()
	if err := os.WriteFile(h.target, []byte(h.source+"# external edit\n"), 0o600); err != nil {
		h.t.Fatal(err)
	}
	if status, _ := h.requestTo(proposalPath, h.proposalToken, proposalBody); status != http.StatusConflict {
		h.t.Fatalf("stale proposal accepted: %d", status)
	}
	if err := os.WriteFile(h.target, []byte(h.source), 0o600); err != nil {
		h.t.Fatal(err)
	}
}

func assertCreateProposal(h *managementHTTPSHarness, proposalBody string) string {
	h.t.Helper()
	status, body := h.requestTo(proposalPath, h.proposalToken, proposalBody)
	if status != http.StatusCreated {
		h.t.Fatalf("create proposal = %d %s", status, body)
	}
	var proposal struct {
		ID       string `json:"id"`
		Revision string `json:"revision"`
	}
	if err := json.Unmarshal([]byte(body), &proposal); err != nil || len(proposal.ID) != 32 || proposal.Revision != h.revision {
		h.t.Fatalf("invalid created proposal: %s %v", body, err)
	}
	assertNoSecretLeak(h.t, body, []string{"confidential", "/private/model.gguf", h.dir, h.storeDir, h.token, h.proposalToken}, "proposal")
	return proposal.ID
}

func assertListAndGetProposal(h *managementHTTPSHarness, proposalID string) {
	h.t.Helper()
	if status, body := h.requestMethod(http.MethodGet, proposalPath, h.proposalToken, ""); status != http.StatusOK || !strings.Contains(body, proposalID) {
		h.t.Fatalf("list proposals = %d %s", status, body)
	}
	if status, body := h.requestMethod(http.MethodGet, proposalPath+"/"+proposalID, h.proposalToken, ""); status != http.StatusOK || !strings.Contains(body, h.revision) {
		h.t.Fatalf("retrieve proposal = %d %s", status, body)
	}
}

func assertVerifyProposalFlow(h *managementHTTPSHarness, proposalID string) {
	h.t.Helper()
	verifyPath := proposalPath + "/" + proposalID + "/verify"
	if status, _ := h.requestTo(verifyPath, h.token, ""); status != http.StatusUnauthorized {
		h.t.Fatalf("preview credential can verify proposal: %d", status)
	}
	if status, _ := h.requestTo(verifyPath, h.proposalToken, "{}"); status != http.StatusBadRequest {
		h.t.Fatalf("verification accepted a body: %d", status)
	}
	if status, body := h.requestTo(verifyPath, h.proposalToken, ""); status != http.StatusOK ||
		!strings.Contains(body, `"status":"matched_when_checked"`) || !strings.Contains(body, h.revision) {
		h.t.Fatalf("verify proposal = %d %s", status, body)
	}
	if err := os.WriteFile(h.target, []byte(h.source+"# later edit\n"), 0o600); err != nil {
		h.t.Fatal(err)
	}
	if status, _ := h.requestTo(verifyPath, h.proposalToken, ""); status != http.StatusConflict {
		h.t.Fatalf("stale proposal verification status = %d", status)
	}
	if err := os.WriteFile(h.target, []byte(h.source), 0o600); err != nil {
		h.t.Fatal(err)
	}
}

func runReloadAndRotationPhase(h *managementHTTPSHarness) {
	h.t.Helper()
	assertRestartPersistsProposal(h)
	assertInvalidBodiesRejected(h)
	assertListenerGuards(h)
	assertCredentialRotation(h)
	assertReloadFailureKeepsActive(h)
	assertListenerDisabled(h)
	assertTargetBytesUnchanged(h)
}

func assertRestartPersistsProposal(h *managementHTTPSHarness) {
	h.t.Helper()
	restarted, _, err := NewReloader(h.path, discardLogger(), nil, nil, nil, nil, nil)
	if err != nil {
		h.t.Fatal(err)
	}
	restartedRequest := httptest.NewRequest(http.MethodGet, "https://management"+proposalPath+"/"+h.proposalID, nil)
	restartedRequest.Header.Set("Authorization", "Bearer "+h.proposalToken)
	restartedResponse := httptest.NewRecorder()
	restarted.ManagementHandler().ServeHTTP(restartedResponse, restartedRequest)
	if restartedResponse.Code != http.StatusOK || !strings.Contains(restartedResponse.Body.String(), h.proposalID) {
		h.t.Fatalf("restarted listener could not retrieve proposal: %d", restartedResponse.Code)
	}
	restarted.Stop()
}

func assertInvalidBodiesRejected(h *managementHTTPSHarness) {
	h.t.Helper()
	cases := []struct {
		body string
		want int
	}{
		{`{"target_id":"local","TARGET_ID":"link","model_id":"example","settings":{"context_size":4096}}`, http.StatusBadRequest},
		{`{"target_id":"local","model_id":"example","settings":{"context_size":4096,"Context_Size":1024}}`, http.StatusBadRequest},
		{`{"target_id":"local","model_id":"example","settings":{"unknown":1}}`, http.StatusBadRequest},
		{h.valid + `{}`, http.StatusBadRequest},
		{strings.Repeat(" ", maxManagementRequest+1), http.StatusRequestEntityTooLarge},
		{strings.Replace(h.valid, `"local"`, `"missing"`, 1), http.StatusNotFound},
		{strings.Replace(h.valid, `"local"`, `"link"`, 1), http.StatusBadRequest},
		{strings.Replace(h.valid, `"example"`, `"missing"`, 1), http.StatusBadRequest},
		{strings.Replace(h.valid, `"context_size":4096`, `"fit":true`, 1), http.StatusBadRequest},
	}
	for _, test := range cases {
		status, body := h.request(h.token, test.body)
		if status != test.want || strings.Contains(body, h.dir) || strings.Contains(body, "confidential") {
			h.t.Fatalf("invalid request: status=%d want=%d body=%s", status, test.want, body)
		}
	}
}

func assertListenerGuards(h *managementHTTPSHarness) {
	h.t.Helper()
	plain := httptest.NewRecorder()
	h.r.ManagementHandler().ServeHTTP(plain, httptest.NewRequest(http.MethodPost, "/management/v1/preview", strings.NewReader(h.valid)))
	if plain.Code != http.StatusForbidden {
		h.t.Fatalf("plaintext handler status = %d", plain.Code)
	}
	public := httptest.NewRecorder()
	h.r.ServeHTTP(public, httptest.NewRequest(http.MethodPost, "/management/v1/preview", strings.NewReader(h.valid)))
	if public.Code != http.StatusNotFound {
		h.t.Fatalf("client listener status = %d", public.Code)
	}
}

func assertCredentialRotation(h *managementHTTPSHarness) {
	h.t.Helper()
	h.proposalToken = h.newProposalToken
	h.delegationToken = h.newDelegationToken
	h.writeConfig(h.newToken, "127.0.0.1:9294", h.certPath, true)
	if err := h.r.reload(); err != nil {
		h.t.Fatal(err)
	}
	if h.r.delegatedCallerAuthorized(h.delegationRequest, h.oldState) || h.r.delegatedCallerAuthorized(h.delegationRequest, h.r.state.Load()) {
		h.t.Fatal("reload did not revoke delegated caller")
	}
	h.delegationRequest.Header.Set("Authorization", "Bearer "+h.newDelegationToken)
	if !h.r.delegatedCallerAuthorized(h.delegationRequest, h.r.state.Load()) {
		h.t.Fatal("reloaded delegated caller rejected")
	}
	if status, _ := h.request(h.token, h.valid); status != http.StatusUnauthorized {
		h.t.Fatalf("revoked credential status = %d", status)
	}
	if status, _ := h.request(h.newToken, h.valid); status != http.StatusOK {
		h.t.Fatalf("new credential status = %d", status)
	}
	if status, _ := h.requestTo("/management/v1/settings", h.token, h.settingsBody); status != http.StatusUnauthorized {
		h.t.Fatalf("revoked settings credential status = %d", status)
	}
	if status, _ := h.requestTo("/management/v1/settings", h.newToken, h.settingsBody); status != http.StatusOK {
		h.t.Fatalf("new settings credential status = %d", status)
	}
	if status, _ := h.requestMethod(http.MethodGet, proposalPath+"/"+h.proposalID, strings.Repeat("p", 32), ""); status != http.StatusUnauthorized {
		h.t.Fatalf("revoked proposal credential status = %d", status)
	}
	if status, _ := h.requestMethod(http.MethodDelete, proposalPath+"/"+h.proposalID, h.newProposalToken, ""); status != http.StatusNoContent {
		h.t.Fatalf("delete proposal status = %d", status)
	}
	if status, _ := h.requestMethod(http.MethodGet, proposalPath+"/"+h.proposalID, h.newProposalToken, ""); status != http.StatusNotFound {
		h.t.Fatalf("deleted proposal still available: %d", status)
	}
}

func assertReloadFailureKeepsActive(h *managementHTTPSHarness) {
	h.t.Helper()
	h.writeConfig(h.newToken, "127.0.0.1:9295", h.certPath, true)
	if err := h.r.reload(); err == nil {
		h.t.Fatal("changed listener address accepted without restart")
	}
	h.writeConfig(h.newToken, "127.0.0.1:9294", filepath.Join(h.dir, "missing.pem"), true)
	if err := h.r.reload(); err == nil {
		h.t.Fatal("invalid replacement certificate accepted")
	}
	if status, _ := h.request(h.newToken, h.valid); status != http.StatusOK {
		h.t.Fatalf("failed reload changed active policy: %d", status)
	}
}

func assertListenerDisabled(h *managementHTTPSHarness) {
	h.t.Helper()
	h.writeConfig(h.newToken, "", "", false)
	if err := h.r.reload(); err != nil {
		h.t.Fatal(err)
	}
	if status, _ := h.request(h.newToken, h.valid); status != http.StatusUnauthorized {
		h.t.Fatalf("disabled listener status = %d", status)
	}
}

func assertTargetBytesUnchanged(h *managementHTTPSHarness) {
	h.t.Helper()
	got, err := os.ReadFile(h.target)
	if err != nil || string(got) != h.source {
		h.t.Fatalf("preview modified target: %v", err)
	}
}

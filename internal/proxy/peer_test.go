package proxy

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/nodecontract"
)

func TestAuthenticatedPeerBindsIdentityToScopedCredential(t *testing.T) {
	catalogToken, inferenceToken, managementToken := strings.Repeat("a", 32), strings.Repeat("b", 32), strings.Repeat("c", 32)
	state := &reloaderState{peer: &config.PeerConfig{Peers: map[string]config.PeerGrant{
		"node-b": {Token: catalogToken, CatalogRead: true},
		"node-c": {Token: inferenceToken, Inference: true},
		"node-d": {Token: managementToken, ManageSettings: true},
	}}}
	request := httptest.NewRequest(http.MethodGet, "https://peer.example/peer/v1/report", nil)

	t.Run("catalog credential binds node-b and denies other scopes", func(t *testing.T) {
		request.Header.Set("Authorization", "Bearer "+catalogToken)
		assertPeerIdentity(t, request, state, peerCatalog, "node-b")
		assertPeerDenied(t, request, state, peerInference)
		assertPeerDenied(t, request, state, peerManageSettings)
	})

	t.Run("inference credential binds node-c and denies catalog", func(t *testing.T) {
		request.Header.Set("Authorization", "Bearer "+inferenceToken)
		assertPeerIdentity(t, request, state, peerInference, "node-c")
		assertPeerDenied(t, request, state, peerCatalog)
	})

	t.Run("management credential binds node-d and denies catalog", func(t *testing.T) {
		request.Header.Set("Authorization", "Bearer "+managementToken)
		assertPeerIdentity(t, request, state, peerManageSettings, "node-d")
		assertPeerDenied(t, request, state, peerCatalog)
	})

	t.Run("revoked grants are rejected", func(t *testing.T) {
		request.Header.Set("Authorization", "Bearer "+managementToken)
		state.peer.Peers["node-d"] = config.PeerGrant{Token: managementToken}
		assertPeerDenied(t, request, state, peerManageSettings)

		request.Header.Set("Authorization", "Bearer "+inferenceToken)
		state.peer.Peers["node-c"] = config.PeerGrant{Token: inferenceToken}
		assertPeerDenied(t, request, state, peerInference)
	})

	t.Run("ambiguous credential is rejected", func(t *testing.T) {
		state.peer.Peers["node-c"] = config.PeerGrant{Token: catalogToken, CatalogRead: true}
		request.Header.Set("Authorization", "Bearer "+catalogToken)
		assertPeerDenied(t, request, state, peerCatalog)
	})

	t.Run("duplicate authorization header is rejected", func(t *testing.T) {
		request.Header.Add("Authorization", "Bearer "+inferenceToken)
		assertPeerDenied(t, request, state, peerCatalog)
	})
}

func assertPeerIdentity(t *testing.T, request *http.Request, state *reloaderState, scope peerScope, wantID string) {
	t.Helper()
	if id, ok := authenticatedPeer(request, state, scope); !ok || id != wantID {
		t.Fatalf("identity = %q, %v", id, ok)
	}
}

func assertPeerDenied(t *testing.T, request *http.Request, state *reloaderState, scope peerScope) {
	t.Helper()
	if id, ok := authenticatedPeer(request, state, scope); ok || id != "" {
		t.Fatalf("credential granted: %q, %v", id, ok)
	}
}

func TestPeerManagementBindingDoesNotEnableRemoteActions(t *testing.T) {
	original := &config.PeerConfig{
		ManagementTargets: map[string]string{"local": "swap"},
		Peers:             map[string]config.PeerGrant{"peer": {Token: strings.Repeat("x", 32), ManageSettings: true}},
	}
	snapshot := copyPeerConfig(original)
	original.ManagementTargets["local"] = "changed"
	if snapshot.ManagementTargets["local"] != "swap" {
		t.Fatal("peer configuration snapshot shares management bindings")
	}
	r := &Reloader{}
	r.state.Store(&reloaderState{peer: snapshot})
	request := httptest.NewRequest(http.MethodPost, "https://peer.example/peer/v1/management", nil)
	request.Header.Set("Authorization", "Bearer "+strings.Repeat("x", 32))
	response := httptest.NewRecorder()
	r.PeerHandler().ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("remote management unexpectedly available: %d", response.Code)
	}
}

func TestDelegatedSettingsCapabilityRequiresLocalWriteOptIn(t *testing.T) {
	state := &reloaderState{
		peer: &config.PeerConfig{
			LocalBackends:     []string{"local"},
			ManagementTargets: map[string]string{"local": "swap"},
			Peers:             map[string]config.PeerGrant{"reader": {Token: strings.Repeat("r", 32), CatalogRead: true}},
		},
		management: &config.ManagementConfig{
			WriteToken: "write", ProposalStore: &config.ProposalStoreConfig{Token: "proposal"},
			BackupStore:    &config.BackupStoreConfig{Token: "backup"},
			DelegatedStore: &config.DelegatedStoreConfig{},
			Targets:        map[string]config.ManagementTarget{"swap": {}},
		},
	}
	if !delegatedSettingsEnabled(state) {
		t.Fatal("complete owner-side configuration did not advertise settings management")
	}
	if grant := state.peer.Peers["reader"]; grant.ManageSettings {
		t.Fatal("feature advertisement fixture unexpectedly grants the peer management scope")
	}
	for _, disable := range []func(){
		func() { state.peer.ManagementTargets = nil },
		func() { state.peer.ManagementTargets = map[string]string{"remote": "swap"} },
		func() { state.peer.ManagementTargets = map[string]string{"local": "missing"} },
		func() {
			state.peer.ManagementTargets = map[string]string{"local": "swap"}
			state.management.WriteToken = ""
		},
		func() { state.management.WriteToken = "write"; state.management.DelegatedStore = nil },
		func() {
			state.management.DelegatedStore = &config.DelegatedStoreConfig{}
			state.management.BackupStore = nil
		},
	} {
		// Start each case from the same fully enabled configuration.
		state.peer.ManagementTargets = map[string]string{"local": "swap"}
		state.management.WriteToken = "write"
		state.management.ProposalStore = &config.ProposalStoreConfig{Token: "proposal"}
		state.management.BackupStore = &config.BackupStoreConfig{Token: "backup"}
		state.management.DelegatedStore = &config.DelegatedStoreConfig{}
		disable()
		if delegatedSettingsEnabled(state) {
			t.Fatal("incomplete or unmapped owner configuration advertised settings management")
		}
	}
}

func TestDelegatedCallerCredentialIsolationAndRevocation(t *testing.T) {
	delegation, rotated := strings.Repeat("l", 32), strings.Repeat("r", 32)
	preview, proposal, backup, write := strings.Repeat("m", 32), strings.Repeat("p", 32), strings.Repeat("b", 32), strings.Repeat("w", 32)
	ingestion, peer := strings.Repeat("i", 32), strings.Repeat("x", 32)
	state := &reloaderState{
		management: &config.ManagementConfig{
			PreviewToken: preview, DelegationToken: delegation, WriteToken: write,
			ProposalStore: &config.ProposalStoreConfig{Token: proposal},
			BackupStore:   &config.BackupStoreConfig{Token: backup},
			Ingestion:     &config.IngestionConfig{Token: ingestion},
		},
		peer: &config.PeerConfig{Peers: map[string]config.PeerGrant{
			"owner": {Token: peer, ManageSettings: true},
		}},
	}
	r := &Reloader{}
	r.state.Store(state)
	request := httptest.NewRequest(http.MethodPost, "https://proxy.example/management/v1/delegation", nil)
	request.Header.Set("Authorization", "Bearer "+delegation)
	if !r.delegatedCallerAuthorized(request, state) {
		t.Fatal("configured delegated caller rejected")
	}
	if _, ok := authenticatedPeer(request, state, peerManageSettings); ok {
		t.Fatal("local delegation credential authorized as peer")
	}
	for _, invalid := range []string{"", preview, proposal, backup, write, ingestion, peer, rotated} {
		request.Header.Set("Authorization", "Bearer "+invalid)
		if r.delegatedCallerAuthorized(request, state) {
			t.Fatal("wrong credential authorized for delegation")
		}
	}
	request.Header.Set("Authorization", "Bearer "+delegation)
	request.Header.Add("Authorization", "Bearer "+delegation)
	if r.delegatedCallerAuthorized(request, state) {
		t.Fatal("duplicate authorization headers accepted")
	}
	request.Header.Del("Authorization")
	request.Header.Set("Authorization", "Bearer "+delegation)
	request.TLS = nil
	if r.delegatedCallerAuthorized(request, state) {
		t.Fatal("plaintext caller authorized")
	}
	request = httptest.NewRequest(http.MethodPost, "https://proxy.example/management/v1/delegation", nil)
	request.Header.Set("Authorization", "Bearer "+delegation)
	for _, path := range []string{
		"/management/v1/preview", "/management/v1/proposals", "/management/v1/backups",
		"/management/v1/proposals/id/apply", "/management/v1/ingestion", "/management/v1/delegation",
	} {
		request.URL.Path = path
		response := httptest.NewRecorder()
		r.ManagementHandler().ServeHTTP(response, request)
		if response.Code != http.StatusUnauthorized {
			t.Fatalf("delegation credential unexpectedly accessed %s: %d", path, response.Code)
		}
	}
	next := *state
	next.management = copyManagementConfig(state.management)
	next.management.DelegationToken = rotated
	r.state.Store(&next)
	if r.delegatedCallerAuthorized(request, state) || r.delegatedCallerAuthorized(request, &next) {
		t.Fatal("revoked credential or stale state authorized")
	}
	request.Header.Set("Authorization", "Bearer "+rotated)
	if !r.delegatedCallerAuthorized(request, &next) {
		t.Fatal("rotated credential rejected")
	}
	next.management.DelegationToken = ""
	if r.delegatedCallerAuthorized(request, &next) {
		t.Fatal("disabled credential authorized")
	}
}

func TestPeerHTTPSReportAndReload(t *testing.T) {
	var backendRequests atomic.Int32
	var unhealthy atomic.Bool
	local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		backendRequests.Add(1)
		if req.URL.Path == "/health" && unhealthy.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, `{"data":[{"id":"local-model","sensitive":"not-exported"}]}`)
	}))
	defer local.Close()
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":[{"id":"remote-model"}]}`)
	}))
	defer remote.Close()
	certPath, keyPath, roots := peerTestCertificate(t)
	token := strings.Repeat("a", 32)
	revoked := strings.Repeat("b", 32)
	diagnosticToken := strings.Repeat("e", 32)
	previewToken := strings.Repeat("m", 32)
	targetDir := t.TempDir()
	path := configPath(t)
	allowDiagnostics := true
	write := func(active, nodeID, certificate string) {
		t.Helper()
		content := fmt.Sprintf(`default_backend: local
backends:
  local: {url: %q}
  remote: {url: %q, prefix: "remote/"}
peer:
  node_id: %q
  listen: "127.0.0.1:9293"
  tls_cert_file: %q
  tls_key_file: %q
  local_backends: [local]
  management_targets: {local: swap}
  peers:
    node-b: {token: %q, catalog_read: true}
    node-c: {token: %q, catalog_read: false}
    node-d: {token: %q, catalog_read: true, diagnostics_read: %t}
management:
  listen: "127.0.0.1:9294"
  tls_cert_file: %q
  tls_key_file: %q
  preview_token: %q
  targets:
    swap: {directory: %q, filename: config.yaml}
`, local.URL, remote.URL, nodeID, certificate, keyPath, active, revoked, diagnosticToken, allowDiagnostics,
			certPath, keyPath, previewToken, targetDir)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(token, "node-a", certPath)
	r, returnedConfig, err := NewReloader(path, discardLogger(), nil, nil, nil, nil, nil)
	assertNoErr(t, err)
	mutatedToken := strings.Repeat("d", 32)
	returnedConfig.Peer.Peers["node-b"] = config.PeerGrant{Token: mutatedToken, CatalogRead: true}
	returnedConfig.Peer.LocalBackends = append(returnedConfig.Peer.LocalBackends, "remote")
	returnedConfig.Peer.ManagementTargets["local"] = "changed"
	assertCatalogConfigNotMutated(t, r)
	warm := httptest.NewRecorder()
	r.ServeHTTP(warm, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	assertRecorderStatus(t, warm, http.StatusOK)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assertNoErr(t, err)
	server := &http.Server{Handler: r.PeerHandler(), TLSConfig: r.PeerTLSConfig()}
	done := make(chan error, 1)
	go func() { done <- server.ServeTLS(listener, "", "") }()
	t.Cleanup(func() {
		_ = server.Close()
		<-done
	})
	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}}}
	t.Cleanup(client.CloseIdleConnections)
	endpoint := "https://" + listener.Addr().String() + "/peer/v1/report"
	untrusted := &http.Client{Timeout: time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: x509.NewCertPool()}}}
	defer untrusted.CloseIdleConnections()
	assertUntrustedClientRejected(t, untrusted, endpoint)
	get := func(credential string) *http.Response {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
		assertNoErr(t, err)
		if credential != "" {
			req.Header.Set("Authorization", "Bearer "+credential)
		}
		resp, err := client.Do(req)
		assertNoErr(t, err)
		return resp
	}
	// Baseline report before any credential mutations.
	response := get(token)
	receipt := assertReportOK(t, response)
	assertCatalogCapabilities(t, receipt)
	initial := receipt.Report.Source
	assertAllUnauthorized(t, get, []string{"", strings.Repeat("x", 32), revoked, mutatedToken})
	assertInferenceDenied(t, client, endpoint, token)
	getDiagnostics := func() nodecontract.Report {
		t.Helper()
		resp := get(diagnosticToken)
		defer resp.Body.Close()
		receipt, err := nodecontract.DecodeReport(resp.Body, "node-a", time.Now())
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("diagnostic report status=%d error=%v", resp.StatusCode, err)
		}
		return receipt.Report
	}
	beforeProbes := backendRequests.Load()
	unknown := getDiagnostics()
	assertInitialReadiness(t, unknown, beforeProbes, &backendRequests)
	checkReady := func(wantHTTP int, wantStatus string) {
		t.Helper()
		result := httptest.NewRecorder()
		r.ServeHTTP(result, httptest.NewRequest(http.MethodGet, "/ready", nil))
		assertRecorderStatus(t, result, wantHTTP)
		before := backendRequests.Load()
		report := getDiagnostics()
		assertReadinessReport(t, report, wantStatus, before, &backendRequests)
	}
	checkReady(http.StatusOK, "ready")
	unhealthy.Store(true)
	checkReady(http.StatusServiceUnavailable, "unavailable")
	clientSide := httptest.NewRecorder()
	misrouted := httptest.NewRequest(http.MethodGet, "/peer/v1/report", nil)
	misrouted.Header.Set("Authorization", "Bearer "+token)
	before := backendRequests.Load()
	r.ServeHTTP(clientSide, misrouted)
	assertMisroutedRoute(t, clientSide, before, &backendRequests)
	plain := httptest.NewRecorder()
	r.PeerHandler().ServeHTTP(plain, httptest.NewRequest(http.MethodGet, "/peer/v1/report", nil))
	assertRecorderStatus(t, plain, http.StatusForbidden)
	newToken := strings.Repeat("c", 32)
	allowDiagnostics = false
	write(newToken, "node-a", certPath)
	assertNoErr(t, r.reload())
	assertDiagnosticsRevoked(t, getDiagnostics())
	old := get(token)
	_ = old.Body.Close()
	assertStatus(t, old, http.StatusUnauthorized)
	response = get(newToken)
	receipt = assertReportDecodeOnly(t, response)
	assertReloadSourceChanged(t, receipt, initial)
	write(newToken, "node-a", certPath+"-missing")
	assertReloadFailed(t, r.reload())
	write(newToken, "node-other", certPath)
	assertReloadFailed(t, r.reload())
	response = get(newToken)
	receipt = assertReportDecodeOnly(t, response)
	assertReloadSourceNotChanged(t, receipt, initial)
	if err := os.WriteFile(path, []byte(fmt.Sprintf("default_backend: local\nbackends:\n  local: {url: %q}\n", local.URL)), 0o600); err != nil {
		t.Fatal(err)
	}
	assertNoErr(t, r.reload())
	response = get(newToken)
	_ = response.Body.Close()
	assertStatus(t, response, http.StatusUnauthorized)
	write(newToken, "node-a", certPath)
	assertNoErr(t, r.reload())
	response = get(newToken)
	receipt = assertReportDecodeOnly(t, response)
	assertReloadSourceIncremented(t, receipt, initial)
}

func assertNoErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func assertStatus(t *testing.T, resp *http.Response, want int) {
	t.Helper()
	if resp.StatusCode != want {
		t.Fatalf("unexpected status = %d", resp.StatusCode)
	}
}

func assertRecorderStatus(t *testing.T, rec *httptest.ResponseRecorder, want int) {
	t.Helper()
	if rec.Code != want {
		t.Fatalf("unexpected status = %d", rec.Code)
	}
}

func assertCatalogConfigNotMutated(t *testing.T, r *Reloader) {
	t.Helper()
	if r.state.Load().peer.ManagementTargets["local"] != "swap" {
		t.Fatal("returned config changed active management binding")
	}
}

func assertUntrustedClientRejected(t *testing.T, untrusted *http.Client, endpoint string) {
	t.Helper()
	if response, err := untrusted.Get(endpoint); err == nil {
		_ = response.Body.Close()
		t.Fatal("untrusted peer certificate accepted")
	}
}

func assertAllUnauthorized(t *testing.T, get func(string) *http.Response, credentials []string) {
	t.Helper()
	for _, invalid := range credentials {
		resp := get(invalid)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("unauthorized status = %d", resp.StatusCode)
		}
	}
}

func assertInferenceDenied(t *testing.T, client *http.Client, endpoint string, token string) {
	t.Helper()
	inference, err := http.NewRequest(http.MethodPost, strings.TrimSuffix(endpoint, "/report")+"/inference", nil)
	assertNoErr(t, err)
	inference.Header.Set("Authorization", "Bearer "+token)
	denied, err := client.Do(inference)
	assertNoErr(t, err)
	_ = denied.Body.Close()
	if denied.StatusCode != http.StatusUnauthorized {
		t.Fatalf("catalog-only peer inference status = %d", denied.StatusCode)
	}
}

func assertReportOK(t *testing.T, response *http.Response) nodecontract.Receipt {
	t.Helper()
	receipt, err := nodecontract.DecodeReport(response.Body, "node-a", time.Now())
	_ = response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || len(receipt.Report.Observations) != 1 ||
		receipt.Report.Observations[0].Identity.LocalID != "local-model" {
		t.Fatalf("report = %#v, status %d", receipt.Report, response.StatusCode)
	}
	return receipt
}

// assertReportDecodeOnly decodes a peer report without requiring observations.
// Reload-state assertions in the original code checked only status/source fields,
// not observation content, so this helper matches that looser contract.
func assertReportDecodeOnly(t *testing.T, response *http.Response) nodecontract.Receipt {
	t.Helper()
	receipt, err := nodecontract.DecodeReport(response.Body, "node-a", time.Now())
	_ = response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}

func assertCatalogCapabilities(t *testing.T, receipt nodecontract.Receipt) {
	t.Helper()
	if receipt.Report.Capabilities["diagnostics_read"] != "enabled" || receipt.Report.Capabilities["config_write"] != "disabled" ||
		receipt.Report.BackendReadiness != nil {
		t.Fatal("catalog-only peer received readiness or feature not advertised")
	}
}

func assertInitialReadiness(t *testing.T, unknown nodecontract.Report, beforeProbes int32, backendRequests *atomic.Int32) {
	t.Helper()
	if unknown.BackendReadiness == nil || len(*unknown.BackendReadiness) != 1 ||
		(*unknown.BackendReadiness)[0].BackendID != "local" ||
		(*unknown.BackendReadiness)[0].Status != "unknown" || backendRequests.Load() != beforeProbes {
		t.Fatalf("unexpected initial readiness: %#v", unknown.BackendReadiness)
	}
}

func assertReadinessReport(t *testing.T, report nodecontract.Report, wantStatus string, before int32, backendRequests *atomic.Int32) {
	t.Helper()
	if report.BackendReadiness == nil || len(*report.BackendReadiness) != 1 {
		t.Fatalf("readiness report = %#v", report.BackendReadiness)
	}
	item := (*report.BackendReadiness)[0]
	if item.BackendID != "local" || item.Status != wantStatus || item.ObservedAt == nil ||
		item.Freshness != "fresh" || backendRequests.Load() != before {
		t.Fatalf("readiness report = %#v", item)
	}
}

func assertMisroutedRoute(t *testing.T, clientSide *httptest.ResponseRecorder, before int32, backendRequests *atomic.Int32) {
	t.Helper()
	if clientSide.Code != http.StatusNotFound || backendRequests.Load() != before {
		t.Fatalf("client listener exposed peer route: %d", clientSide.Code)
	}
}

func assertDiagnosticsRevoked(t *testing.T, report nodecontract.Report) {
	t.Helper()
	if report.BackendReadiness != nil {
		t.Fatal("revoked diagnostics grant still received readiness")
	}
}

func assertReloadSourceChanged(t *testing.T, receipt nodecontract.Receipt, initial nodecontract.Source) {
	t.Helper()
	if receipt.Report.Source.ProxyInstanceID != initial.ProxyInstanceID || receipt.Report.Source.ConfigGeneration != initial.ConfigGeneration+1 {
		t.Fatalf("reloaded source = %#v, initial = %#v", receipt.Report.Source, initial)
	}
}

func assertReloadFailed(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("reload unexpectedly succeeded")
	}
}

func assertReloadSourceNotChanged(t *testing.T, receipt nodecontract.Receipt, initial nodecontract.Source) {
	t.Helper()
	if receipt.Report.Source.ConfigGeneration != initial.ConfigGeneration+1 {
		t.Fatalf("failed reload changed active state: %#v", receipt.Report.Source)
	}
}

func assertReloadSourceIncremented(t *testing.T, receipt nodecontract.Receipt, initial nodecontract.Source) {
	t.Helper()
	if receipt.Report.Source.ConfigGeneration != initial.ConfigGeneration+3 ||
		receipt.Report.Source.ProxyInstanceID != initial.ProxyInstanceID {
		t.Fatalf("reenabled peer source = %#v", receipt.Report.Source)
	}
}

func peerTestCertificate(t *testing.T) (string, string, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "peer-test"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:    x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certPath, keyPath := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certPEM) {
		t.Fatal("failed to trust test certificate")
	}
	return certPath, keyPath, roots
}

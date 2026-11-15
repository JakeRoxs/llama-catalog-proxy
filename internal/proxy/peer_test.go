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
  peers:
    node-b: {token: %q, catalog_read: true}
    node-c: {token: %q, catalog_read: false}
    node-d: {token: %q, catalog_read: true, diagnostics_read: %t}
`, local.URL, remote.URL, nodeID, certificate, keyPath, active, revoked, diagnosticToken, allowDiagnostics)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(token, "node-a", certPath)
	r, returnedConfig, err := NewReloader(path, discardLogger(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	mutatedToken := strings.Repeat("d", 32)
	returnedConfig.Peer.Peers["node-b"] = config.PeerGrant{Token: mutatedToken, CatalogRead: true}
	returnedConfig.Peer.LocalBackends = append(returnedConfig.Peer.LocalBackends, "remote")
	warm := httptest.NewRecorder()
	r.ServeHTTP(warm, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	if warm.Code != http.StatusOK {
		t.Fatalf("warm catalog: %d", warm.Code)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
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
	if response, err := untrusted.Get(endpoint); err == nil {
		_ = response.Body.Close()
		t.Fatal("untrusted peer certificate accepted")
	}
	get := func(credential string) *http.Response {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
		if err != nil {
			t.Fatal(err)
		}
		if credential != "" {
			req.Header.Set("Authorization", "Bearer "+credential)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	for _, invalid := range []string{"", strings.Repeat("x", 32), revoked, mutatedToken} {
		resp := get(invalid)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("unauthorized status = %d", resp.StatusCode)
		}
	}
	inference, err := http.NewRequest(http.MethodPost, strings.TrimSuffix(endpoint, "/report")+"/inference", nil)
	if err != nil {
		t.Fatal(err)
	}
	inference.Header.Set("Authorization", "Bearer "+token)
	denied, err := client.Do(inference)
	if err != nil {
		t.Fatal(err)
	}
	_ = denied.Body.Close()
	if denied.StatusCode != http.StatusUnauthorized {
		t.Fatalf("catalog-only peer inference status = %d", denied.StatusCode)
	}
	response := get(token)
	receipt, err := nodecontract.DecodeReport(response.Body, "node-a", time.Now())
	_ = response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || len(receipt.Report.Observations) != 1 ||
		receipt.Report.Observations[0].Identity.LocalID != "local-model" {
		t.Fatalf("report = %#v, status %d", receipt.Report, response.StatusCode)
	}
	if receipt.Report.Capabilities["diagnostics_read"] != "enabled" || receipt.Report.BackendReadiness != nil {
		t.Fatal("catalog-only peer received readiness or feature not advertised")
	}
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
	if unknown.BackendReadiness == nil || len(*unknown.BackendReadiness) != 1 ||
		(*unknown.BackendReadiness)[0].BackendID != "local" ||
		(*unknown.BackendReadiness)[0].Status != "unknown" || backendRequests.Load() != beforeProbes {
		t.Fatalf("unexpected initial readiness: %#v", unknown.BackendReadiness)
	}
	checkReady := func(wantHTTP int, wantStatus string) {
		t.Helper()
		result := httptest.NewRecorder()
		r.ServeHTTP(result, httptest.NewRequest(http.MethodGet, "/ready", nil))
		if result.Code != wantHTTP {
			t.Fatalf("local readiness status=%d", result.Code)
		}
		before := backendRequests.Load()
		report := getDiagnostics()
		if report.BackendReadiness == nil || len(*report.BackendReadiness) != 1 {
			t.Fatalf("readiness report = %#v", report.BackendReadiness)
		}
		item := (*report.BackendReadiness)[0]
		if item.BackendID != "local" || item.Status != wantStatus || item.ObservedAt == nil ||
			item.Freshness != "fresh" || backendRequests.Load() != before {
			t.Fatalf("readiness report = %#v", item)
		}
	}
	checkReady(http.StatusOK, "ready")
	unhealthy.Store(true)
	checkReady(http.StatusServiceUnavailable, "unavailable")
	initial := receipt.Report.Source
	clientSide := httptest.NewRecorder()
	misrouted := httptest.NewRequest(http.MethodGet, "/peer/v1/report", nil)
	misrouted.Header.Set("Authorization", "Bearer "+token)
	before := backendRequests.Load()
	r.ServeHTTP(clientSide, misrouted)
	if clientSide.Code != http.StatusNotFound || backendRequests.Load() != before {
		t.Fatalf("client listener exposed peer route: %d", clientSide.Code)
	}
	plain := httptest.NewRecorder()
	r.PeerHandler().ServeHTTP(plain, httptest.NewRequest(http.MethodGet, "/peer/v1/report", nil))
	if plain.Code != http.StatusForbidden {
		t.Fatalf("plaintext peer request = %d", plain.Code)
	}

	newToken := strings.Repeat("c", 32)
	allowDiagnostics = false
	write(newToken, "node-a", certPath)
	if err := r.reload(); err != nil {
		t.Fatal(err)
	}
	if report := getDiagnostics(); report.BackendReadiness != nil {
		t.Fatal("revoked diagnostics grant still received readiness")
	}
	old := get(token)
	_ = old.Body.Close()
	if old.StatusCode != http.StatusUnauthorized {
		t.Fatalf("old token status = %d", old.StatusCode)
	}
	response = get(newToken)
	receipt, err = nodecontract.DecodeReport(response.Body, "node-a", time.Now())
	_ = response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Report.Source.ProxyInstanceID != initial.ProxyInstanceID || receipt.Report.Source.ConfigGeneration != initial.ConfigGeneration+1 {
		t.Fatalf("reloaded source = %#v, initial = %#v", receipt.Report.Source, initial)
	}
	write(newToken, "node-a", certPath+"-missing")
	if err := r.reload(); err == nil {
		t.Fatal("invalid TLS reload succeeded")
	}
	write(newToken, "node-other", certPath)
	if err := r.reload(); err == nil {
		t.Fatal("node identity change succeeded without restart")
	}
	response = get(newToken)
	receipt, err = nodecontract.DecodeReport(response.Body, "node-a", time.Now())
	_ = response.Body.Close()
	if err != nil || receipt.Report.Source.ConfigGeneration != initial.ConfigGeneration+1 {
		t.Fatalf("failed reload changed active state: %#v, %v", receipt.Report.Source, err)
	}
	if err := os.WriteFile(path, []byte(fmt.Sprintf("default_backend: local\nbackends:\n  local: {url: %q}\n", local.URL)), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := r.reload(); err != nil {
		t.Fatal(err)
	}
	response = get(newToken)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("disabled peer status = %d", response.StatusCode)
	}
	write(newToken, "node-a", certPath)
	if err := r.reload(); err != nil {
		t.Fatal(err)
	}
	response = get(newToken)
	receipt, err = nodecontract.DecodeReport(response.Body, "node-a", time.Now())
	_ = response.Body.Close()
	if err != nil || receipt.Report.Source.ConfigGeneration != initial.ConfigGeneration+3 ||
		receipt.Report.Source.ProxyInstanceID != initial.ProxyInstanceID {
		t.Fatalf("reenabled peer source = %#v, %v", receipt.Report.Source, err)
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

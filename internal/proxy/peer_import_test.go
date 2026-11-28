package proxy

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/discovery"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/federation"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/nodecontract"
)

func TestPeerCatalogRefreshKeepsOwnerEvidenceWithinConfiguredTTL(t *testing.T) {
	refreshed := make(chan struct{}, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			t.Errorf("unexpected upstream request %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"data":[{"id":"model"}]}`))
		refreshed <- struct{}{}
	}))
	defer backend.Close()
	cfg := baseConfig(backend.URL)
	cfg.ModelsCacheTTL = 5 * time.Minute
	handler := newHandler(t, cfg).(*Handler)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	state := &reloaderState{
		handler: handler, peerContext: ctx,
		peer:     &config.PeerConfig{NodeID: "owner", LocalBackends: []string{"default"}},
		cacheTTL: cfg.ModelsCacheTTL,
	}
	r := &Reloader{}
	r.refreshPeerCatalogs(state)
	select {
	case <-refreshed:
	case <-time.After(time.Second):
		t.Fatal("owner without frontend catalog traffic did not refresh its cache")
	}
	now := time.Now()
	report, err := nodecontract.LocalReportFromCatalog(ctx, nodecontract.Source{
		NodeID: "owner", ProxyInstanceID: strings.Repeat("a", 32), ConfigGeneration: 1,
	},
		[]string{"default"}, handler.catalog, now, cfg.ModelsCacheTTL)
	if err != nil || len(report.Observations) != 1 {
		t.Fatalf("owner report: %v, %#v", err, report)
	}
	cache := &peerCache{entries: make(map[string]peerCacheEntry)}
	cache.update("owner", &nodecontract.Receipt{Report: report, ReceivedAt: now}, "", now)
	if _, ok := cache.freshModel(report.Observations[0].Identity, now); !ok {
		t.Fatal("background-refreshed owner model is not routable")
	}
}

func TestPeerCacheDropsRevokedDynamicCatalogButRetainsStaticAndGrantedPeers(t *testing.T) {
	now := time.Now().UTC()
	cache := &peerCache{entries: make(map[string]peerCacheEntry)}
	cache.updateSource("static-peer", nil, "", now, false, false)
	cache.updateSource("granted-peer", nil, "", now, true, false)
	cache.updateSource("revoked-peer", nil, "", now, true, false)
	cache.retainDynamic(map[string]struct{}{"granted-peer": {}})
	if _, ok := cache.entries["static-peer"]; !ok {
		t.Fatal("static peer cache was removed with dynamic grant cleanup")
	}
	if entry, ok := cache.entries["granted-peer"]; !ok || !entry.dynamic {
		t.Fatal("active dynamic grant cache was removed")
	}
	if _, ok := cache.entries["revoked-peer"]; ok {
		t.Fatal("revoked dynamic catalog cache remained available")
	}
	cache.markDynamicUnavailable("granted-peer", "not_discovered", false, now.Add(time.Second))
	if entry := cache.entries["granted-peer"]; entry.lastError != "not_discovered" {
		t.Fatalf("unavailable dynamic peer cache error = %q", entry.lastError)
	}
}

type discoveredPeerHarness struct {
	server            *httptest.Server
	reportRequests    *atomic.Int32
	inferenceRequests *atomic.Int32
	localCertificate  tls.Certificate
	roots             *x509.CertPool
	serverLeaf        *x509.Certificate
	crls              []*x509.RevocationList
}

// newDiscoveredPeerHarness creates an mTLS peer test server that serves catalog
// reports and inference, together with the client identity material used by the
// discovered peer client.
func newDiscoveredPeerHarness(t *testing.T) *discoveredPeerHarness {
	t.Helper()
	ca, caKey, roots := newPeerIdentityTestCA(t)
	localCertificate, _ := newPeerIdentityTestPair(t, ca, caKey, "lab.example", "node-a")
	serverCertificate, serverLeaf := newPeerIdentityTestPair(t, ca, caKey, "lab.example", "node-b")
	var reportRequests, inferenceRequests atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "" {
			t.Errorf("mTLS-only dynamic peer request carried Authorization: %q", request.Header.Get("Authorization"))
		}
		if request.Method != http.MethodGet {
			t.Errorf("unexpected discovered peer method %s", request.Method)
		}
		if request.URL.Path == "/peer/v1/inference" {
			inferenceRequests.Add(1)
			w.WriteHeader(http.StatusOK)
			return
		}
		if request.URL.Path != "/peer/v1/report" {
			t.Errorf("unexpected discovered peer path %s", request.URL.Path)
		}
		reportRequests.Add(1)
		now := time.Now().UTC()
		expires := now.Add(time.Minute)
		report := nodecontract.Report{
			ProtocolVersion: nodecontract.Version{Major: 1}, RequiredFeatures: []string{},
			Source:      nodecontract.Source{NodeID: "node-b", ProxyInstanceID: strings.Repeat("b", 32), ConfigGeneration: 1},
			GeneratedAt: now,
			Capabilities: nodecontract.Capabilities{
				"catalog_read": "enabled", "diagnostics_read": "disabled", "federation_exchange": "enabled",
				"artifact_read": "disabled", "artifact_write": "disabled", "config_write": "disabled",
			},
			Coverage: "complete", Observations: []nodecontract.Observation{{
				Identity: nodecontract.ModelIdentity{OwnerNodeID: "node-b", BackendID: "gpu", LocalID: "model"},
				State:    "supported", ObservedAt: &now, ExpiresAt: &expires, Freshness: "fresh", Coverage: "complete",
			}},
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(report); err != nil {
			t.Errorf("encode test peer report: %v", err)
		}
	}))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{serverCertificate},
		ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	server.StartTLS()
	return &discoveredPeerHarness{
		server:            server,
		reportRequests:    &reportRequests,
		inferenceRequests: &inferenceRequests,
		localCertificate:  localCertificate,
		roots:             roots,
		serverLeaf:        serverLeaf,
		crls:              []*x509.RevocationList{newPeerIdentityTestCRL(t, ca, caKey)},
	}
}

// discoveredPeerStore bundles the pairing and catalog grant stores plus the
// provisioned member for the discovered peer end-to-end test.
type discoveredPeerStore struct {
	members *federation.PairingStore
	grants  *federation.CatalogGrantStore
}

// newDiscoveredPeerStore creates temp pairing and catalog grant stores, pairs
// node-b with the given server leaf fingerprint, and grants catalog read plus
// outgoing inference to the member.
func newDiscoveredPeerStore(t *testing.T, serverLeaf *x509.Certificate) *discoveredPeerStore {
	t.Helper()
	membershipDir, grantDir := t.TempDir(), t.TempDir()
	for _, directory := range []string{membershipDir, grantDir} {
		if err := os.Chmod(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	members, err := federation.OpenPairingStore(membershipDir, "lab.example", "node-a")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	member, err := members.AddMember("node-b", peerCertificateFingerprint(serverLeaf), now)
	if err != nil {
		t.Fatal(err)
	}
	grants, err := federation.OpenCatalogGrantStore(grantDir, "lab.example", "node-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := grants.GrantCatalogRead(member, now); err != nil {
		t.Fatal(err)
	}
	if _, err := grants.GrantInference(member, false, true, now); err != nil {
		t.Fatal(err)
	}
	return &discoveredPeerStore{members: members, grants: grants}
}

func newDiscoveredPeerState(t *testing.T, harness *discoveredPeerHarness, store *discoveredPeerStore) *reloaderState {
	t.Helper()
	return &reloaderState{
		peer: &config.PeerConfig{NodeID: "node-a", FederationID: "lab.example", Discovery: &config.PeerDiscoveryConfig{
			Enabled: true, Interface: "lan0", AdvertiseURL: "https://node-a.local:9293",
		}},
		certificate: &harness.localCertificate, peerClientCAs: harness.roots,
		peerClientCRLs: harness.crls,
		pairingStore:   store.members, catalogGrantStore: store.grants, peerContext: context.Background(), cacheTTL: time.Minute,
	}
}

// assertDiscoveredPeerClient verifies the discovered client's dynamic mTLS flags
// and the authorized discovery ID set.
func assertDiscoveredPeerClient(t *testing.T, client *peerClient, discovered discoveredPeerClients) {
	t.Helper()
	if client == nil || !client.dynamic || client.token != "" || !client.routeInference || len(discovered.authorizedIDs) != 1 {
		t.Fatalf("dynamic peer client set = %+v / %+v", client, discovered)
	}
}

// assertDiscoveredPeerFetch verifies the mTLS catalog fetch returns an
// authenticated receipt with the expected observation.
func assertDiscoveredPeerFetch(t *testing.T, client *peerClient) {
	t.Helper()
	receipt, reason := client.fetch(context.Background())
	if reason != "" || receipt == nil || receipt.AuthenticatedNodeID != "node-b" || len(receipt.Report.Observations) != 1 {
		t.Fatalf("mTLS catalog fetch = %#v, reason %q", receipt, reason)
	}
}

// assertDiscoveredPeerInference verifies the separately granted mTLS inference
// request succeeds.
func assertDiscoveredPeerInference(t *testing.T, client *peerClient, serverURL string) {
	t.Helper()
	inferenceResponse, err := (&http.Client{Transport: client.inferenceTransport, Timeout: peerReportTimeout}).Get(serverURL + "/peer/v1/inference")
	if err != nil {
		t.Fatalf("separately granted mTLS inference request failed: %v", err)
	}
	_ = inferenceResponse.Body.Close()
	if inferenceResponse.StatusCode != http.StatusOK {
		t.Fatalf("dynamic inference status = %d", inferenceResponse.StatusCode)
	}
}

// assertDiscoveredPeerRevoked verifies that revoking the catalog read grant makes
// the catalog fetch unavailable and disables inference through the cascade.
func assertDiscoveredPeerRevoked(t *testing.T, client *peerClient, serverURL string) {
	t.Helper()
	if receipt, reason := client.fetch(context.Background()); receipt != nil || reason != "unavailable" {
		t.Fatalf("revoked mTLS catalog fetch = %#v, reason %q", receipt, reason)
	}
	if _, err := (&http.Client{Transport: client.inferenceTransport, Timeout: peerReportTimeout}).Get(serverURL + "/peer/v1/inference"); err == nil {
		t.Fatal("outbound inference remained enabled after catalog grant revocation cascade")
	}
}

func TestDiscoveredPeerClientUsesMTLSAndRechecksExplicitCatalogGrant(t *testing.T) {
	harness := newDiscoveredPeerHarness(t)
	defer harness.server.Close()
	store := newDiscoveredPeerStore(t, harness.serverLeaf)
	defer store.members.Close()
	defer store.grants.Close()
	state := newDiscoveredPeerState(t, harness, store)
	discovered, err := (&Reloader{}).buildDiscoveredPeerClientsWithBrowse(state, func(_ context.Context, settings discovery.Settings, timeout time.Duration) (discovery.CandidateResult, error) {
		if settings.NodeID != "node-a" || settings.FederationID != "lab.example" || settings.Interface != "lan0" || timeout < 250*time.Millisecond {
			t.Fatalf("dynamic discovery settings = %+v, timeout %v", settings, timeout)
		}
		return discovery.CandidateResult{Candidates: []discovery.Candidate{{
			NodeID: "node-b", FederationID: "lab.example", PeerURL: harness.server.URL,
		}}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	client := discovered.clients["node-b"]
	assertDiscoveredPeerClient(t, client, discovered)
	defer closePeerClients(map[string]*peerClient{"node-b": client})
	assertDiscoveredPeerFetch(t, client)
	assertDiscoveredPeerInference(t, client, harness.server.URL)
	if err := store.grants.RevokeCatalogRead("node-b"); err != nil {
		t.Fatal(err)
	}
	assertDiscoveredPeerRevoked(t, client, harness.server.URL)
	if got := harness.reportRequests.Load(); got != 1 {
		t.Fatalf("report requests after grant revocation = %d, want 1", got)
	}
	if got := harness.inferenceRequests.Load(); got != 1 {
		t.Fatalf("inference requests after grant revocation = %d, want 1", got)
	}
}

func TestPeerRefreshIntervalTracksShortCatalogTTL(t *testing.T) {
	if got := peerRefreshIntervalForTTL(30 * time.Second); got != 10*time.Second {
		t.Fatalf("refresh interval = %v, want 10s", got)
	}
	if got := peerRefreshIntervalForTTL(5 * time.Minute); got != peerRefreshInterval {
		t.Fatalf("refresh interval = %v, want %v", got, peerRefreshInterval)
	}
}

func TestCopyPeerClientAddressHeadersUsesObservedAddressAndAllowlist(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req.RemoteAddr = "192.0.2.45:54321"
	req.Header.Set("Cookie", "private-session")
	got := copyPeerClientAddressHeaders(req)
	if got.Get("X-Forwarded-For") != "192.0.2.45" || got.Get("Cookie") != "" {
		t.Fatalf("fallback address headers = %v", got)
	}

	req.Header.Set("X-Forwarded-For", "203.0.113.7")
	req.Header.Set("X-Real-IP", "203.0.113.8")
	got = copyPeerClientAddressHeaders(req)
	if got.Get("X-Forwarded-For") != "203.0.113.7" || got.Get("X-Real-IP") != "203.0.113.8" || got.Get("Cookie") != "" {
		t.Fatalf("preserved address headers = %v", got)
	}

	req.Header.Del("X-Forwarded-For")
	got = copyPeerClientAddressHeaders(req)
	if got.Get("X-Forwarded-For") != "203.0.113.8" || got.Get("X-Real-IP") != "203.0.113.8" {
		t.Fatalf("X-Real-IP-only fallback headers = %v", got)
	}
}

type peerInferenceEvent struct {
	model, authorization, apiKey, private, partPrivate, file string
	forwardedFor, realIP                                     string
}

type peerExchangeLocalConfig struct {
	id, addr, cert, key, backend string
}

type peerExchangePeerConfig struct {
	addr, ca, inToken, outToken string
}

type peerExchangeConfig struct {
	path   string
	local  peerExchangeLocalConfig
	peerID string
	peer   *peerExchangePeerConfig
}

type twoPeerExchangeHarness struct {
	t              *testing.T
	events         chan peerInferenceEvent
	canceled       chan struct{}
	localInference *atomic.Int32
	backendA       *httptest.Server
	backendB       *httptest.Server
	certA, keyA    string
	certB, keyB    string
	listenerA      net.Listener
	listenerB      net.Listener
	addrA, addrB   string
	tokenA, tokenB string
	diag           string
	pathA, pathB   string
	a, b           *Reloader
	serverA        *http.Server
	clientServer   *httptest.Server
	clientServerA  *httptest.Server
	modelID        string
	reverseModelID string
	inferenceBody  string
}

func writePeerExchangeConfig(t *testing.T, diag string, cfg peerExchangeConfig) {
	t.Helper()
	text := fmt.Sprintf(`default_backend: local
diagnostics_api_key: %q
backends: {local: {url: %q, api_key: "owner-backend-key", prefix: %q}}
path_routes: {"/v1": local}
peer:
  node_id: %q
  route_aliases: [%q]
  listen: %q
  tls_cert_file: %q
  tls_key_file: %q
  local_backends: [local]
`, diag, cfg.local.backend, cfg.local.id+"/", cfg.local.id, cfg.peerID, cfg.local.addr, cfg.local.cert, cfg.local.key)
	if cfg.peer != nil {
		text += fmt.Sprintf("  peers:\n    %s: {token: %q, catalog_read: true, diagnostics_read: true, inference: true, url: %q, outbound_token: %q, ca_file: %q, route_inference: true}\n",
			cfg.peerID, cfg.peer.inToken, "https://"+cfg.peer.addr, cfg.peer.outToken, cfg.peer.ca)
	}
	if err := os.WriteFile(cfg.path, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
}

func startPeerExchangeServer(t *testing.T, reloader *Reloader, listener net.Listener) *http.Server {
	t.Helper()
	server := &http.Server{Handler: reloader.PeerHandler(), TLSConfig: reloader.PeerTLSConfig()}
	go func() { _ = server.ServeTLS(listener, "", "") }()
	t.Cleanup(func() { _ = server.Close() })
	return server
}

func waitPeerExchangeView(t *testing.T, reloader *Reloader, peerID, status string) peerView {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		views, ok := reloader.state.Load().peerCache.views(time.Now(), peerID)
		if ok && len(views) == 1 && views[0].Status == status {
			return views[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("peer %s did not become %s", peerID, status)
	return peerView{}
}

func assertPeerExchangeReports(t *testing.T, viewA, viewB peerView) {
	t.Helper()
	if viewA.Report == nil || len(viewA.Report.Observations) != 1 ||
		viewA.Report.Observations[0].Identity.LocalID != "model-b" ||
		!slices.Contains(viewA.Report.Observations[0].Aliases, "model-b-alias") ||
		viewA.Report.Capabilities["federation_exchange"] != "enabled" ||
		viewB.Report == nil || len(viewB.Report.Observations) != 1 ||
		viewB.Report.Observations[0].Identity.LocalID != "model-a" {
		t.Fatal("two-node reports did not contain exactly the owned model")
	}
	if viewA.Report.BackendReadiness == nil || len(*viewA.Report.BackendReadiness) != 1 ||
		(*viewA.Report.BackendReadiness)[0].Status != "ready" ||
		viewB.Report.BackendReadiness == nil || len(*viewB.Report.BackendReadiness) != 1 ||
		(*viewB.Report.BackendReadiness)[0].Status != "ready" {
		t.Fatal("two-node reports did not retain readiness evidence")
	}
}

func newTwoPeerExchangeHarness(t *testing.T) *twoPeerExchangeHarness {
	t.Helper()
	h := &twoPeerExchangeHarness{
		t: t, events: make(chan peerInferenceEvent, 4), canceled: make(chan struct{}, 1),
		localInference: &atomic.Int32{}, tokenA: strings.Repeat("a", 32),
		tokenB: strings.Repeat("b", 32), diag: strings.Repeat("d", 32),
		pathA: configPath(t), pathB: configPath(t),
		modelID: "udms/model-a", reverseModelID: "bigboi/model-b-alias",
	}
	h.backendA = newTwoPeerBackend(t, "model-a", h.events, h.canceled, h.localInference)
	h.backendB = newTwoPeerBackend(t, "model-b", h.events, h.canceled, h.localInference)
	t.Cleanup(h.backendA.Close)
	t.Cleanup(h.backendB.Close)
	h.certA, h.keyA, _ = peerTestCertificate(t)
	h.certB, h.keyB, _ = peerTestCertificate(t)
	listenerA, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h.addrA = listenerA.Addr().String()
	listenerB, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = listenerA.Close()
		t.Fatal(err)
	}
	h.addrB = listenerB.Addr().String()
	h.listenerA, h.listenerB = listenerA, listenerB
	writePeerExchangeConfig(t, h.diag, peerExchangeConfig{
		path: h.pathA, local: peerExchangeLocalConfig{id: "udms", addr: h.addrA, cert: h.certA, key: h.keyA, backend: h.backendA.URL},
		peerID: "bigboi", peer: &peerExchangePeerConfig{addr: h.addrB, ca: h.certB, inToken: h.tokenA, outToken: h.tokenB},
	})
	writePeerExchangeConfig(t, h.diag, peerExchangeConfig{
		path: h.pathB, local: peerExchangeLocalConfig{id: "bigboi", addr: h.addrB, cert: h.certB, key: h.keyB, backend: h.backendB.URL},
		peerID: "udms", peer: &peerExchangePeerConfig{addr: h.addrA, ca: h.certA, inToken: h.tokenB, outToken: h.tokenA},
	})
	var errA, errB error
	h.a, _, errA = NewReloader(h.pathA, discardLogger(), nil, nil, nil, nil, nil)
	if errA != nil {
		t.Fatal(errA)
	}
	h.b, _, errB = NewReloader(h.pathB, discardLogger(), nil, nil, nil, nil, nil)
	if errB != nil {
		h.a.Stop()
		t.Fatal(errB)
	}
	warmPeerExchangeReloader(t, h.a)
	warmPeerExchangeReloader(t, h.b)
	h.serverA = startPeerExchangeServer(t, h.a, listenerA)
	startPeerExchangeServer(t, h.b, listenerB)
	h.a.Start()
	h.b.Start()
	t.Cleanup(h.a.Stop)
	t.Cleanup(h.b.Stop)
	viewA := waitPeerExchangeView(t, h.a, "bigboi", "fresh")
	viewB := waitPeerExchangeView(t, h.b, "udms", "fresh")
	assertPeerExchangeReports(t, viewA, viewB)
	h.b.refreshPeers(h.b.state.Load())
	viewB = waitPeerExchangeView(t, h.b, "udms", "fresh")
	if viewB.Report == nil || len(viewB.Report.Observations) != 1 ||
		viewB.Report.Observations[0].Identity.OwnerNodeID != "udms" {
		t.Fatal("imported models were re-exported")
	}
	h.clientServer = httptest.NewServer(h.b)
	h.clientServerA = httptest.NewServer(h.a)
	t.Cleanup(h.clientServer.Close)
	t.Cleanup(h.clientServerA.Close)
	h.inferenceBody = fmt.Sprintf(`{"model":%q,"stream":true}`, h.modelID)
	return h
}

func warmPeerExchangeReloader(t *testing.T, reloader *Reloader) {
	t.Helper()
	for _, path := range []string{"/v1/models", "/ready"} {
		w := httptest.NewRecorder()
		reloader.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != http.StatusOK {
			t.Fatalf("warm %s: %d", strings.Trim(path, "/"), w.Code)
		}
	}
}

func newTwoPeerBackend(t *testing.T, id string, events chan<- peerInferenceEvent, canceled chan<- struct{}, localInference *atomic.Int32) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodGet {
			servePeerExchangeCatalog(w, id)
			return
		}
		if id == "model-b" {
			servePeerExchangeLocalInference(t, w, request, localInference)
			return
		}
		if strings.HasPrefix(request.Header.Get("Content-Type"), "multipart/form-data") {
			servePeerExchangeMultipart(t, w, request, events)
			return
		}
		servePeerExchangeStream(t, w, request, events, canceled)
	}))
}

func servePeerExchangeCatalog(w http.ResponseWriter, id string) {
	if id == "model-b" {
		_, _ = io.WriteString(w, `{"data":[{"id":"model-b","meta":{"llamaswap":{"aliases":["model-b-alias"]}}}]}`)
		return
	}
	_, _ = fmt.Fprintf(w, `{"data":[{"id":%q}]}`, id)
}

func servePeerExchangeLocalInference(t *testing.T, w http.ResponseWriter, request *http.Request, localInference *atomic.Int32) {
	t.Helper()
	model := ""
	if strings.HasPrefix(request.Header.Get("Content-Type"), "multipart/form-data") {
		model = readPeerExchangeMultipartModel(t, request)
		if model != "bigboi/model-b" {
			t.Errorf("local multipart model = %q", model)
		}
	} else {
		var body struct {
			Model string `json:"model"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Error(err)
			return
		}
		model = body.Model
		if model != "model-b-alias" {
			t.Errorf("peer alias was not preserved to the backend: %q", model)
		}
	}
	localInference.Add(1)
	_, _ = io.WriteString(w, `{"ok":true}`)
}

func readPeerExchangeMultipartModel(t *testing.T, request *http.Request) string {
	t.Helper()
	reader, err := request.MultipartReader()
	if err != nil {
		t.Error(err)
		return ""
	}
	model := ""
	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			return model
		}
		if err != nil {
			t.Error(err)
			return model
		}
		data, err := io.ReadAll(part)
		_ = part.Close()
		if err != nil {
			t.Error(err)
			return model
		}
		if part.FormName() == "model" {
			model = string(data)
		}
	}
}

func servePeerExchangeMultipart(t *testing.T, w http.ResponseWriter, request *http.Request, events chan<- peerInferenceEvent) {
	t.Helper()
	reader, err := request.MultipartReader()
	if err != nil {
		t.Error(err)
		return
	}
	event := peerInferenceEvent{
		authorization: request.Header.Get("Authorization"), apiKey: request.Header.Get("X-Api-Key"),
		private: request.Header.Get("X-Private"), forwardedFor: request.Header.Get("X-Forwarded-For"),
		realIP: request.Header.Get("X-Real-IP"),
	}
	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Error(err)
			return
		}
		data, err := io.ReadAll(part)
		if err != nil {
			t.Error(err)
			return
		}
		if part.FormName() == "model" {
			event.model = string(data)
		} else if part.FormName() == "file" {
			event.file, event.partPrivate = string(data), part.Header.Get("X-Private")
		}
	}
	events <- event
	_, _ = io.WriteString(w, `{"ok":true}`)
}

func servePeerExchangeStream(t *testing.T, w http.ResponseWriter, request *http.Request, events chan<- peerInferenceEvent, canceled chan<- struct{}) {
	t.Helper()
	var body struct {
		Model string `json:"model"`
	}
	if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
		t.Error(err)
		return
	}
	events <- peerInferenceEvent{
		model: body.Model, authorization: request.Header.Get("Authorization"),
		apiKey: request.Header.Get("X-Api-Key"), private: request.Header.Get("X-Private"),
		forwardedFor: request.Header.Get("X-Forwarded-For"), realIP: request.Header.Get("X-Real-IP"),
	}
	w.Header().Set("Content-Type", "text/event-stream")
	_, _ = io.WriteString(w, "data: first\n\n")
	w.(http.Flusher).Flush()
	<-request.Context().Done()
	canceled <- struct{}{}
}

func TestTwoPeerReportExchangeOutageAndRemoval(t *testing.T) {
	h := newTwoPeerExchangeHarness(t)
	h.verifyPeerCatalogs()
	h.verifyReversePeerInference()
	h.verifyPeerStreamingInference()
	h.verifyPeerMultipartInference()
	h.verifyPeerDiagnostics()
	h.simulatePeerOutage()
	h.recoverPeer()
	h.removePeer()
}

func (h *twoPeerExchangeHarness) verifyPeerCatalogs() {
	catalogResp, err := http.Get(h.clientServer.URL + "/v1/models")
	if err != nil {
		h.t.Fatal(err)
	}
	catalogBody, err := io.ReadAll(catalogResp.Body)
	_ = catalogResp.Body.Close()
	if err != nil || !strings.Contains(string(catalogBody), h.modelID) || !strings.Contains(string(catalogBody), `"receipt_status":"fresh"`) {
		h.t.Fatalf("combined catalog missing peer: %s (%v)", catalogBody, err)
	}
	reverseCatalog, err := http.Get(h.clientServerA.URL + "/v1/models")
	if err != nil {
		h.t.Fatal(err)
	}
	reverseCatalogBody, err := io.ReadAll(reverseCatalog.Body)
	_ = reverseCatalog.Body.Close()
	if err != nil || !strings.Contains(string(reverseCatalogBody), `"id":"bigboi/model-b"`) || !strings.Contains(string(reverseCatalogBody), `"model-b-alias"`) {
		h.t.Fatalf("reverse combined catalog missing peer: %s (%v)", reverseCatalogBody, err)
	}
}

func (h *twoPeerExchangeHarness) verifyReversePeerInference() {
	body := fmt.Sprintf(`{"model":%q,"stream":false}`, h.reverseModelID)
	response, err := http.Post(h.clientServerA.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		h.t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK || h.localInference.Load() != 1 {
		h.t.Fatalf("reverse peer inference status=%d owner requests=%d", response.StatusCode, h.localInference.Load())
	}
}

func (h *twoPeerExchangeHarness) verifyPeerStreamingInference() {
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, h.clientServer.URL+"/v1/chat/completions", strings.NewReader(h.inferenceBody))
	if err != nil {
		h.t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer client-inference-secret")
	request.Header.Set("X-Api-Key", "client-inference-secret")
	request.Header.Set("X-Private", "client-private-value")
	request.Header.Set("X-Forwarded-For", "203.0.113.77")
	request.Header.Set("X-Real-IP", "203.0.113.77")
	stream, err := (&http.Client{Timeout: 4 * time.Second}).Do(request)
	if err != nil {
		h.t.Fatal(err)
	}
	buf := make([]byte, len("data: first\n\n"))
	if _, err := io.ReadFull(stream.Body, buf); err != nil || string(buf) != "data: first\n\n" {
		h.t.Fatalf("stream first chunk = %q, %v", buf, err)
	}
	select {
	case event := <-h.events:
		if event.model != "model-a" || event.authorization != "Bearer owner-backend-key" ||
			event.apiKey != "owner-backend-key" || event.private != "" ||
			!strings.HasPrefix(event.forwardedFor, "203.0.113.77") || event.realIP != "203.0.113.77" {
			h.t.Fatalf("credential or model isolation failed: %#v", event)
		}
	case <-time.After(time.Second):
		h.t.Fatal("owner backend did not receive request")
	}
	cancelRequest()
	_ = stream.Body.Close()
	select {
	case <-h.canceled:
	case <-time.After(time.Second):
		h.t.Fatal("client cancellation did not reach owner backend")
	}
}

func (h *twoPeerExchangeHarness) verifyPeerMultipartInference() {
	localBody := &bytes.Buffer{}
	localForm := multipart.NewWriter(localBody)
	if err := localForm.WriteField("model", "bigboi/model-b"); err != nil {
		h.t.Fatal(err)
	}
	localContentType := localForm.FormDataContentType()
	if err := localForm.Close(); err != nil {
		h.t.Fatal(err)
	}
	localResponse, err := http.Post(h.clientServer.URL+"/v1/audio/transcriptions", localContentType, localBody)
	if err != nil {
		h.t.Fatal(err)
	}
	_ = localResponse.Body.Close()
	if localResponse.StatusCode != http.StatusOK || h.localInference.Load() != 2 {
		h.t.Fatalf("local multipart status=%d count=%d", localResponse.StatusCode, h.localInference.Load())
	}

	peerBody := &bytes.Buffer{}
	form := multipart.NewWriter(peerBody)
	if err := form.WriteField("model", h.modelID); err != nil {
		h.t.Fatal(err)
	}
	partHeader := make(textproto.MIMEHeader)
	partHeader.Set("Content-Disposition", `form-data; name="file"; filename="audio.wav"`)
	partHeader.Set("Content-Type", "audio/wav")
	partHeader.Set("X-Private", "private-part-value")
	part, err := form.CreatePart(partHeader)
	if err != nil {
		h.t.Fatal(err)
	}
	audioBytes := []byte{0x00, 0xff, 0x80, '\r', '\n', 0x01}
	if _, err := part.Write(audioBytes); err != nil {
		h.t.Fatal(err)
	}
	contentType := form.FormDataContentType()
	if err := form.Close(); err != nil {
		h.t.Fatal(err)
	}
	response, err := http.Post(h.clientServer.URL+"/v1/audio/transcriptions", contentType, peerBody)
	if err != nil {
		h.t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK || h.localInference.Load() != 2 {
		h.t.Fatalf("peer multipart failed or fell back: status=%d count=%d", response.StatusCode, h.localInference.Load())
	}
	select {
	case event := <-h.events:
		if event.model != "model-a" || event.authorization != "Bearer owner-backend-key" ||
			event.apiKey != "owner-backend-key" || event.partPrivate != "" || event.file != string(audioBytes) {
			h.t.Fatalf("peer multipart isolation failed: %#v", event)
		}
	case <-time.After(time.Second):
		h.t.Fatal("owner backend did not receive multipart request")
	}
}

func (h *twoPeerExchangeHarness) verifyPeerDiagnostics() {
	unauthorized := httptest.NewRecorder()
	h.b.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/diagnostics/v1/peers?node_id=udms", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		h.t.Fatalf("diagnostics without authorization: %d", unauthorized.Code)
	}
	request := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/peers?node_id=udms", nil)
	request.Header.Set("Authorization", "Bearer "+h.diag)
	diagnostics := httptest.NewRecorder()
	h.b.ServeHTTP(diagnostics, request)
	if diagnostics.Code != http.StatusOK || !strings.Contains(diagnostics.Body.String(), "model-a") ||
		!strings.Contains(diagnostics.Body.String(), `"backend_readiness"`) ||
		strings.Contains(diagnostics.Body.String(), h.tokenA) {
		h.t.Fatalf("unexpected diagnostics status/content: %d", diagnostics.Code)
	}
}

func (h *twoPeerExchangeHarness) simulatePeerOutage() {
	if err := h.serverA.Close(); err != nil {
		h.t.Fatal(err)
	}
	select {
	case h.b.peerWake <- struct{}{}:
	default:
	}
	stale := waitPeerExchangeView(h.t, h.b, "udms", "stale")
	if stale.Report == nil || stale.LastError != "unavailable" {
		h.t.Fatalf("outage did not preserve stale report: %#v", stale)
	}
	catalog, err := http.Get(h.clientServer.URL + "/v1/models")
	if err != nil {
		h.t.Fatal(err)
	}
	data, err := io.ReadAll(catalog.Body)
	_ = catalog.Body.Close()
	if err != nil || !strings.Contains(string(data), h.modelID) || !strings.Contains(string(data), `"receipt_status":"stale"`) {
		h.t.Fatalf("stale peer disappeared from catalog: %s (%v)", data, err)
	}
	response, err := http.Post(h.clientServer.URL+"/v1/chat/completions", "application/json", strings.NewReader(h.inferenceBody))
	if err != nil {
		h.t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable || h.localInference.Load() != 2 {
		h.t.Fatalf("stale peer inference fell back: status=%d", response.StatusCode)
	}
}

func (h *twoPeerExchangeHarness) recoverPeer() {
	listener, err := net.Listen("tcp", h.addrA)
	if err != nil {
		h.t.Fatal(err)
	}
	startPeerExchangeServer(h.t, h.a, listener)
	select {
	case h.b.peerWake <- struct{}{}:
	default:
	}
	waitPeerExchangeView(h.t, h.b, "udms", "fresh")
	body := &bytes.Buffer{}
	form := multipart.NewWriter(body)
	if err := form.WriteField("model", h.modelID); err != nil {
		h.t.Fatal(err)
	}
	contentType := form.FormDataContentType()
	if err := form.Close(); err != nil {
		h.t.Fatal(err)
	}
	response, err := http.Post(h.clientServer.URL+"/v1/audio/transcriptions", contentType, body)
	if err != nil {
		h.t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK || h.localInference.Load() != 2 {
		h.t.Fatalf("recovered peer inference status=%d count=%d", response.StatusCode, h.localInference.Load())
	}
	select {
	case event := <-h.events:
		if event.model != "model-a" {
			h.t.Fatalf("recovered peer received wrong model: %q", event.model)
		}
	case <-time.After(time.Second):
		h.t.Fatal("recovered owner backend did not receive request")
	}
}

func (h *twoPeerExchangeHarness) removePeer() {
	writePeerExchangeConfig(h.t, h.diag, peerExchangeConfig{
		path: h.pathB, local: peerExchangeLocalConfig{id: "bigboi", addr: h.addrB, cert: h.certB, key: h.keyB, backend: h.backendB.URL},
		peerID: "udms",
	})
	if err := h.b.reload(); err != nil {
		h.t.Fatal(err)
	}
	if views, ok := h.b.state.Load().peerCache.views(time.Now(), "udms"); ok || len(views) != 0 {
		h.t.Fatal("removed peer retained in active cache")
	}
	h.assertPeerInferenceUnavailable("removed")
	disabledPeerConfig := fmt.Sprintf(`default_backend: local
backends: {local: {url: %q}}
peer:
  node_id: bigboi
  route_aliases: [udms]
  listen: %q
  tls_cert_file: %q
  tls_key_file: %q
  local_backends: [local]
`, h.backendB.URL, h.addrB, h.certB, h.keyB)
	if err := os.WriteFile(h.pathB, []byte(disabledPeerConfig), 0o600); err != nil {
		h.t.Fatal(err)
	}
	if err := h.b.reload(); err != nil {
		h.t.Fatal(err)
	}
	h.assertPeerInferenceUnavailable("disabled")
}

func (h *twoPeerExchangeHarness) assertPeerInferenceUnavailable(state string) {
	response, err := http.Post(h.clientServer.URL+"/v1/chat/completions", "application/json", strings.NewReader(h.inferenceBody))
	if err != nil {
		h.t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable || h.localInference.Load() != 2 {
		h.t.Fatalf("%s peer inference fell back: status=%d", state, response.StatusCode)
	}
}

func TestPeerRefreshStopsWithCanceledRequest(t *testing.T) {
	connected := make(chan struct{})
	server := httptest.NewTLSServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(connected)
		<-r.Context().Done()
	}))
	defer server.Close()
	caPath := filepath.Join(t.TempDir(), "peer-ca.pem")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	certPath, keyPath, _ := peerTestCertificate(t)
	path := configPath(t)
	text := fmt.Sprintf(`default_backend: local
backends: {local: {url: "http://127.0.0.1:1"}}
peer:
  node_id: node-a
  listen: "127.0.0.1:9293"
  tls_cert_file: %q
  tls_key_file: %q
  local_backends: [local]
  peers:
    node-b: {url: %q, outbound_token: %q, ca_file: %q}
`, certPath, keyPath, server.URL, strings.Repeat("x", 32), caPath)
	if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	r, _, err := NewReloader(path, discardLogger(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	r.Start()
	t.Cleanup(r.Stop)
	select {
	case <-connected:
	case <-time.After(2 * time.Second):
		t.Fatal("peer request did not start")
	}
	stopped := make(chan struct{})
	go func() { r.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("peer refresh did not stop promptly")
	}
}

func TestPeerReceiptFreshnessAndRetention(t *testing.T) {
	now := time.Now()
	cache := &peerCache{entries: map[string]peerCacheEntry{"node-a": {}}}
	receipt := &nodecontract.Receipt{AuthenticatedNodeID: "node-a", ReceivedAt: now,
		Report: nodecontract.Report{GeneratedAt: now.Add(-time.Hour)}}
	cache.update("node-a", receipt, "", now)
	views, _ := cache.views(now.Add(31*time.Second), "node-a")
	if views[0].Status != "stale" || views[0].Report == nil ||
		!views[0].Report.GeneratedAt.Equal(now.Add(-time.Hour)) {
		t.Fatal("receipt age changed or replaced source evidence")
	}
	cache.update("node-a", nil, "unavailable", now.Add(peerRetention+time.Second))
	views, _ = cache.views(now.Add(peerRetention+time.Second), "node-a")
	if views[0].Status != "unavailable" || views[0].Report != nil {
		t.Fatal("expired peer report was retained")
	}
}

func assertRejectedPeerFetchModes(t *testing.T, client *peerClient, mode *atomic.Int32, calls *atomic.Int32) {
	t.Helper()
	for n, want := range []string{"http_error", "invalid_report", "invalid_report", "unauthorized", "invalid_report"} {
		mode.Store(int32(n))
		if receipt, reason := client.fetch(context.Background()); receipt != nil || reason != want {
			t.Fatalf("mode %d: receipt=%v reason=%s", n, receipt, reason)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("peer redirect was followed")
	}
}

func TestPeerFetchRejectsRedirectsAndInvalidReports(t *testing.T) {
	var calls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer target.Close()
	var mode atomic.Int32
	makeReport := func(id string) []byte {
		t.Helper()
		report, err := nodecontract.LocalReport(nodecontract.Source{
			NodeID: id, ProxyInstanceID: strings.Repeat("a", 32), ConfigGeneration: 1,
		}, nil, nil, time.Now(), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(report)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	wrongNode, correctNode := makeReport("node-a"), makeReport("node-b")
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch mode.Load() {
		case 0:
			http.Redirect(w, r, target.URL, http.StatusFound)
		case 1:
			_, _ = io.WriteString(w, `{"protocol_version":{"major":2,"minor":0}}`)
		case 2:
			_, _ = io.WriteString(w, strings.Repeat("x", nodecontract.MaxReportBytes+1))
		case 3:
			w.WriteHeader(http.StatusUnauthorized)
		case 4:
			_, _ = w.Write(wrongNode)
		case 5:
			_, _ = w.Write(correctNode)
		}
	}))
	defer server.Close()
	caPath := filepath.Join(t.TempDir(), "peer-ca.pem")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	clients, _, err := newPeerClients(&config.PeerConfig{Peers: map[string]config.PeerGrant{
		"node-b": {URL: server.URL, OutboundToken: strings.Repeat("t", 32), CAFile: caPath},
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer closePeerClients(clients)
	client := clients["node-b"]
	assertRejectedPeerFetchModes(t, client, &mode, &calls)
	mode.Store(5)
	if receipt, reason := client.fetch(context.Background()); receipt == nil || reason != "" || receipt.Report.Source.NodeID != "node-b" {
		t.Fatalf("valid peer report: receipt=%v reason=%s", receipt, reason)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if receipt, reason := client.fetch(ctx); receipt != nil || reason != "unavailable" {
		t.Fatalf("canceled fetch: receipt=%v reason=%s", receipt, reason)
	}
}

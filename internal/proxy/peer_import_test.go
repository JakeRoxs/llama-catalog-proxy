package proxy

import (
	"bytes"
	"context"
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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
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

func TestPeerRefreshIntervalTracksShortCatalogTTL(t *testing.T) {
	if got := peerRefreshIntervalForTTL(30 * time.Second); got != 10*time.Second {
		t.Fatalf("refresh interval = %v, want 10s", got)
	}
	if got := peerRefreshIntervalForTTL(5 * time.Minute); got != peerRefreshInterval {
		t.Fatalf("refresh interval = %v, want %v", got, peerRefreshInterval)
	}
}

func TestTwoPeerReportExchangeOutageAndRemoval(t *testing.T) {
	type inferenceEvent struct {
		model, authorization, apiKey, private, partPrivate, file string
	}
	events := make(chan inferenceEvent, 4)
	canceled := make(chan struct{}, 1)
	var localInference atomic.Int32
	makeBackend := func(id string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			if request.Method == http.MethodGet {
				_, _ = fmt.Fprintf(w, `{"data":[{"id":%q}]}`, id)
				return
			}
			if id == "model-b" {
				localInference.Add(1)
				_, _ = io.WriteString(w, `{"ok":true}`)
				return
			}
			if strings.HasPrefix(request.Header.Get("Content-Type"), "multipart/form-data") {
				reader, err := request.MultipartReader()
				if err != nil {
					t.Error(err)
					return
				}
				event := inferenceEvent{authorization: request.Header.Get("Authorization"), apiKey: request.Header.Get("X-Api-Key"), private: request.Header.Get("X-Private")}
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
				return
			}
			var body struct {
				Model string `json:"model"`
			}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Error(err)
				return
			}
			events <- inferenceEvent{model: body.Model, authorization: request.Header.Get("Authorization"), apiKey: request.Header.Get("X-Api-Key"), private: request.Header.Get("X-Private")}
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: first\n\n")
			w.(http.Flusher).Flush()
			<-request.Context().Done()
			canceled <- struct{}{}
		}))
	}
	backendA, backendB := makeBackend("model-a"), makeBackend("model-b")
	defer backendA.Close()
	defer backendB.Close()
	certA, keyA, _ := peerTestCertificate(t)
	certB, keyB, _ := peerTestCertificate(t)
	listenerA, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addrA := listenerA.Addr().String()
	listenerB, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = listenerA.Close()
		t.Fatal(err)
	}
	addrB := listenerB.Addr().String()
	tokenA, tokenB := strings.Repeat("a", 32), strings.Repeat("b", 32)
	diag := strings.Repeat("d", 32)
	pathA, pathB := configPath(t), configPath(t)
	write := func(path, id, addr, cert, key, backend, peerID, peerAddr, peerCA, incoming, outgoing string, withPeer bool) {
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
`, diag, backend, id+"/", id, peerID, addr, cert, key)
		if withPeer {
			text += fmt.Sprintf("  peers:\n    %s: {token: %q, catalog_read: true, diagnostics_read: true, inference: true, url: %q, outbound_token: %q, ca_file: %q, route_inference: true}\n",
				peerID, incoming, "https://"+peerAddr, outgoing, peerCA)
		}
		if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(pathA, "udms", addrA, certA, keyA, backendA.URL, "bigboi", addrB, certB, tokenA, tokenB, true)
	write(pathB, "bigboi", addrB, certB, keyB, backendB.URL, "udms", addrA, certA, tokenB, tokenA, true)
	a, _, err := NewReloader(pathA, discardLogger(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := NewReloader(pathB, discardLogger(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range []*Reloader{a, b} {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("warm catalog: %d", w.Code)
		}
		w = httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/ready", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("warm readiness: %d", w.Code)
		}
	}
	startServer := func(r *Reloader, listener net.Listener) *http.Server {
		server := &http.Server{Handler: r.PeerHandler(), TLSConfig: r.PeerTLSConfig()}
		go func() { _ = server.ServeTLS(listener, "", "") }()
		t.Cleanup(func() { _ = server.Close() })
		return server
	}
	serverA := startServer(a, listenerA)
	startServer(b, listenerB)
	a.Start()
	b.Start()
	t.Cleanup(a.Stop)
	t.Cleanup(b.Stop)
	waitView := func(r *Reloader, peerID, status string) peerView {
		t.Helper()
		deadline := time.Now().Add(4 * time.Second)
		for time.Now().Before(deadline) {
			views, ok := r.state.Load().peerCache.views(time.Now(), peerID)
			if ok && len(views) == 1 && views[0].Status == status {
				return views[0]
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("peer %s did not become %s", peerID, status)
		return peerView{}
	}
	viewA := waitView(a, "bigboi", "fresh")
	viewB := waitView(b, "udms", "fresh")
	if viewA.Report == nil || len(viewA.Report.Observations) != 1 ||
		viewA.Report.Observations[0].Identity.LocalID != "model-b" ||
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
	b.refreshPeers(b.state.Load())
	viewB = waitView(b, "udms", "fresh")
	if viewB.Report == nil || len(viewB.Report.Observations) != 1 ||
		viewB.Report.Observations[0].Identity.OwnerNodeID != "udms" {
		t.Fatal("imported models were re-exported")
	}
	modelID := "udms/model-a"
	clientServer := httptest.NewServer(b)
	defer clientServer.Close()
	catalogResp, err := http.Get(clientServer.URL + "/v1/models")
	if err != nil {
		t.Fatal(err)
	}
	catalogBody, err := io.ReadAll(catalogResp.Body)
	_ = catalogResp.Body.Close()
	if err != nil || !strings.Contains(string(catalogBody), modelID) || !strings.Contains(string(catalogBody), `"receipt_status":"fresh"`) {
		t.Fatalf("combined catalog missing peer: %s (%v)", catalogBody, err)
	}
	reverseModelID := "bigboi/model-b"
	clientServerA := httptest.NewServer(a)
	defer clientServerA.Close()
	reverseCatalog, err := http.Get(clientServerA.URL + "/v1/models")
	if err != nil {
		t.Fatal(err)
	}
	reverseCatalogBody, err := io.ReadAll(reverseCatalog.Body)
	_ = reverseCatalog.Body.Close()
	if err != nil || !strings.Contains(string(reverseCatalogBody), reverseModelID) {
		t.Fatalf("reverse combined catalog missing peer: %s (%v)", reverseCatalogBody, err)
	}
	reverseInferenceBody := fmt.Sprintf(`{"model":%q,"stream":false}`, reverseModelID)
	reverseResponse, err := http.Post(clientServerA.URL+"/v1/chat/completions", "application/json", strings.NewReader(reverseInferenceBody))
	if err != nil {
		t.Fatal(err)
	}
	_ = reverseResponse.Body.Close()
	if reverseResponse.StatusCode != http.StatusOK || localInference.Load() != 1 {
		t.Fatalf("reverse peer inference status=%d owner requests=%d", reverseResponse.StatusCode, localInference.Load())
	}
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	inferenceBody := fmt.Sprintf(`{"model":%q,"stream":true}`, modelID)
	inferenceReq, err := http.NewRequestWithContext(requestCtx, http.MethodPost, clientServer.URL+"/v1/chat/completions", strings.NewReader(inferenceBody))
	if err != nil {
		t.Fatal(err)
	}
	inferenceReq.Header.Set("Content-Type", "application/json")
	inferenceReq.Header.Set("Authorization", "Bearer client-inference-secret")
	inferenceReq.Header.Set("X-Api-Key", "client-inference-secret")
	inferenceReq.Header.Set("X-Private", "client-private-value")
	stream, err := (&http.Client{Timeout: 4 * time.Second}).Do(inferenceReq)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, len("data: first\n\n"))
	if _, err := io.ReadFull(stream.Body, buf); err != nil || string(buf) != "data: first\n\n" {
		t.Fatalf("stream first chunk = %q, %v", buf, err)
	}
	select {
	case event := <-events:
		if event.model != "model-a" || event.authorization != "Bearer owner-backend-key" ||
			event.apiKey != "owner-backend-key" || event.private != "" {
			t.Fatalf("credential or model isolation failed: %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("owner backend did not receive request")
	}
	cancelRequest()
	_ = stream.Body.Close()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("client cancellation did not reach owner backend")
	}
	multipartBody := &bytes.Buffer{}
	form := multipart.NewWriter(multipartBody)
	if err := form.WriteField("model", "bigboi/model-b"); err != nil {
		t.Fatal(err)
	}
	contentType := form.FormDataContentType()
	if err := form.Close(); err != nil {
		t.Fatal(err)
	}
	localResp, err := http.Post(clientServer.URL+"/v1/audio/transcriptions", contentType, multipartBody)
	if err != nil {
		t.Fatal(err)
	}
	_ = localResp.Body.Close()
	if localResp.StatusCode != http.StatusOK || localInference.Load() != 2 {
		t.Fatalf("local multipart status=%d count=%d", localResp.StatusCode, localInference.Load())
	}
	peerMultipart := &bytes.Buffer{}
	form = multipart.NewWriter(peerMultipart)
	if err := form.WriteField("model", modelID); err != nil {
		t.Fatal(err)
	}
	partHeader := make(textproto.MIMEHeader)
	partHeader.Set("Content-Disposition", `form-data; name="file"; filename="audio.wav"`)
	partHeader.Set("Content-Type", "audio/wav")
	partHeader.Set("X-Private", "private-part-value")
	part, err := form.CreatePart(partHeader)
	if err != nil {
		t.Fatal(err)
	}
	audioBytes := []byte{0x00, 0xff, 0x80, '\r', '\n', 0x01}
	if _, err := part.Write(audioBytes); err != nil {
		t.Fatal(err)
	}
	contentType = form.FormDataContentType()
	if err := form.Close(); err != nil {
		t.Fatal(err)
	}
	remoteResp, err := http.Post(clientServer.URL+"/v1/audio/transcriptions", contentType, peerMultipart)
	if err != nil {
		t.Fatal(err)
	}
	_ = remoteResp.Body.Close()
	if remoteResp.StatusCode != http.StatusOK || localInference.Load() != 2 {
		t.Fatalf("peer multipart failed or fell back: status=%d count=%d", remoteResp.StatusCode, localInference.Load())
	}
	select {
	case event := <-events:
		if event.model != "model-a" || event.authorization != "Bearer owner-backend-key" ||
			event.apiKey != "owner-backend-key" || event.partPrivate != "" || event.file != string(audioBytes) {
			t.Fatalf("peer multipart isolation failed: %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("owner backend did not receive multipart request")
	}
	unauth := httptest.NewRecorder()
	b.ServeHTTP(unauth, httptest.NewRequest(http.MethodGet, "/diagnostics/v1/peers?node_id=udms", nil))
	if unauth.Code != http.StatusUnauthorized {
		t.Fatalf("diagnostics without authorization: %d", unauth.Code)
	}
	request := httptest.NewRequest(http.MethodGet, "/diagnostics/v1/peers?node_id=udms", nil)
	request.Header.Set("Authorization", "Bearer "+diag)
	diagnostics := httptest.NewRecorder()
	b.ServeHTTP(diagnostics, request)
	if diagnostics.Code != http.StatusOK || !strings.Contains(diagnostics.Body.String(), "model-a") ||
		!strings.Contains(diagnostics.Body.String(), `"backend_readiness"`) ||
		strings.Contains(diagnostics.Body.String(), tokenA) {
		t.Fatalf("unexpected diagnostics status/content: %d", diagnostics.Code)
	}
	if err := serverA.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case b.peerWake <- struct{}{}:
	default:
	}
	stale := waitView(b, "udms", "stale")
	if stale.Report == nil || stale.LastError != "unavailable" {
		t.Fatalf("outage did not preserve stale report: %#v", stale)
	}
	staleCatalog, err := http.Get(clientServer.URL + "/v1/models")
	if err != nil {
		t.Fatal(err)
	}
	staleData, err := io.ReadAll(staleCatalog.Body)
	_ = staleCatalog.Body.Close()
	if err != nil || !strings.Contains(string(staleData), modelID) || !strings.Contains(string(staleData), `"receipt_status":"stale"`) {
		t.Fatalf("stale peer disappeared from catalog: %s (%v)", staleData, err)
	}
	staleResp, err := http.Post(clientServer.URL+"/v1/chat/completions", "application/json", strings.NewReader(inferenceBody))
	if err != nil {
		t.Fatal(err)
	}
	_ = staleResp.Body.Close()
	if staleResp.StatusCode != http.StatusServiceUnavailable || localInference.Load() != 2 {
		t.Fatalf("stale peer inference fell back: status=%d", staleResp.StatusCode)
	}
	listenerA, err = net.Listen("tcp", addrA)
	if err != nil {
		t.Fatal(err)
	}
	startServer(a, listenerA)
	select {
	case b.peerWake <- struct{}{}:
	default:
	}
	waitView(b, "udms", "fresh")
	recoveredBody := &bytes.Buffer{}
	form = multipart.NewWriter(recoveredBody)
	if err := form.WriteField("model", modelID); err != nil {
		t.Fatal(err)
	}
	contentType = form.FormDataContentType()
	if err := form.Close(); err != nil {
		t.Fatal(err)
	}
	recoveredResp, err := http.Post(clientServer.URL+"/v1/audio/transcriptions", contentType, recoveredBody)
	if err != nil {
		t.Fatal(err)
	}
	_ = recoveredResp.Body.Close()
	if recoveredResp.StatusCode != http.StatusOK || localInference.Load() != 2 {
		t.Fatalf("recovered peer inference status=%d count=%d", recoveredResp.StatusCode, localInference.Load())
	}
	select {
	case event := <-events:
		if event.model != "model-a" {
			t.Fatalf("recovered peer received wrong model: %q", event.model)
		}
	case <-time.After(time.Second):
		t.Fatal("recovered owner backend did not receive request")
	}
	write(pathB, "bigboi", addrB, certB, keyB, backendB.URL, "udms", addrA, certA, tokenB, tokenA, false)
	if err := b.reload(); err != nil {
		t.Fatal(err)
	}
	if views, ok := b.state.Load().peerCache.views(time.Now(), "udms"); ok || len(views) != 0 {
		t.Fatal("removed peer retained in active cache")
	}
	removedResp, err := http.Post(clientServer.URL+"/v1/chat/completions", "application/json", strings.NewReader(inferenceBody))
	if err != nil {
		t.Fatal(err)
	}
	_ = removedResp.Body.Close()
	if removedResp.StatusCode != http.StatusServiceUnavailable || localInference.Load() != 2 {
		t.Fatalf("removed peer inference fell back: status=%d", removedResp.StatusCode)
	}
	disabledPeerConfig := fmt.Sprintf(`default_backend: local
backends: {local: {url: %q}}
peer:
  node_id: bigboi
  route_aliases: [udms]
  listen: %q
  tls_cert_file: %q
  tls_key_file: %q
  local_backends: [local]
`, backendB.URL, addrB, certB, keyB)
	if err := os.WriteFile(pathB, []byte(disabledPeerConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := b.reload(); err != nil {
		t.Fatal(err)
	}
	disabledResp, err := http.Post(clientServer.URL+"/v1/chat/completions", "application/json", strings.NewReader(inferenceBody))
	if err != nil {
		t.Fatal(err)
	}
	_ = disabledResp.Body.Close()
	if disabledResp.StatusCode != http.StatusServiceUnavailable || localInference.Load() != 2 {
		t.Fatalf("disabled peer inference fell back: status=%d", disabledResp.StatusCode)
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
	for n, want := range []string{"http_error", "invalid_report", "invalid_report", "unauthorized", "invalid_report"} {
		mode.Store(int32(n))
		if receipt, reason := client.fetch(context.Background()); receipt != nil || reason != want {
			t.Fatalf("mode %d: receipt=%v reason=%s", n, receipt, reason)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("peer redirect was followed")
	}
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

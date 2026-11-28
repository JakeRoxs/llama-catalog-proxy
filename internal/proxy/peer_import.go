package proxy

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/federation"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/identity"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/nodecontract"
)

const (
	peerRefreshInterval = 15 * time.Second
	peerFreshAge        = 30 * time.Second
	peerRetention       = 15 * time.Minute
	peerRefreshWorkers  = 4
	maxPeerCABytes      = 1 << 20
)

type peerClient struct {
	id                 string
	endpoint           string
	origin             string
	token              string
	client             *http.Client
	serverCRLs         []*x509.RevocationList
	crlMaxAge          time.Duration
	dynamic            bool
	catalogRead        bool
	routeInference     bool
	routeManagement    bool
	routePaths         map[string]string
	inferenceTransport *http.Transport
}

type peerCache struct {
	mu      sync.RWMutex
	entries map[string]peerCacheEntry
}

type peerCacheEntry struct {
	receipt        *nodecontract.Receipt
	lastAttempt    time.Time
	lastError      string
	dynamic        bool
	routeInference bool
}

type peerView struct {
	NodeID      string               `json:"node_id"`
	Status      string               `json:"receipt_status"`
	LastAttempt *time.Time           `json:"last_attempt,omitempty"`
	LastError   string               `json:"last_error,omitempty"`
	ReceivedAt  *time.Time           `json:"received_at,omitempty"`
	Report      *nodecontract.Report `json:"report,omitempty"`
}

func newPeerClients(peer *config.PeerConfig) (map[string]*peerClient, *peerCache, error) {
	return newPeerClientsWithMembership(peer, nil)
}

func newPeerClientsWithMembership(peer *config.PeerConfig, membership *federation.PairingStore) (map[string]*peerClient, *peerCache, error) {
	clients := make(map[string]*peerClient)
	cache := &peerCache{entries: make(map[string]peerCacheEntry)}
	if peer == nil {
		return clients, cache, nil
	}
	clientCertificates, err := peerClientCertificates(peer)
	if err != nil {
		return nil, nil, err
	}
	for id, grant := range peer.Peers {
		if grant.URL == "" {
			continue
		}
		roots, serverCRLs, err := peerTrustMaterial(peer, grant, id)
		if err != nil {
			return nil, nil, err
		}
		clients[id], err = newPeerClientForGrant(peer, membership, id, grant, roots, serverCRLs, clientCertificates)
		if err != nil {
			return nil, nil, err
		}
		clients[id].inferenceTransport.MaxConnsPerHost = 8
		cache.entries[id] = peerCacheEntry{routeInference: grant.RouteInference}
	}
	return clients, cache, nil
}

func peerClientCertificates(peer *config.PeerConfig) ([]tls.Certificate, error) {
	if peer.FederationID == "" {
		return nil, nil
	}
	certificate, err := tls.LoadX509KeyPair(peer.TLSCertFile, peer.TLSKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load peer client identity: %w", err)
	}
	if len(certificate.Certificate) == 0 {
		return nil, fmt.Errorf("peer client identity certificate is empty")
	}
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("parse peer client identity certificate: %w", err)
	}
	certificate.Leaf = leaf
	if err := validatePeerIdentityCertificate(leaf, peer.FederationID, peer.NodeID); err != nil {
		return nil, fmt.Errorf("invalid peer client identity certificate: %w", err)
	}
	return []tls.Certificate{certificate}, nil
}

func peerTrustMaterial(peer *config.PeerConfig, grant config.PeerGrant, id string) (*x509.CertPool, []*x509.RevocationList, error) {
	roots, err := loadPeerTrustRoots(peer, grant.CAFile, id)
	if err != nil {
		return nil, nil, err
	}
	var serverCRLs []*x509.RevocationList
	if peer.FederationID != "" {
		serverCRLs, err = federation.LoadCRLs(grant.CRLFile, peer.RevocationMaxAge())
		if err != nil {
			return nil, nil, fmt.Errorf("invalid peer CRL for %q", id)
		}
	}
	return roots, serverCRLs, nil
}

func loadPeerTrustRoots(peer *config.PeerConfig, caFile, id string) (*x509.CertPool, error) {
	if caFile == "" {
		return nil, nil
	}
	if peer.FederationID != "" {
		roots, err := federation.LoadTrustRoots(caFile)
		if err != nil {
			return nil, fmt.Errorf("invalid peer CA for %q", id)
		}
		return roots, nil
	}
	file, err := os.Open(caFile)
	if err != nil {
		return nil, fmt.Errorf("invalid peer CA for %q", id)
	}
	data, readErr := io.ReadAll(io.LimitReader(file, maxPeerCABytes+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		return nil, fmt.Errorf("invalid peer CA for %q", id)
	}
	roots := x509.NewCertPool()
	if len(data) > maxPeerCABytes || !roots.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("invalid peer CA for %q", id)
	}
	return roots, nil
}

func newPeerClientForGrant(peer *config.PeerConfig, membership *federation.PairingStore, id string, grant config.PeerGrant,
	roots *x509.CertPool, serverCRLs []*x509.RevocationList, clientCertificates []tls.Certificate) (*peerClient, error) {
	transport := &http.Transport{
		TLSClientConfig:    &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: clientCertificates},
		DisableCompression: true, MaxConnsPerHost: 1, MaxIdleConnsPerHost: 1,
		IdleConnTimeout: 30 * time.Second, TLSHandshakeTimeout: peerReportTimeout,
	}
	if peer.FederationID != "" {
		if membership == nil {
			return nil, errors.New("peer membership store unavailable")
		}
		transport.DisableKeepAlives = true
		transport.TLSClientConfig.VerifyConnection = peerServerIdentityVerifier(peer, membership, id, serverCRLs)
	}
	client := &peerClient{
		id: id, endpoint: strings.TrimSuffix(grant.URL, "/") + "/peer/v1/report", token: grant.OutboundToken,
		origin: strings.TrimSuffix(grant.URL, "/"), routeInference: grant.RouteInference,
		routeManagement: grant.RouteManagement, routePaths: grant.RoutePaths, serverCRLs: serverCRLs,
		crlMaxAge:          peer.RevocationMaxAge(),
		inferenceTransport: transport.Clone(),
		client: &http.Client{Transport: transport, Timeout: peerReportTimeout, CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}},
	}
	client.inferenceTransport.MaxConnsPerHost = 8
	return client, nil
}

func peerServerIdentityVerifier(peer *config.PeerConfig, membership *federation.PairingStore, id string, serverCRLs []*x509.RevocationList) func(tls.ConnectionState) error {
	return func(connection tls.ConnectionState) error {
		if len(connection.VerifiedChains) == 0 || len(connection.PeerCertificates) == 0 {
			return errors.New("peer server identity unavailable")
		}
		if federation.VerifyCertificateRevocation(connection.VerifiedChains, serverCRLs, peer.RevocationMaxAge(), time.Now()) != nil {
			return errors.New("peer server certificate is revoked or CRL is stale")
		}
		serverCert := connection.PeerCertificates[0]
		serverNodeID, err := federation.PeerIdentityNodeID(serverCert, peer.FederationID)
		if err != nil || serverNodeID != id {
			return errors.New("peer server identity mismatch")
		}
		member, ok := membership.GetMember(id)
		if !ok || member.Fingerprint != federation.CertificateFingerprint(serverCert) {
			return errors.New("peer is not paired with this certificate")
		}
		return nil
	}
}

func (c *peerClient) fetch(ctx context.Context) (*nodecontract.Receipt, string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint, nil)
	if err != nil {
		return nil, "request_error"
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, "unavailable"
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			return nil, "unauthorized"
		}
		return nil, "http_error"
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, nodecontract.MaxReportBytes+1))
	if err != nil {
		return nil, "unavailable"
	}
	if len(data) > nodecontract.MaxReportBytes {
		return nil, "invalid_report"
	}
	receipt, err := nodecontract.DecodeReport(bytes.NewReader(data), c.id, time.Now())
	if err != nil {
		return nil, "invalid_report"
	}
	return &receipt, ""
}

func (c *peerCache) update(id string, receipt *nodecontract.Receipt, reason string, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.entries[id]
	entry.dynamic = false
	entry.lastAttempt, entry.lastError = now, reason
	if receipt != nil {
		entry.receipt = receipt
	} else if entry.receipt != nil && now.Sub(entry.receipt.ReceivedAt) > peerRetention {
		entry.receipt = nil
	}
	c.entries[id] = entry
}

func (c *peerCache) updateSource(id string, receipt *nodecontract.Receipt, reason string, now time.Time, dynamic, routeInference bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.entries[id]
	entry.dynamic = dynamic
	entry.routeInference = routeInference
	entry.lastAttempt, entry.lastError = now, reason
	if receipt != nil {
		entry.receipt = receipt
	} else if entry.receipt != nil && now.Sub(entry.receipt.ReceivedAt) > peerRetention {
		entry.receipt = nil
	}
	c.entries[id] = entry
}

func (c *peerCache) retainDynamic(authorized map[string]struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for nodeID, entry := range c.entries {
		if !entry.dynamic {
			continue
		}
		if _, ok := authorized[nodeID]; !ok {
			delete(c.entries, nodeID)
		}
	}
}

func (c *peerCache) markDynamicUnavailable(nodeID, reason string, routeInference bool, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.entries[nodeID]
	entry.dynamic = true
	entry.routeInference = routeInference
	entry.lastAttempt, entry.lastError = now, reason
	if entry.receipt != nil && now.Sub(entry.receipt.ReceivedAt) > peerRetention {
		entry.receipt = nil
	}
	c.entries[nodeID] = entry
}

func (c *peerCache) views(now time.Time, selected string) ([]peerView, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if selected != "" {
		if _, ok := c.entries[selected]; !ok {
			return nil, false
		}
	}
	views := make([]peerView, 0, len(c.entries))
	for id, entry := range c.entries {
		if selected != "" && selected != id {
			continue
		}
		views = append(views, peerCacheEntryView(id, entry, now, selected != ""))
	}
	sort.Slice(views, func(i, j int) bool { return views[i].NodeID < views[j].NodeID })
	return views, true
}

func peerCacheEntryView(id string, entry peerCacheEntry, now time.Time, includeReport bool) peerView {
	view := peerView{NodeID: id, Status: "unknown", LastError: entry.lastError}
	if !entry.lastAttempt.IsZero() {
		at := entry.lastAttempt
		view.LastAttempt = &at
		view.Status = "unavailable"
	}
	if entry.receipt == nil {
		return view
	}
	age := now.Sub(entry.receipt.ReceivedAt)
	if age < 0 || age > peerRetention {
		return view
	}
	at := entry.receipt.ReceivedAt
	view.ReceivedAt = &at
	view.Status = "stale"
	if entry.lastError == "" && age <= peerFreshAge {
		view.Status = "fresh"
	}
	if includeReport {
		view.Report = &entry.receipt.Report
	}
	return view
}

func (r *Reloader) refreshPeers(state *reloaderState) {
	r.refreshPeerClients(state, state.peerClients)
}

func (r *Reloader) refreshPeerClients(state *reloaderState, clients map[string]*peerClient) {
	jobs := make(chan *peerClient)
	var workers sync.WaitGroup
	for range peerRefreshWorkers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for client := range jobs {
				r.refreshPeerClient(state, client)
			}
		}()
	}
	for _, client := range clients {
		select {
		case <-state.peerContext.Done():
			close(jobs)
			workers.Wait()
			return
		case jobs <- client:
		}
	}
	close(jobs)
	workers.Wait()
}

func (r *Reloader) refreshPeerClient(state *reloaderState, client *peerClient) {
	if client.dynamic && !client.catalogRead {
		state.peerCache.markDynamicUnavailable(client.id, "catalog_read_not_granted", client.routeInference, time.Now())
		return
	}
	receipt, reason := client.fetch(state.peerContext)
	if state.peerContext.Err() != nil {
		return
	}
	state.peerCache.updateSource(client.id, receipt, reason, time.Now(), client.dynamic, client.routeInference)
	if reason != "" {
		r.logger.Warn("peer report refresh failed", "node_id", client.id, "reason", reason)
	}
}

func (r *Reloader) watchPeers() {
	defer r.wg.Done()
	for {
		state := r.state.Load()
		r.refreshPeerCatalogs(state)
		clients, dynamicClients := r.peerClientsForState(state)
		if len(clients) != 0 {
			r.refreshPeerClients(state, clients)
		}
		if len(dynamicClients) != 0 {
			closePeerClients(dynamicClients)
		}
		if !r.waitForPeerRefresh(state.cacheTTL) {
			return
		}
	}
}

func (r *Reloader) peerClientsForState(state *reloaderState) (map[string]*peerClient, map[string]*peerClient) {
	if !peerDiscoveryEnabled(state.peer) {
		return state.peerClients, nil
	}
	discovered, err := r.buildDiscoveredPeerClients(state)
	r.updateDiscoveredPeerState(state, discovered, err)
	return mergePeerClientSets(state.peerClients, discovered.clients), discovered.clients
}

func peerDiscoveryEnabled(peer *config.PeerConfig) bool {
	return peer != nil && peer.Discovery != nil && peer.Discovery.Enabled
}

func (r *Reloader) updateDiscoveredPeerState(state *reloaderState, discovered discoveredPeerClients, discoveryErr error) {
	state.peerCache.retainDynamic(discovered.authorizedIDs)
	now := time.Now()
	for nodeID := range discovered.authorizedIDs {
		if _, configured := state.peerClients[nodeID]; configured {
			continue
		}
		if _, found := discovered.clients[nodeID]; found {
			continue
		}
		reason := discovered.unavailable[nodeID]
		if reason == "" {
			reason = "not_discovered"
		}
		state.peerCache.markDynamicUnavailable(nodeID, reason, discovered.routeInference[nodeID], now)
	}
	if discoveryErr != nil && state.peerContext.Err() == nil {
		if r.lastPeerDiscoveryError != discoveryErr.Error() {
			r.logger.Warn("peer mDNS catalog discovery failed", "error", discoveryErr)
			r.lastPeerDiscoveryError = discoveryErr.Error()
		}
		return
	}
	if discoveryErr == nil {
		r.lastPeerDiscoveryError = ""
	}
}

func mergePeerClientSets(configured, discovered map[string]*peerClient) map[string]*peerClient {
	clients := make(map[string]*peerClient, len(configured)+len(discovered))
	for nodeID, client := range configured {
		clients[nodeID] = client
	}
	for nodeID, client := range discovered {
		if _, exists := clients[nodeID]; !exists {
			clients[nodeID] = client
		}
	}
	return clients
}

func (r *Reloader) waitForPeerRefresh(cacheTTL time.Duration) bool {
	timer := time.NewTimer(peerRefreshIntervalForTTL(cacheTTL))
	select {
	case <-r.stop:
		timer.Stop()
		return false
	case <-r.peerWake:
		if !timer.Stop() {
			<-timer.C
		}
	case <-timer.C:
	}
	return true
}

func peerRefreshIntervalForTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return peerRefreshInterval
	}
	interval := ttl / 3
	if interval < 250*time.Millisecond {
		interval = 250 * time.Millisecond
	}
	return min(interval, peerRefreshInterval)
}

func (r *Reloader) refreshPeerCatalogs(state *reloaderState) {
	if state.peer == nil || len(state.peer.LocalBackends) == 0 {
		return
	}
	handler, ok := state.handler.(*Handler)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(state.peerContext, peerReportTimeout)
	defer cancel()
	// Reports remain cache-only, but refresh owner caches before the configured
	// catalog TTL expires, independent of frontend catalog traffic.
	refreshAge := state.cacheTTL * 2 / 3
	if refreshAge <= 0 {
		return
	}
	handler.catalog.RefreshBackendModels(ctx, state.peer.LocalBackends, refreshAge)
}

func closePeerClients(clients map[string]*peerClient) {
	for _, client := range clients {
		client.client.CloseIdleConnections()
		client.inferenceTransport.CloseIdleConnections()
	}
}

func (h *Handler) serveDiagnosticsPeers(w http.ResponseWriter, r *http.Request) {
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil || len(query) > 1 {
		writeDiagnosticsError(w, http.StatusBadRequest, "invalid peer query")
		return
	}
	selected := ""
	if len(query) != 0 {
		values, exists := query["node_id"]
		if !exists || len(values) != 1 || !identity.ValidNodeID(values[0]) {
			writeDiagnosticsError(w, http.StatusBadRequest, "invalid peer query")
			return
		}
		selected = values[0]
	}
	ctx, cancel := context.WithTimeout(r.Context(), diagnosticRequestTimeout)
	defer cancel()
	release, err := h.diagnoseRateLimit(ctx)
	if err != nil {
		writeDiagnosticsError(w, http.StatusServiceUnavailable, "diagnostics temporarily unavailable")
		return
	}
	defer release()
	peers, exists := h.peerCache.views(time.Now(), selected)
	if !exists {
		writeDiagnosticsError(w, http.StatusNotFound, "unknown peer")
		return
	}
	writeDiagnosticsResponse(w, http.StatusOK, map[string]any{"coverage": "partial", "peers": peers})
}

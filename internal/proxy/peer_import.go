package proxy

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
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
	routeInference     bool
	routeManagement    bool
	inferenceTransport *http.Transport
}

type peerCache struct {
	mu      sync.RWMutex
	entries map[string]peerCacheEntry
}

type peerCacheEntry struct {
	receipt     *nodecontract.Receipt
	lastAttempt time.Time
	lastError   string
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
	clients := make(map[string]*peerClient)
	cache := &peerCache{entries: make(map[string]peerCacheEntry)}
	if peer == nil {
		return clients, cache, nil
	}
	for id, grant := range peer.Peers {
		if grant.URL == "" {
			continue
		}
		var roots *x509.CertPool
		if grant.CAFile != "" {
			file, err := os.Open(grant.CAFile)
			if err != nil {
				return nil, nil, fmt.Errorf("open peer CA for %q: %w", id, err)
			}
			data, readErr := io.ReadAll(io.LimitReader(file, maxPeerCABytes+1))
			closeErr := file.Close()
			if readErr != nil {
				return nil, nil, fmt.Errorf("read peer CA for %q: %w", id, readErr)
			}
			if closeErr != nil {
				return nil, nil, fmt.Errorf("close peer CA for %q: %w", id, closeErr)
			}
			roots = x509.NewCertPool()
			if len(data) > maxPeerCABytes || !roots.AppendCertsFromPEM(data) {
				return nil, nil, fmt.Errorf("invalid peer CA for %q", id)
			}
		}
		transport := &http.Transport{
			TLSClientConfig:    &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots},
			DisableCompression: true, MaxConnsPerHost: 1, MaxIdleConnsPerHost: 1,
			IdleConnTimeout: 30 * time.Second, TLSHandshakeTimeout: peerReportTimeout,
		}
		clients[id] = &peerClient{
			id: id, endpoint: strings.TrimSuffix(grant.URL, "/") + "/peer/v1/report", token: grant.OutboundToken,
			origin: strings.TrimSuffix(grant.URL, "/"), routeInference: grant.RouteInference,
			routeManagement:    grant.RouteManagement,
			inferenceTransport: transport.Clone(),
			client: &http.Client{Transport: transport, Timeout: peerReportTimeout, CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			}},
		}
		clients[id].inferenceTransport.MaxConnsPerHost = 8
		cache.entries[id] = peerCacheEntry{}
	}
	return clients, cache, nil
}

func (c *peerClient) fetch(ctx context.Context) (*nodecontract.Receipt, string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint, nil)
	if err != nil {
		return nil, "request_error"
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
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
	entry.lastAttempt, entry.lastError = now, reason
	if receipt != nil {
		entry.receipt = receipt
	} else if entry.receipt != nil && now.Sub(entry.receipt.ReceivedAt) > peerRetention {
		entry.receipt = nil
	}
	c.entries[id] = entry
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
		view := peerView{NodeID: id, Status: "unknown", LastError: entry.lastError}
		if !entry.lastAttempt.IsZero() {
			at := entry.lastAttempt
			view.LastAttempt = &at
			view.Status = "unavailable"
		}
		if entry.receipt != nil {
			age := now.Sub(entry.receipt.ReceivedAt)
			if age >= 0 && age <= peerRetention {
				at := entry.receipt.ReceivedAt
				view.ReceivedAt = &at
				view.Status = "stale"
				if entry.lastError == "" && age <= peerFreshAge {
					view.Status = "fresh"
				}
				if selected != "" {
					view.Report = &entry.receipt.Report
				}
			}
		}
		views = append(views, view)
	}
	sort.Slice(views, func(i, j int) bool { return views[i].NodeID < views[j].NodeID })
	return views, true
}

func (r *Reloader) refreshPeers(state *reloaderState) {
	jobs := make(chan *peerClient)
	var workers sync.WaitGroup
	for range peerRefreshWorkers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for client := range jobs {
				receipt, reason := client.fetch(state.peerContext)
				if state.peerContext.Err() != nil {
					continue
				}
				state.peerCache.update(client.id, receipt, reason, time.Now())
				if reason != "" {
					r.logger.Warn("peer report refresh failed", "node_id", client.id, "reason", reason)
				}
			}
		}()
	}
	for _, client := range state.peerClients {
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

func (r *Reloader) watchPeers() {
	defer r.wg.Done()
	ticker := time.NewTicker(peerRefreshInterval)
	defer ticker.Stop()
	for {
		state := r.state.Load()
		if len(state.peerClients) != 0 {
			r.refreshPeers(state)
		}
		select {
		case <-r.stop:
			return
		case <-r.peerWake:
		case <-ticker.C:
		}
	}
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

package proxy

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/nodecontract"
)

const peerReportTimeout = 5 * time.Second

type peerScope int

const (
	peerCatalog peerScope = iota
	peerDiagnostics
	peerInference
)

// PeerTLSConfig serves the currently installed peer certificate. Removing peer
// configuration disables new handshakes until it is configured again.
func (r *Reloader) PeerTLSConfig() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			certificate := r.state.Load().certificate
			if certificate == nil {
				return nil, errors.New("peer listener disabled")
			}
			return certificate, nil
		},
	}
}

// PeerHandler serves only explicitly authorized peer protocol operations.
func (r *Reloader) PeerHandler() http.Handler {
	return http.HandlerFunc(r.servePeerHTTP)
}

func (r *Reloader) servePeerHTTP(w http.ResponseWriter, req *http.Request) {
	if req.TLS == nil {
		http.Error(w, "TLS required", http.StatusForbidden)
		return
	}
	if req.URL.RawQuery != "" {
		http.NotFound(w, req)
		return
	}
	if req.URL.Path == "/peer/v1/inference" {
		r.servePeerInference(w, req)
		return
	}
	if req.URL.Path != "/peer/v1/report" {
		http.NotFound(w, req)
		return
	}
	if req.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if req.ContentLength != 0 || len(req.TransferEncoding) != 0 {
		req.Close = true
		w.Header().Set("Connection", "close")
		http.Error(w, "request body not allowed", http.StatusBadRequest)
		return
	}
	select {
	case r.peerSlots <- struct{}{}:
		defer func() { <-r.peerSlots }()
	default:
		http.Error(w, "peer service busy", http.StatusServiceUnavailable)
		return
	}
	state := r.state.Load()
	peer := state.peer
	if peer == nil || !peerAuthorized(req, state, peerCatalog) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	handler, ok := state.handler.(*Handler)
	if !ok {
		r.logger.Error("peer report unavailable: unexpected handler type")
		http.Error(w, "peer report unavailable", http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(req.Context(), peerReportTimeout)
	defer cancel()
	report, err := nodecontract.LocalReportFromCatalog(ctx, nodecontract.Source{
		NodeID: peer.NodeID, ProxyInstanceID: r.instanceID, ConfigGeneration: state.generation,
	}, peer.LocalBackends, handler.catalog, time.Now(), state.cacheTTL)
	if err != nil {
		r.logger.Warn("peer report unavailable", "error", err)
		http.Error(w, "peer report unavailable", http.StatusServiceUnavailable)
		return
	}
	if len(state.peerClients) != 0 {
		report.Capabilities["federation_exchange"] = "enabled"
	}
	// This advertises the feature, not the requesting peer's authorization.
	report.Capabilities["diagnostics_read"] = "enabled"
	if peerAuthorized(req, state, peerDiagnostics) {
		report, err = nodecontract.AddBackendReadiness(report, handler.catalog.CachedBackendReadiness(peer.LocalBackends), state.cacheTTL)
		if err != nil {
			r.logger.Warn("peer readiness report unavailable", "error", err)
			http.Error(w, "peer report unavailable", http.StatusServiceUnavailable)
			return
		}
	}
	data, err := json.Marshal(report)
	if err != nil {
		r.logger.Error("encode peer report failed", "error", err)
		http.Error(w, "peer report unavailable", http.StatusServiceUnavailable)
		return
	}
	if r.state.Load() != state {
		http.Error(w, "peer configuration changed", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	controller := http.NewResponseController(w)
	if err := controller.SetWriteDeadline(time.Now().Add(15 * time.Second)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		r.logger.Warn("peer report write deadline unavailable", "error", err)
	}
	defer func() { _ = controller.SetWriteDeadline(time.Time{}) }()
	_, _ = w.Write(data)
}

func peerAuthorized(req *http.Request, state *reloaderState, scope peerScope) bool {
	values := req.Header.Values("Authorization")
	if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
		return false
	}
	token := strings.TrimPrefix(values[0], "Bearer ")
	if len(token) < 32 || len(token) > 256 {
		return false
	}
	hash := sha256.Sum256([]byte(token))
	authorized := 0
	for _, peer := range state.peer.Peers {
		expected := sha256.Sum256([]byte(peer.Token))
		match := subtle.ConstantTimeCompare(hash[:], expected[:])
		if (scope == peerCatalog && peer.CatalogRead) ||
			(scope == peerDiagnostics && peer.DiagnosticsRead) ||
			(scope == peerInference && peer.Inference) {
			authorized |= match
		}
	}
	return authorized == 1
}

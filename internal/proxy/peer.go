package proxy

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/federation"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/nodecontract"
)

const peerReportTimeout = 5 * time.Second
const peerEnrollmentPath = "/peer/v1/enroll"
const peerInvitationHeader = "X-Llama-Catalog-Invite"

// peerReportUnavailable is the exact HTTP body and log message reported when a
// peer catalog report cannot be produced. It replaces the duplicated literal so
// the message is defined once and referenced everywhere.
const peerReportUnavailable = "peer report unavailable"

// peerEnrollmentUnavailable is the exact HTTP body reported when peer enrollment
// cannot be completed. It replaces the duplicated literal so the message is
// defined once and referenced everywhere.
const peerEnrollmentUnavailable = "peer enrollment unavailable"

type peerScope int

const (
	peerCatalog peerScope = iota
	peerDiagnostics
	peerInference
	peerManageSettings
	peerPathService
)

// PeerTLSConfig serves the currently installed peer certificate. Removing peer
// configuration disables new handshakes until it is configured again.
func (r *Reloader) PeerTLSConfig() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			state := r.state.Load()
			if state == nil || state.certificate == nil {
				return nil, errors.New("peer listener disabled")
			}
			config := &tls.Config{
				MinVersion:   tls.VersionTLS12,
				Certificates: []tls.Certificate{*state.certificate},
			}
			if state.peer != nil && state.peer.FederationID != "" {
				if state.peerClientCAs == nil {
					return nil, errors.New("peer client CA unavailable")
				}
				config.ClientAuth = tls.RequireAndVerifyClientCert
				config.ClientCAs = state.peerClientCAs
			}
			return config, nil
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
	if req.URL.Path == peerEnrollmentPath {
		r.servePeerEnrollment(w, req)
		return
	}
	if req.URL.Path == peerPathEndpoint {
		r.servePeerPath(w, req)
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
	switch req.URL.Path {
	case peerSettingsGrantsPath:
		r.servePeerDelegatedSettingsGrants(w, req)
		return
	case peerSettingsSnapshotPath:
		r.servePeerDelegatedSettingsSnapshot(w, req)
		return
	case peerSettingsPreviewPath:
		r.servePeerDelegatedSettingsPreview(w, req)
		return
	case peerSettingsProposalPath:
		r.servePeerDelegatedSettingsProposal(w, req)
		return
	case peerSettingsProposalRecoveryPath:
		r.servePeerDelegatedSettingsProposalRecovery(w, req)
		return
	case peerSettingsApplyPath:
		r.servePeerDelegatedSettingsApply(w, req)
		return
	}
	if strings.HasPrefix(req.URL.Path, peerOperationPath) {
		r.servePeerDelegatedOperation(w, req)
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
		http.Error(w, peerReportUnavailable, http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(req.Context(), peerReportTimeout)
	defer cancel()
	report, err := nodecontract.LocalReportFromCatalog(ctx, nodecontract.Source{
		NodeID: peer.NodeID, ProxyInstanceID: r.instanceID, ConfigGeneration: state.generation,
	}, peer.LocalBackends, handler.catalog, time.Now(), state.cacheTTL)
	if err != nil {
		r.logger.Warn(peerReportUnavailable, "error", err)
		http.Error(w, peerReportUnavailable, http.StatusServiceUnavailable)
		return
	}
	for service := range peer.ExposedPathRoutes {
		report.PathServices = append(report.PathServices, service)
	}
	slices.Sort(report.PathServices)
	if len(state.peerClients) != 0 {
		report.Capabilities["federation_exchange"] = "enabled"
	}
	// This advertises the feature, not the requesting peer's authorization.
	report.Capabilities["diagnostics_read"] = "enabled"
	if delegatedSettingsEnabled(state) {
		// The peer's manage_settings grant is still checked independently per request.
		report.Capabilities["config_write"] = "enabled"
	}
	if peerAuthorized(req, state, peerDiagnostics) {
		report, err = nodecontract.AddBackendReadiness(report, handler.catalog.CachedBackendReadiness(peer.LocalBackends), state.cacheTTL)
		if err != nil {
			r.logger.Warn("peer readiness report unavailable", "error", err)
			http.Error(w, peerReportUnavailable, http.StatusServiceUnavailable)
			return
		}
	}
	data, err := json.Marshal(report)
	if err != nil {
		r.logger.Error("encode peer report failed", "error", err)
		http.Error(w, peerReportUnavailable, http.StatusServiceUnavailable)
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

func (r *Reloader) servePeerEnrollment(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if req.URL.RawQuery != "" || req.URL.RawPath != "" || req.ContentLength != 0 || len(req.TransferEncoding) != 0 ||
		len(req.Header.Values("Authorization")) != 0 {
		req.Close = true
		w.Header().Set("Connection", "close")
		http.Error(w, "invalid enrollment request", http.StatusBadRequest)
		return
	}
	codes := req.Header.Values(peerInvitationHeader)
	if len(codes) != 1 || len(codes[0]) > 64 {
		http.Error(w, "invalid invitation", http.StatusUnauthorized)
		return
	}
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.stopped {
		http.Error(w, "peer service unavailable", http.StatusServiceUnavailable)
		return
	}
	state := r.state.Load()
	if state == nil || state.peer == nil || state.peer.FederationID == "" || state.pairingStore == nil || req.TLS == nil ||
		len(req.TLS.VerifiedChains) == 0 || len(req.TLS.PeerCertificates) == 0 {
		http.Error(w, peerEnrollmentUnavailable, http.StatusServiceUnavailable)
		return
	}
	clientCert := req.TLS.PeerCertificates[0]
	nodeID, err := peerIdentityNodeID(clientCert, state.peer.FederationID)
	if err != nil || verifyPeerClientIdentity(req.TLS.PeerCertificates, state.peerClientCAs, state.peerClientCRLs, state.peer.RevocationMaxAge(), state.peer.FederationID, nodeID) != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	auditTx, auditErr := beginPeerAudit(state, state.auditStore, nodeID, "pairing.accept", nodeID, "", "")
	if auditErr != nil {
		http.Error(w, peerEnrollmentUnavailable, http.StatusServiceUnavailable)
		return
	}
	member, err := state.pairingStore.ConsumeInvitation(codes[0], nodeID, peerCertificateFingerprint(clientCert), time.Now())
	if finishAudit(auditTx, auditOutcomeForError(err), nodeID, "", "") != nil {
		http.Error(w, "peer enrollment outcome unknown", http.StatusServiceUnavailable)
		return
	}
	if err != nil {
		if errors.Is(err, federation.ErrPairingStore) || errors.Is(err, federation.ErrInvitationFull) {
			http.Error(w, peerEnrollmentUnavailable, http.StatusServiceUnavailable)
			return
		}
		if errors.Is(err, federation.ErrMemberExists) {
			http.Error(w, "identity already paired", http.StatusConflict)
			return
		}
		http.Error(w, "invalid invitation", http.StatusUnauthorized)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(struct {
		State        string    `json:"state"`
		FederationID string    `json:"federation_id"`
		LocalNodeID  string    `json:"local_node_id"`
		NodeID       string    `json:"node_id"`
		PairedAt     time.Time `json:"paired_at"`
	}{"paired", state.peer.FederationID, state.peer.NodeID, member.NodeID, member.PairedAt})
}

func delegatedSettingsEnabled(state *reloaderState) bool {
	if state == nil || state.peer == nil || state.management == nil ||
		len(state.peer.ManagementTargets) == 0 || state.management.WriteToken == "" ||
		state.management.ProposalStore == nil || state.management.BackupStore == nil ||
		state.management.DelegatedStore == nil {
		return false
	}
	for backendID, targetID := range state.peer.ManagementTargets {
		if slices.Contains(state.peer.LocalBackends, backendID) {
			if _, ok := state.management.Targets[targetID]; ok {
				return true
			}
		}
	}
	return false
}

func peerAuthorized(req *http.Request, state *reloaderState, scope peerScope) bool {
	_, authorized := authenticatedPeer(req, state, scope)
	return authorized
}

func authenticatedPeer(req *http.Request, state *reloaderState, scope peerScope) (string, bool) {
	if state == nil || state.peer == nil {
		return "", false
	}
	values := req.Header.Values("Authorization")
	if len(values) == 0 {
		return authenticatedDynamicPeer(req, state, scope)
	}
	if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
		return "", false
	}
	token := strings.TrimPrefix(values[0], "Bearer ")
	if len(token) < 32 || len(token) > 256 {
		return "", false
	}
	hash := sha256.Sum256([]byte(token))
	var nodeID string
	matches := 0
	authorized := false
	for id, peer := range state.peer.Peers {
		expected := sha256.Sum256([]byte(peer.Token))
		match := subtle.ConstantTimeCompare(hash[:], expected[:])
		if match == 1 {
			matches++
			nodeID = id
			authorized = (scope == peerCatalog && peer.CatalogRead) ||
				(scope == peerDiagnostics && peer.DiagnosticsRead) ||
				(scope == peerInference && peer.Inference) ||
				(scope == peerManageSettings && peer.ManageSettings) ||
				(scope == peerPathService && len(peer.PathAccess) != 0)
		}
	}
	if matches != 1 || !authorized {
		return "", false
	}
	if state.peer.FederationID != "" {
		if req.TLS == nil || len(req.TLS.VerifiedChains) == 0 ||
			verifyPeerClientIdentity(req.TLS.PeerCertificates, state.peerClientCAs, state.peerClientCRLs, state.peer.RevocationMaxAge(), state.peer.FederationID, nodeID) != nil ||
			state.pairingStore == nil {
			return "", false
		}
		member, paired := state.pairingStore.GetMember(nodeID)
		if !paired || member.Fingerprint != peerCertificateFingerprint(req.TLS.PeerCertificates[0]) {
			return "", false
		}
	}
	return nodeID, true
}

func authenticatedDynamicPeer(req *http.Request, state *reloaderState, scope peerScope) (string, bool) {
	if (scope != peerCatalog && scope != peerInference) || state.peer.FederationID == "" || state.catalogGrantStore == nil ||
		state.pairingStore == nil || req.TLS == nil || len(req.TLS.VerifiedChains) == 0 ||
		len(req.TLS.PeerCertificates) == 0 {
		return "", false
	}
	certificate := req.TLS.PeerCertificates[0]
	nodeID, err := peerIdentityNodeID(certificate, state.peer.FederationID)
	if err != nil || verifyPeerClientIdentity(req.TLS.PeerCertificates, state.peerClientCAs,
		state.peerClientCRLs, state.peer.RevocationMaxAge(), state.peer.FederationID, nodeID) != nil {
		return "", false
	}
	member, paired, err := state.pairingStore.LookupMember(nodeID)
	if err != nil || !paired || member.Fingerprint != peerCertificateFingerprint(certificate) {
		return "", false
	}
	var allowed bool
	if scope == peerCatalog {
		allowed, err = state.catalogGrantStore.AllowsCatalogRead(nodeID, member.Fingerprint)
	} else {
		allowed, err = state.catalogGrantStore.AllowsIncomingInference(nodeID, member.Fingerprint)
	}
	if err != nil || !allowed {
		return "", false
	}
	return nodeID, true
}

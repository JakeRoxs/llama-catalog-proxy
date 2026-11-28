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

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
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
			return peerTLSClientConfig(r.state.Load())
		},
	}
}

// peerTLSClientConfig returns the TLS configuration for a peer handshake using
// the currently installed certificate. It preserves the exact TLS 1.2 minimum,
// certificate selection, and mutual TLS guard behavior of the inline closure.
func peerTLSClientConfig(state *reloaderState) (*tls.Config, error) {
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
	if r.dispatchPeerDirectRoute(w, req) {
		return
	}
	if req.URL.RawQuery != "" {
		http.NotFound(w, req)
		return
	}
	if r.dispatchPeerManagementRoute(w, req) {
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
	r.servePeerReport(w, req)
}

func (r *Reloader) dispatchPeerDirectRoute(w http.ResponseWriter, req *http.Request) bool {
	switch req.URL.Path {
	case peerEnrollmentPath:
		r.servePeerEnrollment(w, req)
		return true
	case peerPathEndpoint:
		r.servePeerPath(w, req)
		return true
	case "/peer/v1/inference":
		r.servePeerInference(w, req)
		return true
	default:
		return false
	}
}

func (r *Reloader) dispatchPeerManagementRoute(w http.ResponseWriter, req *http.Request) bool {
	switch req.URL.Path {
	case peerSettingsGrantsPath:
		r.servePeerDelegatedSettingsGrants(w, req)
	case peerSettingsSnapshotPath:
		r.servePeerDelegatedSettingsSnapshot(w, req)
	case peerSettingsPreviewPath:
		r.servePeerDelegatedSettingsPreview(w, req)
	case peerSettingsProposalPath:
		r.servePeerDelegatedSettingsProposal(w, req)
	case peerSettingsProposalRecoveryPath:
		r.servePeerDelegatedSettingsProposalRecovery(w, req)
	case peerSettingsApplyPath:
		r.servePeerDelegatedSettingsApply(w, req)
	default:
		return false
	}
	return true
}

func (r *Reloader) servePeerReport(w http.ResponseWriter, req *http.Request) {
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
	report, state, ok := r.buildPeerReport(req, w)
	if !ok {
		return
	}
	r.writePeerReport(w, report, state)
}

func (r *Reloader) buildPeerReport(req *http.Request, w http.ResponseWriter) (nodecontract.Report, *reloaderState, bool) {
	state := r.state.Load()
	peer := state.peer
	if peer == nil || !peerAuthorized(req, state, peerCatalog) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return nodecontract.Report{}, nil, false
	}
	handler, ok := state.handler.(*Handler)
	if !ok {
		r.logger.Error("peer report unavailable: unexpected handler type")
		http.Error(w, peerReportUnavailable, http.StatusServiceUnavailable)
		return nodecontract.Report{}, nil, false
	}
	ctx, cancel := context.WithTimeout(req.Context(), peerReportTimeout)
	defer cancel()
	report, err := nodecontract.LocalReportFromCatalog(ctx, nodecontract.Source{
		NodeID: peer.NodeID, ProxyInstanceID: r.instanceID, ConfigGeneration: state.generation,
	}, peer.LocalBackends, handler.catalog, time.Now(), state.cacheTTL)
	if err != nil {
		r.logger.Warn(peerReportUnavailable, "error", err)
		http.Error(w, peerReportUnavailable, http.StatusServiceUnavailable)
		return nodecontract.Report{}, nil, false
	}
	report = addPeerReportCapabilities(report, peer, state)
	if peerAuthorized(req, state, peerDiagnostics) {
		report, err = addPeerReportReadiness(report, handler, peer, state)
		if err != nil {
			r.logger.Warn("peer readiness report unavailable", "error", err)
			http.Error(w, peerReportUnavailable, http.StatusServiceUnavailable)
			return nodecontract.Report{}, nil, false
		}
	}
	return report, state, true
}

func addPeerReportCapabilities(report nodecontract.Report, peer *config.PeerConfig, state *reloaderState) nodecontract.Report {
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
	return report
}

func addPeerReportReadiness(report nodecontract.Report, handler *Handler, peer *config.PeerConfig, state *reloaderState) (nodecontract.Report, error) {
	return nodecontract.AddBackendReadiness(report, handler.catalog.CachedBackendReadiness(peer.LocalBackends), state.cacheTTL)
}

func (r *Reloader) writePeerReport(w http.ResponseWriter, report nodecontract.Report, state *reloaderState) {
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
	codes, ok := peerEnrollmentRequest(w, req)
	if !ok {
		return
	}
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.stopped {
		http.Error(w, "peer service unavailable", http.StatusServiceUnavailable)
		return
	}
	state := r.state.Load()
	if state == nil || state.peer == nil || state.peer.FederationID == "" || state.pairingStore == nil ||
		req.TLS == nil || len(req.TLS.VerifiedChains) == 0 || len(req.TLS.PeerCertificates) == 0 {
		http.Error(w, peerEnrollmentUnavailable, http.StatusServiceUnavailable)
		return
	}
	clientCert := req.TLS.PeerCertificates[0]
	nodeID, err := peerIdentityNodeID(clientCert, state.peer.FederationID)
	if err != nil || verifyPeerClientIdentity(req.TLS.PeerCertificates, state.peerClientCAs, state.peerClientCRLs, state.peer.RevocationMaxAge(), state.peer.FederationID, nodeID) != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	member, ok := r.consumePeerEnrollment(w, state, nodeID, peerCertificateFingerprint(clientCert), codes[0])
	if !ok {
		return
	}
	writePeerEnrollmentResponse(w, state, member)
}

func peerEnrollmentRequest(w http.ResponseWriter, req *http.Request) ([]string, bool) {
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return nil, false
	}
	if req.URL.RawQuery != "" || req.URL.RawPath != "" || req.ContentLength != 0 ||
		len(req.TransferEncoding) != 0 || len(req.Header.Values("Authorization")) != 0 {
		req.Close = true
		w.Header().Set("Connection", "close")
		http.Error(w, "invalid enrollment request", http.StatusBadRequest)
		return nil, false
	}
	codes := req.Header.Values(peerInvitationHeader)
	if len(codes) != 1 || len(codes[0]) > 64 {
		http.Error(w, "invalid invitation", http.StatusUnauthorized)
		return nil, false
	}
	return codes, true
}

func (r *Reloader) consumePeerEnrollment(w http.ResponseWriter, state *reloaderState, nodeID, fingerprint, code string) (federation.Membership, bool) {
	auditTx, auditErr := beginPeerAudit(state, state.auditStore, nodeID, "pairing.accept", nodeID, "", "")
	if auditErr != nil {
		http.Error(w, peerEnrollmentUnavailable, http.StatusServiceUnavailable)
		return federation.Membership{}, false
	}
	member, err := state.pairingStore.ConsumeInvitation(code, nodeID, fingerprint, time.Now())
	if finishAudit(auditTx, auditOutcomeForError(err), nodeID, "", "") != nil {
		http.Error(w, "peer enrollment outcome unknown", http.StatusServiceUnavailable)
		return federation.Membership{}, false
	}
	if err != nil {
		writePeerEnrollmentError(w, err)
		return federation.Membership{}, false
	}
	return member, true
}

func writePeerEnrollmentError(w http.ResponseWriter, err error) {
	if errors.Is(err, federation.ErrPairingStore) || errors.Is(err, federation.ErrInvitationFull) {
		http.Error(w, peerEnrollmentUnavailable, http.StatusServiceUnavailable)
		return
	}
	if errors.Is(err, federation.ErrMemberExists) {
		http.Error(w, "identity already paired", http.StatusConflict)
		return
	}
	http.Error(w, "invalid invitation", http.StatusUnauthorized)
}

func writePeerEnrollmentResponse(w http.ResponseWriter, state *reloaderState, member federation.Membership) {
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
	return hasConfiguredDelegatedManagementTarget(state.peer, state.management)
}

// hasConfiguredDelegatedManagementTarget reports whether a local backend is
// bound to a configured management target, which is the condition under which
// delegated settings management is advertised.
func hasConfiguredDelegatedManagementTarget(peer *config.PeerConfig, management *config.ManagementConfig) bool {
	for backendID, targetID := range peer.ManagementTargets {
		if slices.Contains(peer.LocalBackends, backendID) {
			if _, ok := management.Targets[targetID]; ok {
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
	token, ok := peerBearerToken(values)
	if !ok {
		return "", false
	}
	nodeID, ok := peerTokenIdentity(state.peer, token, scope)
	if !ok {
		return "", false
	}
	if state.peer.FederationID != "" && !verifiedFederatedPeer(req, state, nodeID) {
		return "", false
	}
	return nodeID, true
}

func peerBearerToken(values []string) (string, bool) {
	if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
		return "", false
	}
	token := strings.TrimPrefix(values[0], "Bearer ")
	return token, len(token) >= 32 && len(token) <= 256
}

func peerTokenIdentity(peerConfig *config.PeerConfig, token string, scope peerScope) (string, bool) {
	hash := sha256.Sum256([]byte(token))
	var nodeID string
	matches := 0
	authorized := false
	for id, peer := range peerConfig.Peers {
		expected := sha256.Sum256([]byte(peer.Token))
		if subtle.ConstantTimeCompare(hash[:], expected[:]) == 1 {
			matches++
			nodeID = id
			authorized = peerHasScope(peer, scope)
		}
	}
	return nodeID, matches == 1 && authorized
}

func peerHasScope(peer config.PeerGrant, scope peerScope) bool {
	switch scope {
	case peerCatalog:
		return peer.CatalogRead
	case peerDiagnostics:
		return peer.DiagnosticsRead
	case peerInference:
		return peer.Inference
	case peerManageSettings:
		return peer.ManageSettings
	case peerPathService:
		return len(peer.PathAccess) != 0
	default:
		return false
	}
}

func verifiedFederatedPeer(req *http.Request, state *reloaderState, nodeID string) bool {
	if req.TLS == nil || len(req.TLS.VerifiedChains) == 0 ||
		verifyPeerClientIdentity(req.TLS.PeerCertificates, state.peerClientCAs, state.peerClientCRLs,
			state.peer.RevocationMaxAge(), state.peer.FederationID, nodeID) != nil ||
		state.pairingStore == nil {
		return false
	}
	member, paired := state.pairingStore.GetMember(nodeID)
	return paired && member.Fingerprint == peerCertificateFingerprint(req.TLS.PeerCertificates[0])
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

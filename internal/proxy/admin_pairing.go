package proxy

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/federation"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/identity"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
)

const adminPairingPath = "/admin/api/v1/federation"

const (
	errAuditStoreUnavailable          = "audit store unavailable"
	errCatalogGrantStoreUnavailable   = "catalog grant store unavailable"
	errInferenceGrantStoreUnavailable = "inference grant store unavailable"
	errInvalidNodeID                  = "invalid node ID"
	errInvalidOwnerGrantRequest       = "invalid owner grant request"
	errOwnerGrantStoreUnavailable     = "owner grant store unavailable"
	errOwnerGrantUnavailable          = "owner grant unavailable"
	errPairingUnavailable             = "pairing unavailable"
	contentTypeHeaderKey              = "Content-Type"
	contentTypeJSON                   = "application/json"
)

func (r *Reloader) serveAdminPairing(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if req.URL.RawQuery != "" || req.URL.RawPath != "" {
		http.Error(w, "invalid pairing request", http.StatusBadRequest)
		return
	}
	subject, ok := r.adminAuth.RequireRole(req, "administer_federation")
	if !ok {
		r.auditDeniedAdminRole(req)
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	req = withAdminAuditPrincipal(req, subject, "administer_federation")
	if req.Method != http.MethodGet && !r.adminAuth.RequireCSRF(req) {
		r.auditDeniedAdminRole(req)
		http.Error(w, "invalid CSRF token or origin", http.StatusForbidden)
		return
	}
	if r.dispatchPairingRoutes(w, req) || r.dispatchGrantRoutes(w, req) || r.dispatchOwnerGrantRoutes(w, req) {
		return
	}
	w.Header().Set("Allow", "GET, POST, DELETE")
	http.NotFound(w, req)
}

// dispatchPairingRoutes handles pairing, invitation, and member routes. It returns
// true when a route was handled; the caller is responsible for the 404 default.
func (r *Reloader) dispatchPairingRoutes(w http.ResponseWriter, req *http.Request) bool {
	switch {
	case req.URL.Path == adminPairingPath+"/members" && req.Method == http.MethodGet:
		r.listAdminPairingMembers(w, req)
		return true
	case req.URL.Path == adminPairingPath+"/invitations" && req.Method == http.MethodGet:
		r.listAdminPairingInvitations(w, req)
		return true
	case req.URL.Path == adminPairingPath+"/invitations" && req.Method == http.MethodPost:
		r.createAdminPairingInvitation(w, req)
		return true
	case req.URL.Path == adminPairingPath+"/join" && req.Method == http.MethodPost:
		r.joinAdminPairing(w, req)
		return true
	case strings.HasPrefix(req.URL.Path, adminPairingPath+"/members/") && req.Method == http.MethodDelete:
		r.removeAdminPairingMember(w, req, strings.TrimPrefix(req.URL.Path, adminPairingPath+"/members/"))
		return true
	case strings.HasPrefix(req.URL.Path, adminPairingPath+"/invitations/") && req.Method == http.MethodDelete:
		r.removeAdminPairingInvitation(w, req, strings.TrimPrefix(req.URL.Path, adminPairingPath+"/invitations/"))
		return true
	default:
		return false
	}
}

// dispatchGrantRoutes handles catalog-read and inference grant routes. It returns
// true when a route was handled; the caller is responsible for the 404 default.
func (r *Reloader) dispatchGrantRoutes(w http.ResponseWriter, req *http.Request) bool {
	switch {
	case req.URL.Path == adminPairingPath+"/catalog-grants" && req.Method == http.MethodGet:
		r.listAdminCatalogGrants(w, req)
		return true
	case req.URL.Path == adminPairingPath+"/catalog-grants" && req.Method == http.MethodPost:
		r.createAdminCatalogGrant(w, req)
		return true
	case strings.HasPrefix(req.URL.Path, adminPairingPath+"/catalog-grants/") && req.Method == http.MethodDelete:
		r.revokeAdminCatalogGrant(w, req, strings.TrimPrefix(req.URL.Path, adminPairingPath+"/catalog-grants/"))
		return true
	case req.URL.Path == adminPairingPath+"/inference-grants" && req.Method == http.MethodGet:
		r.listAdminInferenceGrants(w, req)
		return true
	case req.URL.Path == adminPairingPath+"/inference-grants" && req.Method == http.MethodPost:
		r.createAdminInferenceGrant(w, req)
		return true
	case strings.HasPrefix(req.URL.Path, adminPairingPath+"/inference-grants/") && req.Method == http.MethodDelete:
		r.revokeAdminInferenceGrant(w, req, strings.TrimPrefix(req.URL.Path, adminPairingPath+"/inference-grants/"))
		return true
	default:
		return false
	}
}

// dispatchOwnerGrantRoutes handles owner grant routes. It returns true when a route
// was handled; the caller is responsible for the 404 default.
func (r *Reloader) dispatchOwnerGrantRoutes(w http.ResponseWriter, req *http.Request) bool {
	switch {
	case req.URL.Path == adminPairingPath+"/grants" && req.Method == http.MethodGet:
		r.listAdminOwnerGrants(w, req)
		return true
	case req.URL.Path == adminPairingPath+"/grants" && req.Method == http.MethodPost:
		r.createAdminOwnerGrant(w, req)
		return true
	case strings.HasPrefix(req.URL.Path, adminPairingPath+"/grants/") && req.Method == http.MethodDelete:
		r.revokeAdminOwnerGrant(w, req, strings.TrimPrefix(req.URL.Path, adminPairingPath+"/grants/"))
		return true
	default:
		return false
	}
}

type adminCatalogGrantView struct {
	federation.CatalogReadGrant
	CurrentFingerprint bool `json:"current_fingerprint"`
}

type adminInferenceGrantView struct {
	federation.InferenceGrant
	CurrentFingerprint bool `json:"current_fingerprint"`
}

func (r *Reloader) listAdminCatalogGrants(w http.ResponseWriter, _ *http.Request) {
	state, status := r.adminCatalogGrantStores()
	if status != 0 {
		http.Error(w, errCatalogGrantStoreUnavailable, status)
		return
	}
	grants, err := state.catalogGrantStore.ListCatalogReadGrants()
	if err != nil {
		http.Error(w, errCatalogGrantStoreUnavailable, http.StatusServiceUnavailable)
		return
	}
	result := make([]adminCatalogGrantView, 0, len(grants))
	for _, grant := range grants {
		member, paired, err := state.pairingStore.LookupMember(grant.NodeID)
		if err != nil {
			http.Error(w, errPairingUnavailable, http.StatusServiceUnavailable)
			return
		}
		result = append(result, adminCatalogGrantView{
			CatalogReadGrant: grant, CurrentFingerprint: paired && member.Fingerprint == grant.Fingerprint,
		})
	}
	writeJSON(r.logger, w, http.StatusOK, map[string]any{"state": "available", "grants": result})
}

func (r *Reloader) createAdminCatalogGrant(w http.ResponseWriter, req *http.Request) {
	var input struct {
		NodeID string `json:"node_id"`
	}
	if err := decodeAdminPairingJSON(w, req, &input); err != nil || input.NodeID == "" {
		http.Error(w, "invalid catalog grant request", http.StatusBadRequest)
		return
	}
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.stopped {
		http.Error(w, errCatalogGrantStoreUnavailable, http.StatusServiceUnavailable)
		return
	}
	state, status := r.adminCatalogGrantStores()
	if status != 0 {
		http.Error(w, errCatalogGrantStoreUnavailable, status)
		return
	}
	member, paired, err := state.pairingStore.LookupMember(input.NodeID)
	if err != nil {
		http.Error(w, errPairingUnavailable, http.StatusServiceUnavailable)
		return
	}
	if !paired {
		http.Error(w, "catalog read can only be granted to a paired identity", http.StatusConflict)
		return
	}
	transaction, err := r.beginAdminAudit(req, "catalog_read_grant.create", input.NodeID, "", "")
	if err != nil {
		http.Error(w, errAuditStoreUnavailable, http.StatusServiceUnavailable)
		return
	}
	grant, err := state.catalogGrantStore.GrantCatalogRead(member, time.Now())
	if !r.finishAdminMutation(w, transaction, err, input.NodeID, "", "") {
		return
	}
	if err != nil {
		http.Error(w, errCatalogGrantStoreUnavailable, http.StatusServiceUnavailable)
		return
	}
	r.wakePeerRefresh()
	w.Header().Set(contentTypeHeaderKey, contentTypeJSON)
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{"grant": grant})
}

func (r *Reloader) revokeAdminCatalogGrant(w http.ResponseWriter, req *http.Request, nodeID string) {
	if strings.Contains(nodeID, "/") || !identity.ValidNodeID(nodeID) {
		http.Error(w, errInvalidNodeID, http.StatusBadRequest)
		return
	}
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.stopped {
		http.Error(w, errCatalogGrantStoreUnavailable, http.StatusServiceUnavailable)
		return
	}
	state, status := r.adminCatalogGrantStores()
	if status != 0 {
		http.Error(w, errCatalogGrantStoreUnavailable, status)
		return
	}
	transaction, err := r.beginAdminAudit(req, "catalog_read_grant.revoke", nodeID, "", "")
	if err != nil {
		http.Error(w, errAuditStoreUnavailable, http.StatusServiceUnavailable)
		return
	}
	err = state.catalogGrantStore.RevokeCatalogRead(nodeID)
	if !r.finishAdminMutation(w, transaction, err, nodeID, "", "") {
		return
	}
	if err != nil {
		if errors.Is(err, federation.ErrCatalogGrantMissing) {
			http.NotFound(w, req)
		} else {
			http.Error(w, errCatalogGrantStoreUnavailable, http.StatusServiceUnavailable)
		}
		return
	}
	r.wakePeerRefresh()
	w.WriteHeader(http.StatusNoContent)
}

func (r *Reloader) adminCatalogGrantStores() (*reloaderState, int) {
	state := r.state.Load()
	if state == nil || state.peer == nil || state.peer.FederationID == "" || state.pairingStore == nil || state.catalogGrantStore == nil {
		return nil, http.StatusServiceUnavailable
	}
	return state, 0
}

func (r *Reloader) listAdminInferenceGrants(w http.ResponseWriter, _ *http.Request) {
	state, status := r.adminCatalogGrantStores()
	if status != 0 {
		http.Error(w, errInferenceGrantStoreUnavailable, status)
		return
	}
	grants, err := state.catalogGrantStore.ListInferenceGrants()
	if err != nil {
		http.Error(w, errInferenceGrantStoreUnavailable, http.StatusServiceUnavailable)
		return
	}
	result := make([]adminInferenceGrantView, 0, len(grants))
	for _, grant := range grants {
		member, paired, err := state.pairingStore.LookupMember(grant.NodeID)
		if err != nil {
			http.Error(w, errPairingUnavailable, http.StatusServiceUnavailable)
			return
		}
		result = append(result, adminInferenceGrantView{
			InferenceGrant: grant, CurrentFingerprint: paired && member.Fingerprint == grant.Fingerprint,
		})
	}
	writeJSON(r.logger, w, http.StatusOK, map[string]any{"state": "available", "grants": result})
}

func (r *Reloader) createAdminInferenceGrant(w http.ResponseWriter, req *http.Request) {
	var input struct {
		NodeID        string `json:"node_id"`
		AllowIncoming bool   `json:"allow_incoming_inference"`
		RouteOutgoing bool   `json:"route_outgoing_inference"`
	}
	if err := decodeAdminPairingJSON(w, req, &input); err != nil || input.NodeID == "" || (!input.AllowIncoming && !input.RouteOutgoing) {
		http.Error(w, "invalid inference grant request", http.StatusBadRequest)
		return
	}
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.stopped {
		http.Error(w, errInferenceGrantStoreUnavailable, http.StatusServiceUnavailable)
		return
	}
	state, status := r.adminCatalogGrantStores()
	if status != 0 {
		http.Error(w, errInferenceGrantStoreUnavailable, status)
		return
	}
	member, paired, err := state.pairingStore.LookupMember(input.NodeID)
	if err != nil {
		http.Error(w, errPairingUnavailable, http.StatusServiceUnavailable)
		return
	}
	if !paired {
		http.Error(w, "inference access can only be granted to a paired identity", http.StatusConflict)
		return
	}
	transaction, err := r.beginAdminAudit(req, "inference_grant.create", input.NodeID, "", "")
	if err != nil {
		http.Error(w, errAuditStoreUnavailable, http.StatusServiceUnavailable)
		return
	}
	grant, err := state.catalogGrantStore.GrantInference(member, input.AllowIncoming, input.RouteOutgoing, time.Now())
	if !r.finishAdminMutation(w, transaction, err, input.NodeID, "", "") {
		return
	}
	if err != nil {
		if errors.Is(err, federation.ErrInferenceNeedsCatalog) {
			http.Error(w, "route_inference requires a separate current catalog_read grant", http.StatusConflict)
		} else {
			http.Error(w, errInferenceGrantStoreUnavailable, http.StatusServiceUnavailable)
		}
		return
	}
	r.wakePeerRefresh()
	w.Header().Set(contentTypeHeaderKey, contentTypeJSON)
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{"grant": grant})
}

func (r *Reloader) revokeAdminInferenceGrant(w http.ResponseWriter, req *http.Request, nodeID string) {
	if strings.Contains(nodeID, "/") || !identity.ValidNodeID(nodeID) {
		http.Error(w, errInvalidNodeID, http.StatusBadRequest)
		return
	}
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.stopped {
		http.Error(w, errInferenceGrantStoreUnavailable, http.StatusServiceUnavailable)
		return
	}
	state, status := r.adminCatalogGrantStores()
	if status != 0 {
		http.Error(w, errInferenceGrantStoreUnavailable, status)
		return
	}
	transaction, err := r.beginAdminAudit(req, "inference_grant.revoke", nodeID, "", "")
	if err != nil {
		http.Error(w, errAuditStoreUnavailable, http.StatusServiceUnavailable)
		return
	}
	err = state.catalogGrantStore.RevokeInference(nodeID)
	if !r.finishAdminMutation(w, transaction, err, nodeID, "", "") {
		return
	}
	if err != nil {
		if errors.Is(err, federation.ErrInferenceGrantMissing) {
			http.NotFound(w, req)
		} else {
			http.Error(w, errInferenceGrantStoreUnavailable, http.StatusServiceUnavailable)
		}
		return
	}
	r.wakePeerRefresh()
	w.WriteHeader(http.StatusNoContent)
}

func (r *Reloader) wakePeerRefresh() {
	select {
	case r.peerWake <- struct{}{}:
	default:
	}
}

func (r *Reloader) listAdminPairingMembers(w http.ResponseWriter, _ *http.Request) {
	store, status := r.adminPairingStore()
	if status != 0 {
		http.Error(w, errPairingUnavailable, status)
		return
	}
	members, err := store.ListMembers()
	if err != nil {
		http.Error(w, errPairingUnavailable, http.StatusServiceUnavailable)
		return
	}
	writeJSON(r.logger, w, http.StatusOK, map[string]any{"state": "available", "members": members})
}

func (r *Reloader) listAdminPairingInvitations(w http.ResponseWriter, _ *http.Request) {
	store, status := r.adminPairingStore()
	if status != 0 {
		http.Error(w, errPairingUnavailable, status)
		return
	}
	invitations, err := store.ListInvitations(time.Now())
	if err != nil {
		http.Error(w, errPairingUnavailable, http.StatusServiceUnavailable)
		return
	}
	writeJSON(r.logger, w, http.StatusOK, map[string]any{"state": "available", "invitations": invitations})
}

func (r *Reloader) createAdminPairingInvitation(w http.ResponseWriter, req *http.Request) {
	var input struct {
		NodeID string `json:"node_id"`
	}
	if err := decodeAdminPairingJSON(w, req, &input); err != nil || input.NodeID == "" {
		http.Error(w, "invalid invitation request", http.StatusBadRequest)
		return
	}
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.stopped {
		http.Error(w, errPairingUnavailable, http.StatusServiceUnavailable)
		return
	}
	store, status := r.adminPairingStore()
	if status != 0 {
		http.Error(w, errPairingUnavailable, status)
		return
	}
	state := r.state.Load()
	transaction, err := r.beginAdminAudit(req, "pairing.invitation.create", input.NodeID, "", "")
	if err != nil {
		http.Error(w, errAuditStoreUnavailable, http.StatusServiceUnavailable)
		return
	}
	invite, err := store.CreateInvitation(input.NodeID, time.Now())
	if !r.finishAdminMutation(w, transaction, err, input.NodeID, "", "") {
		return
	}
	if err != nil {
		switch {
		case errors.Is(err, federation.ErrInvitationFull):
			http.Error(w, "invitation limit reached", http.StatusTooManyRequests)
		case errors.Is(err, federation.ErrMemberExists):
			http.Error(w, "identity already paired", http.StatusConflict)
		default:
			http.Error(w, "invalid invitation request", http.StatusBadRequest)
		}
		return
	}
	w.Header().Set(contentTypeHeaderKey, contentTypeJSON)
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"code": invite.Code, "node_id": invite.NodeID, "federation_id": state.peer.FederationID,
		"expires_at": invite.ExpiresAt,
	})
}

func (r *Reloader) joinAdminPairing(w http.ResponseWriter, req *http.Request) {
	var input struct {
		URL  string `json:"url"`
		Code string `json:"invitation_code"`
	}
	if err := decodeAdminPairingJSON(w, req, &input); err != nil || input.URL == "" || input.Code == "" {
		http.Error(w, "invalid join request", http.StatusBadRequest)
		return
	}
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.stopped {
		http.Error(w, errPairingUnavailable, http.StatusServiceUnavailable)
		return
	}
	state := r.state.Load()
	if state == nil || state.peer == nil || state.peer.FederationID == "" || state.pairingStore == nil ||
		state.certificate == nil || state.peerClientCAs == nil || len(state.peerClientCRLs) == 0 {
		http.Error(w, errPairingUnavailable, http.StatusServiceUnavailable)
		return
	}
	transaction, err := r.beginAdminAudit(req, "pairing.join", "", "", "")
	if err != nil {
		http.Error(w, errAuditStoreUnavailable, http.StatusServiceUnavailable)
		return
	}
	member, err := state.pairingStore.Join(req.Context(), input.URL, input.Code, *state.certificate, state.peerClientCAs,
		state.peerClientCRLs, state.peer.RevocationMaxAge())
	if !r.finishAdminMutation(w, transaction, err, member.NodeID, "", "") {
		return
	}
	if err != nil {
		if errors.Is(err, federation.ErrPairingStore) || errors.Is(err, federation.ErrInvitationFull) {
			http.Error(w, errPairingUnavailable, http.StatusServiceUnavailable)
			return
		}
		http.Error(w, "pairing failed", http.StatusBadGateway)
		return
	}
	writeJSON(r.logger, w, http.StatusOK, map[string]any{"state": "paired", "member": member})
}

func (r *Reloader) removeAdminPairingMember(w http.ResponseWriter, req *http.Request, nodeID string) {
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.stopped {
		http.Error(w, errPairingUnavailable, http.StatusServiceUnavailable)
		return
	}
	store, status := r.adminPairingStore()
	if status != 0 {
		http.Error(w, errPairingUnavailable, status)
		return
	}
	if strings.Contains(nodeID, "/") || !identity.ValidNodeID(nodeID) {
		http.Error(w, errInvalidNodeID, http.StatusBadRequest)
		return
	}
	transaction, err := r.beginAdminAudit(req, "pairing.member.revoke", nodeID, "", "")
	if err != nil {
		http.Error(w, errAuditStoreUnavailable, http.StatusServiceUnavailable)
		return
	}
	err = store.RemoveMember(nodeID, time.Now())
	if !r.finishAdminMutation(w, transaction, err, nodeID, "", "") {
		return
	}
	if err != nil {
		if errors.Is(err, federation.ErrPairingStore) {
			http.Error(w, errPairingUnavailable, http.StatusServiceUnavailable)
		} else {
			http.NotFound(w, nil)
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (r *Reloader) removeAdminPairingInvitation(w http.ResponseWriter, req *http.Request, nodeID string) {
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.stopped {
		http.Error(w, errPairingUnavailable, http.StatusServiceUnavailable)
		return
	}
	store, status := r.adminPairingStore()
	if status != 0 {
		http.Error(w, errPairingUnavailable, status)
		return
	}
	if strings.Contains(nodeID, "/") || !identity.ValidNodeID(nodeID) {
		http.Error(w, errInvalidNodeID, http.StatusBadRequest)
		return
	}
	transaction, err := r.beginAdminAudit(req, "pairing.invitation.revoke", nodeID, "", "")
	if err != nil {
		http.Error(w, errAuditStoreUnavailable, http.StatusServiceUnavailable)
		return
	}
	err = store.RemoveInvitation(nodeID)
	if !r.finishAdminMutation(w, transaction, err, nodeID, "", "") {
		return
	}
	if err != nil {
		if errors.Is(err, federation.ErrPairingStore) {
			http.Error(w, errPairingUnavailable, http.StatusServiceUnavailable)
		} else {
			http.NotFound(w, nil)
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (r *Reloader) listAdminOwnerGrants(w http.ResponseWriter, _ *http.Request) {
	_, directory, status := r.adminOwnerGrantStore()
	if status != 0 {
		http.Error(w, errOwnerGrantStoreUnavailable, status)
		return
	}
	grants, err := modelconfig.ListOwnerGrants(directory)
	if err != nil {
		http.Error(w, errOwnerGrantStoreUnavailable, http.StatusServiceUnavailable)
		return
	}
	writeJSON(r.logger, w, http.StatusOK, map[string]any{"state": "available", "grants": grants})
}

type ownerGrantCreateRequest struct {
	RequesterNodeID string   `json:"requester_node_id"`
	BackendID       string   `json:"backend_id"`
	ModelIDs        []string `json:"model_ids"`
	ExpiresIn       string   `json:"expires_in"`
	ConfirmGrant    bool     `json:"confirm_grant"`
}

// validateOwnerGrantRequest decodes the request body and validates the confirmation,
// node IDs, model count, and expiry lifetime before the lifecycle lock is taken.
// It writes the error response and returns false when invalid; otherwise returns
// the parsed request, the validated lifetime, and true.
func validateOwnerGrantRequest(w http.ResponseWriter, req *http.Request) (*ownerGrantCreateRequest, time.Duration, bool) {
	var input ownerGrantCreateRequest
	if err := decodeAdminPairingJSON(w, req, &input); err != nil {
		http.Error(w, errInvalidOwnerGrantRequest, http.StatusBadRequest)
		return nil, 0, false
	}
	if !input.ConfirmGrant || !identity.ValidNodeID(input.RequesterNodeID) || !identity.ValidNodeID(input.BackendID) || len(input.ModelIDs) == 0 ||
		len(input.ModelIDs) > modelconfig.MaxOwnerGrantModels {
		http.Error(w, errInvalidOwnerGrantRequest, http.StatusBadRequest)
		return nil, 0, false
	}
	lifetime, err := time.ParseDuration(strings.TrimSpace(input.ExpiresIn))
	if err != nil || lifetime < 5*time.Minute || lifetime > modelconfig.MaxOwnerGrantLifetime {
		http.Error(w, "grant expiry must be between five minutes and 90 days", http.StatusBadRequest)
		return nil, 0, false
	}
	return &input, lifetime, true
}

// authorizeOwnerGrantTarget checks the stopped state, grant store, local backend
// ownership, manage_settings ceiling, paired identity, and management target while
// the lifecycle lock is held. It writes the error response and returns false when
// invalid; otherwise returns the target ID, grant directory, target directory,
// target filename, and true.
func (r *Reloader) authorizeOwnerGrantTarget(w http.ResponseWriter, req *ownerGrantCreateRequest) (string, string, string, string, bool) {
	if r.stopped {
		http.Error(w, errOwnerGrantUnavailable, http.StatusServiceUnavailable)
		return "", "", "", "", false
	}
	state, dir, status := r.adminOwnerGrantStore()
	if status != 0 {
		http.Error(w, errOwnerGrantStoreUnavailable, status)
		return "", "", "", "", false
	}
	if state.peer == nil || !slices.Contains(state.peer.LocalBackends, req.BackendID) {
		http.Error(w, "backend is not locally owned", http.StatusBadRequest)
		return "", "", "", "", false
	}
	peerGrant, ok := state.peer.Peers[req.RequesterNodeID]
	if !ok || !peerGrant.ManageSettings {
		http.Error(w, "requester has no owner-approved manage_settings ceiling", http.StatusForbidden)
		return "", "", "", "", false
	}
	member, paired, err := state.pairingStore.LookupMember(req.RequesterNodeID)
	if err != nil || !paired || member.NodeID != req.RequesterNodeID {
		http.Error(w, "requester is not a paired identity", http.StatusForbidden)
		return "", "", "", "", false
	}
	targetID, ok := state.peer.ManagementTargets[req.BackendID]
	if !ok || state.management == nil {
		http.Error(w, "backend has no local management target", http.StatusBadRequest)
		return "", "", "", "", false
	}
	target, ok := state.management.Targets[targetID]
	if !ok {
		http.Error(w, "management target unavailable", http.StatusBadRequest)
		return "", "", "", "", false
	}
	return targetID, dir, target.Directory, target.Filename, true
}

// validateOwnerGrantModels validates each model ID against the target while the
// lifecycle lock is held. It writes the error response and returns false when
// invalid; otherwise returns true.
func validateOwnerGrantModels(w http.ResponseWriter, targetDir, targetFile, targetID string, modelIDs []string) bool {
	modelsSeen := make(map[string]struct{}, len(modelIDs))
	for _, modelID := range modelIDs {
		if !identity.ValidModelComponent(modelID) || strings.TrimSpace(modelID) == "" {
			http.Error(w, "invalid model ID", http.StatusBadRequest)
			return false
		}
		if _, duplicate := modelsSeen[modelID]; duplicate {
			http.Error(w, "duplicate model ID", http.StatusBadRequest)
			return false
		}
		modelsSeen[modelID] = struct{}{}
		if _, err := modelconfig.ReadSettingsTarget(targetDir, targetFile, targetID, modelID); err != nil {
			http.Error(w, "model is not safely manageable on this target", http.StatusBadRequest)
			return false
		}
	}
	return true
}

func (r *Reloader) createAdminOwnerGrant(w http.ResponseWriter, req *http.Request) {
	input, lifetime, ok := validateOwnerGrantRequest(w, req)
	if !ok {
		return
	}
	grantID, err := randomOwnerGrantID()
	if err != nil {
		http.Error(w, errOwnerGrantUnavailable, http.StatusServiceUnavailable)
		return
	}
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	targetID, directory, targetDir, targetFile, ok := r.authorizeOwnerGrantTarget(w, input)
	if !ok {
		return
	}
	if !validateOwnerGrantModels(w, targetDir, targetFile, targetID, input.ModelIDs) {
		return
	}
	created := time.Now().UTC()
	grant := modelconfig.OwnerGrant{
		ID: grantID, RequesterNodeID: input.RequesterNodeID, Action: "manage_settings",
		BackendID: input.BackendID, TargetID: targetID, ModelIDs: input.ModelIDs,
		CreatedAt: created, ExpiresAt: created.Add(lifetime), ApprovedBy: "oidc_admin",
	}
	modelID := ""
	if len(grant.ModelIDs) == 1 {
		modelID = grant.ModelIDs[0]
	}
	transaction, err := r.beginAdminAudit(req, "owner_grant.create", grant.TargetID, modelID, grant.ID)
	if err != nil {
		http.Error(w, errAuditStoreUnavailable, http.StatusServiceUnavailable)
		return
	}
	err = modelconfig.CreateOwnerGrant(directory, grant)
	if !r.finishAdminMutation(w, transaction, err, grant.TargetID, modelID, grant.ID) {
		return
	}
	if err != nil {
		switch {
		case errors.Is(err, modelconfig.ErrOwnerGrantConflict):
			http.Error(w, "overlapping owner grant exists; revoke or narrow it first", http.StatusConflict)
		case errors.Is(err, modelconfig.ErrOwnerGrantStore):
			http.Error(w, errOwnerGrantStoreUnavailable, http.StatusServiceUnavailable)
		default:
			http.Error(w, errInvalidOwnerGrantRequest, http.StatusBadRequest)
		}
		return
	}
	w.Header().Set(contentTypeHeaderKey, contentTypeJSON)
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(grant)
}

// findOwnerGrantMetadata locates the grant matching grantID and returns its target
// ID and, when the grant carries exactly one model, that model ID. If no grant
// matches, both values are blank. The helper is pure: it performs no I/O and has
// no side effects, keeping the audit metadata lookup separable from the revoke flow.
func findOwnerGrantMetadata(grants []modelconfig.OwnerGrant, grantID string) (targetID, modelID string) {
	for _, grant := range grants {
		if grant.ID == grantID {
			targetID = grant.TargetID
			if len(grant.ModelIDs) == 1 {
				modelID = grant.ModelIDs[0]
			}
			break
		}
	}
	return
}

func (r *Reloader) revokeAdminOwnerGrant(w http.ResponseWriter, req *http.Request, grantID string) {
	if !modelconfig.ValidDelegatedOperationID(grantID) {
		http.Error(w, "invalid grant ID", http.StatusBadRequest)
		return
	}
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.stopped {
		http.Error(w, errOwnerGrantUnavailable, http.StatusServiceUnavailable)
		return
	}
	_, directory, status := r.adminOwnerGrantStore()
	if status != 0 {
		http.Error(w, errOwnerGrantStoreUnavailable, status)
		return
	}
	grants, err := modelconfig.ListOwnerGrants(directory)
	if err != nil {
		http.Error(w, errOwnerGrantStoreUnavailable, http.StatusServiceUnavailable)
		return
	}
	targetID, modelID := findOwnerGrantMetadata(grants, grantID)
	transaction, err := r.beginAdminAudit(req, "owner_grant.revoke", targetID, modelID, grantID)
	if err != nil {
		http.Error(w, errAuditStoreUnavailable, http.StatusServiceUnavailable)
		return
	}
	err = modelconfig.RevokeOwnerGrant(directory, grantID)
	if !r.finishAdminMutation(w, transaction, err, targetID, modelID, grantID) {
		return
	}
	if err != nil {
		if errors.Is(err, modelconfig.ErrOwnerGrantNotFound) {
			http.NotFound(w, nil)
		} else {
			http.Error(w, errOwnerGrantStoreUnavailable, http.StatusServiceUnavailable)
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (r *Reloader) adminOwnerGrantStore() (*reloaderState, string, int) {
	state := r.state.Load()
	if state == nil || state.peer == nil || state.peer.FederationID == "" || state.pairingStore == nil ||
		state.management == nil || state.management.DelegatedStore == nil || state.management.DelegatedStore.GrantDirectory == "" {
		return nil, "", http.StatusServiceUnavailable
	}
	return state, state.management.DelegatedStore.GrantDirectory, 0
}

func randomOwnerGrantID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

func (r *Reloader) adminPairingStore() (*federation.PairingStore, int) {
	state := r.state.Load()
	if state == nil || state.peer == nil || state.peer.FederationID == "" || state.pairingStore == nil {
		return nil, http.StatusServiceUnavailable
	}
	return state.pairingStore, 0
}

func decodeAdminPairingJSON(w http.ResponseWriter, req *http.Request, out any) error {
	mediaType, _, mediaTypeErr := mime.ParseMediaType(req.Header.Get(contentTypeHeaderKey))
	if req.ContentLength < 0 || req.ContentLength > 4096 || len(req.TransferEncoding) != 0 ||
		mediaTypeErr != nil || mediaType != contentTypeJSON {
		return federation.ErrInvitation
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, req.Body, 4096))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return federation.ErrInvitation
	}
	return nil
}

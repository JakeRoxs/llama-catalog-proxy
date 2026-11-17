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
	switch {
	case req.URL.Path == adminPairingPath+"/members" && req.Method == http.MethodGet:
		r.listAdminPairingMembers(w, req)
	case req.URL.Path == adminPairingPath+"/invitations" && req.Method == http.MethodGet:
		r.listAdminPairingInvitations(w, req)
	case req.URL.Path == adminPairingPath+"/invitations" && req.Method == http.MethodPost:
		r.createAdminPairingInvitation(w, req)
	case req.URL.Path == adminPairingPath+"/join" && req.Method == http.MethodPost:
		r.joinAdminPairing(w, req)
	case req.URL.Path == adminPairingPath+"/grants" && req.Method == http.MethodGet:
		r.listAdminOwnerGrants(w, req)
	case req.URL.Path == adminPairingPath+"/grants" && req.Method == http.MethodPost:
		r.createAdminOwnerGrant(w, req)
	case strings.HasPrefix(req.URL.Path, adminPairingPath+"/grants/") && req.Method == http.MethodDelete:
		r.revokeAdminOwnerGrant(w, req, strings.TrimPrefix(req.URL.Path, adminPairingPath+"/grants/"))
	case strings.HasPrefix(req.URL.Path, adminPairingPath+"/members/") && req.Method == http.MethodDelete:
		r.removeAdminPairingMember(w, req, strings.TrimPrefix(req.URL.Path, adminPairingPath+"/members/"))
	case strings.HasPrefix(req.URL.Path, adminPairingPath+"/invitations/") && req.Method == http.MethodDelete:
		r.removeAdminPairingInvitation(w, req, strings.TrimPrefix(req.URL.Path, adminPairingPath+"/invitations/"))
	default:
		w.Header().Set("Allow", "GET, POST, DELETE")
		http.NotFound(w, req)
	}
}

func (r *Reloader) listAdminPairingMembers(w http.ResponseWriter, _ *http.Request) {
	store, status := r.adminPairingStore()
	if status != 0 {
		http.Error(w, "pairing unavailable", status)
		return
	}
	members, err := store.ListMembers()
	if err != nil {
		http.Error(w, "pairing unavailable", http.StatusServiceUnavailable)
		return
	}
	writeJSON(r.logger, w, http.StatusOK, map[string]any{"state": "available", "members": members})
}

func (r *Reloader) listAdminPairingInvitations(w http.ResponseWriter, _ *http.Request) {
	store, status := r.adminPairingStore()
	if status != 0 {
		http.Error(w, "pairing unavailable", status)
		return
	}
	invitations, err := store.ListInvitations(time.Now())
	if err != nil {
		http.Error(w, "pairing unavailable", http.StatusServiceUnavailable)
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
		http.Error(w, "pairing unavailable", http.StatusServiceUnavailable)
		return
	}
	store, status := r.adminPairingStore()
	if status != 0 {
		http.Error(w, "pairing unavailable", status)
		return
	}
	state := r.state.Load()
	transaction, err := r.beginAdminAudit(req, "pairing.invitation.create", input.NodeID, "", "")
	if err != nil {
		http.Error(w, "audit store unavailable", http.StatusServiceUnavailable)
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
	w.Header().Set("Content-Type", "application/json")
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
		http.Error(w, "pairing unavailable", http.StatusServiceUnavailable)
		return
	}
	state := r.state.Load()
	if state == nil || state.peer == nil || state.peer.FederationID == "" || state.pairingStore == nil ||
		state.certificate == nil || state.peerClientCAs == nil || len(state.peerClientCRLs) == 0 {
		http.Error(w, "pairing unavailable", http.StatusServiceUnavailable)
		return
	}
	transaction, err := r.beginAdminAudit(req, "pairing.join", "", "", "")
	if err != nil {
		http.Error(w, "audit store unavailable", http.StatusServiceUnavailable)
		return
	}
	member, err := state.pairingStore.Join(req.Context(), input.URL, input.Code, *state.certificate, state.peerClientCAs,
		state.peerClientCRLs, state.peer.RevocationMaxAge())
	if !r.finishAdminMutation(w, transaction, err, member.NodeID, "", "") {
		return
	}
	if err != nil {
		if errors.Is(err, federation.ErrPairingStore) || errors.Is(err, federation.ErrInvitationFull) {
			http.Error(w, "pairing unavailable", http.StatusServiceUnavailable)
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
		http.Error(w, "pairing unavailable", http.StatusServiceUnavailable)
		return
	}
	store, status := r.adminPairingStore()
	if status != 0 {
		http.Error(w, "pairing unavailable", status)
		return
	}
	if strings.Contains(nodeID, "/") || !identity.ValidNodeID(nodeID) {
		http.Error(w, "invalid node ID", http.StatusBadRequest)
		return
	}
	transaction, err := r.beginAdminAudit(req, "pairing.member.revoke", nodeID, "", "")
	if err != nil {
		http.Error(w, "audit store unavailable", http.StatusServiceUnavailable)
		return
	}
	err = store.RemoveMember(nodeID, time.Now())
	if !r.finishAdminMutation(w, transaction, err, nodeID, "", "") {
		return
	}
	if err != nil {
		if errors.Is(err, federation.ErrPairingStore) {
			http.Error(w, "pairing unavailable", http.StatusServiceUnavailable)
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
		http.Error(w, "pairing unavailable", http.StatusServiceUnavailable)
		return
	}
	store, status := r.adminPairingStore()
	if status != 0 {
		http.Error(w, "pairing unavailable", status)
		return
	}
	if strings.Contains(nodeID, "/") || !identity.ValidNodeID(nodeID) {
		http.Error(w, "invalid node ID", http.StatusBadRequest)
		return
	}
	transaction, err := r.beginAdminAudit(req, "pairing.invitation.revoke", nodeID, "", "")
	if err != nil {
		http.Error(w, "audit store unavailable", http.StatusServiceUnavailable)
		return
	}
	err = store.RemoveInvitation(nodeID)
	if !r.finishAdminMutation(w, transaction, err, nodeID, "", "") {
		return
	}
	if err != nil {
		if errors.Is(err, federation.ErrPairingStore) {
			http.Error(w, "pairing unavailable", http.StatusServiceUnavailable)
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
		http.Error(w, "owner grant store unavailable", status)
		return
	}
	grants, err := modelconfig.ListOwnerGrants(directory)
	if err != nil {
		http.Error(w, "owner grant store unavailable", http.StatusServiceUnavailable)
		return
	}
	writeJSON(r.logger, w, http.StatusOK, map[string]any{"state": "available", "grants": grants})
}

func (r *Reloader) createAdminOwnerGrant(w http.ResponseWriter, req *http.Request) {
	var input struct {
		RequesterNodeID string   `json:"requester_node_id"`
		BackendID       string   `json:"backend_id"`
		ModelIDs        []string `json:"model_ids"`
		ExpiresIn       string   `json:"expires_in"`
		ConfirmGrant    bool     `json:"confirm_grant"`
	}
	if err := decodeAdminPairingJSON(w, req, &input); err != nil {
		http.Error(w, "invalid owner grant request", http.StatusBadRequest)
		return
	}
	if !input.ConfirmGrant || !identity.ValidNodeID(input.RequesterNodeID) || !identity.ValidNodeID(input.BackendID) || len(input.ModelIDs) == 0 ||
		len(input.ModelIDs) > modelconfig.MaxOwnerGrantModels {
		http.Error(w, "invalid owner grant request", http.StatusBadRequest)
		return
	}
	grantID, err := randomOwnerGrantID()
	if err != nil {
		http.Error(w, "owner grant unavailable", http.StatusServiceUnavailable)
		return
	}
	lifetime, err := time.ParseDuration(strings.TrimSpace(input.ExpiresIn))
	if err != nil || lifetime < 5*time.Minute || lifetime > modelconfig.MaxOwnerGrantLifetime {
		http.Error(w, "grant expiry must be between five minutes and 90 days", http.StatusBadRequest)
		return
	}
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.stopped {
		http.Error(w, "owner grant unavailable", http.StatusServiceUnavailable)
		return
	}
	state, directory, status := r.adminOwnerGrantStore()
	if status != 0 {
		http.Error(w, "owner grant store unavailable", status)
		return
	}
	if state.peer == nil || !slices.Contains(state.peer.LocalBackends, input.BackendID) {
		http.Error(w, "backend is not locally owned", http.StatusBadRequest)
		return
	}
	peerGrant, ok := state.peer.Peers[input.RequesterNodeID]
	if !ok || !peerGrant.ManageSettings {
		http.Error(w, "requester has no owner-approved manage_settings ceiling", http.StatusForbidden)
		return
	}
	member, paired, err := state.pairingStore.LookupMember(input.RequesterNodeID)
	if err != nil || !paired || member.NodeID != input.RequesterNodeID {
		http.Error(w, "requester is not a paired identity", http.StatusForbidden)
		return
	}
	targetID, ok := state.peer.ManagementTargets[input.BackendID]
	if !ok || state.management == nil {
		http.Error(w, "backend has no local management target", http.StatusBadRequest)
		return
	}
	target, ok := state.management.Targets[targetID]
	if !ok {
		http.Error(w, "management target unavailable", http.StatusBadRequest)
		return
	}
	modelsSeen := make(map[string]struct{}, len(input.ModelIDs))
	for _, modelID := range input.ModelIDs {
		if !identity.ValidModelComponent(modelID) || strings.TrimSpace(modelID) == "" {
			http.Error(w, "invalid model ID", http.StatusBadRequest)
			return
		}
		if _, duplicate := modelsSeen[modelID]; duplicate {
			http.Error(w, "duplicate model ID", http.StatusBadRequest)
			return
		}
		modelsSeen[modelID] = struct{}{}
		if _, err := modelconfig.ReadSettingsTarget(target.Directory, target.Filename, targetID, modelID); err != nil {
			http.Error(w, "model is not safely manageable on this target", http.StatusBadRequest)
			return
		}
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
		http.Error(w, "audit store unavailable", http.StatusServiceUnavailable)
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
			http.Error(w, "owner grant store unavailable", http.StatusServiceUnavailable)
		default:
			http.Error(w, "invalid owner grant request", http.StatusBadRequest)
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(grant)
}

func (r *Reloader) revokeAdminOwnerGrant(w http.ResponseWriter, req *http.Request, grantID string) {
	if !modelconfig.ValidDelegatedOperationID(grantID) {
		http.Error(w, "invalid grant ID", http.StatusBadRequest)
		return
	}
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.stopped {
		http.Error(w, "owner grant unavailable", http.StatusServiceUnavailable)
		return
	}
	_, directory, status := r.adminOwnerGrantStore()
	if status != 0 {
		http.Error(w, "owner grant store unavailable", status)
		return
	}
	grants, err := modelconfig.ListOwnerGrants(directory)
	if err != nil {
		http.Error(w, "owner grant store unavailable", http.StatusServiceUnavailable)
		return
	}
	targetID, modelID := "", ""
	for _, grant := range grants {
		if grant.ID == grantID {
			targetID = grant.TargetID
			if len(grant.ModelIDs) == 1 {
				modelID = grant.ModelIDs[0]
			}
			break
		}
	}
	transaction, err := r.beginAdminAudit(req, "owner_grant.revoke", targetID, modelID, grantID)
	if err != nil {
		http.Error(w, "audit store unavailable", http.StatusServiceUnavailable)
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
			http.Error(w, "owner grant store unavailable", http.StatusServiceUnavailable)
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
	mediaType, _, mediaTypeErr := mime.ParseMediaType(req.Header.Get("Content-Type"))
	if req.ContentLength < 0 || req.ContentLength > 4096 || len(req.TransferEncoding) != 0 ||
		mediaTypeErr != nil || mediaType != "application/json" {
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

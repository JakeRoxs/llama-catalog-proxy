package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/identity"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
)

const adminManagementPath = "/admin/api/v1/management"

const auditStoreUnavailableMsg = "audit store unavailable"

func (r *Reloader) serveAdminManagement(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	subject, ok := r.adminAuth.RequireRole(req, "configure_models")
	if !ok {
		r.auditDeniedAdminRole(req)
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	req = withAdminAuditPrincipal(req, subject, "configure_models")
	if req.Method != http.MethodGet && !r.adminAuth.RequireCSRF(req) {
		r.auditDeniedAdminRole(req)
		http.Error(w, "invalid CSRF token or origin", http.StatusForbidden)
		return
	}
	state := r.state.Load()
	if state == nil {
		http.Error(w, "management unavailable", http.StatusServiceUnavailable)
		return
	}
	switch {
	case req.URL.Path == adminManagementPath+"/grants" && req.Method == http.MethodGet:
		r.adminForwardSettingsGrants(w, req, state)
	case req.URL.Path == adminManagementPath+"/settings/snapshot" && req.Method == http.MethodPost:
		r.adminForwardSettingsSnapshot(w, req, state)
	case req.URL.Path == adminManagementPath+"/settings/preview" && req.Method == http.MethodPost:
		r.adminForwardSettingsPreview(w, req, state)
	case req.URL.Path == adminManagementPath+"/proposals" && req.Method == http.MethodPost:
		r.adminForwardProposal(w, req, state, false)
	case req.URL.Path == adminManagementPath+"/proposals/recover" && req.Method == http.MethodPost:
		r.adminForwardProposal(w, req, state, true)
	case req.URL.Path == adminManagementPath+"/proposals/apply" && req.Method == http.MethodPost:
		r.adminForwardApply(w, req, state)
	case req.URL.Path == adminManagementPath+"/operations" && req.Method == http.MethodGet:
		r.adminForwardOperation(w, req, state)
	default:
		http.NotFound(w, req)
	}
}

func (r *Reloader) adminForwardSettingsGrants(w http.ResponseWriter, req *http.Request, state *reloaderState) {
	query := req.URL.Query()
	if req.URL.RawPath != "" || req.URL.ForceQuery || len(query) != 1 || len(query["owner_node_id"]) != 1 ||
		!identity.ValidNodeID(query.Get("owner_node_id")) {
		http.Error(w, "invalid owner query", http.StatusBadRequest)
		return
	}
	ownerID := query.Get("owner_node_id")
	tx, err := r.beginAdminAudit(req, "delegation.grants.list", ownerID, "", "")
	if err != nil {
		http.Error(w, auditStoreUnavailableMsg, http.StatusServiceUnavailable)
		return
	}
	owner, grants, err := r.forwardDelegatedSettingsGrants(req, state, ownerID)
	if !r.finishAdminMutation(w, tx, err, ownerID, "", "") {
		return
	}
	if err != nil {
		r.writeAdminManagementError(w, err)
		return
	}
	writeJSON(r.logger, w, http.StatusOK, map[string]any{"owner": owner, "grants": grants})
}

func (r *Reloader) adminForwardSettingsSnapshot(w http.ResponseWriter, req *http.Request, state *reloaderState) {
	var input struct {
		OwnerNodeID string `json:"owner_node_id"`
		BackendID   string `json:"backend_id"`
		TargetID    string `json:"target_id"`
		ModelID     string `json:"model_id"`
	}
	if err := decodeAdminManagementRequest(w, req, &input); err != nil || !identity.ValidNodeID(input.OwnerNodeID) {
		http.Error(w, "invalid settings snapshot request", http.StatusBadRequest)
		return
	}
	tx, err := r.beginAdminAudit(req, "settings.snapshot.read", input.TargetID, input.ModelID, "")
	if err != nil {
		http.Error(w, auditStoreUnavailableMsg, http.StatusServiceUnavailable)
		return
	}
	owner, snapshot, err := r.forwardDelegatedSettingsSnapshot(req, state, input.OwnerNodeID,
		delegatedSettingsTargetRequest{BackendID: input.BackendID, TargetID: input.TargetID, ModelID: input.ModelID})
	if !r.finishAdminMutation(w, tx, err, input.TargetID, input.ModelID, "") {
		return
	}
	if err != nil {
		r.writeAdminManagementError(w, err)
		return
	}
	writeJSON(r.logger, w, http.StatusOK, map[string]any{"owner": owner, "snapshot": snapshot})
}

func (r *Reloader) adminForwardSettingsPreview(w http.ResponseWriter, req *http.Request, state *reloaderState) {
	var input struct {
		OwnerNodeID string                      `json:"owner_node_id"`
		BackendID   string                      `json:"backend_id"`
		TargetID    string                      `json:"target_id"`
		ModelID     string                      `json:"model_id"`
		Settings    modelconfig.CommandSettings `json:"settings"`
	}
	if err := decodeAdminManagementRequest(w, req, &input); err != nil || !identity.ValidNodeID(input.OwnerNodeID) {
		http.Error(w, "invalid settings preview request", http.StatusBadRequest)
		return
	}
	tx, err := r.beginAdminAudit(req, "settings.preview", input.TargetID, input.ModelID, "")
	if err != nil {
		http.Error(w, auditStoreUnavailableMsg, http.StatusServiceUnavailable)
		return
	}
	owner, preview, err := r.forwardDelegatedSettingsPreview(req, state, input.OwnerNodeID,
		delegatedSettingsPreviewRequest{BackendID: input.BackendID, TargetID: input.TargetID, ModelID: input.ModelID, Settings: input.Settings})
	if !r.finishAdminMutation(w, tx, err, input.TargetID, input.ModelID, "") {
		return
	}
	if err != nil {
		r.writeAdminManagementError(w, err)
		return
	}
	writeJSON(r.logger, w, http.StatusOK, map[string]any{"owner": owner, "preview": preview})
}

func (r *Reloader) adminForwardProposal(w http.ResponseWriter, req *http.Request, state *reloaderState, recoverProposal bool) {
	data, err := readAdminManagementJSON(req)
	if err != nil {
		http.Error(w, "invalid delegated settings request", http.StatusBadRequest)
		return
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var input delegatedSettingsRequest
	if decoder.Decode(&input) != nil || decoder.Decode(new(any)) != io.EOF || input.Owner.NodeID == "" {
		http.Error(w, "invalid delegated settings request", http.StatusBadRequest)
		return
	}
	action := "settings.proposal.create"
	if recoverProposal {
		action = "settings.proposal.recover"
	}
	tx, err := r.beginAdminAudit(req, action, input.TargetID, input.Model, "")
	if err != nil {
		http.Error(w, auditStoreUnavailableMsg, http.StatusServiceUnavailable)
		return
	}
	var proposal modelconfig.DelegatedProposal
	if recoverProposal {
		proposal, err = r.forwardDelegatedSettingsProposalRecovery(req, state, input)
	} else {
		proposal, err = r.forwardDelegatedSettingsProposal(req, state, input)
	}
	if !r.finishAdminMutation(w, tx, err, input.TargetID, input.Model, "") {
		return
	}
	if err != nil {
		r.writeAdminManagementError(w, err)
		return
	}
	writeJSON(r.logger, w, http.StatusOK, proposal)
}

func (r *Reloader) adminForwardApply(w http.ResponseWriter, req *http.Request, state *reloaderState) {
	data, err := readAdminManagementJSON(req)
	if err != nil {
		http.Error(w, "invalid delegated apply request", http.StatusBadRequest)
		return
	}
	var input struct {
		Review                     modelconfig.DelegatedProposal `json:"review"`
		OperationID                string                        `json:"operation_id"`
		AcknowledgeReview          bool                          `json:"acknowledge_review"`
		AcknowledgeDisruption      bool                          `json:"acknowledge_disruption"`
		AcknowledgeExclusiveWindow bool                          `json:"acknowledge_exclusive_window"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&input) != nil || decoder.Decode(new(any)) != io.EOF {
		http.Error(w, "invalid delegated apply request", http.StatusBadRequest)
		return
	}
	tx, err := r.beginAdminAudit(req, "settings.apply", input.Review.Proposal.TargetID, input.Review.Proposal.ModelID, input.OperationID)
	if err != nil {
		http.Error(w, auditStoreUnavailableMsg, http.StatusServiceUnavailable)
		return
	}
	record, err := r.forwardDelegatedSettingsApply(req, state, input.Review, input.OperationID,
		input.AcknowledgeReview, input.AcknowledgeDisruption, input.AcknowledgeExclusiveWindow)
	if !r.finishAdminMutation(w, tx, err, input.Review.Proposal.TargetID, input.Review.Proposal.ModelID, input.OperationID) {
		return
	}
	if err != nil {
		r.writeAdminManagementError(w, err)
		return
	}
	writeJSON(r.logger, w, http.StatusOK, record)
}

func (r *Reloader) adminForwardOperation(w http.ResponseWriter, req *http.Request, state *reloaderState) {
	query := req.URL.Query()
	if len(query) != 3 || len(query["owner_node_id"]) != 1 || len(query["proposal_id"]) != 1 || len(query["operation_id"]) != 1 {
		http.Error(w, "invalid operation query", http.StatusBadRequest)
		return
	}
	tx, err := r.beginAdminAudit(req, "settings.operation.read", query.Get("owner_node_id"), "", query.Get("operation_id"))
	if err != nil {
		http.Error(w, auditStoreUnavailableMsg, http.StatusServiceUnavailable)
		return
	}
	record, err := r.forwardDelegatedOperation(req, state, query.Get("owner_node_id"), query.Get("proposal_id"), query.Get("operation_id"))
	if !r.finishAdminMutation(w, tx, err, record.TargetID, record.ModelID, query.Get("operation_id")) {
		return
	}
	if err != nil {
		r.writeAdminManagementError(w, err)
		return
	}
	writeJSON(r.logger, w, http.StatusOK, record)
}

func (r *Reloader) writeAdminManagementError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errDelegationUnauthorized):
		http.Error(w, "delegated action unauthorized", http.StatusForbidden)
	case errors.Is(err, errDelegationBusy):
		http.Error(w, "delegated management busy", http.StatusServiceUnavailable)
	case errors.Is(err, errDelegationPeerUnavailable), errors.Is(err, errDelegationPeerResponse):
		http.Error(w, "delegated peer unavailable", http.StatusBadGateway)
	case errors.Is(err, errDelegationProposalUnknown), errors.Is(err, errDelegationOutcomeUnknown):
		http.Error(w, "delegated outcome unknown; recover proposal or inspect the exact operation before retrying", http.StatusBadGateway)
	case errors.Is(err, errDelegationChanged), errors.Is(err, modelconfig.ErrDelegatedProposalBinding):
		http.Error(w, "delegated state changed", http.StatusConflict)
	case errors.Is(err, errDelegationAcknowledgment):
		http.Error(w, "explicit apply acknowledgments required", http.StatusBadRequest)
	default:
		http.Error(w, "delegated action failed", http.StatusBadRequest)
	}
}

func readAdminManagementJSON(req *http.Request) ([]byte, error) {
	if req.Body == nil || req.URL.RawQuery != "" || req.URL.RawPath != "" || req.ContentLength < 0 ||
		req.ContentLength > maxManagementRequest || len(req.TransferEncoding) != 0 {
		return nil, errDelegationUnauthorized
	}
	contentType, _, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
	if err != nil || contentType != "application/json" {
		return nil, errDelegationUnauthorized
	}
	data, err := io.ReadAll(io.LimitReader(req.Body, maxManagementRequest+1))
	if err != nil || len(data) > maxManagementRequest || validateManagementJSON(data) != nil {
		return nil, errDelegationUnauthorized
	}
	return data, nil
}

func decodeAdminManagementRequest(w http.ResponseWriter, req *http.Request, out any) error {
	data, err := readAdminManagementJSON(req)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(out) != nil || decoder.Decode(new(any)) != io.EOF {
		return errDelegationUnauthorized
	}
	return nil
}

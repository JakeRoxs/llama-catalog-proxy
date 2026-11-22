package proxy

import (
	"errors"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/identity"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
)

const (
	peerOperationPath                = "/peer/v1/management/operations/"
	receivedOperationPath            = "/management/v1/delegation/operations/"
	ownerOperationPath               = "/management/v1/delegation/owner/operations/"
	peerSettingsApplyPath            = "/peer/v1/management/settings/apply"
	peerSettingsProposalPath         = "/peer/v1/management/settings/proposals"
	peerSettingsProposalRecoveryPath = "/peer/v1/management/settings/proposals/recover"
	peerSettingsGrantsPath           = "/peer/v1/management/settings/grants"
	peerSettingsSnapshotPath         = "/peer/v1/management/settings/snapshot"
	peerSettingsPreviewPath          = "/peer/v1/management/settings/preview"
	receivedSettingsPathPrefix       = "/management/v1/delegation/settings/"
	receivedSettingsProposalPath     = receivedSettingsPathPrefix + "proposals"
	receivedSettingsRecoveryPath     = receivedSettingsPathPrefix + "proposals/recover"
	receivedSettingsApplyPath        = receivedSettingsPathPrefix + "apply"
	ownerProposalsPath               = "/management/v1/delegation/owner/proposals"
)

// Duplicated string literals addressed by sonarqube go:S1192.
// Each constant replaces a literal repeated across multiple handler paths.
const (
	headerCacheControl        = "Cache-Control"
	headerXContentTypeOptions = "X-Content-Type-Options"
	cacheControlNoStore       = "no-store"

	errTLSRequired                = "TLS required"
	errMethodNotAllowed           = "method not allowed"
	errInvalidRequestOrigin       = "invalid request origin"
	errRequestBodyNotAllowed      = "request body not allowed"
	errPeerServiceBusy            = "peer service busy"
	errPeerAuditStoreUnavailable  = "audit store unavailable"
	errAuditCompletionUnavailable = "audit completion unavailable"
)

const delegatedProposalLifetime = 15 * time.Minute

func (r *Reloader) serveDelegatedManagement(w http.ResponseWriter, req *http.Request, state *reloaderState) {
	switch {
	case req.URL.Path == receivedSettingsProposalPath || req.URL.Path == receivedSettingsRecoveryPath || req.URL.Path == receivedSettingsApplyPath:
		r.serveReceivingDelegatedSettings(w, req, state)
	case strings.HasPrefix(req.URL.Path, receivedOperationPath):
		r.serveReceivingDelegatedOperation(w, req)
	case req.URL.Path == ownerProposalsPath:
		r.serveOwnerDelegatedProposals(w, req)
	case strings.HasPrefix(req.URL.Path, ownerOperationPath):
		r.serveOwnerDelegatedOperation(w, req)
	default:
		http.NotFound(w, req)
	}
}

func (r *Reloader) serveReceivingDelegatedSettings(w http.ResponseWriter, req *http.Request, state *reloaderState) {
	w.Header().Set(headerCacheControl, cacheControlNoStore)
	w.Header().Set(headerXContentTypeOptions, "nosniff")
	if req.TLS == nil {
		http.Error(w, errTLSRequired, http.StatusForbidden)
		return
	}
	if req.URL.RawPath != "" || req.URL.EscapedPath() != req.URL.Path || req.URL.RawQuery != "" || req.URL.ForceQuery {
		http.NotFound(w, req)
		return
	}
	if !sameIngestionOrigin(req) {
		http.Error(w, errInvalidRequestOrigin, http.StatusForbidden)
		return
	}
	if state == nil || state.management == nil || !r.delegatedCallerAuthorized(req, state) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, errMethodNotAllowed, http.StatusMethodNotAllowed)
		return
	}
	switch req.URL.Path {
	case receivedSettingsProposalPath, receivedSettingsRecoveryPath:
		var input delegatedSettingsRequest
		if err := decodeBenchmarkJSON(req, &input); err != nil {
			http.Error(w, "invalid delegated settings request", http.StatusBadRequest)
			return
		}
		var proposal modelconfig.DelegatedProposal
		var err error
		if req.URL.Path == receivedSettingsProposalPath {
			proposal, err = r.forwardDelegatedSettingsProposal(req, state, input)
		} else {
			proposal, err = r.forwardDelegatedSettingsProposalRecovery(req, state, input)
		}
		if err != nil {
			writeDelegatedOperationError(w, err)
			return
		}
		writeJSON(r.logger, w, http.StatusOK, proposal)
	case receivedSettingsApplyPath:
		var input struct {
			Review                     modelconfig.DelegatedProposal `json:"review"`
			OperationID                string                        `json:"operation_id"`
			AcknowledgeReview          bool                          `json:"acknowledge_review"`
			AcknowledgeDisruption      bool                          `json:"acknowledge_disruption"`
			AcknowledgeExclusiveWindow bool                          `json:"acknowledge_exclusive_window"`
		}
		if err := decodeBenchmarkJSON(req, &input); err != nil {
			http.Error(w, "invalid delegated apply request", http.StatusBadRequest)
			return
		}
		operation, err := r.forwardDelegatedSettingsApply(req, state, input.Review, input.OperationID,
			input.AcknowledgeReview, input.AcknowledgeDisruption, input.AcknowledgeExclusiveWindow)
		if err != nil {
			writeDelegatedOperationError(w, err)
			return
		}
		writeJSON(r.logger, w, http.StatusOK, operation)
	}
}

func delegatedOperationPath(req *http.Request, prefix string, withOwner bool) (string, string, string, bool) {
	if req.URL.RawQuery != "" || req.URL.ForceQuery || req.URL.RawPath != "" || req.URL.EscapedPath() != req.URL.Path {
		return "", "", "", false
	}
	path, ok := strings.CutPrefix(req.URL.Path, prefix)
	if !ok {
		return "", "", "", false
	}
	parts := strings.Split(path, "/")
	if withOwner {
		if len(parts) != 3 || !identity.ValidNodeID(parts[0]) ||
			!modelconfig.ValidDelegatedOperationID(parts[1]) || !modelconfig.ValidDelegatedOperationID(parts[2]) {
			return "", "", "", false
		}
		return parts[0], parts[1], parts[2], true
	}
	if len(parts) != 2 || !modelconfig.ValidDelegatedOperationID(parts[0]) || !modelconfig.ValidDelegatedOperationID(parts[1]) {
		return "", "", "", false
	}
	return "", parts[0], parts[1], true
}

func prepareDelegatedOperationLookup(w http.ResponseWriter, req *http.Request) bool {
	w.Header().Set(headerCacheControl, cacheControlNoStore)
	w.Header().Set(headerXContentTypeOptions, "nosniff")
	if req.TLS == nil {
		http.Error(w, errTLSRequired, http.StatusForbidden)
		return false
	}
	if req.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, errMethodNotAllowed, http.StatusMethodNotAllowed)
		return false
	}
	if req.ContentLength != 0 || len(req.TransferEncoding) != 0 {
		req.Close = true
		w.Header().Set("Connection", "close")
		http.Error(w, errRequestBodyNotAllowed, http.StatusBadRequest)
		return false
	}
	return true
}

// These handlers are intentionally not registered on the public listeners.
func (r *Reloader) servePeerDelegatedOperation(w http.ResponseWriter, req *http.Request) {
	if !prepareDelegatedOperationLookup(w, req) {
		return
	}
	_, proposalID, operationID, ok := delegatedOperationPath(req, peerOperationPath, false)
	if !ok {
		http.NotFound(w, req)
		return
	}
	select {
	case r.peerSlots <- struct{}{}:
		defer func() { <-r.peerSlots }()
	default:
		http.Error(w, errPeerServiceBusy, http.StatusServiceUnavailable)
		return
	}
	record, err := r.peerDelegatedOperation(req, r.state.Load(), proposalID, operationID)
	if err != nil {
		writeDelegatedOperationError(w, err)
		return
	}
	writeJSON(r.logger, w, http.StatusOK, record)
}

func (r *Reloader) serveReceivingDelegatedOperation(w http.ResponseWriter, req *http.Request) {
	if !prepareDelegatedOperationLookup(w, req) {
		return
	}
	if !sameIngestionOrigin(req) {
		http.Error(w, errInvalidRequestOrigin, http.StatusForbidden)
		return
	}
	owner, proposalID, operationID, ok := delegatedOperationPath(req, receivedOperationPath, true)
	if !ok {
		http.NotFound(w, req)
		return
	}
	record, err := r.forwardDelegatedOperation(req, r.state.Load(), owner, proposalID, operationID)
	if err != nil {
		writeDelegatedOperationError(w, err)
		return
	}
	writeJSON(r.logger, w, http.StatusOK, record)
}

func (r *Reloader) preparePeerDelegatedSettings(w http.ResponseWriter, req *http.Request, path string) (*reloaderState, bool) {
	w.Header().Set(headerCacheControl, cacheControlNoStore)
	w.Header().Set(headerXContentTypeOptions, "nosniff")
	if req.TLS == nil {
		http.Error(w, errTLSRequired, http.StatusForbidden)
		return nil, false
	}
	if req.URL.Path != path || req.URL.RawPath != "" || req.URL.EscapedPath() != req.URL.Path ||
		req.URL.RawQuery != "" || req.URL.ForceQuery {
		http.NotFound(w, req)
		return nil, false
	}
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, errMethodNotAllowed, http.StatusMethodNotAllowed)
		return nil, false
	}
	select {
	case r.peerSlots <- struct{}{}:
	default:
		http.Error(w, errPeerServiceBusy, http.StatusServiceUnavailable)
		return nil, false
	}
	state := r.state.Load()
	if !peerAuthorized(req, state, peerManageSettings) {
		<-r.peerSlots
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return nil, false
	}
	return state, true
}

func (r *Reloader) preparePeerDelegatedSettingsGet(w http.ResponseWriter, req *http.Request, path string) (*reloaderState, string, bool) {
	w.Header().Set(headerCacheControl, cacheControlNoStore)
	w.Header().Set(headerXContentTypeOptions, "nosniff")
	if req.TLS == nil {
		http.Error(w, errTLSRequired, http.StatusForbidden)
		return nil, "", false
	}
	if req.URL.Path != path || req.URL.RawPath != "" || req.URL.EscapedPath() != req.URL.Path || req.URL.RawQuery != "" || req.URL.ForceQuery {
		http.NotFound(w, req)
		return nil, "", false
	}
	if req.Method != http.MethodGet || req.ContentLength != 0 || len(req.TransferEncoding) != 0 {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, errMethodNotAllowed, http.StatusMethodNotAllowed)
		return nil, "", false
	}
	select {
	case r.peerSlots <- struct{}{}:
	default:
		http.Error(w, errPeerServiceBusy, http.StatusServiceUnavailable)
		return nil, "", false
	}
	state := r.state.Load()
	requester, ok := authenticatedPeer(req, state, peerManageSettings)
	if !ok {
		<-r.peerSlots
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return nil, "", false
	}
	return state, requester, true
}

func (r *Reloader) servePeerDelegatedSettingsGrants(w http.ResponseWriter, req *http.Request) {
	state, requester, ok := r.preparePeerDelegatedSettingsGet(w, req, peerSettingsGrantsPath)
	if !ok {
		return
	}
	defer func() { <-r.peerSlots }()
	if state.peer == nil || state.peer.FederationID == "" || state.management == nil ||
		state.management.DelegatedStore == nil || state.management.DelegatedStore.GrantDirectory == "" {
		http.Error(w, "target grants unavailable", http.StatusForbidden)
		return
	}
	auditTx, auditErr := beginPeerAudit(state, state.auditStore, requester, "delegation.grants.list", "", "", "")
	if auditErr != nil {
		http.Error(w, errPeerAuditStoreUnavailable, http.StatusServiceUnavailable)
		return
	}
	grants, err := modelconfig.ListOwnerGrants(state.management.DelegatedStore.GrantDirectory)
	if finishAudit(auditTx, auditOutcomeForError(err), "", "", "") != nil {
		http.Error(w, errAuditCompletionUnavailable, http.StatusServiceUnavailable)
		return
	}
	if err != nil {
		http.Error(w, "owner grant store unavailable", http.StatusServiceUnavailable)
		return
	}
	active := make([]modelconfig.OwnerGrant, 0, len(grants))
	now := time.Now()
	for _, grant := range grants {
		if grant.RequesterNodeID == requester && grant.Action == "manage_settings" && now.Before(grant.ExpiresAt) {
			active = append(active, grant)
		}
	}
	writeJSON(r.logger, w, http.StatusOK, struct {
		Owner  modelconfig.DelegatedOwner `json:"owner"`
		Grants []modelconfig.OwnerGrant   `json:"grants"`
	}{modelconfig.DelegatedOwner{NodeID: state.peer.NodeID, ProxyInstanceID: r.instanceID, ConfigGeneration: state.generation}, active})
}

type delegatedSettingsTargetRequest struct {
	BackendID string `json:"backend_id"`
	TargetID  string `json:"target_id"`
	ModelID   string `json:"model_id"`
}

type delegatedSettingsPreviewRequest struct {
	BackendID string                      `json:"backend_id"`
	TargetID  string                      `json:"target_id"`
	ModelID   string                      `json:"model_id"`
	Settings  modelconfig.CommandSettings `json:"settings"`
}

func (r *Reloader) servePeerDelegatedSettingsSnapshot(w http.ResponseWriter, req *http.Request) {
	state, ok := r.preparePeerDelegatedSettings(w, req, peerSettingsSnapshotPath)
	if !ok {
		return
	}
	defer func() { <-r.peerSlots }()
	var input delegatedSettingsTargetRequest
	if err := decodeBenchmarkJSON(req, &input); err != nil {
		http.Error(w, "invalid settings snapshot request", http.StatusBadRequest)
		return
	}
	requester, _ := authenticatedPeer(req, state, peerManageSettings)
	target, err := r.authorizedDelegatedSettingsTarget(state, requester, input.BackendID, input.TargetID, input.ModelID)
	if err != nil {
		writeDelegatedOperationError(w, err)
		return
	}
	auditTx, auditErr := beginPeerAudit(state, state.auditStore, requester, "settings.snapshot.read", input.TargetID, input.ModelID, "")
	if auditErr != nil {
		http.Error(w, errPeerAuditStoreUnavailable, http.StatusServiceUnavailable)
		return
	}
	snapshot, err := modelconfig.ReadSettingsTarget(target.Directory, target.Filename, input.TargetID, input.ModelID)
	if finishAudit(auditTx, auditOutcomeForError(err), input.TargetID, input.ModelID, "") != nil {
		http.Error(w, errAuditCompletionUnavailable, http.StatusServiceUnavailable)
		return
	}
	if err != nil {
		writeDelegatedOperationError(w, err)
		return
	}
	writeJSON(r.logger, w, http.StatusOK, struct {
		Owner    modelconfig.DelegatedOwner   `json:"owner"`
		Snapshot modelconfig.SettingsSnapshot `json:"snapshot"`
	}{modelconfig.DelegatedOwner{NodeID: state.peer.NodeID, ProxyInstanceID: r.instanceID, ConfigGeneration: state.generation}, snapshot})
}

func (r *Reloader) servePeerDelegatedSettingsPreview(w http.ResponseWriter, req *http.Request) {
	state, ok := r.preparePeerDelegatedSettings(w, req, peerSettingsPreviewPath)
	if !ok {
		return
	}
	defer func() { <-r.peerSlots }()
	var input delegatedSettingsPreviewRequest
	if err := decodeBenchmarkJSON(req, &input); err != nil {
		http.Error(w, "invalid settings preview request", http.StatusBadRequest)
		return
	}
	requester, _ := authenticatedPeer(req, state, peerManageSettings)
	target, err := r.authorizedDelegatedSettingsTarget(state, requester, input.BackendID, input.TargetID, input.ModelID)
	if err != nil {
		writeDelegatedOperationError(w, err)
		return
	}
	auditTx, auditErr := beginPeerAudit(state, state.auditStore, requester, "settings.preview", input.TargetID, input.ModelID, "")
	if auditErr != nil {
		http.Error(w, errPeerAuditStoreUnavailable, http.StatusServiceUnavailable)
		return
	}
	preview, err := modelconfig.PreviewTarget(target.Directory, target.Filename, input.TargetID, input.ModelID, input.Settings)
	if finishAudit(auditTx, auditOutcomeForError(err), input.TargetID, input.ModelID, "") != nil {
		http.Error(w, errAuditCompletionUnavailable, http.StatusServiceUnavailable)
		return
	}
	if err != nil {
		writeDelegatedOperationError(w, err)
		return
	}
	writeJSON(r.logger, w, http.StatusOK, struct {
		Owner   modelconfig.DelegatedOwner `json:"owner"`
		Preview modelconfig.Preview        `json:"preview"`
	}{modelconfig.DelegatedOwner{NodeID: state.peer.NodeID, ProxyInstanceID: r.instanceID, ConfigGeneration: state.generation}, preview})
}

func (r *Reloader) authorizedDelegatedSettingsTarget(state *reloaderState, requester, backendID, targetID, modelID string) (config.ManagementTarget, error) {
	if state == nil || state.peer == nil || state.management == nil || !slices.Contains(state.peer.LocalBackends, backendID) {
		return config.ManagementTarget{}, errDelegationTarget
	}
	configuredTarget, ok := state.peer.ManagementTargets[backendID]
	if !ok || configuredTarget != targetID {
		return config.ManagementTarget{}, errDelegationTarget
	}
	target, ok := state.management.Targets[targetID]
	if !ok {
		return config.ManagementTarget{}, errDelegationTarget
	}
	if !delegatedOwnerGrantAllows(state, requester, backendID, targetID, modelID) {
		return config.ManagementTarget{}, errDelegationUnauthorized
	}
	return target, nil
}

// This internal handler is not registered on the peer listener.
func (r *Reloader) servePeerDelegatedSettingsProposal(w http.ResponseWriter, req *http.Request) {
	state, ok := r.preparePeerDelegatedSettings(w, req, peerSettingsProposalPath)
	if !ok {
		return
	}
	defer func() { <-r.peerSlots }()
	var input delegatedSettingsRequest
	if err := decodeBenchmarkJSON(req, &input); err != nil {
		http.Error(w, "invalid proposal request", http.StatusBadRequest)
		return
	}
	proposal, err := r.createDelegatedSettingsProposal(req, state, input, delegatedProposalLifetime)
	if err != nil {
		writeDelegatedOperationError(w, err)
		return
	}
	writeJSON(r.logger, w, http.StatusOK, proposal)
}

// Requester-scoped recovery is internal and intentionally not registered on
// the peer listener. It matches the authenticated peer and complete typed
// request; it does not infer a request ID or claim that delivery was lost.
func (r *Reloader) servePeerDelegatedSettingsProposalRecovery(w http.ResponseWriter, req *http.Request) {
	state, ok := r.preparePeerDelegatedSettings(w, req, peerSettingsProposalRecoveryPath)
	if !ok {
		return
	}
	defer func() { <-r.peerSlots }()
	var input delegatedSettingsRequest
	if err := decodeBenchmarkJSON(req, &input); err != nil {
		http.Error(w, "invalid proposal recovery request", http.StatusBadRequest)
		return
	}
	proposal, err := r.recoverDelegatedSettingsProposal(req, state, input)
	if err != nil {
		writeDelegatedOperationError(w, err)
		return
	}
	writeJSON(r.logger, w, http.StatusOK, proposal)
}

// This internal handler is not registered on the peer listener.
func (r *Reloader) servePeerDelegatedSettingsApply(w http.ResponseWriter, req *http.Request) {
	state, ok := r.preparePeerDelegatedSettings(w, req, peerSettingsApplyPath)
	if !ok {
		return
	}
	defer func() { <-r.peerSlots }()
	var input delegatedSettingsApply
	if err := decodeBenchmarkJSON(req, &input); err != nil {
		http.Error(w, "invalid apply request", http.StatusBadRequest)
		return
	}
	record, _, err := r.applyDelegatedSettings(req, state, input)
	if err != nil {
		// Once dispatch begins, an error alone cannot prove that no write occurred.
		writeDelegatedOperationError(w, err)
		return
	}
	writeJSON(r.logger, w, http.StatusOK, record)
}

// requireReviewAcknowledgements decodes and validates the owner review acknowledgement
// body. It returns nil when no review is requested or when both acknowledgements are
// present; otherwise it returns an error describing the rejection.
func requireReviewAcknowledgements(req *http.Request, review bool) error {
	if !review {
		return nil
	}
	var input struct {
		AcknowledgeReview  bool `json:"acknowledge_review"`
		AcknowledgeNoRetry bool `json:"acknowledge_no_retry"`
	}
	if err := decodeBenchmarkJSON(req, &input); err != nil || !input.AcknowledgeReview || !input.AcknowledgeNoRetry {
		return errors.New("review and no-retry acknowledgements required")
	}
	return nil
}

// Owner review acknowledges an unknown outcome without resolving it or permitting retry.
// This handler is intentionally not registered on the management listener.
func (r *Reloader) serveOwnerDelegatedOperation(w http.ResponseWriter, req *http.Request) {
	w.Header().Set(headerCacheControl, cacheControlNoStore)
	w.Header().Set(headerXContentTypeOptions, "nosniff")
	if req.TLS == nil {
		http.Error(w, errTLSRequired, http.StatusForbidden)
		return
	}
	if !sameIngestionOrigin(req) {
		http.Error(w, errInvalidRequestOrigin, http.StatusForbidden)
		return
	}
	pathRequest := *req
	pathURL := *req.URL
	pathRequest.URL = &pathURL
	review := strings.HasSuffix(pathURL.Path, "/review")
	if review {
		pathURL.Path = strings.TrimSuffix(pathURL.Path, "/review")
	}
	_, proposalID, operationID, ok := delegatedOperationPath(&pathRequest, ownerOperationPath, false)
	if !ok {
		http.NotFound(w, req)
		return
	}
	method := http.MethodGet
	if review {
		method = http.MethodPost
	}
	if req.Method != method {
		w.Header().Set("Allow", method)
		http.Error(w, errMethodNotAllowed, http.StatusMethodNotAllowed)
		return
	}
	if !review && (req.ContentLength != 0 || len(req.TransferEncoding) != 0) {
		req.Close = true
		w.Header().Set("Connection", "close")
		http.Error(w, errRequestBodyNotAllowed, http.StatusBadRequest)
		return
	}
	state := r.state.Load()
	if state == nil || state.management == nil || state.management.BackupStore == nil ||
		!managementBearerAuthorized(req, state.management.BackupStore.Token) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	select {
	case r.managementSlots <- struct{}{}:
		defer func() { <-r.managementSlots }()
	default:
		http.Error(w, "management service busy", http.StatusServiceUnavailable)
		return
	}
	if err := requireReviewAcknowledgements(req, review); err != nil {
		http.Error(w, "review and no-retry acknowledgements required", http.StatusBadRequest)
		return
	}
	record, err := r.ownerDelegatedOperation(req, state, proposalID, operationID, review)
	if err != nil {
		writeDelegatedOperationError(w, err)
		return
	}
	writeJSON(r.logger, w, http.StatusOK, record)
}

// Owner proposal inspection and cleanup are intentionally not registered on
// the management listener. Cleanup names proposals explicitly and rechecks
// expiry and operation-ledger absence while both stores are locked.
func (r *Reloader) serveOwnerDelegatedProposals(w http.ResponseWriter, req *http.Request) {
	w.Header().Set(headerCacheControl, cacheControlNoStore)
	w.Header().Set(headerXContentTypeOptions, "nosniff")
	if req.TLS == nil {
		http.Error(w, errTLSRequired, http.StatusForbidden)
		return
	}
	if req.URL.Path != ownerProposalsPath || req.URL.RawPath != "" || req.URL.EscapedPath() != req.URL.Path ||
		req.URL.RawQuery != "" || req.URL.ForceQuery {
		http.NotFound(w, req)
		return
	}
	if !sameIngestionOrigin(req) {
		http.Error(w, errInvalidRequestOrigin, http.StatusForbidden)
		return
	}
	cleanup := req.Method == http.MethodPost
	if !cleanup && req.Method != http.MethodGet {
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, errMethodNotAllowed, http.StatusMethodNotAllowed)
		return
	}
	if !cleanup && (req.ContentLength != 0 || len(req.TransferEncoding) != 0) {
		req.Close = true
		w.Header().Set("Connection", "close")
		http.Error(w, errRequestBodyNotAllowed, http.StatusBadRequest)
		return
	}
	state := r.state.Load()
	if state == nil || state.management == nil || state.management.BackupStore == nil ||
		!managementBearerAuthorized(req, state.management.BackupStore.Token) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	select {
	case r.managementSlots <- struct{}{}:
		defer func() { <-r.managementSlots }()
	default:
		http.Error(w, "management service busy", http.StatusServiceUnavailable)
		return
	}
	var proposalIDs []string
	if cleanup {
		var input struct {
			ProposalIDs []string `json:"proposal_ids"`
		}
		if err := decodeBenchmarkJSON(req, &input); err != nil || len(input.ProposalIDs) == 0 || len(input.ProposalIDs) > modelconfig.MaxProposals {
			http.Error(w, "invalid proposal cleanup request", http.StatusBadRequest)
			return
		}
		proposalIDs = input.ProposalIDs
	}
	candidates, removed, err := r.ownerDelegatedProposals(req, state, proposalIDs, cleanup)
	if err != nil {
		writeDelegatedOperationError(w, err)
		return
	}
	if cleanup {
		writeJSON(r.logger, w, http.StatusOK, struct {
			Removed []string `json:"removed"`
		}{Removed: removed})
		return
	}
	writeJSON(r.logger, w, http.StatusOK, struct {
		Eligible []modelconfig.DelegatedProposalCleanupCandidate `json:"eligible"`
	}{Eligible: candidates})
}

func writeDelegatedOperationError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errDelegationUnauthorized):
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	case errors.Is(err, modelconfig.ErrProposalNotFound):
		http.Error(w, "not found", http.StatusNotFound)
	case errors.Is(err, modelconfig.ErrDelegatedOperationConflict):
		http.Error(w, "delegated operation conflict", http.StatusConflict)
	case errors.Is(err, modelconfig.ErrDelegatedProposalCleanup):
		http.Error(w, "delegated proposal is not eligible for cleanup", http.StatusConflict)
	case errors.Is(err, modelconfig.ErrDelegatedProposalAmbiguous):
		http.Error(w, "delegated proposal recovery is ambiguous", http.StatusConflict)
	default:
		http.Error(w, "delegated operation unavailable", http.StatusServiceUnavailable)
	}
}

package proxy

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/identity"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
)

const (
	peerOperationPath        = "/peer/v1/management/operations/"
	receivedOperationPath    = "/management/v1/delegation/operations/"
	ownerOperationPath       = "/management/v1/delegation/owner/operations/"
	peerSettingsApplyPath    = "/peer/v1/management/settings/apply"
	peerSettingsProposalPath = "/peer/v1/management/settings/proposals"
)

const delegatedProposalLifetime = 15 * time.Minute

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
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if req.TLS == nil {
		http.Error(w, "TLS required", http.StatusForbidden)
		return false
	}
	if req.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return false
	}
	if req.ContentLength != 0 || len(req.TransferEncoding) != 0 {
		req.Close = true
		w.Header().Set("Connection", "close")
		http.Error(w, "request body not allowed", http.StatusBadRequest)
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
		http.Error(w, "peer service busy", http.StatusServiceUnavailable)
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
		http.Error(w, "invalid request origin", http.StatusForbidden)
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
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if req.TLS == nil {
		http.Error(w, "TLS required", http.StatusForbidden)
		return nil, false
	}
	if req.URL.Path != path || req.URL.RawPath != "" || req.URL.EscapedPath() != req.URL.Path ||
		req.URL.RawQuery != "" || req.URL.ForceQuery {
		http.NotFound(w, req)
		return nil, false
	}
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return nil, false
	}
	select {
	case r.peerSlots <- struct{}{}:
	default:
		http.Error(w, "peer service busy", http.StatusServiceUnavailable)
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

// Owner review acknowledges an unknown outcome without resolving it or permitting retry.
// This handler is intentionally not registered on the management listener.
func (r *Reloader) serveOwnerDelegatedOperation(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if req.TLS == nil {
		http.Error(w, "TLS required", http.StatusForbidden)
		return
	}
	if !sameIngestionOrigin(req) {
		http.Error(w, "invalid request origin", http.StatusForbidden)
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
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !review && (req.ContentLength != 0 || len(req.TransferEncoding) != 0) {
		req.Close = true
		w.Header().Set("Connection", "close")
		http.Error(w, "request body not allowed", http.StatusBadRequest)
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
	if review {
		var input struct {
			AcknowledgeReview  bool `json:"acknowledge_review"`
			AcknowledgeNoRetry bool `json:"acknowledge_no_retry"`
		}
		if err := decodeBenchmarkJSON(req, &input); err != nil || !input.AcknowledgeReview || !input.AcknowledgeNoRetry {
			http.Error(w, "review and no-retry acknowledgements required", http.StatusBadRequest)
			return
		}
	}
	record, err := r.ownerDelegatedOperation(req, state, proposalID, operationID, review)
	if err != nil {
		writeDelegatedOperationError(w, err)
		return
	}
	writeJSON(r.logger, w, http.StatusOK, record)
}

func writeDelegatedOperationError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errDelegationUnauthorized):
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	case errors.Is(err, modelconfig.ErrProposalNotFound):
		http.Error(w, "not found", http.StatusNotFound)
	case errors.Is(err, modelconfig.ErrDelegatedOperationConflict):
		http.Error(w, "delegated operation conflict", http.StatusConflict)
	default:
		http.Error(w, "delegated operation unavailable", http.StatusServiceUnavailable)
	}
}

package proxy

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/ingestion"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
)

// managementConfigChangedMsg centralizes the HTTP error message reported when
// the management configuration changes while a proposal request is in flight.
const managementConfigChangedMsg = "management configuration changed"

// proposalsBasePath centralizes the management API base path for proposal routes.
const proposalsBasePath = "/management/v1/proposals"

func (r *Reloader) serveProposals(w http.ResponseWriter, req *http.Request, state *reloaderState) {
	if req.URL.RawQuery != "" {
		http.NotFound(w, req)
		return
	}
	select {
	case r.managementSlots <- struct{}{}:
		defer func() { <-r.managementSlots }()
	default:
		http.Error(w, "management service busy", http.StatusServiceUnavailable)
		return
	}
	directory := state.management.ProposalStore.Directory
	if !separateProposalDirectory(directory, state.management.Targets) ||
		state.management.BackupStore != nil && !separateDirectories(directory, state.management.BackupStore.Directory) {
		writeProposalError(w, modelconfig.ErrProposalStore)
		return
	}
	if r.serveProposalSpecialRoutes(w, req, state, directory) {
		return
	}
	r.serveProposalListGetDelete(w, req, state, directory)
}

// serveProposalSpecialRoutes handles the create, creation, catalog, and verify
// routes for the proposals resource. It returns true when a response was written.
func (r *Reloader) serveProposalSpecialRoutes(w http.ResponseWriter, req *http.Request, state *reloaderState, directory string) bool {
	if req.URL.Path == proposalsBasePath && req.Method == http.MethodPost {
		r.createProposal(w, req, state, directory)
		return true
	}
	if req.URL.Path == proposalsBasePath+"/creation" {
		r.serveCreation(w, req, state, true)
		return true
	}
	if strings.HasPrefix(req.URL.Path, proposalsBasePath+"/") && strings.HasSuffix(req.URL.Path, "/catalog") {
		id := strings.TrimSuffix(strings.TrimPrefix(req.URL.Path, proposalsBasePath+"/"), "/catalog")
		r.serveCreationCatalog(w, req, state, directory, id)
		return true
	}
	if strings.HasPrefix(req.URL.Path, proposalsBasePath+"/") && strings.HasSuffix(req.URL.Path, "/verify") && req.Method == http.MethodPost {
		if req.ContentLength != 0 || len(req.TransferEncoding) != 0 {
			http.Error(w, "verification does not accept a request body", http.StatusBadRequest)
			return true
		}
		id := strings.TrimSuffix(strings.TrimPrefix(req.URL.Path, proposalsBasePath+"/"), "/verify")
		r.verifyProposal(w, state, directory, id)
		return true
	}
	return false
}

// serveProposalListGetDelete responds to GET list, GET/DELETE by id, and returns
// 405 for anything else. It returns true when a response was written.
func (r *Reloader) serveProposalListGetDelete(w http.ResponseWriter, req *http.Request, state *reloaderState, directory string) bool {
	if req.URL.Path == proposalsBasePath && req.Method == http.MethodGet {
		ids, err := modelconfig.ListProposals(directory)
		if err != nil {
			writeProposalError(w, err)
			return true
		}
		if r.state.Load() != state {
			http.Error(w, managementConfigChangedMsg, http.StatusServiceUnavailable)
			return true
		}
		writeJSON(r.logger, w, http.StatusOK, struct {
			IDs []string `json:"ids"`
		}{IDs: ids})
		return true
	}
	if strings.HasPrefix(req.URL.Path, proposalsBasePath+"/") && (req.Method == http.MethodGet || req.Method == http.MethodDelete) {
		id := strings.TrimPrefix(req.URL.Path, proposalsBasePath+"/")
		return r.serveProposalGetDelete(w, req, state, directory, id)
	}
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	return true
}

// serveProposalGetDelete loads or deletes a single proposal by id. It returns
// true when a response was written. The lifecycle lock and stale-state recheck
// are held for the duration of the delete operation.
func (r *Reloader) serveProposalGetDelete(w http.ResponseWriter, req *http.Request, state *reloaderState, directory, id string) bool {
	if req.Method == http.MethodGet {
		result, err := modelconfig.LoadStoredProposal(directory, id)
		if err != nil {
			writeProposalError(w, err)
			return true
		}
		if r.state.Load() != state {
			http.Error(w, managementConfigChangedMsg, http.StatusServiceUnavailable)
			return true
		}
		writeJSON(r.logger, w, http.StatusOK, result)
		return true
	}
	r.lifecycleMu.Lock()
	if r.stopped || r.state.Load() != state {
		r.lifecycleMu.Unlock()
		http.Error(w, managementConfigChangedMsg, http.StatusServiceUnavailable)
		return true
	}
	err := modelconfig.DeleteProposal(directory, id)
	r.lifecycleMu.Unlock()
	if err != nil {
		writeProposalError(w, err)
		return true
	}
	w.WriteHeader(http.StatusNoContent)
	return true
}

func (r *Reloader) verifyProposal(w http.ResponseWriter, state *reloaderState, directory, id string) {
	stored, err := modelconfig.LoadStoredProposal(directory, id)
	if err != nil {
		writeProposalError(w, err)
		return
	}
	if proposal, ok := stored.(modelconfig.CreationProposal); ok {
		r.verifyCreationProposal(w, state, directory, proposal)
		return
	}
	proposal, ok := stored.(modelconfig.Proposal)
	if !ok {
		writeProposalError(w, modelconfig.ErrProposalStore)
		return
	}
	target, ok := state.management.Targets[proposal.TargetID]
	if !ok {
		http.Error(w, "proposal target changed", http.StatusConflict)
		return
	}
	preview, err := modelconfig.RevalidateProposalTarget(target.Directory, target.Filename, proposal.TargetID, proposal)
	if err != nil {
		switch {
		case errors.Is(err, modelconfig.ErrProposalTarget),
			errors.Is(err, modelconfig.ErrProposalStale),
			errors.Is(err, modelconfig.ErrProposalReview):
			http.Error(w, err.Error(), http.StatusConflict)
		default:
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		return
	}
	if r.state.Load() != state {
		http.Error(w, managementConfigChangedMsg, http.StatusServiceUnavailable)
		return
	}
	writeJSON(r.logger, w, http.StatusOK, struct {
		ID         string `json:"id"`
		Status     string `json:"status"`
		Revision   string `json:"revision"`
		Validation string `json:"validation"`
	}{ID: proposal.ID, Status: "matched_when_checked", Revision: preview.Revision, Validation: preview.Validation})
}

func (r *Reloader) verifyCreationProposal(w http.ResponseWriter, state *reloaderState, directory string, proposal modelconfig.CreationProposal) {
	target, ok := state.management.Targets[proposal.TargetID]
	if !ok {
		http.Error(w, "proposal target changed", http.StatusConflict)
		return
	}
	r.lifecycleMu.Lock()
	if r.stopped || r.state.Load() != state || r.ingestion == nil {
		r.lifecycleMu.Unlock()
		http.Error(w, "management configuration changed or ingestion unavailable", http.StatusServiceUnavailable)
		return
	}
	preview, err := verifyRecordedCreation(r.ingestion.jobs, r.ingestion.config.Directory, directory, target, proposal.TargetID, proposal.ID)
	r.lifecycleMu.Unlock()
	if err != nil {
		switch {
		case errors.Is(err, modelconfig.ErrProposalTarget), errors.Is(err, modelconfig.ErrProposalStale),
			errors.Is(err, modelconfig.ErrProposalReview), errors.Is(err, modelconfig.ErrProposalArtifact),
			errors.Is(err, ingestion.ErrArtifactChanged), errors.Is(err, ingestion.ErrArtifactUnavailable),
			errors.Is(err, ingestion.ErrArtifactRecord), errors.Is(err, ingestion.ErrRecordNotFound):
			http.Error(w, "proposal no longer matches", http.StatusConflict)
		default:
			writeProposalError(w, err)
		}
		return
	}
	if r.state.Load() != state {
		http.Error(w, managementConfigChangedMsg, http.StatusServiceUnavailable)
		return
	}
	writeJSON(r.logger, w, http.StatusOK, struct {
		ID         string `json:"id"`
		Status     string `json:"status"`
		Revision   string `json:"revision"`
		Validation string `json:"validation"`
	}{ID: proposal.ID, Status: "matched_when_checked", Revision: preview.Preview.Revision, Validation: preview.Preview.Validation})
}

func separateProposalDirectory(directory string, targets map[string]config.ManagementTarget) bool {
	for _, target := range targets {
		if !separateDirectories(directory, target.Directory) {
			return false
		}
	}
	return true
}

func separateDirectories(first, second string) bool {
	a, err := filepath.EvalSymlinks(first)
	if err != nil {
		return false
	}
	b, err := filepath.EvalSymlinks(second)
	if err != nil || pathWithin(a, b) || pathWithin(b, a) {
		return false
	}
	aInfo, err := os.Stat(a)
	if err != nil {
		return false
	}
	bInfo, err := os.Stat(b)
	return err == nil && aInfo.IsDir() && bInfo.IsDir() && !os.SameFile(aInfo, bInfo)
}

func pathWithin(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// readManagementJSONBody guards and reads the request body for management JSON
// requests. It enforces the application/json content type, rejects a non-empty
// Content-Encoding header, reads at most maxManagementRequest+1 bytes, and
// writes the exact HTTP errors used by the proposal and creation-catalog
// handlers. The returned bool is false when an error response was already
// written and the caller must return. validateManagementJSON is intentionally
// not applied here; callers apply it in their own positions/conditions.
func readManagementJSONBody(w http.ResponseWriter, req *http.Request) ([]byte, bool) {
	contentType, _, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
	if err != nil || contentType != "application/json" || req.Header.Get("Content-Encoding") != "" {
		http.Error(w, "JSON request required", http.StatusUnsupportedMediaType)
		return nil, false
	}
	if req.Body == nil {
		http.Error(w, "request body required", http.StatusBadRequest)
		return nil, false
	}
	data, err := io.ReadAll(io.LimitReader(req.Body, maxManagementRequest+1))
	if err != nil {
		http.Error(w, "request could not be read", http.StatusBadRequest)
		return nil, false
	}
	if len(data) > maxManagementRequest {
		http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		return nil, false
	}
	return data, true
}

func (r *Reloader) createProposal(w http.ResponseWriter, req *http.Request, state *reloaderState, directory string) {
	data, ok := readManagementJSONBody(w, req)
	if !ok {
		return
	}
	if err := validateManagementJSON(data); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	var input struct {
		TargetID string                      `json:"target_id"`
		ModelID  string                      `json:"model_id"`
		Revision string                      `json:"revision"`
		Settings modelconfig.CommandSettings `json:"settings"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	digest, err := hex.DecodeString(input.Revision)
	if err != nil || len(digest) != 32 || input.Revision != strings.ToLower(input.Revision) {
		http.Error(w, "invalid revision", http.StatusBadRequest)
		return
	}
	target, ok := state.management.Targets[input.TargetID]
	if !ok {
		http.NotFound(w, req)
		return
	}
	preview, err := modelconfig.PreviewTarget(target.Directory, target.Filename, input.TargetID, input.ModelID, input.Settings)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if input.Revision != preview.Revision {
		http.Error(w, "source revision changed", http.StatusConflict)
		return
	}
	proposal, err := modelconfig.NewProposal(target.Directory, target.Filename, preview, input.Settings)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	r.lifecycleMu.Lock()
	if r.stopped || r.state.Load() != state {
		r.lifecycleMu.Unlock()
		http.Error(w, managementConfigChangedMsg, http.StatusServiceUnavailable)
		return
	}
	err = modelconfig.SaveProposal(directory, proposal)
	r.lifecycleMu.Unlock()
	if err != nil {
		writeProposalError(w, err)
		return
	}
	writeJSON(r.logger, w, http.StatusCreated, proposal)
}

func writeProposalError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, modelconfig.ErrProposalNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
	case errors.Is(err, modelconfig.ErrProposalStoreFull):
		http.Error(w, err.Error(), http.StatusConflict)
	default:
		http.Error(w, "proposal store unavailable or invalid; operation may not be durable", http.StatusServiceUnavailable)
	}
}

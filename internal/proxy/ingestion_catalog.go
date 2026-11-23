package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
)

const catalogUnavailableMsg = "catalog unavailable"

// parseManagementJSONInput validates the management request body and decodes
// the backend_id field. It returns the decoded input and false when an error
// response was already written and the caller must return.
func parseManagementJSONInput(data []byte, w http.ResponseWriter) (struct {
	BackendID string `json:"backend_id"`
}, bool) {
	var input struct {
		BackendID string `json:"backend_id"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if validateManagementJSON(data) != nil || decoder.Decode(&input) != nil || input.BackendID == "" {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return input, false
	}
	return input, true
}

// loadCreationProposal loads the stored proposal for the given id and requires
// it to be a CreationProposal. It returns the proposal and false when an error
// response was already written and the caller must return.
func loadCreationProposal(directory, id string, w http.ResponseWriter) (*modelconfig.CreationProposal, bool) {
	stored, err := modelconfig.LoadStoredProposal(directory, id)
	if err != nil {
		writeProposalError(w, err)
		return nil, false
	}
	proposal, ok := stored.(modelconfig.CreationProposal)
	if !ok {
		http.Error(w, "creation proposal required", http.StatusBadRequest)
		return nil, false
	}
	return &proposal, true
}

// stateChanged reports whether the management configuration changed while the
// request was in flight: the reloader is stopped or the current state differs
// from the snapshot the request was bound to.
func (r *Reloader) stateChanged(state *reloaderState) bool {
	r.lifecycleMu.Lock()
	unavailable := r.stopped || r.state.Load() != state
	r.lifecycleMu.Unlock()
	return unavailable
}

// fetchCatalogSnapshot obtains a bounded catalog snapshot. The context already
// carries the catalog timeout budget. It returns the response body and false
// when an error response was already written and the caller must return.
func fetchCatalogSnapshot(ctx context.Context, handler *Handler, w http.ResponseWriter) ([]byte, bool) {
	response, _, err := handler.modelCatalog(ctx, nil)
	if err != nil {
		http.Error(w, catalogUnavailableMsg, http.StatusBadGateway)
		return nil, false
	}
	if ctx.Err() != nil {
		http.Error(w, "catalog check timed out or was canceled", http.StatusServiceUnavailable)
		return nil, false
	}
	return response.Body, true
}

// matchCatalogStatus reports whether the owner-prefixed public model ID is
// listed in the aggregated catalog snapshot. It returns the status and false
// when an error response was already written and the caller must return.
func matchCatalogStatus(body []byte, prefix string, proposal *modelconfig.CreationProposal, w http.ResponseWriter) (string, bool) {
	var catalog struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &catalog); err != nil {
		http.Error(w, catalogUnavailableMsg, http.StatusBadGateway)
		return "", false
	}
	publicID := prefix + proposal.ModelID
	status := "not_listed_when_checked"
	for _, model := range catalog.Data {
		if model.ID == publicID {
			status = "listed_when_checked"
			break
		}
	}
	return status, true
}

func (r *Reloader) serveCreationCatalog(w http.ResponseWriter, req *http.Request, state *reloaderState, directory, id string) {
	if req.URL.RawQuery != "" || req.URL.ForceQuery || !sameIngestionOrigin(req) {
		http.Error(w, "invalid request origin or query", http.StatusForbidden)
		return
	}
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	data, ok := readManagementJSONBody(w, req)
	if !ok {
		return
	}
	input, ok := parseManagementJSONInput(data, w)
	if !ok {
		return
	}
	proposal, ok := loadCreationProposal(directory, id, w)
	if !ok {
		return
	}
	handler, ok := state.handler.(*Handler)
	if !ok || handler.catalog == nil {
		http.Error(w, catalogUnavailableMsg, http.StatusServiceUnavailable)
		return
	}
	prefix, ok := handler.catalog.BackendPrefix(input.BackendID)
	if !ok {
		http.Error(w, "backend not found", http.StatusNotFound)
		return
	}
	if r.stateChanged(state) {
		http.Error(w, managementConfigChangedMsg, http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(req.Context(), handler.catalogTimeout)
	defer cancel()
	body, ok := fetchCatalogSnapshot(ctx, handler, w)
	if !ok {
		return
	}
	status, ok := matchCatalogStatus(body, prefix, proposal, w)
	if !ok {
		return
	}
	if r.stateChanged(state) {
		return
	}
	writeJSON(r.logger, w, http.StatusOK, struct {
		ProposalID              string    `json:"proposal_id"`
		BackendID               string    `json:"backend_id"`
		PublicModelID           string    `json:"public_model_id"`
		Catalog                 string    `json:"catalog"`
		ObservedAt              time.Time `json:"observed_at"`
		UpstreamFreshness       string    `json:"upstream_freshness"`
		SelectedBackendRelation string    `json:"selected_backend_relation"`
		Loadability             string    `json:"loadability"`
		LoadedState             string    `json:"loaded_state"`
		EffectiveSettings       string    `json:"effective_settings"`
	}{
		ProposalID: proposal.ID, BackendID: input.BackendID, PublicModelID: prefix + proposal.ModelID,
		Catalog: status, ObservedAt: time.Now().UTC(), UpstreamFreshness: "unknown",
		SelectedBackendRelation: "unknown", Loadability: "unknown", LoadedState: "unknown", EffectiveSettings: "unknown",
	})
}

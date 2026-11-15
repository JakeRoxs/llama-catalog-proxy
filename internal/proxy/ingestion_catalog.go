package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
)

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
	contentType, _, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
	if err != nil || contentType != "application/json" || req.Header.Get("Content-Encoding") != "" {
		http.Error(w, "JSON request required", http.StatusUnsupportedMediaType)
		return
	}
	if req.Body == nil {
		http.Error(w, "request body required", http.StatusBadRequest)
		return
	}
	data, err := io.ReadAll(io.LimitReader(req.Body, maxManagementRequest+1))
	if err != nil {
		http.Error(w, "request could not be read", http.StatusBadRequest)
		return
	}
	if len(data) > maxManagementRequest {
		http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		return
	}
	var input struct {
		BackendID string `json:"backend_id"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if validateManagementJSON(data) != nil || decoder.Decode(&input) != nil || input.BackendID == "" {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	stored, err := modelconfig.LoadStoredProposal(directory, id)
	if err != nil {
		writeProposalError(w, err)
		return
	}
	proposal, ok := stored.(modelconfig.CreationProposal)
	if !ok {
		http.Error(w, "creation proposal required", http.StatusBadRequest)
		return
	}
	handler, ok := state.handler.(*Handler)
	if !ok || handler.catalog == nil {
		http.Error(w, "catalog unavailable", http.StatusServiceUnavailable)
		return
	}
	prefix, ok := handler.catalog.BackendPrefix(input.BackendID)
	if !ok {
		http.Error(w, "backend not found", http.StatusNotFound)
		return
	}
	r.lifecycleMu.Lock()
	unavailable := r.stopped || r.state.Load() != state
	r.lifecycleMu.Unlock()
	if unavailable {
		http.Error(w, "management configuration changed", http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(req.Context(), handler.catalogTimeout)
	defer cancel()
	response, _, err := handler.modelCatalog(ctx, nil)
	if err != nil {
		http.Error(w, "catalog unavailable", http.StatusBadGateway)
		return
	}
	if ctx.Err() != nil {
		http.Error(w, "catalog check timed out or was canceled", http.StatusServiceUnavailable)
		return
	}
	var catalog struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body, &catalog); err != nil {
		http.Error(w, "catalog unavailable", http.StatusBadGateway)
		return
	}
	publicID := prefix + proposal.ModelID
	status := "not_listed_when_checked"
	for _, model := range catalog.Data {
		if model.ID == publicID {
			status = "listed_when_checked"
			break
		}
	}
	r.lifecycleMu.Lock()
	unavailable = r.stopped || r.state.Load() != state
	r.lifecycleMu.Unlock()
	if unavailable {
		http.Error(w, "management configuration changed", http.StatusServiceUnavailable)
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
		ProposalID: proposal.ID, BackendID: input.BackendID, PublicModelID: publicID,
		Catalog: status, ObservedAt: time.Now().UTC(), UpstreamFreshness: "unknown",
		SelectedBackendRelation: "unknown", Loadability: "unknown", LoadedState: "unknown", EffectiveSettings: "unknown",
	})
}

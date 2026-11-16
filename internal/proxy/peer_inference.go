package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/catalog"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/nodecontract"
)

func (r *Reloader) servePeerInference(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	select {
	case r.peerInferenceSlots <- struct{}{}:
		defer func() { <-r.peerInferenceSlots }()
	default:
		http.Error(w, "peer inference busy", http.StatusServiceUnavailable)
		return
	}
	state := r.state.Load()
	if state.peer == nil || !peerAuthorized(req, state, peerInference) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	id, path, instance, generation, err := peerTargetHeaders(req, state.peer.NodeID)
	if err != nil || instance != r.instanceID || generation != state.generation {
		http.Error(w, "peer target unavailable", http.StatusServiceUnavailable)
		return
	}
	owned := false
	for _, backend := range state.peer.LocalBackends {
		if backend == id.BackendID {
			owned = true
			break
		}
	}
	check := req.Clone(req.Context())
	check.URL.Path = path
	if !owned || !isInferenceRequest(check) || strings.ContainsAny(path, "?#") {
		http.Error(w, "peer target unavailable", http.StatusServiceUnavailable)
		return
	}
	contentType, params, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
	if err != nil || (contentType != "application/json" && contentType != "multipart/form-data") || req.Header.Get("Content-Encoding") != "" {
		http.Error(w, "JSON or multipart request required", http.StatusUnsupportedMediaType)
		return
	}
	handler, ok := state.handler.(*Handler)
	if !ok {
		http.Error(w, "peer target unavailable", http.StatusServiceUnavailable)
		return
	}
	if req.Body == nil {
		http.Error(w, "request body required", http.StatusBadRequest)
		return
	}
	if req.ContentLength > handler.maxJSONBody {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}
	body, err := io.ReadAll(io.LimitReader(req.Body, handler.maxJSONBody+1))
	if err != nil || int64(len(body)) > handler.maxJSONBody {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	var rewritten []byte
	aliases := handler.modelAliases(id.BackendID, id.LocalID)
	if contentType == "multipart/form-data" {
		requestedModel, ambiguous, parseErr := multipartPeerModel(body, params["boundary"], func(model string) bool {
			return model == id.LocalID || modelHasAlias(aliases, model)
		})
		if parseErr != nil || ambiguous || requestedModel == "" {
			err = errors.New("invalid peer model")
		} else {
			rewritten, err = rewriteMultipartModel(body, params["boundary"], requestedModel, requestedModel, handler.maxJSONBody)
		}
	} else {
		rewritten, err = verifyOwnerModelBody(body, id.LocalID, aliases...)
	}
	if err != nil {
		http.Error(w, "invalid peer model", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithCancel(req.Context())
	stop := context.AfterFunc(state.peerContext, cancel)
	defer func() { stop(); cancel() }()
	reportCtx, reportCancel := context.WithTimeout(ctx, peerReportTimeout)
	report, err := nodecontract.LocalReportFromCatalog(reportCtx, nodecontract.Source{
		NodeID: state.peer.NodeID, ProxyInstanceID: r.instanceID, ConfigGeneration: state.generation,
	}, []string{id.BackendID}, handler.catalog, time.Now(), state.cacheTTL)
	reportCancel()
	found := false
	if err == nil {
		for _, obs := range report.Observations {
			if obs.Identity == id && obs.State == "supported" && obs.Freshness == "fresh" {
				found = true
				break
			}
		}
	}
	if !found || r.state.Load() != state {
		http.Error(w, "peer target unavailable", http.StatusServiceUnavailable)
		return
	}
	ctx = context.WithValue(ctx, requestIDKey{}, generateRequestID())
	req = req.WithContext(ctx)
	req.URL.Path, req.URL.RawPath, req.URL.RawQuery = path, "", ""
	accept := req.Header.Get("Accept")
	originalContentType := req.Header.Get("Content-Type")
	addressHeaders := make(http.Header)
	for _, name := range []string{"X-Forwarded-For", "X-Real-IP"} {
		for _, value := range req.Header.Values(name) {
			addressHeaders.Add(name, value)
		}
	}
	req.Header = make(http.Header)
	req.Trailer = nil
	req.TransferEncoding = nil
	req.Header.Set("Content-Type", originalContentType)
	for name, values := range addressHeaders {
		req.Header[name] = values
	}
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	restoreRequestBody(req, rewritten)
	if !handler.reserveModelRequest(w, req, id.BackendID) {
		return
	}
	if handler.comfyUIWaitTimeout > 0 && handler.comfyUIJobs != nil && handler.sharesComfyUIBackend(id.BackendID) {
		defer handler.comfyUIJobs.endInference()
	}
	handler.proxies[id.BackendID].ServeHTTP(w, req)
}

func (h *Handler) modelAliases(backendID, localID string) []string {
	if h.catalog == nil {
		return nil
	}
	for _, backend := range h.catalog.BackendModels() {
		if backend.BackendID != backendID {
			continue
		}
		for _, model := range backend.Models {
			id, _ := model["id"].(string)
			if id == localID && !catalog.IsAliasModelRow(model) {
				return catalog.ModelAliases(model)
			}
		}
	}
	return nil
}

func verifyOwnerModelBody(body []byte, expected string, aliases ...string) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var object map[string]any
	if err := decoder.Decode(&object); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return nil, io.ErrUnexpectedEOF
	}
	scanner := json.NewDecoder(bytes.NewReader(body))
	if _, err := scanner.Token(); err != nil {
		return nil, err
	}
	count := 0
	for scanner.More() {
		token, err := scanner.Token()
		if err != nil {
			return nil, err
		}
		key, ok := token.(string)
		if !ok {
			return nil, io.ErrUnexpectedEOF
		}
		var raw json.RawMessage
		if err := scanner.Decode(&raw); err != nil {
			return nil, err
		}
		if strings.EqualFold(key, "model") {
			if key != "model" {
				return nil, io.ErrUnexpectedEOF
			}
			count++
		}
	}
	model, ok := object["model"].(string)
	if !ok || (model != expected && !modelHasAlias(aliases, model)) || count != 1 {
		return nil, io.ErrUnexpectedEOF
	}
	return json.Marshal(object)
}

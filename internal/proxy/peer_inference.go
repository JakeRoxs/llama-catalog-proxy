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

// peerTargetUnavailable is the error message returned when the inferred peer
// target is unauthorized, not owned, or no longer supported. Declared once so
// the literal is not duplicated (go:S1192).
const peerTargetUnavailable = "peer target unavailable"

type peerAddressHeadersContextKey struct{}

type preparedPeerInference struct {
	id        nodecontract.ModelIdentity
	path      string
	handler   *Handler
	rewritten []byte
}

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
	prepared, ok := preparePeerInference(w, req, state, r.instanceID)
	if !ok {
		return
	}
	ctx, cancel := context.WithCancel(req.Context())
	stop := context.AfterFunc(state.peerContext, cancel)
	defer func() { stop(); cancel() }()
	if !r.peerInferenceTargetFresh(ctx, state, prepared.id, prepared.handler) || r.state.Load() != state {
		http.Error(w, peerTargetUnavailable, http.StatusServiceUnavailable)
		return
	}
	forwardPeerInference(w, req, ctx, prepared)
}

func preparePeerInference(w http.ResponseWriter, req *http.Request, state *reloaderState, instanceID string) (preparedPeerInference, bool) {
	if state.peer == nil || !peerAuthorized(req, state, peerInference) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return preparedPeerInference{}, false
	}
	id, path, instance, generation, err := peerTargetHeaders(req, state.peer.NodeID)
	if err != nil || instance != instanceID || generation != state.generation {
		http.Error(w, peerTargetUnavailable, http.StatusServiceUnavailable)
		return preparedPeerInference{}, false
	}
	if !peerInferenceTargetOwned(req, state, id, path) {
		http.Error(w, peerTargetUnavailable, http.StatusServiceUnavailable)
		return preparedPeerInference{}, false
	}
	handler, ok := state.handler.(*Handler)
	if !ok {
		http.Error(w, peerTargetUnavailable, http.StatusServiceUnavailable)
		return preparedPeerInference{}, false
	}
	body, contentType, params, ok := readPeerInferenceBody(w, req, handler)
	if !ok {
		return preparedPeerInference{}, false
	}
	rewritten, err := rewritePeerInferenceBody(body, contentType, params, handler, id)
	if err != nil {
		http.Error(w, "invalid peer model", http.StatusBadRequest)
		return preparedPeerInference{}, false
	}
	return preparedPeerInference{id: id, path: path, handler: handler, rewritten: rewritten}, true
}

func peerInferenceTargetOwned(req *http.Request, state *reloaderState, id nodecontract.ModelIdentity, path string) bool {
	for _, backend := range state.peer.LocalBackends {
		if backend == id.BackendID {
			check := req.Clone(req.Context())
			check.URL.Path = path
			return isInferenceRequest(check) && !strings.ContainsAny(path, "?#")
		}
	}
	return false
}

func readPeerInferenceBody(w http.ResponseWriter, req *http.Request, handler *Handler) ([]byte, string, map[string]string, bool) {
	contentType, params, err := mime.ParseMediaType(req.Header.Get(contentTypeHeader))
	if err != nil || (contentType != "application/json" && contentType != "multipart/form-data") || req.Header.Get("Content-Encoding") != "" {
		http.Error(w, "JSON or multipart request required", http.StatusUnsupportedMediaType)
		return nil, "", nil, false
	}
	if req.Body == nil {
		http.Error(w, "request body required", http.StatusBadRequest)
		return nil, "", nil, false
	}
	if req.ContentLength > handler.maxJSONBody {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return nil, "", nil, false
	}
	body, err := io.ReadAll(io.LimitReader(req.Body, handler.maxJSONBody+1))
	if err != nil || int64(len(body)) > handler.maxJSONBody {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return nil, "", nil, false
	}
	return body, contentType, params, true
}

func rewritePeerInferenceBody(body []byte, contentType string, params map[string]string, handler *Handler, id nodecontract.ModelIdentity) ([]byte, error) {
	aliases := handler.modelAliases(id.BackendID, id.LocalID)
	if contentType == "multipart/form-data" {
		requestedModel, ambiguous, parseErr := multipartPeerModel(body, params["boundary"], func(model string) bool {
			return model == id.LocalID || modelHasAlias(aliases, model)
		})
		if parseErr != nil || ambiguous || requestedModel == "" {
			return nil, errors.New("invalid peer model")
		}
		return rewriteMultipartModel(body, params["boundary"], requestedModel, requestedModel, handler.maxJSONBody)
	}
	return verifyOwnerModelBody(body, id.LocalID, aliases...)
}

func (r *Reloader) peerInferenceTargetFresh(ctx context.Context, state *reloaderState, id nodecontract.ModelIdentity, handler *Handler) bool {
	reportCtx, reportCancel := context.WithTimeout(ctx, peerReportTimeout)
	defer reportCancel()
	report, err := nodecontract.LocalReportFromCatalog(reportCtx, nodecontract.Source{
		NodeID: state.peer.NodeID, ProxyInstanceID: r.instanceID, ConfigGeneration: state.generation,
	}, []string{id.BackendID}, handler.catalog, time.Now(), state.cacheTTL)
	found := false
	if err == nil {
		for _, obs := range report.Observations {
			if obs.Identity == id && obs.State == "supported" && obs.Freshness == "fresh" {
				found = true
				break
			}
		}
	}
	return found
}

func forwardPeerInference(w http.ResponseWriter, req *http.Request, ctx context.Context, prepared preparedPeerInference) {
	ctx = context.WithValue(ctx, requestIDKey{}, generateRequestID())
	ctx = context.WithValue(ctx, peerAddressHeadersContextKey{}, true)
	req = req.WithContext(ctx)
	req.URL.Path, req.URL.RawPath, req.URL.RawQuery = prepared.path, "", ""
	accept := req.Header.Get("Accept")
	originalContentType := req.Header.Get(contentTypeHeader)
	addressHeaders := make(http.Header)
	for _, name := range []string{"X-Forwarded-For", "X-Real-IP"} {
		for _, value := range req.Header.Values(name) {
			addressHeaders.Add(name, value)
		}
	}
	req.Header = make(http.Header)
	req.Trailer = nil
	req.TransferEncoding = nil
	req.Header.Set(contentTypeHeader, originalContentType)
	for name, values := range addressHeaders {
		req.Header[name] = values
	}
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	restoreRequestBody(req, prepared.rewritten)
	if !prepared.handler.reserveModelRequest(w, req, prepared.id.BackendID) {
		return
	}
	if prepared.handler.comfyUIWaitTimeout > 0 && prepared.handler.comfyUIJobs != nil &&
		prepared.handler.sharesComfyUIBackend(prepared.id.BackendID) {
		defer prepared.handler.comfyUIJobs.endInference()
	}
	prepared.handler.dispatchBackend(w, req, prepared.id.BackendID)
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

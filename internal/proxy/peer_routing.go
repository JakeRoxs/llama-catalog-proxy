package proxy

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httputil"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/nodecontract"
)

const (
	peerBackendHeader    = "X-Catalog-Peer-Backend"
	peerModelHeader      = "X-Catalog-Peer-Model"
	peerPathHeader       = "X-Catalog-Peer-Path"
	peerInstanceHeader   = "X-Catalog-Peer-Instance"
	peerGenerationHeader = "X-Catalog-Peer-Generation"
)

// S1192: duplicated string literals replaced by package-level constants.
// Names are unique within the proxy package: contentTypeHeader and
// contentTypeJSON are already declared elsewhere in the package.
const (
	applicationJSONMediaType   = "application/json"
	multipartFormDataMediaType = "multipart/form-data"
	peerModelUnavailableMsg    = "peer model unavailable"
	httpContentTypeHeader      = "Content-Type"
	xForwardedForHeader        = "X-Forwarded-For"
)

func (h *Handler) servePeerModelRoute(w http.ResponseWriter, r *http.Request, info *routeInfo) bool {
	if strings.Contains(r.URL.Path, peerModelPrefix) {
		writeJSONError(h.logger, w, http.StatusBadRequest, "peer model IDs require JSON inference requests")
		return true
	}
	if h.rejectPeerModelQuery(w, r) {
		return true
	}
	if isNoBody(r) {
		return false
	}
	proceed, handled := h.peerContentEncoding(w, r)
	if handled {
		return true
	}
	if !proceed {
		return false
	}
	mediaType, params, proceed, handled := h.peerContentType(w, r)
	if handled {
		return true
	}
	if !proceed {
		return false
	}
	body, handled := h.readPeerRequestBody(w, r)
	if handled {
		return true
	}
	model, object, ambiguous, handled := h.extractPeerModel(w, r, body, mediaType, params)
	if handled {
		return true
	}
	if ambiguous {
		writeJSONError(h.logger, w, http.StatusBadRequest, "ambiguous peer model")
		return true
	}
	if model == "" {
		return false
	}
	id, client, requestedLocalID, source, matched, handled := h.resolvePeerModel(w, r, model)
	if handled {
		return true
	}
	if !matched {
		return false
	}
	rewritten, handled := h.rewritePeerModelBody(w, r, body, mediaType, params, model, object, requestedLocalID)
	if handled {
		return true
	}
	restoreRequestBody(r, rewritten)
	*info = routeInfo{kind: "peer"}
	h.proxyPeerInference(w, r, client, id, source)
	return true
}

// isNoBody reports whether the request has no body to read.
func isNoBody(r *http.Request) bool {
	return r.Body == nil || r.Body == http.NoBody
}

// rejectPeerModelQuery rejects inference requests that carry a peer model ID
// in the query string. It returns true when a 400 error was written and the
// route is handled; false when the request may continue.
func (h *Handler) rejectPeerModelQuery(w http.ResponseWriter, r *http.Request) bool {
	if !isInferenceRequest(r) || r.URL.RawQuery == "" {
		return false
	}
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		writeJSONError(h.logger, w, http.StatusBadRequest, "invalid inference query")
		return true
	}
	for key, values := range query {
		if strings.EqualFold(key, "model") {
			for _, value := range values {
				if h.isPeerPublicModelID(value) {
					writeJSONError(h.logger, w, http.StatusBadRequest, "peer model IDs require JSON inference requests")
					return true
				}
			}
		}
	}
	return false
}

// peerContentEncoding handles the Content-Encoding header for peer routing.
// It returns (proceed, handled): proceed=true when the request may continue,
// handled=true when a 415 error was written and the route is handled. A
// non-inference request with an encoding is not handled and must fall through.
func (h *Handler) peerContentEncoding(w http.ResponseWriter, r *http.Request) (proceed, handled bool) {
	if r.Header.Get("Content-Encoding") == "" {
		return true, false
	}
	if isInferenceRequest(r) {
		writeJSONError(h.logger, w, http.StatusUnsupportedMediaType, "encoded inference requests are unavailable with peer routing")
		return false, true
	}
	return false, false
}

// peerContentType resolves and validates the request content type for peer
// inference routing. It returns the media type and params when valid
// (proceed=true), or reports a 415 error and returns handled=true for
// inference requests with an invalid or unsupported content type. Non-inference
// requests with an invalid or non-JSON/multipart content type fall through
// (proceed=false, handled=false).
func (h *Handler) peerContentType(w http.ResponseWriter, r *http.Request) (mediaType string, params map[string]string, proceed, handled bool) {
	mediaType, params, err := mime.ParseMediaType(r.Header.Get(httpContentTypeHeader))
	if r.Header.Get(httpContentTypeHeader) == "" {
		mediaType, err = applicationJSONMediaType, nil
	}
	if err != nil {
		if isInferenceRequest(r) {
			writeJSONError(h.logger, w, http.StatusUnsupportedMediaType, "invalid inference content type")
			return "", nil, false, true
		}
		return "", nil, false, false
	}
	if mediaType != applicationJSONMediaType && mediaType != multipartFormDataMediaType {
		if isInferenceRequest(r) {
			writeJSONError(h.logger, w, http.StatusUnsupportedMediaType, "unsupported inference content type with peer routing")
			return "", nil, false, true
		}
		return "", nil, false, false
	}
	if mediaType == multipartFormDataMediaType && !isInferenceRequest(r) {
		return "", nil, false, false
	}
	return mediaType, params, true, false
}

// readPeerRequestBody reads and bounds the request body, restores the caller
// body, and reports whether the route is handled (a limit or read error was
// written). It returns the body bytes.
func (h *Handler) readPeerRequestBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	if r.ContentLength > h.maxJSONBody {
		writeJSONError(h.logger, w, http.StatusRequestEntityTooLarge, "request body exceeds the configured limit")
		return nil, true
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, h.maxJSONBody+1))
	_ = r.Body.Close()
	if err != nil {
		writeJSONError(h.logger, w, http.StatusBadRequest, "request body could not be read")
		return nil, true
	}
	if int64(len(body)) > h.maxJSONBody {
		writeJSONError(h.logger, w, http.StatusRequestEntityTooLarge, "request body exceeds the configured limit")
		return nil, true
	}
	restoreRequestBody(r, body)
	return body, false
}

// extractPeerModel parses the model from the request body based on the media
// type. It returns the model, the decoded JSON object (nil for multipart),
// whether the model is ambiguous, and whether the route is handled (an error
// was written). The caller body is already restored; non-inference JSON parse
// failures fall through unhandled.
func (h *Handler) extractPeerModel(w http.ResponseWriter, r *http.Request, body []byte, mediaType string, params map[string]string) (string, map[string]any, bool, bool) {
	if mediaType == multipartFormDataMediaType {
		model, ambiguous, err := multipartPeerModel(body, params["boundary"], h.isPeerPublicModelID)
		if err != nil {
			writeJSONError(h.logger, w, http.StatusBadRequest, "invalid multipart request")
			return "", nil, false, true
		}
		return model, nil, ambiguous, false
	}
	model, object, ambiguous, err := jsonPeerModel(body, h.isPeerPublicModelID)
	if err != nil {
		if isInferenceRequest(r) {
			writeJSONError(h.logger, w, http.StatusBadRequest, "invalid JSON inference request")
			return "", nil, false, true
		}
		return "", nil, false, false
	}
	return model, object, ambiguous, false
}

// resolvePeerModel resolves a peer model ID, validates the inference path,
// freshness, and route grants. It returns the identity, client, requested local
// ID, source, whether the model was matched, and whether the route is handled
// (an error was written). Unmatched models fall through unhandled.
func (h *Handler) resolvePeerModel(w http.ResponseWriter, r *http.Request, model string) (nodecontract.ModelIdentity, *peerClient, string, nodecontract.Source, bool, bool) {
	if !isInferenceRequest(r) || r.URL.RawQuery != "" {
		writeJSONError(h.logger, w, http.StatusBadRequest, "peer model requires a supported inference path without a query")
		return nodecontract.ModelIdentity{}, nil, "", nodecontract.Source{}, false, true
	}
	id, client, requestedLocalID, matched, err := h.resolvePeerPublicModel(model)
	if !matched {
		return nodecontract.ModelIdentity{}, nil, "", nodecontract.Source{}, false, false
	}
	if err != nil {
		writeJSONError(h.logger, w, http.StatusBadRequest, "invalid peer model ID")
		return nodecontract.ModelIdentity{}, nil, "", nodecontract.Source{}, false, true
	}
	if id.OwnerNodeID == "" {
		writeJSONError(h.logger, w, http.StatusServiceUnavailable, peerModelUnavailableMsg)
		return nodecontract.ModelIdentity{}, nil, "", nodecontract.Source{}, false, true
	}
	source, fresh := h.peerCache.freshModel(id, time.Now())
	if client == nil || !client.routeInference || !fresh {
		writeJSONError(h.logger, w, http.StatusServiceUnavailable, peerModelUnavailableMsg)
		return nodecontract.ModelIdentity{}, nil, "", nodecontract.Source{}, false, true
	}
	return id, client, requestedLocalID, source, true, false
}

// rewritePeerModelBody rewrites the request body with the resolved local model
// ID. It returns the rewritten body and whether the route is handled (an
// encoding error was written). The caller restores the request body before
// proxying.
func (h *Handler) rewritePeerModelBody(w http.ResponseWriter, r *http.Request, body []byte, mediaType string, params map[string]string, model string, object map[string]any, requestedLocalID string) ([]byte, bool) {
	var rewritten []byte
	var err error
	if mediaType == multipartFormDataMediaType {
		rewritten, err = rewriteMultipartModel(body, params["boundary"], model, requestedLocalID, h.maxJSONBody)
	} else {
		object["model"] = requestedLocalID
		rewritten, err = json.Marshal(object)
	}
	if err != nil {
		writeJSONError(h.logger, w, http.StatusBadRequest, "request body could not be encoded")
		return nil, true
	}
	return rewritten, false
}

func jsonPeerModel(body []byte, isPeerModel func(string) bool) (string, map[string]any, bool, error) {
	var object map[string]any
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&object); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return "", nil, false, errors.New("invalid JSON")
	}
	scanner := json.NewDecoder(bytes.NewReader(body))
	if _, err := scanner.Token(); err != nil {
		return "", nil, false, err
	}
	count := 0
	found := ""
	peerFound := false
	for scanner.More() {
		token, err := scanner.Token()
		if err != nil {
			return "", nil, false, err
		}
		key, ok := token.(string)
		if !ok {
			return "", nil, false, errors.New("invalid JSON object key")
		}
		var raw json.RawMessage
		if err := scanner.Decode(&raw); err != nil {
			return "", nil, false, err
		}
		if strings.EqualFold(key, "model") {
			count++
			var value string
			if json.Unmarshal(raw, &value) == nil {
				if found == "" {
					found = value
				}
				if isPeerModel(value) {
					found = value
					peerFound = true
					if key != "model" {
						count++
					}
				}
			}
		}
	}
	return found, object, peerFound && count != 1, nil
}

func multipartPeerModel(body []byte, boundary string, isPeerModel func(string) bool) (string, bool, error) {
	if boundary == "" {
		return "", false, errors.New("missing multipart boundary")
	}
	reader := multipart.NewReader(bytes.NewReader(body), boundary)
	count := 0
	found := ""
	peerFound := false
	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", false, err
		}
		if strings.EqualFold(part.FormName(), "model") {
			count++
			value, err := io.ReadAll(io.LimitReader(part, 1025))
			if err != nil {
				return "", false, err
			}
			model := string(value)
			if found == "" {
				found = model
			}
			if isPeerModel(model) {
				found = model
				peerFound = true
				if part.FormName() != "model" {
					count++
				}
			}
		}
		_ = part.Close()
	}
	return found, peerFound && count != 1, nil
}

func (h *Handler) isPeerPublicModelID(publicID string) bool {
	if strings.HasPrefix(publicID, peerModelPrefix) {
		return true
	}
	owner, model, ok := strings.Cut(publicID, "/")
	if !ok || model == "" {
		return false
	}
	if client := h.peerClients[owner]; client != nil {
		return client.routeInference
	}
	_, reserved := h.peerRouteAliases[owner]
	return reserved
}

// resolvePeerPublicModel dispatches a public model ID to the canonical
// "@peer/owner/backend/localID" resolver or the legacy "owner/localID"
// resolver based on the ID prefix. The result tuple
// (identity, client, requestedLocalID, matched, error) is preserved exactly:
// malformed canonical IDs are matched+error, unavailable peer IDs remain
// matched, a nil peer cache on the legacy route returns unmatched, and
// unknown/cross-scope owners fall back to peerRouteAliases.
func (h *Handler) resolvePeerPublicModel(publicID string) (nodecontract.ModelIdentity, *peerClient, string, bool, error) {
	if strings.HasPrefix(publicID, peerModelPrefix) {
		return h.resolvePeerCanonicalModel(publicID)
	}
	return h.resolvePeerLegacyModel(publicID)
}

// resolvePeerCanonicalModel resolves a canonical "@peer/owner/backend/localID"
// public model ID through the peer cache. Malformed IDs are reported as
// matched with an error so the caller surfaces a 400 rather than falling
// through to legacy resolution. Unavailable peer IDs remain matched; the
// caller validates freshness and route grants downstream.
func (h *Handler) resolvePeerCanonicalModel(publicID string) (nodecontract.ModelIdentity, *peerClient, string, bool, error) {
	id, err := parsePeerPublicID(publicID)
	if err != nil {
		return nodecontract.ModelIdentity{}, nil, "", true, err
	}
	requestedLocalID := id.LocalID
	if h.peerCache != nil {
		if resolved, found := h.peerCache.modelForRoute(id.OwnerNodeID, id.BackendID, requestedLocalID); found {
			id = resolved
		}
	}
	return id, h.peerClients[id.OwnerNodeID], requestedLocalID, true, nil
}

// resolvePeerLegacyModel resolves a legacy "owner/localID" public model ID. A
// nil peer cache leaves the route unmatched. Unknown owners fall back to the
// peerRouteAliases reservation; known inference clients are resolved through
// the cached local-ID route identity when present, otherwise the client is
// returned with an unmatched identity.
func (h *Handler) resolvePeerLegacyModel(publicID string) (nodecontract.ModelIdentity, *peerClient, string, bool, error) {
	if h.peerCache == nil {
		return nodecontract.ModelIdentity{}, nil, "", false, nil
	}
	owner, requestedLocalID, ok := strings.Cut(publicID, "/")
	if !ok || requestedLocalID == "" {
		return nodecontract.ModelIdentity{}, nil, "", false, nil
	}
	client := h.peerClients[owner]
	if client == nil {
		_, reserved := h.peerRouteAliases[owner]
		return nodecontract.ModelIdentity{}, nil, requestedLocalID, reserved, nil
	}
	if !client.routeInference {
		return nodecontract.ModelIdentity{}, nil, "", false, nil
	}
	id, found := h.peerCache.modelForRoute(owner, "", requestedLocalID)
	if !found {
		return nodecontract.ModelIdentity{}, client, requestedLocalID, true, nil
	}
	return id, client, requestedLocalID, true, nil
}

// copyPartHeaders copies the Content-Disposition and Content-Type headers from a
// multipart part into a fresh MIME header, preserving header casing and values.
func copyPartHeaders(part *multipart.Part) textproto.MIMEHeader {
	headers := make(textproto.MIMEHeader)
	for _, name := range []string{"Content-Disposition", httpContentTypeHeader} {
		for _, value := range part.Header.Values(name) {
			headers.Add(name, value)
		}
	}
	return headers
}

// writeModelPart validates a single model part (value must equal expected) and
// writes the replacement to the destination part. It returns the read/write
// error or "invalid multipart model" on mismatch.
func writeModelPart(destination io.Writer, part *multipart.Part, expected, replacement string) error {
	value, err := io.ReadAll(io.LimitReader(part, 1025))
	if err != nil || part.FormName() != "model" || string(value) != expected {
		return errors.New("invalid multipart model")
	}
	_, err = io.WriteString(destination, replacement)
	return err
}

// processPart handles one multipart part: it clones the part headers, creates a
// destination part, rewrites the model field or copies the payload, enforces the
// body limit and closes the part. It returns the number of model fields found.
func processPart(part *multipart.Part, writer *multipart.Writer, output *bytes.Buffer, expected, replacement string, limit int64) (int, error) {
	headers := copyPartHeaders(part)
	destination, err := writer.CreatePart(headers)
	if err != nil {
		return 0, err
	}
	var increment int
	if strings.EqualFold(part.FormName(), "model") {
		increment = 1
		if err := writeModelPart(destination, part, expected, replacement); err != nil {
			return 0, err
		}
	} else if _, err := io.Copy(destination, part); err != nil {
		return 0, err
	}
	if int64(output.Len()) > limit {
		return 0, errJSONBodyTooLarge
	}
	if err := part.Close(); err != nil {
		return 0, err
	}
	return increment, nil
}

func rewriteMultipartModel(body []byte, boundary, expected, replacement string, limit int64) ([]byte, error) {
	reader := multipart.NewReader(bytes.NewReader(body), boundary)
	var output bytes.Buffer
	writer := multipart.NewWriter(&output)
	if err := writer.SetBoundary(boundary); err != nil {
		return nil, err
	}
	count := 0
	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		n, err := processPart(part, writer, &output, expected, replacement, limit)
		if err != nil {
			return nil, err
		}
		count += n
	}
	if count != 1 {
		return nil, errors.New("invalid multipart model")
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	if int64(output.Len()) > limit {
		return nil, errJSONBodyTooLarge
	}
	return output.Bytes(), nil
}

// newPeerInferenceProxy builds the ReverseProxy used to forward a peer
// inference request. It contains the Transport, FlushInterval, Rewrite
// closure, and ErrorHandler closure verbatim from the original inline
// construction.
func newPeerInferenceProxy(h *Handler, client *peerClient, id nodecontract.ModelIdentity, source nodecontract.Source, target *url.URL) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Transport:     client.inferenceTransport,
		FlushInterval: -1,
		Rewrite: func(req *httputil.ProxyRequest) {
			req.SetURL(target)
			req.Out.URL.Path, req.Out.URL.RawPath, req.Out.URL.RawQuery = "/peer/v1/inference", "", ""
			req.Out.Trailer = nil
			req.Out.TransferEncoding = nil
			req.Out.Header = copyPeerClientAddressHeaders(req.In)
			req.Out.Header.Set(httpContentTypeHeader, req.In.Header.Get(httpContentTypeHeader))
			if req.Out.Header.Get(httpContentTypeHeader) == "" {
				req.Out.Header.Set(httpContentTypeHeader, applicationJSONMediaType)
			}
			if accept := req.In.Header.Get("Accept"); accept != "" {
				req.Out.Header.Set("Accept", accept)
			}
			if client.token != "" {
				req.Out.Header.Set("Authorization", "Bearer "+client.token)
			}
			req.Out.Header.Set(peerBackendHeader, base64.RawURLEncoding.EncodeToString([]byte(id.BackendID)))
			req.Out.Header.Set(peerModelHeader, base64.RawURLEncoding.EncodeToString([]byte(id.LocalID)))
			req.Out.Header.Set(peerPathHeader, req.In.URL.Path)
			req.Out.Header.Set(peerInstanceHeader, source.ProxyInstanceID)
			req.Out.Header.Set(peerGenerationHeader, strconv.FormatUint(source.ConfigGeneration, 10))
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			h.logger.Warn("peer inference failed", "node_id", id.OwnerNodeID, "error", safeErrorDetail(err))
			writeJSONError(h.logger, w, http.StatusBadGateway, "peer inference unavailable")
		},
	}
}

func (h *Handler) proxyPeerInference(w http.ResponseWriter, r *http.Request, client *peerClient, id nodecontract.ModelIdentity, source nodecontract.Source) {
	select {
	case h.peerInferenceSlots <- struct{}{}:
		defer func() { <-h.peerInferenceSlots }()
	default:
		writeJSONError(h.logger, w, http.StatusServiceUnavailable, "peer inference busy")
		return
	}
	target, err := url.Parse(client.origin)
	if err != nil {
		h.logger.Error("invalid configured peer origin")
		writeJSONError(h.logger, w, http.StatusServiceUnavailable, peerModelUnavailableMsg)
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	stop := context.AfterFunc(h.peerContext, cancel)
	defer func() { stop(); cancel() }()
	r = r.WithContext(ctx)
	proxy := newPeerInferenceProxy(h, client, id, source, target)
	proxy.ServeHTTP(w, r)
}

func copyPeerClientAddressHeaders(req *http.Request) http.Header {
	forwarded := make(http.Header)
	for _, name := range []string{xForwardedForHeader, "X-Real-IP"} {
		for _, value := range req.Header.Values(name) {
			forwarded.Add(name, value)
		}
	}
	if forwarded.Get(xForwardedForHeader) == "" {
		address := forwarded.Get("X-Real-IP")
		if address == "" {
			address = req.RemoteAddr
			if host, _, err := net.SplitHostPort(address); err == nil {
				address = host
			}
		}
		if address != "" {
			forwarded.Set(xForwardedForHeader, address)
		}
	}
	return forwarded
}

func peerTargetHeaders(r *http.Request, nodeID string) (nodecontract.ModelIdentity, string, string, uint64, error) {
	for _, name := range []string{peerBackendHeader, peerModelHeader, peerPathHeader, peerInstanceHeader, peerGenerationHeader} {
		if len(r.Header.Values(name)) != 1 {
			return nodecontract.ModelIdentity{}, "", "", 0, errors.New("invalid peer target")
		}
	}
	id, err := parsePeerPublicID(peerModelPrefix + nodeID + "/" + r.Header.Get(peerBackendHeader) + "/" + r.Header.Get(peerModelHeader))
	if err != nil {
		return nodecontract.ModelIdentity{}, "", "", 0, err
	}
	path, instance := r.Header.Get(peerPathHeader), r.Header.Get(peerInstanceHeader)
	generation, err := strconv.ParseUint(r.Header.Get(peerGenerationHeader), 10, 64)
	if err != nil || generation == 0 || strconv.FormatUint(generation, 10) != r.Header.Get(peerGenerationHeader) {
		return nodecontract.ModelIdentity{}, "", "", 0, fmt.Errorf("invalid peer generation")
	}
	return id, path, instance, generation, nil
}

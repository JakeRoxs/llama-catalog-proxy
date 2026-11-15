package proxy

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/catalog"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/correlation"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/diagnostics"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modeldiagnostics"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/provenance"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/timing"
)

// requestIDKey is the context key type for the proxy-generated request ID.
type requestIDKey struct{}

// routeInfoKind describes how a request was routed.
const (
	routeInfoKindDefault = "default"
	routeInfoKindModel   = "model_prefix"
	routeInfoKindPath    = "path"
	routeInfoKindLocal   = "local"
)

// routeInfo captures how a request was routed for logging and correlation.
type routeInfo struct {
	backendID string
	kind      string
}

// requestIDLength is the bounded length (in bytes) of the generated request ID.
const requestIDLength = 16

// generateRequestID returns a crypto-random, bounded request ID. It falls back
// to a time-based identifier if randomness is unavailable, but never logs or
// trusts an incoming client-supplied ID.
func generateRequestID() string {
	buf := make([]byte, requestIDLength)
	if _, err := rand.Read(buf); err == nil {
		return hex.EncodeToString(buf)
	}
	return fmt.Sprintf("req-%d", time.Now().UnixNano())
}

func requestIDFromContext(ctx context.Context) string {
	requestID, _ := ctx.Value(requestIDKey{}).(string)
	return requestID
}

// observationKey is the context key type for the per-request timing observer.
type observationKey struct{}

// observerFromContext returns the per-request timing observer stored in the
// request context, or nil when observation is not enabled for this request.
func observerFromContext(ctx context.Context) *timing.Observer {
	if o, _ := ctx.Value(observationKey{}).(*timing.Observer); o != nil {
		return o
	}
	return nil
}

// isGenerationPath reports whether the request is a POST to a known inference
// generation path that is eligible for per-request observation.
func isGenerationPath(request *http.Request) bool {
	if request.Method != http.MethodPost {
		return false
	}
	if request.Header.Get("Upgrade") != "" {
		return false
	}
	for _, token := range strings.Split(request.Header.Get("Connection"), ",") {
		if strings.EqualFold(strings.TrimSpace(token), "upgrade") {
			return false
		}
	}
	path := request.URL.Path
	return path == "/v1/chat/completions" || path == "/v1/completions"
}

type Handler struct {
	catalog            *catalog.Service
	proxies            map[string]*httputil.ReverseProxy
	defaultBackend     string
	modelRoutes        []route
	pathRoutes         []route
	logger             *slog.Logger
	healthTimeout      time.Duration
	catalogTimeout     time.Duration
	comfyUIWaitTimeout time.Duration
	comfyUIBackendID   string
	comfyUIJobs        *comfyUIJobTracker
	maxJSONBody        int64
	diagnostics        *diagnostics.Service
	modelDiagnostics   *modeldiagnostics.Service
	provenance         *provenance.Service
	history            *correlation.Correlator
	// requestObservation enables per-request inference timing observation.
	requestObservation bool
	// Diagnostics API auth boundary. A nonempty diagnosticsAPIKey protects
	// /diagnostics and /diagnostics/v1 requests. Collectors and the
	// concurrency/rate limiter are constructed only when a key is configured;
	// construction issues no requests.
	diagnosticsAPIKey   string
	diagnosticSlots     chan struct{}
	diagnosticRateMu    sync.Mutex
	lastDiagnosticQuery time.Time
	activityCollector   *correlation.ActivityCollector
	aliasCollector      *correlation.AliasCollector
	gpuCollector        *correlation.GPUCollector
	lifecycleCollector  *correlation.LifecycleCollector
	// modelSnapshotPages is the bounded per-handler immutable snapshot pager
	// for the /diagnostics/v1/models endpoint. It is constructed only when the
	// diagnostics API key is configured.
	modelSnapshotPages *modelSnapshotPageStore
	// pagerMu guards the model snapshot pager.
	pagerMu sync.Mutex
}

type route struct {
	backendID string
	prefix    string
}

func New(cfg config.Config, logger *slog.Logger, diagnostics *diagnostics.Service, modelDiagnostics *modeldiagnostics.Service, provenance *provenance.Service, timing *timing.Observer) (http.Handler, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate configuration: %w", err)
	}
	// Health and catalog operations apply their own context budgets below. A
	// client-wide timeout would accidentally cap the longer of those budgets.
	client := &http.Client{}
	var comfyJobs *comfyUIJobTracker
	if cfg.ComfyUIWaitTimeout > 0 {
		if backendID, exists := cfg.PathRoutes["/comfyui"]; exists {
			comfyJobs = newComfyUIJobTracker(cfg.Backends[backendID], client)
		}
	}
	backends := make([]catalog.Backend, 0, len(cfg.Backends))
	routes := make([]route, 0, len(cfg.Backends)-1)
	pathRoutes := make([]route, 0, len(cfg.PathRoutes))
	proxies := make(map[string]*httputil.ReverseProxy, len(cfg.Backends))
	for id, backend := range cfg.Backends {
		target, err := url.Parse(backend.URL)
		if err != nil {
			return nil, fmt.Errorf("parse backend %q URL: %w", id, err)
		}
		headers := make(http.Header, len(backend.Headers))
		for name, value := range backend.Headers {
			headers.Set(name, value)
		}
		catalogBackend := catalog.Backend{
			ID: id, URL: target, Prefix: backend.Prefix, HealthPath: backend.HealthPath,
			ShowEnrichment: backend.OllamaShowEnrichment,
			APIKey:         backend.APIKey, Headers: headers, Default: id == cfg.DefaultBackend,
		}
		backends = append(backends, catalogBackend)
		if backend.Prefix != "" {
			routes = append(routes, route{backendID: id, prefix: backend.Prefix})
		}

		backendID := id
		backendConfig := backend
		proxies[id] = &httputil.ReverseProxy{
			Rewrite: func(request *httputil.ProxyRequest) {
				request.SetURL(target)
				request.SetXForwarded()
				applyBackendHeaders(request.Out.Header, backendConfig)
				if requestID := requestIDFromContext(request.Out.Context()); requestID != "" {
					request.Out.Header.Set("X-Request-ID", requestID)
				}
			},
			ModifyResponse: func(response *http.Response) error {
				if response.Request != nil {
					if requestID := requestIDFromContext(response.Request.Context()); requestID != "" {
						response.Header.Set("X-Request-ID", requestID)
					}
					// Observe upstream headers at the actual received headers,
					// never inferred from the response writer. Skip 101
					// switching protocol upgrades.
					if obs := observerFromContext(response.Request.Context()); obs != nil && response.StatusCode != http.StatusSwitchingProtocols {
						obs.ObserveUpstream(response.StatusCode, response.Header)
					}
				}
				if backendID == cfg.PathRoutes["/comfyui"] && comfyJobs != nil && isComfyPromptRequest(response.Request) {
					comfyJobs.promptResponse(response.StatusCode)
				}
				return nil
			},
			FlushInterval: -1,
			ErrorHandler:  proxyErrorHandler(logger, backendID),
		}
	}
	for prefix, backendID := range cfg.PathRoutes {
		pathRoutes = append(pathRoutes, route{backendID: backendID, prefix: prefix})
	}
	sort.Slice(routes, func(i, j int) bool {
		if len(routes[i].prefix) != len(routes[j].prefix) {
			return len(routes[i].prefix) > len(routes[j].prefix)
		}
		return routes[i].backendID < routes[j].backendID
	})
	sort.Slice(pathRoutes, func(i, j int) bool {
		if len(pathRoutes[i].prefix) != len(pathRoutes[j].prefix) {
			return len(pathRoutes[i].prefix) > len(pathRoutes[j].prefix)
		}
		return pathRoutes[i].prefix < pathRoutes[j].prefix
	})
	catalogService := catalog.New(backends, client, cfg.ModelsCacheTTL, cfg.HealthTimeout, cfg.CatalogTimeout, logger)
	h := &Handler{
		catalog:            catalogService,
		proxies:            proxies,
		defaultBackend:     cfg.DefaultBackend,
		modelRoutes:        routes,
		pathRoutes:         pathRoutes,
		logger:             logger,
		healthTimeout:      cfg.HealthTimeout,
		catalogTimeout:     cfg.CatalogTimeout,
		comfyUIWaitTimeout: cfg.ComfyUIWaitTimeout,
		comfyUIBackendID:   cfg.PathRoutes["/comfyui"],
		comfyUIJobs:        comfyJobs,
		maxJSONBody:        cfg.MaxJSONBody,
		diagnostics:        diagnostics,
		modelDiagnostics:   modelDiagnostics,
		provenance:         provenance,
		// The legacy timing prototype argument is accepted for signature
		// compatibility but is ignored: per-request observers are created in
		// ServeHTTP only when request_observation is enabled.
		history:            historyStore(cfg),
		requestObservation: cfg.RequestObservation,
	}

	// Diagnostics API auth boundary: collectors and the concurrency/rate
	// limiter are constructed only when a dedicated API key is configured.
	// Construction copies configuration and issues no requests.
	if cfg.DiagnosticsAPIKey != "" {
		h.diagnosticsAPIKey = cfg.DiagnosticsAPIKey
		h.diagnosticSlots = make(chan struct{}, 2)
		h.modelDiagnostics = modeldiagnostics.New()
		h.modelSnapshotPages = newModelSnapshotPageStore()
		h.activityCollector = correlation.NewActivityCollector(cfg.Backends)
		h.aliasCollector = correlation.NewAliasCollector(cfg.Backends)
		h.gpuCollector = correlation.NewGPUCollector(cfg.Backends)
		h.lifecycleCollector = correlation.NewLifecycleCollector(cfg.Backends)
	}

	return h, nil
}

func safeErrorDetail(err error) string {
	var urlErr *url.Error
	if errors.As(err, &urlErr) && urlErr.Err != nil {
		return urlErr.Err.Error()
	}
	return err.Error()
}

func proxyErrorHandler(logger *slog.Logger, backendID string) func(http.ResponseWriter, *http.Request, error) {
	return func(writer http.ResponseWriter, request *http.Request, proxyErr error) {
		requestID := requestIDFromContext(request.Context())
		errorDetail := safeErrorDetail(proxyErr)
		if request.Context().Err() != nil && (errors.Is(proxyErr, context.Canceled) || errors.Is(proxyErr, context.DeadlineExceeded)) {
			logger.Warn("reverse proxy request canceled by client",
				"backend_id", backendID, "request_id", requestID,
				"method", request.Method, "path", request.URL.Path, "error", errorDetail)
		} else {
			logger.Error("reverse proxy request failed",
				"backend_id", backendID, "request_id", requestID,
				"method", request.Method, "path", request.URL.Path, "error", errorDetail)
		}
		writeJSONError(logger, writer, http.StatusBadGateway, "backend unavailable")
		// Mark transport failure with a fixed sentinel so the observation
		// records the transport outcome without storing the raw error. This is
		// done after the error response is written so the wrapped writer chain
		// is not disturbed.
		if obs := observerFromContext(request.Context()); obs != nil {
			obs.Finish(errTransportFailure)
		}
	}
}

func applyBackendHeaders(header http.Header, backend config.Backend) {
	for name, value := range backend.Headers {
		if header.Get(name) == "" {
			header.Set(name, value)
		}
	}
	setAPIKeyIfMissing(header, backend.APIKey)
}

func setAPIKeyIfMissing(header http.Header, apiKey string) {
	if apiKey == "" || header.Get("Authorization") != "" || header.Get("X-Api-Key") != "" {
		return
	}
	header.Set("Authorization", "Bearer "+apiKey)
	header.Set("X-Api-Key", apiKey)
}

func (h *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	start := time.Now()
	requestID := generateRequestID()
	ctx := context.WithValue(request.Context(), requestIDKey{}, requestID)
	request = request.WithContext(ctx)

	var wrapped http.ResponseWriter = writer
	var obs *timing.Observer
	// Per-request observation is created only for POST known generation paths
	// when the request_observation config flag is enabled. Disabled requests
	// do not allocate a parser, wrap the writer, or store context state.
	if h.requestObservation && isGenerationPath(request) {
		obs = timing.New(writer)
		wrapped = obs
		ctx = context.WithValue(ctx, observationKey{}, obs)
		request = request.WithContext(ctx)
	}
	w := &responseWriter{ResponseWriter: wrapped}
	w.Header().Set("X-Request-ID", requestID)
	info := routeInfo{}

	// Record one transport outcome, including aborted streams, without swallowing
	// the original panic or treating a partial HTTP response as a generation success.
	defer func() {
		panicValue := recover()
		h.logRequest(request, w, start, info)
		if h.history != nil && info.backendID != "" {
			status := classifyOutcome(ctx, w.status)
			if panicValue != nil {
				status = statusAborted
			}
			h.history.RegisterRequest(requestID, info.backendID, "", start)
			h.history.CompleteRequest(requestID, time.Since(start), status)
		}
		if obs != nil {
			// Finish the observation. Any panic (including non-error values)
			// must be reported as timing.ErrAborted; the original panic value
			// is rethrown verbatim after Finish. Context cancellation takes
			// precedence over a nil error; the ErrorHandler may have already
			// finished the observation with a fixed transport-failure sentinel.
			if panicValue != nil {
				obs.Finish(timing.ErrAborted)
			} else if ctx.Err() != nil {
				obs.Finish(ctx.Err())
			} else {
				obs.Finish(nil)
			}
			if h.requestObservation {
				summary := obs.Summary()
				h.logger.Info("request_observation",
					"request_id", requestID,
					"backend_id", info.backendID,
					"summary", summary,
				)
				// After final Finish + Summary, attach the observation to the
				// correlation record. This is fail-open: if the record was
				// evicted or never registered, the request completes normally
				// without the observation being retained. The raw summary is
				// never stored in the metadata map.
				if h.history != nil && info.backendID != "" {
					if !h.history.AttachObservation(requestID, summary) {
						// fail-open: no new logs with raw data, no availability effect
					}
				}
			}
			obs.Close()
		}
		if panicValue != nil {
			panic(panicValue)
		}
	}()

	h.serve(w, request, &info)
}

func (h *Handler) serve(writer http.ResponseWriter, request *http.Request, info *routeInfo) {
	if request.Method == http.MethodGet {
		switch request.URL.Path {
		case "/v1/models":
			*info = routeInfo{kind: routeInfoKindLocal}
			h.serveModels(writer, request)
			return
		case "/health":
			*info = routeInfo{kind: routeInfoKindLocal}
			writeJSON(h.logger, writer, http.StatusOK, map[string]string{"status": "ok"})
			return
		case "/ready":
			*info = routeInfo{kind: routeInfoKindLocal}
			h.serveReady(writer, request)
			return
		}
	}
	// Reserve the diagnostics API boundary before path routing so no upstream
	// path passthrough occurs for unknown diagnostics subpaths. All
	// /diagnostics and /diagnostics/v1 requests are authenticated and handled
	// by serveDiagnosticsAPI (owned by the diagnostics API worker).
	if request.URL.Path == "/diagnostics" || strings.HasPrefix(request.URL.Path, "/diagnostics/v1") {
		*info = routeInfo{kind: routeInfoKindLocal}
		h.serveDiagnosticsAPI(writer, request, info)
		return
	}
	if backendID, matched := h.routePath(request.URL.Path); matched {
		*info = routeInfo{backendID: backendID, kind: routeInfoKindPath}
		if backendID == h.comfyUIBackendID && isComfyPromptRequest(request) && h.comfyUIJobs != nil {
			ctx, cancel := context.WithTimeout(request.Context(), h.comfyUIWaitTimeout)
			err := h.comfyUIJobs.beginSubmission(ctx)
			cancel()
			if err != nil {
				h.writeComfyUIWaitError(writer, request, err)
				return
			}
			defer h.comfyUIJobs.endSubmission()
		} else if isInferenceRequest(request) {
			if err := h.waitForComfyUI(request); err != nil {
				h.writeComfyUIWaitError(writer, request, err)
				return
			}
		}
		h.proxies[backendID].ServeHTTP(writer, request)
		return
	}
	backendID, modelMatched, err := h.routeRequest(request)
	if err != nil {
		if errors.Is(err, errJSONBodyTooLarge) {
			*info = routeInfo{backendID: h.defaultBackend, kind: routeInfoKindDefault}
			h.logger.Warn("JSON request body rejected", "request_id", requestIDFromContext(request.Context()), "content_length", request.ContentLength, "limit", h.maxJSONBody)
			writeJSONError(h.logger, writer, http.StatusRequestEntityTooLarge, "JSON request body exceeds the configured limit")
			return
		}
		*info = routeInfo{backendID: h.defaultBackend, kind: routeInfoKindDefault}
		h.logger.Warn("request body inspection failed", "request_id", requestIDFromContext(request.Context()), "error", err)
		writeJSONError(h.logger, writer, http.StatusBadRequest, "request body could not be read")
		return
	}
	kind := routeInfoKindDefault
	if modelMatched {
		kind = routeInfoKindModel
	}
	*info = routeInfo{backendID: backendID, kind: kind}
	reservedInference := false
	if isInferenceRequest(request) {
		if h.comfyUIWaitTimeout > 0 && h.comfyUIJobs != nil {
			ctx, cancel := context.WithTimeout(request.Context(), h.comfyUIWaitTimeout)
			err := h.comfyUIJobs.reserveInference(ctx)
			cancel()
			if err != nil {
				h.writeComfyUIWaitError(writer, request, err)
				return
			}
			reservedInference = true
		}
	}
	if reservedInference {
		defer h.comfyUIJobs.endInference()
	}
	h.proxies[backendID].ServeHTTP(writer, request)
}

func isInferenceRequest(request *http.Request) bool {
	if request.Method != http.MethodPost {
		return false
	}
	switch request.URL.Path {
	case "/v1/chat/completions", "/v1/completions", "/v1/responses", "/v1/embeddings",
		"/rerank", "/reranking", "/v1/rerank", "/v1/reranking",
		"/v1/images/generations", "/v1/images/edits", "/sdapi/v1/txt2img", "/sdapi/v1/img2img",
		"/v1/audio/speech", "/v1/audio/transcriptions":
		return true
	default:
		return false
	}
}

func (h *Handler) waitForComfyUI(request *http.Request) error {
	if h.comfyUIWaitTimeout <= 0 || h.comfyUIJobs == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(request.Context(), h.comfyUIWaitTimeout)
	defer cancel()
	return h.comfyUIJobs.wait(ctx)
}

func (h *Handler) writeComfyUIWaitError(writer http.ResponseWriter, request *http.Request, err error) {
	if errors.Is(err, context.DeadlineExceeded) {
		writeJSONError(h.logger, writer, http.StatusGatewayTimeout, "timed out waiting for ComfyUI jobs to finish")
		return
	}
	if errors.Is(err, context.Canceled) {
		return
	}
	h.logger.Warn("ComfyUI job wait failed", "request_id", requestIDFromContext(request.Context()), "error", err)
	writeJSONError(h.logger, writer, http.StatusBadGateway, "could not confirm ComfyUI job completion")
}

func (h *Handler) routeRequest(request *http.Request) (string, bool, error) {
	contentType := request.Header.Get("Content-Type")
	if request.Body == nil || request.Body == http.NoBody || request.Header.Get("Content-Encoding") != "" ||
		(contentType != "" && !strings.HasPrefix(strings.ToLower(contentType), "application/json")) {
		return h.defaultBackend, false, nil
	}
	if request.ContentLength > h.maxJSONBody {
		_ = request.Body.Close()
		return "", false, errJSONBodyTooLarge
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, h.maxJSONBody+1))
	_ = request.Body.Close()
	if err != nil {
		return "", false, err
	}
	if int64(len(body)) > h.maxJSONBody {
		return "", false, errJSONBodyTooLarge
	}
	restoreRequestBody(request, body)
	if len(bytes.TrimSpace(body)) == 0 {
		return h.defaultBackend, false, nil
	}

	var object map[string]any
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&object); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return h.defaultBackend, false, nil
	}
	publicModel, ok := object["model"].(string)
	if !ok {
		return h.defaultBackend, false, nil
	}
	backendID, localModel := h.routeModel(publicModel)
	if localModel == publicModel {
		return backendID, false, nil
	}
	object["model"] = localModel
	rewritten, err := json.Marshal(object)
	if err != nil {
		return "", false, err
	}
	restoreRequestBody(request, rewritten)
	return backendID, true, nil
}

var errJSONBodyTooLarge = errors.New("JSON request body exceeds configured limit")

// errTransportFailure is a fixed sentinel used to mark transport failures in
// observations without leaking raw error details into summaries or logs.
var errTransportFailure = errors.New("proxy transport failure")

func (h *Handler) routeModel(model string) (string, string) {
	for _, candidate := range h.modelRoutes {
		if localModel, matched := strings.CutPrefix(model, candidate.prefix); matched {
			return candidate.backendID, localModel
		}
	}
	return h.defaultBackend, model
}

func (h *Handler) routePath(requestPath string) (string, bool) {
	for _, candidate := range h.pathRoutes {
		if candidate.prefix == "/" || requestPath == candidate.prefix || strings.HasPrefix(requestPath, candidate.prefix+"/") {
			return candidate.backendID, true
		}
	}
	return "", false
}

func restoreRequestBody(request *http.Request, body []byte) {
	request.Body = io.NopCloser(bytes.NewReader(body))
	request.ContentLength = int64(len(body))
	request.Header.Set("Content-Length", fmt.Sprint(len(body)))
	request.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(body)), nil
	}
}

func (h *Handler) logRequest(request *http.Request, response *responseWriter, start time.Time, info routeInfo) {
	if h.logger != nil {
		requestID := requestIDFromContext(request.Context())
		h.logger.Info("request served",
			"request_id", requestID,
			"method", request.Method,
			"path", request.URL.Path,
			"route_kind", info.kind,
			"backend_id", info.backendID,
			"status", response.status,
			"duration_ms", time.Since(start).Seconds()*1000,
			"resp_bytes", response.written,
		)
	}
}

type responseWriter struct {
	http.ResponseWriter
	status  int
	written int64
}

func (w *responseWriter) WriteHeader(status int) {
	if w.status == 0 && (status >= 200 || status == http.StatusSwitchingProtocols) {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *responseWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(data)
	w.written += int64(n)
	return n, err
}

func (w *responseWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("response writer does not support hijacking")
	}
	connection, readWriter, err := hijacker.Hijack()
	if err == nil && w.status == 0 {
		w.status = http.StatusSwitchingProtocols
	}
	return connection, readWriter, err
}

func (w *responseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (h *Handler) serveModels(writer http.ResponseWriter, request *http.Request) {
	ctx, cancel := context.WithTimeout(request.Context(), h.catalogTimeout)
	defer cancel()
	response, err := h.catalog.Models(ctx, request.Header)
	if err != nil {
		h.logger.Error("model catalog request failed", "request_id", requestIDFromContext(request.Context()), "error", safeErrorDetail(err))
		writeJSONError(h.logger, writer, http.StatusBadGateway, "default backend model catalog unavailable")
		return
	}
	copyResponseHeaders(writer.Header(), response.Header)
	writer.WriteHeader(response.StatusCode)
	if _, err := writer.Write(response.Body); err != nil {
		h.logger.Warn("model catalog response write failed", "request_id", requestIDFromContext(request.Context()), "error", err)
	}
}

func (h *Handler) serveReady(writer http.ResponseWriter, request *http.Request) {
	ctx, cancel := context.WithTimeout(request.Context(), h.healthTimeout)
	defer cancel()
	statuses, err := h.catalog.BackendStatuses(ctx)
	status := "ready"
	code := http.StatusOK
	if err != nil {
		status = "not_ready"
		code = http.StatusServiceUnavailable
		h.logger.Warn("default backend readiness check failed", "request_id", requestIDFromContext(request.Context()), "error", safeErrorDetail(err))
	}
	writeJSON(h.logger, writer, code, map[string]any{"status": status, "backends": statuses})
}

func copyResponseHeaders(destination, source http.Header) {
	for key, values := range source {
		for _, value := range values {
			destination.Add(key, value)
		}
	}
}

func writeJSONError(logger *slog.Logger, writer http.ResponseWriter, status int, message string) {
	writeJSON(logger, writer, status, map[string]any{"error": map[string]string{"message": message, "type": "proxy_error"}})
}

// Finite transport outcome statuses recorded by the proxy-only history store.
const (
	statusHTTPSuccess     = "http_success"
	statusHTTPClientError = "http_client_error"
	statusHTTPServerError = "http_server_error"
	statusHTTPOther       = "http_other"
	statusClientCanceled  = "client_canceled"
	statusAborted         = "aborted"
	statusTransportError  = "transport_error"
)

// historyStore returns a Correlator when request_history is enabled, or nil
// when disabled. It is constructed per Handler and reset on config reload.
func historyStore(cfg config.Config) *correlation.Correlator {
	if !cfg.RequestHistory {
		return nil
	}
	return correlation.New()
}

// classifyOutcome maps a response status to a finite transport outcome.
// Client cancellation takes precedence, then unset status (transport error),
// then HTTP status classes. Generation completion is never inferred.
func classifyOutcome(ctx context.Context, status int) string {
	if ctx.Err() != nil {
		return statusClientCanceled
	}
	if status == 0 {
		return statusTransportError
	}
	switch {
	case status >= 200 && status < 300:
		return statusHTTPSuccess
	case status >= 400 && status < 500:
		return statusHTTPClientError
	case status >= 500:
		return statusHTTPServerError
	default:
		return statusHTTPOther
	}
}

func writeJSON(logger *slog.Logger, writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		logger.Warn("JSON response write failed", "error", err)
	}
}

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
	comfyUIPathPrefix    = "/comfyui"
	requestIDHeader      = "X-Request-ID"
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
	comfyUIEndpoint    string
	backendEndpoints   map[string]string
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

type handlerDependencies struct {
	catalog          *catalog.Service
	proxies          map[string]*httputil.ReverseProxy
	modelRoutes      []route
	pathRoutes       []route
	backendEndpoints map[string]string
	comfyUIJobs      *comfyUIJobTracker
}

func New(cfg config.Config, logger *slog.Logger, diagnostics *diagnostics.Service, modelDiagnostics *modeldiagnostics.Service, provenance *provenance.Service, timing *timing.Observer) (http.Handler, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate configuration: %w", err)
	}
	client := &http.Client{}
	dependencies, err := buildHandlerDependencies(cfg, logger, client)
	if err != nil {
		return nil, err
	}
	h := assembleHandler(cfg, logger, diagnostics, modelDiagnostics, provenance, dependencies)
	return h, nil
}

func buildHandlerDependencies(cfg config.Config, logger *slog.Logger, client *http.Client) (handlerDependencies, error) {
	// Health and catalog operations apply their own context budgets below. A
	// client-wide timeout would accidentally cap the longer of those budgets.
	var comfyJobs *comfyUIJobTracker
	if cfg.ComfyUIWaitTimeout > 0 {
		if backendID, exists := cfg.PathRoutes[comfyUIPathPrefix]; exists {
			comfyJobs = newComfyUIJobTracker(cfg.Backends[backendID], client)
		}
	}
	backends, routes, proxies, backendEndpoints, err := buildBackendRoutes(cfg, logger, comfyJobs)
	if err != nil {
		return handlerDependencies{}, err
	}
	pathRoutes := buildPathRoutes(cfg.PathRoutes)
	catalogService := catalog.New(backends, client, cfg.ModelsCacheTTL, cfg.HealthTimeout, cfg.CatalogTimeout, logger)
	return handlerDependencies{
		catalog: catalogService, proxies: proxies, modelRoutes: routes, pathRoutes: pathRoutes,
		backendEndpoints: backendEndpoints, comfyUIJobs: comfyJobs,
	}, nil
}

func buildBackendRoutes(cfg config.Config, logger *slog.Logger, comfyJobs *comfyUIJobTracker) ([]catalog.Backend, []route, map[string]*httputil.ReverseProxy, map[string]string, error) {
	backends := make([]catalog.Backend, 0, len(cfg.Backends))
	routes := make([]route, 0, len(cfg.Backends)-1)
	proxies := make(map[string]*httputil.ReverseProxy, len(cfg.Backends))
	backendEndpoints := make(map[string]string, len(cfg.Backends))
	for id, backend := range cfg.Backends {
		backendInfo, prefixRoute, endpoint, reverseProxy, err := buildBackend(cfg, logger, id, backend, comfyJobs)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		backends = append(backends, backendInfo)
		if prefixRoute != nil {
			routes = append(routes, *prefixRoute)
		}
		backendEndpoints[id] = endpoint
		proxies[id] = reverseProxy
	}
	sortRoutes(routes)
	return backends, routes, proxies, backendEndpoints, nil
}

func buildPathRoutes(configured map[string]string) []route {
	routes := make([]route, 0, len(configured))
	for prefix, backendID := range configured {
		routes = append(routes, route{backendID: backendID, prefix: prefix})
	}
	sortPathRoutes(routes)
	return routes
}

func buildBackend(cfg config.Config, logger *slog.Logger, id string, backend config.Backend, comfyJobs *comfyUIJobTracker) (catalog.Backend, *route, string, *httputil.ReverseProxy, error) {
	target, err := url.Parse(backend.URL)
	if err != nil {
		return catalog.Backend{}, nil, "", nil, fmt.Errorf("parse backend %q URL: %w", id, err)
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
	var prefixRoute *route
	if backend.Prefix != "" {
		prefixRoute = &route{backendID: id, prefix: backend.Prefix}
	}
	return catalogBackend, prefixRoute, backendEndpointKey(target), newReverseProxy(cfg, logger, id, backend, target, comfyJobs), nil
}

func newReverseProxy(cfg config.Config, logger *slog.Logger, backendID string, backend config.Backend, target *url.URL, comfyJobs *comfyUIJobTracker) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Rewrite: func(request *httputil.ProxyRequest) {
			request.SetURL(target)
			request.SetXForwarded()
			applyBackendHeaders(request.Out.Header, backend)
			setRequestIDHeader(request.Out.Header, requestIDFromContext(request.Out.Context()))
		},
		ModifyResponse: func(response *http.Response) error {
			modifyProxiedResponse(response, backendID, cfg.PathRoutes[comfyUIPathPrefix], comfyJobs)
			return nil
		},
		FlushInterval: -1,
		ErrorHandler:  proxyErrorHandler(logger, backendID),
	}
}

func modifyProxiedResponse(response *http.Response, backendID, comfyBackendID string, comfyJobs *comfyUIJobTracker) {
	if response.Request != nil {
		setRequestIDHeader(response.Header, requestIDFromContext(response.Request.Context()))
		observer := observerFromContext(response.Request.Context())
		if observer != nil && response.StatusCode != http.StatusSwitchingProtocols {
			observer.ObserveUpstream(response.StatusCode, response.Header)
		}
	}
	if backendID == comfyBackendID && comfyJobs != nil && isComfyPromptRequest(response.Request) {
		comfyJobs.promptResponse(response.StatusCode)
	}
}

func assembleHandler(cfg config.Config, logger *slog.Logger, diagnostics *diagnostics.Service, modelDiagnostics *modeldiagnostics.Service, provenance *provenance.Service, dependencies handlerDependencies) *Handler {
	h := &Handler{
		catalog:            dependencies.catalog,
		proxies:            dependencies.proxies,
		defaultBackend:     cfg.DefaultBackend,
		modelRoutes:        dependencies.modelRoutes,
		pathRoutes:         dependencies.pathRoutes,
		logger:             logger,
		healthTimeout:      cfg.HealthTimeout,
		catalogTimeout:     cfg.CatalogTimeout,
		comfyUIWaitTimeout: cfg.ComfyUIWaitTimeout,
		comfyUIBackendID:   cfg.PathRoutes[comfyUIPathPrefix],
		comfyUIEndpoint:    dependencies.backendEndpoints[cfg.PathRoutes[comfyUIPathPrefix]],
		backendEndpoints:   dependencies.backendEndpoints,
		comfyUIJobs:        dependencies.comfyUIJobs,
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

	return h
}

func sortRoutes(routes []route) {
	sort.Slice(routes, func(i, j int) bool {
		if len(routes[i].prefix) != len(routes[j].prefix) {
			return len(routes[i].prefix) > len(routes[j].prefix)
		}
		return routes[i].backendID < routes[j].backendID
	})
}

func sortPathRoutes(routes []route) {
	sort.Slice(routes, func(i, j int) bool {
		if len(routes[i].prefix) != len(routes[j].prefix) {
			return len(routes[i].prefix) > len(routes[j].prefix)
		}
		return routes[i].prefix < routes[j].prefix
	})
}

func setRequestIDHeader(header http.Header, requestID string) {
	if requestID != "" {
		header.Set(requestIDHeader, requestID)
	}
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

type requestExecution struct {
	request   *http.Request
	response  *responseWriter
	start     time.Time
	info      routeInfo
	ctx       context.Context
	observer  *timing.Observer
	requestID string
}

func (h *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	execution := h.prepareRequest(writer, request)
	defer h.finishRequest(&execution)
	h.serve(execution.response, execution.request, &execution.info)
}

func (h *Handler) prepareRequest(writer http.ResponseWriter, request *http.Request) requestExecution {
	start := time.Now()
	requestID := generateRequestID()
	ctx := context.WithValue(request.Context(), requestIDKey{}, requestID)
	request = request.WithContext(ctx)

	var wrapped http.ResponseWriter = writer
	var observer *timing.Observer
	// Per-request observation is created only for POST known generation paths
	// when the request_observation config flag is enabled. Disabled requests
	// do not allocate a parser, wrap the writer, or store context state.
	if h.requestObservation && isGenerationPath(request) {
		observer = timing.New(writer)
		wrapped = observer
		ctx = context.WithValue(ctx, observationKey{}, observer)
		request = request.WithContext(ctx)
	}
	response := &responseWriter{ResponseWriter: wrapped}
	response.Header().Set(requestIDHeader, requestID)
	return requestExecution{
		request: request, response: response, start: start, ctx: ctx,
		observer: observer, requestID: requestID,
	}
}

func (h *Handler) finishRequest(execution *requestExecution) {
	// Record one transport outcome, including aborted streams, without swallowing
	// the original panic or treating a partial HTTP response as a generation success.
	panicValue := recover()
	h.logRequest(execution.request, execution.response, execution.start, execution.info)
	h.finishRequestHistory(execution, panicValue)
	h.finishRequestObservation(execution, panicValue)
	if panicValue != nil {
		panic(panicValue)
	}
}

func (h *Handler) finishRequestHistory(execution *requestExecution, panicValue any) {
	if h.history == nil || execution.info.backendID == "" {
		return
	}
	status := classifyOutcome(execution.ctx, execution.response.status)
	if panicValue != nil {
		status = statusAborted
	}
	h.history.RegisterRequest(execution.requestID, execution.info.backendID, "", execution.start)
	h.history.CompleteRequest(execution.requestID, time.Since(execution.start), status)
}

func (h *Handler) finishRequestObservation(execution *requestExecution, panicValue any) {
	observer := execution.observer
	if observer == nil {
		return
	}
	if panicValue != nil {
		observer.Finish(timing.ErrAborted)
	} else if execution.ctx.Err() != nil {
		observer.Finish(execution.ctx.Err())
	} else {
		observer.Finish(nil)
	}
	if h.requestObservation {
		summary := observer.Summary()
		h.logger.Info("request_observation",
			"request_id", execution.requestID,
			"backend_id", execution.info.backendID,
			"summary", summary,
		)
		if h.history != nil && execution.info.backendID != "" {
			h.history.AttachObservation(execution.requestID, summary)
		}
	}
	observer.Close()
}

func (h *Handler) serve(writer http.ResponseWriter, request *http.Request, info *routeInfo) {
	if h.serveLocalGET(writer, request, info) {
		return
	}
	if isDiagnosticsPath(request.URL.Path) {
		*info = routeInfo{kind: routeInfoKindLocal}
		h.serveDiagnosticsAPI(writer, request, info)
		return
	}
	if backendID, matched := h.routePath(request.URL.Path); matched {
		h.servePathRoute(writer, request, info, backendID)
		return
	}
	h.serveModelRoute(writer, request, info)
}

func (h *Handler) serveLocalGET(writer http.ResponseWriter, request *http.Request, info *routeInfo) bool {
	if request.Method != http.MethodGet {
		return false
	}
	switch request.URL.Path {
	case "/v1/models":
		*info = routeInfo{kind: routeInfoKindLocal}
		h.serveModels(writer, request)
	case "/health":
		*info = routeInfo{kind: routeInfoKindLocal}
		writeJSON(h.logger, writer, http.StatusOK, map[string]string{"status": "ok"})
	case "/ready":
		*info = routeInfo{kind: routeInfoKindLocal}
		h.serveReady(writer, request)
	default:
		return false
	}
	return true
}

func isDiagnosticsPath(path string) bool {
	return path == "/diagnostics" || strings.HasPrefix(path, "/diagnostics/v1")
}

func (h *Handler) servePathRoute(writer http.ResponseWriter, request *http.Request, info *routeInfo, backendID string) {
	*info = routeInfo{backendID: backendID, kind: routeInfoKindPath}
	cleanup, allowed := h.waitForPathRequest(writer, request, backendID)
	if !allowed {
		return
	}
	if cleanup != nil {
		defer cleanup()
	}
	h.proxies[backendID].ServeHTTP(writer, request)
}

func (h *Handler) waitForPathRequest(writer http.ResponseWriter, request *http.Request, backendID string) (func(), bool) {
	if backendID == h.comfyUIBackendID && isComfyPromptRequest(request) && h.comfyUIJobs != nil {
		ctx, cancel := context.WithTimeout(request.Context(), h.comfyUIWaitTimeout)
		err := h.comfyUIJobs.beginSubmission(ctx)
		cancel()
		if err != nil {
			h.writeComfyUIWaitError(writer, request, err)
			return nil, false
		}
		return h.comfyUIJobs.endSubmission, true
	} else if isInferenceRequest(request) && h.sharesComfyUIBackend(backendID) {
		if err := h.waitForComfyUI(request); err != nil {
			h.writeComfyUIWaitError(writer, request, err)
			return nil, false
		}
	}
	return nil, true
}

func (h *Handler) serveModelRoute(writer http.ResponseWriter, request *http.Request, info *routeInfo) {
	backendID, modelMatched, err := h.routeRequest(request)
	if err != nil {
		h.writeRouteRequestError(writer, request, info, err)
		return
	}
	kind := routeInfoKindDefault
	if modelMatched {
		kind = routeInfoKindModel
	}
	*info = routeInfo{backendID: backendID, kind: kind}
	if !h.reserveModelRequest(writer, request, backendID) {
		return
	}
	if isInferenceRequest(request) && h.comfyUIWaitTimeout > 0 && h.comfyUIJobs != nil && h.sharesComfyUIBackend(backendID) {
		defer h.comfyUIJobs.endInference()
	}
	h.proxies[backendID].ServeHTTP(writer, request)
}

func (h *Handler) writeRouteRequestError(writer http.ResponseWriter, request *http.Request, info *routeInfo, err error) {
	*info = routeInfo{backendID: h.defaultBackend, kind: routeInfoKindDefault}
	if errors.Is(err, errJSONBodyTooLarge) {
		h.logger.Warn("JSON request body rejected", "request_id", requestIDFromContext(request.Context()), "content_length", request.ContentLength, "limit", h.maxJSONBody)
		writeJSONError(h.logger, writer, http.StatusRequestEntityTooLarge, "JSON request body exceeds the configured limit")
		return
	}
	h.logger.Warn("request body inspection failed", "request_id", requestIDFromContext(request.Context()), "error", err)
	writeJSONError(h.logger, writer, http.StatusBadRequest, "request body could not be read")
}

func (h *Handler) reserveModelRequest(writer http.ResponseWriter, request *http.Request, backendID string) bool {
	if !isInferenceRequest(request) || h.comfyUIWaitTimeout <= 0 || h.comfyUIJobs == nil || !h.sharesComfyUIBackend(backendID) {
		return true
	}
	ctx, cancel := context.WithTimeout(request.Context(), h.comfyUIWaitTimeout)
	err := h.comfyUIJobs.reserveInference(ctx)
	cancel()
	if err != nil {
		h.writeComfyUIWaitError(writer, request, err)
		return false
	}
	return true
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

func (h *Handler) sharesComfyUIBackend(backendID string) bool {
	return h.comfyUIEndpoint != "" && h.backendEndpoints[backendID] == h.comfyUIEndpoint
}

func backendEndpointKey(target *url.URL) string {
	endpoint := *target
	endpoint.Scheme = strings.ToLower(endpoint.Scheme)
	endpoint.Host = strings.ToLower(endpoint.Host)
	endpoint.Path = strings.TrimSuffix(endpoint.Path, "/")
	endpoint.RawPath = ""
	endpoint.User = nil
	return endpoint.String()
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

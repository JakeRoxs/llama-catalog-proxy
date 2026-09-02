package proxy

import (
	"bufio"
	"bytes"
	"context"
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
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/catalog"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

type Handler struct {
	catalog        *catalog.Service
	proxies        map[string]*httputil.ReverseProxy
	defaultBackend string
	modelRoutes    []route
	pathRoutes     []route
	logger         *slog.Logger
	timeout        time.Duration
	maxJSONBody    int64
}

type route struct {
	backendID string
	prefix    string
}

func New(cfg config.Config, logger *slog.Logger) (http.Handler, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate configuration: %w", err)
	}
	client := &http.Client{Timeout: cfg.RequestTimeout}
	proxies := make(map[string]*httputil.ReverseProxy, len(cfg.Backends))
	backends := make([]catalog.Backend, 0, len(cfg.Backends))
	routes := make([]route, 0, len(cfg.Backends)-1)
	pathRoutes := make([]route, 0, len(cfg.PathRoutes))
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
			ID: id, URL: target, Prefix: backend.Prefix, APIKey: backend.APIKey,
			Headers: headers, Default: id == cfg.DefaultBackend,
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
			},
			FlushInterval: -1,
			ErrorHandler: func(writer http.ResponseWriter, request *http.Request, proxyErr error) {
				logger.Error("reverse proxy request failed", "backend_id", backendID, "method", request.Method, "path", request.URL.Path, "error", proxyErr)
				writeJSONError(logger, writer, http.StatusBadGateway, "backend unavailable")
			},
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
	return &Handler{
		catalog:        catalog.New(backends, client, cfg.ModelsCacheTTL, logger),
		proxies:        proxies,
		defaultBackend: cfg.DefaultBackend,
		modelRoutes:    routes,
		pathRoutes:     pathRoutes,
		logger:         logger,
		timeout:        cfg.RequestTimeout,
		maxJSONBody:    cfg.MaxJSONBody,
	}, nil
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
	w := responseWriter{ResponseWriter: writer}
	h.serve(&w, request)
	h.logRequest(request, &w, start)
}

func (h *Handler) serve(writer http.ResponseWriter, request *http.Request) {
	if request.Method == http.MethodGet {
		switch request.URL.Path {
		case "/v1/models":
			h.serveModels(writer, request)
			return
		case "/health":
			writeJSON(h.logger, writer, http.StatusOK, map[string]string{"status": "ok"})
			return
		case "/ready":
			h.serveReady(writer, request)
			return
		}
	}
	if backendID, matched := h.routePath(request.URL.Path); matched {
		h.proxies[backendID].ServeHTTP(writer, request)
		return
	}
	backendID, err := h.routeRequest(request)
	if err != nil {
		if errors.Is(err, errJSONBodyTooLarge) {
			h.logger.Warn("JSON request body rejected", "content_length", request.ContentLength, "limit", h.maxJSONBody)
			writeJSONError(h.logger, writer, http.StatusRequestEntityTooLarge, "JSON request body exceeds the configured limit")
			return
		}
		h.logger.Warn("request body inspection failed", "error", err)
		writeJSONError(h.logger, writer, http.StatusBadRequest, "request body could not be read")
		return
	}
	h.proxies[backendID].ServeHTTP(writer, request)
}

func (h *Handler) routeRequest(request *http.Request) (string, error) {
	contentType := request.Header.Get("Content-Type")
	if request.Body == nil || request.Body == http.NoBody || request.Header.Get("Content-Encoding") != "" ||
		(contentType != "" && !strings.HasPrefix(strings.ToLower(contentType), "application/json")) {
		return h.defaultBackend, nil
	}
	if request.ContentLength > h.maxJSONBody {
		_ = request.Body.Close()
		return "", errJSONBodyTooLarge
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, h.maxJSONBody+1))
	_ = request.Body.Close()
	if err != nil {
		return "", err
	}
	if int64(len(body)) > h.maxJSONBody {
		return "", errJSONBodyTooLarge
	}
	restoreRequestBody(request, body)
	if len(bytes.TrimSpace(body)) == 0 {
		return h.defaultBackend, nil
	}

	var object map[string]any
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&object); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return h.defaultBackend, nil
	}
	publicModel, ok := object["model"].(string)
	if !ok {
		return h.defaultBackend, nil
	}
	backendID, localModel := h.routeModel(publicModel)
	if localModel == publicModel {
		return backendID, nil
	}
	object["model"] = localModel
	rewritten, err := json.Marshal(object)
	if err != nil {
		return "", err
	}
	restoreRequestBody(request, rewritten)
	return backendID, nil
}

var errJSONBodyTooLarge = errors.New("JSON request body exceeds configured limit")

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

func (h *Handler) logRequest(request *http.Request, response *responseWriter, start time.Time) {
	if h.logger != nil {
		h.logger.Info("request served", "method", request.Method, "path", request.URL.Path, "status", response.status, "duration_ms", time.Since(start).Seconds()*1000, "resp_bytes", response.written)
	}
}

type responseWriter struct {
	http.ResponseWriter
	status  int
	written int64
}

func (w *responseWriter) WriteHeader(status int) {
	if w.status == 0 {
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
	response, err := h.catalog.Models(request.Context(), request.Header)
	if err != nil {
		h.logger.Error("model catalog request failed", "error", err)
		writeJSONError(h.logger, writer, http.StatusBadGateway, "default backend model catalog unavailable")
		return
	}
	copyResponseHeaders(writer.Header(), response.Header)
	writer.WriteHeader(response.StatusCode)
	if _, err := writer.Write(response.Body); err != nil {
		h.logger.Warn("model catalog response write failed", "error", err)
	}
}

func (h *Handler) serveReady(writer http.ResponseWriter, request *http.Request) {
	ctx, cancel := context.WithTimeout(request.Context(), h.timeout)
	defer cancel()
	statuses, err := h.catalog.BackendStatuses(ctx)
	status := "ready"
	code := http.StatusOK
	if err != nil {
		status = "not_ready"
		code = http.StatusServiceUnavailable
		h.logger.Warn("default backend readiness check failed", "error", err)
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

func writeJSON(logger *slog.Logger, writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		logger.Warn("JSON response write failed", "error", err)
	}
}

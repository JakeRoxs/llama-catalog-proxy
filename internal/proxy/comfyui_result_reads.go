package proxy

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

const (
	comfyUIHistoryPath      = "/comfyui/history"
	comfyUIViewPath         = "/comfyui/view"
	comfyUIJobsPath         = "/comfyui/api/jobs"
	maxComfyUIResultQuery   = 4096
	maxComfyUIResultValue   = 2048
	maxComfyUIPromptIDBytes = 128
)

const comfyUIRootProbeTimeout = 2 * time.Second

func newComfyUIResultReadProxy(cfg *config.ComfyUIResultReadConfig, logger *slog.Logger) (*httputil.ReverseProxy, error) {
	if cfg == nil {
		return nil, nil
	}
	target, err := url.Parse(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parse ComfyUI result-read target: %w", err)
	}
	backend := config.Backend{URL: cfg.URL, APIKey: cfg.APIKey, Headers: cfg.Headers}
	return &httputil.ReverseProxy{
		Rewrite:        comfyUIResultRewrite(target, backend),
		ModifyResponse: comfyUIResultModifyResponse,
		FlushInterval:  -1,
		ErrorHandler:   proxyErrorHandler(logger, "comfyui-results"),
	}, nil
}

// comfyUIResultRewrite builds the ReverseProxy rewrite closure that normalizes
// the inbound ComfyUI path, applies the whitelisted and backend headers, and
// forwards WebSocket upgrade headers. It preserves the encoded RawPath for
// userdata filenames while rewriting the decoded path.
func comfyUIResultRewrite(target *url.URL, backend config.Backend) func(request *httputil.ProxyRequest) {
	return func(request *httputil.ProxyRequest) {
		request.SetURL(target)
		rewriteComfyUIResultPath(request)
		request.Out.Header = comfyUIResultRequestHeaders(request.In.Header)
		request.SetXForwarded()
		applyBackendHeaders(request.Out.Header, backend)
		applyComfyUIResultSpecialHeaders(request)
		setRequestIDHeader(request.Out.Header, requestIDFromContext(request.Out.Context()))
	}
}

func rewriteComfyUIResultPath(request *httputil.ProxyRequest) {
	request.Out.URL.Path = strings.TrimPrefix(request.In.URL.Path, comfyUIPathPrefix)
	if request.Out.URL.Path == "" {
		request.Out.URL.Path = "/"
	}
	request.Out.URL.RawPath = ""
	if request.In.URL.RawPath != "" {
		// Validated userdata paths may encode a nested filename as one
		// route parameter. Preserve that encoding for ComfyUI's router.
		request.Out.URL.RawPath = strings.TrimPrefix(request.In.URL.RawPath, comfyUIPathPrefix)
	}
}

func applyComfyUIResultSpecialHeaders(request *httputil.ProxyRequest) {
	headers := request.Out.Header
	if isComfyUIImageUpload(request.In) {
		headers.Set(contentTypeHeader, request.In.Header.Get(contentTypeHeader))
	}
	if comfyUIJSONSaveKind(request.In) != "" {
		headers.Set(contentTypeHeader, "application/json")
	}
	if websocketUpgrade(request.In.Header) {
		copyComfyUIWebSocketHeaders(headers, request.In.Header)
	}
}

func copyComfyUIWebSocketHeaders(target, source http.Header) {
	target.Set("Connection", "Upgrade")
	target.Set("Upgrade", "websocket")
	for _, name := range []string{"Sec-WebSocket-Key", "Sec-WebSocket-Version", "Sec-WebSocket-Protocol", "Sec-WebSocket-Extensions"} {
		target.Del(name)
		for _, value := range source.Values(name) {
			target.Add(name, value)
		}
	}
}

// comfyUIResultModifyResponse records the request ID on the proxied response
// and strips caller credentials (Authorization, Proxy-Authenticate,
// Set-Cookie, WWW-Authenticate) so they never leak back to the client.
func comfyUIResultModifyResponse(response *http.Response) error {
	if response.Request != nil {
		setRequestIDHeader(response.Header, requestIDFromContext(response.Request.Context()))
	}
	for _, name := range []string{"Authorization", "Proxy-Authenticate", "Set-Cookie", "WWW-Authenticate"} {
		response.Header.Del(name)
	}
	return nil
}

func newComfyUIResultReadProbe(cfg *config.ComfyUIResultReadConfig) (func(context.Context) error, error) {
	if cfg == nil {
		return nil, nil
	}
	target, err := url.Parse(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parse ComfyUI result-read probe target: %w", err)
	}
	queueURL := endpointURL(target, "/queue")
	backend := config.Backend{URL: cfg.URL, APIKey: cfg.APIKey, Headers: cfg.Headers}
	client := &http.Client{
		Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true},
		Timeout:   comfyUIRootProbeTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return func(parent context.Context) error {
		ctx, cancel := context.WithTimeout(parent, comfyUIRootProbeTimeout)
		defer cancel()
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, queueURL.String(), nil)
		if err != nil {
			return err
		}
		request.Header.Set("Accept", "application/json")
		applyBackendHeaders(request.Header, backend)
		response, err := client.Do(request)
		if err != nil {
			return err
		}
		_ = response.Body.Close()
		return nil
	}, nil
}

func (h *Handler) serveComfyUIResultRead(writer http.ResponseWriter, request *http.Request, info *routeInfo, allowOwnerHash bool) bool {
	if h.comfyUIResultReadProxy == nil {
		return false
	}
	if h.serveComfyUIResultReadEarly(writer, request, info, allowOwnerHash) {
		return true
	}
	matched, _, _, frontend := classifyComfyUIResultRead(request, allowOwnerHash)
	if !matched {
		return false
	}
	return h.serveMatchedComfyUIResultRead(writer, request, info, frontend)
}

func (h *Handler) serveMatchedComfyUIResultRead(writer http.ResponseWriter, request *http.Request, info *routeInfo, frontend bool) bool {
	if !comfyUIResultReadRootAllowed(h, request, frontend) {
		return false
	}
	if request.URL.Path == "/comfyui" {
		return serveComfyUIResultRedirect(writer, request)
	}
	slots := comfyUIResultSlots(h, request, frontend)
	release, ok := acquireComfyUIResultSlot(h, writer, slots)
	if !ok {
		return true
	}
	defer release()
	*info = routeInfo{backendID: "comfyui-results", kind: routeInfoKindPath}
	if frontend && isComfyUIObjectInfo(request.URL.Path) && !h.ensureComfyUIStarted(writer, request) {
		return true
	}
	h.comfyUIResultReadProxy.ServeHTTP(writer, request)
	return true
}

func comfyUIResultSlots(h *Handler, request *http.Request, frontend bool) chan struct{} {
	if websocketUpgrade(request.Header) {
		return h.comfyUIWebSocketSlots
	}
	if frontend && h.comfyUIFrontendSlots != nil {
		return h.comfyUIFrontendSlots
	}
	return h.comfyUIResultSlots
}

// serveComfyUIResultReadEarly handles cases that do not proceed to proxying.
func (h *Handler) serveComfyUIResultReadEarly(writer http.ResponseWriter, request *http.Request, info *routeInfo, allowOwnerHash bool) bool {
	if h.comfyUIImageUploadMaxBytes > 0 && isComfyUIImageUpload(request) {
		h.serveComfyUIImageUpload(writer, request, info)
		return true
	}
	if kind := comfyUIJSONSaveKind(request); kind != "" {
		h.serveComfyUIJSONSave(writer, request, info, kind)
		return true
	}
	matched, status, message, _ := classifyComfyUIResultRead(request, allowOwnerHash)
	if !matched {
		return false
	}
	if status != 0 {
		writeComfyUIResultReadError(h, writer, status, message)
		return true
	}
	return false
}

func writeComfyUIResultReadError(h *Handler, writer http.ResponseWriter, status int, message string) {
	if status == http.StatusMethodNotAllowed {
		writer.Header().Set("Allow", "GET")
	}
	writeJSONError(h.logger, writer, status, message)
}

func acquireComfyUIResultSlot(h *Handler, writer http.ResponseWriter, slots chan struct{}) (func(), bool) {
	select {
	case slots <- struct{}{}:
		return func() { <-slots }, true
	default:
		writeJSONError(h.logger, writer, http.StatusServiceUnavailable, "ComfyUI result service busy")
		return nil, false
	}
}

// classifyComfyUIResultRead runs the frontend allowlist followed by the
// result-read allowlist when the frontend does not match. It returns whether
// the request is allowed, the error status/message (0 when none), and whether
// the request originated from the ComfyUI frontend.
func classifyComfyUIResultRead(request *http.Request, allowOwnerHash bool) (matched bool, status int, message string, frontend bool) {
	matched, status, message = validateComfyUIFrontendRequest(request)
	frontend = matched
	if !matched {
		matched, status, message = validateComfyUIResultReadRequest(request, allowOwnerHash)
	}
	return matched, status, message, frontend
}

// comfyUIResultReadRootAllowed reports whether a frontend root request may be
// served directly from the persistent origin. It returns false only when the
// probe is configured and the origin is not yet alive, causing the caller to
// fall back to llama-swap.
func comfyUIResultReadRootAllowed(h *Handler, request *http.Request, frontend bool) bool {
	if frontend && isComfyUIFrontendRoot(request) {
		// Use the persistent origin when it is already alive. Only fall back to
		// llama-swap for the initial start of a stopped service.
		if h.comfyUIResultReadProbe == nil || h.comfyUIResultReadProbe(request.Context()) != nil {
			return false
		}
	}
	return true
}

// serveComfyUIResultRedirect issues a temporary redirect from the bare
// /comfyui path to the trailing-slash frontend root.
func serveComfyUIResultRedirect(writer http.ResponseWriter, request *http.Request) bool {
	location := "/comfyui/"
	if request.URL.RawQuery != "" {
		location += "?" + request.URL.RawQuery
	}
	http.Redirect(writer, request, location, http.StatusTemporaryRedirect)
	return true
}

func validateComfyUIResultReadRequest(request *http.Request, allowOwnerHash bool) (bool, int, string) {
	if request == nil || request.URL == nil {
		return false, 0, ""
	}
	requestPath, kind, ok := classifyComfyUIResultPath(request.URL.Path)
	if !ok {
		return false, 0, ""
	}
	if request.Method != http.MethodGet && (kind == "status" || kind == "jobs") {
		return false, 0, ""
	}
	if status, message := validateComfyUIResultReadMetadata(request, requestPath, kind); status != 0 {
		return comfyUIResultReadError(status, message)
	}
	if len(request.URL.RawQuery) > maxComfyUIResultQuery {
		return comfyUIResultReadError(http.StatusBadRequest, "ComfyUI result query is too long")
	}
	query, err := url.ParseQuery(request.URL.RawQuery)
	if err != nil {
		return comfyUIResultReadError(http.StatusBadRequest, "invalid ComfyUI result query")
	}
	return validateComfyUIResultQuery(requestPath, kind, query, request.URL.ForceQuery, allowOwnerHash)
}

func validateComfyUIResultReadMetadata(request *http.Request, requestPath, kind string) (int, string) {
	if request.Method != http.MethodGet {
		return http.StatusMethodNotAllowed, "ComfyUI result reads allow only GET"
	}
	if request.Body != nil && request.Body != http.NoBody || request.ContentLength > 0 || len(request.TransferEncoding) != 0 {
		return http.StatusBadRequest, "ComfyUI result reads do not accept request bodies"
	}
	if request.URL.RawPath != "" && !validComfyUIResultPathEncoding(request.URL.RawPath) {
		return http.StatusBadRequest, "invalid ComfyUI result path encoding"
	}
	if path.Clean(request.URL.Path) != request.URL.Path || strings.ContainsAny(requestPath, "\\?#\x00") {
		return http.StatusBadRequest, "invalid ComfyUI result path"
	}
	return validateComfyUIResultPathItem(requestPath, kind)
}

func classifyComfyUIResultPath(requestPath string) (string, string, bool) {
	// Normalize only verified core API aliases for classification. Preserve the
	// original URL when forwarding to ComfyUI.
	if strings.HasPrefix(requestPath, "/comfyui/api/") && !strings.HasPrefix(requestPath, comfyUIJobsPath) {
		requestPath = "/comfyui/" + strings.TrimPrefix(requestPath, "/comfyui/api/")
	}
	switch {
	case requestPath == "/comfyui/prompt" || requestPath == "/comfyui/queue" || requestPath == "/comfyui/system_stats":
		return requestPath, "status", true
	case requestPath == comfyUIJobsPath || strings.HasPrefix(requestPath, comfyUIJobsPath+"/"):
		return requestPath, "jobs", true
	case requestPath == comfyUIHistoryPath || strings.HasPrefix(requestPath, comfyUIHistoryPath+"/"):
		return requestPath, "history", true
	case requestPath == comfyUIViewPath || strings.HasPrefix(requestPath, comfyUIViewPath+"/"):
		return requestPath, "view", true
	default:
		return requestPath, "", false
	}
}

func comfyUIResultReadError(status int, message string) (bool, int, string) {
	return true, status, message
}

func validateComfyUIResultPathItem(requestPath, kind string) (int, string) {
	if kind == "history" && requestPath != comfyUIHistoryPath {
		promptID := strings.TrimPrefix(requestPath, comfyUIHistoryPath+"/")
		if !validComfyUIPromptID(promptID) {
			return http.StatusNotFound, "ComfyUI history item not found"
		}
	}
	if kind == "view" && requestPath != comfyUIViewPath {
		return http.StatusNotFound, "ComfyUI result file not found"
	}
	return 0, ""
}

func validateComfyUIResultQuery(requestPath, kind string, query url.Values, forceQuery, allowOwnerHash bool) (bool, int, string) {
	if kind == "status" {
		if len(query) != 0 || forceQuery {
			return true, http.StatusBadRequest, "ComfyUI status reads do not accept query parameters"
		}
		return true, 0, ""
	}
	if kind == "jobs" {
		return validateComfyUIJobsQuery(requestPath, query)
	}
	if kind == "history" {
		return validateComfyUIHistoryQuery(requestPath, query)
	}
	return validateComfyUIViewQuery(query, allowOwnerHash)
}

func validateComfyUIJobsQuery(requestPath string, query url.Values) (bool, int, string) {
	if requestPath != comfyUIJobsPath {
		if !validComfyUIPromptID(strings.TrimPrefix(requestPath, comfyUIJobsPath+"/")) || len(query) != 0 {
			return comfyUIResultReadError(http.StatusBadRequest, "invalid ComfyUI job status path or query")
		}
		return true, 0, ""
	}
	for key, values := range query {
		if len(values) != 1 || len(values[0]) > maxComfyUIResultValue {
			return comfyUIResultReadError(http.StatusBadRequest, "invalid ComfyUI jobs query")
		}
		if status, message := validateComfyUIJobQueryValue(key, values[0]); status != 0 {
			return comfyUIResultReadError(status, message)
		}
	}
	return true, 0, ""
}

func validateComfyUIJobQueryValue(key, value string) (int, string) {
	switch key {
	case "limit", "offset":
		if !validComfyUIJobPagination(key, value) {
			return http.StatusBadRequest, "invalid ComfyUI jobs pagination"
		}
	case "status":
		if !validComfyUIJobStatuses(value) {
			return http.StatusBadRequest, "invalid ComfyUI job status"
		}
	case "sort_by":
		if value != "created_at" && value != "execution_duration" {
			return http.StatusBadRequest, "invalid ComfyUI jobs sort field"
		}
	case "sort_order":
		if value != "asc" && value != "desc" {
			return http.StatusBadRequest, "invalid ComfyUI jobs sort order"
		}
	case "workflow_id":
		if !validComfyUIPromptID(value) {
			return http.StatusBadRequest, "invalid ComfyUI workflow ID"
		}
	default:
		return http.StatusBadRequest, "unsupported ComfyUI jobs query"
	}
	return 0, ""
}

func validComfyUIJobPagination(key, value string) bool {
	number, err := strconv.Atoi(value)
	if err != nil || number < 0 {
		return false
	}
	if key == "limit" && (number == 0 || number > 1000) {
		return false
	}
	return true
}

func validComfyUIJobStatuses(value string) bool {
	for _, status := range strings.Split(value, ",") {
		switch status {
		case "pending", "in_progress", "completed", "failed", "cancelled":
		default:
			return false
		}
	}
	return true
}

func validateComfyUIHistoryQuery(requestPath string, query url.Values) (bool, int, string) {
	if requestPath != comfyUIHistoryPath && len(query) != 0 {
		return comfyUIResultReadError(http.StatusBadRequest, "history item path cannot contain query parameters")
	}
	for key, values := range query {
		if !validComfyUIHistoryKey(key) || len(values) != 1 || len(values[0]) > maxComfyUIResultValue {
			return comfyUIResultReadError(http.StatusBadRequest, "unsupported ComfyUI history query")
		}
		if status, message := validateComfyUIHistoryValue(key, values[0]); status != 0 {
			return comfyUIResultReadError(status, message)
		}
	}
	return true, 0, ""
}

func validComfyUIHistoryKey(key string) bool {
	return key == "prompt_id" || key == "max_items" || key == "offset"
}

func validateComfyUIHistoryValue(key, value string) (int, string) {
	switch key {
	case "prompt_id":
		if !validComfyUIPromptID(value) {
			return http.StatusBadRequest, "invalid ComfyUI prompt ID"
		}
	case "max_items":
		limit, err := strconv.Atoi(value)
		if err != nil || limit < 1 || limit > 1000 {
			return http.StatusBadRequest, "invalid ComfyUI history limit"
		}
	case "offset":
		offset, err := strconv.Atoi(value)
		if err != nil || offset < -1 {
			return http.StatusBadRequest, "invalid ComfyUI history offset"
		}
	}
	return 0, ""
}

func validateComfyUIViewQuery(query url.Values, allowOwnerHash bool) (bool, int, string) {
	filename, status, message := validateComfyUIViewQueryValues(query)
	if status != 0 {
		return comfyUIResultReadError(status, message)
	}
	if !validComfyUIOutputFilename(filename) {
		return comfyUIResultReadError(http.StatusBadRequest, "invalid ComfyUI output filename")
	}
	if !allowOwnerHash && strings.HasPrefix(strings.ToLower(filename), "blake3:") {
		return comfyUIResultReadError(http.StatusForbidden, "ComfyUI hash filenames require a verified caller identity")
	}
	return true, 0, ""
}

func validateComfyUIViewQueryValues(query url.Values) (string, int, string) {
	filename := ""
	for key, values := range query {
		if !validComfyUIViewKey(key) || len(values) != 1 || len(values[0]) > maxComfyUIResultValue {
			return "", http.StatusBadRequest, "unsupported ComfyUI view query"
		}
		value := values[0]
		switch key {
		case "filename":
			filename = value
		case "subfolder":
			if !validComfyUIOutputSubfolder(value) {
				return "", http.StatusBadRequest, "invalid ComfyUI output subfolder"
			}
		case "type":
			if !validComfyUIViewType(value) {
				return "", http.StatusForbidden, "unsupported ComfyUI result file type"
			}
		case "preview":
			if !validComfyUIViewPreview(value) {
				return "", http.StatusBadRequest, "invalid ComfyUI image preview format"
			}
		case "rand":
			if !validComfyUIViewCacheBuster(value) {
				return "", http.StatusBadRequest, "invalid ComfyUI image cache buster"
			}
		case "channel":
			if value != "rgba" && value != "rgb" && value != "a" {
				return "", http.StatusBadRequest, "invalid ComfyUI image channel"
			}
		}
	}
	return filename, 0, ""
}

func validComfyUIOutputFilename(filename string) bool {
	return filename != "" && filename != "." && filename != ".." &&
		path.Base(filename) == filename && !strings.ContainsAny(filename, "\\\x00")
}

func validComfyUIViewKey(key string) bool {
	// The ComfyUI frontend adds preview-format and cache-busting parameters to
	// /view requests; keep the allowlist aligned with those image URLs.
	switch key {
	case "filename", "subfolder", "type", "format", "preview", "rand", "channel":
		return true
	default:
		return false
	}
}

func validComfyUIViewType(value string) bool {
	return value == "" || value == "input" || value == "output" || value == "temp"
}

func validComfyUIViewPreview(value string) bool {
	format, quality, hasQuality := strings.Cut(value, ";")
	if format != "webp" && format != "jpeg" {
		return false
	}
	if !hasQuality {
		return true
	}
	number, err := strconv.Atoi(quality)
	return err == nil && number >= 1 && number <= 100
}

func validComfyUIViewCacheBuster(value string) bool {
	if value == "" || len(value) > 64 {
		return false
	}
	for _, character := range value {
		if !(character >= '0' && character <= '9') && character != '.' && character != '-' && character != '_' {
			return false
		}
	}
	return true
}

func validComfyUIOutputSubfolder(value string) bool {
	return value == "" || (!strings.HasPrefix(value, "/") && !strings.ContainsAny(value, "\\\x00") &&
		path.Clean(value) == value && value != ".." && !strings.HasPrefix(value, "../"))
}

func validComfyUIResultPathEncoding(rawPath string) bool {
	lower := strings.ToLower(rawPath)
	return !strings.Contains(lower, "%2f") && !strings.Contains(lower, "%5c")
}

func validComfyUIPromptID(promptID string) bool {
	if promptID == "" || len(promptID) > maxComfyUIPromptIDBytes {
		return false
	}
	for _, character := range promptID {
		if !(character >= 'a' && character <= 'z') && !(character >= 'A' && character <= 'Z') &&
			!(character >= '0' && character <= '9') && character != '-' && character != '_' {
			return false
		}
	}
	return true
}

func comfyUIResultRequestHeaders(source http.Header) http.Header {
	allowed := []string{"Accept", "Accept-Encoding", "Cache-Control", "If-Modified-Since", "If-None-Match", "If-Range", "Range", "User-Agent"}
	target := make(http.Header)
	for _, name := range allowed {
		for _, value := range source.Values(name) {
			target.Add(name, value)
		}
	}
	return target
}

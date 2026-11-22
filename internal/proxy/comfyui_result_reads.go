package proxy

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

const (
	comfyUIHistoryPath      = "/comfyui/history"
	comfyUIViewPath         = "/comfyui/view"
	maxComfyUIResultQuery   = 4096
	maxComfyUIResultValue   = 2048
	maxComfyUIPromptIDBytes = 128
)

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
		Rewrite: func(request *httputil.ProxyRequest) {
			request.SetURL(target)
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
			request.Out.Header = comfyUIResultRequestHeaders(request.In.Header)
			request.SetXForwarded()
			applyBackendHeaders(request.Out.Header, backend)
			if isComfyUIImageUpload(request.In) {
				request.Out.Header.Set("Content-Type", request.In.Header.Get("Content-Type"))
			}
			if comfyUIJSONSaveKind(request.In) != "" {
				request.Out.Header.Set("Content-Type", "application/json")
			}
			if websocketUpgrade(request.In.Header) {
				request.Out.Header.Set("Connection", "Upgrade")
				request.Out.Header.Set("Upgrade", "websocket")
				for _, name := range []string{"Sec-WebSocket-Key", "Sec-WebSocket-Version", "Sec-WebSocket-Protocol", "Sec-WebSocket-Extensions"} {
					request.Out.Header.Del(name)
					for _, value := range request.In.Header.Values(name) {
						request.Out.Header.Add(name, value)
					}
				}
			}
			setRequestIDHeader(request.Out.Header, requestIDFromContext(request.Out.Context()))
		},
		ModifyResponse: func(response *http.Response) error {
			if response.Request != nil {
				setRequestIDHeader(response.Header, requestIDFromContext(response.Request.Context()))
			}
			for _, name := range []string{"Authorization", "Proxy-Authenticate", "Set-Cookie", "WWW-Authenticate"} {
				response.Header.Del(name)
			}
			return nil
		},
		FlushInterval: -1,
		ErrorHandler:  proxyErrorHandler(logger, "comfyui-results"),
	}, nil
}

func (h *Handler) serveComfyUIResultRead(writer http.ResponseWriter, request *http.Request, info *routeInfo, allowOwnerHash bool) bool {
	if h.comfyUIResultReadProxy == nil {
		return false
	}
	if h.comfyUIImageUploadMaxBytes > 0 && isComfyUIImageUpload(request) {
		h.serveComfyUIImageUpload(writer, request, info)
		return true
	}
	if kind := comfyUIJSONSaveKind(request); kind != "" {
		h.serveComfyUIJSONSave(writer, request, info, kind)
		return true
	}
	matched, status, message := validateComfyUIFrontendRequest(request)
	frontend := matched
	if !matched {
		matched, status, message = validateComfyUIResultReadRequest(request, allowOwnerHash)
	}
	if !matched {
		return false
	}
	if status != 0 {
		if status == http.StatusMethodNotAllowed {
			writer.Header().Set("Allow", "GET")
		}
		writeJSONError(h.logger, writer, status, message)
		return true
	}
	if frontend && isComfyUIFrontendRoot(request) {
		// Bootstrap the persistent service through llama-swap. Once started, the
		// rest of the allowlisted frontend and result reads use the direct target.
		return false
	}
	if request.URL.Path == "/comfyui" {
		location := "/comfyui/"
		if request.URL.RawQuery != "" {
			location += "?" + request.URL.RawQuery
		}
		http.Redirect(writer, request, location, http.StatusTemporaryRedirect)
		return true
	}
	slots := h.comfyUIResultSlots
	if frontend && h.comfyUIFrontendSlots != nil {
		slots = h.comfyUIFrontendSlots
	}
	if websocketUpgrade(request.Header) {
		slots = h.comfyUIWebSocketSlots
	}
	select {
	case slots <- struct{}{}:
		defer func() { <-slots }()
	default:
		writeJSONError(h.logger, writer, http.StatusServiceUnavailable, "ComfyUI result service busy")
		return true
	}
	*info = routeInfo{backendID: "comfyui-results", kind: routeInfoKindPath}
	h.comfyUIResultReadProxy.ServeHTTP(writer, request)
	return true
}

func validateComfyUIResultReadRequest(request *http.Request, allowOwnerHash bool) (bool, int, string) {
	if request == nil || request.URL == nil {
		return false, 0, ""
	}
	requestPath := request.URL.Path
	// Normalize only verified core API aliases for classification. Preserve the
	// original URL when forwarding to ComfyUI.
	if strings.HasPrefix(requestPath, "/comfyui/api/") && !strings.HasPrefix(requestPath, "/comfyui/api/jobs") {
		requestPath = "/comfyui/" + strings.TrimPrefix(requestPath, "/comfyui/api/")
	}
	kind := ""
	switch {
	case requestPath == "/comfyui/prompt" || requestPath == "/comfyui/queue" || requestPath == "/comfyui/system_stats":
		if request.Method != http.MethodGet {
			return false, 0, ""
		}
		kind = "status"
	case requestPath == "/comfyui/api/jobs" || strings.HasPrefix(requestPath, "/comfyui/api/jobs/"):
		if request.Method != http.MethodGet {
			return false, 0, ""
		}
		kind = "jobs"
	case requestPath == comfyUIHistoryPath || strings.HasPrefix(requestPath, comfyUIHistoryPath+"/"):
		kind = "history"
	case requestPath == comfyUIViewPath || strings.HasPrefix(requestPath, comfyUIViewPath+"/"):
		kind = "view"
	default:
		return false, 0, ""
	}
	if request.Method != http.MethodGet {
		return true, http.StatusMethodNotAllowed, "ComfyUI result reads allow only GET"
	}
	if request.Body != nil && request.Body != http.NoBody || request.ContentLength > 0 || len(request.TransferEncoding) != 0 {
		return true, http.StatusBadRequest, "ComfyUI result reads do not accept request bodies"
	}
	if request.URL.RawPath != "" && !validComfyUIResultPathEncoding(request.URL.RawPath) {
		return true, http.StatusBadRequest, "invalid ComfyUI result path encoding"
	}
	if path.Clean(request.URL.Path) != request.URL.Path || strings.ContainsAny(requestPath, "\\?#\x00") {
		return true, http.StatusBadRequest, "invalid ComfyUI result path"
	}
	if kind == "history" && requestPath != comfyUIHistoryPath {
		promptID := strings.TrimPrefix(requestPath, comfyUIHistoryPath+"/")
		if !validComfyUIPromptID(promptID) {
			return true, http.StatusNotFound, "ComfyUI history item not found"
		}
	}
	if kind == "view" && requestPath != comfyUIViewPath {
		return true, http.StatusNotFound, "ComfyUI result file not found"
	}
	if len(request.URL.RawQuery) > maxComfyUIResultQuery {
		return true, http.StatusBadRequest, "ComfyUI result query is too long"
	}
	query, err := url.ParseQuery(request.URL.RawQuery)
	if err != nil {
		return true, http.StatusBadRequest, "invalid ComfyUI result query"
	}
	if kind == "status" {
		if len(query) != 0 || request.URL.ForceQuery {
			return true, http.StatusBadRequest, "ComfyUI status reads do not accept query parameters"
		}
		return true, 0, ""
	}
	if kind == "jobs" {
		if requestPath != "/comfyui/api/jobs" {
			if !validComfyUIPromptID(strings.TrimPrefix(requestPath, "/comfyui/api/jobs/")) || len(query) != 0 {
				return true, http.StatusBadRequest, "invalid ComfyUI job status path or query"
			}
			return true, 0, ""
		}
		for key, values := range query {
			if len(values) != 1 || len(values[0]) > maxComfyUIResultValue {
				return true, http.StatusBadRequest, "invalid ComfyUI jobs query"
			}
			value := values[0]
			switch key {
			case "limit", "offset":
				number, err := strconv.Atoi(value)
				if err != nil || number < 0 || (key == "limit" && (number == 0 || number > 1000)) {
					return true, http.StatusBadRequest, "invalid ComfyUI jobs pagination"
				}
			case "status":
				for _, status := range strings.Split(value, ",") {
					switch status {
					case "pending", "in_progress", "completed", "failed", "cancelled":
					default:
						return true, http.StatusBadRequest, "invalid ComfyUI job status"
					}
				}
			case "sort_by":
				if value != "created_at" && value != "execution_duration" {
					return true, http.StatusBadRequest, "invalid ComfyUI jobs sort field"
				}
			case "sort_order":
				if value != "asc" && value != "desc" {
					return true, http.StatusBadRequest, "invalid ComfyUI jobs sort order"
				}
			case "workflow_id":
				if !validComfyUIPromptID(value) {
					return true, http.StatusBadRequest, "invalid ComfyUI workflow ID"
				}
			default:
				return true, http.StatusBadRequest, "unsupported ComfyUI jobs query"
			}
		}
		return true, 0, ""
	}
	if kind == "history" {
		if requestPath != comfyUIHistoryPath && len(query) != 0 {
			return true, http.StatusBadRequest, "history item path cannot contain query parameters"
		}
		for key, values := range query {
			if (key != "prompt_id" && key != "max_items" && key != "offset") || len(values) != 1 || len(values[0]) > maxComfyUIResultValue {
				return true, http.StatusBadRequest, "unsupported ComfyUI history query"
			}
			if key == "prompt_id" && !validComfyUIPromptID(values[0]) {
				return true, http.StatusBadRequest, "invalid ComfyUI prompt ID"
			}
			if key == "max_items" {
				limit, err := strconv.Atoi(values[0])
				if err != nil || limit < 1 || limit > 1000 {
					return true, http.StatusBadRequest, "invalid ComfyUI history limit"
				}
			}
			if key == "offset" {
				offset, err := strconv.Atoi(values[0])
				if err != nil || offset < -1 {
					return true, http.StatusBadRequest, "invalid ComfyUI history offset"
				}
			}
		}
		return true, 0, ""
	}
	allowed := map[string]bool{"filename": true, "subfolder": true, "type": true, "format": true}
	filename := ""
	for key, values := range query {
		if !allowed[key] || len(values) != 1 || len(values[0]) > maxComfyUIResultValue {
			return true, http.StatusBadRequest, "unsupported ComfyUI view query"
		}
		value := values[0]
		switch key {
		case "filename":
			filename = value
		case "subfolder":
			if value != "" && (strings.HasPrefix(value, "/") || strings.ContainsAny(value, "\\\x00") ||
				path.Clean(value) != value || value == ".." || strings.HasPrefix(value, "../")) {
				return true, http.StatusBadRequest, "invalid ComfyUI output subfolder"
			}
		case "type":
			if value != "" && value != "output" {
				return true, http.StatusForbidden, "ComfyUI result reads are limited to output files"
			}
		}
	}
	if filename == "" || filename == "." || filename == ".." || path.Base(filename) != filename || strings.ContainsAny(filename, "\\\x00") {
		return true, http.StatusBadRequest, "invalid ComfyUI output filename"
	}
	if !allowOwnerHash && strings.HasPrefix(strings.ToLower(filename), "blake3:") {
		return true, http.StatusForbidden, "ComfyUI hash filenames require a verified caller identity"
	}
	return true, 0, ""
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

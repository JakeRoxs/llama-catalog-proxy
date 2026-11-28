package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strings"
	"unicode/utf8"
)

const (
	maxComfyUISettingsSaveBytes = 1 << 20
	maxComfyUIWorkflowSaveBytes = 4 << 20

	// comfyUIUserDataPathPrefix is the path fragment under the ComfyUI
	// frontend root under which user data files are served.
	comfyUIUserDataPathPrefix = "/userdata/"
)

func comfyUIJSONSaveKind(request *http.Request) string {
	if request == nil || request.URL == nil || request.Method != http.MethodPost || !strings.HasPrefix(request.URL.Path, "/comfyui/") {
		return ""
	}
	endpoint := strings.TrimPrefix(request.URL.Path, comfyUIPathPrefix)
	if strings.HasPrefix(endpoint, "/api/") {
		endpoint = strings.TrimPrefix(endpoint, "/api")
	}
	switch {
	case endpoint == "/settings":
		return "settings"
	case strings.HasPrefix(endpoint, "/settings/"):
		return "setting"
	case strings.HasPrefix(endpoint, comfyUIUserDataPathPrefix):
		return "workflow"
	default:
		return ""
	}
}

// validateComfyUIJSONSaveRequest validates the request path and headers for a
// ComfyUI JSON save request. It returns false and writes an error response if
// the request is invalid.
func validateComfyUIJSONSaveRequest(logger *slog.Logger, w http.ResponseWriter, request *http.Request, kind string) bool {
	if len(request.URL.Path) > 4096 || path.Clean(request.URL.Path) != request.URL.Path ||
		strings.ContainsAny(request.URL.Path, "\\%:\x00\r\n") ||
		(kind != "workflow" && request.URL.RawPath != "" && !validComfyUIResultPathEncoding(request.URL.RawPath)) ||
		len(request.URL.RawQuery) > maxComfyUIResultQuery || request.Header.Get("Content-Encoding") != "" ||
		websocketUpgrade(request.Header) {
		writeJSONError(logger, w, http.StatusBadRequest, "invalid ComfyUI JSON save request")
		return false
	}
	return true
}

// validateComfyUIJSONSaveKind validates kind-specific request constraints and
// returns the effective body size limit through limit. It returns false and
// writes an error response if the request is invalid.
func validateComfyUIJSONSaveKind(logger *slog.Logger, w http.ResponseWriter, request *http.Request, endpoint string, kind string, limit *int64) bool {
	if kind == "setting" {
		settingID := strings.TrimPrefix(endpoint, "/settings/")
		if settingID == "" || strings.Contains(settingID, "/") || strings.HasPrefix(settingID, ".") {
			writeJSONError(logger, w, http.StatusBadRequest, "invalid ComfyUI setting ID")
			return false
		}
	}
	if kind == "workflow" {
		filename := strings.TrimPrefix(endpoint, comfyUIUserDataPathPrefix)
		escapedEndpoint := strings.TrimPrefix(request.URL.EscapedPath(), comfyUIPathPrefix)
		if strings.HasPrefix(escapedEndpoint, "/api/") {
			escapedEndpoint = strings.TrimPrefix(escapedEndpoint, "/api")
		}
		// One escaped parameter distinguishes a save from /userdata/{file}/move/{dest}.
		if !strings.HasPrefix(escapedEndpoint, comfyUIUserDataPathPrefix) || strings.Contains(strings.TrimPrefix(escapedEndpoint, comfyUIUserDataPathPrefix), "/") {
			writeJSONError(logger, w, http.StatusBadRequest, "nested ComfyUI save filenames must be URL-encoded")
			return false
		}
		if !validComfyUIUserDataPath(filename) || path.Ext(filename) != ".json" ||
			(!strings.HasPrefix(filename, "workflows/") && filename != "comfy.templates.json" && filename != "comfy.keybindings.json") {
			writeJSONError(logger, w, http.StatusForbidden, "ComfyUI saves are limited to workflow and named preference JSON files")
			return false
		}
		*limit = maxComfyUIWorkflowSaveBytes
	}
	return true
}

// validateComfyUIJSONSaveQuery validates the query parameters for a ComfyUI
// JSON save request. It returns false and writes an error response if the
// request is invalid.
func validateComfyUIJSONSaveQuery(logger *slog.Logger, w http.ResponseWriter, request *http.Request, kind string) bool {
	query, err := url.ParseQuery(request.URL.RawQuery)
	if err != nil || (kind != "workflow" && (len(query) != 0 || request.URL.ForceQuery)) {
		writeJSONError(logger, w, http.StatusBadRequest, "invalid ComfyUI save query")
		return false
	}
	for name, values := range query {
		if (name != "overwrite" && name != "full_info") || len(values) != 1 || (values[0] != "true" && values[0] != "false") {
			writeJSONError(logger, w, http.StatusBadRequest, "unsupported ComfyUI save query")
			return false
		}
	}
	return true
}

// validateComfyUIJSONSaveContentType validates the Content-Type header for a
// ComfyUI JSON save request. It returns false and writes an error response if
// the request is invalid.
func validateComfyUIJSONSaveContentType(logger *slog.Logger, w http.ResponseWriter, request *http.Request) bool {
	if len(request.Header.Values("Content-Type")) > 1 {
		writeJSONError(logger, w, http.StatusBadRequest, "ambiguous ComfyUI JSON content type")
		return false
	}
	if contentType := request.Header.Get("Content-Type"); contentType != "" {
		mediaType, parameters, err := mime.ParseMediaType(contentType)
		if err != nil || (mediaType != "application/json" && mediaType != "text/plain") ||
			(parameters["charset"] != "" && !strings.EqualFold(parameters["charset"], "utf-8")) {
			writeJSONError(logger, w, http.StatusUnsupportedMediaType, "ComfyUI saves require UTF-8 JSON")
			return false
		}
	}
	return true
}

// validateComfyUIJSONSaveContentLength validates the request body size for a
// ComfyUI JSON save request. It returns false and writes an error response if
// the request is invalid.
func validateComfyUIJSONSaveContentLength(logger *slog.Logger, w http.ResponseWriter, request *http.Request, limit int64) bool {
	if request.ContentLength > limit {
		writeJSONError(logger, w, http.StatusRequestEntityTooLarge, "ComfyUI JSON save exceeds size limit")
		return false
	}
	return true
}

// acquireComfyUIFrontendSlot acquires a concurrency slot for a ComfyUI frontend
// save request. It returns a release function and whether acquisition succeeded.
// It writes an error response and returns nil, false if no slot is available.
func acquireComfyUIFrontendSlot(h *Handler, w http.ResponseWriter) (func(), bool) {
	slots := h.comfyUIFrontendSlots
	if slots == nil {
		slots = h.comfyUIResultSlots
	}
	select {
	case slots <- struct{}{}:
		return func() { <-slots }, true
	default:
		writeJSONError(h.logger, w, http.StatusServiceUnavailable, "ComfyUI frontend service busy")
		return nil, false
	}
}

// readComfyUIJSONSaveBody reads and validates the request body for a ComfyUI
// JSON save request. It returns the body and whether validation succeeded. It
// writes an error response and returns nil, false if the body is invalid.
func readComfyUIJSONSaveBody(logger *slog.Logger, w http.ResponseWriter, request *http.Request, limit int64, kind string) ([]byte, bool) {
	if request.Body == nil {
		writeJSONError(logger, w, http.StatusBadRequest, "ComfyUI JSON save body required")
		return nil, false
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, request.Body, limit))
	_ = request.Body.Close()
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSONError(logger, w, http.StatusRequestEntityTooLarge, "ComfyUI JSON save exceeds size limit")
		} else {
			writeJSONError(logger, w, http.StatusBadRequest, "could not read ComfyUI JSON save")
		}
		return nil, false
	}
	trimmed := bytes.TrimSpace(body)
	if !utf8.Valid(body) || !json.Valid(body) || (kind == "settings" && (len(trimmed) == 0 || trimmed[0] != '{')) {
		writeJSONError(logger, w, http.StatusBadRequest, "invalid ComfyUI JSON save payload")
		return nil, false
	}
	return body, true
}

func (h *Handler) serveComfyUIJSONSave(w http.ResponseWriter, request *http.Request, info *routeInfo, kind string) {
	endpoint := strings.TrimPrefix(request.URL.Path, comfyUIPathPrefix)
	if strings.HasPrefix(endpoint, "/api/") {
		endpoint = strings.TrimPrefix(endpoint, "/api")
	}
	if !validateComfyUIJSONSaveRequest(h.logger, w, request, kind) {
		return
	}
	limit := int64(maxComfyUISettingsSaveBytes)
	if !validateComfyUIJSONSaveKind(h.logger, w, request, endpoint, kind, &limit) {
		return
	}
	if !validateComfyUIJSONSaveQuery(h.logger, w, request, kind) {
		return
	}
	if !validateComfyUIJSONSaveContentType(h.logger, w, request) {
		return
	}
	if !validateComfyUIJSONSaveContentLength(h.logger, w, request, limit) {
		return
	}
	releaseSlot, ok := acquireComfyUIFrontendSlot(h, w)
	if !ok {
		return
	}
	defer releaseSlot()
	body, ok := readComfyUIJSONSaveBody(h.logger, w, request, limit, kind)
	if !ok {
		return
	}
	forwarded := request.Clone(request.Context())
	forwarded.Body = io.NopCloser(bytes.NewReader(body))
	forwarded.GetBody = nil
	forwarded.ContentLength = int64(len(body))
	forwarded.TransferEncoding = nil
	forwarded.Trailer = nil
	*info = routeInfo{backendID: "comfyui-results", kind: routeInfoKindPath}
	h.comfyUIResultReadProxy.ServeHTTP(w, forwarded)
}

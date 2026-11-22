package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
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

func (h *Handler) serveComfyUIJSONSave(w http.ResponseWriter, request *http.Request, info *routeInfo, kind string) {
	endpoint := strings.TrimPrefix(request.URL.Path, comfyUIPathPrefix)
	if strings.HasPrefix(endpoint, "/api/") {
		endpoint = strings.TrimPrefix(endpoint, "/api")
	}
	if len(request.URL.Path) > 4096 || path.Clean(request.URL.Path) != request.URL.Path ||
		strings.ContainsAny(request.URL.Path, "\\%:\x00\r\n") ||
		(kind != "workflow" && request.URL.RawPath != "" && !validComfyUIResultPathEncoding(request.URL.RawPath)) ||
		len(request.URL.RawQuery) > maxComfyUIResultQuery || request.Header.Get("Content-Encoding") != "" ||
		websocketUpgrade(request.Header) {
		writeJSONError(h.logger, w, http.StatusBadRequest, "invalid ComfyUI JSON save request")
		return
	}
	limit := int64(maxComfyUISettingsSaveBytes)
	if kind == "setting" {
		settingID := strings.TrimPrefix(endpoint, "/settings/")
		if settingID == "" || strings.Contains(settingID, "/") || strings.HasPrefix(settingID, ".") {
			writeJSONError(h.logger, w, http.StatusBadRequest, "invalid ComfyUI setting ID")
			return
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
			writeJSONError(h.logger, w, http.StatusBadRequest, "nested ComfyUI save filenames must be URL-encoded")
			return
		}
		if !validComfyUIUserDataPath(filename) || path.Ext(filename) != ".json" ||
			(!strings.HasPrefix(filename, "workflows/") && filename != "comfy.templates.json" && filename != "comfy.keybindings.json") {
			writeJSONError(h.logger, w, http.StatusForbidden, "ComfyUI saves are limited to workflow and named preference JSON files")
			return
		}
		limit = maxComfyUIWorkflowSaveBytes
	}
	query, err := url.ParseQuery(request.URL.RawQuery)
	if err != nil || (kind != "workflow" && (len(query) != 0 || request.URL.ForceQuery)) {
		writeJSONError(h.logger, w, http.StatusBadRequest, "invalid ComfyUI save query")
		return
	}
	for name, values := range query {
		if (name != "overwrite" && name != "full_info") || len(values) != 1 || (values[0] != "true" && values[0] != "false") {
			writeJSONError(h.logger, w, http.StatusBadRequest, "unsupported ComfyUI save query")
			return
		}
	}
	if len(request.Header.Values("Content-Type")) > 1 {
		writeJSONError(h.logger, w, http.StatusBadRequest, "ambiguous ComfyUI JSON content type")
		return
	}
	if contentType := request.Header.Get("Content-Type"); contentType != "" {
		mediaType, parameters, err := mime.ParseMediaType(contentType)
		if err != nil || (mediaType != "application/json" && mediaType != "text/plain") ||
			(parameters["charset"] != "" && !strings.EqualFold(parameters["charset"], "utf-8")) {
			writeJSONError(h.logger, w, http.StatusUnsupportedMediaType, "ComfyUI saves require UTF-8 JSON")
			return
		}
	}
	if request.ContentLength > limit {
		writeJSONError(h.logger, w, http.StatusRequestEntityTooLarge, "ComfyUI JSON save exceeds size limit")
		return
	}
	slots := h.comfyUIFrontendSlots
	if slots == nil {
		slots = h.comfyUIResultSlots
	}
	select {
	case slots <- struct{}{}:
		defer func() { <-slots }()
	default:
		writeJSONError(h.logger, w, http.StatusServiceUnavailable, "ComfyUI frontend service busy")
		return
	}
	if request.Body == nil {
		writeJSONError(h.logger, w, http.StatusBadRequest, "ComfyUI JSON save body required")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, request.Body, limit))
	_ = request.Body.Close()
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSONError(h.logger, w, http.StatusRequestEntityTooLarge, "ComfyUI JSON save exceeds size limit")
		} else {
			writeJSONError(h.logger, w, http.StatusBadRequest, "could not read ComfyUI JSON save")
		}
		return
	}
	trimmed := bytes.TrimSpace(body)
	if !utf8.Valid(body) || !json.Valid(body) || (kind == "settings" && (len(trimmed) == 0 || trimmed[0] != '{')) {
		writeJSONError(h.logger, w, http.StatusBadRequest, "invalid ComfyUI JSON save payload")
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

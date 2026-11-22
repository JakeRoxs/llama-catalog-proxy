package proxy

import (
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"net/url"
	"path"
	"strings"
)

func validateComfyUIFrontendRequest(request *http.Request) (bool, int, string) {
	if request == nil || request.URL == nil || request.Method != http.MethodGet {
		return false, 0, ""
	}
	if request.URL.Path != "/comfyui" && !strings.HasPrefix(request.URL.Path, "/comfyui/") {
		return false, 0, ""
	}
	endpoint := strings.TrimPrefix(request.URL.Path, "/comfyui")
	coreEndpoint := endpoint
	if strings.HasPrefix(coreEndpoint, "/api/") {
		coreEndpoint = strings.TrimPrefix(coreEndpoint, "/api")
	}
	static := false
	socket := coreEndpoint == "/ws"
	root := endpoint == "" || endpoint == "/" || endpoint == "/index.html"
	metadata := false
	userListing := coreEndpoint == "/userdata" || coreEndpoint == "/v2/userdata"
	userFile := strings.HasPrefix(coreEndpoint, "/userdata/")
	switch coreEndpoint {
	case "/features", "/extensions", "/embeddings", "/models", "/object_info", "/users", "/settings", "/workflow_templates":
		metadata = true
	case "/i18n", "/global_subgraphs", "/node_replacements":
		metadata = true
	}
	if id, ok := strings.CutPrefix(coreEndpoint, "/global_subgraphs/"); ok {
		metadata = true
		if len(id) != 64 || strings.ToLower(id) != id {
			return true, http.StatusBadRequest, "invalid ComfyUI subgraph ID"
		}
		if _, err := hex.DecodeString(id); err != nil {
			return true, http.StatusBadRequest, "invalid ComfyUI subgraph ID"
		}
	}
	for _, prefix := range []string{"/models/", "/object_info/", "/settings/"} {
		if strings.HasPrefix(coreEndpoint, prefix) {
			metadata = true
			component := strings.TrimPrefix(coreEndpoint, prefix)
			if component == "" || strings.ContainsAny(component, "/\\\x00") || component == "." || component == ".." {
				return true, http.StatusBadRequest, "invalid ComfyUI metadata path"
			}
		}
	}
	for _, prefix := range []string{"/assets/", "/scripts/", "/extensions/", "/locales/", "/fonts/", "/templates/"} {
		if strings.HasPrefix(endpoint, prefix) {
			static = true
		}
	}
	if endpoint == "/favicon.ico" || endpoint == "/user.css" || endpoint == "/materialdesignicons.min.css" {
		static = true
	}
	if !root && !static && !metadata && !socket && !userListing && !userFile {
		return false, 0, ""
	}
	if len(request.URL.Path) > 4096 || (!userFile && request.URL.RawPath != "" && !validComfyUIResultPathEncoding(request.URL.RawPath)) ||
		strings.ContainsAny(endpoint, "\\\x00") ||
		(!root && path.Clean(request.URL.Path) != request.URL.Path) {
		return true, http.StatusBadRequest, "invalid ComfyUI frontend path"
	}
	for _, component := range strings.Split(endpoint, "/") {
		if strings.HasPrefix(component, ".") {
			return true, http.StatusBadRequest, "invalid ComfyUI frontend path"
		}
	}
	if userFile {
		filename := strings.TrimPrefix(coreEndpoint, "/userdata/")
		if !validComfyUIUserDataPath(filename) {
			return true, http.StatusBadRequest, "invalid ComfyUI user data path"
		}
		switch strings.ToLower(path.Ext(filename)) {
		case ".json", ".css":
		default:
			return true, http.StatusForbidden, "ComfyUI user data reads are limited to JSON and CSS files"
		}
	}
	if static {
		switch strings.ToLower(path.Ext(endpoint)) {
		case ".js", ".mjs", ".css", ".json", ".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico", ".woff", ".woff2", ".ttf", ".eot", ".otf", ".wasm":
		default:
			return true, http.StatusNotFound, "unsupported ComfyUI frontend asset"
		}
	}
	if request.Body != nil && request.Body != http.NoBody || request.ContentLength > 0 || len(request.TransferEncoding) != 0 {
		return true, http.StatusBadRequest, "ComfyUI frontend GET requests cannot contain bodies"
	}
	if len(request.URL.RawQuery) > maxComfyUIResultQuery {
		return true, http.StatusBadRequest, "ComfyUI frontend query is too long"
	}
	query, err := url.ParseQuery(request.URL.RawQuery)
	if err != nil {
		return true, http.StatusBadRequest, "invalid ComfyUI frontend query"
	}
	if userFile {
		if len(query) != 0 {
			return true, http.StatusBadRequest, "ComfyUI user data file reads do not accept query parameters"
		}
		return true, 0, ""
	}
	if userListing {
		if coreEndpoint == "/userdata" && !validComfyUIUserDataPath(query.Get("dir")) {
			return true, http.StatusBadRequest, "ComfyUI user data listing requires a safe directory"
		}
		for name, values := range query {
			if len(values) != 1 || len(values[0]) > 2048 {
				return true, http.StatusBadRequest, "invalid ComfyUI user data listing query"
			}
			if coreEndpoint == "/v2/userdata" {
				if name != "path" || (values[0] != "" && !validComfyUIUserDataPath(values[0])) {
					return true, http.StatusBadRequest, "invalid ComfyUI user data listing path"
				}
				continue
			}
			switch name {
			case "dir":
				if !validComfyUIUserDataPath(values[0]) {
					return true, http.StatusBadRequest, "invalid ComfyUI user data listing directory"
				}
			case "recurse", "full_info", "split":
				if values[0] != "true" && values[0] != "false" {
					return true, http.StatusBadRequest, "invalid ComfyUI user data listing option"
				}
			default:
				return true, http.StatusBadRequest, "unsupported ComfyUI user data listing query"
			}
		}
		return true, 0, ""
	}
	if socket {
		key, err := base64.StdEncoding.DecodeString(request.Header.Get("Sec-WebSocket-Key"))
		if !websocketUpgrade(request.Header) || request.Header.Get("Sec-WebSocket-Version") != "13" || err != nil || len(key) != 16 {
			return true, http.StatusBadRequest, "invalid ComfyUI WebSocket handshake"
		}
		for name, values := range query {
			if name != "clientId" || len(values) != 1 || !validComfyUIPromptID(values[0]) {
				return true, http.StatusBadRequest, "invalid ComfyUI WebSocket client ID"
			}
		}
	} else {
		if websocketUpgrade(request.Header) {
			return true, http.StatusBadRequest, "WebSocket upgrades require the ComfyUI ws endpoint"
		}
		if metadata && len(query) != 0 {
			return true, http.StatusBadRequest, "ComfyUI metadata does not accept query parameters"
		}
		for name, values := range query {
			if metadata || (name != "v" && name != "t") || len(values) != 1 || len(values[0]) > 128 {
				return true, http.StatusBadRequest, "unsupported ComfyUI frontend query"
			}
		}
	}
	return true, 0, ""
}

func isComfyUIFrontendRoot(request *http.Request) bool {
	return request != nil && request.URL != nil && request.Method == http.MethodGet &&
		(request.URL.Path == "/comfyui/" || request.URL.Path == "/comfyui/index.html")
}

func validComfyUIUserDataPath(value string) bool {
	if value == "" || len(value) > 2048 || strings.HasPrefix(value, "/") || path.Clean(value) != value ||
		strings.ContainsAny(value, "\\%:\x00\r\n") {
		return false
	}
	for _, component := range strings.Split(value, "/") {
		if strings.HasPrefix(component, ".") {
			return false
		}
	}
	return true
}

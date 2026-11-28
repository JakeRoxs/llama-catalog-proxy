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
	route, status, message := classifyComfyUIFrontendRoute(endpoint, coreEndpoint)
	if status != 0 {
		return true, status, message
	}
	if !route.matched {
		return false, 0, ""
	}
	if status, message = validateComfyUIFrontendPath(request, endpoint, route); status != 0 {
		return true, status, message
	}
	if route.userFile {
		if status, message = validateComfyUIUserFile(coreEndpoint); status != 0 {
			return true, status, message
		}
	}
	if route.static && !validComfyUIFrontendAsset(endpoint) {
		return true, http.StatusNotFound, "unsupported ComfyUI frontend asset"
	}
	return validateComfyUIFrontendRequestQuery(request, coreEndpoint, route)
}

func validateComfyUIFrontendRequestQuery(
	request *http.Request, coreEndpoint string, route comfyUIFrontendRoute,
) (bool, int, string) {
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
	if route.userFile {
		if len(query) != 0 {
			return true, http.StatusBadRequest, "ComfyUI user data file reads do not accept query parameters"
		}
		return true, 0, ""
	}
	if route.userListing {
		return validateComfyUIUserListingQuery(coreEndpoint, query)
	}
	return validateComfyUIFrontendQuery(request, route, query)
}

type comfyUIFrontendRoute struct {
	matched     bool
	root        bool
	static      bool
	socket      bool
	metadata    bool
	userListing bool
	userFile    bool
}

func classifyComfyUIFrontendRoute(endpoint, coreEndpoint string) (comfyUIFrontendRoute, int, string) {
	route := comfyUIFrontendRoute{
		socket:      coreEndpoint == "/ws",
		root:        endpoint == "" || endpoint == "/" || endpoint == "/index.html",
		userListing: coreEndpoint == "/userdata" || coreEndpoint == "/v2/userdata",
		userFile:    strings.HasPrefix(coreEndpoint, "/userdata/"),
	}
	switch coreEndpoint {
	case "/features", "/extensions", "/embeddings", "/models", "/object_info", "/users", "/settings", "/workflow_templates",
		"/i18n", "/global_subgraphs", "/node_replacements", "/experiment/models":
		route.metadata = true
	}
	if status, message := validateComfyUISubgraphRoute(coreEndpoint, &route); status != 0 {
		return route, status, message
	}
	if status, message := validateComfyUIMetadataRoute(coreEndpoint, &route); status != 0 {
		return route, status, message
	}
	route.static = isComfyUIStaticRoute(endpoint)
	route.matched = route.root || route.static || route.metadata || route.socket || route.userListing || route.userFile
	return route, 0, ""
}

func validateComfyUISubgraphRoute(coreEndpoint string, route *comfyUIFrontendRoute) (int, string) {
	id, ok := strings.CutPrefix(coreEndpoint, "/global_subgraphs/")
	if !ok {
		return 0, ""
	}
	route.metadata = true
	if len(id) != 64 || strings.ToLower(id) != id {
		return http.StatusBadRequest, "invalid ComfyUI subgraph ID"
	}
	if _, err := hex.DecodeString(id); err != nil {
		return http.StatusBadRequest, "invalid ComfyUI subgraph ID"
	}
	return 0, ""
}

func validateComfyUIMetadataRoute(coreEndpoint string, route *comfyUIFrontendRoute) (int, string) {
	for _, prefix := range []string{"/models/", "/object_info/", "/settings/", "/experiment/models/"} {
		if !strings.HasPrefix(coreEndpoint, prefix) {
			continue
		}
		component := strings.TrimPrefix(coreEndpoint, prefix)
		if prefix == "/experiment/models/" && strings.HasPrefix(component, "preview/") {
			continue
		}
		route.metadata = true
		if component == "" || strings.ContainsAny(component, "/\\\x00") || component == "." || component == ".." {
			return http.StatusBadRequest, "invalid ComfyUI metadata path"
		}
	}
	return 0, ""
}

func isComfyUIStaticRoute(endpoint string) bool {
	for _, prefix := range []string{"/assets/", "/scripts/", "/extensions/", "/locales/", "/fonts/", "/templates/"} {
		if strings.HasPrefix(endpoint, prefix) {
			return true
		}
	}
	return endpoint == "/favicon.ico" || endpoint == "/user.css" || endpoint == "/materialdesignicons.min.css"
}

func validateComfyUIFrontendPath(request *http.Request, endpoint string, route comfyUIFrontendRoute) (int, string) {
	if len(request.URL.Path) > 4096 || (!route.userFile && request.URL.RawPath != "" && !validComfyUIResultPathEncoding(request.URL.RawPath)) ||
		strings.ContainsAny(endpoint, "\\\x00") ||
		(!route.root && path.Clean(request.URL.Path) != request.URL.Path) {
		return http.StatusBadRequest, "invalid ComfyUI frontend path"
	}
	for _, component := range strings.Split(endpoint, "/") {
		if strings.HasPrefix(component, ".") {
			return http.StatusBadRequest, "invalid ComfyUI frontend path"
		}
	}
	return 0, ""
}

func validateComfyUIUserFile(coreEndpoint string) (int, string) {
	filename := strings.TrimPrefix(coreEndpoint, "/userdata/")
	if !validComfyUIUserDataPath(filename) {
		return http.StatusBadRequest, "invalid ComfyUI user data path"
	}
	switch strings.ToLower(path.Ext(filename)) {
	case ".json", ".css":
		return 0, ""
	default:
		return http.StatusForbidden, "ComfyUI user data reads are limited to JSON and CSS files"
	}
}

func validComfyUIFrontendAsset(endpoint string) bool {
	switch strings.ToLower(path.Ext(endpoint)) {
	case ".js", ".mjs", ".css", ".json", ".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico", ".woff", ".woff2", ".ttf", ".eot", ".otf", ".wasm":
		return true
	default:
		return false
	}
}

func validateComfyUIUserListingQuery(coreEndpoint string, query url.Values) (bool, int, string) {
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
		if status, message := validateComfyUIUserListingParameter(name, values[0]); status != 0 {
			return true, status, message
		}
	}
	return true, 0, ""
}

func validateComfyUIUserListingParameter(name, value string) (int, string) {
	switch name {
	case "dir":
		if !validComfyUIUserDataPath(value) {
			return http.StatusBadRequest, "invalid ComfyUI user data listing directory"
		}
	case "recurse", "full_info", "split":
		if value != "true" && value != "false" {
			return http.StatusBadRequest, "invalid ComfyUI user data listing option"
		}
	default:
		return http.StatusBadRequest, "unsupported ComfyUI user data listing query"
	}
	return 0, ""
}

func validateComfyUIFrontendQuery(request *http.Request, route comfyUIFrontendRoute, query url.Values) (bool, int, string) {
	if route.socket {
		return validateComfyUIWebSocketQuery(request, query)
	}
	if websocketUpgrade(request.Header) {
		return true, http.StatusBadRequest, "WebSocket upgrades require the ComfyUI ws endpoint"
	}
	if route.metadata && len(query) != 0 {
		return true, http.StatusBadRequest, "ComfyUI metadata does not accept query parameters"
	}
	for name, values := range query {
		if route.metadata || (name != "v" && name != "t") || len(values) != 1 || len(values[0]) > 128 {
			return true, http.StatusBadRequest, "unsupported ComfyUI frontend query"
		}
	}
	return true, 0, ""
}

func validateComfyUIWebSocketQuery(request *http.Request, query url.Values) (bool, int, string) {
	key, err := base64.StdEncoding.DecodeString(request.Header.Get("Sec-WebSocket-Key"))
	if !websocketUpgrade(request.Header) || request.Header.Get("Sec-WebSocket-Version") != "13" || err != nil || len(key) != 16 {
		return true, http.StatusBadRequest, "invalid ComfyUI WebSocket handshake"
	}
	for name, values := range query {
		if name != "clientId" || len(values) != 1 || !validComfyUIPromptID(values[0]) {
			return true, http.StatusBadRequest, "invalid ComfyUI WebSocket client ID"
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

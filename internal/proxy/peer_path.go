package proxy

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"slices"
	"sort"
	"strings"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/identity"
)

const (
	peerPathEndpoint       = "/peer/v1/path"
	peerPathServiceHeader  = "X-Catalog-Peer-Path-Service"
	peerServicePathHeader  = "X-Catalog-Peer-Service-Path"
	maxPeerServicePathSize = 8192
)

type peerPathRoute struct {
	prefix  string
	peerID  string
	service string
}

func buildPeerPathRoutes(peer *config.PeerConfig) []peerPathRoute {
	if peer == nil {
		return nil
	}
	var routes []peerPathRoute
	for peerID, grant := range peer.Peers {
		for prefix, service := range grant.RoutePaths {
			routes = append(routes, peerPathRoute{prefix: prefix, peerID: peerID, service: service})
		}
	}
	sort.Slice(routes, func(i, j int) bool {
		if len(routes[i].prefix) != len(routes[j].prefix) {
			return len(routes[i].prefix) > len(routes[j].prefix)
		}
		return routes[i].prefix < routes[j].prefix
	})
	return routes
}

func (h *Handler) servePeerPathRoute(w http.ResponseWriter, req *http.Request, info *routeInfo) bool {
	var route peerPathRoute
	matched := false
	for _, candidate := range h.peerPathRoutes {
		if req.URL.Path == candidate.prefix || strings.HasPrefix(req.URL.Path, candidate.prefix+"/") {
			route, matched = candidate, true
			break
		}
	}
	if !matched {
		return false
	}
	*info = routeInfo{backendID: route.peerID, kind: "peer_path"}
	if !allowedPeerPathMethod(req.Method) {
		w.Header().Set("Allow", "GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS")
		writeJSONError(h.logger, w, http.StatusMethodNotAllowed, "method not allowed for peer path service")
		return true
	}
	client := h.peerClients[route.peerID]
	if client == nil || client.routePaths[route.prefix] != route.service {
		writeJSONError(h.logger, w, http.StatusServiceUnavailable, "peer path service unavailable")
		return true
	}
	// The route is explicitly configured; the owner validates the service name,
	// current path mapping, and caller's path grant on every request.
	select {
	case h.peerPathSlots <- struct{}{}:
		defer func() { <-h.peerPathSlots }()
	default:
		writeJSONError(h.logger, w, http.StatusServiceUnavailable, "peer path service busy")
		return true
	}
	target, err := url.Parse(client.origin)
	if err != nil {
		writeJSONError(h.logger, w, http.StatusServiceUnavailable, "peer path service unavailable")
		return true
	}
	encodedPath := base64.RawURLEncoding.EncodeToString([]byte(req.URL.EscapedPath()))
	proxy := &httputil.ReverseProxy{
		Rewrite: func(proxyReq *httputil.ProxyRequest) {
			proxyReq.SetURL(target)
			proxyReq.Out.URL.Path = peerPathEndpoint
			proxyReq.Out.URL.RawPath = ""
			proxyReq.Out.URL.RawQuery = proxyReq.In.URL.RawQuery
			proxyReq.Out.URL.ForceQuery = proxyReq.In.URL.ForceQuery
			proxyReq.Out.Header = peerPathForwardHeaders(proxyReq.In.Header)
			proxyReq.Out.Header.Set("Authorization", "Bearer "+client.token)
			proxyReq.Out.Header.Set(peerPathServiceHeader, route.service)
			proxyReq.Out.Header.Set(peerServicePathHeader, encodedPath)
			setRequestIDHeader(proxyReq.Out.Header, requestIDFromContext(proxyReq.Out.Context()))
		},
		Transport: client.inferenceTransport,
		ErrorHandler: func(writer http.ResponseWriter, request *http.Request, err error) {
			h.logger.Warn("peer path request failed", "peer_id", route.peerID, "error", err)
			writeJSONError(h.logger, writer, http.StatusBadGateway, "peer path service unavailable")
		},
	}
	proxy.ServeHTTP(w, req)
	return true
}

func (r *Reloader) servePeerPath(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if !allowedPeerPathMethod(req.Method) {
		w.Header().Set("Allow", "GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	select {
	case r.peerPathSlots <- struct{}{}:
		defer func() { <-r.peerPathSlots }()
	default:
		http.Error(w, "peer path service busy", http.StatusServiceUnavailable)
		return
	}
	state := r.state.Load()
	peerID, authorized := authenticatedPeer(req, state, peerPathService)
	if !authorized {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	service, err := exactlyOneHeader(req.Header, peerPathServiceHeader, 128)
	if err != nil || !identity.ValidNodeID(service) {
		http.Error(w, "invalid path service", http.StatusBadRequest)
		return
	}
	grant := state.peer.Peers[peerID]
	if !slices.Contains(grant.PathAccess, service) {
		http.Error(w, "path service is not authorized", http.StatusForbidden)
		return
	}
	localPrefix, exposed := state.peer.ExposedPathRoutes[service]
	if !exposed {
		http.Error(w, "path service unavailable", http.StatusServiceUnavailable)
		return
	}
	encodedPath, err := exactlyOneHeader(req.Header, peerServicePathHeader, maxPeerServicePathSize)
	if err != nil {
		http.Error(w, "invalid path service request", http.StatusBadRequest)
		return
	}
	escapedPathBytes, err := base64.RawURLEncoding.DecodeString(encodedPath)
	if err != nil || len(escapedPathBytes) == 0 || len(escapedPathBytes) > maxPeerServicePathSize {
		http.Error(w, "invalid path service request", http.StatusBadRequest)
		return
	}
	escapedPath := string(escapedPathBytes)
	requestPath, err := url.PathUnescape(escapedPath)
	if err != nil || requestPath == "" || requestPath[0] != '/' || path.Clean(requestPath) != requestPath ||
		strings.ContainsAny(requestPath, "?#\\") || !pathHasPrefix(requestPath, localPrefix) {
		http.Error(w, "invalid path service request", http.StatusBadRequest)
		return
	}
	handler, ok := state.handler.(*Handler)
	if !ok {
		http.Error(w, "path service unavailable", http.StatusServiceUnavailable)
		return
	}
	backendID, matchedPrefix, matched := handler.routePathMatch(requestPath)
	if !matched || matchedPrefix != localPrefix {
		http.Error(w, "path service unavailable", http.StatusServiceUnavailable)
		return
	}
	forwarded := req.Clone(req.Context())
	forwarded.URL.Path = requestPath
	forwarded.URL.RawPath = escapedPath
	forwarded.Header = peerPathBackendHeaders(req.Header)
	forwarded.Host = ""
	handler.servePathRoute(w, forwarded, &routeInfo{}, backendID)
}

func exactlyOneHeader(header http.Header, name string, limit int) (string, error) {
	values := header.Values(name)
	if len(values) != 1 || values[0] == "" || len(values[0]) > limit {
		return "", fmt.Errorf("invalid %s header", name)
	}
	return values[0], nil
}

func allowedPeerPathMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodOptions:
		return true
	default:
		return false
	}
}

func pathHasPrefix(requestPath, prefix string) bool {
	return requestPath == prefix || strings.HasPrefix(requestPath, prefix+"/")
}

func peerPathForwardHeaders(source http.Header) http.Header {
	return copyPeerPathHeaders(source)
}

func peerPathBackendHeaders(source http.Header) http.Header {
	return copyPeerPathHeaders(source)
}

func copyPeerPathHeaders(source http.Header) http.Header {
	allowed := []string{
		"Accept", "Accept-Encoding", "Cache-Control", "Content-Encoding", "Content-Type", "Origin",
		"If-Modified-Since", "If-None-Match", "If-Range", "Last-Event-ID", "Range", "Referer", "User-Agent",
		"X-Requested-With",
	}
	target := make(http.Header)
	for _, name := range allowed {
		for _, value := range source.Values(name) {
			target.Add(name, value)
		}
	}
	if websocketUpgrade(source) {
		target.Set("Connection", "Upgrade")
		target.Set("Upgrade", "websocket")
		for _, name := range []string{"Sec-WebSocket-Key", "Sec-WebSocket-Version", "Sec-WebSocket-Protocol", "Sec-WebSocket-Extensions"} {
			for _, value := range source.Values(name) {
				target.Add(name, value)
			}
		}
	}
	return target
}

func websocketUpgrade(header http.Header) bool {
	if !strings.EqualFold(strings.TrimSpace(header.Get("Upgrade")), "websocket") {
		return false
	}
	for _, value := range header.Values("Connection") {
		for _, token := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(token), "upgrade") {
				return true
			}
		}
	}
	return false
}

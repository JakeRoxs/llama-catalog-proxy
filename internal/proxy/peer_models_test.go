package proxy

import (
	"bytes"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/catalog"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/nodecontract"
)

func TestPeerModelIDsAndCatalogCollisions(t *testing.T) {
	now := time.Now()
	a := nodecontract.ModelIdentity{OwnerNodeID: "node-a", BackendID: "backend/one", LocalID: "alias/model"}
	b := nodecontract.ModelIdentity{OwnerNodeID: "node-b", BackendID: "backend/one", LocalID: "alias/model"}
	for _, id := range []nodecontract.ModelIdentity{a, b} {
		got, err := parsePeerPublicID(peerPublicID(id))
		if err != nil || got != id {
			t.Fatalf("round trip: %#v %v", got, err)
		}
	}
	if peerPublicID(a) == peerPublicID(b) {
		t.Fatal("different owners generated the same ID")
	}
	for _, invalid := range []string{"@peer/node-a/!!!!/YQ", "@peer/node-a/YQ==/YQ", "@peer/node-a/YQ", "@peer/node-a/YQ/YQ/extra"} {
		if _, err := parsePeerPublicID(invalid); err == nil {
			t.Fatalf("accepted invalid peer ID %q", invalid)
		}
	}
	cache := &peerCache{entries: map[string]peerCacheEntry{}}
	for _, id := range []nodecontract.ModelIdentity{b, a} {
		receipt := &nodecontract.Receipt{AuthenticatedNodeID: id.OwnerNodeID, ReceivedAt: now,
			Report: nodecontract.Report{Observations: []nodecontract.Observation{{
				Identity: id, State: "supported", ObservedAt: &now, Freshness: "fresh", Coverage: "unknown",
			}}}}
		cache.update(id.OwnerNodeID, receipt, "", now)
	}
	h := &Handler{logger: discardLogger(), peerCache: cache}
	response, err := h.mergePeerCatalog(catalog.Response{Body: []byte(`{"object":"list","data":[{"id":"local"},{"id":"@peer/conflict"}]}`)})
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Data) != 3 || result.Data[0].ID != "local" ||
		result.Data[1].ID != peerPublicID(a) || result.Data[2].ID != peerPublicID(b) ||
		strings.Contains(string(response.Body), `"id":"@peer/conflict"`) {
		t.Fatalf("combined catalog: %s", response.Body)
	}
}

func TestPeerRouteAliasDeduplicatesLegacyBackendPrefix(t *testing.T) {
	now := time.Now()
	identity := nodecontract.ModelIdentity{OwnerNodeID: "bigboi", BackendID: "local", LocalID: "comfyui_auto"}
	expiresAt := now.Add(time.Minute)
	cache := &peerCache{entries: map[string]peerCacheEntry{"bigboi": {routeInference: true}}}
	cache.update("bigboi", &nodecontract.Receipt{
		AuthenticatedNodeID: "bigboi", ReceivedAt: now,
		Report: nodecontract.Report{Observations: []nodecontract.Observation{{
			Identity: identity, Aliases: []string{"comfyui-alias"}, State: "supported", ObservedAt: &now, ExpiresAt: &expiresAt, Freshness: "fresh", Coverage: "complete",
		}}},
	}, "", now)
	h := &Handler{logger: discardLogger(), peerCache: cache,
		peerClients: map[string]*peerClient{"bigboi": {routeInference: true}}}
	response, err := h.mergePeerCatalog(catalog.Response{Body: []byte(`{"data":[{"id":"udms/local"},{"id":"bigboi/comfyui_auto"}]}`)})
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Data []struct {
			ID      string         `json:"id"`
			OwnedBy string         `json:"owned_by"`
			Meta    map[string]any `json:"meta"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Data) != 2 || result.Data[0].ID != "udms/local" || result.Data[1].ID != "bigboi/comfyui_auto" || result.Data[1].OwnedBy != "bigboi" {
		t.Fatalf("combined catalog did not preserve aliases/provenance: %s", response.Body)
	}
	if got := result.Data[1].Meta["llamaswap"].(map[string]any)["aliases"].([]any); len(got) != 1 || got[0] != "comfyui-alias" {
		t.Fatalf("peer alias metadata = %#v", result.Data[1].Meta)
	}
	for _, tc := range []struct {
		backend string
		model   string
		found   bool
	}{{"", "comfyui-alias", true}, {"local", "comfyui-alias", true}, {"other", "comfyui-alias", false}} {
		resolved, found := cache.modelForRoute("bigboi", tc.backend, tc.model)
		if found != tc.found || (found && resolved != identity) {
			t.Fatalf("route backend=%q model=%q resolved=%#v found=%v", tc.backend, tc.model, resolved, found)
		}
	}
}

// newPeerModelRouteTestHandler builds a Handler and peer identity for the peer
// model alias routing tests. It constructs a fresh receipt for owner "bigboi"
// with the given local model ID and a cache entry whose routeInference flag is
// true; cacheStatus is passed through to cache.update (e.g. "" or "unavailable").
func newPeerModelRouteTestHandler(t *testing.T, localID, cacheStatus string) (*Handler, nodecontract.ModelIdentity) {
	t.Helper()
	now := time.Now()
	identity := nodecontract.ModelIdentity{OwnerNodeID: "bigboi", BackendID: "local", LocalID: localID}
	expiresAt := now.Add(time.Minute)
	receipt := &nodecontract.Receipt{AuthenticatedNodeID: "bigboi", ReceivedAt: now,
		Report: nodecontract.Report{Source: nodecontract.Source{NodeID: "bigboi"}, GeneratedAt: now,
			Observations: []nodecontract.Observation{{Identity: identity, State: "supported", ObservedAt: &now, ExpiresAt: &expiresAt, Freshness: "fresh"}}}}
	cache := &peerCache{entries: map[string]peerCacheEntry{"bigboi": {routeInference: true}}}
	cache.update("bigboi", receipt, cacheStatus, now)
	h := &Handler{logger: discardLogger(), maxJSONBody: 4096, peerCache: cache,
		peerClients: map[string]*peerClient{"bigboi": {routeInference: true}}}
	return h, identity
}

// servePeerModelTestRequest builds a JSON peer-model request, records the
// response, and invokes the peer model route. It returns the request (so the
// caller can verify body restoration), the response recorder, and whether the
// route handled the request.
func servePeerModelTestRequest(t *testing.T, h *Handler, path, body string) (*http.Request, *httptest.ResponseRecorder, bool) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handled := h.servePeerModelRoute(response, req, &routeInfo{})
	return req, response, handled
}

func TestPeerModelAliasDoesNotInterceptComfyUIPathAPI(t *testing.T) {
	h, _ := newPeerModelRouteTestHandler(t, "comfyui_auto", "")
	body := `{"prompt":{"1":{"class_type":"KSampler"}}}`
	req, response, handled := servePeerModelTestRequest(t, h, "/comfyui/prompt", body)
	if handled {
		t.Fatalf("ComfyUI path was intercepted by model alias router: status=%d", response.Code)
	}
	restored, err := io.ReadAll(req.Body)
	if err != nil || string(restored) != body {
		t.Fatalf("ComfyUI path body was changed: %q (%v)", restored, err)
	}
}

func TestPeerRouteAliasIsReservedWhilePeerInferenceIsConfigured(t *testing.T) {
	h, _ := newPeerModelRouteTestHandler(t, "model", "unavailable")
	body := `{"model":"bigboi/model"}`
	_, response, handled := servePeerModelTestRequest(t, h, "/v1/chat/completions", body)
	if !handled || response.Code != http.StatusServiceUnavailable {
		t.Fatalf("stale alias fell back: handled=%v status=%d", handled, response.Code)
	}
	h.peerClients["bigboi"].routeInference = false
	_, response, handled = servePeerModelTestRequest(t, h, "/v1/chat/completions", body)
	if handled {
		t.Fatalf("disabled peer alias intercepted the legacy static route: status=%d", response.Code)
	}
}

func TestMultipartPeerInspection(t *testing.T) {
	peerID := peerPublicID(nodecontract.ModelIdentity{OwnerNodeID: "node-a", BackendID: "local", LocalID: "model"})
	assertMultipartPeerInspectionTable(t, peerID)
	body, contentType := makeMultipartBody(t, "model", "local")
	assertOversizedStreamingBody(t, body, contentType)
	assertInvalidBoundaryRejected(t)
	assertValidOwnerRewrite(t, body, contentType)
	missing, missingContentType := makeMultipartBody(t, "other", "value")
	assertMissingModelRejected(t, missing, missingContentType)
}

// assertMultipartPeerInspectionTable runs the five multipart peer model
// inspection cases: pass-through (with body restoration), duplicate, case
// variant, and invalid peer ID rejection.
func assertMultipartPeerInspectionTable(t *testing.T, peerID string) {
	for _, tc := range []struct {
		name       string
		fields     []string
		wantStatus int
		handled    bool
	}{
		{"missing model passes through", nil, http.StatusOK, false},
		{"local model passes through", []string{"model", "local"}, http.StatusOK, false},
		{"duplicate model rejected", []string{"model", peerID, "model", "local"}, http.StatusBadRequest, true},
		{"case variant rejected", []string{"Model", peerID}, http.StatusBadRequest, true},
		{"invalid peer ID rejected", []string{"model", "@peer/invalid"}, http.StatusBadRequest, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertMultipartPeerCase(t, tc.fields, tc.wantStatus, tc.handled)
		})
	}
}

// assertMultipartPeerCase builds a multipart request from the given fields,
// serves it through the peer model route, and asserts the handled/status
// outcome plus body restoration when not handled.
func assertMultipartPeerCase(t *testing.T, fields []string, wantStatus int, handled bool) {
	body, contentType := makeMultipartBody(t, fields...)
	req := httptest.NewRequest(http.MethodPost, "/v1/audio/transcriptions", bytes.NewReader(body))
	req.Header.Set("Content-Type", contentType)
	response := httptest.NewRecorder()
	h := &Handler{logger: discardLogger(), maxJSONBody: 4096}
	gotHandled := h.servePeerModelRoute(response, req, &routeInfo{})
	if gotHandled != handled || response.Code != wantStatus {
		t.Fatalf("handled=%v status=%d", gotHandled, response.Code)
	}
	if !handled {
		restored, err := io.ReadAll(req.Body)
		if err != nil || !bytes.Equal(restored, body) {
			t.Fatalf("local body changed: %v", err)
		}
	}
}

// assertOversizedStreamingBody verifies the streaming size cap: a multipart
// body larger than maxJSONBody is rejected with 413.
func assertOversizedStreamingBody(t *testing.T, body []byte, contentType string) {
	h := &Handler{logger: discardLogger(), maxJSONBody: int64(len(body) - 1)}
	req := httptest.NewRequest(http.MethodPost, "/v1/audio/transcriptions", bytes.NewReader(body))
	req.ContentLength = -1
	req.Header.Set("Content-Type", contentType)
	response := httptest.NewRecorder()
	if !h.servePeerModelRoute(response, req, &routeInfo{}) || response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized streaming body status=%d", response.Code)
	}
}

// assertInvalidBoundaryRejected verifies that a multipart request with an
// invalid boundary is rejected with 400.
func assertInvalidBoundaryRejected(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/audio/transcriptions", strings.NewReader("invalid"))
	req.Header.Set("Content-Type", "multipart/form-data; boundary=example")
	response := httptest.NewRecorder()
	h := &Handler{logger: discardLogger(), maxJSONBody: 4096}
	if !h.servePeerModelRoute(response, req, &routeInfo{}) || response.Code != http.StatusBadRequest {
		t.Fatalf("invalid boundary status=%d", response.Code)
	}
}

// assertValidOwnerRewrite verifies that rewriteMultipartModel accepts a valid
// model field owned by the expected owner and rewrites it.
func assertValidOwnerRewrite(t *testing.T, body []byte, contentType string) {
	boundary := strings.TrimPrefix(contentType, "multipart/form-data; boundary=")
	if _, err := rewriteMultipartModel(body, boundary, "local", "local", 4096); err != nil {
		t.Fatalf("owner rejected valid multipart model: %v", err)
	}
}

// assertMissingModelRejected verifies that rewriteMultipartModel rejects a
// multipart body that has no model field.
func assertMissingModelRejected(t *testing.T, body []byte, contentType string) {
	boundary := strings.TrimPrefix(contentType, "multipart/form-data; boundary=")
	if _, err := rewriteMultipartModel(body, boundary, "local", "local", 4096); err == nil {
		t.Fatal("owner accepted missing multipart model")
	}
}

// makeMultipartBody builds a multipart form-data body from alternating field
// name/value pairs and returns the body bytes plus its content type.
func makeMultipartBody(t *testing.T, fields ...string) ([]byte, string) {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for i := 0; i < len(fields); i += 2 {
		if err := writer.WriteField(fields[i], fields[i+1]); err != nil {
			t.Fatal(err)
		}
	}
	contentType := writer.FormDataContentType()
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return body.Bytes(), contentType
}

func TestPeerRoutingRejectsAmbiguousModelFields(t *testing.T) {
	id := peerPublicID(nodecontract.ModelIdentity{OwnerNodeID: "node-a", BackendID: "local", LocalID: "model"})
	for _, body := range []string{
		`{"model":"` + id + `","model":"local"}`,
		`{"model":"local","Model":"` + id + `"}`,
	} {
		_, _, ambiguous, err := jsonPeerModel([]byte(body), func(value string) bool { return strings.HasPrefix(value, peerModelPrefix) })
		if err != nil || !ambiguous {
			t.Fatalf("ambiguous request accepted: %s", body)
		}

	}
	if _, err := verifyOwnerModelBody([]byte(`{"model":"x","model":"y"}`), "y"); err == nil {
		t.Fatal("owner accepted duplicate model fields")
	}
	if _, err := verifyOwnerModelBody([]byte(`{"model":"@peer/local/model"}`), "@peer/local/model"); err != nil {
		t.Fatal("owner could not route an explicitly owned model ID containing the reserved prefix")
	}
}

func TestPeerRoutingRequiresFreshSourceEvidence(t *testing.T) {
	now := time.Now()
	expiresAt := now.Add(5 * time.Minute)
	id := nodecontract.ModelIdentity{OwnerNodeID: "node-a", BackendID: "local", LocalID: "model"}
	receipt := &nodecontract.Receipt{AuthenticatedNodeID: "node-a", ReceivedAt: now, Report: nodecontract.Report{
		Source:      nodecontract.Source{NodeID: "node-a", ProxyInstanceID: strings.Repeat("a", 32), ConfigGeneration: 1},
		GeneratedAt: now, Capabilities: nodecontract.Capabilities{"catalog_read": "enabled"},
		Observations: []nodecontract.Observation{{Identity: id, State: "supported", ObservedAt: &now, ExpiresAt: &expiresAt, Freshness: "fresh"}},
	}}
	cache := &peerCache{entries: map[string]peerCacheEntry{"node-a": {}}}
	cache.update("node-a", receipt, "", now)
	if _, ok := cache.freshModel(id, now); !ok {
		t.Fatal("fresh model rejected")
	}
	h := &Handler{logger: discardLogger(), maxJSONBody: 4096, peerCache: cache,
		peerClients: map[string]*peerClient{"node-a": {routeInference: false}}}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"`+peerPublicID(id)+`"}`))
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	if !h.servePeerModelRoute(response, req, &routeInfo{}) || response.Code != http.StatusServiceUnavailable {
		t.Fatalf("outbound inference without grant status=%d", response.Code)
	}
	if _, ok := cache.freshModel(id, now.Add(peerFreshAge+time.Second)); ok {
		t.Fatal("stale peer receipt accepted despite valid owner evidence")
	}
	reportAt := now.Add(peerFreshAge + time.Second)
	refreshedReceipt := *receipt
	refreshedReceipt.ReceivedAt = reportAt
	refreshedReceipt.Report.GeneratedAt = reportAt
	cache.update("node-a", &refreshedReceipt, "", reportAt)
	if _, ok := cache.freshModel(id, reportAt); !ok {
		t.Fatal("owner evidence inside its validity window rejected after a fresh peer report")
	}
	if _, ok := cache.freshModel(id, expiresAt); ok {
		t.Fatal("expired owner catalog evidence accepted")
	}
	cache.update("node-a", nil, "unavailable", now)
	if _, ok := cache.freshModel(id, now); ok {
		t.Fatal("failed refresh remained routable")
	}
}

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
	cache := &peerCache{entries: map[string]peerCacheEntry{"bigboi": {routeInference: true}}}
	cache.update("bigboi", &nodecontract.Receipt{
		AuthenticatedNodeID: "bigboi", ReceivedAt: now,
		Report: nodecontract.Report{Observations: []nodecontract.Observation{{
			Identity: identity, State: "supported", ObservedAt: &now, Freshness: "fresh", Coverage: "complete",
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
			ID      string `json:"id"`
			OwnedBy string `json:"owned_by"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Data) != 2 || result.Data[0].ID != "udms/local" || result.Data[1].ID != "bigboi/comfyui_auto" || result.Data[1].OwnedBy != "bigboi" {
		t.Fatalf("combined catalog did not preserve aliases/provenance: %s", response.Body)
	}
}

func TestPeerModelAliasDoesNotInterceptComfyUIPathAPI(t *testing.T) {
	now := time.Now()
	identity := nodecontract.ModelIdentity{OwnerNodeID: "bigboi", BackendID: "local", LocalID: "comfyui_auto"}
	receipt := &nodecontract.Receipt{AuthenticatedNodeID: "bigboi", ReceivedAt: now,
		Report: nodecontract.Report{Source: nodecontract.Source{NodeID: "bigboi"}, GeneratedAt: now,
			Observations: []nodecontract.Observation{{Identity: identity, State: "supported", ObservedAt: &now, Freshness: "fresh"}}}}
	cache := &peerCache{entries: map[string]peerCacheEntry{"bigboi": {routeInference: true}}}
	cache.update("bigboi", receipt, "", now)
	h := &Handler{logger: discardLogger(), maxJSONBody: 4096, peerCache: cache,
		peerClients: map[string]*peerClient{"bigboi": {routeInference: true}}}
	body := `{"prompt":{"1":{"class_type":"KSampler"}}}`
	req := httptest.NewRequest(http.MethodPost, "/comfyui/prompt", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	if h.servePeerModelRoute(response, req, &routeInfo{}) {
		t.Fatalf("ComfyUI path was intercepted by model alias router: status=%d", response.Code)
	}
	restored, err := io.ReadAll(req.Body)
	if err != nil || string(restored) != body {
		t.Fatalf("ComfyUI path body was changed: %q (%v)", restored, err)
	}
}

func TestPeerRouteAliasIsReservedWhilePeerInferenceIsConfigured(t *testing.T) {
	now := time.Now()
	identity := nodecontract.ModelIdentity{OwnerNodeID: "bigboi", BackendID: "local", LocalID: "model"}
	receipt := &nodecontract.Receipt{AuthenticatedNodeID: "bigboi", ReceivedAt: now,
		Report: nodecontract.Report{Source: nodecontract.Source{NodeID: "bigboi"}, GeneratedAt: now,
			Observations: []nodecontract.Observation{{Identity: identity, State: "supported", ObservedAt: &now, Freshness: "fresh"}}}}
	cache := &peerCache{entries: map[string]peerCacheEntry{"bigboi": {routeInference: true}}}
	cache.update("bigboi", receipt, "unavailable", now)
	h := &Handler{logger: discardLogger(), maxJSONBody: 4096, peerCache: cache,
		peerClients: map[string]*peerClient{"bigboi": {routeInference: true}}}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"bigboi/model"}`))
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handled := h.servePeerModelRoute(response, req, &routeInfo{})
	if !handled || response.Code != http.StatusServiceUnavailable {
		t.Fatalf("stale alias fell back: handled=%v status=%d", handled, response.Code)
	}
	h.peerClients["bigboi"].routeInference = false
	req = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"bigboi/model"}`))
	req.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	if h.servePeerModelRoute(response, req, &routeInfo{}) {
		t.Fatalf("disabled peer alias intercepted the legacy static route: status=%d", response.Code)
	}
}

func TestMultipartPeerInspection(t *testing.T) {
	peerID := peerPublicID(nodecontract.ModelIdentity{OwnerNodeID: "node-a", BackendID: "local", LocalID: "model"})
	makeBody := func(fields ...string) ([]byte, string) {
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
			body, contentType := makeBody(tc.fields...)
			req := httptest.NewRequest(http.MethodPost, "/v1/audio/transcriptions", bytes.NewReader(body))
			req.Header.Set("Content-Type", contentType)
			response := httptest.NewRecorder()
			h := &Handler{logger: discardLogger(), maxJSONBody: 4096}
			handled := h.servePeerModelRoute(response, req, &routeInfo{})
			if handled != tc.handled || response.Code != tc.wantStatus {
				t.Fatalf("handled=%v status=%d", handled, response.Code)
			}
			if !handled {
				restored, err := io.ReadAll(req.Body)
				if err != nil || !bytes.Equal(restored, body) {
					t.Fatalf("local body changed: %v", err)
				}
			}
		})
	}

	body, contentType := makeBody("model", "local")
	h := &Handler{logger: discardLogger(), maxJSONBody: int64(len(body) - 1)}
	req := httptest.NewRequest(http.MethodPost, "/v1/audio/transcriptions", bytes.NewReader(body))
	req.ContentLength = -1
	req.Header.Set("Content-Type", contentType)
	response := httptest.NewRecorder()
	if !h.servePeerModelRoute(response, req, &routeInfo{}) || response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized streaming body status=%d", response.Code)
	}

	req = httptest.NewRequest(http.MethodPost, "/v1/audio/transcriptions", strings.NewReader("invalid"))
	req.Header.Set("Content-Type", "multipart/form-data; boundary=example")
	response = httptest.NewRecorder()
	h.maxJSONBody = 4096
	if !h.servePeerModelRoute(response, req, &routeInfo{}) || response.Code != http.StatusBadRequest {
		t.Fatalf("invalid boundary status=%d", response.Code)
	}
	if _, err := rewriteMultipartModel(body, strings.TrimPrefix(contentType, "multipart/form-data; boundary="), "local", "local", 4096); err != nil {
		t.Fatalf("owner rejected valid multipart model: %v", err)
	}
	missing, missingContentType := makeBody("other", "value")
	if _, err := rewriteMultipartModel(missing, strings.TrimPrefix(missingContentType, "multipart/form-data; boundary="), "local", "local", 4096); err == nil {
		t.Fatal("owner accepted missing multipart model")
	}
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
	id := nodecontract.ModelIdentity{OwnerNodeID: "node-a", BackendID: "local", LocalID: "model"}
	receipt := &nodecontract.Receipt{AuthenticatedNodeID: "node-a", ReceivedAt: now, Report: nodecontract.Report{
		Source:      nodecontract.Source{NodeID: "node-a", ProxyInstanceID: strings.Repeat("a", 32), ConfigGeneration: 1},
		GeneratedAt: now, Capabilities: nodecontract.Capabilities{"catalog_read": "enabled"},
		Observations: []nodecontract.Observation{{Identity: id, State: "supported", ObservedAt: &now, Freshness: "fresh"}},
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
		t.Fatal("aged source observation accepted")
	}
	cache.update("node-a", nil, "unavailable", now)
	if _, ok := cache.freshModel(id, now); ok {
		t.Fatal("failed refresh remained routable")
	}
}

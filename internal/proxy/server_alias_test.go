package proxy

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRouteRequestPreservesPrefixedBackendAlias(t *testing.T) {
	body := `{"model":"bigboi/nemotron-bolt","messages":[]}`
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	h := &Handler{
		defaultBackend: "udms",
		maxJSONBody:    1024,
		modelRoutes:    []route{{backendID: "bigboi", prefix: "bigboi/"}},
	}

	backend, rewritten, err := h.routeRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	if backend != "bigboi" || !rewritten {
		t.Fatalf("route result = backend %q rewritten %v", backend, rewritten)
	}
	var routed struct {
		Model string `json:"model"`
	}
	if err := json.NewDecoder(req.Body).Decode(&routed); err != nil {
		t.Fatal(err)
	}
	if routed.Model != "nemotron-bolt" {
		t.Fatalf("upstream model = %q", routed.Model)
	}
}

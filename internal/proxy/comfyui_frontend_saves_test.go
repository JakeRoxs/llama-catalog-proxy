package proxy

import (
	"crypto/tls"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

func TestComfyUIJSONSavesBypassSchedulerLocallyAndOverPeer(t *testing.T) {
	var saves, scheduled atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		saves.Add(1)
		if req.Method != http.MethodPost || req.Header.Get("Content-Type") != "application/json" ||
			req.Header.Get("Authorization") != "Bearer owner-key" || req.Header.Get("Comfy-User") != "owner-user" ||
			req.Header.Get("Cookie") != "" || req.Header.Get("X-Caller-Secret") != "" || req.Header.Get("Idempotency-Key") != "" {
			t.Errorf("save method or credentials incorrect: %s %v", req.Method, req.Header)
		}
		body, err := io.ReadAll(req.Body)
		if err != nil {
			t.Errorf("read saved JSON: %v", err)
		}
		w.Header().Set("Set-Cookie", "owner=secret")
		if req.URL.Path == "/settings/unavailable" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, req.URL.RequestURI()+" "+string(body))
	}))
	defer backend.Close()
	scheduler := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		scheduled.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer scheduler.Close()
	cfg := baseConfig(scheduler.URL)
	cfg.PathRoutes = map[string]string{"/comfyui": "default"}
	cfg.Peer = &config.PeerConfig{NodeID: "owner", Listen: "127.0.0.1:9294", TLSCertFile: "cert.pem", TLSKeyFile: "key.pem",
		ComfyUIResultRead: &config.ComfyUIResultReadConfig{URL: backend.URL, APIKey: "owner-key",
			Headers: map[string]string{"Comfy-User": "owner-user", "Content-Type": "wrong-owner-default"}}}
	handler := newHandler(t, cfg).(*Handler)
	token := strings.Repeat("p", 32)
	peerConfig := &config.PeerConfig{NodeID: "owner", ExposedPathRoutes: map[string]string{"comfyui": "/comfyui"},
		Peers: map[string]config.PeerGrant{"caller": {Token: token, PathAccess: []string{"comfyui"}}}}
	owner := &Reloader{logger: discardLogger(), peerPathSlots: make(chan struct{}, 4)}
	owner.state.Store(&reloaderState{handler: handler, peer: peerConfig})
	for _, viaPeer := range []bool{false, true} {
		for _, test := range []struct{ endpoint, body, contentType string }{
			{"/settings", `{"Comfy.ColorPalette":"dark"}`, "application/json"},
			{"/api/settings", `{"Comfy.ColorPalette":"light"}`, "text/plain;charset=UTF-8"},
			{"/settings/Comfy.Workflow.Tabs", `true`, ""},
			{"/api/settings/Comfy.Workflow.Tabs", `null`, "application/json"},
			{"/userdata/workflows%2Fexample.json?overwrite=false&full_info=true", `{"nodes":[]}`, "text/plain"},
			{"/api/userdata/workflows%2Fproject%2Fexample.json?overwrite=true", `{"nodes":[]}`, "application/json"},
			{"/api/userdata/comfy.templates.json", `[]`, "text/plain;charset=UTF-8"},
			{"/userdata/comfy.keybindings.json", `[]`, ""},
		} {
			request := httptest.NewRequest(http.MethodPost, "/comfyui"+test.endpoint, strings.NewReader(test.body))
			request.Header.Set("Authorization", "Bearer caller-key")
			request.Header.Set("Cookie", "caller=secret")
			request.Header.Set("Comfy-User", "forged-caller")
			request.Header.Set("X-Caller-Secret", "secret")
			request.Header.Set("Idempotency-Key", "caller-id")
			if test.contentType != "" {
				request.Header.Set("Content-Type", test.contentType)
			}
			response := httptest.NewRecorder()
			if viaPeer {
				request.Header.Set(peerServicePathHeader, base64.RawURLEncoding.EncodeToString([]byte(request.URL.EscapedPath())))
				request.Header.Set(peerPathServiceHeader, "comfyui")
				request.Header.Set("Authorization", "Bearer "+token)
				request.URL.Path = peerPathEndpoint
				request.TLS = &tls.ConnectionState{}
				owner.PeerHandler().ServeHTTP(response, request)
			} else {
				handler.ServeHTTP(response, request)
			}
			if response.Code != http.StatusOK || response.Body.String() != test.endpoint+" "+test.body || response.Header().Get("Set-Cookie") != "" {
				t.Fatalf("peer=%v save=%s status=%d body=%q", viaPeer, test.endpoint, response.Code, response.Body.String())
			}
		}
	}
	if saves.Load() != 16 || scheduled.Load() != 0 {
		t.Fatalf("save routing: saves=%d scheduled=%d", saves.Load(), scheduled.Load())
	}
	failed := httptest.NewRecorder()
	handler.ServeHTTP(failed, httptest.NewRequest(http.MethodPost, "/comfyui/settings/unavailable", strings.NewReader(`true`)))
	if failed.Code != http.StatusServiceUnavailable || saves.Load() != 17 || scheduled.Load() != 0 {
		t.Fatalf("failed save retried or scheduled: status=%d saves=%d scheduled=%d", failed.Code, saves.Load(), scheduled.Load())
	}
	peerConfig.Peers["caller"] = config.PeerGrant{Token: token, PathAccess: []string{"other"}}
	denied := httptest.NewRequest(http.MethodPost, peerPathEndpoint, strings.NewReader(`{}`))
	denied.TLS = &tls.ConnectionState{}
	denied.Header.Set("Authorization", "Bearer "+token)
	denied.Header.Set(peerPathServiceHeader, "comfyui")
	denied.Header.Set(peerServicePathHeader, base64.RawURLEncoding.EncodeToString([]byte("/comfyui/settings")))
	response := httptest.NewRecorder()
	owner.PeerHandler().ServeHTTP(response, denied)
	if response.Code != http.StatusForbidden || saves.Load() != 17 {
		t.Fatal("peer without ComfyUI path grant saved settings")
	}
	for _, endpoint := range []string{"/comfyui/prompt", "/comfyui/api/prompt", "/comfyui/users", "/settings"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(`{}`)))
		if response.Code != http.StatusAccepted {
			t.Fatalf("non-save POST routed incorrectly: %s status=%d", endpoint, response.Code)
		}
	}
	if scheduled.Load() != 4 || saves.Load() != 17 {
		t.Fatalf("generation/account routing: saves=%d scheduled=%d", saves.Load(), scheduled.Load())
	}
}

func TestComfyUIJSONSavesRejectUnsafeRequestsBeforeForwarding(t *testing.T) {
	var forwarded atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		forwarded.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()
	resultProxy, err := newComfyUIResultReadProxy(&config.ComfyUIResultReadConfig{URL: backend.URL}, discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	handler := &Handler{logger: discardLogger(), comfyUIResultReadProxy: resultProxy, comfyUIFrontendSlots: make(chan struct{}, 1)}
	for _, test := range []struct {
		endpoint, body string
		status         int
	}{
		{"/comfyui/settings", `[]`, http.StatusBadRequest},
		{"/comfyui/settings", `null`, http.StatusBadRequest},
		{"/comfyui/settings", `{"bad":`, http.StatusBadRequest},
		{"/comfyui/settings", `{} {}`, http.StatusBadRequest},
		{"/comfyui/settings?user_id=other", `{}`, http.StatusBadRequest},
		{"/comfyui/settings/one/two", `true`, http.StatusBadRequest},
		{"/comfyui/settings%2Fone", `true`, http.StatusBadRequest},
		{"/comfyui/api/userdata/workflows%2F..%2Fsecret.json", `{}`, http.StatusBadRequest},
		{"/comfyui/api/userdata/workflows%252Fsecret.json", `{}`, http.StatusBadRequest},
		{"/comfyui/api/userdata/workflows/old.json/move/new.json", `{}`, http.StatusBadRequest},
		{"/comfyui/api/userdata/user.css", `{}`, http.StatusForbidden},
		{"/comfyui/api/userdata/arbitrary.json", `{}`, http.StatusForbidden},
		{"/comfyui/api/userdata/workflows%2Fnode.py", `{}`, http.StatusForbidden},
		{"/comfyui/api/userdata/workflows%2Fone.json?overwrite=true&overwrite=false", `{}`, http.StatusBadRequest},
		{"/comfyui/api/userdata/workflows%2Fone.json?user_id=other", `{}`, http.StatusBadRequest},
		{"/comfyui/api/userdata/workflows%2Fone.json?full_info=1", `{}`, http.StatusBadRequest},
	} {
		request := httptest.NewRequest(http.MethodPost, test.endpoint, strings.NewReader(test.body))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != test.status || forwarded.Load() != 0 {
			t.Fatalf("unsafe save %s: status=%d forwarded=%d", test.endpoint, response.Code, forwarded.Load())
		}
	}
	for _, chunked := range []bool{false, true} {
		request := httptest.NewRequest(http.MethodPost, "/comfyui/settings", strings.NewReader(strings.Repeat(" ", maxComfyUISettingsSaveBytes+1)))
		if chunked {
			request.ContentLength = -1
			request.TransferEncoding = []string{"chunked"}
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusRequestEntityTooLarge || forwarded.Load() != 0 {
			t.Fatalf("oversized chunked=%v: status=%d forwarded=%d", chunked, response.Code, forwarded.Load())
		}
	}
	for _, test := range []struct {
		name   string
		modify func(*http.Request)
		status int
	}{
		{"compressed JSON", func(r *http.Request) { r.Header.Set("Content-Encoding", "gzip") }, http.StatusBadRequest},
		{"unsupported type", func(r *http.Request) { r.Header.Set("Content-Type", "multipart/form-data; boundary=a") }, http.StatusUnsupportedMediaType},
		{"unsupported charset", func(r *http.Request) { r.Header.Set("Content-Type", "application/json;charset=utf-16") }, http.StatusUnsupportedMediaType},
		{"duplicate content type", func(r *http.Request) { r.Header["Content-Type"] = []string{"text/plain", "application/json"} }, http.StatusBadRequest},
		{"invalid UTF8", func(r *http.Request) {
			r.Body = io.NopCloser(strings.NewReader("{\"bad\":\"\xff\"}"))
			r.ContentLength = -1
		}, http.StatusBadRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/comfyui/settings", strings.NewReader(`{}`))
			test.modify(request)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.status || forwarded.Load() != 0 {
				t.Fatalf("unsafe JSON save status=%d forwarded=%d", response.Code, forwarded.Load())
			}
		})
	}
	for _, endpoint := range []string{"/comfyui/settings", "/comfyui/userdata/workflows%2Fexample.json"} {
		limit := maxComfyUISettingsSaveBytes
		if strings.Contains(endpoint, "userdata") {
			limit = maxComfyUIWorkflowSaveBytes
		}
		request := httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader("{}"+strings.Repeat(" ", limit-2)))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("exact-boundary save %s: status=%d", endpoint, response.Code)
		}
	}
	if forwarded.Load() != 2 {
		t.Fatalf("exact-boundary save calls=%d", forwarded.Load())
	}
}

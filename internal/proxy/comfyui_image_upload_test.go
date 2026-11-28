package proxy

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

var testUploadPNG = []byte("\x89PNG\r\n\x1a\nimage-test-data")

func imageUploadBody(t *testing.T, filename string, contents []byte, fields [][2]string) ([]byte, string) {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("image", filename)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Write(contents); err != nil {
		t.Fatal(err)
	}
	for _, field := range fields {
		if err := writer.WriteField(field[0], field[1]); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return body.Bytes(), writer.FormDataContentType()
}

func TestComfyUIImageUploadMultipartValidation(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		contents []byte
		fields   [][2]string
		valid    bool
	}{
		{"default input", "input.png", testUploadPNG, nil, true},
		{"explicit input", "input.png", testUploadPNG, [][2]string{{"type", "input"}, {"subfolder", "runs/one"}, {"overwrite", "false"}}, true},
		{"output disallowed", "input.png", testUploadPNG, [][2]string{{"type", "output"}}, false},
		{"temp disallowed", "input.png", testUploadPNG, [][2]string{{"type", "temp"}}, false},
		{"traversal filename", "../input.png", testUploadPNG, nil, false},
		{"traversal subfolder", "input.png", testUploadPNG, [][2]string{{"subfolder", "../other"}}, false},
		{"absolute subfolder", "input.png", testUploadPNG, [][2]string{{"subfolder", "/other"}}, false},
		{"unknown field", "input.png", testUploadPNG, [][2]string{{"user_id", "other"}}, false},
		{"duplicate field", "input.png", testUploadPNG, [][2]string{{"type", "input"}, {"type", "output"}}, false},
		{"empty image", "input.png", nil, nil, false},
		{"not an image", "input.png", []byte("this is plain text"), nil, false},
		{"executable filename", "input.py", testUploadPNG, nil, false},
		{"oversized metadata", "input.png", testUploadPNG, [][2]string{{"subfolder", strings.Repeat("x", 2049)}}, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body, contentType := imageUploadBody(t, test.filename, test.contents, test.fields)
			request := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
			request.Header.Set("Content-Type", contentType)
			boundary := strings.TrimPrefix(contentType, "multipart/form-data; boundary=")
			if got := validComfyUIImageMultipart(body, boundary); got != test.valid {
				t.Fatalf("multipart valid=%v, want %v", got, test.valid)
			}
		})
	}
	if validComfyUIImageMultipart([]byte("broken multipart"), "boundary") {
		t.Fatal("malformed multipart accepted")
	}
}

// newComfyUIImageUploadHarness builds the shared fixture for
// TestComfyUIImageUploadsBypassSchedulerLocallyAndOverPeer: a backend that
// records upload calls, a scheduler that records scheduler calls, a handler
// wired to the scheduler, and a relayer owner with a path-grant peer config.
// Callers retain ownership and must close backend and scheduler.
func newComfyUIImageUploadHarness(t *testing.T) (*Handler, *Reloader, *httptest.Server, *httptest.Server, *atomic.Int32, *atomic.Int32, string, *config.PeerConfig) {
	var uploadCalls, schedulerCalls atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodGet && req.URL.Path == "/queue" {
			_, _ = io.WriteString(w, `{"queue_running":[],"queue_pending":[]}`)
			return
		}
		uploadCalls.Add(1)
		if req.Method != http.MethodPost || (req.URL.Path != "/upload/image" && req.URL.Path != "/api/upload/image") {
			t.Errorf("unexpected upload request %s %s", req.Method, req.URL.Path)
		}
		if req.Header.Get("Authorization") != "Bearer owner-secret" || req.Header.Get("Cookie") != "" || req.Header.Get("X-Caller-Key") != "" {
			t.Errorf("upload credentials not isolated: %v", req.Header)
		}
		reader, err := req.MultipartReader()
		if err != nil {
			t.Errorf("upload boundary not preserved: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		part, err := reader.NextPart()
		if err != nil {
			t.Errorf("missing upload part: %v", err)
			return
		}
		data, err := io.ReadAll(part)
		if err != nil || !bytes.Equal(data, testUploadPNG) || part.FileName() != "input.png" {
			t.Errorf("upload content changed: filename=%q data=%q err=%v", part.FileName(), data, err)
		}
		w.Header().Set("Set-Cookie", "backend-secret=value")
		_, _ = io.WriteString(w, `{"name":"input.png","type":"input"}`)
	}))
	scheduler := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		schedulerCalls.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	cfg := baseConfig(scheduler.URL)
	cfg.PathRoutes = map[string]string{"/comfyui": "default"}
	cfg.Peer = &config.PeerConfig{NodeID: "owner", Listen: "127.0.0.1:9294", TLSCertFile: "cert.pem", TLSKeyFile: "key.pem", ComfyUIResultRead: &config.ComfyUIResultReadConfig{
		URL: backend.URL, APIKey: "owner-secret", Headers: map[string]string{"Content-Type": "application/json"},
	}}
	handler := newHandler(t, cfg).(*Handler)
	token := strings.Repeat("p", 32)
	owner := &Reloader{logger: discardLogger(), peerSlots: make(chan struct{}, 4), peerPathSlots: make(chan struct{}, 4)}
	peerConfig := &config.PeerConfig{
		NodeID: "owner", ExposedPathRoutes: map[string]string{"comfyui": "/comfyui"},
		Peers: map[string]config.PeerGrant{"caller": {Token: token, PathAccess: []string{"comfyui"}}},
	}
	owner.state.Store(&reloaderState{handler: handler, peer: peerConfig})
	return handler, owner, backend, scheduler, &uploadCalls, &schedulerCalls, token, peerConfig
}

// performUpload issues one multipart image upload either directly to the
// handler or through the owner's peer path handler, per viaPeer, and returns
// the response recorder.
func performUpload(t *testing.T, handler *Handler, owner *Reloader, viaPeer bool, endpoint, token string) *httptest.ResponseRecorder {
	body, contentType := imageUploadBody(t, "input.png", testUploadPNG, nil)
	request := httptest.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	request.Header.Set("Content-Type", contentType)
	request.Header.Set("Authorization", "Bearer caller-secret")
	request.Header.Set("Cookie", "caller=secret")
	request.Header.Set("X-Caller-Key", "caller-secret")
	response := httptest.NewRecorder()
	if viaPeer {
		request.URL.Path = peerPathEndpoint
		request.TLS = &tls.ConnectionState{}
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set(peerPathServiceHeader, "comfyui")
		request.Header.Set(peerServicePathHeader, base64.RawURLEncoding.EncodeToString([]byte(endpoint)))
		owner.PeerHandler().ServeHTTP(response, request)
	} else {
		handler.ServeHTTP(response, request)
	}
	return response
}

// assertUploadsReachBackendNotScheduler verifies image uploads reach the
// backend (not the scheduler) both directly and over the peer path, for both
// endpoints, and that exactly four uploads are recorded with zero scheduler
// calls.
func assertUploadsReachBackendNotScheduler(t *testing.T, handler *Handler, owner *Reloader, uploadCalls, schedulerCalls *atomic.Int32, token string) {
	for _, viaPeer := range []bool{false, true} {
		for _, endpoint := range []string{"/comfyui/upload/image", "/comfyui/api/upload/image"} {
			response := performUpload(t, handler, owner, viaPeer, endpoint, token)
			if response.Code != http.StatusOK || response.Header().Get("Set-Cookie") != "" {
				t.Fatalf("peer=%v upload=%s: status=%d body=%s", viaPeer, endpoint, response.Code, response.Body.String())
			}
		}
	}
	if uploadCalls.Load() != 4 || schedulerCalls.Load() != 0 {
		t.Fatalf("uploads=%d scheduler=%d", uploadCalls.Load(), schedulerCalls.Load())
	}
}

// assertUploadDeniedWithoutPathGrant verifies an upload over the peer path is
// rejected with 403 when the caller lacks the path grant, and that it is not
// forwarded to the backend (upload call count unchanged).
func assertUploadDeniedWithoutPathGrant(t *testing.T, owner *Reloader, uploadCalls *atomic.Int32, token string, peerConfig *config.PeerConfig) {
	peerConfig.Peers["caller"] = config.PeerGrant{Token: token, PathAccess: []string{"other"}}
	body, contentType := imageUploadBody(t, "input.png", testUploadPNG, nil)
	denied := httptest.NewRequest(http.MethodPost, peerPathEndpoint, bytes.NewReader(body))
	denied.TLS = &tls.ConnectionState{}
	denied.Header.Set("Authorization", "Bearer "+token)
	denied.Header.Set("Content-Type", contentType)
	denied.Header.Set(peerPathServiceHeader, "comfyui")
	denied.Header.Set(peerServicePathHeader, base64.RawURLEncoding.EncodeToString([]byte("/comfyui/upload/image")))
	response := httptest.NewRecorder()
	owner.PeerHandler().ServeHTTP(response, denied)
	if response.Code != http.StatusForbidden || uploadCalls.Load() != 4 {
		t.Fatalf("upload without path grant status=%d calls=%d", response.Code, uploadCalls.Load())
	}
}

// assertNonImageOperationsRouteToScheduler verifies non-image operations
// (prompt, api/prompt, upload/mask) are routed to the scheduler and that
// exactly three scheduler calls are recorded with four upload calls unchanged.
func assertNonImageOperationsRouteToScheduler(t *testing.T, handler *Handler, uploadCalls, schedulerCalls *atomic.Int32) {
	for _, endpoint := range []string{"/comfyui/prompt", "/comfyui/api/prompt", "/comfyui/upload/mask"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(`{}`)))
		if response.Code != http.StatusAccepted {
			t.Fatalf("non-image operation escaped scheduler: %s status=%d", endpoint, response.Code)
		}
	}
	if schedulerCalls.Load() != 3 || uploadCalls.Load() != 4 {
		t.Fatalf("non-upload routing: uploads=%d scheduler=%d", uploadCalls.Load(), schedulerCalls.Load())
	}
}

func TestComfyUIImageUploadsBypassSchedulerLocallyAndOverPeer(t *testing.T) {
	handler, owner, backend, scheduler, uploadCalls, schedulerCalls, token, peerConfig := newComfyUIImageUploadHarness(t)
	defer backend.Close()
	defer scheduler.Close()

	if handler.comfyUIImageUploadMaxBytes != 16<<20 {
		t.Fatalf("automatic upload default=%d", handler.comfyUIImageUploadMaxBytes)
	}

	t.Run("uploads reach backend not scheduler directly and over peer", func(t *testing.T) {
		assertUploadsReachBackendNotScheduler(t, handler, owner, uploadCalls, schedulerCalls, token)
	})
	t.Run("upload denied without path grant", func(t *testing.T) {
		assertUploadDeniedWithoutPathGrant(t, owner, uploadCalls, token, peerConfig)
	})
	t.Run("non-image operations route to scheduler", func(t *testing.T) {
		assertNonImageOperationsRouteToScheduler(t, handler, uploadCalls, schedulerCalls)
	})
}

func TestComfyUIImageUploadRejectsOversizedAndInvalidBodiesWithoutForwarding(t *testing.T) {
	var backendCalls atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		backendCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()
	resultProxy, err := newComfyUIResultReadProxy(&config.ComfyUIResultReadConfig{URL: backend.URL}, discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	handler := &Handler{logger: discardLogger(), comfyUIResultReadProxy: resultProxy, comfyUIResultSlots: make(chan struct{}, 1), comfyUIImageUploadMaxBytes: 1024}
	for _, chunked := range []bool{false, true} {
		body, contentType := imageUploadBody(t, "input.png", bytes.Repeat([]byte("x"), 2048), nil)
		request := httptest.NewRequest(http.MethodPost, "/comfyui/upload/image", bytes.NewReader(body))
		request.Header.Set("Content-Type", contentType)
		if chunked {
			request.ContentLength = -1
			request.TransferEncoding = []string{"chunked"}
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("chunked=%v oversized status=%d", chunked, response.Code)
		}
	}
	body, contentType := imageUploadBody(t, "input.png", testUploadPNG, [][2]string{{"type", "output"}})
	request := httptest.NewRequest(http.MethodPost, "/comfyui/upload/image", bytes.NewReader(body))
	request.Header.Set("Content-Type", contentType)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || backendCalls.Load() != 0 {
		t.Fatalf("invalid upload forwarded: status=%d calls=%d", response.Code, backendCalls.Load())
	}
	body, contentType = imageUploadBody(t, "input.png", testUploadPNG, nil)
	for _, test := range []struct {
		name   string
		modify func(*http.Request)
		status int
	}{
		{"query", func(r *http.Request) { r.URL.RawQuery = "type=output" }, http.StatusBadRequest},
		{"content encoding", func(r *http.Request) { r.Header.Set("Content-Encoding", "gzip") }, http.StatusBadRequest},
		{"wrong content type", func(r *http.Request) { r.Header.Set("Content-Type", "application/json") }, http.StatusUnsupportedMediaType},
		{"duplicate content type", func(r *http.Request) { r.Header.Add("Content-Type", contentType) }, http.StatusBadRequest},
		{"encoded slash", func(r *http.Request) { r.URL.RawPath = "/comfyui/upload%2Fimage" }, http.StatusBadRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/comfyui/upload/image", bytes.NewReader(body))
			request.Header.Set("Content-Type", contentType)
			test.modify(request)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.status || backendCalls.Load() != 0 {
				t.Fatalf("invalid upload status=%d calls=%d", response.Code, backendCalls.Load())
			}
		})
	}
	handler.comfyUIImageUploadMaxBytes = int64(len(body))
	request = httptest.NewRequest(http.MethodPost, "/comfyui/upload/image", bytes.NewReader(body))
	request.Header.Set("Content-Type", contentType)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || backendCalls.Load() != 1 {
		t.Fatalf("upload at exact size limit status=%d calls=%d", response.Code, backendCalls.Load())
	}
}

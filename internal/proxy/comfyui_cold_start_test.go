package proxy

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

func TestComfyUIColdStart(t *testing.T) {
	for _, tc := range []struct {
		name, method, path     string
		blocked                bool
		status, admits, starts int
		direct                 int
	}{
		{"metadata", http.MethodGet, "/comfyui/api/object_info/CheckpointLoaderSimple", false, 200, 1, 1, 1},
		{"upload", http.MethodPost, "/comfyui/upload/image", false, 200, 1, 1, 1},
		{"denied upload", http.MethodPost, "/comfyui/api/upload/image", true, 503, 1, 0, 0},
		{"background poll", http.MethodGet, "/comfyui/api/jobs", false, 502, 0, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var ready atomic.Bool
			var starts, direct atomic.Int32
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				direct.Add(1)
				if !ready.Load() {
					w.WriteHeader(http.StatusBadGateway)
					return
				}
				if req.Method == http.MethodPost {
					if err := req.ParseMultipartForm(1 << 20); err != nil {
						t.Error(err)
						return
					}
					file, _, err := req.FormFile("image")
					if err != nil {
						t.Error(err)
						return
					}
					data, err := io.ReadAll(file)
					_ = file.Close()
					if err != nil || !bytes.Equal(data, testUploadPNG) {
						t.Error("cold-start upload did not preserve image bytes")
					}
				}
			}))
			defer backend.Close()
			scheduler := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				starts.Add(1)
				if req.Method != http.MethodGet || req.URL.RequestURI() != "/comfyui/" ||
					req.ContentLength != 0 || req.Header.Get("Authorization") != "" || req.Header.Get("Cookie") != "" {
					t.Error("startup request carried caller path, body, or credentials")
				}
				ready.Store(true)
				w.Header().Set("Set-Cookie", "startup-only=value")
				_, _ = io.WriteString(w, "startup UI page")
			}))
			defer scheduler.Close()
			cfg := baseConfig(scheduler.URL)
			cfg.PathRoutes = map[string]string{comfyUIPathPrefix: "default"}
			cfg.Peer = &config.PeerConfig{NodeID: "owner", Listen: "127.0.0.1:9294", TLSCertFile: "cert.pem", TLSKeyFile: "key.pem",
				ComfyUIResultRead: &config.ComfyUIResultReadConfig{URL: backend.URL}}
			h := newHandler(t, cfg).(*Handler)
			h.comfyUIResultReadProbe = func(context.Context) error {
				if !ready.Load() {
					return syscall.ECONNREFUSED
				}
				return nil
			}
			admissions := 0
			h.backendAdmission = func(*http.Request, string) (func(), bool) {
				admissions++
				return nil, !tc.blocked
			}
			req := httptest.NewRequest(tc.method, tc.path, nil)
			if tc.method == http.MethodPost {
				body, contentType := imageUploadBody(t, "input.png", testUploadPNG, nil)
				req = httptest.NewRequest(tc.method, tc.path, bytes.NewReader(body))
				req.Header.Set("Content-Type", contentType)
			}
			req.Header.Set("Authorization", "Bearer caller")
			req.Header.Set("Cookie", "caller=value")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			if w.Code != tc.status || admissions != tc.admits || int(starts.Load()) != tc.starts || int(direct.Load()) != tc.direct || w.Header().Get("Set-Cookie") != "" {
				t.Fatalf("status=%d admits=%d starts=%d direct=%d body=%q", w.Code, admissions, starts.Load(), direct.Load(), w.Body.String())
			}
		})
	}
}

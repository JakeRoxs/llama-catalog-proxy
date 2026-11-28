package proxy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

func TestSchedulerStartupRecoveryAdmitsResidentModelWithoutFree(t *testing.T) {
	for _, singleBackend := range []bool{true, false} {
		t.Run(fmt.Sprintf("single_backend=%t", singleBackend), func(t *testing.T) {
			testSchedulerStartupAndHandoff(t, singleBackend)
		})
	}
}

func testSchedulerStartupAndHandoff(t *testing.T, singleBackend bool) {
	t.Helper()
	var freeRequests, statsRequests, runningRequests atomic.Int32
	comfy := newComfyTestServer(t, &freeRequests, &statsRequests)
	defer comfy.Close()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/running" {
			runningRequests.Add(1)
			_, _ = io.WriteString(w, `{"running":[{"model":"resident","state":"ready"}]}`)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer backend.Close()

	cfg := baseConfig(backend.URL)
	cfg.PathRoutes = map[string]string{"/comfyui": "default"}
	cfg.Peer = &config.PeerConfig{
		NodeID: "owner", Listen: "127.0.0.1:9294", TLSCertFile: "cert.pem", TLSKeyFile: "key.pem",
		ComfyUIResultRead: &config.ComfyUIResultReadConfig{URL: comfy.URL},
		ComfyUILease: &config.ComfyUILeaseConfig{
			ReleaseTimeout: "5s", PollInterval: "100ms", QueueIdleSamples: 2,
			MinimumFreeVRAMBytes: map[string]int64{"cuda:0": 800},
			AdmissionBackends:    []string{"default"},
		},
	}
	if !singleBackend {
		cfg.Backends["other"] = config.Backend{URL: backend.URL, Prefix: "other/"}
		cfg.Peer.ComfyUILease.AdmissionBackends = append(cfg.Peer.ComfyUILease.AdmissionBackends, "other")
	}
	h := newHandler(t, cfg)
	request := func(path, body string) {
		t.Helper()
		r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusAccepted {
			t.Fatalf("%s: status=%d, body=%s", path, w.Code, w.Body.String())
		}
	}
	request("/v1/chat/completions", `{"model":"resident"}`)
	wantRunning, wantFrees := int32(2), int32(0)
	if !singleBackend {
		wantRunning, wantFrees = 0, 1
	}
	if runningRequests.Load() != wantRunning || freeRequests.Load() != wantFrees || (statsRequests.Load() == 0) != singleBackend {
		t.Fatalf("startup /running=%d /free=%d /system_stats=%d", runningRequests.Load(), freeRequests.Load(), statsRequests.Load())
	}
	request("/comfyui/api/prompt", `{}`)
	request("/v1/chat/completions", `{"model":"resident"}`)
	if runningRequests.Load() != wantRunning || freeRequests.Load() != wantFrees+1 || statsRequests.Load() < 3 {
		t.Fatalf("handoff /running=%d /free=%d /system_stats=%d", runningRequests.Load(), freeRequests.Load(), statsRequests.Load())
	}
}

func TestSchedulerStartupProbeRejectsInconclusiveEvidence(t *testing.T) {
	const ready = `{"running":[{"model":"resident","state":"ready"}]}`
	tests := []struct {
		name, first, second, queue string
	}{
		{"ComfyUI running", `{"running":[{"model":"comfyui_auto","state":"ready"}]}`, ready, ""},
		{"scheduler transitioning", `{"running":[{"model":"resident","state":"starting"}]}`, ready, ""},
		{"multiple processes", `{"running":[{"model":"resident","state":"ready"},{"model":"other","state":"ready"}]}`, ready, ""},
		{"unsupported response", `{"running":null}`, ready, ""},
		{"resident changed", ready, `{"running":[{"model":"other","state":"ready"}]}`, ""},
		{"busy queue", ready, ready, `[[]]`},
		{"unsupported queue", ready, ready, `null`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/running":
					if calls.Add(1) == 1 {
						_, _ = io.WriteString(w, tc.first)
					} else {
						_, _ = io.WriteString(w, tc.second)
					}
				case "/queue":
					queue := tc.queue
					if queue == "" {
						queue = "[]"
					}
					_, _ = fmt.Fprintf(w, `{"queue_running":%s,"queue_pending":[]}`, queue)
				default:
					t.Errorf("unexpected probe: %s %s", r.Method, r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer srv.Close()
			probe, err := newComfyUISchedulerStartupProbe(config.Backend{URL: srv.URL}, srv.URL, nil,
				&config.ComfyUILeaseConfig{PollInterval: "100ms"}, srv.Client())
			if err != nil {
				t.Fatal(err)
			}
			if probe(t.Context()) {
				t.Fatal("inconclusive startup evidence accepted")
			}
		})
	}
}

func TestSchedulerStartupRecoveryCannotBypassAmbiguousRelease(t *testing.T) {
	var releases, probes int
	c := newComfyUICoordinator(func(context.Context) (ComfyUIReleaseResult, error) {
		releases++
		return ComfyUIReleaseResult{FreeRequested: releases == 1}, ErrComfyUIReleaseUnknown
	})
	c.startupReady = func(context.Context) bool {
		probes++
		return probes > 1
	}
	for range 3 {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
		if ensureComfyUIAdmissionState(c, req, time.Second, true) {
			t.Fatal("ambiguous release admitted via startup shortcut")
		}
	}
	if probes != 1 || releases != 3 {
		t.Fatalf("scheduler probes=%d release checks=%d", probes, releases)
	}
}

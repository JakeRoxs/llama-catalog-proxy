package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

func TestComfyUIJobTrackerWaitsUntilQueueIsEmpty(t *testing.T) {
	var busy atomic.Bool
	busy.Store(true)
	queue := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		if busy.Load() {
			_, _ = writer.Write([]byte(`{"queue_running":[["job"]],"queue_pending":[]}`))
			return
		}
		_, _ = writer.Write([]byte(`{"queue_running":[],"queue_pending":[]}`))
	}))
	defer queue.Close()

	tracker := &comfyUIJobTracker{url: queue.URL, client: queue.Client()}
	tracker.promptResponse(http.StatusOK)

	done := make(chan error, 1)
	go func() {
		done <- tracker.wait(context.Background())
	}()
	select {
	case err := <-done:
		t.Fatalf("wait returned while ComfyUI was busy: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	busy.Store(false)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("wait returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("wait did not return after ComfyUI queue emptied")
	}
}

func TestComfyUIJobTrackerDoesNotProbeWhenNoTrackedJob(t *testing.T) {
	var probeCalled atomic.Bool
	queue := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		probeCalled.Store(true)
		writer.WriteHeader(http.StatusInternalServerError)
	}))
	defer queue.Close()

	tracker := &comfyUIJobTracker{url: queue.URL, client: queue.Client()}
	if err := tracker.wait(context.Background()); err != nil {
		t.Fatalf("wait returned error without a tracked ComfyUI job: %v", err)
	}
	if probeCalled.Load() {
		t.Fatal("queue was probed while no ComfyUI job was tracked")
	}
}

func TestComfyUIJobTrackerHonorsContextDeadline(t *testing.T) {
	queue := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`{"queue_running":[["job"]],"queue_pending":[]}`))
	}))
	defer queue.Close()

	tracker := &comfyUIJobTracker{url: queue.URL, client: queue.Client()}
	tracker.promptResponse(http.StatusOK)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := tracker.wait(ctx); err != context.DeadlineExceeded {
		t.Fatalf("wait error = %v, want deadline exceeded", err)
	}
}

func TestCompetingInferenceTimesOutWithoutForwarding(t *testing.T) {
	comfy := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/comfyui/queue" {
			t.Errorf("unexpected ComfyUI path %q", request.URL.Path)
		}
		_, _ = writer.Write([]byte(`{"queue_running":[["job"]],"queue_pending":[]}`))
	}))
	defer comfy.Close()

	var inferenceCalled atomic.Bool
	inference := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		inferenceCalled.Store(true)
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer inference.Close()

	cfg := baseConfig(inference.URL)
	cfg.Backends["default"] = config.Backend{URL: inference.URL, Prefix: "local/"}
	cfg.Backends["comfy"] = config.Backend{URL: comfy.URL, Prefix: "comfy/"}
	cfg.PathRoutes = map[string]string{"/comfyui": "comfy"}
	cfg.DefaultBackend = "default"
	cfg.ComfyUIWaitTimeout = 30 * time.Millisecond
	handler := newHandler(t, cfg)
	handler.(*Handler).comfyUIJobs.promptResponse(http.StatusOK)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"comfy/model"}`)))
	if recorder.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504; body=%s", recorder.Code, recorder.Body.String())
	}
	if inferenceCalled.Load() {
		t.Fatal("inference request reached backend after wait timeout")
	}
}

func TestComfyUIBarrierDoesNotBlockDifferentBackend(t *testing.T) {
	comfy := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusInternalServerError)
	}))
	defer comfy.Close()

	var udmsCalled atomic.Bool
	udms := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		udmsCalled.Store(true)
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer udms.Close()

	cfg := baseConfig(udms.URL)
	cfg.Backends["default"] = config.Backend{URL: udms.URL}
	cfg.Backends["bigboi"] = config.Backend{URL: comfy.URL, Prefix: "bigboi/"}
	cfg.PathRoutes = map[string]string{"/comfyui": "bigboi"}
	cfg.ComfyUIWaitTimeout = time.Second
	handler := newHandler(t, cfg)
	handler.(*Handler).comfyUIJobs.promptResponse(http.StatusOK)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"udms/model"}`)))
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("UDMS request status = %d, want 204", recorder.Code)
	}
	if !udmsCalled.Load() {
		t.Fatal("UDMS request was not forwarded while Bigboi ComfyUI was active")
	}
}

func TestComfyUIBarrierBlocksInferenceOnSameBackend(t *testing.T) {
	var busy atomic.Bool
	var inferenceCalled atomic.Bool
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/comfyui/queue" {
			if busy.Load() {
				_, _ = writer.Write([]byte(`{"queue_running":[["job"]],"queue_pending":[]}`))
				return
			}
			_, _ = writer.Write([]byte(`{"queue_running":[],"queue_pending":[]}`))
			return
		}
		inferenceCalled.Store(true)
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer backend.Close()

	cfg := baseConfig(backend.URL)
	cfg.Backends["same-host"] = config.Backend{URL: backend.URL, Prefix: "same/"}
	cfg.PathRoutes = map[string]string{"/comfyui": "default"}
	cfg.ComfyUIWaitTimeout = time.Second
	handler := newHandler(t, cfg)
	handler.(*Handler).comfyUIJobs.promptResponse(http.StatusOK)

	recorder := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"same/model"}`)))
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("same-backend inference did not wait for ComfyUI")
	case <-time.After(50 * time.Millisecond):
	}
	if inferenceCalled.Load() {
		t.Fatal("same-backend inference reached backend while ComfyUI was active")
	}

	busy.Store(false)
	select {
	case <-done:
		if recorder.Code != http.StatusNoContent {
			t.Fatalf("same-backend inference status = %d", recorder.Code)
		}
	case <-time.After(time.Second):
		t.Fatal("same-backend inference did not proceed after ComfyUI completed")
	}
}

func TestComfyUIPromptTrackingHoldsCompetingInference(t *testing.T) {
	var busy atomic.Bool
	var inferenceCalled atomic.Bool
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/comfyui/queue":
			if busy.Load() {
				_, _ = writer.Write([]byte(`{"queue_running":[["job-1"]],"queue_pending":[]}`))
			} else {
				_, _ = writer.Write([]byte(`{"queue_running":[],"queue_pending":[]}`))
			}
		default:
			if request.URL.Path == "/prompt" || request.URL.Path == "/comfyui/prompt" {
				busy.Store(true)
				writer.Header().Set("Content-Type", "application/json")
				_, _ = writer.Write([]byte(`{"prompt_id":"job-1"}`))
			} else {
				inferenceCalled.Store(true)
				writer.WriteHeader(http.StatusNoContent)
			}
		}
	}))
	defer backend.Close()

	cfg := baseConfig(backend.URL)
	cfg.PathRoutes = map[string]string{"/comfyui": "default"}
	cfg.ComfyUIWaitTimeout = 3 * time.Second
	handler := newHandler(t, cfg)
	if tracker, ok := handler.(*Handler); ok {
		tracker.comfyUIJobs = newComfyUIJobTracker(config.Backend{URL: backend.URL}, backend.Client())
		tracker.comfyUIJobs.promptResponse(http.StatusOK)
	}

	promptResponse := httptest.NewRecorder()
	handler.ServeHTTP(promptResponse, httptest.NewRequest(http.MethodPost, "/comfyui/prompt", strings.NewReader(`{"prompt":{}}`)))
	if promptResponse.Code != http.StatusOK {
		t.Fatalf("prompt status = %d", promptResponse.Code)
	}
	requestDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"local"}`)))
		requestDone <- recorder
	}()
	select {
	case <-requestDone:
		t.Fatal("competing inference did not wait for ComfyUI queue")
	case <-time.After(50 * time.Millisecond):
	}
	if inferenceCalled.Load() {
		t.Fatal("competing inference reached backend while ComfyUI job was active")
	}

	busy.Store(false)
	select {
	case recorder := <-requestDone:
		if recorder.Code != http.StatusNoContent {
			t.Fatalf("inference status = %d", recorder.Code)
		}
	case <-time.After(time.Second):
		t.Fatal("competing inference did not proceed after ComfyUI completed")
	}
}

func TestComfyUIPromptWaitsForActiveInference(t *testing.T) {
	started := make(chan struct{})
	finishInference := make(chan struct{})
	var promptCalled atomic.Bool
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/v1/chat/completions" {
			close(started)
			<-finishInference
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		if request.URL.Path == "/comfyui/prompt" {
			promptCalled.Store(true)
			writer.WriteHeader(http.StatusOK)
			return
		}
		writer.WriteHeader(http.StatusNotFound)
	}))
	defer backend.Close()

	cfg := baseConfig(backend.URL)
	cfg.PathRoutes = map[string]string{"/comfyui": "default"}
	cfg.ComfyUIWaitTimeout = time.Second
	handler := newHandler(t, cfg)
	tracker := handler.(*Handler)
	tracker.comfyUIJobs = newComfyUIJobTracker(config.Backend{URL: backend.URL}, backend.Client())

	inferenceDone := make(chan struct{})
	go func() {
		defer close(inferenceDone)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"local"}`)))
	}()
	<-started

	promptDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/comfyui/prompt", strings.NewReader(`{}`)))
		promptDone <- recorder
	}()
	select {
	case <-promptDone:
		t.Fatal("ComfyUI prompt did not wait for active inference")
	case <-time.After(50 * time.Millisecond):
	}
	if promptCalled.Load() {
		t.Fatal("ComfyUI prompt reached backend during active inference")
	}

	close(finishInference)
	<-inferenceDone
	select {
	case recorder := <-promptDone:
		if recorder.Code != http.StatusOK {
			t.Fatalf("prompt status = %d", recorder.Code)
		}
	case <-time.After(time.Second):
		t.Fatal("ComfyUI prompt did not proceed after inference completed")
	}
}

func TestComfyUIQueueFailureFailsClosedAfterTrackedPrompt(t *testing.T) {
	tracker := &comfyUIJobTracker{
		url:    "http://127.0.0.1:1/comfyui/queue",
		client: &http.Client{Timeout: 20 * time.Millisecond},
	}
	tracker.promptResponse(http.StatusOK)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := tracker.wait(ctx); err == nil {
		t.Fatal("wait succeeded without confirming completion of a tracked prompt")
	}
}

package proxy

import (
	"context"
	"net/http"
	"strings"
	"time"
)

const comfyUIStartTimeout = 2 * time.Minute

type comfyUIStartAttempt struct {
	done  chan struct{}
	ready bool
}

func isComfyUIObjectInfo(requestPath string) bool {
	endpoint := strings.TrimPrefix(strings.TrimPrefix(requestPath, comfyUIPathPrefix), "/api/")
	endpoint = strings.TrimPrefix(endpoint, "/")
	return endpoint == "object_info" || strings.HasPrefix(endpoint, "object_info/")
}

// Only a definitive DNS/refused connection failure can trigger a cold start.
// Any other probe result leaves the original request on its normal direct path.
func (h *Handler) ensureComfyUIStarted(w http.ResponseWriter, req *http.Request) bool {
	ctx := req.Context()
	if h.comfyUIResultReadProbe == nil || !isComfyUIUnavailableBeforeFree(h.comfyUIResultReadProbe(ctx)) {
		return true
	}
	backendID, prefix, matched := h.routePathMatch(comfyUIPathPrefix + "/")
	if !matched || prefix != comfyUIPathPrefix {
		return true
	}
	ctx, cancel := context.WithTimeout(ctx, comfyUIStartTimeout)
	defer cancel()
	if h.startComfyUI(ctx, backendID) {
		return true
	}
	writeJSONError(h.logger, w, http.StatusServiceUnavailable, "ComfyUI cold start did not become ready")
	return false
}

func (h *Handler) startComfyUI(ctx context.Context, backendID string) bool {
	h.comfyUIStartMu.Lock()
	if attempt := h.comfyUIStart; attempt != nil {
		h.comfyUIStartMu.Unlock()
		select {
		case <-attempt.done:
			return attempt.ready
		case <-ctx.Done():
			return false
		}
	}
	attempt := &comfyUIStartAttempt{done: make(chan struct{})}
	h.comfyUIStart = attempt
	h.comfyUIStartMu.Unlock()
	defer func() {
		h.comfyUIStartMu.Lock()
		h.comfyUIStart = nil
		close(attempt.done)
		h.comfyUIStartMu.Unlock()
	}()

	// Recheck in case another request started the service since the initial probe.
	probeErr := h.comfyUIResultReadProbe(ctx)
	if probeErr == nil {
		attempt.ready = true
	} else if isComfyUIUnavailableBeforeFree(probeErr) && ctx.Err() == nil {
		// Use the same admission path as opening the UI, without forwarding caller
		// headers or the original request body to the scheduler.
		start, err := http.NewRequestWithContext(ctx, http.MethodGet, comfyUIPathPrefix+"/", nil)
		if err == nil {
			response := &comfyUIStartResponse{header: make(http.Header)}
			h.dispatchBackend(response, start, backendID)
			attempt.ready = response.status >= 200 && response.status < 300 && h.comfyUIResultReadProbe(ctx) == nil
		}
	}
	return attempt.ready
}

// Discard the internal UI response without buffering its body or returning its headers.
type comfyUIStartResponse struct {
	header http.Header
	status int
}

func (r *comfyUIStartResponse) Header() http.Header { return r.header }

func (r *comfyUIStartResponse) WriteHeader(status int) {
	if status >= 200 && r.status == 0 {
		r.status = status
	}
}

func (r *comfyUIStartResponse) Write(p []byte) (int, error) {
	r.WriteHeader(http.StatusOK)
	return len(p), nil
}

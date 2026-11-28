package proxy

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"
)

type comfyUILeaseActivationContextKey struct{}

type comfyUILeaseActivation struct {
	coordinator *comfyUICoordinator
	leaseID     string
}

// newComfyUIDomainAdmission coordinates ComfyUI requests and known model-dispatch
// routes. Other backend traffic passes through without lease transitions.
func newComfyUIDomainAdmission(coordinator *comfyUICoordinator, backendIDs []string, promptBackend string, releaseTimeout time.Duration) (func(*http.Request, string) (func(), bool), error) {
	managed, err := managedComfyUIBackends(backendIDs)
	if err != nil {
		return nil, err
	}
	if coordinator == nil || !managed[promptBackend] || releaseTimeout <= 0 || releaseTimeout > 30*time.Minute {
		return nil, errors.New("invalid ComfyUI admission domain")
	}
	return func(req *http.Request, backendID string) (func(), bool) {
		return admitComfyUIDomainRequest(coordinator, managed, promptBackend, releaseTimeout, req, backendID)
	}, nil
}

func managedComfyUIBackends(backendIDs []string) (map[string]bool, error) {
	managed := make(map[string]bool, len(backendIDs))
	for _, id := range backendIDs {
		if id == "" || managed[id] {
			return nil, errors.New("invalid managed backend IDs")
		}
		managed[id] = true
	}
	return managed, nil
}

func admitComfyUIDomainRequest(
	coordinator *comfyUICoordinator,
	managed map[string]bool,
	promptBackend string,
	releaseTimeout time.Duration,
	req *http.Request,
	backendID string,
) (func(), bool) {
	if !managed[backendID] {
		return nil, true
	}
	if req == nil || req.URL == nil || req.URL.RawPath != "" || req.Context().Err() != nil {
		return nil, false
	}
	prompt := isComfyPromptRequest(req)
	comfyUIRequest, modelRequest := isComfyUIDomainRoute(req)
	if !comfyUIRequest && !modelRequest {
		return nil, true
	}
	if !validComfyUIBackend(comfyUIRequest, prompt, req, backendID, promptBackend) {
		return nil, false
	}
	if prompt || isComfyUISchedulerStart(req) {
		return admitComfyUISchedulerPrompt(coordinator, req, true)
	}
	if !ensureComfyUIAdmissionState(coordinator, req, releaseTimeout) {
		return nil, false
	}
	if comfyUIRequest {
		return admitComfyUISchedulerPrompt(coordinator, req, false)
	}
	return admitCompetingModelRequest(coordinator, req, releaseTimeout)
}

func isComfyUIDomainRoute(req *http.Request) (bool, bool) {
	// The llama-swap UI starts comfyui_auto through the generic model route.
	comfyUIRequest := isComfyUISchedulerStart(req) || isComfyUIPathRequest(req) ||
		(isComfyUIFrontendRoot(req) && req.ContentLength <= 0 &&
			(req.Body == nil || req.Body == http.NoBody) && len(req.TransferEncoding) == 0)
	return comfyUIRequest, isLlamaSwapModelRequest(req)
}

func isComfyUISchedulerStart(req *http.Request) bool {
	return req.Method == http.MethodGet && req.URL.Path == "/upstream/comfyui_auto/"
}

func validComfyUIBackend(comfyUIRequest, prompt bool, req *http.Request, backendID, promptBackend string) bool {
	if !comfyUIRequest {
		return true
	}
	return backendID == promptBackend && (!prompt || (req.URL.RawQuery == "" && !req.URL.ForceQuery))
}

func ensureComfyUIAdmissionState(
	coordinator *comfyUICoordinator, req *http.Request, releaseTimeout time.Duration,
) bool {
	if coordinator.snapshot().State != "unknown" {
		return true
	}
	ctx, cancel := context.WithTimeout(req.Context(), releaseTimeout)
	_, err := coordinator.reconcileInitial(ctx)
	if errors.Is(err, errComfyUIAdmissionBlocked) {
		// Initial reconciliation is one-shot when its outcome may be ambiguous.
		// A later request may try again, but only by repeating the full queue,
		// /free, and VRAM verification—not by assuming the prior operation worked.
		_, err = coordinator.reconcile(ctx)
	}
	cancel()
	if errors.Is(err, errComfyUIServiceUnavailable) {
		err = coordinator.assumeInitialStopped()
	}
	return err == nil
}

func admitComfyUISchedulerPrompt(
	coordinator *comfyUICoordinator, req *http.Request, initialAllowed bool,
) (func(), bool) {
	leaseID, done, firstPrompt, err := coordinator.admitSchedulerPrompt(initialAllowed)
	if err == nil && firstPrompt {
		ctx := context.WithValue(req.Context(), comfyUILeaseActivationContextKey{}, comfyUILeaseActivation{
			coordinator: coordinator, leaseID: leaseID,
		})
		*req = *req.WithContext(ctx)
	}
	return done, err == nil
}

func admitCompetingModelRequest(
	coordinator *comfyUICoordinator, req *http.Request, releaseTimeout time.Duration,
) (func(), bool) {
	state := coordinator.snapshot()
	if !releaseComfyUIForModelRequest(coordinator, req, releaseTimeout, state) {
		return nil, false
	}
	// Reserve under the coordinator lock: another acquisition may have won
	// after verification. In that case no competing request is forwarded.
	if req.Context().Err() != nil {
		return nil, false
	}
	done, err := coordinator.reserveCompeting()
	return done, err == nil
}

func releaseComfyUIForModelRequest(
	coordinator *comfyUICoordinator, req *http.Request, releaseTimeout time.Duration, state comfyUICoordinatorStatus,
) bool {
	switch state.State {
	case "active", "draining", "releasing":
		ctx, cancel := context.WithTimeout(req.Context(), releaseTimeout)
		defer cancel()
		_, err := coordinator.release(ctx, state.LeaseID)
		if errors.Is(err, errComfyUIServiceUnavailable) {
			err = coordinator.assumeLeaseStopped(state.LeaseID)
		}
		return err == nil && ctx.Err() == nil
	case "released":
		return true
	default:
		return false
	}
}

func isComfyUIPathRequest(req *http.Request) bool {
	return req != nil && req.URL != nil &&
		(req.URL.Path == "/comfyui" || strings.HasPrefix(req.URL.Path, "/comfyui/"))
}

func isLlamaSwapModelRequest(req *http.Request) bool {
	if req == nil || req.URL == nil {
		return false
	}
	if req.Method == http.MethodGet {
		switch req.URL.Path {
		case "/v1/audio/voices", "/sdapi/v1/loras", "/props", "/v1/realtime":
			return true
		}
		return strings.HasPrefix(req.URL.Path, "/upstream/")
	}
	if req.Method != http.MethodPost {
		return false
	}
	switch req.URL.Path {
	case "/v1/chat/completions", "/v1/responses", "/v1/completions", "/v1/messages",
		"/v1/messages/count_tokens", "/v1/embeddings", "/reranking", "/rerank", "/v1/rerank",
		"/v1/reranking", "/infill", "/completion", "/v1/audio/speech", "/v1/audio/voices",
		"/v1/images/generations", "/sdapi/v1/txt2img", "/sdapi/v1/img2img", "/v1/systemone",
		"/audioapi/v1/tasks/run", "/v/chat/completions", "/v/responses", "/v/completions",
		"/v/messages", "/v/messages/count_tokens", "/v/embeddings", "/v/rerank", "/v/reranking",
		"/v1/audio/transcriptions", "/v1/images/edits":
		return true
	default:
		return false
	}
}

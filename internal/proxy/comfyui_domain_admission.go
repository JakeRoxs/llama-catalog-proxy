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
	managed := make(map[string]bool, len(backendIDs))
	for _, id := range backendIDs {
		if id == "" || managed[id] {
			return nil, errors.New("invalid managed backend IDs")
		}
		managed[id] = true
	}
	if coordinator == nil || !managed[promptBackend] || releaseTimeout <= 0 || releaseTimeout > 30*time.Minute {
		return nil, errors.New("invalid ComfyUI admission domain")
	}
	return func(req *http.Request, backendID string) (func(), bool) {
		if !managed[backendID] {
			return nil, true
		}
		if req == nil || req.URL == nil || req.URL.RawPath != "" || req.Context().Err() != nil {
			return nil, false
		}
		prompt := isComfyPromptRequest(req)
		comfyUIRequest := isComfyUIPathRequest(req) ||
			(isComfyUIFrontendRoot(req) && req.ContentLength <= 0 &&
				(req.Body == nil || req.Body == http.NoBody) && len(req.TransferEncoding) == 0)
		modelRequest := isLlamaSwapModelRequest(req)
		if !comfyUIRequest && !modelRequest {
			return nil, true
		}
		if comfyUIRequest {
			if backendID != promptBackend ||
				prompt && (req.URL.RawQuery != "" || req.URL.ForceQuery) {
				return nil, false
			}
		}
		state := coordinator.snapshot()
		if state.State == "unknown" {
			ctx, cancel := context.WithTimeout(req.Context(), releaseTimeout)
			_, err := coordinator.reconcileInitial(ctx)
			cancel()
			if errors.Is(err, errComfyUIServiceUnavailable) {
				err = coordinator.assumeInitialStopped()
			}
			if err != nil {
				return nil, false
			}
			state = coordinator.snapshot()
		}
		if comfyUIRequest {
			firstPrompt := state.State == "released"
			leaseID, done, err := coordinator.admitPrompt()
			if err == nil && firstPrompt {
				ctx := context.WithValue(req.Context(), comfyUILeaseActivationContextKey{}, comfyUILeaseActivation{
					coordinator: coordinator, leaseID: leaseID,
				})
				*req = *req.WithContext(ctx)
			}
			return done, err == nil
		}
		// GET /props can load a model, so it goes through the same handoff as a
		// competing generation POST.
		state = coordinator.snapshot()
		switch state.State {
		case "active", "draining", "releasing":
			ctx, cancel := context.WithTimeout(req.Context(), releaseTimeout)
			defer cancel()
			if _, err := coordinator.release(ctx, state.LeaseID); err != nil || ctx.Err() != nil {
				return nil, false
			}
		case "released":
		default:
			return nil, false
		}
		// Reserve under the coordinator lock: another acquisition may have won
		// after verification. In that case no competing request is forwarded.
		if req.Context().Err() != nil {
			return nil, false
		}
		done, err := coordinator.reserveCompeting()
		return done, err == nil
	}, nil
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

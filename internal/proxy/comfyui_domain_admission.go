package proxy

import (
	"context"
	"errors"
	"net/http"
	"time"
)

type comfyUILeaseActivationContextKey struct{}

type comfyUILeaseActivation struct {
	coordinator *comfyUICoordinator
	leaseID     string
}

// newComfyUIDomainAdmission is deliberately not wired into runtime config.
// The owner must establish exclusive ingress and verify scheduler model routing
// before binding it; the HTTP method and path alone do not prove GPU ownership.
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
		startComfyUI := prompt || (isComfyUIFrontendRoot(req) && req.ContentLength <= 0 &&
			(req.Body == nil || req.Body == http.NoBody) && len(req.TransferEncoding) == 0)
		competing := (req.Method == http.MethodGet && req.URL.Path == "/props") ||
			(req.Method == http.MethodPost && req.URL.Path == "/v1/chat/completions")
		if !startComfyUI && !competing {
			return nil, false
		}
		if startComfyUI {
			if backendID != promptBackend ||
				prompt && (req.URL.RawQuery != "" || req.URL.ForceQuery) {
				return nil, false
			}
		} else if req.URL.RawPath != "" {
			return nil, false
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
		if startComfyUI {
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

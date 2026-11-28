package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

const (
	comfyUISchedulerModel    = "comfyui_auto"
	maxSchedulerRunningBytes = 1 << 20
)

// The operator must ensure the one scheduler exclusively manages GPU workloads.
// Its /running response is a process snapshot, not a report of VRAM ownership.
func newComfyUISchedulerStartupProbe(backend config.Backend, comfyURL string, comfyHeaders http.Header,
	lease *config.ComfyUILeaseConfig, client *http.Client) (func(context.Context) bool, error) {
	scheduler, err := url.Parse(backend.URL)
	if err != nil {
		return nil, fmt.Errorf("parse ComfyUI scheduler URL: %w", err)
	}
	origin, err := url.Parse(comfyURL)
	if err != nil {
		return nil, fmt.Errorf("parse ComfyUI origin URL: %w", err)
	}
	runningURL := scheduler.JoinPath("running").String()
	queue := &comfyUIJobTracker{url: endpointURL(origin, "/queue").String(), headers: comfyHeaders, client: client}
	return func(ctx context.Context) bool {
		before := readySchedulerModel(ctx, client, runningURL, backend)
		if before == "" {
			return false
		}
		// Do not wait out a busy queue in this shortcut; the full verifier
		// owns that path.
		for i := 0; i < lease.StableQueueSamples(); i++ {
			if i != 0 && waitComfyUIReleasePoll(ctx, lease.PollIntervalDuration()) != nil {
				return false
			}
			busy, err := queue.queueBusy(ctx)
			if err != nil || busy {
				return false
			}
		}
		return readySchedulerModel(ctx, client, runningURL, backend) == before
	}, nil
}

func readySchedulerModel(ctx context.Context, client *http.Client, endpoint string, backend config.Backend) string {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return ""
	}
	applyBackendHeaders(request.Header, backend)
	response, err := client.Do(request)
	if err != nil {
		return ""
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return ""
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxSchedulerRunningBytes+1))
	if err != nil || len(body) > maxSchedulerRunningBytes {
		return ""
	}
	var snapshot struct {
		Running []struct {
			Model string `json:"model"`
			State string `json:"state"`
		} `json:"running"`
	}
	if json.Unmarshal(body, &snapshot) != nil || len(snapshot.Running) != 1 {
		return ""
	}
	model := snapshot.Running[0]
	if model.State != "ready" || model.Model == "" || strings.TrimSpace(model.Model) != model.Model || model.Model == comfyUISchedulerModel {
		return ""
	}
	return model.Model
}

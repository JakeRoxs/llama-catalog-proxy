package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

const (
	comfyUIQueuePollInterval = 500 * time.Millisecond
	comfyUIQueueRequestLimit = 2 * time.Second
	comfyUIQueueBodyLimit    = 1 << 20
)

type comfyUIJobTracker struct {
	url     string
	headers http.Header
	client  *http.Client

	mu               sync.Mutex
	inFlightPrompts  int
	pendingPrompts   int
	inFlightRequests int
	active           bool
	changed          chan struct{}
}

func newComfyUIJobTracker(backend config.Backend, client *http.Client) *comfyUIJobTracker {
	base, _ := url.Parse(backend.URL)
	base.Path = strings.TrimSuffix(base.Path, "/") + "/comfyui/queue"
	base.RawPath = ""
	base.RawQuery = ""

	headers := make(http.Header, len(backend.Headers)+2)
	for name, value := range backend.Headers {
		headers.Set(name, value)
	}
	setAPIKeyIfMissing(headers, backend.APIKey)

	return &comfyUIJobTracker{
		url:     base.String(),
		headers: headers,
		client:  client,
		changed: make(chan struct{}),
	}
}

func (t *comfyUIJobTracker) beginSubmission(ctx context.Context) error {
	t.mu.Lock()
	t.pendingPrompts++
	t.signalChangeLocked()
	for t.inFlightRequests > 0 {
		changed := t.changed
		t.mu.Unlock()
		select {
		case <-ctx.Done():
			t.mu.Lock()
			t.pendingPrompts--
			t.signalChangeLocked()
			t.mu.Unlock()
			return ctx.Err()
		case <-changed:
			t.mu.Lock()
		}
	}
	t.pendingPrompts--
	t.inFlightPrompts++
	t.signalChangeLocked()
	t.mu.Unlock()
	return nil
}

func (t *comfyUIJobTracker) endSubmission() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.inFlightPrompts > 0 {
		t.inFlightPrompts--
		t.signalChangeLocked()
	}
}

func (t *comfyUIJobTracker) endInference() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.inFlightRequests > 0 {
		t.inFlightRequests--
		t.signalChangeLocked()
	}
}

func (t *comfyUIJobTracker) reserveInference(ctx context.Context) error {
	for {
		if err := t.wait(ctx); err != nil {
			return err
		}
		t.mu.Lock()
		if t.pendingPrompts == 0 && t.inFlightPrompts == 0 {
			t.inFlightRequests++
			t.signalChangeLocked()
			t.mu.Unlock()
			return nil
		}
		t.mu.Unlock()
	}
}

func (t *comfyUIJobTracker) promptResponse(status int) {
	if status < 200 || status >= 300 {
		return
	}
	t.mu.Lock()
	t.active = true
	t.signalChangeLocked()
	t.mu.Unlock()
}

func (t *comfyUIJobTracker) wait(ctx context.Context) error {
	ticker := time.NewTicker(comfyUIQueuePollInterval)
	defer ticker.Stop()
	emptySamples := 0

	for {
		t.mu.Lock()
		if t.inFlightPrompts > 0 || t.pendingPrompts > 0 {
			changed := t.changed
			t.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-changed:
			}
			continue
		}
		t.mu.Unlock()

		t.mu.Lock()
		tracked := t.active
		t.mu.Unlock()
		if !tracked {
			return nil
		}
		wasActive := tracked
		busy, err := t.queueBusy(ctx)
		if err != nil {
			t.mu.Lock()
			tracked := t.active
			t.mu.Unlock()
			if !tracked {
				return nil
			}
			return err
		}
		if busy {
			t.mu.Lock()
			t.active = true
			t.mu.Unlock()
			emptySamples = 0
		} else {
			if !wasActive {
				return nil
			}
			emptySamples++
			if emptySamples >= 2 {
				t.mu.Lock()
				if t.inFlightPrompts == 0 && t.pendingPrompts == 0 {
					t.active = false
					t.signalChangeLocked()
					t.mu.Unlock()
					return nil
				}
				t.mu.Unlock()
				emptySamples = 0
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (t *comfyUIJobTracker) signalChangeLocked() {
	if t.changed == nil {
		t.changed = make(chan struct{})
		return
	}
	close(t.changed)
	t.changed = make(chan struct{})
}

func (t *comfyUIJobTracker) queueBusy(ctx context.Context) (bool, error) {
	requestCtx, cancel := context.WithTimeout(ctx, comfyUIQueueRequestLimit)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, t.url, nil)
	if err != nil {
		return false, err
	}
	request.Header = t.headers.Clone()
	response, err := t.client.Do(request)
	if err != nil {
		return false, fmt.Errorf("query ComfyUI queue: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, comfyUIQueueBodyLimit))
		return false, fmt.Errorf("query ComfyUI queue returned HTTP %d", response.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(response.Body, comfyUIQueueBodyLimit+1))
	if err != nil {
		return false, fmt.Errorf("read ComfyUI queue response: %w", err)
	}
	if len(body) > comfyUIQueueBodyLimit {
		return false, fmt.Errorf("ComfyUI queue response exceeds %d bytes", comfyUIQueueBodyLimit)
	}
	var queue struct {
		Running *[]json.RawMessage `json:"queue_running"`
		Pending *[]json.RawMessage `json:"queue_pending"`
	}
	if err := json.Unmarshal(body, &queue); err != nil {
		return false, fmt.Errorf("decode ComfyUI queue response: %w", err)
	}
	if queue.Running == nil || queue.Pending == nil {
		return false, fmt.Errorf("ComfyUI queue response is missing queue_running or queue_pending")
	}
	return len(*queue.Running) > 0 || len(*queue.Pending) > 0, nil
}

func isComfyPromptRequest(request *http.Request) bool {
	return request != nil && request.URL != nil && request.Method == http.MethodPost &&
		(request.URL.Path == "/comfyui/prompt" || request.URL.Path == "/comfyui/api/prompt")
}

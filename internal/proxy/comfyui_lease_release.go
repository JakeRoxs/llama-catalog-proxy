package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

const (
	maxComfyUIFreeResponseBytes = 64 << 10
	maxComfyUISystemStatsBytes  = 1 << 20
	maxComfyUIReleaseDevices    = 16
	maxComfyUIReleaseVRAMBytes  = int64(1 << 60)
)

var ErrComfyUIReleaseUnknown = errors.New("ComfyUI release state is unknown")

var errComfyUIServiceUnavailable = errors.New("ComfyUI service is unavailable")

type ComfyUIDeviceReleaseEvidence struct {
	DeviceKey            string `json:"device"`
	Name                 string `json:"name,omitempty"`
	FreeVRAMBytes        int64  `json:"free_vram_bytes"`
	MinimumFreeVRAMBytes int64  `json:"minimum_free_vram_bytes"`
}

type ComfyUIReleaseResult struct {
	State                   string                         `json:"state"`
	QueueIdleSamples        int                            `json:"queue_idle_samples"`
	MinimumQueueIdleSamples int                            `json:"minimum_queue_idle_samples"`
	FreeRequested           bool                           `json:"free_requested"`
	FreeHTTPStatus          int                            `json:"free_http_status,omitempty"`
	Devices                 []ComfyUIDeviceReleaseEvidence `json:"devices,omitempty"`
}

type comfyUIReleaseStats struct {
	Devices []struct {
		Name      string `json:"name"`
		Type      string `json:"type"`
		Index     *int   `json:"index"`
		VRAMFree  *int64 `json:"vram_free"`
		VRAMTotal *int64 `json:"vram_total"`
	} `json:"devices"`
}

// VerifyComfyUIRelease requests asynchronous ComfyUI unloading and confirms only the configured queue and VRAM evidence.
func VerifyComfyUIRelease(ctx context.Context, targetURL string, targetHeaders http.Header,
	lease *config.ComfyUILeaseConfig, client *http.Client) (ComfyUIReleaseResult, error) {
	result := ComfyUIReleaseResult{State: "unknown"}
	if ctx == nil || lease == nil || len(lease.MinimumFreeVRAMBytes) == 0 {
		return result, ErrComfyUIReleaseUnknown
	}
	base, err := url.Parse(targetURL)
	if err != nil || base == nil || base.Scheme == "" || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" {
		return result, ErrComfyUIReleaseUnknown
	}
	releaseTimeout := lease.ReleaseTimeoutDuration()
	pollInterval := lease.PollIntervalDuration()
	idleSamples := lease.StableQueueSamples()
	if releaseTimeout < 5*time.Second || releaseTimeout > 30*time.Minute || pollInterval < 100*time.Millisecond ||
		pollInterval > 10*time.Second || idleSamples < 2 || idleSamples > 10 || validateVRAMThresholds(lease.MinimumFreeVRAMBytes) != nil {
		return result, ErrComfyUIReleaseUnknown
	}
	result.MinimumQueueIdleSamples = idleSamples
	if client == nil {
		client = &http.Client{
			Timeout:       5 * time.Second,
			Transport:     &http.Transport{Proxy: nil, DisableKeepAlives: true},
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
	}
	ctx, cancel := context.WithTimeout(ctx, releaseTimeout)
	defer cancel()
	queueURL := endpointURL(base, "/queue")
	queueTracker := &comfyUIJobTracker{url: queueURL.String(), headers: cloneComfyUILeaseHeaders(targetHeaders), client: client}
	if err := waitForEmptyComfyUIQueue(ctx, queueTracker, idleSamples, pollInterval); err != nil {
		if isComfyUIUnavailableBeforeFree(err) {
			return result, errors.Join(errComfyUIServiceUnavailable, err)
		}
		return result, fmt.Errorf("%w: queue did not reach a stable empty state", ErrComfyUIReleaseUnknown)
	}
	freeURL := endpointURL(base, "/free")
	body, _ := json.Marshal(map[string]bool{"unload_models": true, "free_memory": true})
	freeRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, freeURL.String(), bytes.NewReader(body))
	if err != nil {
		return result, ErrComfyUIReleaseUnknown
	}
	freeRequest.Header = cloneComfyUILeaseHeaders(targetHeaders)
	freeRequest.Header.Set("Content-Type", "application/json")
	result.FreeRequested = true
	response, err := client.Do(freeRequest)
	if err != nil {
		return result, fmt.Errorf("%w: /free request outcome unknown", ErrComfyUIReleaseUnknown)
	}
	result.FreeHTTPStatus = response.StatusCode
	responseBytes, readErr := io.Copy(io.Discard, io.LimitReader(response.Body, maxComfyUIFreeResponseBytes+1))
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil || responseBytes > maxComfyUIFreeResponseBytes {
		return result, fmt.Errorf("%w: /free response could not be read", ErrComfyUIReleaseUnknown)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		if response.StatusCode >= http.StatusInternalServerError {
			return result, fmt.Errorf("%w: ComfyUI /free returned HTTP %d", ErrComfyUIReleaseUnknown, response.StatusCode)
		}
		return result, fmt.Errorf("ComfyUI /free returned HTTP %d", response.StatusCode)
	}
	return confirmComfyUIReleaseAfterFree(ctx, comfyUIReleaseConfirmation{
		queueTracker: queueTracker,
		idleSamples:  idleSamples,
		pollInterval: pollInterval,
		client:       client,
		targetURL:    targetURL,
		headers:      targetHeaders,
		thresholds:   lease.MinimumFreeVRAMBytes,
	}, &result)
}

type comfyUIReleaseConfirmation struct {
	queueTracker *comfyUIJobTracker
	idleSamples  int
	pollInterval time.Duration
	client       *http.Client
	targetURL    string
	headers      http.Header
	thresholds   map[string]int64
}

// confirmComfyUIReleaseAfterFree polls the queue after /free until the queue is stably
// empty and per-device VRAM evidence meets the configured thresholds for the required
// number of consecutive idle samples. It returns the accumulated release result.
func confirmComfyUIReleaseAfterFree(ctx context.Context, confirmation comfyUIReleaseConfirmation, out *ComfyUIReleaseResult) (ComfyUIReleaseResult, error) {
	stableSamples := 0
	for {
		busy, err := confirmation.queueTracker.queueBusy(ctx)
		if err != nil {
			return *out, fmt.Errorf("%w: queue state after /free is unsupported", ErrComfyUIReleaseUnknown)
		}
		if busy {
			stableSamples = 0
		} else {
			devices, enough, err := readComfyUIReleaseEvidence(ctx, confirmation.client, confirmation.targetURL, confirmation.headers, confirmation.thresholds)
			if err != nil {
				return *out, fmt.Errorf("%w: %v", ErrComfyUIReleaseUnknown, err)
			}
			out.Devices = devices
			if enough {
				stableSamples++
				out.QueueIdleSamples = stableSamples
				if stableSamples >= confirmation.idleSamples {
					out.State = "confirmed"
					return *out, nil
				}
			} else {
				stableSamples = 0
				out.QueueIdleSamples = 0
			}
		}
		if err := waitComfyUIReleasePoll(ctx, confirmation.pollInterval); err != nil {
			return *out, fmt.Errorf("%w: release did not meet the configured queue and VRAM criteria before timeout", ErrComfyUIReleaseUnknown)
		}
	}
}

func isComfyUIUnavailableBeforeFree(err error) bool {
	if errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}
	var dnsError *net.DNSError
	return errors.As(err, &dnsError) && dnsError.IsNotFound
}

func waitForEmptyComfyUIQueue(ctx context.Context, tracker *comfyUIJobTracker, samples int, interval time.Duration) error {
	emptySamples := 0
	for emptySamples < samples {
		busy, err := tracker.queueBusy(ctx)
		if err != nil {
			return err
		}
		if busy {
			emptySamples = 0
		} else {
			emptySamples++
		}
		if emptySamples < samples {
			if err := waitComfyUIReleasePoll(ctx, interval); err != nil {
				return err
			}
		}
	}
	return nil
}

func readComfyUIReleaseEvidence(ctx context.Context, client *http.Client, targetURL string, headers http.Header,
	thresholds map[string]int64) ([]ComfyUIDeviceReleaseEvidence, bool, error) {
	base, err := url.Parse(targetURL)
	if err != nil {
		return nil, false, ErrComfyUIReleaseUnknown
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpointURL(base, "/system_stats").String(), nil)
	if err != nil {
		return nil, false, ErrComfyUIReleaseUnknown
	}
	request.Header = cloneComfyUILeaseHeaders(headers)
	response, err := client.Do(request)
	if err != nil {
		return nil, false, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxComfyUISystemStatsBytes))
		return nil, false, fmt.Errorf("system_stats returned HTTP %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxComfyUISystemStatsBytes+1))
	if err != nil || len(body) > maxComfyUISystemStatsBytes {
		return nil, false, ErrComfyUIReleaseUnknown
	}
	var stats comfyUIReleaseStats
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(&stats); err != nil {
		return nil, false, ErrComfyUIReleaseUnknown
	}
	if decoder.Decode(new(any)) != io.EOF || len(stats.Devices) == 0 || len(stats.Devices) > maxComfyUIReleaseDevices {
		return nil, false, ErrComfyUIReleaseUnknown
	}
	return collectComfyUIReleaseEvidence(stats, thresholds)
}

func collectComfyUIReleaseEvidence(stats comfyUIReleaseStats, thresholds map[string]int64) ([]ComfyUIDeviceReleaseEvidence, bool, error) {
	seen := make(map[string]struct{}, len(stats.Devices))
	evidence := make([]ComfyUIDeviceReleaseEvidence, 0, len(stats.Devices))
	enough := true
	for _, device := range stats.Devices {
		if device.Type == "cpu" {
			continue
		}
		if device.Index == nil || device.VRAMTotal == nil || device.VRAMFree == nil || *device.VRAMTotal <= 0 ||
			*device.VRAMFree < 0 || *device.VRAMFree > *device.VRAMTotal || *device.VRAMTotal > maxComfyUIReleaseVRAMBytes {
			return nil, false, ErrComfyUIReleaseUnknown
		}
		deviceKey := strings.ToLower(device.Type) + ":" + strconv.Itoa(*device.Index)
		threshold, configured := thresholds[deviceKey]
		if !configured {
			return nil, false, fmt.Errorf("no configured free-VRAM threshold for device %s", deviceKey)
		}
		seen[deviceKey] = struct{}{}
		evidence = append(evidence, ComfyUIDeviceReleaseEvidence{
			DeviceKey: deviceKey, Name: device.Name, FreeVRAMBytes: *device.VRAMFree, MinimumFreeVRAMBytes: threshold,
		})
		if *device.VRAMFree < threshold {
			enough = false
		}
	}
	if len(seen) == 0 || len(seen) != len(thresholds) {
		return nil, false, ErrComfyUIReleaseUnknown
	}
	for device := range thresholds {
		if _, ok := seen[device]; !ok {
			return nil, false, fmt.Errorf("configured GPU device %s is missing from system_stats", device)
		}
	}
	return evidence, enough, nil
}

func validateVRAMThresholds(thresholds map[string]int64) error {
	if len(thresholds) == 0 || len(thresholds) > maxComfyUIReleaseDevices {
		return ErrComfyUIReleaseUnknown
	}
	for device, threshold := range thresholds {
		deviceType, indexText, ok := strings.Cut(device, ":")
		index, err := strconv.Atoi(indexText)
		if !ok || deviceType == "" || strings.ToLower(deviceType) != deviceType || deviceType == "cpu" ||
			strings.ContainsAny(deviceType, " :/\\\t\r\n") || err != nil || index < 0 || index > 63 ||
			strconv.Itoa(index) != indexText || threshold <= 0 || threshold > maxComfyUIReleaseVRAMBytes {
			return ErrComfyUIReleaseUnknown
		}
	}
	return nil
}

func endpointURL(base *url.URL, endpoint string) *url.URL {
	endpointCopy := *base
	endpointCopy.Path = endpoint
	endpointCopy.RawPath = ""
	endpointCopy.RawQuery = ""
	endpointCopy.Fragment = ""
	return &endpointCopy
}

func cloneComfyUILeaseHeaders(headers http.Header) http.Header {
	if headers == nil {
		return make(http.Header)
	}
	return headers.Clone()
}

func waitComfyUIReleasePoll(ctx context.Context, interval time.Duration) error {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

package benchmark

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/correlation"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modeldiagnostics"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/privateorigin"
)

const (
	maxDiagnosticsResponseBytes = 1 << 20
	maxChatResponseBytes        = 1 << 20
	maxChatRequestBytes         = 64 << 10
	maxReadyObservationAge      = 5 * time.Minute

	// contentTypeJSON is the media type used for diagnostics and chat
	// request/response bodies. Centralizing it avoids duplicating the
	// "application/json" literal across header assignments.
	contentTypeJSON = "application/json"
)

type APIClient struct {
	origin              *url.URL
	apiKey              string
	http                *http.Client
	diagnosticInterval  time.Duration
	diagnosticMu        sync.Mutex
	lastDiagnosticQuery time.Time
}

type modelsResponse struct {
	Records         []*modeldiagnostics.ModelSnapshot `json:"models"`
	Cursor          string                            `json:"cursor"`
	GeneratedAt     time.Time                         `json:"generated_at"`
	Coverage        string                            `json:"coverage"`
	ResolvedModelID string                            `json:"resolved_model_id"`
}

type chatMessage struct {
	Role       string     `json:"role"`
	Content    string     `json:"content,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
	ToolCalls  []toolCall `json:"tool_calls,omitempty"`
}

type toolCall struct {
	ID       string       `json:"id"`
	Type     string       `json:"type"`
	Function toolFunction `json:"function"`
}

type toolFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type chatChoice struct {
	Message struct {
		Content   string     `json:"content"`
		ToolCalls []toolCall `json:"tool_calls"`
	} `json:"message"`
}

type chatResponse struct {
	Choices []chatChoice `json:"choices"`
}

type requestRecord struct {
	RequestID        string                        `json:"request_id"`
	BackendID        string                        `json:"backend_id"`
	ProxyObservation *correlation.ProxyObservation `json:"proxy_observation"`
}

type chatResult struct {
	Content   string
	ToolCalls []toolCall
	Measure   RequestMeasurement
}

func NewAPIClient(endpoint, apiKey string) (*APIClient, error) {
	origin, err := privateorigin.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("benchmark endpoint: %w", err)
	}
	if apiKey == "" || len(apiKey) > 4096 {
		return nil, errors.New("diagnostics API key is required and must be at most 4096 bytes")
	}
	for _, r := range apiKey {
		if r < 0x20 || r == 0x7f {
			return nil, errors.New("diagnostics API key contains a control character")
		}
	}
	return &APIClient{
		origin:             origin,
		apiKey:             apiKey,
		diagnosticInterval: time.Second,
		http: &http.Client{
			Timeout: 45 * time.Second,
			Transport: &http.Transport{
				ForceAttemptHTTP2:   true,
				TLSHandshakeTimeout: 10 * time.Second,
				IdleConnTimeout:     90 * time.Second,
			},
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

func (c *APIClient) ObserveReadyModel(ctx context.Context, backend, model string) (*ModelObservation, error) {
	query := url.Values{
		"backend": []string{backend},
		"model":   []string{model},
		"status":  []string{"ready"},
		"limit":   []string{"2"},
	}
	var response modelsResponse
	if err := c.getDiagnostics(ctx, "/diagnostics/v1/models", query, &response); err != nil {
		return nil, err
	}
	if len(response.Records) != 1 || response.Cursor != "" {
		return nil, errors.New("requested model is not uniquely observed ready")
	}
	snapshot := response.Records[0]
	if snapshot == nil || !snapshot.Identity.Valid() ||
		snapshot.Identity.BackendID != backend ||
		(snapshot.Identity.PublicID != model && snapshot.Identity.LocalID != model && response.ResolvedModelID != snapshot.Identity.PublicID) ||
		snapshot.Lifecycle == nil || !snapshot.Lifecycle.Observed ||
		snapshot.Lifecycle.CurrentState != "ready" || snapshot.Lifecycle.ObservedAt == nil ||
		snapshot.Lifecycle.ObservedAt.IsZero() {
		return nil, errors.New("requested model is not uniquely observed ready with a lifecycle timestamp")
	}
	observedAt := *snapshot.Lifecycle.ObservedAt
	age := time.Since(observedAt)
	if age > maxReadyObservationAge || age < -time.Minute {
		return nil, errors.New("requested model readiness evidence is stale or future-dated")
	}
	return &ModelObservation{
		State:       snapshot.Lifecycle.CurrentState,
		ObservedAt:  observedAt,
		Coverage:    response.Coverage,
		Identity:    snapshot.Identity,
		Lifecycle:   snapshot.Lifecycle,
		Provenance:  snapshot.Provenance,
		Activity:    snapshot.Activity,
		GPUWindow:   snapshot.GPUWindow,
		ConfigState: "not_exposed_by_diagnostics_api",
	}, nil
}

func (c *APIClient) RequestObservation(ctx context.Context, requestID, backend string) (*correlation.ProxyObservation, string) {
	if !validRequestID(requestID) {
		return nil, "request_id_unavailable"
	}
	var record requestRecord
	path := "/diagnostics/v1/requests/" + url.PathEscape(requestID)
	if err := c.getDiagnostics(ctx, path, nil, &record); err != nil {
		return nil, diagnosticErrorClass(err)
	}
	if record.RequestID != requestID || record.BackendID != backend || record.ProxyObservation == nil {
		return nil, "observation_unavailable"
	}
	return record.ProxyObservation, "available"
}

func diagnosticErrorClass(err error) string {
	var status *httpStatusError
	if errors.As(err, &status) {
		switch status.status {
		case http.StatusNotFound, http.StatusServiceUnavailable:
			return "history_unavailable"
		case http.StatusUnauthorized:
			return "unauthorized"
		default:
			return "query_failed"
		}
	}
	return "query_failed"
}

type httpStatusError struct {
	status int
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("diagnostics API returned HTTP %d", e.status)
}

func (c *APIClient) getDiagnostics(ctx context.Context, path string, query url.Values, dst any) error {
	if err := c.waitDiagnosticSlot(ctx); err != nil {
		return err
	}
	u := *c.origin
	u.Path = path
	u.RawPath = ""
	if query != nil {
		u.RawQuery = query.Encode()
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return errors.New("could not create diagnostics API request")
	}
	request.Header.Set("Accept", contentTypeJSON)
	request.Header.Set("Authorization", "Bearer "+c.apiKey)
	response, err := c.http.Do(request)
	if err != nil {
		return errors.New("diagnostics API request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return &httpStatusError{status: response.StatusCode}
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxDiagnosticsResponseBytes+1))
	if err != nil || len(body) > maxDiagnosticsResponseBytes {
		return errors.New("diagnostics API response is unavailable or too large")
	}
	if err := json.Unmarshal(body, dst); err != nil {
		return errors.New("diagnostics API returned an invalid response")
	}
	return nil
}

func (c *APIClient) waitDiagnosticSlot(ctx context.Context) error {
	c.diagnosticMu.Lock()
	defer c.diagnosticMu.Unlock()
	if !c.lastDiagnosticQuery.IsZero() {
		wait := c.diagnosticInterval - time.Since(c.lastDiagnosticQuery)
		if wait > 0 {
			timer := time.NewTimer(wait)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	c.lastDiagnosticQuery = time.Now()
	return nil
}

func (c *APIClient) Complete(ctx context.Context, model string, messages []chatMessage, workload syntheticWorkload, config RequestedConfiguration, maxTokens int, allowTools bool) (chatResult, string) {
	measurement := RequestMeasurement{
		StartedAt:       time.Now(),
		PromptBytes:     promptBytes(messages),
		MaxTokens:       maxTokens,
		DiagnosticState: "pending",
	}
	requestBody := map[string]any{
		"model":       model,
		"messages":    messages,
		"temperature": config.Temperature,
		"top_p":       config.TopP,
		"max_tokens":  maxTokens,
		"stream":      false,
	}
	if workload.ReasoningEffort != "" {
		requestBody["reasoning_effort"] = workload.ReasoningEffort
	}
	if allowTools {
		requestBody["tools"] = []map[string]any{{
			"type": "function",
			"function": map[string]any{
				"name":        "benchmark_echo",
				"description": "Return the supplied benchmark message unchanged.",
				"parameters": map[string]any{
					"type":                 "object",
					"properties":           map[string]any{"message": map[string]string{"type": "string"}},
					"required":             []string{"message"},
					"additionalProperties": false,
				},
			},
		}}
	}
	body, err := json.Marshal(requestBody)
	if err != nil || len(body) > maxChatRequestBytes {
		measurement.EndedAt = time.Now()
		measurement.ErrorClass = "request_failed"
		return chatResult{Measure: measurement}, "request_failed"
	}
	measurement.RequestBytes = len(body)
	u := *c.origin
	u.Path = "/v1/chat/completions"
	u.RawPath = ""
	u.RawQuery = ""
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		measurement.EndedAt = time.Now()
		measurement.ErrorClass = "request_failed"
		return chatResult{Measure: measurement}, "request_failed"
	}
	request.Header.Set("Accept", contentTypeJSON)
	request.Header.Set("Content-Type", contentTypeJSON)
	response, err := c.http.Do(request)
	measurement.EndedAt = time.Now()
	if err != nil {
		switch {
		case errors.Is(ctx.Err(), context.Canceled):
			measurement.ErrorClass = "request_canceled"
			return chatResult{Measure: measurement}, "request_canceled"
		case errors.Is(ctx.Err(), context.DeadlineExceeded):
			measurement.ErrorClass = "request_timeout"
			return chatResult{Measure: measurement}, "request_timeout"
		default:
			measurement.ErrorClass = "request_failed"
			return chatResult{Measure: measurement}, "request_failed"
		}
	}
	defer response.Body.Close()
	measurement.EndedAt = time.Now()
	measurement.HTTPStatus = response.StatusCode
	if requestID := response.Header.Get("X-Request-ID"); validRequestID(requestID) {
		measurement.RequestID = requestID
	}
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, maxChatResponseBytes+1))
	measurement.ResponseBytes = len(responseBody)
	measurement.EndedAt = time.Now()
	if readErr != nil || len(responseBody) > maxChatResponseBytes || response.StatusCode < 200 || response.StatusCode >= 300 {
		measurement.ErrorClass = "request_failed"
		if readErr == nil && response.StatusCode >= 200 && response.StatusCode < 300 {
			measurement.ErrorClass = "response_invalid"
		}
		return chatResult{Measure: measurement}, measurement.ErrorClass
	}
	var completion chatResponse
	if err := json.Unmarshal(responseBody, &completion); err != nil || len(completion.Choices) == 0 {
		measurement.ErrorClass = "response_invalid"
		return chatResult{Measure: measurement}, "response_invalid"
	}
	choice := completion.Choices[0]
	outputBytes := len(choice.Message.Content)
	for _, call := range choice.Message.ToolCalls {
		outputBytes += len(call.Function.Arguments)
	}
	measurement.OutputBytes = outputBytes
	return chatResult{
		Content:   choice.Message.Content,
		ToolCalls: choice.Message.ToolCalls,
		Measure:   measurement,
	}, ""
}

func (c *APIClient) attachObservation(ctx context.Context, measurement *RequestMeasurement, backend string) {
	observation, state := c.RequestObservation(ctx, measurement.RequestID, backend)
	measurement.ProxyObservation = observation
	measurement.DiagnosticState = state
}

func promptBytes(messages []chatMessage) int {
	total := 0
	for _, message := range messages {
		total += len(message.Content)
		for _, call := range message.ToolCalls {
			total += len(call.Function.Arguments)
		}
	}
	return total
}

func toolMessage(call toolCall, expected string) (string, bool) {
	if call.Type != "function" || call.Function.Name != "benchmark_echo" ||
		call.ID == "" || len(call.ID) > 128 || strings.ContainsAny(call.ID, "\r\n\x00") ||
		len(call.Function.Arguments) > 1024 {
		return "", false
	}
	var args struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal([]byte(call.Function.Arguments), &args); err != nil ||
		!utf8.ValidString(args.Message) || args.Message != expected {
		return "", false
	}
	return args.Message, true
}

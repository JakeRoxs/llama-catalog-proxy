package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

const maxCatalogBytes = 64 << 20

const showEnrichmentConcurrency = 8

type Response struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

type Backend struct {
	ID             string
	URL            *url.URL
	Prefix         string
	HealthPath     string
	ShowEnrichment bool
	APIKey         string
	Headers        http.Header
	Default        bool
}

type Service struct {
	client    *http.Client
	ttl       time.Duration
	logger    *slog.Logger
	backends  []*backendState
	defaultID string
}

type backendState struct {
	Backend

	mu        sync.Mutex
	root      map[string]any
	models    []map[string]any
	header    http.Header
	fetchedAt time.Time
	lastTry   time.Time
	lastErr   error
}

type catalogResult struct {
	backend *backendState
	root    map[string]any
	models  []map[string]any
	header  http.Header
	err     error
}

func New(backends []Backend, client *http.Client, ttl time.Duration, logger *slog.Logger) *Service {
	states := make([]*backendState, 0, len(backends))
	defaultID := ""
	for _, backend := range backends {
		states = append(states, &backendState{Backend: backend})
		if backend.Default {
			defaultID = backend.ID
		}
	}
	sort.Slice(states, func(i, j int) bool {
		if states[i].Default != states[j].Default {
			return states[i].Default
		}
		return states[i].ID < states[j].ID
	})
	return &Service{client: client, ttl: ttl, logger: logger, backends: states, defaultID: defaultID}
}

func (s *Service) Models(ctx context.Context, requestHeader http.Header) (Response, error) {
	results := s.fetchCatalogs(ctx, requestHeader)
	var defaultResult catalogResult
	for _, result := range results {
		if result.backend.Default {
			defaultResult = result
			break
		}
	}
	if defaultResult.err != nil {
		return Response{}, fmt.Errorf("fetch default backend model catalog: %w", defaultResult.err)
	}
	if defaultResult.root == nil {
		return Response{}, errors.New("default backend model catalog is unavailable")
	}

	root := cloneObject(defaultResult.root)
	aggregated := make([]any, 0)
	owners := make(map[string]string)
	for _, result := range results {
		if result.err != nil {
			s.logger.Warn("optional backend model catalog unavailable", "backend_id", result.backend.ID, "error", result.err)
			continue
		}
		for _, source := range result.models {
			model := cloneObject(source)
			localID := stringValue(model["id"])
			if localID == "" {
				continue
			}
			publicID := result.backend.Prefix + localID
			if owner, exists := owners[publicID]; exists {
				s.logger.Warn("duplicate public model ID rejected", "model_id", publicID, "backend_id", result.backend.ID, "existing_backend_id", owner)
				continue
			}
			model["id"] = publicID
			owners[publicID] = result.backend.ID
			aggregated = append(aggregated, model)
		}
	}
	root["data"] = aggregated
	body, err := json.Marshal(root)
	if err != nil {
		return Response{}, fmt.Errorf("encode aggregated model catalog: %w", err)
	}
	header := defaultResult.header.Clone()
	removeRepresentationHeaders(header)
	header.Set("Content-Type", "application/json")
	return Response{StatusCode: http.StatusOK, Header: header, Body: body}, nil
}

func (s *Service) BackendStatuses(ctx context.Context) (map[string]string, error) {
	statuses := make(map[string]string, len(s.backends))
	var defaultErr error
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, backend := range s.backends {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.backendReady(ctx, backend)
			status := "ready"
			if err != nil {
				status = "unavailable"
			}
			mu.Lock()
			statuses[backend.ID] = status
			if backend.Default && err != nil {
				defaultErr = err
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	return statuses, defaultErr
}

func (s *Service) fetchCatalogs(ctx context.Context, requestHeader http.Header) []catalogResult {
	results := make([]catalogResult, len(s.backends))
	var wg sync.WaitGroup
	for index, backend := range s.backends {
		wg.Add(1)
		go func() {
			defer wg.Done()
			headers := http.Header(nil)
			if backend.Default {
				headers = requestHeader
			}
			root, models, header, err := s.backendCatalog(ctx, backend, headers)
			results[index] = catalogResult{backend: backend, root: root, models: models, header: header, err: err}
		}()
	}
	wg.Wait()
	return results
}

func (s *Service) backendCatalog(ctx context.Context, backend *backendState, requestHeader http.Header) (map[string]any, []map[string]any, http.Header, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()

	now := time.Now()
	if backend.root != nil && now.Sub(backend.fetchedAt) < s.ttl {
		return backend.root, backend.models, backend.header, nil
	}
	if backend.lastErr != nil && now.Sub(backend.lastTry) < s.ttl {
		if backend.root != nil {
			return backend.root, backend.models, backend.header, nil
		}
		return nil, nil, nil, backend.lastErr
	}

	s.logger.Info("refreshing backend model catalog", "backend_id", backend.ID, "backend_url", backend.URL.Redacted())
	response, err := s.fetch(ctx, endpointURL(backend.URL, "/v1/models"), requestHeader, backend)
	if err == nil && (response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices) {
		err = fmt.Errorf("backend returned HTTP %d", response.StatusCode)
	}
	var root map[string]any
	var models []map[string]any
	if err == nil {
		var values []any
		root, values, err = decodeCatalog(response.Body)
		if err == nil {
			models = modelObjects(values)
		}
	}
	if err != nil {
		backend.lastTry = time.Now()
		backend.lastErr = err
		s.logger.Warn("backend model catalog refresh failed", "backend_id", backend.ID, "backend_url", backend.URL.Redacted(), "error", err, "using_stale", backend.root != nil)
		if backend.root != nil {
			return backend.root, backend.models, backend.header, nil
		}
		return nil, nil, nil, err
	}

	backend.root = root
	backend.models = models
	backend.header = response.Header
	backend.fetchedAt = time.Now()
	backend.lastTry = backend.fetchedAt
	backend.lastErr = nil
	if backend.ShowEnrichment {
		s.enrichWithShow(ctx, backend, models)
	}
	s.logger.Info("backend model catalog refreshed", "backend_id", backend.ID, "models", len(models))
	return backend.root, backend.models, backend.header, nil
}

func (s *Service) backendReady(ctx context.Context, backend *backendState) error {
	healthPath := backend.HealthPath
	if healthPath == "" {
		healthPath = "/health"
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpointURL(backend.URL, healthPath).String(), nil)
	if err != nil {
		return err
	}
	applyBackendHeaders(request.Header, backend)
	response, err := s.client.Do(request)
	if err != nil {
		return err
	}
	response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("backend %q health endpoint returned HTTP %d", backend.ID, response.StatusCode)
	}
	return nil
}

func (s *Service) enrichWithShow(ctx context.Context, backend *backendState, models []map[string]any) {
	semaphore := make(chan struct{}, showEnrichmentConcurrency)
	var wg sync.WaitGroup
	for index := range models {
		index := index
		wg.Add(1)
		semaphore <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-semaphore }()
			localID := stringValue(models[index]["id"])
			metadata, err := s.showMetadata(ctx, backend, localID)
			if err != nil {
				s.logger.Warn("ollama model metadata refresh failed", "backend_id", backend.ID, "model_id", localID, "error", err)
				return
			}
			if len(metadata) > 0 {
				models[index]["ollama"] = metadata
			}
		}()
	}
	wg.Wait()
}

func (s *Service) showMetadata(ctx context.Context, backend *backendState, modelID string) (map[string]any, error) {
	body, err := json.Marshal(map[string]string{"model": modelID})
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpointURL(backend.URL, "/api/show").String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	applyBackendHeaders(request.Header, backend)
	response, err := s.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("backend returned HTTP %d", response.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, maxCatalogBytes+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > maxCatalogBytes {
		return nil, errors.New("model details exceed 64 MiB limit")
	}
	var shown map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&shown); err != nil {
		return nil, err
	}
	return normalizeShowMetadata(shown), nil
}

func normalizeShowMetadata(shown map[string]any) map[string]any {
	metadata := make(map[string]any)
	details := nestedObject(shown, "details")
	for _, field := range []string{"context_length", "max_completion_tokens", "parameter_size", "quantization_level", "family", "format"} {
		if value, ok := details[field]; ok {
			metadata[field] = value
		}
	}
	if capabilities, ok := shown["capabilities"].([]any); ok {
		values := make([]any, 0, len(capabilities))
		for _, capability := range capabilities {
			if text, ok := capability.(string); ok {
				values = append(values, text)
			}
		}
		if len(values) > 0 {
			metadata["capabilities"] = values
		}
	}
	if _, exists := metadata["context_length"]; !exists {
		if architecture, ok := nestedValue(shown, "model_info", "general.architecture"); ok {
			if text, ok := architecture.(string); ok && text != "" {
				if contextLength, ok := nestedValue(shown, "model_info", text+".context_length"); ok {
					metadata["context_length"] = contextLength
				}
			}
		}
	}
	return metadata
}

func nestedObject(source map[string]any, key string) map[string]any {
	object, _ := source[key].(map[string]any)
	return object
}

func nestedValue(source map[string]any, key, field string) (any, bool) {
	if object := nestedObject(source, key); object != nil {
		if value, ok := object[field]; ok {
			return value, true
		}
	}
	return nil, false
}

func (s *Service) fetch(ctx context.Context, target *url.URL, requestHeader http.Header, backend *backendState) (Response, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return Response{}, err
	}
	copyHeaders(request.Header, requestHeader)
	removeHopByHopHeaders(request.Header)
	request.Header.Del("Accept-Encoding")
	applyBackendHeaders(request.Header, backend)
	response, err := s.client.Do(request)
	if err != nil {
		return Response{}, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxCatalogBytes+1))
	if err != nil {
		return Response{}, err
	}
	if len(body) > maxCatalogBytes {
		return Response{}, errors.New("model catalog exceeds 64 MiB limit")
	}
	header := response.Header.Clone()
	removeHopByHopHeaders(header)
	return Response{StatusCode: response.StatusCode, Header: header, Body: body}, nil
}

func applyBackendHeaders(header http.Header, backend *backendState) {
	for name, values := range backend.Headers {
		if header.Get(name) == "" {
			header[name] = append([]string(nil), values...)
		}
	}
	setAPIKeyIfMissing(header, backend.APIKey)
}

func decodeCatalog(body []byte) (map[string]any, []any, error) {
	var root map[string]any
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&root); err != nil {
		return nil, nil, err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return nil, nil, errors.New("model catalog contains trailing JSON data")
	}
	models, ok := root["data"].([]any)
	if !ok {
		return nil, nil, errors.New("model catalog data must be an array")
	}
	return root, models, nil
}

func modelObjects(values []any) []map[string]any {
	models := make([]map[string]any, 0, len(values))
	for _, value := range values {
		if model, ok := value.(map[string]any); ok {
			models = append(models, model)
		}
	}
	return models
}

func cloneObject(source map[string]any) map[string]any {
	clone := make(map[string]any, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

func endpointURL(base *url.URL, endpoint string) *url.URL {
	target := *base
	target.Path = strings.TrimRight(base.Path, "/") + endpoint
	target.RawPath = ""
	target.RawQuery = ""
	target.Fragment = ""
	return &target
}

func copyHeaders(destination, source http.Header) {
	for key, values := range source {
		for _, value := range values {
			destination.Add(key, value)
		}
	}
}

func setAPIKeyIfMissing(header http.Header, apiKey string) {
	if apiKey == "" || header.Get("Authorization") != "" || header.Get("X-Api-Key") != "" {
		return
	}
	header.Set("Authorization", "Bearer "+apiKey)
	header.Set("X-Api-Key", apiKey)
}

func removeHopByHopHeaders(header http.Header) {
	for _, connectionValue := range header.Values("Connection") {
		for _, name := range strings.Split(connectionValue, ",") {
			header.Del(strings.TrimSpace(name))
		}
	}
	for _, name := range []string{"Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Proxy-Connection", "Te", "Trailer", "Transfer-Encoding", "Upgrade"} {
		header.Del(name)
	}
}

func removeRepresentationHeaders(header http.Header) {
	for _, key := range []string{"Accept-Ranges", "Content-Digest", "Content-Encoding", "Content-Length", "Content-MD5", "Content-Range", "Digest", "ETag", "Last-Modified", "Repr-Digest"} {
		header.Del(key)
	}
}

func stringValue(value any) string {
	text, _ := value.(string)
	return text
}

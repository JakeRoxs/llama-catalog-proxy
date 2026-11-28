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
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/identity"
)

const maxCatalogBytes = 64 << 20

const maxModelAliases = 64

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
	client         *http.Client
	ttl            time.Duration
	healthTimeout  time.Duration
	catalogTimeout time.Duration
	logger         *slog.Logger
	backends       []*backendState
	defaultID      string
}

type backendState struct {
	Backend

	readinessMu sync.RWMutex
	readiness   BackendReadiness
	mu          sync.Mutex
	root        map[string]any
	models      []map[string]any
	header      http.Header
	fetchedAt   time.Time
	lastTry     time.Time
	lastErr     error
}

// BackendReadiness is the result of a completed local readiness check.
type BackendReadiness struct {
	BackendID  string
	Status     string
	ObservedAt time.Time
}

// BackendModel describes the models served by one backend, using cached
// catalog data only. The local model IDs have no prefix; the public ID is
// prefix + local ID. FetchedAt is the time the backend catalog was last
// successfully fetched; it is zero when no successful fetch has occurred.
type BackendModel struct {
	BackendID string
	Prefix    string
	Models    []map[string]any
	FetchedAt time.Time
}

type catalogResult struct {
	backend *backendState
	root    map[string]any
	models  []map[string]any
	header  http.Header
	err     error
}

func New(backends []Backend, client *http.Client, ttl time.Duration, healthTimeout, catalogTimeout time.Duration, logger *slog.Logger) *Service {
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
	return &Service{client: client, ttl: ttl, healthTimeout: healthTimeout, catalogTimeout: catalogTimeout, logger: logger, backends: states, defaultID: defaultID}
}

// aggregateModels merges cached backend model catalogs into one aggregated
// model list. Optional backend failures are logged as warnings and skipped;
// duplicate public IDs are rejected with the first backend winning; model
// objects are cloned and their IDs prefixed by the owning backend.
func aggregateModels(results []catalogResult, logger *slog.Logger) []any {
	aggregated := make([]any, 0)
	owners := make(map[string]string)
	for _, result := range results {
		if result.err != nil {
			logger.Warn("optional backend model catalog unavailable", "backend_id", result.backend.ID, "error", result.err)
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
				logger.Warn("duplicate public model ID rejected", "model_id", publicID, "backend_id", result.backend.ID, "existing_backend_id", owner)
				continue
			}
			model["id"] = publicID
			owners[publicID] = result.backend.ID
			aggregated = append(aggregated, model)
		}
	}
	return aggregated
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
	root["data"] = aggregateModels(results, s.logger)
	body, err := json.Marshal(root)
	if err != nil {
		return Response{}, fmt.Errorf("encode aggregated model catalog: %w", err)
	}
	header := defaultResult.header.Clone()
	removeRepresentationHeaders(header)
	header.Set("Content-Type", "application/json")
	return Response{StatusCode: http.StatusOK, Header: header, Body: body}, nil
}

// BackendModels returns the cached catalog models for each backend. No
// upstream requests are made; stale or unavailable backends return their last
// known models or an empty list. The returned models are clones so callers
// cannot alias cached data.
func (s *Service) BackendModels() []BackendModel {
	result := make([]BackendModel, 0, len(s.backends))
	for _, bs := range s.backends {
		bs.mu.Lock()
		models := make([]map[string]any, 0, len(bs.models))
		for _, m := range bs.models {
			models = append(models, cloneObject(m))
		}
		fetchedAt := bs.fetchedAt
		bs.mu.Unlock()
		result = append(result, BackendModel{
			BackendID: bs.ID,
			Prefix:    bs.Prefix,
			Models:    models,
			FetchedAt: fetchedAt,
		})
	}
	return result
}

// BackendPrefix returns the configured public prefix without fetching a catalog.
func (s *Service) BackendPrefix(id string) (string, bool) {
	for _, backend := range s.backends {
		if backend.ID == id {
			return backend.Prefix, true
		}
	}
	return "", false
}

// aliasSources collects the raw alias values advertised by a model from the
// top-level aliases field and the llama-swap meta.aliases field. Within each
// source, both []any and []string forms are accepted.
func aliasSources(model map[string]any) []any {
	var values []any
	if aliases, ok := model["aliases"].([]any); ok {
		values = append(values, aliases...)
	} else if aliases, ok := model["aliases"].([]string); ok {
		values = append(values, aliasSourceValues(aliases)...)
	}
	if meta, ok := model["meta"].(map[string]any); ok {
		if llamaSwap, ok := meta["llamaswap"].(map[string]any); ok {
			if aliases, ok := llamaSwap["aliases"].([]any); ok {
				values = append(values, aliases...)
			} else if aliases, ok := llamaSwap["aliases"].([]string); ok {
				values = append(values, aliasSourceValues(aliases)...)
			}
		}
	}
	return values
}

// aliasSourceValues converts one aliases field value ([]any or []string) into
// a []any slice.
func aliasSourceValues(source any) []any {
	switch a := source.(type) {
	case []any:
		return a
	case []string:
		result := make([]any, 0, len(a))
		for _, alias := range a {
			result = append(result, alias)
		}
		return result
	}
	return nil
}

// ModelAliases returns bounded, exact aliases advertised by a backend model
// catalog. Llama-swap exposes them in meta.llamaswap.aliases; a top-level
// aliases field is accepted for compatible backends. Alias-list records are
// not model identities and should be skipped when projecting canonical rows.
func ModelAliases(model map[string]any) []string {
	values := aliasSources(model)
	result := make([]string, 0, min(len(values), maxModelAliases))
	seen := make(map[string]struct{}, cap(result))
	for _, value := range values {
		alias, ok := value.(string)
		if !ok || !identity.ValidModelComponent(alias) || alias == "" {
			continue
		}
		if _, exists := seen[alias]; exists {
			continue
		}
		seen[alias] = struct{}{}
		result = append(result, alias)
		if len(result) == maxModelAliases {
			break
		}
	}
	sort.Strings(result)
	return result
}

// IsAliasModelRow reports whether a catalog row is a llama-swap's optional
// duplicate alias entry. Canonical model rows already contain its aliases.
func IsAliasModelRow(model map[string]any) bool {
	meta, ok := model["meta"].(map[string]any)
	if !ok {
		return false
	}
	llamaSwap, ok := meta["llamaswap"].(map[string]any)
	if !ok {
		return false
	}
	typeName, _ := llamaSwap["type"].(string)
	return typeName == "alias"
}

// VisitCachedModelIDs visits explicitly selected cached backends and their
// canonical model IDs and aliases without copying upstream objects or making
// network requests.
// Callbacks run under the individual backend's cache lock and must not call
// back into the service. Context cancellation interrupts lock waits and scans.
func (s *Service) VisitCachedModelIDs(ctx context.Context, selected map[string]bool, backend func(string, time.Time) error, model func(string, []string) error) error {
	for _, bs := range s.backends {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !selected[bs.ID] {
			continue
		}
		if err := bs.visitCachedModelIDs(ctx, backend, model); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (bs *backendState) visitCachedModelIDs(ctx context.Context, backend func(string, time.Time) error, model func(string, []string) error) error {
	if !bs.mu.TryLock() {
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for locked := false; !locked; {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				locked = bs.mu.TryLock()
			}
		}
	}
	defer bs.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := backend(bs.ID, bs.fetchedAt); err != nil {
		return err
	}
	for _, row := range bs.models {
		if err := ctx.Err(); err != nil {
			return err
		}
		if IsAliasModelRow(row) {
			continue
		}
		id, _ := row["id"].(string)
		if err := model(id, ModelAliases(row)); err != nil {
			return err
		}
	}
	return ctx.Err()
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
			observedAt := time.Now()
			status := "ready"
			if err != nil {
				status = "unavailable"
			}
			if ctx.Err() == nil {
				backend.readinessMu.Lock()
				if observedAt.After(backend.readiness.ObservedAt) {
					backend.readiness = BackendReadiness{BackendID: backend.ID, Status: status, ObservedAt: observedAt}
				}
				backend.readinessMu.Unlock()
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

// CachedBackendReadiness returns only existing checks; it never probes backends.
func (s *Service) CachedBackendReadiness(selected []string) []BackendReadiness {
	wanted := make(map[string]bool, len(selected))
	for _, id := range selected {
		wanted[id] = true
	}
	results := make([]BackendReadiness, 0, len(selected))
	for _, backend := range s.backends {
		if !wanted[backend.ID] {
			continue
		}
		backend.readinessMu.RLock()
		result := backend.readiness
		backend.readinessMu.RUnlock()
		if result.ObservedAt.IsZero() {
			result = BackendReadiness{BackendID: backend.ID, Status: "unknown"}
		}
		results = append(results, result)
	}
	sort.Slice(results, func(i, j int) bool { return results[i].BackendID < results[j].BackendID })
	return results
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
	return s.backendCatalogWithTTL(ctx, backend, requestHeader, s.ttl)
}

// RefreshBackendModels refreshes only selected backend caches with a tighter
// maximum age than the public catalog TTL. Failed refreshes retain their original
// observation timestamps, so consumers can still reject stale evidence.
func (s *Service) RefreshBackendModels(ctx context.Context, selected []string, maxAge time.Duration) {
	if maxAge <= 0 {
		return
	}
	ttl := min(s.ttl, maxAge)
	var workers sync.WaitGroup
	for _, backend := range s.backends {
		if !slices.Contains(selected, backend.ID) {
			continue
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			_, _, _, _ = s.backendCatalogWithTTL(ctx, backend, nil, ttl)
		}()
	}
	workers.Wait()
}

// acquireBackendLock acquires backend.mu, retrying with a 5 ms ticker until the
// lock is obtained or the context is cancelled. The returned unlock function
// must be deferred by the caller, so the caller retains lock ownership for the
// whole catalog operation. If the context is cancelled before the lock is
// obtained, the lock is not held and no unlock is performed.
func acquireBackendLock(ctx context.Context, backend *backendState) (unlock func(), err error) {
	if backend.mu.TryLock() {
		return func() { backend.mu.Unlock() }, nil
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for !backend.mu.TryLock() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
	return func() { backend.mu.Unlock() }, nil
}

// cacheHit returns the cached catalog when the backend has a fresh successful
// cache or a fresh error cache within ttl. Fresh successful caches take
// precedence; a fresh error cache returns the last error unless a stale
// successful cache is available, which is returned instead. The ok flag
// reports whether a short-circuit occurred.
func cacheHit(backend *backendState, now time.Time, ttl time.Duration) (map[string]any, []map[string]any, http.Header, error, bool) {
	if backend.root != nil && now.Sub(backend.fetchedAt) < ttl {
		return backend.root, backend.models, backend.header, nil, true
	}
	if backend.lastErr != nil && now.Sub(backend.lastTry) < ttl {
		if backend.root != nil {
			return backend.root, backend.models, backend.header, nil, true
		}
		return nil, nil, nil, backend.lastErr, true
	}
	return nil, nil, nil, nil, false
}

// fetchBackendCatalog fetches and decodes one backend model catalog using the
// supplied, already timeout-bounded context. Non-2xx responses and decode
// failures are returned as errors; the response header is returned only on
// success.
func (s *Service) fetchBackendCatalog(ctx context.Context, backend *backendState, requestHeader http.Header) (map[string]any, []map[string]any, http.Header, error) {
	s.logger.Info("refreshing backend model catalog", "backend_id", backend.ID, "backend_url", safeBackendURL(backend.URL))
	response, err := s.fetch(ctx, endpointURL(backend.URL, "/v1/models"), requestHeader, backend)
	if err == nil && (response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices) {
		err = fmt.Errorf("backend returned HTTP %d", response.StatusCode)
	}
	var root map[string]any
	var models []map[string]any
	var header http.Header
	if err == nil {
		var values []any
		root, values, err = decodeCatalog(response.Body)
		if err == nil {
			models = modelObjects(values)
			header = response.Header
		}
	}
	return root, models, header, err
}

func (s *Service) backendCatalogWithTTL(ctx context.Context, backend *backendState, requestHeader http.Header, ttl time.Duration) (map[string]any, []map[string]any, http.Header, error) {
	unlock, err := acquireBackendLock(ctx, backend)
	if err != nil {
		return nil, nil, nil, err
	}
	defer unlock()

	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}

	now := time.Now()
	if root, models, header, err, ok := cacheHit(backend, now, ttl); ok {
		return root, models, header, err
	}

	var root map[string]any
	var models []map[string]any
	var header http.Header

	operationCtx, cancel := context.WithTimeout(ctx, s.catalogTimeout)
	defer cancel()
	refreshStart := time.Now()

	root, models, header, err = s.fetchBackendCatalog(operationCtx, backend, requestHeader)
	if err != nil {
		backend.lastTry = time.Now()
		backend.lastErr = err
		refreshDuration := time.Since(refreshStart)
		staleAge := time.Duration(0)
		if backend.root != nil && !backend.fetchedAt.IsZero() {
			staleAge = time.Since(backend.fetchedAt)
		}
		s.logger.Warn("backend model catalog refresh failed",
			"backend_id", backend.ID,
			"backend_url", safeBackendURL(backend.URL),
			"error", safeErrorDetail(err),
			"using_stale", backend.root != nil,
			"refresh_duration_ms", refreshDuration.Seconds()*1000,
			"stale_cache_age_ms", staleAge.Seconds()*1000,
		)
		if backend.root != nil {
			return backend.root, backend.models, backend.header, nil
		}
		return nil, nil, nil, err
	}

	backend.root = root
	backend.models = models
	backend.header = header
	backend.fetchedAt = time.Now()
	backend.lastTry = backend.fetchedAt
	backend.lastErr = nil
	if backend.ShowEnrichment {
		s.enrichWithShow(operationCtx, backend, models)
	}
	refreshDuration := time.Since(refreshStart)
	s.logger.Info("backend model catalog refreshed",
		"backend_id", backend.ID,
		"models", len(models),
		"refresh_duration_ms", refreshDuration.Seconds()*1000,
	)
	return backend.root, backend.models, backend.header, nil
}

func safeErrorDetail(err error) string {
	var urlErr *url.Error
	if errors.As(err, &urlErr) && urlErr.Err != nil {
		return urlErr.Err.Error()
	}
	return err.Error()
}

func safeBackendURL(target *url.URL) string {
	redacted := *target
	if redacted.User != nil {
		redacted.User = url.User("xxxxx")
	}
	return redacted.String()
}

func (s *Service) backendReady(ctx context.Context, backend *backendState) error {
	healthPath := backend.HealthPath
	if healthPath == "" {
		healthPath = "/health"
	}
	readyCtx, cancel := context.WithTimeout(ctx, s.healthTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(readyCtx, http.MethodGet, endpointURL(backend.URL, healthPath).String(), nil)
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

// showDetails extracts the documented show-metadata fields from the details
// object when present.
func showDetails(details map[string]any) map[string]any {
	metadata := make(map[string]any)
	for _, field := range []string{"context_length", "max_completion_tokens", "parameter_size", "quantization_level", "family", "format"} {
		if value, ok := details[field]; ok {
			metadata[field] = value
		}
	}
	return metadata
}

// showCapabilities filters the capabilities list to its string entries.
func showCapabilities(capabilities []any) []any {
	values := make([]any, 0, len(capabilities))
	for _, capability := range capabilities {
		if text, ok := capability.(string); ok {
			values = append(values, text)
		}
	}
	return values
}

// architectureContextLength derives context_length from the model's general
// architecture when no explicit context length is present.
func architectureContextLength(shown map[string]any) (any, bool) {
	if architecture, ok := nestedValue(shown, "model_info", "general.architecture"); ok {
		if text, ok := architecture.(string); ok && text != "" {
			if contextLength, ok := nestedValue(shown, "model_info", text+".context_length"); ok {
				return contextLength, true
			}
		}
	}
	return nil, false
}

func normalizeShowMetadata(shown map[string]any) map[string]any {
	metadata := make(map[string]any)
	if details, ok := shown["details"].(map[string]any); ok {
		for field, value := range showDetails(details) {
			metadata[field] = value
		}
	}
	if capabilities, ok := shown["capabilities"].([]any); ok {
		if values := showCapabilities(capabilities); len(values) > 0 {
			metadata["capabilities"] = values
		}
	}
	if _, exists := metadata["context_length"]; !exists {
		if contextLength, ok := architectureContextLength(shown); ok {
			metadata["context_length"] = contextLength
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

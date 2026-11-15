package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/identity"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/privateorigin"
	"gopkg.in/yaml.v3"
)

const (
	defaultListen         = "0.0.0.0:8080"
	defaultHealthPath     = "/health"
	defaultModelsCacheTTL = 15 * time.Second
	defaultRequestTimeout = 10 * time.Second
	defaultLogLevel       = "INFO"
	defaultMaxJSONBody    = 64 << 20
)

const (
	envHealthTimeout  = "LLAMA_CATALOG_HEALTH_TIMEOUT"
	envCatalogTimeout = "LLAMA_CATALOG_CATALOG_TIMEOUT"
)

type Config struct {
	Listen             string
	LogLevel           string
	DefaultBackend     string
	Backends           map[string]Backend
	PathRoutes         map[string]string
	ModelsCacheTTL     time.Duration
	RequestTimeout     time.Duration
	HealthTimeout      time.Duration
	CatalogTimeout     time.Duration
	ComfyUIWaitTimeout time.Duration
	MaxJSONBody        int64
	RequestHistory     bool
	// RequestObservation enables per-request inference timing observation for
	// POST generation paths. It defaults to false and is opt-in.
	RequestObservation bool
	// DiagnosticsAPIKey is the dedicated token that protects the internal
	// diagnostics API. Empty disables the API; when set it authenticates
	// /diagnostics and /diagnostics/v1 requests. It is never echoed in
	// errors, logs, or JSON responses.
	DiagnosticsAPIKey string
	Peer              *PeerConfig
	Management        *ManagementConfig
	Benchmark         *BenchmarkConfig
}

type BenchmarkConfig struct {
	Token           string `yaml:"token"`
	ProxyOrigin     string `yaml:"proxy_origin"`
	ReportDirectory string `yaml:"report_directory"`
}

// ManagementConfig enables a separate HTTPS endpoint for local config previews.
type ManagementConfig struct {
	Listen          string                      `yaml:"listen"`
	TLSCertFile     string                      `yaml:"tls_cert_file"`
	TLSKeyFile      string                      `yaml:"tls_key_file"`
	PreviewToken    string                      `yaml:"preview_token"`
	DelegationToken string                      `yaml:"delegation_token,omitempty"`
	DelegatedStore  *DelegatedStoreConfig       `yaml:"delegated_store,omitempty"`
	ProposalStore   *ProposalStoreConfig        `yaml:"proposal_store,omitempty"`
	BackupStore     *BackupStoreConfig          `yaml:"backup_store,omitempty"`
	WriteToken      string                      `yaml:"write_token,omitempty"`
	Ingestion       *IngestionConfig            `yaml:"ingestion,omitempty"`
	Targets         map[string]ManagementTarget `yaml:"targets"`
}

type DelegatedStoreConfig struct {
	ProposalDirectory  string `yaml:"proposal_directory"`
	OperationDirectory string `yaml:"operation_directory"`
}

type IngestionConfig struct {
	Directory         string `yaml:"directory"`
	RecordDirectory   string `yaml:"record_directory"`
	MaxRecords        int    `yaml:"max_records"`
	Token             string `yaml:"token"`
	HuggingFaceToken  string `yaml:"huggingface_token,omitempty"`
	MaxBytes          int64  `yaml:"max_bytes"`
	MaxDirectoryBytes int64  `yaml:"max_directory_bytes"`
	MinFreeBytes      int64  `yaml:"min_free_bytes"`
	Timeout           string `yaml:"timeout"`
	FileMode          string `yaml:"file_mode"`
}

type ProposalStoreConfig struct {
	Directory string `yaml:"directory"`
	Token     string `yaml:"token"`
}

type BackupStoreConfig struct {
	Directory  string `yaml:"directory"`
	Token      string `yaml:"token"`
	MaxBackups int    `yaml:"max_backups"`
}

type ManagementTarget struct {
	Directory string `yaml:"directory"`
	Filename  string `yaml:"filename"`
}

// PeerConfig describes the opt-in HTTPS listener and explicitly configured peers.
type PeerConfig struct {
	NodeID            string               `yaml:"node_id"`
	Listen            string               `yaml:"listen"`
	TLSCertFile       string               `yaml:"tls_cert_file"`
	TLSKeyFile        string               `yaml:"tls_key_file"`
	LocalBackends     []string             `yaml:"local_backends"`
	ManagementTargets map[string]string    `yaml:"management_targets,omitempty"`
	Peers             map[string]PeerGrant `yaml:"peers"`
}

type PeerGrant struct {
	Token           string `yaml:"token"`
	CatalogRead     bool   `yaml:"catalog_read"`
	DiagnosticsRead bool   `yaml:"diagnostics_read"`
	Inference       bool   `yaml:"inference"`
	ManageSettings  bool   `yaml:"manage_settings"`
	URL             string `yaml:"url"`
	OutboundToken   string `yaml:"outbound_token"`
	CAFile          string `yaml:"ca_file"`
	RouteInference  bool   `yaml:"route_inference"`
	RouteManagement bool   `yaml:"route_management"`
}

type Backend struct {
	URL                  string            `yaml:"url"`
	Prefix               string            `yaml:"prefix"`
	HealthPath           string            `yaml:"health_path"`
	OllamaShowEnrichment bool              `yaml:"ollama_show_enrichment"`
	Diagnostics          string            `yaml:"diagnostics,omitempty"`
	APIKey               string            `yaml:"api_key,omitempty"`
	Headers              map[string]string `yaml:"headers,omitempty"`
}

type fileConfig struct {
	Listen             string             `yaml:"listen"`
	LogLevel           string             `yaml:"log_level"`
	DefaultBackend     string             `yaml:"default_backend"`
	Backends           map[string]Backend `yaml:"backends"`
	PathRoutes         map[string]string  `yaml:"path_routes"`
	ModelsCacheTTL     string             `yaml:"models_cache_ttl"`
	RequestTimeout     string             `yaml:"request_timeout"`
	HealthTimeout      string             `yaml:"health_timeout"`
	CatalogTimeout     string             `yaml:"catalog_timeout"`
	ComfyUIWaitTimeout string             `yaml:"comfyui_wait_timeout"`
	MaxJSONBody        *int64             `yaml:"max_json_request_body_bytes"`
	RequestHistory     *bool              `yaml:"request_history,omitempty"`
	RequestObservation *bool              `yaml:"request_observation,omitempty"`
	DiagnosticsAPIKey  *string            `yaml:"diagnostics_api_key,omitempty"`
	Peer               *PeerConfig        `yaml:"peer,omitempty"`
	Management         *ManagementConfig  `yaml:"management,omitempty"`
	Benchmark          *BenchmarkConfig   `yaml:"benchmark,omitempty"`
}

func Load(path string) (Config, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", path, err)
	}

	var raw fileConfig
	decoder := yaml.NewDecoder(strings.NewReader(string(contents)))
	decoder.KnownFields(true)
	if err := decoder.Decode(&raw); err != nil {
		return Config{}, fmt.Errorf("decode config %q: %w", path, err)
	}

	applyEnvironment(&raw)

	cfg := Config{
		Listen:         valueOrDefault(raw.Listen, defaultListen),
		LogLevel:       valueOrDefault(raw.LogLevel, defaultLogLevel),
		DefaultBackend: raw.DefaultBackend,
		Backends:       raw.Backends,
		PathRoutes:     raw.PathRoutes,
		ModelsCacheTTL: defaultModelsCacheTTL,
		RequestTimeout: defaultRequestTimeout,
		MaxJSONBody:    defaultMaxJSONBody,
		Peer:           raw.Peer,
		Management:     raw.Management,
		Benchmark:      raw.Benchmark,
	}
	if cfg.Backends == nil {
		cfg.Backends = make(map[string]Backend)
	}
	for id, backend := range cfg.Backends {
		if backend.HealthPath == "" {
			backend.HealthPath = defaultHealthPath
		}
		cfg.Backends[id] = backend
	}

	if raw.ModelsCacheTTL != "" {
		cfg.ModelsCacheTTL, err = time.ParseDuration(raw.ModelsCacheTTL)
		if err != nil {
			return Config{}, fmt.Errorf("parse models_cache_ttl: %w", err)
		}
	}
	if raw.RequestTimeout != "" {
		cfg.RequestTimeout, err = time.ParseDuration(raw.RequestTimeout)
		if err != nil {
			return Config{}, fmt.Errorf("parse request_timeout: %w", err)
		}
	}
	// Optional timeouts inherit request_timeout when not explicitly configured.
	cfg.HealthTimeout = cfg.RequestTimeout
	cfg.CatalogTimeout = cfg.RequestTimeout
	if raw.HealthTimeout != "" {
		cfg.HealthTimeout, err = time.ParseDuration(raw.HealthTimeout)
		if err != nil {
			return Config{}, fmt.Errorf("parse health_timeout: %w", err)
		}
	}
	if raw.CatalogTimeout != "" {
		cfg.CatalogTimeout, err = time.ParseDuration(raw.CatalogTimeout)
		if err != nil {
			return Config{}, fmt.Errorf("parse catalog_timeout: %w", err)
		}
	}
	if raw.ComfyUIWaitTimeout != "" {
		cfg.ComfyUIWaitTimeout, err = time.ParseDuration(raw.ComfyUIWaitTimeout)
		if err != nil {
			return Config{}, fmt.Errorf("parse comfyui_wait_timeout: %w", err)
		}
	}
	if raw.MaxJSONBody != nil {
		cfg.MaxJSONBody = *raw.MaxJSONBody
	}
	if raw.RequestHistory != nil {
		cfg.RequestHistory = *raw.RequestHistory
	}
	if raw.RequestObservation != nil {
		cfg.RequestObservation = *raw.RequestObservation
	}
	if raw.DiagnosticsAPIKey != nil {
		cfg.DiagnosticsAPIKey = *raw.DiagnosticsAPIKey
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.Listen) == "" {
		return errors.New("listen address is required")
	}
	if c.ModelsCacheTTL <= 0 {
		return errors.New("models_cache_ttl must be greater than zero")
	}
	if c.RequestTimeout <= 0 {
		return errors.New("request_timeout must be greater than zero")
	}
	if c.HealthTimeout <= 0 {
		return errors.New("health_timeout must be greater than zero")
	}
	if c.CatalogTimeout <= 0 {
		return errors.New("catalog_timeout must be greater than zero")
	}
	if c.ComfyUIWaitTimeout < 0 {
		return errors.New("comfyui_wait_timeout must not be negative")
	}
	if c.MaxJSONBody <= 0 {
		return errors.New("max_json_request_body_bytes must be greater than zero")
	}
	if !validLogLevel(c.LogLevel) {
		return errors.New("log_level must be one of debug, info, warn, error")
	}
	if strings.TrimSpace(c.DefaultBackend) == "" {
		return errors.New("default_backend is required")
	}
	_, exists := c.Backends[c.DefaultBackend]
	if !exists {
		return fmt.Errorf("default_backend %q is not defined in backends", c.DefaultBackend)
	}
	if err := validateDiagnosticsAPIKey(c.DiagnosticsAPIKey); err != nil {
		return err
	}
	if err := validatePeer(c); err != nil {
		return err
	}
	if err := validateManagement(c); err != nil {
		return err
	}
	if err := validateBenchmark(c); err != nil {
		return err
	}
	prefixOwners := make(map[string]string)
	for backendID, backend := range c.Backends {
		if strings.TrimSpace(backendID) == "" {
			return errors.New("backend ID must be non-empty")
		}
		if err := validateHTTPURL("backend "+backendID, backend.URL); err != nil {
			return err
		}
		if backend.Diagnostics != "" && backend.Diagnostics != "llama_swap" {
			return fmt.Errorf("backend %q diagnostics must be empty or llama_swap", backendID)
		}
		if backend.HealthPath != "" {
			if err := validateHealthPath("backend "+backendID, backend.HealthPath); err != nil {
				return err
			}
		}
		if strings.TrimSpace(backend.Prefix) != backend.Prefix {
			return fmt.Errorf("backend %q prefix must not have surrounding whitespace", backendID)
		}
		if backend.Prefix != "" {
			if owner, duplicate := prefixOwners[backend.Prefix]; duplicate {
				return fmt.Errorf("backends %q and %q use the same prefix %q", owner, backendID, backend.Prefix)
			}
			prefixOwners[backend.Prefix] = backendID
		}
		for name := range backend.Headers {
			if !validHeaderName(name) {
				return fmt.Errorf("backend %q contains invalid header name %q", backendID, name)
			}
			if !validHeaderValue(backend.Headers[name]) {
				return fmt.Errorf("backend %q header %q contains an invalid value", backendID, name)
			}
		}
	}
	paths := make([]string, 0, len(c.PathRoutes))
	for routePath := range c.PathRoutes {
		paths = append(paths, routePath)
	}
	sort.Strings(paths)
	for _, routePath := range paths {
		backendID := c.PathRoutes[routePath]
		if routePath == "" || routePath[0] != '/' || strings.TrimSpace(routePath) != routePath || strings.ContainsAny(routePath, "?#") ||
			path.Clean(routePath) != routePath || (routePath != "/" && strings.HasSuffix(routePath, "/")) {
			return fmt.Errorf("path route %q must be a canonical absolute path without a trailing slash", routePath)
		}
		if _, exists := c.Backends[backendID]; !exists {
			return fmt.Errorf("path route %q references undefined backend %q", routePath, backendID)
		}
	}
	if c.ComfyUIWaitTimeout > 0 {
		if _, exists := c.PathRoutes["/comfyui"]; !exists {
			return errors.New("comfyui_wait_timeout requires path_routes /comfyui")
		}
	}
	return nil
}

func validateBenchmark(c Config) error {
	b := c.Benchmark
	if b == nil {
		return nil
	}
	if c.DiagnosticsAPIKey == "" {
		return errors.New("benchmark requires the diagnostics API")
	}
	if err := validateManagementToken(c, b.Token); err != nil {
		return fmt.Errorf("benchmark credential invalid: %w", err)
	}
	if c.Management != nil {
		m := c.Management
		if b.Token == m.PreviewToken || b.Token == m.WriteToken ||
			m.ProposalStore != nil && b.Token == m.ProposalStore.Token ||
			m.BackupStore != nil && b.Token == m.BackupStore.Token {
			return errors.New("benchmark and management credentials must be distinct")
		}
	}
	if !filepath.IsAbs(b.ReportDirectory) || filepath.Clean(b.ReportDirectory) != b.ReportDirectory {
		return errors.New("benchmark report directory must be an absolute clean path")
	}
	if c.Management != nil {
		m := c.Management
		dirs := make([]string, 0, len(m.Targets)+2)
		for _, target := range m.Targets {
			dirs = append(dirs, target.Directory)
		}
		if m.ProposalStore != nil {
			dirs = append(dirs, m.ProposalStore.Directory)
		}
		if m.BackupStore != nil {
			dirs = append(dirs, m.BackupStore.Directory)
		}
		for _, directory := range dirs {
			if containsPath(b.ReportDirectory, directory) || containsPath(directory, b.ReportDirectory) {
				return errors.New("benchmark reports must be separate from management stores and targets")
			}
		}
	}
	if _, err := privateorigin.Parse(b.ProxyOrigin); err != nil {
		return fmt.Errorf("benchmark proxy origin: %w", err)
	}
	return nil
}

func validateManagement(c Config) error {
	m := c.Management
	if m == nil {
		return nil
	}
	_, port, err := net.SplitHostPort(m.Listen)
	if err != nil {
		return errors.New("management listen must be a host:port address")
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65535 || m.Listen == c.Listen || (c.Peer != nil && m.Listen == c.Peer.Listen) {
		return errors.New("management listen must use a valid distinct port")
	}
	if strings.TrimSpace(m.TLSCertFile) == "" || strings.TrimSpace(m.TLSKeyFile) == "" {
		return errors.New("management TLS certificate and key files are required")
	}
	delegationOnly := m.DelegationToken != "" && m.PreviewToken == "" && len(m.Targets) == 0 &&
		m.DelegatedStore == nil && m.ProposalStore == nil && m.BackupStore == nil && m.WriteToken == "" && m.Ingestion == nil
	if !delegationOnly {
		if err := validateManagementToken(c, m.PreviewToken); err != nil {
			return err
		}
	}
	if m.ProposalStore != nil {
		if !filepath.IsAbs(m.ProposalStore.Directory) || filepath.Clean(m.ProposalStore.Directory) != m.ProposalStore.Directory {
			return errors.New("proposal store directory must be an absolute clean path")
		}
		if m.ProposalStore.Token == m.PreviewToken {
			return errors.New("proposal and preview credentials must be distinct")
		}
		if err := validateManagementToken(c, m.ProposalStore.Token); err != nil {
			return err
		}
	}
	if m.BackupStore != nil {
		store := m.BackupStore
		if !filepath.IsAbs(store.Directory) || filepath.Clean(store.Directory) != store.Directory {
			return errors.New("backup store directory must be an absolute clean path")
		}
		if store.MaxBackups < 1 || store.MaxBackups > modelconfig.MaxBackups {
			return errors.New("backup store max_backups must be between 1 and 32")
		}
		if store.Token == m.PreviewToken || m.ProposalStore != nil && store.Token == m.ProposalStore.Token {
			return errors.New("backup, proposal, and preview credentials must be distinct")
		}
		if err := validateManagementToken(c, store.Token); err != nil {
			return err
		}
		if m.ProposalStore != nil && (containsPath(store.Directory, m.ProposalStore.Directory) ||
			containsPath(m.ProposalStore.Directory, store.Directory)) {
			return errors.New("backup and proposal stores must be separate")
		}
	}
	if m.WriteToken != "" {
		if m.ProposalStore == nil || m.BackupStore == nil {
			return errors.New("management write token requires proposal and backup stores")
		}
		if m.WriteToken == m.PreviewToken || m.WriteToken == m.ProposalStore.Token || m.WriteToken == m.BackupStore.Token {
			return errors.New("management write credential must be distinct")
		}
		if err := validateManagementToken(c, m.WriteToken); err != nil {
			return err
		}
	}
	if m.DelegationToken != "" {
		if err := validateManagementToken(c, m.DelegationToken); err != nil {
			return fmt.Errorf("delegation credential invalid: %w", err)
		}
		credentials := []string{m.PreviewToken, m.WriteToken}
		if m.ProposalStore != nil {
			credentials = append(credentials, m.ProposalStore.Token)
		}
		if m.BackupStore != nil {
			credentials = append(credentials, m.BackupStore.Token)
		}
		if m.Ingestion != nil {
			credentials = append(credentials, m.Ingestion.Token, m.Ingestion.HuggingFaceToken)
		}
		if c.Benchmark != nil {
			credentials = append(credentials, c.Benchmark.Token)
		}
		for _, credential := range credentials {
			if m.DelegationToken == credential {
				return errors.New("delegation credential must be distinct from other service credentials")
			}
		}
	}
	if m.Ingestion != nil {
		i := m.Ingestion
		if !filepath.IsAbs(i.Directory) || filepath.Clean(i.Directory) != i.Directory {
			return errors.New("ingestion directory must be an absolute clean path")
		}
		if !filepath.IsAbs(i.RecordDirectory) || filepath.Clean(i.RecordDirectory) != i.RecordDirectory ||
			i.MaxRecords < 1 || i.MaxRecords > 256 {
			return errors.New("ingestion requires an absolute record directory and max_records between 1 and 256")
		}
		timeout, err := time.ParseDuration(i.Timeout)
		if i.MaxBytes < 1 || err != nil || timeout <= 0 || (i.FileMode != "0600" && i.FileMode != "0640") {
			return errors.New("ingestion requires a positive byte limit and timeout and file mode 0600 or 0640")
		}
		if i.MaxDirectoryBytes < 1 || i.MinFreeBytes < 1 {
			return errors.New("ingestion requires positive max_directory_bytes and min_free_bytes")
		}
		if err := validateManagementToken(c, i.Token); err != nil {
			return fmt.Errorf("ingestion credential invalid: %w", err)
		}
		credentials := []string{m.PreviewToken, m.WriteToken}
		if m.ProposalStore != nil {
			credentials = append(credentials, m.ProposalStore.Token)
		}
		if m.BackupStore != nil {
			credentials = append(credentials, m.BackupStore.Token)
		}
		if c.Benchmark != nil {
			credentials = append(credentials, c.Benchmark.Token)
		}
		for _, credential := range credentials {
			if i.Token == credential || i.HuggingFaceToken != "" && i.HuggingFaceToken == credential {
				return errors.New("ingestion credentials must be distinct from other service credentials")
			}
		}
		if i.HuggingFaceToken != "" {
			if len(i.HuggingFaceToken) > 4096 || i.HuggingFaceToken == i.Token || managementCredentialConflict(c, i.HuggingFaceToken) != nil {
				return errors.New("invalid or reused Hugging Face credential")
			}
			for _, ch := range i.HuggingFaceToken {
				if ch < 0x21 || ch > 0x7e || ch == ',' {
					return errors.New("invalid Hugging Face credential")
				}
			}
		}
		dirs := make([]string, 0, len(m.Targets)+3)
		for _, target := range m.Targets {
			dirs = append(dirs, target.Directory)
		}
		if m.ProposalStore != nil {
			dirs = append(dirs, m.ProposalStore.Directory)
		}
		if m.BackupStore != nil {
			dirs = append(dirs, m.BackupStore.Directory)
		}
		if c.Benchmark != nil {
			dirs = append(dirs, c.Benchmark.ReportDirectory)
		}
		if containsPath(i.Directory, i.RecordDirectory) || containsPath(i.RecordDirectory, i.Directory) {
			return errors.New("ingestion records and model directory must be separate")
		}
		for _, directory := range dirs {
			if containsPath(i.Directory, directory) || containsPath(directory, i.Directory) ||
				containsPath(i.RecordDirectory, directory) || containsPath(directory, i.RecordDirectory) {
				return errors.New("ingestion directory must be separate from management and benchmark directories")
			}
		}
	}
	if len(m.Targets) > 32 || len(m.Targets) == 0 && !delegationOnly {
		return errors.New("management requires 1 to 32 targets")
	}
	for id, target := range m.Targets {
		if !identity.ValidNodeID(id) || id == "." || id == ".." ||
			!filepath.IsAbs(target.Directory) || filepath.Clean(target.Directory) != target.Directory ||
			target.Filename == "" || target.Filename == "." || target.Filename == ".." ||
			filepath.Base(target.Filename) != target.Filename || strings.ContainsAny(target.Filename, `/\`) {
			return errors.New("invalid management target")
		}
		if m.ProposalStore != nil && (containsPath(m.ProposalStore.Directory, target.Directory) ||
			containsPath(target.Directory, m.ProposalStore.Directory)) {
			return errors.New("proposal store must be separate from target directories")
		}
		if m.BackupStore != nil && (containsPath(m.BackupStore.Directory, target.Directory) ||
			containsPath(target.Directory, m.BackupStore.Directory)) {
			return errors.New("backup store must be separate from target directories")
		}
	}
	if m.DelegatedStore != nil {
		stores := m.DelegatedStore
		if m.BackupStore == nil {
			return errors.New("delegated stores require a backup store for owner review")
		}
		for _, directory := range []string{stores.ProposalDirectory, stores.OperationDirectory} {
			if !filepath.IsAbs(directory) || filepath.Clean(directory) != directory {
				return errors.New("delegated store directories must be absolute clean paths")
			}
		}
		dirs := []string{m.BackupStore.Directory}
		if m.ProposalStore != nil {
			dirs = append(dirs, m.ProposalStore.Directory)
		}
		if m.Ingestion != nil {
			dirs = append(dirs, m.Ingestion.Directory, m.Ingestion.RecordDirectory)
		}
		if c.Benchmark != nil {
			dirs = append(dirs, c.Benchmark.ReportDirectory)
		}
		for _, target := range m.Targets {
			dirs = append(dirs, target.Directory)
		}
		if containsPath(stores.ProposalDirectory, stores.OperationDirectory) ||
			containsPath(stores.OperationDirectory, stores.ProposalDirectory) {
			return errors.New("delegated proposal and operation stores must be separate")
		}
		for _, directory := range dirs {
			if containsPath(stores.ProposalDirectory, directory) || containsPath(directory, stores.ProposalDirectory) ||
				containsPath(stores.OperationDirectory, directory) || containsPath(directory, stores.OperationDirectory) {
				return errors.New("delegated stores must be separate from other stores and targets")
			}
		}
	}
	return nil
}

func containsPath(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func validateManagementToken(c Config, token string) error {
	if len(token) < 32 || len(token) > 256 || token == c.DiagnosticsAPIKey {
		return errors.New("management token must be distinct and contain 32 to 256 printable characters")
	}
	for _, ch := range token {
		if ch < 0x21 || ch > 0x7e || ch == ',' {
			return errors.New("management token contains invalid characters")
		}
	}
	return managementCredentialConflict(c, token)
}

func managementCredentialConflict(c Config, token string) error {
	if token != "" && token == c.DiagnosticsAPIKey {
		return errors.New("management and diagnostics credentials must be distinct")
	}
	for _, backend := range c.Backends {
		if token == backend.APIKey {
			return errors.New("management and backend credentials must be distinct")
		}
		for _, value := range backend.Headers {
			if value == token || value == "Bearer "+token {
				return errors.New("management and backend credentials must be distinct")
			}
		}
	}
	if c.Peer != nil {
		for _, grant := range c.Peer.Peers {
			if token == grant.Token || token == grant.OutboundToken {
				return errors.New("management and peer credentials must be distinct")
			}
		}
	}
	return nil
}

func validatePeer(c Config) error {
	p := c.Peer
	if p == nil {
		return nil
	}
	if !identity.ValidNodeID(p.NodeID) {
		return errors.New("peer node_id is invalid")
	}
	_, port, err := net.SplitHostPort(p.Listen)
	if err != nil {
		return errors.New("peer listen must be a host:port address")
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65535 || p.Listen == c.Listen {
		return errors.New("peer listen must use a valid port distinct from the client listener")
	}
	if strings.TrimSpace(p.TLSCertFile) == "" || strings.TrimSpace(p.TLSKeyFile) == "" {
		return errors.New("peer TLS certificate and key files are required")
	}
	if len(p.LocalBackends) > 256 || len(p.Peers) > 32 {
		return errors.New("too many peer backends or grants")
	}
	local := make(map[string]bool, len(p.LocalBackends))
	for _, id := range p.LocalBackends {
		if _, ok := c.Backends[id]; !ok || local[id] || id == "" || !identity.ValidModelComponent(id) {
			return errors.New("peer local_backends contains an invalid, duplicate, or undefined backend")
		}
		local[id] = true
	}
	if len(p.ManagementTargets) > len(local) || len(p.ManagementTargets) > 256 {
		return errors.New("peer management targets must refer to owned backends")
	}
	for backend, target := range p.ManagementTargets {
		if !local[backend] || c.Management == nil {
			return errors.New("peer management target requires an owned backend and management configuration")
		}
		if _, ok := c.Management.Targets[target]; !ok {
			return errors.New("peer management target references an undefined management target")
		}
	}
	tokens := make(map[string]bool, len(p.Peers))
	for id, grant := range p.Peers {
		if !identity.ValidNodeID(id) || id == p.NodeID {
			return errors.New("peer grant has invalid or self node identity")
		}
		if grant.DiagnosticsRead && !grant.CatalogRead {
			return errors.New("peer diagnostics_read requires catalog_read")
		}
		if grant.ManageSettings && (c.Management == nil || len(p.ManagementTargets) == 0) {
			return errors.New("peer manage_settings requires owner management targets")
		}
		if grant.Token == "" && (grant.CatalogRead || grant.DiagnosticsRead || grant.Inference || grant.ManageSettings || grant.URL == "") {
			return errors.New("peer grant requires an incoming token or outbound URL")
		}
		if grant.URL == "" {
			if grant.OutboundToken != "" || grant.CAFile != "" || grant.RouteInference || grant.RouteManagement {
				return errors.New("peer outbound token and CA require a URL")
			}
		} else {
			target, err := url.Parse(grant.URL)
			if err != nil || len(grant.URL) > 2048 || target.Scheme != "https" || target.Hostname() == "" || target.User != nil ||
				target.RawQuery != "" || target.Fragment != "" || (target.Path != "" && target.Path != "/") ||
				target.RawPath != "" || target.ForceQuery || strings.TrimSpace(grant.URL) != grant.URL {
				return errors.New("peer URL must be an HTTPS origin without credentials, path, query, or fragment")
			}
			if grant.OutboundToken == "" {
				return errors.New("peer outbound token is required")
			}
		}
		for _, token := range []string{grant.Token, grant.OutboundToken} {
			if token == "" {
				continue
			}
			if len(token) < 32 || len(token) > 256 || tokens[token] || token == c.DiagnosticsAPIKey {
				return errors.New("peer tokens must be distinct and contain 32 to 256 printable characters")
			}
			for _, ch := range token {
				if ch < 0x21 || ch > 0x7e || ch == ',' {
					return errors.New("peer token contains invalid characters")
				}
			}
			for _, backend := range c.Backends {
				if token == backend.APIKey {
					return errors.New("peer and backend credentials must be distinct")
				}
				for _, value := range backend.Headers {
					if value == token || value == "Bearer "+token {
						return errors.New("peer and backend credentials must be distinct")
					}
				}
			}
			tokens[token] = true
		}
	}
	return nil
}

func validHeaderName(name string) bool {
	if name == "" {
		return false
	}
	for index := range len(name) {
		character := name[index]
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || strings.ContainsRune("!#$%&'*+-.^_`|~", rune(character)) {
			continue
		}
		return false
	}
	return true
}

func validHeaderValue(value string) bool {
	return !strings.ContainsAny(value, "\r\n")
}

func RedactedURL(value string) string {
	parsed, err := url.Parse(value)
	if err != nil {
		return "<invalid-url>"
	}
	if parsed.User != nil {
		parsed.User = url.User("xxxxx")
	}
	return parsed.String()
}

func applyEnvironment(raw *fileConfig) {
	if value := os.Getenv("LLAMA_CATALOG_LISTEN"); value != "" {
		raw.Listen = value
	}
	if backend, exists := raw.Backends[raw.DefaultBackend]; exists {
		if value := os.Getenv("LLAMA_CATALOG_DEFAULT_BACKEND_URL"); value != "" {
			backend.URL = value
		}
		if value := os.Getenv("LLAMA_CATALOG_DEFAULT_BACKEND_API_KEY"); value != "" {
			backend.APIKey = value
		}
		raw.Backends[raw.DefaultBackend] = backend
	}
	if value := os.Getenv("LLAMA_CATALOG_MODELS_CACHE_TTL"); value != "" {
		raw.ModelsCacheTTL = value
	}
	if value := os.Getenv("LLAMA_CATALOG_REQUEST_TIMEOUT"); value != "" {
		raw.RequestTimeout = value
	}
	if value := os.Getenv(envHealthTimeout); value != "" {
		raw.HealthTimeout = value
	}
	if value := os.Getenv(envCatalogTimeout); value != "" {
		raw.CatalogTimeout = value
	}
	if value := os.Getenv("LLAMA_CATALOG_LOG_LEVEL"); value != "" {
		raw.LogLevel = value
	}
}

func validateHTTPURL(name, value string) error {
	parsed, err := url.Parse(value)
	if err != nil {
		return fmt.Errorf("parse %s URL: %w", name, err)
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return fmt.Errorf("%s URL must be an absolute http or https URL", name)
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("%s URL must not contain a query or fragment", name)
	}
	return nil
}

func validateHealthPath(name, value string) error {
	if !strings.HasPrefix(value, "/") || strings.TrimSpace(value) != value ||
		strings.ContainsAny(value, " \t\r\n?#") || path.Clean(value) != value {
		return fmt.Errorf("%s health_path %q must be a canonical absolute path without a query or fragment", name, value)
	}
	return nil
}

func valueOrDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func validLogLevel(level string) bool {
	switch strings.ToUpper(strings.TrimSpace(level)) {
	case "DEBUG", "INFO", "WARN", "ERROR":
		return true
	default:
		return false
	}
}

// validateDiagnosticsAPIKey rejects an empty key is allowed (disabled); a
// nonempty key must be at most 4096 bytes and contain no control characters
// (which covers CR and LF). The key value is never included in the error.
func validateDiagnosticsAPIKey(key string) error {
	if key == "" {
		return nil
	}
	if len(key) > 4096 {
		return errors.New("diagnostics_api_key must not exceed 4096 bytes")
	}
	for i := 0; i < len(key); i++ {
		if key[i] < 0x20 || key[i] == 0x7F {
			return errors.New("diagnostics_api_key must not contain control characters")
		}
	}
	return nil
}

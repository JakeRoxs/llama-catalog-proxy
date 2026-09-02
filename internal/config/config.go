package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"time"

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

type Config struct {
	Listen         string
	LogLevel       string
	DefaultBackend string
	Backends       map[string]Backend
	PathRoutes     map[string]string
	ModelsCacheTTL time.Duration
	RequestTimeout time.Duration
	MaxJSONBody    int64
}

type Backend struct {
	URL                  string            `yaml:"url"`
	Prefix               string            `yaml:"prefix"`
	HealthPath           string            `yaml:"health_path"`
	OllamaShowEnrichment bool              `yaml:"ollama_show_enrichment"`
	APIKey               string            `yaml:"api_key,omitempty"`
	Headers              map[string]string `yaml:"headers,omitempty"`
}

type fileConfig struct {
	Listen         string             `yaml:"listen"`
	LogLevel       string             `yaml:"log_level"`
	DefaultBackend string             `yaml:"default_backend"`
	Backends       map[string]Backend `yaml:"backends"`
	PathRoutes     map[string]string  `yaml:"path_routes"`
	ModelsCacheTTL string             `yaml:"models_cache_ttl"`
	RequestTimeout string             `yaml:"request_timeout"`
	MaxJSONBody    *int64             `yaml:"max_json_request_body_bytes"`
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
	if raw.MaxJSONBody != nil {
		cfg.MaxJSONBody = *raw.MaxJSONBody
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
	prefixOwners := make(map[string]string)
	for backendID, backend := range c.Backends {
		if strings.TrimSpace(backendID) == "" {
			return errors.New("backend ID must be non-empty")
		}
		if err := validateHTTPURL("backend "+backendID, backend.URL); err != nil {
			return err
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
	return parsed.Redacted()
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

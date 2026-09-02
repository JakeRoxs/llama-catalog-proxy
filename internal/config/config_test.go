package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadAppliesDefaultsAndEnvironmentOverrides(t *testing.T) {
	path := writeConfig(t, `
default_backend: default
backends:
  default:
    url: http://default-host:9292
  remote:
    url: http://remote-host:9292
    prefix: remote/
`)
	t.Setenv("LLAMA_CATALOG_LISTEN", "127.0.0.1:9090")
	t.Setenv("LLAMA_CATALOG_REQUEST_TIMEOUT", "3s")
	t.Setenv("LLAMA_CATALOG_DEFAULT_BACKEND_API_KEY", "default-secret")

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Listen != "127.0.0.1:9090" || cfg.ModelsCacheTTL != 15*time.Second || cfg.RequestTimeout != 3*time.Second {
		t.Fatalf("defaults or overrides not applied: %#v", cfg)
	}
	if cfg.Backends["remote"].Prefix != "remote/" {
		t.Errorf("backend prefix = %q", cfg.Backends["remote"].Prefix)
	}
	if cfg.Backends["default"].APIKey != "default-secret" {
		t.Error("default backend API key override was not applied")
	}
	if cfg.MaxJSONBody != 64<<20 {
		t.Errorf("default max JSON body = %d", cfg.MaxJSONBody)
	}
}

func TestLoadSupportsJSONRequestBodyLimit(t *testing.T) {
	cfg, err := Load(writeConfig(t, "default_backend: default\nbackends: {default: {url: http://default-host:9292}}\nmax_json_request_body_bytes: 1024\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxJSONBody != 1024 {
		t.Fatalf("max JSON body = %d", cfg.MaxJSONBody)
	}
}

func TestLoadRejectsInvalidJSONRequestBodyLimit(t *testing.T) {
	_, err := Load(writeConfig(t, "default_backend: default\nbackends: {default: {url: http://default-host:9292}}\nmax_json_request_body_bytes: 0\n"))
	if err == nil || !strings.Contains(err.Error(), "max_json_request_body_bytes") {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadSupportsBackendHeaders(t *testing.T) {
	cfg, err := Load(writeConfig(t, `
default_backend: default
backends:
  default:
    url: http://default-host:9292
    headers:
      X-Backend-Token: secret
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Backends["default"].Headers["X-Backend-Token"] != "secret" {
		t.Fatal("backend header was not loaded")
	}
}

func TestLoadRejectsInvalidBackendHeaders(t *testing.T) {
	for _, header := range []string{"Bad Header", "X-Bad:Header"} {
		_, err := Load(writeConfig(t, "default_backend: default\nbackends:\n  default:\n    url: http://default-host:9292\n    headers:\n      \""+header+"\": value\n"))
		if err == nil || !strings.Contains(err.Error(), "invalid header name") {
			t.Fatalf("header %q error = %v", header, err)
		}
	}
	_, err := Load(writeConfig(t, "default_backend: default\nbackends:\n  default:\n    url: http://default-host:9292\n    headers:\n      X-Test: \"bad\\nvalue\"\n"))
	if err == nil {
		t.Fatal("accepted backend header with a newline")
	}
}

func TestLoadSupportsPathRoutes(t *testing.T) {
	cfg, err := Load(writeConfig(t, `
default_backend: default
backends:
  default: {url: http://default-host:9292}
  ai5090: {url: http://ai5090:9292, prefix: ai5090/}
path_routes:
  /comfyui: ai5090
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PathRoutes["/comfyui"] != "ai5090" {
		t.Fatalf("path route = %q", cfg.PathRoutes["/comfyui"])
	}
}

func TestLoadRejectsInvalidPathRoutes(t *testing.T) {
	tests := []struct {
		name      string
		route     string
		backend   string
		wantError string
	}{
		{name: "relative", route: "comfyui", backend: "default", wantError: "canonical absolute path"},
		{name: "trailing slash", route: "/comfyui/", backend: "default", wantError: "canonical absolute path"},
		{name: "unclean", route: "/one/../comfyui", backend: "default", wantError: "canonical absolute path"},
		{name: "query", route: "/comfyui?mode=test", backend: "default", wantError: "canonical absolute path"},
		{name: "undefined backend", route: "/comfyui", backend: "missing", wantError: "undefined backend"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			contents := "default_backend: default\nbackends: {default: {url: http://default-host:9292}}\npath_routes:\n  " + test.route + ": " + test.backend + "\n"
			_, err := Load(writeConfig(t, contents))
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestLoadRejectsUnknownFields(t *testing.T) {
	_, err := Load(writeConfig(t, "default_backend: default\nbackends: {default: {url: http://default-host:9292}}\nunknown: true\n"))
	if err == nil {
		t.Fatal("Load() accepted an unknown field")
	}
}

func TestLoadRejectsMissingDefaultBackend(t *testing.T) {
	_, err := Load(writeConfig(t, "backends: {default: {url: http://default-host:9292}}\n"))
	if err == nil || !strings.Contains(err.Error(), "default_backend") {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadRejectsUndefinedDefaultBackend(t *testing.T) {
	_, err := Load(writeConfig(t, "default_backend: missing\nbackends: {default: {url: http://default-host:9292}}\n"))
	if err == nil || !strings.Contains(err.Error(), "not defined") {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadAcceptsPrefixedDefaultBackend(t *testing.T) {
	cfg, err := Load(writeConfig(t, "default_backend: default\nbackends: {default: {url: http://default-host:9292, prefix: default/}}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Backends["default"].Prefix != "default/" {
		t.Fatalf("default prefix = %q", cfg.Backends["default"].Prefix)
	}
}

func TestLoadRejectsDuplicateRoutingPrefixes(t *testing.T) {
	_, err := Load(writeConfig(t, `
default_backend: default
backends:
  default: {url: http://default-host:9292}
  one: {url: http://one:9292, prefix: shared/}
  two: {url: http://two:9292, prefix: shared/}
`))
	if err == nil || !strings.Contains(err.Error(), "same prefix") {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadRejectsInvalidLogLevel(t *testing.T) {
	_, err := Load(writeConfig(t, "default_backend: default\nbackends: {default: {url: http://default-host:9292}}\nlog_level: noisy\n"))
	if err == nil {
		t.Fatal("Load() accepted an invalid log_level")
	}
}

func TestRedactedURLHidesPassword(t *testing.T) {
	if got := RedactedURL("http://alice:supersecret@example.test:9292/base"); got != "http://alice:xxxxx@example.test:9292/base" {
		t.Fatalf("RedactedURL() = %q", got)
	}
}

func writeConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	proxyserver "github.com/JakeRoxs/llama-catalog-proxy/internal/proxy"
)

const shutdownTimeout = 10 * time.Second

var (
	version   = "dev"
	commit    = "unknown"
	buildDate = "unknown"
)

func parseLogLevel(level string) slog.Level {
	switch strings.ToUpper(strings.TrimSpace(level)) {
	case "DEBUG":
		return slog.LevelDebug
	case "WARN":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

type dynamicLevelHandler struct {
	inner slog.Handler
	level *int32
}

func (h *dynamicLevelHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= slog.Level(atomic.LoadInt32(h.level))
}

func (h *dynamicLevelHandler) Handle(ctx context.Context, record slog.Record) error {
	return h.inner.Handle(ctx, record)
}

func (h *dynamicLevelHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &dynamicLevelHandler{inner: h.inner.WithAttrs(attrs), level: h.level}
}

func (h *dynamicLevelHandler) WithGroup(name string) slog.Handler {
	return &dynamicLevelHandler{inner: h.inner.WithGroup(name), level: h.level}
}

func (h *dynamicLevelHandler) setLevel(level slog.Level) {
	atomic.StoreInt32(h.level, int32(level))
}

func main() {
	configPath := flag.String("config", "/config/config.yaml", "path to YAML configuration")
	healthcheckURL := flag.String("healthcheck", "", "check an HTTP health endpoint and exit")
	checkConfig := flag.Bool("check-config", false, "validate configuration and exit")
	showVersion := flag.Bool("version", false, "print version information and exit")
	flag.Parse()
	if *showVersion {
		fmt.Printf("llama-catalog-proxy %s (commit %s, built %s)\n", version, commit, buildDate)
		return
	}
	if *healthcheckURL != "" {
		if err := runHealthcheck(*healthcheckURL); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if *checkConfig {
		if _, err := config.Load(*configPath); err != nil {
			fmt.Fprintf(os.Stderr, "configuration invalid: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("configuration valid")
		return
	}

	level := int32(slog.LevelInfo)
	levelHandler := &dynamicLevelHandler{inner: slog.NewJSONHandler(os.Stdout, nil), level: &level}
	logger := slog.New(levelHandler)

	var initialListen string
	reloader, cfg, err := proxyserver.NewReloader(*configPath, logger, func(newCfg config.Config) {
		levelHandler.setLevel(parseLogLevel(newCfg.LogLevel))
		if newCfg.Listen != initialListen {
			logger.Warn("listen address change requires a restart", "requested", newCfg.Listen, "current", initialListen)
		}
	})
	if err != nil {
		logger.Error("configuration failed", "error", err)
		os.Exit(1)
	}
	initialListen = cfg.Listen
	levelHandler.setLevel(parseLogLevel(cfg.LogLevel))

	backendURLs := make(map[string]string, len(cfg.Backends))
	for id, backend := range cfg.Backends {
		backendURLs[id] = config.RedactedURL(backend.URL)
	}
	logger.Info("starting llama-catalog-proxy",
		"listen", cfg.Listen,
		"default_backend", cfg.DefaultBackend,
		"backends", backendURLs,
		"log_level", cfg.LogLevel,
		"models_cache_ttl", cfg.ModelsCacheTTL,
		"request_timeout", cfg.RequestTimeout,
		"max_json_request_body_bytes", cfg.MaxJSONBody,
		"version", version,
		"commit", commit,
	)

	reloader.Start()

	server := &http.Server{
		Addr:              cfg.Listen,
		Handler:           reloader,
		ReadHeaderTimeout: cfg.RequestTimeout,
	}

	signals, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	serverErrors := make(chan error, 1)
	go func() {
		logger.Info("server listening", "address", cfg.Listen)
		serverErrors <- server.ListenAndServe()
	}()

	select {
	case <-signals.Done():
		logger.Info("shutdown signal received")
	case err := <-serverErrors:
		if err != nil && err != http.ErrServerClosed {
			logger.Error("server stopped unexpectedly", "error", err)
			os.Exit(1)
		}
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
		_ = server.Close()
		os.Exit(1)
	}
	logger.Info("server stopped")
}

func runHealthcheck(target string) error {
	client := &http.Client{Timeout: 3 * time.Second}
	response, err := client.Get(target)
	if err != nil {
		return fmt.Errorf("healthcheck request failed: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("healthcheck returned HTTP %d", response.StatusCode)
	}
	return nil
}

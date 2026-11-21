package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/correlation"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/diagnostics"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modeldiagnostics"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/provenance"
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
	if len(os.Args) > 1 && os.Args[1] == "peer" {
		if err := runPeerCommand(os.Args[2:], os.Stdout, os.Stderr); err != nil {
			peerCommandError(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "comfyui-lease" {
		if err := runComfyUILeaseCommand(os.Args[2:], os.Stdout, os.Stderr); err != nil {
			fmt.Fprintln(os.Stderr, "comfyui lease command failed:", err)
			os.Exit(1)
		}
		return
	}
	configPath := flag.String("config", "/config/config.yaml", "path to YAML configuration")
	healthcheckURL := flag.String("healthcheck", "", "check an HTTP health endpoint and exit")
	checkConfig := flag.Bool("check-config", false, "validate configuration and exit")
	showVersion := flag.Bool("version", false, "print version information and exit")
	showDiagnostics := flag.Bool("diagnostics", false, "report opt-in backend diagnostics and, when request history is enabled, normalized backend activity as JSON and exit")
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
	if *showDiagnostics {
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		if err := runDiagnostics(ctx, *configPath, os.Stdout); err != nil {
			fmt.Fprintf(os.Stderr, "diagnostics failed: %v\n", err)
			os.Exit(1)
		}
		return
	}

	level := int32(slog.LevelInfo)
	levelHandler := &dynamicLevelHandler{inner: slog.NewJSONHandler(os.Stdout, nil), level: &level}
	logger := slog.New(levelHandler)

	var initialListen string
	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Error("configuration failed", "error", err)
		os.Exit(1)
	}
	diagnosticsService, err := diagnostics.New(cfg.Backends)
	if err != nil {
		logger.Error("diagnostics service failed", "error", err)
		os.Exit(1)
	}
	modelDiagnosticsService := modeldiagnostics.New()
	provenanceService := provenance.New()
	// The legacy timing prototype argument is accepted for signature
	// compatibility but is ignored: per-request observers are created in
	// ServeHTTP only when request_observation is enabled.
	reloader, cfg, err := proxyserver.NewReloader(*configPath, logger, func(newCfg config.Config) {
		levelHandler.setLevel(parseLogLevel(newCfg.LogLevel))
		if newCfg.Listen != initialListen {
			logger.Warn("listen address change requires a restart", "requested", newCfg.Listen, "current", initialListen)
		}
	}, diagnosticsService, modelDiagnosticsService, provenanceService, nil)
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

	server := &http.Server{
		Addr:              cfg.Listen,
		Handler:           reloader,
		ReadHeaderTimeout: cfg.RequestTimeout,
	}
	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		logger.Error("client listener failed", "error", err)
		os.Exit(1)
	}
	var peerServer *http.Server
	var peerListener net.Listener
	if cfg.Peer != nil {
		peerListener, err = net.Listen("tcp", cfg.Peer.Listen)
		if err != nil {
			_ = listener.Close()
			logger.Error("peer listener failed", "error", err)
			os.Exit(1)
		}
		peerServer = &http.Server{
			Handler: reloader.PeerHandler(), TLSConfig: reloader.PeerTLSConfig(),
			ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
			MaxHeaderBytes: 16 << 10,
		}
	}
	var managementServer *http.Server
	var managementListener net.Listener
	if cfg.Management != nil {
		managementListener, err = net.Listen("tcp", cfg.Management.Listen)
		if err != nil {
			_ = listener.Close()
			if peerListener != nil {
				_ = peerListener.Close()
			}
			logger.Error("management listener failed", "error", err)
			os.Exit(1)
		}
		managementServer = &http.Server{
			Handler: reloader.ManagementHandler(), TLSConfig: reloader.ManagementTLSConfig(),
			ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
			WriteTimeout: 15 * time.Second, MaxHeaderBytes: 16 << 10,
		}
	}
	reloader.Start()
	defer reloader.Stop()

	signals, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	serverErrors := make(chan error, 3)
	go func() {
		logger.Info("server listening", "address", cfg.Listen)
		serverErrors <- server.Serve(listener)
	}()
	if peerServer != nil {
		go func() {
			logger.Info("peer server listening", "address", cfg.Peer.Listen)
			serverErrors <- peerServer.ServeTLS(peerListener, "", "")
		}()
	}
	if managementServer != nil {
		go func() {
			logger.Info("management server listening", "address", cfg.Management.Listen)
			serverErrors <- managementServer.ServeTLS(managementListener, "", "")
		}()
	}

	failed := false
	select {
	case <-signals.Done():
		logger.Info("shutdown signal received")
	case err := <-serverErrors:
		if err != nil && err != http.ErrServerClosed {
			logger.Error("server stopped unexpectedly", "error", err)
			failed = true
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	reloader.Stop()
	if err := server.Shutdown(ctx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
		_ = server.Close()
		os.Exit(1)
	}
	if peerServer != nil {
		if err := peerServer.Shutdown(ctx); err != nil {
			logger.Error("peer graceful shutdown failed", "error", err)
			_ = peerServer.Close()
			failed = true
		}
	}
	if managementServer != nil {
		if err := managementServer.Shutdown(ctx); err != nil {
			logger.Error("management graceful shutdown failed", "error", err)
			_ = managementServer.Close()
			failed = true
		}
	}
	logger.Info("server stopped")
	if failed {
		os.Exit(1)
	}
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

func runDiagnostics(ctx context.Context, path string, writer io.Writer) error {
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	service, err := diagnostics.New(cfg.Backends)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	snapshots, err := service.All(ctx)
	if err != nil {
		return err
	}
	report := map[string]any{"backends": snapshots}
	if cfg.RequestHistory {
		// The collector gates collection itself: only backends with
		// Diagnostics == "llama_swap" are collected; all others make no
		// requests and are reported as disabled. No server is constructed
		// and no background collection is started here.
		collector := correlation.NewActivityCollector(cfg.Backends)
		if _, err := collector.Refresh(ctx); err != nil {
			return err
		}
		report["backend_activity"] = collector.Stats()

		// The same collector epoch scopes alias and lifecycle collection
		// so that activity, alias, and lifecycle observations share one
		// collection scope. Only GET /v1/models (source cache) and GET
		// /running (proven finite read-only) are requested; no public
		// route, background collection, SSE, or lifecycle action is
		// invoked.
		epoch := collector.Epoch()

		// Describe lifecycle event support: pure, no HTTP, no goroutines.
		// Reports what the adapter contract exposes (or does not expose)
		// rather than probing a deployed backend. The unsupported adapter
		// contract explains the absence of transition events; we do not
		// pretend 0 events were collected.
		support, err := correlation.DescribeLifecycleEventSupport(cfg.Backends, epoch)
		if err != nil {
			return err
		}
		report["backend_lifecycle_events"] = support

		// Alias collection: GET /v1/models only, source-cache read-only.
		aliasCollector := correlation.NewAliasCollector(cfg.Backends)
		aliasReport, err := aliasCollector.Collect(ctx, epoch)
		if err != nil {
			return err
		}
		report["backend_aliases"] = aliasReport

		// Resolve activity identities against the alias catalog.
		identities, err := correlation.ResolveActivityAliases(collector.Snapshot(), aliasReport)
		if err != nil {
			return err
		}
		report["activity_model_identity"] = identities

		// Associate activity records with lifecycle transition events.
		// The source adapter (llama-swap) does not expose transition events,
		// so the event list is empty and associations correctly report
		// no_matching_event (for resolved identities) or identity_unresolved
		// (for unresolved identities). This is expected: the unsupported
		// adapter contract explains the absence; we do not pretend 0 events
		// were collected.
		activityLifecycleEvents, err := correlation.AssociateLifecycleEvents(collector.Snapshot(), identities, nil, correlation.DefaultAssociationWindow())
		if err != nil {
			return err
		}
		report["activity_lifecycle_events"] = activityLifecycleEvents

		// Lifecycle collection: GET /running only, proven finite
		// read-only.
		lifecycleCollector := correlation.NewLifecycleCollector(cfg.Backends)
		lifecycleReport, err := lifecycleCollector.Collect(ctx, epoch)
		if err != nil {
			return err
		}
		report["backend_lifecycle"] = lifecycleReport

		// Link the current lifecycle snapshot to the resolved activity
		// identities.
		observations, err := correlation.LinkCurrentLifecycle(identities, lifecycleReport)
		if err != nil {
			return err
		}
		report["activity_lifecycle"] = observations

		// GPU collection runs after activity collection and shares the
		// same collector epoch so activity and GPU samples can be
		// associated within a single collection scope. No public endpoint,
		// background collection, inference, or model request is made here.
		gpuCollector := correlation.NewGPUCollector(cfg.Backends)
		gpuReport, err := gpuCollector.Collect(ctx, epoch)
		if err != nil {
			return err
		}
		report["backend_gpu"] = gpuReport

		// Associate activity records with GPU device samples within the
		// default time window. Samples are converted to normalized
		// timed device samples; raw upstream metadata is never copied.
		timed := make([]correlation.TimedDeviceSample, 0, len(gpuReport.Samples))
		for _, s := range gpuReport.Samples {
			timed = append(timed, correlation.TimedDeviceSample{
				BackendID: s.BackendID,
				Epoch:     s.Epoch,
				DeviceID:  s.DeviceID,
				Timestamp: s.Timestamp,
			})
		}
		associations, err := correlation.AssociateActivities(collector.Snapshot(), timed, correlation.DefaultAssociationWindow())
		if err != nil {
			return err
		}
		report["activity_gpu_associations"] = associations
	}
	return json.NewEncoder(writer).Encode(report)
}

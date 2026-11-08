package proxy

import (
	"crypto/sha256"
	"log/slog"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

const defaultWatchInterval = 2 * time.Second

type Reloader struct {
	path     string
	interval time.Duration
	logger   *slog.Logger
	onReload func(config.Config)
	handler  atomic.Pointer[http.Handler]
	lastHash [sha256.Size]byte
	// lastReloadError is the last reload failure that was logged. Identical
	// failures are suppressed so a broken config does not flood the logs;
	// retries continue on every watch tick.
	lastReloadError error
}

func NewReloader(path string, logger *slog.Logger, onReload func(config.Config)) (*Reloader, config.Config, error) {
	cfg, err := config.Load(path)
	if err != nil {
		return nil, config.Config{}, err
	}
	handler, err := New(cfg, logger)
	if err != nil {
		return nil, config.Config{}, err
	}
	r := &Reloader{
		path:     path,
		interval: defaultWatchInterval,
		logger:   logger,
		onReload: onReload,
	}
	r.handler.Store(&handler)
	digest, err := configDigest(path)
	if err != nil {
		return nil, config.Config{}, err
	}
	r.lastHash = digest
	return r, cfg, nil
}

func (r *Reloader) SetWatchInterval(interval time.Duration) {
	if interval > 0 {
		r.interval = interval
	}
}

func (r *Reloader) Start() {
	go r.watch()
}

func (r *Reloader) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	(*r.handler.Load()).ServeHTTP(writer, request)
}

func (r *Reloader) watch() {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	last := r.lastHash
	for range ticker.C {
		digest, err := configDigest(r.path)
		if err != nil {
			r.logger.Warn("config watch: cannot read config file", "error", err)
			continue
		}
		if digest == last {
			continue
		}
		if err := r.reload(); err != nil {
			if r.lastReloadError == nil || err.Error() != r.lastReloadError.Error() {
				r.logger.Error("config reload failed; keeping previous configuration", "error", err)
				r.lastReloadError = err
			}
			// Identical failures are suppressed; the next tick retries anyway.
			continue
		}
		r.lastReloadError = nil
		last = digest
	}
}

func (r *Reloader) reload() error {
	cfg, err := config.Load(r.path)
	if err != nil {
		return err
	}
	handler, err := New(cfg, r.logger)
	if err != nil {
		return err
	}
	r.handler.Store(&handler)
	if r.onReload != nil {
		r.onReload(cfg)
	}
	r.logger.Info("configuration reloaded", "default_backend", cfg.DefaultBackend, "backends", len(cfg.Backends))
	return nil
}

func configDigest(path string) ([sha256.Size]byte, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(contents), nil
}

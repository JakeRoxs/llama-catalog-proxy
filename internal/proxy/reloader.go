package proxy

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/auditlog"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/diagnostics"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/discovery"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/federation"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modeldiagnostics"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/provenance"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/timing"
)

const defaultWatchInterval = 2 * time.Second

type Reloader struct {
	path                string
	interval            time.Duration
	logger              *slog.Logger
	onReload            func(config.Config)
	state               atomic.Pointer[reloaderState]
	peerListen          string
	peerNodeID          string
	peerFederationID    string
	peerMembershipDir   string
	peerAuditDir        string
	peerCatalogGrantDir string
	peerDiscovery       *config.PeerDiscoveryConfig
	mdnsAdvertiser      *discovery.Advertiser
	pairingStore        *federation.PairingStore
	catalogGrantStore   *federation.CatalogGrantStore
	auditStore          *auditlog.Store
	managementListen    string
	managementSlots     chan struct{}
	delegatedStores     *config.DelegatedStoreConfig
	benchmark           *benchmarkRuntime
	adminAuth           *adminAuth
	managementUIConfig  *config.ManagementUIConfig
	ingestion           *ingestionRuntime
	instanceID          string
	peerSlots           chan struct{}
	peerInferenceSlots  chan struct{}
	peerPathSlots       chan struct{}
	peerWake            chan struct{}
	stop                chan struct{}
	lifecycleMu         sync.Mutex
	started             bool
	stopped             bool
	wg                  sync.WaitGroup
	lastHash            [sha256.Size]byte
	// lastReloadError is the last reload failure that was logged. Identical
	// failures are suppressed so a broken config does not flood the logs;
	// retries continue on every watch tick.
	lastReloadError        error
	lastPeerDiscoveryError string
}

type reloaderState struct {
	handler               http.Handler
	peer                  *config.PeerConfig
	cacheTTL              time.Duration
	generation            uint64
	certificate           *tls.Certificate
	peerClientCAs         *x509.CertPool
	peerClientCRLs        []*x509.RevocationList
	pairingStore          *federation.PairingStore
	catalogGrantStore     *federation.CatalogGrantStore
	auditStore            *auditlog.Store
	management            *config.ManagementConfig
	managementCertificate *tls.Certificate
	peerClients           map[string]*peerClient
	peerCache             *peerCache
	peerContext           context.Context
	peerCancel            context.CancelFunc
}

func NewReloader(path string, logger *slog.Logger, onReload func(config.Config), diagnostics *diagnostics.Service, modelDiagnostics *modeldiagnostics.Service, provenance *provenance.Service, timing *timing.Observer) (*Reloader, config.Config, error) {
	cfg, err := config.Load(path)
	if err != nil {
		return nil, config.Config{}, err
	}
	handler, err := New(cfg, logger, diagnostics, modelDiagnostics, provenance, timing)
	if err != nil {
		return nil, config.Config{}, err
	}
	r := newReloader(path, logger, onReload)
	initialized := false
	defer func() {
		if !initialized && r.pairingStore != nil {
			_ = r.pairingStore.Close()
		}
		if !initialized && r.catalogGrantStore != nil {
			_ = r.catalogGrantStore.Close()
		}
		if !initialized && r.auditStore != nil {
			_ = r.auditStore.Close()
		}
	}()
	if err := r.setupBenchmarkAndAdmin(cfg, logger); err != nil {
		return nil, config.Config{}, err
	}
	peerCert, err := r.setupPeer(cfg)
	if err != nil {
		return nil, config.Config{}, err
	}
	managementCert, err := r.setupManagement(cfg)
	if err != nil {
		return nil, config.Config{}, err
	}
	state, err := r.createReloaderState(cfg, handler, peerCert, managementCert)
	if err != nil {
		return nil, config.Config{}, err
	}
	if err := r.finalizeReloaderState(cfg, state); err != nil {
		return nil, config.Config{}, err
	}
	initialized = true
	return r, cfg, nil
}

// newReloader allocates the Reloader struct with its channels and returns it.
func newReloader(path string, logger *slog.Logger, onReload func(config.Config)) *Reloader {
	return &Reloader{
		path:               path,
		interval:           defaultWatchInterval,
		logger:             logger,
		onReload:           onReload,
		peerSlots:          make(chan struct{}, 8),
		peerInferenceSlots: make(chan struct{}, 8),
		peerPathSlots:      make(chan struct{}, 4),
		managementSlots:    make(chan struct{}, 2),
		peerWake:           make(chan struct{}, 1),
		stop:               make(chan struct{}),
	}
}

// setupBenchmarkAndAdmin creates the benchmark runtime and admin auth, and
// records the management UI configuration. It fails fast on the first error.
func (r *Reloader) setupBenchmarkAndAdmin(cfg config.Config, logger *slog.Logger) error {
	var err error
	r.benchmark, err = newBenchmarkRuntime(cfg, logger)
	if err != nil {
		return err
	}
	r.adminAuth, err = newAdminAuth(cfg.ManagementUI, logger, http.DefaultClient)
	if err != nil {
		return err
	}
	r.managementUIConfig = cfg.ManagementUI
	return nil
}

// setupPeer configures the peer identity, certificate, and stores. It returns
// the peer certificate (nil when no peer is configured). On failure, stores
// already opened are closed by the caller's !initialized defer backstop.
func (r *Reloader) setupPeer(cfg config.Config) (*tls.Certificate, error) {
	if cfg.Peer == nil {
		return nil, nil
	}
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return nil, fmt.Errorf("generate proxy instance identity: %w", err)
	}
	r.instanceID = hex.EncodeToString(id)
	r.peerListen, r.peerNodeID = cfg.Peer.Listen, cfg.Peer.NodeID
	if cfg.Peer.Discovery != nil {
		discoveryConfig := *cfg.Peer.Discovery
		r.peerDiscovery = &discoveryConfig
	}
	certificate, err := loadPeerCertificate(cfg.Peer)
	if err != nil {
		return nil, err
	}
	if cfg.Peer.FederationID != "" {
		r.peerFederationID = cfg.Peer.FederationID
		r.peerMembershipDir = cfg.Peer.MembershipDirectory
		r.pairingStore, err = federation.OpenPairingStore(cfg.Peer.MembershipDirectory, cfg.Peer.FederationID, cfg.Peer.NodeID)
		if err != nil {
			return nil, fmt.Errorf("open peer membership store: %w", err)
		}
	}
	if cfg.Peer.Discovery != nil && cfg.Peer.Discovery.Enabled {
		r.peerCatalogGrantDir = cfg.Peer.CatalogGrantDirectory
		r.catalogGrantStore, err = federation.OpenCatalogGrantStore(cfg.Peer.CatalogGrantDirectory, cfg.Peer.FederationID, cfg.Peer.NodeID)
		if err != nil {
			return nil, fmt.Errorf("open peer catalog grant store: %w", err)
		}
	}
	if cfg.Peer.AuditDirectory != "" {
		r.peerAuditDir = cfg.Peer.AuditDirectory
		r.auditStore, err = auditlog.Open(cfg.Peer.AuditDirectory, cfg.Peer.NodeID)
		if err != nil {
			return nil, fmt.Errorf("open peer audit store: %w", err)
		}
	}
	return certificate, nil
}

// setupManagement configures the management listener, delegated stores, and
// loads the management TLS certificate. It returns the management certificate
// (nil when no management is configured).
func (r *Reloader) setupManagement(cfg config.Config) (*tls.Certificate, error) {
	var err error
	var managementCertificate *tls.Certificate
	if cfg.Management != nil {
		r.managementListen = cfg.Management.Listen
		if cfg.Management.DelegatedStore != nil {
			stores := *cfg.Management.DelegatedStore
			r.delegatedStores = &stores
		}
		managementCertificate, err = loadManagementCertificate(cfg.Management)
		if err != nil {
			return nil, err
		}
	}
	return managementCertificate, nil
}

// createReloaderState builds the replacement state via makeReloaderState,
// wires the catalog grant store, and stores the state.
func (r *Reloader) createReloaderState(cfg config.Config, handler http.Handler, peerCert, managementCert *tls.Certificate) (*reloaderState, error) {
	state, err := makeReloaderState(cfg, reloaderStateOptions{
		handler:               handler,
		generation:            1,
		certificate:           peerCert,
		managementCertificate: managementCert,
		reservePeer:           r.peerListen != "",
		pairingStore:          r.pairingStore,
		auditStore:            r.auditStore,
	})
	if err != nil {
		return nil, err
	}
	state.catalogGrantStore = r.catalogGrantStore
	r.state.Store(state)
	return state, nil
}

// finalizeReloaderState computes the config digest, records it, and creates
// the ingestion runtime. On failure it cancels the peer context and closes
// the peer clients before returning; the caller's !initialized defer closes
// the stores.
func (r *Reloader) finalizeReloaderState(cfg config.Config, state *reloaderState) error {
	digest, err := configDigest(r.path)
	if err != nil {
		state.peerCancel()
		closePeerClients(state.peerClients)
		return err
	}
	r.lastHash = digest
	r.ingestion, err = newIngestionRuntime(cfg)
	if err != nil {
		state.peerCancel()
		closePeerClients(state.peerClients)
		return err
	}
	return nil
}

func (r *Reloader) SetWatchInterval(interval time.Duration) {
	if interval > 0 {
		r.interval = interval
	}
}

func (r *Reloader) Start() {
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.started || r.stopped {
		return
	}
	r.started = true
	r.startPeerDiscoveryAdvertisement()
	r.wg.Add(2)
	go r.watch()
	go r.watchPeers()
}

// startPeerDiscoveryAdvertisement starts the mDNS peer discovery advertiser when
// discovery is enabled. The caller holds lifecycleMu; the helper does not lock
// it. It does nothing when peerDiscovery is nil or disabled. On
// discovery.Start failure it logs a warning and continues without an
// advertiser; on success it stores the advertiser and logs an info message.
func (r *Reloader) startPeerDiscoveryAdvertisement() {
	if r.peerDiscovery == nil || !r.peerDiscovery.Enabled {
		return
	}
	advertiser, err := discovery.Start(discovery.Settings{
		NodeID: r.peerNodeID, FederationID: r.peerFederationID,
		Interface: r.peerDiscovery.Interface, AdvertiseURL: r.peerDiscovery.AdvertiseURL,
	})
	if err != nil {
		r.logger.Warn("peer mDNS advertisement unavailable; continuing without discovery", "error", err)
		return
	}
	r.mdnsAdvertiser = advertiser
	r.logger.Info("peer mDNS candidate advertisement started", "node_id", r.peerNodeID, "interface", r.peerDiscovery.Interface)
}

func (r *Reloader) Stop() {
	r.lifecycleMu.Lock()
	var advertiser *discovery.Advertiser
	if !r.stopped {
		r.stopped = true
		close(r.stop)
		r.state.Load().peerCancel()
		advertiser = r.mdnsAdvertiser
		r.mdnsAdvertiser = nil
	}
	r.lifecycleMu.Unlock()
	if advertiser != nil {
		if err := advertiser.Close(); err != nil {
			r.logger.Warn("peer mDNS advertisement shutdown failed", "error", err)
		}
	}
	if r.benchmark != nil {
		r.benchmark.jobs.Stop()
	}
	if r.ingestion != nil {
		if err := r.ingestion.jobs.Stop(); err != nil {
			r.logger.Error("ingestion record store shutdown failed", "error", err)
		}
	}
	r.wg.Wait()
	closePeerClients(r.state.Load().peerClients)
	r.closeManagedStores()
}

// closeManagedStores closes the pairing, audit, and catalog grant stores in
// that order, logging each closure failure without halting the remaining
// closures. It is invoked by Stop after peer clients are closed.
func (r *Reloader) closeManagedStores() {
	if r.pairingStore != nil {
		if err := r.pairingStore.Close(); err != nil {
			r.logger.Error("peer membership store shutdown failed", "error", err)
		}
	}
	if r.auditStore != nil {
		if err := r.auditStore.Close(); err != nil {
			r.logger.Error("peer audit store shutdown failed", "error", err)
		}
	}
	if r.catalogGrantStore != nil {
		if err := r.catalogGrantStore.Close(); err != nil {
			r.logger.Error("peer catalog grant store shutdown failed", "error", err)
		}
	}
}

func (r *Reloader) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if r.serveBenchmarkRoute(writer, request) {
		return
	}
	if r.serveAdminRoute(writer, request) {
		return
	}
	if r.serveReservedListenerRoute(writer, request) {
		return
	}
	r.state.Load().handler.ServeHTTP(writer, request)
}

// serveBenchmarkRoute handles /benchmark and /benchmark/* when a benchmark
// runtime is configured. It returns true when the request was handled; a nil
// benchmark leaves the route unmatched so ServeHTTP falls through.
func (r *Reloader) serveBenchmarkRoute(writer http.ResponseWriter, request *http.Request) bool {
	if r.benchmark == nil {
		return false
	}
	if request.URL.Path != "/benchmark" && !strings.HasPrefix(request.URL.Path, "/benchmark/") {
		return false
	}
	r.serveBenchmark(writer, request)
	return true
}

// serveAdminRoute handles /admin and /admin/* routes. It returns true when
// the request was handled. A nil adminAuth results in a 404 for any admin
// prefix; otherwise the named admin API routes dispatch to their handlers and
// the remainder falls through to adminAuth.
func (r *Reloader) serveAdminRoute(writer http.ResponseWriter, request *http.Request) bool {
	if request.URL.Path != "/admin" && !strings.HasPrefix(request.URL.Path, "/admin/") {
		return false
	}
	if r.adminAuth == nil {
		http.NotFound(writer, request)
		return true
	}
	switch request.URL.Path {
	case "/admin/api/v1/overview":
		r.serveAdminOverview(writer, request)
		return true
	case "/admin/api/v1/activity":
		r.serveAdminActivity(writer, request)
		return true
	case adminAuditPath:
		r.serveAdminAuditEvents(writer, request)
		return true
	}
	if request.URL.Path == adminManagementPath || strings.HasPrefix(request.URL.Path, adminManagementPath+"/") {
		r.serveAdminManagement(writer, request)
		return true
	}
	if request.URL.Path == adminPairingPath || strings.HasPrefix(request.URL.Path, adminPairingPath+"/") {
		r.serveAdminPairing(writer, request)
		return true
	}
	r.adminAuth.ServeHTTP(writer, request)
	return true
}

// serveReservedListenerRoute 404s requests to the reserved /peer and
// /management prefixes when the corresponding listener is enabled. It returns
// true when the request was handled; otherwise the request falls through to
// the current state handler.
func (r *Reloader) serveReservedListenerRoute(writer http.ResponseWriter, request *http.Request) bool {
	if r.peerListen != "" && (request.URL.Path == "/peer" || strings.HasPrefix(request.URL.Path, "/peer/")) {
		http.NotFound(writer, request)
		return true
	}
	if r.managementListen != "" && (request.URL.Path == "/management" || strings.HasPrefix(request.URL.Path, "/management/")) {
		http.NotFound(writer, request)
		return true
	}
	return false
}

func (r *Reloader) watch() {
	defer r.wg.Done()
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	last := r.lastHash
	for {
		select {
		case <-r.stop:
			return
		case <-ticker.C:
		}
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
	if err := r.validateReloadConfiguration(cfg); err != nil {
		return err
	}
	peerCert, managementCert, handler, err := r.buildReloadHandler(cfg)
	if err != nil {
		return err
	}
	if err := r.commitReloadState(cfg, handler, &reloadCertificates{
		peer:       peerCert,
		management: managementCert,
	}); err != nil {
		return err
	}
	if r.onReload != nil {
		r.onReload(cfg)
	}
	r.logger.Info("configuration reloaded", "default_backend", cfg.DefaultBackend, "backends", len(cfg.Backends))
	return nil
}

// reloadCertificates bundles the peer and management TLS certificates loaded
// for a reload. commitReloadState installs them into the replacement state
// under the lifecycle lock.
type reloadCertificates struct {
	peer       *tls.Certificate
	management *tls.Certificate
}

// validateReloadConfiguration performs every restart-required comparison and
// ingestion-directory validation in the exact order required by the reload
// lifecycle. It returns the first error, preserving the original error messages
// and order, and mutates no state. Certificate loading and handler
// construction are deferred to buildReloadHandler; the lifecycle lock is
// acquired only by commitReloadState.
func (r *Reloader) validateReloadConfiguration(cfg config.Config) error {
	if err := r.validateBenchmark(cfg); err != nil {
		return err
	}
	if err := r.validateManagementUI(cfg); err != nil {
		return err
	}
	if err := r.validatePeerDiscovery(cfg); err != nil {
		return err
	}
	if err := r.validateCatalogGrantDirectory(cfg); err != nil {
		return err
	}
	if err := r.validateDelegatedStoresAndIngestion(cfg); err != nil {
		return err
	}
	if err := r.validateIngestionDirectories(cfg); err != nil {
		return err
	}
	if err := r.validatePeerListener(cfg); err != nil {
		return err
	}
	if err := r.validateManagementListener(cfg); err != nil {
		return err
	}
	return nil
}

// validateBenchmark returns an error when the benchmark configuration or its
// diagnostics credential would require a restart to change.
func (r *Reloader) validateBenchmark(cfg config.Config) error {
	if (cfg.Benchmark == nil) != (r.benchmark == nil) ||
		cfg.Benchmark != nil && (*cfg.Benchmark != r.benchmark.config || cfg.DiagnosticsAPIKey != r.benchmark.diagnosticsKey) {
		return errors.New("changing benchmark configuration or its diagnostics credential requires a restart")
	}
	return nil
}

// validateManagementUI returns an error when management_ui authentication
// would require a restart to change.
func (r *Reloader) validateManagementUI(cfg config.Config) error {
	if !reflect.DeepEqual(cfg.ManagementUI, r.managementUIConfig) {
		return errors.New("changing management_ui authentication requires a restart")
	}
	return nil
}

// validatePeerDiscovery returns an error when peer discovery configuration
// would require a restart to change.
func (r *Reloader) validatePeerDiscovery(cfg config.Config) error {
	var peerDiscovery *config.PeerDiscoveryConfig
	if cfg.Peer != nil {
		peerDiscovery = cfg.Peer.Discovery
	}
	if !reflect.DeepEqual(peerDiscovery, r.peerDiscovery) {
		return errors.New("changing peer discovery configuration requires a restart")
	}
	return nil
}

// validateCatalogGrantDirectory returns an error when the peer catalog grant
// directory would require a restart to change.
func (r *Reloader) validateCatalogGrantDirectory(cfg config.Config) error {
	var catalogGrantDirectory string
	if cfg.Peer != nil {
		catalogGrantDirectory = cfg.Peer.CatalogGrantDirectory
	}
	if catalogGrantDirectory != r.peerCatalogGrantDir {
		return errors.New("changing peer catalog grant directory requires a restart")
	}
	return nil
}

// validateDelegatedStoresAndIngestion returns an error when delegated store
// directories or the ingestion configuration would require a restart to change.
func (r *Reloader) validateDelegatedStoresAndIngestion(cfg config.Config) error {
	var ingestionConfig *config.IngestionConfig
	if cfg.Management != nil {
		ingestionConfig = cfg.Management.Ingestion
		if stores := cfg.Management.DelegatedStore; stores != nil &&
			(r.delegatedStores == nil || *stores != *r.delegatedStores) {
			return errors.New("enabling or changing delegated store directories requires a restart")
		}
	}
	if (ingestionConfig == nil) != (r.ingestion == nil) ||
		ingestionConfig != nil && *ingestionConfig != r.ingestion.config {
		return errors.New("changing ingestion configuration requires a restart")
	}
	return nil
}

// validateIngestionDirectories validates the ingestion directories when an
// ingestion configuration is present. It is a no-op when ingestion is disabled.
func (r *Reloader) validateIngestionDirectories(cfg config.Config) error {
	var ingestionConfig *config.IngestionConfig
	if cfg.Management != nil {
		ingestionConfig = cfg.Management.Ingestion
	}
	if ingestionConfig == nil {
		return nil
	}
	if err := validateIngestionDirectories(cfg.Management, cfg.Benchmark); err != nil {
		return err
	}
	return nil
}

// validatePeerListener returns an error when the peer listener address or node
// ID, or protected peer state directories, would require a restart to change.
func (r *Reloader) validatePeerListener(cfg config.Config) error {
	if cfg.Peer == nil {
		return nil
	}
	if r.peerListen == "" || cfg.Peer.Listen != r.peerListen || cfg.Peer.NodeID != r.peerNodeID {
		return errors.New("enabling peer listener or changing its address or node ID requires a restart")
	}
	if cfg.Peer.FederationID != r.peerFederationID || cfg.Peer.MembershipDirectory != r.peerMembershipDir ||
		cfg.Peer.AuditDirectory != r.peerAuditDir || cfg.Peer.CatalogGrantDirectory != r.peerCatalogGrantDir {
		return errors.New("changing peer federation identity or protected state directories requires a restart")
	}
	return nil
}

// validateManagementListener returns an error when the management listener
// would require a restart to enable or change.
func (r *Reloader) validateManagementListener(cfg config.Config) error {
	if cfg.Management == nil {
		return nil
	}
	if r.managementListen == "" || cfg.Management.Listen != r.managementListen {
		return errors.New("enabling or changing the management listener requires a restart")
	}
	return nil
}

// buildReloadHandler loads the peer and management TLS certificates (if any),
// creates the diagnostics/model diagnostics/provenance services, and
// constructs the HTTP handler for the new configuration. It returns the
// certificates and handler so commitReloadState can install them under the
// lifecycle lock.
func (r *Reloader) buildReloadHandler(cfg config.Config) (peerCert, managementCert *tls.Certificate, handler http.Handler, err error) {
	if cfg.Peer != nil {
		peerCert, err = loadPeerCertificate(cfg.Peer)
		if err != nil {
			return
		}
	}
	if cfg.Management != nil {
		managementCert, err = loadManagementCertificate(cfg.Management)
		if err != nil {
			return
		}
	}
	diagnosticsService, err := diagnostics.New(cfg.Backends)
	if err != nil {
		return
	}
	modelDiagnosticsService := modeldiagnostics.New()
	provenanceService := provenance.New()
	// The legacy timing prototype argument is accepted for signature
	// compatibility but is ignored: per-request observers are created in
	// ServeHTTP only when request_observation is enabled.
	handler, err = New(cfg, r.logger, diagnosticsService, modelDiagnosticsService, provenanceService, nil)
	if err != nil {
		return
	}
	return
}

// commitReloadState installs the replacement state under lifecycleMu. The
// caller holds no locks. It checks stopped/generation overflow, builds the
// replacement state, swaps it in, cancels the old peer context, closes the
// old peer clients, and signals the peer watcher. Old state is left immutable
// and old clients are closed only after the swap.
func (r *Reloader) commitReloadState(cfg config.Config, handler http.Handler, certs *reloadCertificates) error {
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.stopped {
		return errors.New("configuration reloader stopped")
	}
	old := r.state.Load()
	if old.generation == ^uint64(0) {
		return errors.New("configuration generation exhausted")
	}
	state, err := makeReloaderState(cfg, reloaderStateOptions{
		handler:               handler,
		generation:            old.generation + 1,
		certificate:           certs.peer,
		managementCertificate: certs.management,
		reservePeer:           r.peerListen != "",
		pairingStore:          r.pairingStore,
		auditStore:            r.auditStore,
	})
	if err != nil {
		return err
	}
	state.catalogGrantStore = r.catalogGrantStore
	r.state.Store(state)
	old.peerCancel()
	closePeerClients(old.peerClients)
	select {
	case r.peerWake <- struct{}{}:
	default:
	}
	return nil
}

// reloaderStateOptions groups the non-configuration parameters of
// makeReloaderState so the constructor takes two parameters: the config under
// validation and the options bundle. cfg stays separate because it is the
// configuration being validated/reloaded, not a bundle of opaque state.
type reloaderStateOptions struct {
	handler               http.Handler
	generation            uint64
	certificate           *tls.Certificate
	managementCertificate *tls.Certificate
	reservePeer           bool
	pairingStore          *federation.PairingStore
	auditStore            *auditlog.Store
}

func makeReloaderState(cfg config.Config, opts reloaderStateOptions) (*reloaderState, error) {
	peerClientCAs, err := loadPeerClientCAs(cfg.Peer)
	if err != nil {
		return nil, err
	}
	peerClientCRLs, err := loadPeerClientCRLs(cfg.Peer)
	if err != nil {
		return nil, err
	}
	if cfg.Management != nil && cfg.Management.DelegatedStore != nil && cfg.Management.DelegatedStore.GrantDirectory != "" {
		if err := modelconfig.ValidateOwnerGrantStore(cfg.Management.DelegatedStore.GrantDirectory); err != nil {
			return nil, err
		}
	}
	clients, cache, err := newPeerClientsWithMembership(cfg.Peer, opts.pairingStore)
	if err != nil {
		return nil, err
	}
	if opts.reservePeer {
		h, ok := opts.handler.(*Handler)
		if !ok {
			return nil, errors.New("unexpected handler type for peer configuration")
		}
		h.peerCache = cache
	}
	ctx, cancel := context.WithCancel(context.Background())
	if opts.reservePeer {
		h := opts.handler.(*Handler)
		h.peerClients = clients
		h.peerContext = ctx
		h.peerInferenceSlots = make(chan struct{}, 8)
	}
	return &reloaderState{
		handler: opts.handler, peer: copyPeerConfig(cfg.Peer), cacheTTL: cfg.ModelsCacheTTL,
		generation: opts.generation, certificate: opts.certificate, peerClientCAs: peerClientCAs, peerClientCRLs: peerClientCRLs,
		pairingStore: opts.pairingStore, auditStore: opts.auditStore, peerClients: clients,
		management: copyManagementConfig(cfg.Management), managementCertificate: opts.managementCertificate,
		peerCache: cache, peerContext: ctx, peerCancel: cancel,
	}, nil
}

func loadManagementCertificate(management *config.ManagementConfig) (*tls.Certificate, error) {
	certificate, err := tls.LoadX509KeyPair(management.TLSCertFile, management.TLSKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load management TLS key pair: %w", err)
	}
	return &certificate, nil
}

func copyManagementConfig(management *config.ManagementConfig) *config.ManagementConfig {
	if management == nil {
		return nil
	}
	managementCopy := *management
	if management.DelegatedStore != nil {
		stores := *management.DelegatedStore
		managementCopy.DelegatedStore = &stores
	}
	if management.ProposalStore != nil {
		store := *management.ProposalStore
		managementCopy.ProposalStore = &store
	}
	if management.BackupStore != nil {
		store := *management.BackupStore
		managementCopy.BackupStore = &store
	}
	if management.Ingestion != nil {
		ingestion := *management.Ingestion
		managementCopy.Ingestion = &ingestion
	}
	managementCopy.Targets = make(map[string]config.ManagementTarget, len(management.Targets))
	for id, target := range management.Targets {
		managementCopy.Targets[id] = target
	}
	return &managementCopy
}

func loadPeerCertificate(peer *config.PeerConfig) (*tls.Certificate, error) {
	certificate, err := tls.LoadX509KeyPair(peer.TLSCertFile, peer.TLSKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load peer TLS key pair: %w", err)
	}
	if peer.FederationID != "" {
		if len(certificate.Certificate) == 0 {
			return nil, errors.New("peer identity certificate is empty")
		}
		leaf, err := x509.ParseCertificate(certificate.Certificate[0])
		if err != nil {
			return nil, fmt.Errorf("parse peer identity certificate: %w", err)
		}
		certificate.Leaf = leaf
		if err := validatePeerIdentityCertificate(leaf, peer.FederationID, peer.NodeID); err != nil {
			return nil, fmt.Errorf("invalid peer identity certificate: %w", err)
		}
	}
	return &certificate, nil
}

func loadPeerClientCAs(peer *config.PeerConfig) (*x509.CertPool, error) {
	if peer == nil || peer.FederationID == "" {
		return nil, nil
	}
	return federation.LoadTrustRoots(peer.ClientCAFile)
}

func loadPeerClientCRLs(peer *config.PeerConfig) ([]*x509.RevocationList, error) {
	if peer == nil || peer.FederationID == "" {
		return nil, nil
	}
	return federation.LoadCRLs(peer.ClientCRLFile, peer.RevocationMaxAge())
}

func copyPeerConfig(peer *config.PeerConfig) *config.PeerConfig {
	if peer == nil {
		return nil
	}
	peerCopy := *peer
	peerCopy.LocalBackends = append([]string(nil), peer.LocalBackends...)
	peerCopy.ManagementTargets = make(map[string]string, len(peer.ManagementTargets))
	for backend, target := range peer.ManagementTargets {
		peerCopy.ManagementTargets[backend] = target
	}
	peerCopy.Peers = make(map[string]config.PeerGrant, len(peer.Peers))
	for id, grant := range peer.Peers {
		peerCopy.Peers[id] = grant
	}
	return &peerCopy
}

func configDigest(path string) ([sha256.Size]byte, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(contents), nil
}

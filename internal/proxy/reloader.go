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
	"github.com/JakeRoxs/llama-catalog-proxy/internal/federation"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modeldiagnostics"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/provenance"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/timing"
)

const defaultWatchInterval = 2 * time.Second

type Reloader struct {
	path               string
	interval           time.Duration
	logger             *slog.Logger
	onReload           func(config.Config)
	state              atomic.Pointer[reloaderState]
	peerListen         string
	peerNodeID         string
	peerFederationID   string
	peerMembershipDir  string
	peerAuditDir       string
	pairingStore       *federation.PairingStore
	auditStore         *auditlog.Store
	managementListen   string
	managementSlots    chan struct{}
	delegatedStores    *config.DelegatedStoreConfig
	benchmark          *benchmarkRuntime
	adminAuth          *adminAuth
	managementUIConfig *config.ManagementUIConfig
	ingestion          *ingestionRuntime
	instanceID         string
	peerSlots          chan struct{}
	peerInferenceSlots chan struct{}
	peerPathSlots      chan struct{}
	peerWake           chan struct{}
	stop               chan struct{}
	lifecycleMu        sync.Mutex
	started            bool
	stopped            bool
	wg                 sync.WaitGroup
	lastHash           [sha256.Size]byte
	// lastReloadError is the last reload failure that was logged. Identical
	// failures are suppressed so a broken config does not flood the logs;
	// retries continue on every watch tick.
	lastReloadError error
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
	r := &Reloader{
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
	initialized := false
	defer func() {
		if !initialized && r.pairingStore != nil {
			_ = r.pairingStore.Close()
		}
		if !initialized && r.auditStore != nil {
			_ = r.auditStore.Close()
		}
	}()
	r.benchmark, err = newBenchmarkRuntime(cfg, logger)
	if err != nil {
		return nil, config.Config{}, err
	}
	r.adminAuth, err = newAdminAuth(cfg.ManagementUI, logger, http.DefaultClient)
	if err != nil {
		return nil, config.Config{}, err
	}
	r.managementUIConfig = cfg.ManagementUI
	var certificate *tls.Certificate
	if cfg.Peer != nil {
		id := make([]byte, 16)
		if _, err := rand.Read(id); err != nil {
			return nil, config.Config{}, fmt.Errorf("generate proxy instance identity: %w", err)
		}
		r.instanceID = hex.EncodeToString(id)
		r.peerListen, r.peerNodeID = cfg.Peer.Listen, cfg.Peer.NodeID
		certificate, err = loadPeerCertificate(cfg.Peer)
		if err != nil {
			return nil, config.Config{}, err
		}
		if cfg.Peer.FederationID != "" {
			r.peerFederationID = cfg.Peer.FederationID
			r.peerMembershipDir = cfg.Peer.MembershipDirectory
			r.pairingStore, err = federation.OpenPairingStore(cfg.Peer.MembershipDirectory, cfg.Peer.FederationID, cfg.Peer.NodeID)
			if err != nil {
				return nil, config.Config{}, fmt.Errorf("open peer membership store: %w", err)
			}
		}
		if cfg.Peer.AuditDirectory != "" {
			r.peerAuditDir = cfg.Peer.AuditDirectory
			r.auditStore, err = auditlog.Open(cfg.Peer.AuditDirectory, cfg.Peer.NodeID)
			if err != nil {
				return nil, config.Config{}, fmt.Errorf("open peer audit store: %w", err)
			}
		}
	}
	var managementCertificate *tls.Certificate
	if cfg.Management != nil {
		r.managementListen = cfg.Management.Listen
		if cfg.Management.DelegatedStore != nil {
			stores := *cfg.Management.DelegatedStore
			r.delegatedStores = &stores
		}
		managementCertificate, err = loadManagementCertificate(cfg.Management)
		if err != nil {
			return nil, config.Config{}, err
		}
	}
	state, err := makeReloaderState(cfg, handler, 1, certificate, managementCertificate, r.peerListen != "", r.pairingStore, r.auditStore)
	if err != nil {
		if r.pairingStore != nil {
			_ = r.pairingStore.Close()
		}
		if r.auditStore != nil {
			_ = r.auditStore.Close()
		}
		return nil, config.Config{}, err
	}
	r.state.Store(state)
	digest, err := configDigest(path)
	if err != nil {
		state.peerCancel()
		closePeerClients(state.peerClients)
		if r.pairingStore != nil {
			_ = r.pairingStore.Close()
		}
		if r.auditStore != nil {
			_ = r.auditStore.Close()
		}
		return nil, config.Config{}, err
	}
	r.lastHash = digest
	r.ingestion, err = newIngestionRuntime(cfg)
	if err != nil {
		state.peerCancel()
		closePeerClients(state.peerClients)
		if r.pairingStore != nil {
			_ = r.pairingStore.Close()
		}
		if r.auditStore != nil {
			_ = r.auditStore.Close()
		}
		return nil, config.Config{}, err
	}
	initialized = true
	return r, cfg, nil
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
	r.wg.Add(2)
	go r.watch()
	go r.watchPeers()
}

func (r *Reloader) Stop() {
	r.lifecycleMu.Lock()
	if !r.stopped {
		r.stopped = true
		close(r.stop)
		r.state.Load().peerCancel()
	}
	r.lifecycleMu.Unlock()
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
}

func (r *Reloader) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if r.benchmark != nil && (request.URL.Path == "/benchmark" || strings.HasPrefix(request.URL.Path, "/benchmark/")) {
		r.serveBenchmark(writer, request)
		return
	}
	if request.URL.Path == "/admin" || strings.HasPrefix(request.URL.Path, "/admin/") {
		if r.adminAuth == nil {
			http.NotFound(writer, request)
			return
		}
		if request.URL.Path == "/admin/api/v1/overview" {
			r.serveAdminOverview(writer, request)
			return
		}
		if request.URL.Path == "/admin/api/v1/activity" {
			r.serveAdminActivity(writer, request)
			return
		}
		if request.URL.Path == adminAuditPath {
			r.serveAdminAuditEvents(writer, request)
			return
		}
		if request.URL.Path == adminManagementPath || strings.HasPrefix(request.URL.Path, adminManagementPath+"/") {
			r.serveAdminManagement(writer, request)
			return
		}
		if request.URL.Path == adminPairingPath || strings.HasPrefix(request.URL.Path, adminPairingPath+"/") {
			r.serveAdminPairing(writer, request)
			return
		}
		r.adminAuth.ServeHTTP(writer, request)
		return
	}
	if r.peerListen != "" && (request.URL.Path == "/peer" || strings.HasPrefix(request.URL.Path, "/peer/")) {
		http.NotFound(writer, request)
		return
	}
	if r.managementListen != "" && (request.URL.Path == "/management" || strings.HasPrefix(request.URL.Path, "/management/")) {
		http.NotFound(writer, request)
		return
	}
	r.state.Load().handler.ServeHTTP(writer, request)
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
	if (cfg.Benchmark == nil) != (r.benchmark == nil) ||
		cfg.Benchmark != nil && (*cfg.Benchmark != r.benchmark.config || cfg.DiagnosticsAPIKey != r.benchmark.diagnosticsKey) {
		return errors.New("changing benchmark configuration or its diagnostics credential requires a restart")
	}
	if !reflect.DeepEqual(cfg.ManagementUI, r.managementUIConfig) {
		return errors.New("changing management_ui authentication requires a restart")
	}
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
	if ingestionConfig != nil {
		if err := validateIngestionDirectories(cfg.Management, cfg.Benchmark); err != nil {
			return err
		}
	}
	var certificate *tls.Certificate
	if cfg.Peer != nil {
		if r.peerListen == "" || cfg.Peer.Listen != r.peerListen || cfg.Peer.NodeID != r.peerNodeID {
			return errors.New("enabling peer listener or changing its address or node ID requires a restart")
		}
		if cfg.Peer.FederationID != r.peerFederationID || cfg.Peer.MembershipDirectory != r.peerMembershipDir || cfg.Peer.AuditDirectory != r.peerAuditDir {
			return errors.New("changing peer federation identity or protected state directories requires a restart")
		}
		certificate, err = loadPeerCertificate(cfg.Peer)
		if err != nil {
			return err
		}
	}
	var managementCertificate *tls.Certificate
	if cfg.Management != nil {
		if r.managementListen == "" || cfg.Management.Listen != r.managementListen {
			return errors.New("enabling or changing the management listener requires a restart")
		}
		managementCertificate, err = loadManagementCertificate(cfg.Management)
		if err != nil {
			return err
		}
	}
	diagnosticsService, err := diagnostics.New(cfg.Backends)
	if err != nil {
		return err
	}
	modelDiagnosticsService := modeldiagnostics.New()
	provenanceService := provenance.New()
	// The legacy timing prototype argument is accepted for signature
	// compatibility but is ignored: per-request observers are created in
	// ServeHTTP only when request_observation is enabled.
	handler, err := New(cfg, r.logger, diagnosticsService, modelDiagnosticsService, provenanceService, nil)
	if err != nil {
		return err
	}
	r.lifecycleMu.Lock()
	if r.stopped {
		r.lifecycleMu.Unlock()
		return errors.New("configuration reloader stopped")
	}
	old := r.state.Load()
	if old.generation == ^uint64(0) {
		r.lifecycleMu.Unlock()
		return errors.New("configuration generation exhausted")
	}
	state, err := makeReloaderState(cfg, handler, old.generation+1, certificate, managementCertificate, r.peerListen != "", r.pairingStore, r.auditStore)
	if err != nil {
		r.lifecycleMu.Unlock()
		return err
	}
	r.state.Store(state)
	old.peerCancel()
	closePeerClients(old.peerClients)
	select {
	case r.peerWake <- struct{}{}:
	default:
	}
	r.lifecycleMu.Unlock()
	if r.onReload != nil {
		r.onReload(cfg)
	}
	r.logger.Info("configuration reloaded", "default_backend", cfg.DefaultBackend, "backends", len(cfg.Backends))
	return nil
}

func makeReloaderState(cfg config.Config, handler http.Handler, generation uint64, certificate, managementCertificate *tls.Certificate, reservePeer bool, pairingStore *federation.PairingStore, auditStore *auditlog.Store) (*reloaderState, error) {
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
	clients, cache, err := newPeerClientsWithMembership(cfg.Peer, pairingStore)
	if err != nil {
		return nil, err
	}
	if reservePeer {
		h, ok := handler.(*Handler)
		if !ok {
			return nil, errors.New("unexpected handler type for peer configuration")
		}
		h.peerCache = cache
	}
	ctx, cancel := context.WithCancel(context.Background())
	if reservePeer {
		h := handler.(*Handler)
		h.peerClients = clients
		h.peerContext = ctx
		h.peerInferenceSlots = make(chan struct{}, 8)
	}
	return &reloaderState{
		handler: handler, peer: copyPeerConfig(cfg.Peer), cacheTTL: cfg.ModelsCacheTTL,
		generation: generation, certificate: certificate, peerClientCAs: peerClientCAs, peerClientCRLs: peerClientCRLs,
		pairingStore: pairingStore, auditStore: auditStore, peerClients: clients,
		management: copyManagementConfig(cfg.Management), managementCertificate: managementCertificate,
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
	copy := *management
	if management.DelegatedStore != nil {
		stores := *management.DelegatedStore
		copy.DelegatedStore = &stores
	}
	if management.ProposalStore != nil {
		store := *management.ProposalStore
		copy.ProposalStore = &store
	}
	if management.BackupStore != nil {
		store := *management.BackupStore
		copy.BackupStore = &store
	}
	if management.Ingestion != nil {
		ingestion := *management.Ingestion
		copy.Ingestion = &ingestion
	}
	copy.Targets = make(map[string]config.ManagementTarget, len(management.Targets))
	for id, target := range management.Targets {
		copy.Targets[id] = target
	}
	return &copy
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
	copy := *peer
	copy.LocalBackends = append([]string(nil), peer.LocalBackends...)
	copy.ManagementTargets = make(map[string]string, len(peer.ManagementTargets))
	for backend, target := range peer.ManagementTargets {
		copy.ManagementTargets[backend] = target
	}
	copy.Peers = make(map[string]config.PeerGrant, len(peer.Peers))
	for id, grant := range peer.Peers {
		copy.Peers[id] = grant
	}
	return &copy
}

func configDigest(path string) ([sha256.Size]byte, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(contents), nil
}

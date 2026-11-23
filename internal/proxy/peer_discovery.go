package proxy

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/discovery"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/federation"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/identity"
)

type discoveredPeerClients struct {
	clients        map[string]*peerClient
	authorizedIDs  map[string]struct{}
	unavailable    map[string]string
	routeInference map[string]bool
}

func (r *Reloader) buildDiscoveredPeerClients(state *reloaderState) (discoveredPeerClients, error) {
	return r.buildDiscoveredPeerClientsWithBrowse(state, discovery.Browse)
}

func (r *Reloader) buildDiscoveredPeerClientsWithBrowse(state *reloaderState,
	browse func(context.Context, discovery.Settings, time.Duration) (discovery.CandidateResult, error)) (discoveredPeerClients, error) {
	result := discoveredPeerClients{
		clients: make(map[string]*peerClient), authorizedIDs: make(map[string]struct{}),
		unavailable: make(map[string]string), routeInference: make(map[string]bool),
	}
	if state == nil || state.peer == nil || state.peer.Discovery == nil || !state.peer.Discovery.Enabled ||
		state.pairingStore == nil || state.catalogGrantStore == nil {
		return result, nil
	}
	grants, err := state.catalogGrantStore.ListCatalogReadGrants()
	if err != nil {
		return result, err
	}
	inferenceGrants, err := state.catalogGrantStore.ListInferenceGrants()
	if err != nil {
		return result, err
	}
	routeInference := make(map[string]string)
	for _, grant := range inferenceGrants {
		if !grant.RouteOutgoing {
			continue
		}
		member, paired, err := state.pairingStore.LookupMember(grant.NodeID)
		if err != nil {
			return discoveredPeerClients{clients: make(map[string]*peerClient), authorizedIDs: make(map[string]struct{}), unavailable: make(map[string]string), routeInference: make(map[string]bool)}, err
		}
		if paired && member.Fingerprint == grant.Fingerprint {
			routeInference[grant.NodeID] = grant.Fingerprint
		}
	}
	pairedGrants := make([]federation.CatalogReadGrant, 0, len(grants))
	for _, grant := range grants {
		member, paired, err := state.pairingStore.LookupMember(grant.NodeID)
		if err != nil {
			return discoveredPeerClients{clients: make(map[string]*peerClient), authorizedIDs: make(map[string]struct{}), unavailable: make(map[string]string), routeInference: make(map[string]bool)}, err
		}
		if !paired || member.Fingerprint != grant.Fingerprint {
			continue
		}
		result.authorizedIDs[grant.NodeID] = struct{}{}
		result.routeInference[grant.NodeID] = routeInference[grant.NodeID] == grant.Fingerprint
		pairedGrants = append(pairedGrants, grant)
	}
	if len(pairedGrants) == 0 {
		return result, nil
	}
	timeout := min(peerRefreshIntervalForTTL(state.cacheTTL), 2*time.Second)
	ctx, cancel := context.WithTimeout(state.peerContext, timeout+500*time.Millisecond)
	defer cancel()
	candidates, err := browse(ctx, discovery.Settings{
		NodeID: state.peer.NodeID, FederationID: state.peer.FederationID,
		Interface: state.peer.Discovery.Interface, AdvertiseURL: state.peer.Discovery.AdvertiseURL,
	}, timeout)
	if err != nil {
		for _, grant := range pairedGrants {
			result.unavailable[grant.NodeID] = "discovery_unavailable"
		}
		return result, err
	}
	if candidates.Truncated {
		for _, grant := range pairedGrants {
			result.unavailable[grant.NodeID] = "candidate_list_truncated"
		}
		return result, errors.New("mDNS candidate list was truncated")
	}
	endpoints := make(map[string]string)
	ambiguous := make(map[string]bool)
	for _, candidate := range candidates.Candidates {
		if candidate.FederationID != state.peer.FederationID {
			continue
		}
		if endpoint, exists := endpoints[candidate.NodeID]; exists && endpoint != candidate.PeerURL {
			ambiguous[candidate.NodeID] = true
			continue
		}
		endpoints[candidate.NodeID] = candidate.PeerURL
	}
	for _, grant := range pairedGrants {
		if _, configured := state.peerClients[grant.NodeID]; configured {
			continue
		}
		if ambiguous[grant.NodeID] {
			result.unavailable[grant.NodeID] = "ambiguous_endpoint"
			continue
		}
		endpoint := endpoints[grant.NodeID]
		if endpoint == "" {
			result.unavailable[grant.NodeID] = "not_discovered"
			continue
		}
		client, err := newDiscoveredPeerClient(state, grant, endpoint,
			routeInference[grant.NodeID] == grant.Fingerprint)
		if err != nil {
			result.unavailable[grant.NodeID] = "client_unavailable"
			continue
		}
		result.clients[grant.NodeID] = client
	}
	return result, nil
}

func newDiscoveredPeerClient(state *reloaderState, grant federation.CatalogReadGrant, endpoint string, routeInference bool) (*peerClient, error) {
	if !identity.ValidNodeID(grant.NodeID) || state.peerClientCAs == nil || state.certificate == nil || len(state.certificate.Certificate) == 0 {
		return nil, errors.New("discovered peer client identity unavailable")
	}
	member, ok, err := state.pairingStore.LookupMember(grant.NodeID)
	if err != nil {
		return nil, err
	}
	if !ok || member.Fingerprint != grant.Fingerprint {
		return nil, federation.ErrMemberNotFound
	}
	reportTransport := newDiscoveredPeerTransport(state, grant.NodeID, false)
	inferenceTransport := newDiscoveredPeerTransport(state, grant.NodeID, true)
	return &peerClient{
		id: grant.NodeID, endpoint: strings.TrimSuffix(endpoint, "/") + "/peer/v1/report",
		origin: strings.TrimSuffix(endpoint, "/"), client: &http.Client{
			Transport: reportTransport, Timeout: peerReportTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		serverCRLs: state.peerClientCRLs, crlMaxAge: state.peer.RevocationMaxAge(),
		inferenceTransport: inferenceTransport, dynamic: true, catalogRead: true,
		routeInference: routeInference,
	}, nil
}

func verifyDiscoveredPeerConnection(state *reloaderState, nodeID string, inference bool, connection tls.ConnectionState) error {
	if len(connection.VerifiedChains) == 0 || len(connection.PeerCertificates) == 0 {
		return errors.New("peer server identity unavailable")
	}
	if federation.VerifyCertificateRevocation(connection.VerifiedChains, state.peerClientCRLs,
		state.peer.RevocationMaxAge(), time.Now()) != nil {
		return errors.New("peer server certificate is revoked or CRL is stale")
	}
	serverCertificate := connection.PeerCertificates[0]
	serverNodeID, err := federation.PeerIdentityNodeID(serverCertificate, state.peer.FederationID)
	if err != nil || serverNodeID != nodeID {
		return errors.New("peer server identity mismatch")
	}
	member, paired, err := state.pairingStore.LookupMember(serverNodeID)
	if err != nil || !paired || member.Fingerprint != federation.CertificateFingerprint(serverCertificate) {
		return errors.New("peer is not paired with this certificate")
	}
	var allowed bool
	if inference {
		allowed, err = state.catalogGrantStore.AllowsOutgoingInference(serverNodeID, member.Fingerprint)
	} else {
		allowed, err = state.catalogGrantStore.AllowsCatalogRead(serverNodeID, member.Fingerprint)
	}
	if err != nil || !allowed {
		if inference {
			return errors.New("peer has no active route_inference grant")
		}
		return errors.New("peer has no active catalog_read grant")
	}
	return nil
}

func newDiscoveredPeerTransport(state *reloaderState, nodeID string, inference bool) *http.Transport {
	return &http.Transport{
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12, RootCAs: state.peerClientCAs,
			Certificates: []tls.Certificate{*state.certificate},
			VerifyConnection: func(connection tls.ConnectionState) error {
				return verifyDiscoveredPeerConnection(state, nodeID, inference, connection)
			},
		},
		DisableCompression: true, DisableKeepAlives: true,
		MaxConnsPerHost: 1, MaxIdleConnsPerHost: 1,
		TLSHandshakeTimeout: peerReportTimeout,
	}
}

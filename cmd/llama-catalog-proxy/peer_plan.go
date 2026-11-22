package main

import (
	"encoding/json"
	"errors"
	"io"
	"sort"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

type peerConfigPlan struct {
	FederationID string            `json:"federation_id,omitempty"`
	LocalNodeID  string            `json:"local_node_id"`
	Peers        []peerHandoffPlan `json:"peers"`
}

type peerHandoffPlan struct {
	NodeID                    string   `json:"node_id"`
	Direction                 string   `json:"direction"`
	InboundScopes             []string `json:"inbound_scopes"`
	OutboundScopes            []string `json:"outbound_scopes"`
	OutboundURLConfigured     bool     `json:"outbound_url_configured"`
	InboundTokenConfigured    bool     `json:"inbound_token_configured"`
	OutboundTokenConfigured   bool     `json:"outbound_token_configured"`
	OutboundCATrustConfigured bool     `json:"outbound_ca_trust_configured"`
	OutboundCRLConfigured     bool     `json:"outbound_crl_configured"`
	Handoff                   []string `json:"handoff"`
}

func runPeerConfigPlan(args []string, stdout, stderr io.Writer) error {
	flags := peerFlags("peer config plan", stderr)
	configPath := flags.String("config", defaultPeerConfigPath, "path to YAML configuration")
	if err := parsePeerFlags(flags, args); err != nil {
		return err
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	if cfg.Peer == nil {
		return errors.New("peer federation is not configured")
	}
	plan := peerConfigPlan{LocalNodeID: cfg.Peer.NodeID, FederationID: cfg.Peer.FederationID, Peers: []peerHandoffPlan{}}
	ids := make([]string, 0, len(cfg.Peer.Peers))
	for nodeID := range cfg.Peer.Peers {
		ids = append(ids, nodeID)
	}
	sort.Strings(ids)
	for _, nodeID := range ids {
		grant := cfg.Peer.Peers[nodeID]
		plan.Peers = append(plan.Peers, peerHandoffPlan{
			NodeID:                    nodeID,
			OutboundURLConfigured:     grant.URL != "",
			InboundTokenConfigured:    grant.Token != "",
			OutboundTokenConfigured:   grant.OutboundToken != "",
			OutboundCATrustConfigured: grant.CAFile != "",
			OutboundCRLConfigured:     grant.CRLFile != "",
			InboundScopes:             peerInboundScopes(grant),
			OutboundScopes:            peerOutboundScopes(grant),
			Direction:                 peerDirection(grant),
			Handoff:                   peerHandoff(grant, cfg.Peer.NodeID),
		})
	}
	return json.NewEncoder(stdout).Encode(plan)
}

// peerInboundScopes returns the inbound scope list for a peer grant in the
// canonical order: configured capability scopes first, then path_access when
// any path access entries are present.
func peerInboundScopes(grant config.PeerGrant) []string {
	scopes := make([]string, 0, 5)
	scopes = appendPeerScope(scopes, grant.CatalogRead, "catalog_read")
	scopes = appendPeerScope(scopes, grant.DiagnosticsRead, "diagnostics_read")
	scopes = appendPeerScope(scopes, grant.Inference, "inference")
	scopes = appendPeerScope(scopes, grant.ManageSettings, "manage_settings")
	if len(grant.PathAccess) > 0 {
		scopes = append(scopes, "path_access")
	}
	return scopes
}

// peerOutboundScopes returns the outbound scope list for a peer grant in the
// canonical order: configured route scopes first, then route_paths when any
// route paths are present.
func peerOutboundScopes(grant config.PeerGrant) []string {
	scopes := make([]string, 0, 4)
	scopes = appendPeerScope(scopes, grant.RouteInference, "route_inference")
	scopes = appendPeerScope(scopes, grant.RouteManagement, "route_management")
	if len(grant.RoutePaths) > 0 {
		scopes = append(scopes, "route_paths")
	}
	return scopes
}

// peerDirection returns the peer coupling direction for a grant:
// bidirectional when both a URL and a token are configured, outbound when only
// a URL is configured, inbound when only a token is configured, and
// unconfigured otherwise.
func peerDirection(grant config.PeerGrant) string {
	switch {
	case grant.URL != "" && grant.Token != "":
		return "bidirectional"
	case grant.URL != "":
		return "outbound"
	case grant.Token != "":
		return "inbound"
	default:
		return "unconfigured"
	}
}

// peerHandoff returns the conditional handoff messages for a peer grant, in the
// canonical order: inbound-token match, outbound-token match, bidirectional
// token confirmation (when both directions are configured), and the trust
// verification reminder.
func peerHandoff(grant config.PeerGrant, localNodeID string) []string {
	handoff := make([]string, 0, 3)
	if grant.Token != "" {
		handoff = append(handoff,
			"remote peers."+localNodeID+".outbound_token must match this node's inbound token; provide this node's HTTPS peer URL out of band")
	}
	if grant.URL != "" {
		handoff = append(handoff,
			"remote peers."+localNodeID+".token must match this node's outbound token")
	}
	if grant.URL != "" && grant.Token != "" {
		handoff = append(handoff,
			"bidirectional operation requires two distinct tokens, one per direction; confirm the remote grant independently")
	}
	handoff = append(handoff,
		"pairing membership does not grant these scopes; verify trust, firewall reachability, and remote authorization separately")
	return handoff
}

func appendPeerScope(scopes []string, enabled bool, name string) []string {
	if enabled {
		return append(scopes, name)
	}
	return scopes
}

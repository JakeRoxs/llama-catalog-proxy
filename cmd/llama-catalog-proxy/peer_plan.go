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
		peerPlan := peerHandoffPlan{
			NodeID:                    nodeID,
			OutboundURLConfigured:     grant.URL != "",
			InboundTokenConfigured:    grant.Token != "",
			OutboundTokenConfigured:   grant.OutboundToken != "",
			OutboundCATrustConfigured: grant.CAFile != "",
			OutboundCRLConfigured:     grant.CRLFile != "",
			InboundScopes:             make([]string, 0, 5),
			OutboundScopes:            make([]string, 0, 4),
			Handoff:                   make([]string, 0, 3),
		}
		peerPlan.InboundScopes = appendPeerScope(peerPlan.InboundScopes, grant.CatalogRead, "catalog_read")
		peerPlan.InboundScopes = appendPeerScope(peerPlan.InboundScopes, grant.DiagnosticsRead, "diagnostics_read")
		peerPlan.InboundScopes = appendPeerScope(peerPlan.InboundScopes, grant.Inference, "inference")
		peerPlan.InboundScopes = appendPeerScope(peerPlan.InboundScopes, grant.ManageSettings, "manage_settings")
		if len(grant.PathAccess) > 0 {
			peerPlan.InboundScopes = append(peerPlan.InboundScopes, "path_access")
		}
		peerPlan.OutboundScopes = appendPeerScope(peerPlan.OutboundScopes, grant.RouteInference, "route_inference")
		peerPlan.OutboundScopes = appendPeerScope(peerPlan.OutboundScopes, grant.RouteManagement, "route_management")
		if len(grant.RoutePaths) > 0 {
			peerPlan.OutboundScopes = append(peerPlan.OutboundScopes, "route_paths")
		}
		switch {
		case grant.URL != "" && grant.Token != "":
			peerPlan.Direction = "bidirectional"
		case grant.URL != "":
			peerPlan.Direction = "outbound"
		case grant.Token != "":
			peerPlan.Direction = "inbound"
		default:
			peerPlan.Direction = "unconfigured"
		}
		if grant.Token != "" {
			peerPlan.Handoff = append(peerPlan.Handoff,
				"remote peers."+cfg.Peer.NodeID+".outbound_token must match this node's inbound token; provide this node's HTTPS peer URL out of band")
		}
		if grant.URL != "" {
			peerPlan.Handoff = append(peerPlan.Handoff,
				"remote peers."+cfg.Peer.NodeID+".token must match this node's outbound token")
		}
		if grant.URL != "" && grant.Token != "" {
			peerPlan.Handoff = append(peerPlan.Handoff,
				"bidirectional operation requires two distinct tokens, one per direction; confirm the remote grant independently")
		}
		peerPlan.Handoff = append(peerPlan.Handoff,
			"pairing membership does not grant these scopes; verify trust, firewall reachability, and remote authorization separately")
		plan.Peers = append(plan.Peers, peerPlan)
	}
	return json.NewEncoder(stdout).Encode(plan)
}

func appendPeerScope(scopes []string, enabled bool, name string) []string {
	if enabled {
		return append(scopes, name)
	}
	return scopes
}

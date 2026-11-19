package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/discovery"
)

func runPeerDiscover(args []string, stdout, stderr io.Writer) error {
	flags := peerFlags("peer discover", stderr)
	configPath := flags.String("config", defaultPeerConfigPath, "path to YAML configuration")
	timeout := flags.Duration("timeout", 3*time.Second, "mDNS query duration (250ms to 30s)")
	if err := parsePeerFlags(flags, args); err != nil {
		return err
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	if cfg.Peer == nil || cfg.Peer.Discovery == nil || !cfg.Peer.Discovery.Enabled {
		return errors.New("peer discovery is not enabled in configuration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout+time.Second)
	defer cancel()
	candidates, err := discovery.Browse(ctx, discovery.Settings{
		NodeID: cfg.Peer.NodeID, FederationID: cfg.Peer.FederationID,
		Interface: cfg.Peer.Discovery.Interface, AdvertiseURL: cfg.Peer.Discovery.AdvertiseURL,
	}, *timeout)
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(candidates)
}

func selectPeerCandidate(result discovery.CandidateResult, nodeID string) (string, error) {
	if result.Truncated {
		return "", errors.New("mDNS candidate list was truncated; refusing to pair from an incomplete list")
	}
	var endpoint string
	for _, candidate := range result.Candidates {
		if candidate.NodeID != nodeID {
			continue
		}
		if endpoint != "" && endpoint != candidate.PeerURL {
			return "", errors.New("multiple mDNS endpoints advertise the requested node ID")
		}
		endpoint = candidate.PeerURL
	}
	if endpoint == "" {
		return "", errors.New("requested node ID was not found in mDNS candidates")
	}
	return endpoint, nil
}

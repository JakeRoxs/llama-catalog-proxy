package main

import (
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/federation"
)

const configPathHelpText = "path to YAML configuration"

type peerCatalogGrantView struct {
	federation.CatalogReadGrant
	CurrentFingerprint bool `json:"current_fingerprint"`
}

func runPeerCatalogGrantCreate(args []string, stdout, stderr io.Writer) error {
	flags := peerFlags("peer catalog-grants create", stderr)
	configPath := flags.String("config", defaultPeerConfigPath, configPathHelpText)
	nodeID := flags.String("node-id", "", "paired node identity to grant catalog read")
	acknowledge := flags.Bool("acknowledge", false, "confirm this catalog authorization change")
	if err := parsePeerFlags(flags, args); err != nil {
		return err
	}
	if *nodeID == "" || !*acknowledge {
		return errors.New("--node-id and --acknowledge are required")
	}
	return withPeerGrantStores(*configPath, "catalog_read_grant.create", *nodeID, stdout, func(members *federation.PairingStore, grants *federation.CatalogGrantStore, out io.Writer) (string, string, string, error) {
		member, err := lookupPairedMember(members, *nodeID, errors.New("catalog read can only be granted to a currently paired identity"))
		if err != nil {
			return *nodeID, "", "", err
		}
		grant, err := grants.GrantCatalogRead(member, time.Now().UTC())
		if err != nil {
			return *nodeID, "", "", err
		}
		return *nodeID, "", "", json.NewEncoder(out).Encode(grant)
	})
}

func runPeerCatalogGrantList(args []string, stdout, stderr io.Writer) error {
	flags := peerFlags("peer catalog-grants list", stderr)
	configPath := flags.String("config", defaultPeerConfigPath, configPathHelpText)
	if err := parsePeerFlags(flags, args); err != nil {
		return err
	}
	return withPeerGrantStores(*configPath, "catalog_read_grant.list", "", stdout, func(members *federation.PairingStore, grants *federation.CatalogGrantStore, out io.Writer) (string, string, string, error) {
		stored, err := grants.ListCatalogReadGrants()
		if err != nil {
			return "", "", "", err
		}
		result := make([]peerCatalogGrantView, 0, len(stored))
		for _, grant := range stored {
			member, paired, lookupErr := members.LookupMember(grant.NodeID)
			if lookupErr != nil {
				return "", "", "", lookupErr
			}
			result = append(result, peerCatalogGrantView{
				CatalogReadGrant:   grant,
				CurrentFingerprint: paired && member.Fingerprint == grant.Fingerprint,
			})
		}
		return "", "", "", json.NewEncoder(out).Encode(result)
	})
}

func runPeerCatalogGrantRevoke(args []string, stdout, stderr io.Writer) error {
	flags := peerFlags("peer catalog-grants revoke", stderr)
	configPath := flags.String("config", defaultPeerConfigPath, configPathHelpText)
	nodeID := flags.String("node-id", "", "paired node identity whose catalog grant will be revoked")
	acknowledge := flags.Bool("acknowledge", false, "confirm this catalog authorization change")
	if err := parsePeerFlags(flags, args); err != nil {
		return err
	}
	if *nodeID == "" || !*acknowledge {
		return errors.New("--node-id and --acknowledge are required")
	}
	return withPeerGrantStores(*configPath, "catalog_read_grant.revoke", *nodeID, stdout, func(members *federation.PairingStore, grants *federation.CatalogGrantStore, out io.Writer) (string, string, string, error) {
		if err := grants.RevokeCatalogRead(*nodeID); err != nil {
			return *nodeID, "", "", err
		}
		_, err := io.WriteString(out, "catalog_read grant revoked; outbound dynamic inference routing disabled\n")
		return *nodeID, "", "", err
	})
}

func openConfiguredCatalogGrantStores(configPath string) (*federation.PairingStore, *federation.CatalogGrantStore, error) {
	cfg, err := config.Load(configPath)
	if err != nil {
		return nil, nil, err
	}
	if cfg.Peer == nil || cfg.Peer.Discovery == nil || !cfg.Peer.Discovery.Enabled || cfg.Peer.FederationID == "" {
		return nil, nil, errors.New("peer mTLS discovery and catalog_grant_directory must be configured")
	}
	members, err := federation.OpenPairingStore(cfg.Peer.MembershipDirectory, cfg.Peer.FederationID, cfg.Peer.NodeID)
	if err != nil {
		return nil, nil, err
	}
	grants, err := federation.OpenCatalogGrantStore(cfg.Peer.CatalogGrantDirectory, cfg.Peer.FederationID, cfg.Peer.NodeID)
	if err != nil {
		_ = members.Close()
		return nil, nil, err
	}
	return members, grants, nil
}

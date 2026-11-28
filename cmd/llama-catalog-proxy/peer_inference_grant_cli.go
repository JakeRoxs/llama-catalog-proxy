package main

import (
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/federation"
)

type peerInferenceGrantView struct {
	federation.InferenceGrant
	CurrentFingerprint bool `json:"current_fingerprint"`
}

func runPeerInferenceGrantCreate(args []string, stdout, stderr io.Writer) error {
	flags := peerFlags("peer inference-grants create", stderr)
	configPath := flags.String("config", defaultPeerConfigPath, peerConfigHelpText)
	nodeID := flags.String("node-id", "", "paired node identity for inference authorization")
	allowIncoming := flags.Bool("allow-incoming", false, "allow this peer to invoke local inference")
	routeOutgoing := flags.Bool("route-outgoing", false, "allow this node to route inference to the peer")
	acknowledge := flags.Bool("acknowledge", false, "confirm this inference authorization change")
	if err := parsePeerFlags(flags, args); err != nil {
		return err
	}
	if *nodeID == "" || (!*allowIncoming && !*routeOutgoing) || !*acknowledge {
		return errors.New("--node-id, at least one inference direction, and --acknowledge are required")
	}
	return withPeerGrantStores(*configPath, "inference_grant.create", *nodeID, stdout, func(members *federation.PairingStore, grants *federation.CatalogGrantStore, out io.Writer) (string, string, string, error) {
		member, err := lookupPairedMember(members, *nodeID, errors.New("inference access can only be granted to a currently paired identity"))
		if err != nil {
			return *nodeID, "", "", err
		}
		grant, err := grants.GrantInference(member, *allowIncoming, *routeOutgoing, time.Now().UTC())
		if err != nil {
			return *nodeID, "", "", err
		}
		return *nodeID, "", "", json.NewEncoder(out).Encode(grant)
	})
}

func runPeerInferenceGrantList(args []string, stdout, stderr io.Writer) error {
	flags := peerFlags("peer inference-grants list", stderr)
	configPath := flags.String("config", defaultPeerConfigPath, peerConfigHelpText)
	if err := parsePeerFlags(flags, args); err != nil {
		return err
	}
	return withPeerGrantStores(*configPath, "inference_grant.list", "", stdout, func(members *federation.PairingStore, grants *federation.CatalogGrantStore, out io.Writer) (string, string, string, error) {
		stored, err := grants.ListInferenceGrants()
		if err != nil {
			return "", "", "", err
		}
		result := make([]peerInferenceGrantView, 0, len(stored))
		for _, grant := range stored {
			member, paired, err := members.LookupMember(grant.NodeID)
			if err != nil {
				return "", "", "", err
			}
			result = append(result, peerInferenceGrantView{
				InferenceGrant: grant, CurrentFingerprint: paired && member.Fingerprint == grant.Fingerprint,
			})
		}
		return "", "", "", json.NewEncoder(out).Encode(result)
	})
}

func runPeerInferenceGrantRevoke(args []string, stdout, stderr io.Writer) error {
	flags := peerFlags("peer inference-grants revoke", stderr)
	configPath := flags.String("config", defaultPeerConfigPath, peerConfigHelpText)
	nodeID := flags.String("node-id", "", "paired node identity whose inference grants will be revoked")
	acknowledge := flags.Bool("acknowledge", false, "confirm this inference authorization change")
	if err := parsePeerFlags(flags, args); err != nil {
		return err
	}
	if *nodeID == "" || !*acknowledge {
		return errors.New("--node-id and --acknowledge are required")
	}
	return withPeerGrantStores(*configPath, "inference_grant.revoke", *nodeID, stdout, func(members *federation.PairingStore, grants *federation.CatalogGrantStore, out io.Writer) (string, string, string, error) {
		if err := grants.RevokeInference(*nodeID); err != nil {
			return *nodeID, "", "", err
		}
		_, err := io.WriteString(out, "inference grants revoked\n")
		return *nodeID, "", "", err
	})
}

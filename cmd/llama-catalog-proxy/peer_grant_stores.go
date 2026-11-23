package main

import (
	"io"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/federation"
)

// withPeerGrantStores wraps withPeerCLIAudit, opens the member and catalog-grant
// stores once, defers members.Close() then grants.Close() (defer execution order
// preserved), and passes them to the operation callback. On store open failure it
// returns subjectID as the audit target (empty for list, node ID for
// create/revoke), matching the prior per-command behavior.
func withPeerGrantStores(configPath, action, subjectID string, stdout io.Writer,
	operation func(members *federation.PairingStore, grants *federation.CatalogGrantStore, out io.Writer) (string, string, string, error)) error {
	return withPeerCLIAudit(configPath, action, subjectID, "", "", stdout, func(out io.Writer) (string, string, string, error) {
		members, grants, err := openConfiguredCatalogGrantStores(configPath)
		if err != nil {
			return subjectID, "", "", err
		}
		defer members.Close()
		defer grants.Close()
		return operation(members, grants, out)
	})
}

// lookupPairedMember looks up a member and returns it only when it is currently
// paired. The caller supplies the command-specific not-paired error so catalog
// and inference messages remain separate.
func lookupPairedMember(members *federation.PairingStore, nodeID string, notPaired error) (federation.Membership, error) {
	member, paired, err := members.LookupMember(nodeID)
	if err != nil {
		return federation.Membership{}, err
	}
	if !paired {
		return federation.Membership{}, notPaired
	}
	return member, nil
}

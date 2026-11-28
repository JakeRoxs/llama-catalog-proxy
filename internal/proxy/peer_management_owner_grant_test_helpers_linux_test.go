//go:build linux

package proxy

import (
	"crypto/x509"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/federation"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
)

// createPeerOwnerGrantFixture pairs node-b with clientLeaf in the membership
// store and creates a delegated owner grant for it, returning the grant so
// callers can revoke or assert against it later. The caller retains ownership
// of members.Close() and the grant directory lifecycle.
func createPeerOwnerGrantFixture(t *testing.T, members *federation.PairingStore, grantDirectory string, clientLeaf *x509.Certificate) modelconfig.OwnerGrant {
	t.Helper()
	if _, err := members.AddMember("node-b", peerCertificateFingerprint(clientLeaf), time.Now()); err != nil {
		t.Fatal(err)
	}
	grant := modelconfig.OwnerGrant{
		ID:              "0123456789abcdef0123456789abcdef",
		RequesterNodeID: "node-b",
		Action:          "manage_settings",
		BackendID:       "backend-a",
		TargetID:        "target-a",
		ModelIDs:        []string{"model-a"},
		CreatedAt:       time.Now(),
		ExpiresAt:       time.Now().Add(time.Hour),
		ApprovedBy:      "oidc_admin",
	}
	if err := modelconfig.CreateOwnerGrant(grantDirectory, grant); err != nil {
		t.Fatal(err)
	}
	return grant
}

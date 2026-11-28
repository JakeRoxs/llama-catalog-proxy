package federation

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCatalogReadGrantPersistsAndBindsToCertificateFingerprint(t *testing.T) {
	directory := t.TempDir()
	store := setupCatalogGrantStore(t, directory)
	now := time.Now().UTC()
	firstFingerprint := strings.Repeat("a", 64)
	member := Membership{NodeID: "node-b", Fingerprint: firstFingerprint, PairedAt: now}
	grant := grantCatalogRead(t, store, member, now)
	assertCatalogGrant(t, grant, member, now)
	assertAllowsCatalogRead(t, store, "node-b", firstFingerprint, true)
	assertAllowsCatalogRead(t, store, "node-b", strings.Repeat("b", 64), false)
	closeCatalogGrantStore(t, store)
	assertCatalogGrantFileMode(t, directory)

	store = reopenCatalogGrantStore(t, directory)
	defer store.Close()
	assertCatalogReadGrants(t, store, grant)
	rotatedFingerprint := strings.Repeat("c", 64)
	grantCatalogRead(t, store, Membership{NodeID: "node-b", Fingerprint: rotatedFingerprint, PairedAt: now.Add(time.Hour)}, now.Add(time.Hour))
	assertAllowsCatalogRead(t, store, "node-b", firstFingerprint, false)
	assertAllowsCatalogRead(t, store, "node-b", rotatedFingerprint, true)
	revokeCatalogRead(t, store, "node-b")
	assertAllowsCatalogRead(t, store, "node-b", rotatedFingerprint, false)
	assertRevokeMissingCatalogGrant(t, store, "node-b")
}

func TestCatalogGrantStoreRejectsUnsafeDirectoryAndMalformedData(t *testing.T) {
	insecure := t.TempDir()
	if err := os.Chmod(insecure, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenCatalogGrantStore(insecure, "lab.example", "node-a"); err == nil {
		t.Fatal("group/world accessible catalog grant directory accepted")
	}

	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, catalogGrantFilename), []byte(`{"version":1,"unexpected":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenCatalogGrantStore(directory, "lab.example", "node-a"); err == nil {
		t.Fatal("catalog grant store with unknown field accepted")
	}
}

func TestCatalogGrantStoreRejectsInvalidAndDuplicateCertificateGrants(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := OpenCatalogGrantStore(directory, "lab.example", "node-a")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Now().UTC()
	if _, err := store.GrantCatalogRead(Membership{NodeID: "node-a", Fingerprint: strings.Repeat("a", 64), PairedAt: now}, now); err == nil {
		t.Fatal("local node catalog grant accepted")
	}
	if _, err := store.GrantCatalogRead(Membership{NodeID: "node-b", Fingerprint: "not-a-fingerprint", PairedAt: now}, now); err == nil {
		t.Fatal("invalid certificate fingerprint accepted")
	}
	if _, err := store.GrantCatalogRead(Membership{NodeID: "node-b", Fingerprint: strings.Repeat("a", 64), PairedAt: now}, now); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GrantCatalogRead(Membership{NodeID: "node-c", Fingerprint: strings.Repeat("a", 64), PairedAt: now}, now); err == nil {
		t.Fatal("same certificate fingerprint granted to multiple nodes")
	}
}

func TestInferenceGrantDirectionsRemainSeparateFromCatalogRead(t *testing.T) {
	directory := t.TempDir()
	store := setupCatalogGrantStore(t, directory)
	defer store.Close()
	now := time.Now().UTC()
	fingerprint := strings.Repeat("d", 64)
	member := Membership{NodeID: "node-b", Fingerprint: fingerprint, PairedAt: now}
	assertInferenceNeedsCatalog(t, store, member, now)
	grantCatalogRead(t, store, member, now)
	grant := grantInference(t, store, member, true, false, now)
	assertInferenceGrant(t, grant, true, false)
	assertAllowsCatalogRead(t, store, "node-b", fingerprint, true)
	assertAllowsIncomingInference(t, store, "node-b", fingerprint, true)
	assertAllowsOutgoingInference(t, store, "node-b", fingerprint, false)
	grantInference(t, store, member, true, true, now.Add(time.Minute))
	assertAllowsOutgoingInference(t, store, "node-b", fingerprint, true)
	revokeCatalogRead(t, store, "node-b")
	assertAllowsCatalogRead(t, store, "node-b", fingerprint, false)
	assertAllowsOutgoingInference(t, store, "node-b", fingerprint, false)
	assertAllowsIncomingInference(t, store, "node-b", fingerprint, true)
	revokeInference(t, store, "node-b")
	assertAllowsIncomingInference(t, store, "node-b", fingerprint, false)
	assertAllowsOutgoingInference(t, store, "node-b", fingerprint, false)
}

// setupCatalogGrantStore creates a 0o700-owned temp directory and opens a catalog grant store.
func setupCatalogGrantStore(t *testing.T, directory string) *CatalogGrantStore {
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := OpenCatalogGrantStore(directory, "lab.example", "node-a")
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func closeCatalogGrantStore(t *testing.T, store *CatalogGrantStore) {
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func reopenCatalogGrantStore(t *testing.T, directory string) *CatalogGrantStore {
	store, err := OpenCatalogGrantStore(directory, "lab.example", "node-a")
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func grantCatalogRead(t *testing.T, store *CatalogGrantStore, member Membership, grantedAt time.Time) CatalogReadGrant {
	grant, err := store.GrantCatalogRead(member, grantedAt)
	if err != nil {
		t.Fatal(err)
	}
	return grant
}

func assertCatalogGrant(t *testing.T, grant CatalogReadGrant, member Membership, grantedAt time.Time) {
	if grant.NodeID != member.NodeID || grant.Fingerprint != member.Fingerprint || !grant.GrantedAt.Equal(grantedAt) {
		t.Fatalf("grant = %+v", grant)
	}
}

func assertAllowsCatalogRead(t *testing.T, store *CatalogGrantStore, nodeID, fingerprint string, allowed bool) {
	ok, err := store.AllowsCatalogRead(nodeID, fingerprint)
	if err != nil || ok != allowed {
		t.Fatalf("AllowsCatalogRead(%q, %q) = %v, %v", nodeID, fingerprint, ok, err)
	}
}

func assertCatalogGrantFileMode(t *testing.T, directory string) {
	info, err := os.Stat(filepath.Join(directory, catalogGrantFilename))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("catalog grant file mode = %v, %v", info, err)
	}
}

func assertCatalogReadGrants(t *testing.T, store *CatalogGrantStore, expected CatalogReadGrant) {
	grants, err := store.ListCatalogReadGrants()
	if err != nil || len(grants) != 1 || grants[0] != expected {
		t.Fatalf("restored grants = %+v, %v", grants, err)
	}
}

func revokeCatalogRead(t *testing.T, store *CatalogGrantStore, nodeID string) {
	if err := store.RevokeCatalogRead(nodeID); err != nil {
		t.Fatal(err)
	}
}

func assertRevokeMissingCatalogGrant(t *testing.T, store *CatalogGrantStore, nodeID string) {
	if err := store.RevokeCatalogRead(nodeID); err != ErrCatalogGrantMissing {
		t.Fatalf("revoke missing grant error = %v", err)
	}
}

func assertInferenceNeedsCatalog(t *testing.T, store *CatalogGrantStore, member Membership, at time.Time) {
	if _, err := store.GrantInference(member, false, true, at); err != ErrInferenceNeedsCatalog {
		t.Fatalf("route inference without catalog discovery grant = %v", err)
	}
}

func grantInference(t *testing.T, store *CatalogGrantStore, member Membership, allowIncoming, routeOutgoing bool, at time.Time) InferenceGrant {
	grant, err := store.GrantInference(member, allowIncoming, routeOutgoing, at)
	if err != nil {
		t.Fatal(err)
	}
	return grant
}

func assertInferenceGrant(t *testing.T, grant InferenceGrant, allowIncoming, routeOutgoing bool) {
	if grant.AllowIncoming != allowIncoming || grant.RouteOutgoing != routeOutgoing {
		t.Fatalf("inference grant = %+v", grant)
	}
}

func assertAllowsIncomingInference(t *testing.T, store *CatalogGrantStore, nodeID, fingerprint string, allowed bool) {
	incoming, err := store.AllowsIncomingInference(nodeID, fingerprint)
	if err != nil || incoming != allowed {
		t.Fatalf("AllowsIncomingInference(%q, %q) = %v, %v", nodeID, fingerprint, incoming, err)
	}
}

func assertAllowsOutgoingInference(t *testing.T, store *CatalogGrantStore, nodeID, fingerprint string, allowed bool) {
	outgoing, err := store.AllowsOutgoingInference(nodeID, fingerprint)
	if err != nil || outgoing != allowed {
		t.Fatalf("AllowsOutgoingInference(%q, %q) = %v, %v", nodeID, fingerprint, outgoing, err)
	}
}

func revokeInference(t *testing.T, store *CatalogGrantStore, nodeID string) {
	if err := store.RevokeInference(nodeID); err != nil {
		t.Fatal(err)
	}
}

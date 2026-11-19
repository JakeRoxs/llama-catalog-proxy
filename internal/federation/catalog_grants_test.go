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
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := OpenCatalogGrantStore(directory, "lab.example", "node-a")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	firstFingerprint := strings.Repeat("a", 64)
	member := Membership{NodeID: "node-b", Fingerprint: firstFingerprint, PairedAt: now}
	grant, err := store.GrantCatalogRead(member, now)
	if err != nil || grant.NodeID != member.NodeID || grant.Fingerprint != firstFingerprint || !grant.GrantedAt.Equal(now) {
		t.Fatalf("grant = %+v, %v", grant, err)
	}
	allowed, err := store.AllowsCatalogRead("node-b", firstFingerprint)
	if err != nil || !allowed {
		t.Fatalf("matching fingerprint allowed = %v, %v", allowed, err)
	}
	allowed, err = store.AllowsCatalogRead("node-b", strings.Repeat("b", 64))
	if err != nil || allowed {
		t.Fatalf("different fingerprint allowed = %v, %v", allowed, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(directory, catalogGrantFilename))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("catalog grant file mode = %v, %v", info, err)
	}

	store, err = OpenCatalogGrantStore(directory, "lab.example", "node-a")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	grants, err := store.ListCatalogReadGrants()
	if err != nil || len(grants) != 1 || grants[0] != grant {
		t.Fatalf("restored grants = %+v, %v", grants, err)
	}
	rotatedFingerprint := strings.Repeat("c", 64)
	if _, err := store.GrantCatalogRead(Membership{NodeID: "node-b", Fingerprint: rotatedFingerprint, PairedAt: now.Add(time.Hour)}, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if allowed, err := store.AllowsCatalogRead("node-b", firstFingerprint); err != nil || allowed {
		t.Fatalf("old fingerprint retained grant = %v, %v", allowed, err)
	}
	if allowed, err := store.AllowsCatalogRead("node-b", rotatedFingerprint); err != nil || !allowed {
		t.Fatalf("rotated fingerprint grant = %v, %v", allowed, err)
	}
	if err := store.RevokeCatalogRead("node-b"); err != nil {
		t.Fatal(err)
	}
	if allowed, err := store.AllowsCatalogRead("node-b", rotatedFingerprint); err != nil || allowed {
		t.Fatalf("revoked catalog grant = %v, %v", allowed, err)
	}
	if err := store.RevokeCatalogRead("node-b"); err != ErrCatalogGrantMissing {
		t.Fatalf("revoke missing grant error = %v", err)
	}
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

//go:build linux

package modelconfig

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newOwnerGrantFixture(t *testing.T) (string, OwnerGrant) {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	return directory, OwnerGrant{
		ID: "0123456789abcdef0123456789abcdef", RequesterNodeID: "node-b", Action: "manage_settings",
		BackendID: "peer-owner", TargetID: "owner-swap", ModelIDs: []string{"repo/model-a"},
		CreatedAt: now, ExpiresAt: now.Add(time.Hour), ApprovedBy: "oidc_admin",
	}
}

func TestOwnerGrantStoreIsTargetAndModelScopedAndRevocable(t *testing.T) {
	directory, grant := newOwnerGrantFixture(t)
	if err := ValidateOwnerGrantStore(directory); err != nil {
		t.Fatal(err)
	}
	if err := CreateOwnerGrant(directory, grant); err != nil {
		t.Fatal(err)
	}
	if allowed, err := OwnerGrantAuthorizesAt(directory, "node-b", "manage_settings", "backend-a", "target-a", "model-a", grant.ExpiresAt.Add(time.Second)); err != nil || allowed {
		t.Fatalf("expired owner grant authorization = %v, %v", allowed, err)
	}
	grants, err := ListOwnerGrants(directory)
	if err != nil || len(grants) != 1 || grants[0].ID != grant.ID {
		t.Fatalf("stored grants = %+v, %v", grants, err)
	}
	for _, test := range []struct {
		requester, action, backend, target, model string
		want                                      bool
	}{
		{"node-b", "manage_settings", "peer-owner", "owner-swap", "repo/model-a", true},
		{"node-c", "manage_settings", "peer-owner", "owner-swap", "repo/model-a", false},
		{"node-b", "manage_settings", "peer-other", "owner-swap", "repo/model-a", false},
		{"node-b", "manage_settings", "peer-owner", "other-target", "repo/model-a", false},
		{"node-b", "manage_settings", "peer-owner", "owner-swap", "repo/model-b", false},
		{"node-b", "inference", "peer-owner", "owner-swap", "repo/model-a", false},
	} {
		allowed, err := OwnerGrantAuthorizes(directory, test.requester, test.action, test.backend, test.target, test.model)
		if err != nil || allowed != test.want {
			t.Fatalf("grant check %+v = %v, %v", test, allowed, err)
		}
	}
	if err := RevokeOwnerGrant(directory, grant.ID); err != nil {
		t.Fatal(err)
	}
	if allowed, err := OwnerGrantAuthorizes(directory, "node-b", "manage_settings", "peer-owner", "owner-swap", "repo/model-a"); err != nil || allowed {
		t.Fatalf("revoked grant authorization = %v, %v", allowed, err)
	}
	if err := RevokeOwnerGrant(directory, grant.ID); !errors.Is(err, ErrOwnerGrantNotFound) {
		t.Fatalf("duplicate revoke error = %v", err)
	}
}

func TestOwnerGrantStoreRejectsOverlappingAndInvalidGrants(t *testing.T) {
	directory, grant := newOwnerGrantFixture(t)
	if err := CreateOwnerGrant(directory, grant); err != nil {
		t.Fatal(err)
	}
	overlap := grant
	overlap.ID = "1123456789abcdef0123456789abcdef"
	overlap.ModelIDs = []string{"repo/model-a", "repo/model-b"}
	if err := CreateOwnerGrant(directory, overlap); !errors.Is(err, ErrOwnerGrantConflict) {
		t.Fatalf("overlapping grant error = %v", err)
	}
	invalid := grant
	invalid.ID = "2123456789abcdef0123456789abcdef"
	invalid.ModelIDs = []string{"../outside"}
	if err := CreateOwnerGrant(directory, invalid); !errors.Is(err, ErrOwnerGrantInvalid) {
		t.Fatalf("invalid model grant error = %v", err)
	}
	if err := ValidateOwnerGrantStore(filepath.Join(directory, "missing")); !errors.Is(err, ErrOwnerGrantStore) {
		t.Fatalf("missing grant directory error = %v", err)
	}
}

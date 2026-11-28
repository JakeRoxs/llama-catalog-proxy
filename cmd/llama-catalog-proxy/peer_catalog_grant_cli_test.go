package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/federation"
)

func TestPeerCatalogGrantCLIRequiresPairingAndFingerprintRegrant(t *testing.T) {
	configPath, membershipDir := setupPeerCatalogGrantFixture(t)

	// Pair node-b with the initial fingerprint.
	establishPeerPairing(t, membershipDir, strings.Repeat("a", 64), time.Now().UTC())

	// An outgoing inference grant requires an active catalog read grant.
	requireOutgoingInferenceGrantRejected(t, configPath, "node-b")

	// Catalog read grant is created for the paired node.
	requireCatalogGrantCreated(t, configPath, "node-b", strings.Repeat("a", 64))

	// The grant is listed as currently valid.
	requireCatalogGrantListedCurrent(t, configPath)

	// An independent route-outgoing inference grant is created.
	requireInferenceGrantCreated(t, configPath, "node-b")
	requireInferenceGrantListedActive(t, configPath)

	// Rotate the paired node's fingerprint.
	establishPeerPairing(t, membershipDir, strings.Repeat("b", 64), time.Now().UTC().Add(time.Minute))

	// Existing grants no longer match the rotated fingerprint.
	requireCatalogGrantListedNotCurrent(t, configPath)
	requireInferenceGrantListedNotCurrent(t, configPath)

	// Re-grant after rotation.
	requireCatalogGrantCreated(t, configPath, "node-b", strings.Repeat("b", 64))
	requireInferenceGrantCreated(t, configPath, "node-b")

	// Revoking the catalog grant deactivates both grants.
	requireCatalogGrantRevoked(t, configPath, "node-b")
	requireCatalogGrantEmpty(t, configPath)
	requireInferenceGrantEmpty(t, configPath)
}

// setupPeerCatalogGrantFixture writes a peer config with a catalog grant
// directory and discovery enabled, returning the config and membership paths.
func setupPeerCatalogGrantFixture(t *testing.T) (configPath, membershipDir string) {
	t.Helper()
	configPath, membershipDir = peerCLIConfig(t)
	grantDir := filepath.Join(filepath.Dir(configPath), "catalog-grants")
	if err := os.Mkdir(grantDir, 0o700); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	updated := strings.Replace(string(contents), "  peers: {}", fmt.Sprintf(`  catalog_grant_directory: %q
  discovery:
    enabled: true
    interface: eth0
    advertise_url: https://192.168.1.10:9293
  peers: {}`, grantDir), 1)
	if updated == string(contents) {
		t.Fatal("test config peer map was not replaced")
	}
	if err := os.WriteFile(configPath, []byte(updated), 0o600); err != nil {
		t.Fatal(err)
	}
	return configPath, membershipDir
}

// establishPeerPairing opens the pairing store, registers a member, and closes
// the store, preserving the temporary store lifecycle.
func establishPeerPairing(t *testing.T, membershipDir, fingerprint string, at time.Time) {
	t.Helper()
	store, err := federation.OpenPairingStore(membershipDir, "lab.example", "node-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AddMember("node-b", fingerprint, at); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

// requireOutgoingInferenceGrantRejected asserts that an outgoing inference
// grant is refused when no catalog read grant is active.
func requireOutgoingInferenceGrantRejected(t *testing.T, configPath, nodeID string) {
	t.Helper()
	var output bytes.Buffer
	if err := runPeerCommand([]string{"inference-grants", "create", "--config", configPath, "--node-id", nodeID,
		"--route-outgoing", "--acknowledge"}, &output, &bytes.Buffer{}); err == nil {
		t.Fatal("outgoing inference grant without catalog read was accepted")
	}
}

// requireCatalogGrantCreated asserts a catalog read grant is created with the
// expected node and fingerprint.
func requireCatalogGrantCreated(t *testing.T, configPath, nodeID, fingerprint string) {
	t.Helper()
	var output bytes.Buffer
	if err := runPeerCommand([]string{"catalog-grants", "create", "--config", configPath,
		"--node-id", nodeID, "--acknowledge"}, &output, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var created federation.CatalogReadGrant
	if err := json.Unmarshal(output.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.NodeID != nodeID || created.Fingerprint != fingerprint {
		t.Fatalf("created catalog grant = %+v", created)
	}
}

// requireCatalogGrantListedCurrent asserts exactly one catalog grant is listed
// and matches the current fingerprint.
func requireCatalogGrantListedCurrent(t *testing.T, configPath string) {
	t.Helper()
	var output bytes.Buffer
	if err := runPeerCommand([]string{"catalog-grants", "list", "--config", configPath}, &output, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var listed []peerCatalogGrantView
	if err := json.Unmarshal(output.Bytes(), &listed); err != nil || len(listed) != 1 || !listed[0].CurrentFingerprint {
		t.Fatalf("active catalog grant list = %s, %v", output.String(), err)
	}
}

// requireCatalogGrantListedNotCurrent asserts exactly one catalog grant is
// listed but it no longer matches the current fingerprint.
func requireCatalogGrantListedNotCurrent(t *testing.T, configPath string) {
	t.Helper()
	var output bytes.Buffer
	if err := runPeerCommand([]string{"catalog-grants", "list", "--config", configPath}, &output, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var listed []peerCatalogGrantView
	if err := json.Unmarshal(output.Bytes(), &listed); err != nil || len(listed) != 1 || listed[0].CurrentFingerprint {
		t.Fatalf("rotated certificate inherited catalog grant: %s, %v", output.String(), err)
	}
}

// requireInferenceGrantCreated asserts an inference grant is created.
func requireInferenceGrantCreated(t *testing.T, configPath, nodeID string) {
	t.Helper()
	var output bytes.Buffer
	if err := runPeerCommand([]string{"inference-grants", "create", "--config", configPath, "--node-id", nodeID,
		"--route-outgoing", "--acknowledge"}, &output, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
}

// requireInferenceGrantListedActive asserts exactly one inference grant is
// listed as current, route-outgoing, and allowing incoming.
func requireInferenceGrantListedActive(t *testing.T, configPath string) {
	t.Helper()
	var output bytes.Buffer
	if err := runPeerCommand([]string{"inference-grants", "list", "--config", configPath}, &output, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var inferenceListed []peerInferenceGrantView
	if err := json.Unmarshal(output.Bytes(), &inferenceListed); err != nil || len(inferenceListed) != 1 ||
		!inferenceListed[0].CurrentFingerprint || !inferenceListed[0].RouteOutgoing || inferenceListed[0].AllowIncoming {
		t.Fatalf("independent route-inference grant = %s, %v", output.String(), err)
	}
}

// requireInferenceGrantListedNotCurrent asserts exactly one inference grant is
// listed but it no longer matches the current fingerprint.
func requireInferenceGrantListedNotCurrent(t *testing.T, configPath string) {
	t.Helper()
	var output bytes.Buffer
	if err := runPeerCommand([]string{"inference-grants", "list", "--config", configPath}, &output, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var inferenceListed []peerInferenceGrantView
	if err := json.Unmarshal(output.Bytes(), &inferenceListed); err != nil || len(inferenceListed) != 1 || inferenceListed[0].CurrentFingerprint {
		t.Fatalf("rotated certificate inherited inference grant: %s, %v", output.String(), err)
	}
}

// requireCatalogGrantRevoked revokes the catalog grant for the node.
func requireCatalogGrantRevoked(t *testing.T, configPath, nodeID string) {
	t.Helper()
	var output bytes.Buffer
	if err := runPeerCommand([]string{"catalog-grants", "revoke", "--config", configPath,
		"--node-id", nodeID, "--acknowledge"}, &output, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
}

// requireCatalogGrantEmpty asserts the catalog grant listing is empty.
func requireCatalogGrantEmpty(t *testing.T, configPath string) {
	t.Helper()
	var output bytes.Buffer
	if err := runPeerCommand([]string{"catalog-grants", "list", "--config", configPath}, &output, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(output.String()) != "[]" {
		t.Fatalf("revoked catalog grant remains listed: %s", output.String())
	}
}

// requireInferenceGrantEmpty asserts the inference grant listing is empty.
func requireInferenceGrantEmpty(t *testing.T, configPath string) {
	t.Helper()
	var output bytes.Buffer
	if err := runPeerCommand([]string{"inference-grants", "list", "--config", configPath}, &output, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(output.String()) != "[]" {
		t.Fatalf("catalog revoke left outbound inference grant active: %s", output.String())
	}
}

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
	configPath, membershipDir := peerCLIConfig(t)
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

	store, err := federation.OpenPairingStore(membershipDir, "lab.example", "node-a")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err := store.AddMember("node-b", strings.Repeat("a", 64), now); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	if err := runPeerCommand([]string{"inference-grants", "create", "--config", configPath, "--node-id", "node-b",
		"--route-outgoing", "--acknowledge"}, &output, &bytes.Buffer{}); err == nil {
		t.Fatal("outgoing inference grant without catalog read was accepted")
	}
	output.Reset()
	if err := runPeerCommand([]string{"catalog-grants", "create", "--config", configPath,
		"--node-id", "node-b", "--acknowledge"}, &output, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var created federation.CatalogReadGrant
	if err := json.Unmarshal(output.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.NodeID != "node-b" || created.Fingerprint != strings.Repeat("a", 64) {
		t.Fatalf("created catalog grant = %+v", created)
	}
	output.Reset()
	if err := runPeerCommand([]string{"catalog-grants", "list", "--config", configPath}, &output, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var listed []peerCatalogGrantView
	if err := json.Unmarshal(output.Bytes(), &listed); err != nil || len(listed) != 1 || !listed[0].CurrentFingerprint {
		t.Fatalf("active catalog grant list = %s, %v", output.String(), err)
	}
	if err := runPeerCommand([]string{"inference-grants", "create", "--config", configPath, "--node-id", "node-b",
		"--route-outgoing", "--acknowledge"}, &output, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	output.Reset()
	if err := runPeerCommand([]string{"inference-grants", "list", "--config", configPath}, &output, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var inferenceListed []peerInferenceGrantView
	if err := json.Unmarshal(output.Bytes(), &inferenceListed); err != nil || len(inferenceListed) != 1 ||
		!inferenceListed[0].CurrentFingerprint || !inferenceListed[0].RouteOutgoing || inferenceListed[0].AllowIncoming {
		t.Fatalf("independent route-inference grant = %s, %v", output.String(), err)
	}

	store, err = federation.OpenPairingStore(membershipDir, "lab.example", "node-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AddMember("node-b", strings.Repeat("b", 64), now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	output.Reset()
	if err := runPeerCommand([]string{"catalog-grants", "list", "--config", configPath}, &output, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(output.Bytes(), &listed); err != nil || len(listed) != 1 || listed[0].CurrentFingerprint {
		t.Fatalf("rotated certificate inherited catalog grant: %s, %v", output.String(), err)
	}
	output.Reset()
	if err := runPeerCommand([]string{"inference-grants", "list", "--config", configPath}, &output, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(output.Bytes(), &inferenceListed); err != nil || len(inferenceListed) != 1 || inferenceListed[0].CurrentFingerprint {
		t.Fatalf("rotated certificate inherited inference grant: %s, %v", output.String(), err)
	}

	if err := runPeerCommand([]string{"catalog-grants", "create", "--config", configPath,
		"--node-id", "node-b", "--acknowledge"}, &output, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if err := runPeerCommand([]string{"inference-grants", "create", "--config", configPath, "--node-id", "node-b",
		"--route-outgoing", "--acknowledge"}, &output, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if err := runPeerCommand([]string{"catalog-grants", "revoke", "--config", configPath,
		"--node-id", "node-b", "--acknowledge"}, &output, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	output.Reset()
	if err := runPeerCommand([]string{"catalog-grants", "list", "--config", configPath}, &output, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(output.String()) != "[]" {
		t.Fatalf("revoked catalog grant remains listed: %s", output.String())
	}
	output.Reset()
	if err := runPeerCommand([]string{"inference-grants", "list", "--config", configPath}, &output, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(output.String()) != "[]" {
		t.Fatalf("catalog revoke left outbound inference grant active: %s", output.String())
	}
}

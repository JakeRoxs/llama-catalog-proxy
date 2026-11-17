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

	"github.com/JakeRoxs/llama-catalog-proxy/internal/auditlog"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/federation"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
)

func peerCLIConfig(t *testing.T) (string, string) {
	t.Helper()
	root := t.TempDir()
	store := filepath.Join(root, "federation")
	audit := filepath.Join(root, "audit")
	for _, directory := range []string{store, audit} {
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	contents := fmt.Sprintf(`
default_backend: local
backends:
  local:
    url: http://127.0.0.1:9292
peer:
  node_id: node-a
  listen: 127.0.0.1:9293
  tls_cert_file: %q
  tls_key_file: %q
  federation_id: lab.example
  client_ca_file: %q
  client_crl_file: %q
  crl_max_age: 24h
  membership_directory: %q
  audit_directory: %q
  peers: {}
`, filepath.Join(root, "node.pem"), filepath.Join(root, "node.key"), filepath.Join(root, "ca.pem"), filepath.Join(root, "ca.crl.pem"), store, audit)
	configPath := filepath.Join(root, "config.yaml")
	if err := os.WriteFile(configPath, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return configPath, store
}

func TestPeerCLIInvitationAndMembershipCommandsSharePersistentStore(t *testing.T) {
	configPath, storeDirectory := peerCLIConfig(t)
	serverStore, err := federation.OpenPairingStore(storeDirectory, "lab.example", "node-a")
	if err != nil {
		t.Fatal(err)
	}
	defer serverStore.Close()

	var created bytes.Buffer
	if err := runPeerCommand([]string{"invite", "create", "--config", configPath, "--node-id", "node-b"}, &created, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var invitation federation.Invitation
	if err := json.Unmarshal(created.Bytes(), &invitation); err != nil || invitation.Code == "" {
		t.Fatalf("CLI invitation output = %q, %v", created.String(), err)
	}
	var listed bytes.Buffer
	if err := runPeerCommand([]string{"invite", "list", "--config", configPath}, &listed, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(listed.String(), invitation.Code) || !strings.Contains(listed.String(), "node-b") {
		t.Fatalf("CLI list exposed a code or omitted the expected node: %s", listed.String())
	}
	if _, err := serverStore.ConsumeInvitation(invitation.Code, "node-b", strings.Repeat("a", 64), time.Now()); err != nil {
		t.Fatal(err)
	}
	var members bytes.Buffer
	if err := runPeerCommand([]string{"members", "list", "--config", configPath}, &members, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(members.String(), "node-b") {
		t.Fatalf("CLI did not observe server membership: %s", members.String())
	}
	audit, err := auditlog.Open(filepath.Join(filepath.Dir(configPath), "audit"), "node-a")
	if err != nil {
		t.Fatal(err)
	}
	defer audit.Close()
	events, err := audit.ListRecent(20)
	if err != nil {
		t.Fatal(err)
	}
	auditBytes, _ := json.Marshal(events)
	foundCLI := false
	for _, event := range events {
		if event.Action == "pairing.invitation.create" && event.PrincipalType == "local_cli" && event.LocalAccount != "" {
			foundCLI = true
		}
	}
	if !foundCLI || strings.Contains(string(auditBytes), invitation.Code) {
		t.Fatalf("CLI operator attribution missing or invitation code leaked into audit: %+v", events)
	}
}

func TestPeerCLIRequiresProtectedInvitationFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invite-code")
	if err := os.WriteFile(path, []byte("opaque-code\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readPeerInvitation(path); err == nil {
		t.Fatal("group-readable invitation file accepted")
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if code, err := readPeerInvitation(path); err != nil || code != "opaque-code" {
		t.Fatalf("protected invitation file = %q, %v", code, err)
	}
	if err := os.WriteFile(path, []byte(`{"code":"json-one-time-code","node_id":"node-b","expires_at":"2026-11-16T00:10:00Z"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if code, err := readPeerInvitation(path); err != nil || code != "json-one-time-code" {
		t.Fatalf("protected invitation JSON file = %q, %v", code, err)
	}
}

func TestPeerCLIGrantsRespectStaticCeilingAndPersistOwnerGrant(t *testing.T) {
	root := t.TempDir()
	paths := map[string]string{}
	for _, name := range []string{"membership", "audit", "grants", "proposals", "operations", "backups", "proposal-store", "target"} {
		path := filepath.Join(root, name)
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
		paths[name] = path
	}
	if err := os.WriteFile(filepath.Join(paths["target"], "config.yaml"), []byte("models:\n  model:\n    cmd: llama-server -c 1024\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	configContents := fmt.Sprintf(`
listen: 127.0.0.1:8080
default_backend: backend-a
backends:
  backend-a:
    url: http://127.0.0.1:9292
peer:
  node_id: owner
  listen: 127.0.0.1:8081
  tls_cert_file: %q
  tls_key_file: %q
  federation_id: lab.example
  client_ca_file: %q
  client_crl_file: %q
  membership_directory: %q
  audit_directory: %q
  local_backends: [backend-a]
  management_targets: {backend-a: target-a}
  peers:
    node-b:
      token: %q
      manage_settings: true
management:
  listen: 127.0.0.1:8383
  tls_cert_file: %q
  tls_key_file: %q
  preview_token: %q
  write_token: %q
  delegation_token: %q
  proposal_store: {directory: %q, token: %q}
  backup_store: {directory: %q, token: %q, max_backups: 4}
  delegated_store:
    proposal_directory: %q
    operation_directory: %q
    grant_directory: %q
  targets:
    target-a: {directory: %q, filename: config.yaml}
`,
		filepath.Join(root, "node.pem"), filepath.Join(root, "node.key"), filepath.Join(root, "ca.pem"), filepath.Join(root, "ca.crl.pem"), paths["membership"], paths["audit"],
		strings.Repeat("b", 32), filepath.Join(root, "management.pem"), filepath.Join(root, "management.key"),
		strings.Repeat("p", 32), strings.Repeat("w", 32), strings.Repeat("d", 32),
		paths["proposal-store"], strings.Repeat("q", 32), paths["backups"], strings.Repeat("r", 32),
		paths["proposals"], paths["operations"], paths["grants"], paths["target"])
	configPath := filepath.Join(root, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configContents), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := federation.OpenPairingStore(paths["membership"], "lab.example", "owner")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AddMember("node-b", strings.Repeat("a", 64), time.Now()); err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	createArgs := []string{"grants", "create", "--config", configPath, "--requester-node", "node-b", "--backend", "backend-a", "--models", "model", "--expires-in", "24h", "--acknowledge"}
	var output bytes.Buffer
	if err := runPeerCommand(createArgs, &output, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var grant modelconfig.OwnerGrant
	if err := json.Unmarshal(output.Bytes(), &grant); err != nil || grant.RequesterNodeID != "node-b" || grant.ApprovedBy != "local_cli" {
		t.Fatalf("CLI owner grant output = %+v, %v", grant, err)
	}
	if allowed, err := modelconfig.OwnerGrantAuthorizes(paths["grants"], "node-b", "manage_settings", "backend-a", "target-a", "model"); err != nil || !allowed {
		t.Fatalf("CLI owner grant did not authorize its exact model: %v, %v", allowed, err)
	}
	var grants bytes.Buffer
	if err := runPeerCommand([]string{"grants", "list", "--config", configPath}, &grants, &bytes.Buffer{}); err != nil || !bytes.Contains(grants.Bytes(), []byte(grant.ID)) {
		t.Fatalf("CLI grant list = %s, %v", grants.String(), err)
	}
	if err := runPeerCommand([]string{"grants", "revoke", "--config", configPath, "--grant-id", grant.ID}, &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if allowed, err := modelconfig.OwnerGrantAuthorizes(paths["grants"], "node-b", "manage_settings", "backend-a", "target-a", "model"); err != nil || allowed {
		t.Fatalf("revoked CLI grant remained active: %v, %v", allowed, err)
	}
	audit, err := auditlog.Open(paths["audit"], "owner")
	if err != nil {
		t.Fatal(err)
	}
	defer audit.Close()
	events, err := audit.ListRecent(20)
	if err != nil {
		t.Fatal(err)
	}
	foundGrantAudit := false
	for _, event := range events {
		if event.Action == "owner_grant.create" && event.PrincipalType == "local_cli" && event.LocalAccount != "" && event.TargetID == "target-a" {
			foundGrantAudit = true
		}
	}
	if !foundGrantAudit {
		t.Fatalf("CLI grant audit evidence missing: %+v", events)
	}
}

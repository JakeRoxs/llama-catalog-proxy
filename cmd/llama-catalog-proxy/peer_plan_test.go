package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPeerCLIConfigPlanReportsReciprocalHandoffWithoutSecrets(t *testing.T) {
	configPath, _ := peerCLIConfig(t)
	localInbound := strings.Repeat("a", 32)
	localOutbound := strings.Repeat("b", 32)
	contents, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	configured := strings.Replace(string(contents), "  peers: {}", `  peers:
    node-b:
      token: `+localInbound+`
      catalog_read: true
      url: https://node-b.example:9293
      outbound_token: `+localOutbound+`
      ca_file: /run/secrets/node-b-ca.pem
      crl_file: /run/secrets/node-b.crl.pem
      route_inference: true`, 1)
	if configured == string(contents) {
		t.Fatal("test config peer map was not replaced")
	}
	if err := os.WriteFile(configPath, []byte(configured), 0o600); err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	if err := runPeerCommand([]string{"config", "plan", "--config", configPath}, &output, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var plan peerConfigPlan
	if err := json.Unmarshal(output.Bytes(), &plan); err != nil {
		t.Fatal(err)
	}
	if plan.LocalNodeID != "node-a" || len(plan.Peers) != 1 {
		t.Fatalf("plan identity/peer count = %+v", plan)
	}
	peer := plan.Peers[0]
	if peer.NodeID != "node-b" || peer.Direction != "bidirectional" || !peer.OutboundURLConfigured ||
		!peer.InboundTokenConfigured || !peer.OutboundTokenConfigured {
		t.Fatalf("peer direction/configuration = %+v", peer)
	}
	if strings.Join(peer.InboundScopes, ",") != "catalog_read" || strings.Join(peer.OutboundScopes, ",") != "route_inference" {
		t.Fatalf("scope directionality = in %v / out %v", peer.InboundScopes, peer.OutboundScopes)
	}
	text := output.String()
	if strings.Contains(text, localInbound) || strings.Contains(text, localOutbound) || strings.Contains(text, "node-b.example") {
		t.Fatalf("planner exposed credential or endpoint: %s", text)
	}
	if !strings.Contains(text, "peers.node-a.outbound_token") || !strings.Contains(text, "peers.node-a.token") {
		t.Fatalf("planner omitted exact remote handoff fields: %s", text)
	}
}

func TestPeerCLIConfigPlanRejectsMissingFileAndSupportsEmptyPeerList(t *testing.T) {
	configPath, _ := peerCLIConfig(t)
	var output bytes.Buffer
	if err := runPeerCommand([]string{"config", "plan", "--config", filepath.Join(filepath.Dir(configPath), "missing.yaml")}, &output, &bytes.Buffer{}); err == nil {
		t.Fatal("missing config accepted")
	}
	if err := runPeerCommand([]string{"config", "plan", "--config", configPath}, &output, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"peers":[]`) {
		t.Fatalf("empty peer plan = %s", output.String())
	}

	contents, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	configured := strings.Replace(string(contents), "  peers: {}", "  peers:\n    node-b:\n      token: "+strings.Repeat("c", 32)+"\n      inference: true", 1)
	if configured == string(contents) {
		t.Fatal("test config peer map was not replaced")
	}
	if err := os.WriteFile(configPath, []byte(configured), 0o600); err != nil {
		t.Fatal(err)
	}
	output.Reset()
	if err := runPeerCommand([]string{"config", "plan", "--config", configPath}, &output, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var plan peerConfigPlan
	if err := json.Unmarshal(output.Bytes(), &plan); err != nil {
		t.Fatal(err)
	}
	if len(plan.Peers) != 1 || plan.Peers[0].Direction != "inbound" ||
		strings.Join(plan.Peers[0].InboundScopes, ",") != "inference" || plan.Peers[0].OutboundURLConfigured {
		t.Fatalf("one-way inbound peer plan = %+v", plan)
	}

	invalid := strings.Replace(configured, "      inference: true", "      unknown_scope: true", 1)
	if err := os.WriteFile(configPath, []byte(invalid), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runPeerCommand([]string{"config", "plan", "--config", configPath}, &output, &bytes.Buffer{}); err == nil {
		t.Fatal("config with an unknown peer field bypassed canonical validation")
	}
}

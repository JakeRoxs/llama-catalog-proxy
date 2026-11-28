package main

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/discovery"
)

func TestPeerCLIDiscoveryIsOptInAndDoesNotEnrollCandidates(t *testing.T) {
	configPath, membershipDirectory := peerCLIConfig(t)
	var output bytes.Buffer
	err := runPeerCommand([]string{"discover", "--config", configPath}, &output, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "not enabled") {
		t.Fatalf("disabled discovery error = %v", err)
	}
	if output.Len() != 0 {
		t.Fatalf("disabled discovery produced output: %s", output.String())
	}
	entries, err := os.ReadDir(membershipDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("candidate discovery changed membership store: %v", entries)
	}
}

func TestSelectPeerCandidateFailsOnMissingAmbiguousOrTruncatedResults(t *testing.T) {
	result := discovery.CandidateResult{Candidates: []discovery.Candidate{
		{NodeID: "node-b", PeerURL: "https://node-b.local:9293"},
	}}
	if endpoint, err := selectPeerCandidate(result, "node-b"); err != nil || endpoint != "https://node-b.local:9293" {
		t.Fatalf("selected endpoint = %q, %v", endpoint, err)
	}
	if _, err := selectPeerCandidate(result, "node-c"); err == nil {
		t.Fatal("missing candidate selected")
	}
	result.Candidates = append(result.Candidates, discovery.Candidate{NodeID: "node-b", PeerURL: "https://second.local:9293"})
	if _, err := selectPeerCandidate(result, "node-b"); err == nil {
		t.Fatal("ambiguous candidate endpoints selected")
	}
	result.Truncated = true
	if _, err := selectPeerCandidate(result, "node-b"); err == nil {
		t.Fatal("candidate selected from a truncated list")
	}
}

func TestPeerJoinRequiresOneExplicitOrDiscoveredTarget(t *testing.T) {
	var output bytes.Buffer
	err := runPeerCommand([]string{
		"join", "--url", "https://node-a.local:9293", "--discover-node-id", "node-a",
		"--invitation-file", "/missing/invitation.json",
	}, &output, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("join with two target sources = %v", err)
	}
	if output.Len() != 0 {
		t.Fatalf("invalid join target selection produced output: %s", output.String())
	}
}

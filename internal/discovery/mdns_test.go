package discovery

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/mdns"
)

func TestCandidateFromEntryValidatesIdentityFederationAndEndpoint(t *testing.T) {
	valid := &mdns.ServiceEntry{
		Name: instanceName("node-b"), Host: "node-b.local.", Port: 9293, AddrV4: net.ParseIP("192.168.1.51"),
		InfoFields: []string{
			"protocol=1", "node_id=node-b", "federation_id=lab.example",
			"peer_url=https://node-b.local:9293",
		},
	}
	candidate, ok := candidateFromEntry(valid, "lab.example", "node-a")
	if !ok || candidate != (Candidate{NodeID: "node-b", FederationID: "lab.example", PeerURL: "https://node-b.local:9293", Port: 9293}) {
		t.Fatalf("valid candidate = %+v, %v", candidate, ok)
	}

	cases := map[string]func(*mdns.ServiceEntry){
		"wrong protocol":    func(entry *mdns.ServiceEntry) { entry.InfoFields[0] = "protocol=2" },
		"wrong federation":  func(entry *mdns.ServiceEntry) { entry.InfoFields[2] = "federation_id=other.example" },
		"self identity":     func(entry *mdns.ServiceEntry) { entry.InfoFields[1] = "node_id=node-a" },
		"invalid node ID":   func(entry *mdns.ServiceEntry) { entry.InfoFields[1] = "node_id=../node" },
		"duplicate field":   func(entry *mdns.ServiceEntry) { entry.InfoFields = append(entry.InfoFields, "node_id=node-c") },
		"port mismatch":     func(entry *mdns.ServiceEntry) { entry.Port = 9294 },
		"instance mismatch": func(entry *mdns.ServiceEntry) { entry.Name = instanceName("node-c") },
		"host mismatch":     func(entry *mdns.ServiceEntry) { entry.InfoFields[3] = "peer_url=https://node-c.local:9293" },
		"non-HTTPS URL":     func(entry *mdns.ServiceEntry) { entry.InfoFields[3] = "peer_url=http://node-b.local:9293" },
		"URL path":          func(entry *mdns.ServiceEntry) { entry.InfoFields[3] = "peer_url=https://node-b.local:9293/peer" },
		"URL host address mismatch": func(entry *mdns.ServiceEntry) {
			entry.InfoFields[3] = "peer_url=https://203.0.113.10:9293"
		},
		"non-local URL host": func(entry *mdns.ServiceEntry) {
			entry.Host = "proxy.example.com."
			entry.InfoFields[3] = "peer_url=https://proxy.example.com:9293"
		},
		"missing address": func(entry *mdns.ServiceEntry) { entry.AddrV4 = nil },
		"oversized TXT field": func(entry *mdns.ServiceEntry) {
			entry.InfoFields[3] = "peer_url=https://" + strings.Repeat("x", 250)
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			entry := cloneEntry(valid)
			mutate(entry)
			if candidate, ok := candidateFromEntry(entry, "lab.example", "node-a"); ok {
				t.Fatalf("untrusted candidate accepted: %+v", candidate)
			}
		})
	}
}

func TestAdvertisementTXTContainsOnlyDiscoveryMetadata(t *testing.T) {
	settings := Settings{
		NodeID: "node-a", FederationID: "lab.example", AdvertiseURL: "https://node-a.local:9293",
	}
	want := []string{
		"protocol=1", "node_id=node-a", "federation_id=lab.example", "peer_url=https://node-a.local:9293",
	}
	if got := advertisementTXT(settings); !reflect.DeepEqual(got, want) {
		t.Fatalf("advertisement TXT = %v, want %v", got, want)
	}
}

func TestCollectCandidatesFiltersAndBoundsUntrustedResults(t *testing.T) {
	entries := make(chan *mdns.ServiceEntry, maxCandidateBatch)
	for index := 69; index >= 0; index-- {
		nodeID := fmt.Sprintf("node-%02d", index)
		entries <- &mdns.ServiceEntry{
			Name: instanceName(nodeID), Host: nodeID + ".local.", Port: 9293, AddrV4: net.ParseIP("192.168.1.51"),
			InfoFields: []string{
				"protocol=1", "node_id=" + nodeID, "federation_id=lab.example",
				"peer_url=https://" + nodeID + ".local:9293",
			},
		}
	}
	entries <- &mdns.ServiceEntry{
		Host: "other.local.", Port: 9293, AddrV4: net.ParseIP("192.168.1.52"),
		InfoFields: []string{"protocol=1", "node_id=other", "federation_id=other.example", "peer_url=https://other.local:9293"},
	}
	close(entries)
	result := collectCandidates(entries, "lab.example", "node-a", false)
	if len(result.Candidates) != maxCandidates || !result.Truncated {
		t.Fatalf("candidate count/truncation = %d/%v, want cap %d and truncated", len(result.Candidates), result.Truncated, maxCandidates)
	}
	if result.Candidates[0].NodeID != "node-00" || result.Candidates[len(result.Candidates)-1].NodeID != "node-63" {
		t.Fatalf("candidate order/bounds = %q .. %q", result.Candidates[0].NodeID, result.Candidates[len(result.Candidates)-1].NodeID)
	}

	duplicate := make(chan *mdns.ServiceEntry, 2)
	entry := &mdns.ServiceEntry{
		Name: instanceName("node-b"), Host: "node-b.local.", Port: 9293, AddrV4: net.ParseIP("192.168.1.51"),
		InfoFields: []string{"protocol=1", "node_id=node-b", "federation_id=lab.example", "peer_url=https://node-b.local:9293"},
	}
	duplicate <- cloneEntry(entry)
	duplicate <- cloneEntry(entry)
	close(duplicate)
	got := collectCandidates(duplicate, "lab.example", "node-a", false)
	if !reflect.DeepEqual(got.Candidates, []Candidate{{
		NodeID: "node-b", FederationID: "lab.example", PeerURL: "https://node-b.local:9293", Port: 9293,
	}}) || got.Truncated {
		t.Fatalf("duplicate candidates were not collapsed: %+v", got)
	}
}

func TestBrowseUsesConfiguredInterfaceAndFiltersCandidates(t *testing.T) {
	iface := &net.Interface{Name: "lan0", Index: 7, Flags: net.FlagUp | net.FlagMulticast}
	queryCalled := false
	query := func(_ context.Context, params *mdns.QueryParam) error {
		queryCalled = true
		if params.Service != serviceName || params.Domain != serviceDomain || params.Timeout != 2*time.Second ||
			params.Interface != iface || params.Entries == nil {
			t.Fatalf("mDNS query params = %+v", params)
		}
		params.Entries <- &mdns.ServiceEntry{
			Name: instanceName("node-b"), Host: "node-b.local.", Port: 9293, AddrV4: net.ParseIP("192.168.1.51"),
			InfoFields: []string{"protocol=1", "node_id=node-b", "federation_id=lab.example", "peer_url=https://node-b.local:9293"},
		}
		params.Entries <- &mdns.ServiceEntry{
			Name: instanceName("untrusted"), Host: "untrusted.local.", Port: 9293, AddrV4: net.ParseIP("192.168.1.52"),
			InfoFields: []string{"protocol=1", "node_id=untrusted", "federation_id=elsewhere", "peer_url=https://untrusted.local:9293"},
		}
		return nil
	}
	candidates, err := browse(context.Background(), Settings{
		NodeID: "node-a", FederationID: "lab.example", Interface: "lan0",
	}, 2*time.Second, query, func(name string) (*net.Interface, error) {
		if name != "lan0" {
			t.Fatalf("interface lookup name = %q", name)
		}
		return iface, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !queryCalled || len(candidates.Candidates) != 1 || candidates.Candidates[0].NodeID != "node-b" || candidates.Truncated {
		t.Fatalf("discovery candidates = %+v, query called=%v", candidates, queryCalled)
	}
}

func TestBrowseRejectsInterfaceWithoutMulticastAndPropagatesQueryErrors(t *testing.T) {
	settings := Settings{NodeID: "node-a", FederationID: "lab.example", Interface: "lan0"}
	queryCalled := false
	query := func(context.Context, *mdns.QueryParam) error {
		queryCalled = true
		return nil
	}
	lookup := func(string) (*net.Interface, error) {
		return &net.Interface{Name: "lan0", Flags: net.FlagUp}, nil
	}
	if _, err := browse(context.Background(), settings, time.Second, query, lookup); err == nil || queryCalled {
		t.Fatalf("browse accepted non-multicast interface or queried: err=%v called=%v", err, queryCalled)
	}
	lookup = func(string) (*net.Interface, error) {
		return &net.Interface{Name: "lan0", Flags: net.FlagUp | net.FlagMulticast}, nil
	}
	query = func(context.Context, *mdns.QueryParam) error { return fmt.Errorf("query failed") }
	if _, err := browse(context.Background(), settings, time.Second, query, lookup); err == nil || !strings.Contains(err.Error(), "query failed") {
		t.Fatalf("mDNS query error = %v", err)
	}
}

func TestStartRejectsUnavailableInterface(t *testing.T) {
	_, err := Start(Settings{
		NodeID: "node-a", FederationID: "lab.example", Interface: "interface-that-does-not-exist",
		AdvertiseURL: "https://192.168.1.50:9293",
	})
	if err == nil {
		t.Fatal("advertising on an unavailable interface succeeded")
	}
}

func cloneEntry(entry *mdns.ServiceEntry) *mdns.ServiceEntry {
	cloned := *entry
	cloned.InfoFields = append([]string(nil), entry.InfoFields...)
	cloned.AddrV4 = append(net.IP(nil), entry.AddrV4...)
	return &cloned
}

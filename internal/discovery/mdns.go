// Package discovery provides untrusted mDNS candidate discovery for federation peers.
// Candidates never create membership, credentials, grants, or routes.
package discovery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/identity"
	"github.com/hashicorp/mdns"
)

const (
	serviceName       = "_llama-catalog-peer._tcp"
	serviceDomain     = "local."
	protocolVersion   = "1"
	maxCandidates     = 64
	maxCandidateBatch = 128
)

type Settings struct {
	NodeID       string
	FederationID string
	Interface    string
	AdvertiseURL string
}

type Candidate struct {
	NodeID       string `json:"node_id"`
	FederationID string `json:"federation_id"`
	PeerURL      string `json:"peer_url"`
	Port         int    `json:"port"`
}

type CandidateResult struct {
	Candidates []Candidate `json:"candidates"`
	Truncated  bool        `json:"truncated"`
}

type Advertiser struct {
	server *mdns.Server
}

func Start(settings Settings) (*Advertiser, error) {
	if !identity.ValidNodeID(settings.NodeID) || settings.FederationID == "" || len(settings.FederationID) > 241 {
		return nil, errors.New("invalid peer mDNS service identity")
	}
	iface, addresses, err := discoveryInterface(settings.Interface)
	if err != nil {
		return nil, err
	}
	parsed, err := parsePeerURL(settings.AdvertiseURL)
	if err != nil {
		return nil, err
	}
	port, err := advertisedPort(parsed)
	if err != nil {
		return nil, err
	}
	host := strings.ToLower(parsed.Hostname())
	serviceHost := host
	instance := instanceLabel(settings.NodeID)
	if net.ParseIP(host) != nil {
		serviceHost = instance + ".local"
		if !hasAdvertisedAddress(addresses, host) {
			return nil, errors.New("peer discovery advertise_url address is not assigned to the selected interface")
		}
	} else if !strings.HasSuffix(host, ".local") {
		return nil, errors.New("peer discovery hostname must use the .local domain")
	}
	service, err := mdns.NewMDNSService(instance, serviceName, serviceDomain, serviceHost+".", port, addresses, advertisementTXT(settings))
	if err != nil {
		return nil, fmt.Errorf("create mDNS peer service: %w", err)
	}
	server, err := mdns.NewServer(&mdns.Config{Zone: service, Iface: iface})
	if err != nil {
		return nil, fmt.Errorf("start mDNS peer advertisement: %w", err)
	}
	return &Advertiser{server: server}, nil
}

func (a *Advertiser) Close() error {
	if a == nil || a.server == nil {
		return nil
	}
	return a.server.Shutdown()
}

func Browse(ctx context.Context, settings Settings, timeout time.Duration) (CandidateResult, error) {
	return browse(ctx, settings, timeout, mdns.QueryContext, net.InterfaceByName)
}

func browse(ctx context.Context, settings Settings, timeout time.Duration,
	query func(context.Context, *mdns.QueryParam) error,
	lookupInterface func(string) (*net.Interface, error)) (CandidateResult, error) {
	if ctx == nil || timeout < 250*time.Millisecond || timeout > 30*time.Second || settings.FederationID == "" || len(settings.FederationID) > 241 ||
		!identity.ValidNodeID(settings.NodeID) {
		return CandidateResult{}, errors.New("invalid peer discovery request")
	}
	iface, err := lookupInterface(settings.Interface)
	if err != nil || iface == nil || iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagMulticast == 0 {
		return CandidateResult{}, errors.New("peer discovery interface is unavailable or does not support multicast")
	}
	entries := make(chan *mdns.ServiceEntry, maxCandidateBatch)
	params := mdns.DefaultParams(serviceName)
	params.Domain = serviceDomain
	params.Interface = iface
	params.Timeout = timeout
	params.Entries = entries
	if err := query(ctx, params); err != nil {
		return CandidateResult{}, fmt.Errorf("query mDNS peer candidates: %w", err)
	}
	return collectCandidates(entries, settings.FederationID, settings.NodeID, len(entries) == cap(entries)), nil
}

func collectCandidates(entries <-chan *mdns.ServiceEntry, federationID, localNodeID string, truncated bool) CandidateResult {
	candidates := make(map[string]Candidate)
	for {
		select {
		case entry, ok := <-entries:
			if !ok {
				return sortedCandidates(candidates, truncated)
			}
			candidate, ok := candidateFromEntry(entry, federationID, localNodeID)
			if !ok {
				continue
			}
			key := candidate.NodeID + "\x00" + candidate.PeerURL
			candidates[key] = candidate
		default:
			return sortedCandidates(candidates, truncated)
		}
	}
}

func sortedCandidates(candidates map[string]Candidate, truncated bool) CandidateResult {
	result := CandidateResult{Candidates: make([]Candidate, 0, len(candidates)), Truncated: truncated}
	for _, candidate := range candidates {
		result.Candidates = append(result.Candidates, candidate)
	}
	sort.Slice(result.Candidates, func(i, j int) bool {
		if result.Candidates[i].NodeID == result.Candidates[j].NodeID {
			return result.Candidates[i].PeerURL < result.Candidates[j].PeerURL
		}
		return result.Candidates[i].NodeID < result.Candidates[j].NodeID
	})
	if len(result.Candidates) > maxCandidates {
		result.Candidates = result.Candidates[:maxCandidates]
		result.Truncated = true
	}
	return result
}

func candidateFromEntry(entry *mdns.ServiceEntry, federationID, localNodeID string) (Candidate, bool) {
	if entry == nil || entry.Port < 1 || entry.Port > 65535 || len(entry.InfoFields) > 16 {
		return Candidate{}, false
	}
	fields := make(map[string]string, len(entry.InfoFields))
	for _, field := range entry.InfoFields {
		key, value, ok := strings.Cut(field, "=")
		if len(field) > 255 || !ok || key == "" || value == "" || fields[key] != "" {
			return Candidate{}, false
		}
		fields[key] = value
	}
	if fields["protocol"] != protocolVersion || fields["federation_id"] != federationID ||
		len(federationID) > 241 || !identity.ValidNodeID(fields["node_id"]) || fields["node_id"] == localNodeID {
		return Candidate{}, false
	}
	if strings.ToLower(entry.Name) != strings.ToLower(instanceName(fields["node_id"])) {
		return Candidate{}, false
	}
	peerURL, err := parsePeerURL(fields["peer_url"])
	if err != nil {
		return Candidate{}, false
	}
	port, err := advertisedPort(peerURL)
	if err != nil || port != entry.Port || !entryMatchesURL(entry, peerURL.Hostname()) {
		return Candidate{}, false
	}
	return Candidate{NodeID: fields["node_id"], FederationID: federationID, PeerURL: strings.TrimSuffix(peerURL.String(), "/"), Port: port}, true
}

func parsePeerURL(value string) (*url.URL, error) {
	parsed, err := url.Parse(value)
	if err != nil || parsed == nil || len(value) > 2048 || parsed.Scheme != "https" || parsed.Hostname() == "" ||
		parsed.User != nil || parsed.Opaque != "" || (parsed.Path != "" && parsed.Path != "/") || parsed.RawPath != "" ||
		parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || parsed.RawFragment != "" || strings.TrimSpace(value) != value {
		return nil, errors.New("invalid advertised peer URL")
	}
	return parsed, nil
}

func advertisedPort(parsed *url.URL) (int, error) {
	port := parsed.Port()
	if port == "" {
		if parsed.Scheme == "https" {
			return 443, nil
		}
		return 80, nil
	}
	value, err := strconv.Atoi(port)
	if err != nil || value < 1 || value > 65535 || strconv.Itoa(value) != port {
		return 0, errors.New("invalid advertised peer port")
	}
	return value, nil
}

func entryMatchesURL(entry *mdns.ServiceEntry, host string) bool {
	if ip := net.ParseIP(host); ip != nil {
		return usableAddress(ip) && (ip.Equal(entry.AddrV4) || ip.Equal(entry.AddrV6) ||
			entry.AddrV6IPAddr != nil && ip.Equal(entry.AddrV6IPAddr.IP))
	}
	serviceHost := strings.TrimSuffix(strings.ToLower(entry.Host), ".")
	if serviceHost != strings.ToLower(host) || !strings.HasSuffix(serviceHost, ".local") {
		return false
	}
	return usableAddress(entry.AddrV4) || usableAddress(entry.AddrV6) ||
		entry.AddrV6IPAddr != nil && usableAddress(entry.AddrV6IPAddr.IP)
}

func discoveryInterface(name string) (*net.Interface, []net.IP, error) {
	iface, err := net.InterfaceByName(name)
	if err != nil || iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagMulticast == 0 {
		return nil, nil, errors.New("peer discovery interface is unavailable or does not support multicast")
	}
	addresses, err := iface.Addrs()
	if err != nil {
		return nil, nil, errors.New("could not read peer discovery interface addresses")
	}
	ips := make([]net.IP, 0, len(addresses))
	for _, address := range addresses {
		var ip net.IP
		switch value := address.(type) {
		case *net.IPNet:
			ip = value.IP
		case *net.IPAddr:
			ip = value.IP
		}
		if usableAddress(ip) {
			ips = append(ips, append(net.IP(nil), ip...))
		}
	}
	if len(ips) == 0 {
		return nil, nil, errors.New("peer discovery interface has no usable unicast addresses")
	}
	return iface, ips, nil
}

func usableAddress(ip net.IP) bool {
	return ip != nil && ip.IsPrivate() && ip.IsGlobalUnicast() && !ip.IsLoopback() && !ip.IsMulticast() && !ip.IsUnspecified()
}

func hasAdvertisedAddress(addresses []net.IP, value string) bool {
	want := net.ParseIP(value)
	if want == nil {
		return false
	}
	for _, address := range addresses {
		if want.Equal(address) {
			return true
		}
	}
	return false
}

func advertisementTXT(settings Settings) []string {
	return []string{
		"protocol=" + protocolVersion,
		"node_id=" + settings.NodeID,
		"federation_id=" + settings.FederationID,
		"peer_url=" + settings.AdvertiseURL,
	}
}

func instanceLabel(nodeID string) string {
	hash := sha256.Sum256([]byte(nodeID))
	return "node-" + hex.EncodeToString(hash[:28])
}

func instanceName(nodeID string) string {
	return instanceLabel(nodeID) + "." + serviceName + "." + serviceDomain
}

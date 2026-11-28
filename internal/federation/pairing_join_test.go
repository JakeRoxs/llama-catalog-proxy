package federation

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestJoinExpectedRejectsDiscoveredIdentityBeforeEnrollment(t *testing.T) {
	ca, caKey := newRevocationTestCA(t, 1)
	clientCertificate := newPairingJoinCertificate(t, ca, caKey, "node-a", nil)
	serverCertificate := newPairingJoinCertificate(t, ca, caKey, "node-b", []net.IP{net.ParseIP("127.0.0.1")})
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	now := time.Now().UTC()
	crl := newRevocationTestCRL(t, ca, caKey, 1, now.Add(-time.Minute), now.Add(time.Hour))

	var enrollRequests atomic.Int64
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		enrollRequests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	server.TLS = &tls.Config{
		MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{serverCertificate},
		ClientAuth: tls.RequestClientCert,
	}
	server.StartTLS()
	defer server.Close()

	_, store := pairingStoreFixture(t)
	invitation, err := store.CreateInvitation("node-c", now)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := store.JoinExpected(ctx, server.URL, JoinExpectedOptions{
		ExpectedNodeID: "node-c",
		Code:           invitation.Code,
		Certificate:    clientCertificate,
		Roots:          roots,
		CRLs:           []*x509.RevocationList{crl},
		MaxCRLAge:      24 * time.Hour,
	}); err == nil {
		t.Fatal("pairing accepted a TLS identity different from the selected candidate")
	}
	if enrollRequests.Load() != 0 {
		t.Fatalf("enrollment endpoint received %d requests before node identity match", enrollRequests.Load())
	}
	if store.IsMember("node-b") {
		t.Fatal("identity-mismatched candidate was added to membership")
	}
}

func TestJoinExpectedPairsWithValidPeer(t *testing.T) {
	ca, caKey := newRevocationTestCA(t, 1)
	clientCertificate := newPairingJoinCertificate(t, ca, caKey, "node-a", nil)
	serverCertificate := newPairingJoinCertificate(t, ca, caKey, "node-b", []net.IP{net.ParseIP("127.0.0.1")})
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	now := time.Now().UTC()
	crl := newRevocationTestCRL(t, ca, caKey, 1, now.Add(-time.Minute), now.Add(time.Hour))

	var enrollRequests atomic.Int64
	var inviteHeader string
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		enrollRequests.Add(1)
		inviteHeader = r.Header.Get("X-Llama-Catalog-Invite")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"state":"paired","federation_id":"lab.example","local_node_id":"node-b","node_id":"node-a","paired_at":"2026-11-16T12:00:00Z"}`))
	}))
	server.TLS = &tls.Config{
		MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{serverCertificate},
		ClientAuth: tls.RequestClientCert, ClientCAs: roots,
	}
	server.StartTLS()
	defer server.Close()

	_, store := pairingStoreFixture(t)
	invitation, err := store.CreateInvitation("node-b", now)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	member, err := store.JoinExpected(ctx, server.URL, JoinExpectedOptions{
		ExpectedNodeID: "node-b",
		Code:           invitation.Code,
		Certificate:    clientCertificate,
		Roots:          roots,
		CRLs:           []*x509.RevocationList{crl},
		MaxCRLAge:      24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("join expected = %v", err)
	}
	if enrollRequests.Load() != 1 {
		t.Fatalf("enrollment endpoint received %d requests", enrollRequests.Load())
	}
	if inviteHeader != invitation.Code {
		t.Fatalf("invite header = %q", inviteHeader)
	}
	if member.NodeID != "node-b" {
		t.Fatalf("member = %+v", member)
	}
	if !store.IsMember("node-b") {
		t.Fatal("peer was not added to membership")
	}
}

func newPairingJoinCertificate(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, nodeID string, ips []net.IP) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	uri, err := url.Parse(PeerIdentityURI("lab.example", nodeID))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(now.UnixNano()), Subject: pkix.Name{CommonName: nodeID},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(29 * 24 * time.Hour), URIs: []*url.URL{uri},
		IPAddresses: ips, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	)
	if err != nil {
		t.Fatal(err)
	}
	return certificate
}

// TestValidEndpointRejectsPrivateReservedIPs ensures validEndpoint rejects peer
// endpoints whose host resolves to a private or reserved IP address, preventing
// server-side request forgery to internal services (metadata endpoints,
// link-local services, RFC 1918 networks, multicast). Loopback and public hosts
// are permitted.
func TestValidEndpointRejectsPrivateReservedIPs(t *testing.T) {
	rejectEndpoints := []string{
		"https://10.0.0.5",
		"https://172.16.0.1",
		"https://192.168.1.1",
		"https://169.254.169.254",
		"https://0.0.0.0",
		"https://224.0.0.1",
		"https://[fe80::1]",
		"https://[ff02::1]",
		"https://240.0.0.1",
		"https://100.64.0.1",
		"https://198.51.100.1",
		"https://192.0.2.1",
		"https://203.0.113.1",
	}
	for _, ep := range rejectEndpoints {
		_, err := validEndpoint(ep)
		if err == nil {
			t.Fatalf("validEndpoint(%q) should reject private/reserved IP", ep)
		}
	}

	allowEndpoints := []string{
		"https://127.0.0.1",
		"https://127.0.0.1:12345",
		"https://[::1]",
		"https://[::1]:12345",
		"https://1.2.3.4",
		"https://example.com",
		"https://host-1.example.com",
		"https://example.com:443",
	}
	for _, ep := range allowEndpoints {
		_, err := validEndpoint(ep)
		if err != nil {
			t.Fatalf("validEndpoint(%q) should allow loopback/public/hostname, got: %v", ep, err)
		}
	}
}

// TestValidEndpointRejectsInvalidHostnames ensures validEndpoint rejects
// syntactically invalid hostnames.
func TestValidEndpointRejectsInvalidHostnames(t *testing.T) {
	rejectEndpoints := []string{
		"https://-host.example.com",
		"https://host-.example.com",
		"https://host..example.com",
		"https://.host.example.com",
		"https://host.example.com.",
		"https://host example.com",
		"https://",
	}
	for _, ep := range rejectEndpoints {
		_, err := validEndpoint(ep)
		if err == nil {
			t.Fatalf("validEndpoint(%q) should reject invalid hostname", ep)
		}
	}
}

// TestJoinExpectedRejectsPrivateEndpoint ensures JoinExpected rejects peer
// endpoints whose host is a private or reserved IP address before any network
// connection is attempted.
func TestJoinExpectedRejectsPrivateEndpoint(t *testing.T) {
	ca, caKey := newRevocationTestCA(t, 1)
	clientCertificate := newPairingJoinCertificate(t, ca, caKey, "node-a", nil)
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	now := time.Now().UTC()
	crl := newRevocationTestCRL(t, ca, caKey, 1, now.Add(-time.Minute), now.Add(time.Hour))

	_, store := pairingStoreFixture(t)
	invitation, err := store.CreateInvitation("node-b", now)
	if err != nil {
		t.Fatal(err)
	}

	privateEndpoints := []string{
		"https://10.0.0.5",
		"https://169.254.169.254",
		"https://[fe80::1]",
		"https://0.0.0.0",
	}
	for _, ep := range privateEndpoints {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := store.JoinExpected(ctx, ep, JoinExpectedOptions{
			ExpectedNodeID: "node-b",
			Code:           invitation.Code,
			Certificate:    clientCertificate,
			Roots:          roots,
			CRLs:           []*x509.RevocationList{crl},
			MaxCRLAge:      24 * time.Hour,
		})
		if err == nil {
			t.Fatalf("JoinExpected accepted private endpoint %q", ep)
		}
	}
}

// TestValidEndpointIP verifies validEndpointIP allows public addresses and
// rejects private/reserved addresses.
func TestValidEndpointIP(t *testing.T) {
	for _, ipStr := range []string{"1.2.3.4", "8.8.8.8"} {
		ip := net.ParseIP(ipStr)
		if !validEndpointIP(ip) {
			t.Fatalf("validEndpointIP(%q) should allow public address", ipStr)
		}
	}
	for _, ipStr := range []string{"10.0.0.5", "172.16.0.1", "192.168.1.1", "169.254.169.254", "0.0.0.0", "224.0.0.1", "240.0.0.1", "100.64.0.1", "192.0.2.1", "198.51.100.1", "203.0.113.1", "127.0.0.1", "::1", "fe80::1", "2001:db8::1", "2001::1", "2002::1"} {
		ip := net.ParseIP(ipStr)
		if validEndpointIP(ip) {
			t.Fatalf("validEndpointIP(%q) should reject private/reserved address", ipStr)
		}
	}
}

// TestValidEndpointHostIPv6Documentation verifies validEndpointHost rejects
// literal IPv6 documentation addresses (2001::/23, 2002::/16, 2001:db8::/32)
// as endpoint hosts, consistent with the dial-time policy.
func TestValidEndpointHostIPv6Documentation(t *testing.T) {
	for _, host := range []string{"2001:db8::1", "2001::1", "2002::1"} {
		if validEndpointHost(host) {
			t.Fatalf("validEndpointHost(%q) should reject IPv6 documentation address", host)
		}
	}
}

// TestValidEndpointHostRejectsPrivateReservedIPs verifies validEndpointHost
// rejects literal private/reserved IP addresses while allowing public IPs and
// loopback (loopback is allowed by validEndpointHost and handled separately at
// dial time).
func TestValidEndpointHostRejectsPrivateReservedIPs(t *testing.T) {
	for _, host := range []string{"10.0.0.5", "172.16.0.1", "192.168.1.1", "169.254.169.254", "0.0.0.0", "224.0.0.1", "240.0.0.1", "100.64.0.1", "192.0.2.1", "198.51.100.1", "203.0.113.1", "fe80::1"} {
		if validEndpointHost(host) {
			t.Fatalf("validEndpointHost(%q) should reject private/reserved address", host)
		}
	}
	for _, host := range []string{"1.2.3.4", "8.8.8.8"} {
		if !validEndpointHost(host) {
			t.Fatalf("validEndpointHost(%q) should allow public address", host)
		}
	}
}

// TestValidEndpointHostValidatesHostnames verifies validEndpointHost accepts
// syntactically valid hostnames and rejects invalid ones.
func TestValidEndpointHostValidatesHostnames(t *testing.T) {
	for _, host := range []string{"example.com", "join.example.local", "peer.example.org"} {
		if !validEndpointHost(host) {
			t.Fatalf("validEndpointHost(%q) should allow valid hostname", host)
		}
	}
	for _, host := range []string{"", "example..com", "-example.com", "example-.com", "example.com.", "example.com:443", "example.com/path"} {
		if validEndpointHost(host) {
			t.Fatalf("validEndpointHost(%q) should reject invalid hostname", host)
		}
	}
}

// TestValidHostname verifies validHostname accepts syntactically valid RFC 1123
// hostnames and rejects invalid ones, including label length and hyphen
// boundary rules, without resolving DNS.
func TestValidHostname(t *testing.T) {
	accept := []string{
		"example.com",
		"join.example.local",
		"peer.example.org",
		"a",
		"a.b",
		"host-1.example.com",
		"a-b.c-d",
		strings.Repeat("a", 63) + "." + strings.Repeat("a", 63) + "." + strings.Repeat("a", 63) + "." + strings.Repeat("a", 61),
	}
	for _, host := range accept {
		if !validHostname(host) {
			t.Fatalf("validHostname(%q) should accept valid hostname", host)
		}
	}
	reject := []string{
		"",
		"-host.example.com",
		"host-.example.com",
		"host..example.com",
		".host.example.com",
		"host.example.com.",
		"host example.com",
		"host_example.com",
		"host.example.com:443",
		"host/example.com",
		"a-.",
		".-a",
		strings.Repeat("a", 64),
		strings.Repeat("a", 63) + "." + strings.Repeat("a", 63) + "." + strings.Repeat("a", 63) + "." + strings.Repeat("a", 62),
	}
	for _, host := range reject {
		if validHostname(host) {
			t.Fatalf("validHostname(%q) should reject invalid hostname", host)
		}
	}
}

// TestValidEndpointLANIP verifies validEndpointLANIP matches the
// internal/discovery.usableAddress policy: private RFC 1918/ULA addresses that
// are global-unicast are permitted for .local mDNS/LAN peers; loopback,
// link-local, multicast, unspecified, reserved, and documentation addresses are
// rejected; public addresses are not LAN addresses.
func TestValidEndpointLANIP(t *testing.T) {
	permitted := []struct{ name, ip string }{
		{"RFC 1918 10.0.0.5", "10.0.0.5"},
		{"RFC 1918 172.16.0.1", "172.16.0.1"},
		{"RFC 1918 192.168.1.1", "192.168.1.1"},
		{"ULA fc00::1", "fc00::1"},
		{"ULA fd00::1", "fd00::1"},
	}
	rejected := []struct{ name, ip string }{
		{"loopback 127.0.0.1", "127.0.0.1"},
		{"loopback ::1", "::1"},
		{"link-local 169.254.169.254", "169.254.169.254"},
		{"link-local fe80::1", "fe80::1"},
		{"multicast 224.0.0.1", "224.0.0.1"},
		{"multicast ff02::1", "ff02::1"},
		{"unspecified 0.0.0.0", "0.0.0.0"},
		{"unspecified ::", "::"},
		{"reserved 240.0.0.1", "240.0.0.1"},
		{"shared address space 100.64.0.1", "100.64.0.1"},
		{"TEST-NET-2 198.51.100.1", "198.51.100.1"},
		{"TEST-NET-1 192.0.2.1", "192.0.2.1"},
		{"TEST-NET-3 203.0.113.1", "203.0.113.1"},
		{"IPv6 doc 2001:db8::1", "2001:db8::1"},
		{"IPv6 doc 2001::1", "2001::1"},
		{"IPv6 doc 2002::1", "2002::1"},
		{"public 1.2.3.4", "1.2.3.4"},
		{"public 8.8.8.8", "8.8.8.8"},
	}

	for _, tc := range permitted {
		t.Run(tc.name, func(t *testing.T) {
			assertEndpointLANIP(t, tc.name, tc.ip, true)
		})
	}

	for _, tc := range rejected {
		t.Run(tc.name, func(t *testing.T) {
			assertEndpointLANIP(t, tc.name, tc.ip, false)
		})
	}
}

func assertEndpointLANIP(t *testing.T, name, ipString string, want bool) {
	t.Helper()
	got := validEndpointLANIP(net.ParseIP(ipString))
	if got != want {
		t.Fatalf("validEndpointLANIP(%q) = %t, want %t (%s)", ipString, got, want, name)
	}
}

// TestIsReservedOrDocumentationRange verifies the shared reserved/documentation
// range predicate rejects the reserved IPv4 range 240.0.0.0/4, the RFC 5737
// documentation ranges (TEST-NET-1/2/3), and IPv6 documentation ranges, while
// allowing addresses immediately outside each range. It mirrors the policy
// applied by validEndpointHost, validEndpointIP and validEndpointLANIP.
func TestIsReservedOrDocumentationRange(t *testing.T) {
	cases := []struct {
		name string
		ip   string
		want bool
	}{
		// Reserved IPv4 range 240.0.0.0/4.
		{"below reserved 240.0.0.0/4", "239.255.255.255", false},
		{"reserved 240.0.0.0", "240.0.0.0", true},
		{"reserved 240.0.0.1", "240.0.0.1", true},
		{"reserved 255.255.255.255", "255.255.255.255", true},
		// Shared address space 100.64.0.0/10.
		{"below shared 100.64.0.0/10", "100.63.255.255", false},
		{"shared 100.64.0.0", "100.64.0.0", true},
		{"shared 100.64.0.1", "100.64.0.1", true},
		{"shared 100.127.255.255", "100.127.255.255", true},
		{"above shared 100.64.0.0/10", "100.128.0.0", false},
		// TEST-NET-2 198.51.100.0/24.
		{"below TEST-NET-2", "198.51.99.255", false},
		{"TEST-NET-2 198.51.100.0", "198.51.100.0", true},
		{"TEST-NET-2 198.51.100.1", "198.51.100.1", true},
		{"TEST-NET-2 198.51.100.255", "198.51.100.255", true},
		{"above TEST-NET-2", "198.51.101.0", false},
		// TEST-NET-1 192.0.2.0/24.
		{"below TEST-NET-1", "192.0.1.255", false},
		{"TEST-NET-1 192.0.2.0", "192.0.2.0", true},
		{"TEST-NET-1 192.0.2.1", "192.0.2.1", true},
		{"TEST-NET-1 192.0.2.255", "192.0.2.255", true},
		{"above TEST-NET-1", "192.0.3.0", false},
		// TEST-NET-3 203.0.113.0/24.
		{"below TEST-NET-3", "203.0.112.255", false},
		{"TEST-NET-3 203.0.113.0", "203.0.113.0", true},
		{"TEST-NET-3 203.0.113.1", "203.0.113.1", true},
		{"TEST-NET-3 203.0.113.255", "203.0.113.255", true},
		{"above TEST-NET-3", "203.0.114.0", false},
		// IPv6 documentation ranges.
		{"IPv6 doc 2001::1", "2001::1", true},
		{"IPv6 doc 2002::1", "2002::1", true},
		{"IPv6 doc 2001:db8::1", "2001:db8::1", true},
		{"IPv6 non-doc 2001:0200::1", "2001:0200::1", false},
		{"IPv6 non-doc 2003::1", "2003::1", false},
		{"IPv6 non-doc 2001:db9::1", "2001:db9::1", false},
		// Addresses handled by other policy branches, not this helper.
		{"public 1.2.3.4", "1.2.3.4", false},
		{"public 8.8.8.8", "8.8.8.8", false},
		{"private 10.0.0.5", "10.0.0.5", false},
		{"loopback 127.0.0.1", "127.0.0.1", false},
		{"link-local fe80::1", "fe80::1", false},
		{"multicast 224.0.0.1", "224.0.0.1", false},
		{"unspecified 0.0.0.0", "0.0.0.0", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ip := net.ParseIP(tc.ip)
			if ip == nil {
				t.Fatalf("could not parse %q", tc.ip)
			}
			if got := isReservedOrDocumentationRange(ip); got != tc.want {
				t.Fatalf("isReservedOrDocumentationRange(%q) = %t, want %t", tc.ip, got, tc.want)
			}
		})
	}
}

// TestValidEndpointLiteralIPs verifies validEndpoint rejects literal private/reserved
// IP addresses in the endpoint URL while allowing public IPs and loopback.
func TestValidEndpointLiteralIPs(t *testing.T) {
	for _, ep := range []string{"https://1.2.3.4", "https://1.2.3.4:443", "https://2001:4864:4864:8888:8888:8888:8888:8888:8888"} {
		_, err := validEndpoint(ep)
		if err != nil {
			t.Fatalf("validEndpoint(%q) should allow public IP, got: %v", ep, err)
		}
	}
	for _, ep := range []string{"https://10.0.0.5", "https://172.16.0.1", "https://192.168.1.1", "https://169.254.169.254", "https://0.0.0.0", "https://224.0.0.1", "https://100.64.0.1", "https://192.0.2.1", "https://198.51.100.1", "https://203.0.113.1", "https://[fe80::1]"} {
		_, err := validEndpoint(ep)
		if err == nil {
			t.Fatalf("validEndpoint(%q) should reject private/reserved IP", ep)
		}
	}
	for _, ep := range []string{"https://127.0.0.1", "https://[::1]"} {
		_, err := validEndpoint(ep)
		if err != nil {
			t.Fatalf("validEndpoint(%q) should allow loopback, got: %v", ep, err)
		}
	}
}

// TestJoinDialContextLoopback verifies the dial-time policy allows loopback
// destinations (local development and testing) by dialing the resolved IP.
func TestJoinDialContextLoopback(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		t.Fatal(err)
	}

	_, store := pairingStoreFixture(t)
	conn, err := store.joinDialContext(context.Background(), "tcp", net.JoinHostPort(host, port))
	if err != nil {
		t.Fatalf("joinDialContext with loopback should succeed, got: %v", err)
	}
	defer conn.Close()
}

// TestJoinDialContextUnknownHost verifies the dial-time policy rejects hosts
// that cannot be resolved.
func TestJoinDialContextUnknownHost(t *testing.T) {
	_, store := pairingStoreFixture(t)
	_, err := store.joinDialContext(context.Background(), "tcp", "nonexistent.invalid.example:443")
	if err == nil {
		t.Fatal("joinDialContext with unknown host should fail")
	}
}

// TestJoinExpectedRejectsLiteralPrivateIP verifies JoinExpected rejects a
// literal private IP endpoint before any network connection is attempted.
func TestJoinExpectedRejectsLiteralPrivateIP(t *testing.T) {
	ca, caKey := newRevocationTestCA(t, 1)
	clientCertificate := newPairingJoinCertificate(t, ca, caKey, "node-a", nil)
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	now := time.Now().UTC()
	crl := newRevocationTestCRL(t, ca, caKey, 1, now.Add(-time.Minute), now.Add(time.Hour))

	_, store := pairingStoreFixture(t)
	invitation, err := store.CreateInvitation("node-b", now)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = store.JoinExpected(ctx, "https://10.0.0.5", JoinExpectedOptions{
		ExpectedNodeID: "node-b",
		Code:           invitation.Code,
		Certificate:    clientCertificate,
		Roots:          roots,
		CRLs:           []*x509.RevocationList{crl},
		MaxCRLAge:      24 * time.Hour,
	})
	if err == nil {
		t.Fatal("JoinExpected accepted literal private IP endpoint")
	}
}

// TestJoinDialContextNonLocalRejectsPrivateIP verifies that a non-.local
// hostname resolving to a private IP is rejected at dial time and never
// dials the private address.
func TestJoinDialContextNonLocalRejectsPrivateIP(t *testing.T) {
	_, store := pairingStoreFixture(t)
	store.lookupNetIP = func(ctx context.Context, netw, name string) ([]net.IP, error) {
		return []net.IP{net.ParseIP("10.0.0.5")}, nil
	}
	_, err := store.joinDialContext(context.Background(), "tcp", "join.example.com:443")
	if err == nil {
		t.Fatal("joinDialContext accepted non-.local hostname resolving to private IP")
	}
}

// TestJoinDialContextLocalRejectsNonLANIP verifies that a .local hostname
// resolving to a non-LAN IP (link-local) is rejected at dial time.
func TestJoinDialContextLocalRejectsNonLANIP(t *testing.T) {
	_, store := pairingStoreFixture(t)
	store.lookupNetIP = func(ctx context.Context, netw, name string) ([]net.IP, error) {
		return []net.IP{net.ParseIP("169.254.169.254")}, nil
	}
	_, err := store.joinDialContext(context.Background(), "tcp", "join.test.local:443")
	if err == nil {
		t.Fatal("joinDialContext accepted .local hostname resolving to non-LAN IP")
	}
}

// TestJoinDialContextLocalHostnameAllowsLoopback verifies that a .local
// hostname resolving to loopback is allowed by the explicit loopback
// development/testing policy and dials the resolved IP directly.
func TestJoinDialContextLocalHostnameAllowsLoopback(t *testing.T) {
	_, store := pairingStoreFixture(t)
	store.lookupNetIP = func(ctx context.Context, netw, name string) ([]net.IP, error) {
		return []net.IP{net.ParseIP("127.0.0.1")}, nil
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		t.Fatal(err)
	}

	conn, err := store.joinDialContext(context.Background(), "tcp", net.JoinHostPort("join.test.local", port))
	if err != nil {
		t.Fatalf("joinDialContext with .local hostname resolving to loopback should succeed, got: %v", err)
	}
	defer conn.Close()
}

// TestJoinDialContextPinsDNS verifies that the resolved DNS IP is dialed
// directly and the resolver is consulted exactly once (no re-resolution /
// rebinding).
func TestJoinDialContextPinsDNS(t *testing.T) {
	_, store := pairingStoreFixture(t)
	var callCount int
	store.lookupNetIP = func(ctx context.Context, netw, name string) ([]net.IP, error) {
		callCount++
		return []net.IP{net.ParseIP("127.0.0.1")}, nil
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		t.Fatal(err)
	}

	conn, err := store.joinDialContext(context.Background(), "tcp", net.JoinHostPort("join.test.local", port))
	if err != nil {
		t.Fatalf("joinDialContext should succeed, got: %v", err)
	}
	defer conn.Close()

	if callCount != 1 {
		t.Fatalf("joinDialContext should consult the resolver exactly once (pinning), got %d calls", callCount)
	}
}

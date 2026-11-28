package federation

import (
	"bytes"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/identity"
)

const (
	maxTrustBundleSize = 1 << 20

	// nodeIdentityPathPrefix is the SPIFFE peer identity URI path prefix for a
	// federated node. It is defined once here and referenced by URI construction
	// and prefix validation so the "/node/" literal is never duplicated.
	nodeIdentityPathPrefix = "/node/"
)

func LoadTrustRoots(path string) (*x509.CertPool, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, ErrPairingStore
	}
	data, readErr := io.ReadAll(io.LimitReader(file, maxTrustBundleSize+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil || len(data) == 0 || len(data) > maxTrustBundleSize {
		return nil, ErrPairingStore
	}
	roots := x509.NewCertPool()
	remaining := bytes.TrimSpace(data)
	for len(remaining) > 0 {
		block, rest := pem.Decode(remaining)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return nil, ErrPairingStore
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil || !cert.IsCA || !cert.BasicConstraintsValid ||
			(cert.KeyUsage != 0 && cert.KeyUsage&x509.KeyUsageCertSign == 0) {
			return nil, ErrPairingStore
		}
		roots.AddCert(cert)
		remaining = bytes.TrimSpace(rest)
	}
	if len(roots.Subjects()) == 0 {
		return nil, ErrPairingStore
	}
	return roots, nil
}

func ValidateIdentityCertificate(cert *x509.Certificate, federationID, nodeID string) error {
	if cert == nil || cert.IsCA || len(cert.URIs) != 1 || cert.URIs[0].String() != PeerIdentityURI(federationID, nodeID) {
		return ErrInvitation
	}
	now := time.Now()
	if now.Before(cert.NotBefore) || now.After(cert.NotAfter) {
		return ErrInvitation
	}
	if ValidateFederationCertificateLifetime(cert) != nil {
		return ErrInvitation
	}
	hasClientAuth, hasServerAuth := false, false
	for _, usage := range cert.ExtKeyUsage {
		hasClientAuth = hasClientAuth || usage == x509.ExtKeyUsageClientAuth
		hasServerAuth = hasServerAuth || usage == x509.ExtKeyUsageServerAuth
	}
	if !hasClientAuth || !hasServerAuth {
		return ErrInvitation
	}
	return nil
}

func PeerIdentityURI(federationID, nodeID string) string {
	return "spiffe://" + federationID + nodeIdentityPathPrefix + nodeID
}

func PeerIdentityNodeID(cert *x509.Certificate, federationID string) (string, error) {
	if cert == nil || cert.IsCA || len(cert.URIs) != 1 {
		return "", ErrInvitation
	}
	uri := cert.URIs[0]
	if uri.Scheme != "spiffe" || uri.Host != federationID || uri.User != nil || uri.Opaque != "" ||
		uri.RawQuery != "" || uri.Fragment != "" || uri.RawPath != "" || !strings.HasPrefix(uri.Path, nodeIdentityPathPrefix) {
		return "", ErrInvitation
	}
	nodeID := strings.TrimPrefix(uri.Path, nodeIdentityPathPrefix)
	if !identity.ValidNodeID(nodeID) || uri.Path != nodeIdentityPathPrefix+nodeID || ValidateIdentityCertificate(cert, federationID, nodeID) != nil {
		return "", ErrInvitation
	}
	return nodeID, nil
}

func CertificateFingerprint(cert *x509.Certificate) string {
	if cert == nil {
		return ""
	}
	hash := sha256.Sum256(cert.Raw)
	return hex.EncodeToString(hash[:])
}

func VerifyPeerClientIdentity(certificates []*x509.Certificate, roots *x509.CertPool, crls []*x509.RevocationList, maxCRLAge time.Duration, federationID, nodeID string) error {
	if len(certificates) == 0 || roots == nil {
		return ErrInvitation
	}
	intermediates := x509.NewCertPool()
	for _, cert := range certificates[1:] {
		intermediates.AddCert(cert)
	}
	chains, err := certificates[0].Verify(x509.VerifyOptions{
		Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})
	if err != nil || VerifyCertificateRevocation(chains, crls, maxCRLAge, time.Now()) != nil {
		return ErrInvitation
	}
	return ValidateIdentityCertificate(certificates[0], federationID, nodeID)
}

// ValidateJoinOrigin checks whether an origin is usable for peer enrollment.
func ValidateJoinOrigin(value string) error {
	_, err := validEndpoint(value)
	return err
}

func validEndpoint(value string) (string, error) {
	target, err := url.Parse(value)
	if err != nil || len(value) > 2048 || target.Scheme != "https" || target.Hostname() == "" || target.User != nil ||
		target.RawQuery != "" || target.Fragment != "" || (target.Path != "" && target.Path != "/") ||
		target.RawPath != "" || target.ForceQuery || strings.TrimSpace(value) != value {
		return "", errors.New("invalid peer endpoint")
	}
	// Reject private/reserved IP addresses to prevent server-side request
	// forgery to internal services (metadata endpoints, link-local services,
	// RFC 1918 networks, multicast). Loopback is permitted for local
	// development and testing; the mTLS peer certificate verification
	// performed by the join transport protects the data channel even for
	// loopback destinations. DNS rebinding and host-based SSRF are mitigated
	// by the dial-time resolve-and-pin policy in joinDialContext, which
	// validates each resolved IP and dials the resolved IP directly.
	if !validEndpointHost(target.Hostname()) {
		return "", errors.New("invalid peer endpoint")
	}
	target.Path = ""
	return strings.TrimSuffix(target.String(), "/"), nil
}

// validEndpointHost reports whether host is safe to dial as a peer endpoint.
// IP addresses in private/reserved ranges (RFC 1918, RFC 1000 link-local,
// RFC 5737 documentation, IPv6 link-local, multicast, unspecified, the
// reserved IPv4 range 240.0.0.0/4, and IPv6 documentation ranges
// 2001::/23, 2002::/16, 2001:db8::/32) are rejected to prevent SSRF to
// internal services. Loopback is allowed for local development and testing.
// Hostnames are validated by format only; DNS rebinding and host-based SSRF
// are mitigated by the dial-time resolve-and-pin policy in joinDialContext.
func validEndpointHost(host string) bool {
	if ip := net.ParseIP(host); ip != nil {
		if ip.IsPrivate() || ip.IsUnspecified() || ip.IsLinkLocalUnicast() || ip.IsMulticast() {
			return false
		}
		if isReservedOrDocumentationRange(ip) {
			return false
		}
		return true
	}
	return validHostname(host)
}

// validHostname reports whether host is a syntactically valid RFC 1123
// hostname. It does not resolve DNS.
func validHostname(host string) bool {
	if host == "" || len(host) > 253 {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if !validHostnameLabel(label) {
			return false
		}
	}
	return true
}

// validHostnameLabel reports whether label is a syntactically valid RFC 1123
// hostname label: one to 63 characters, each a letter, digit, or hyphen, with
// hyphens permitted only in the interior (never the first or last character).
func validHostnameLabel(label string) bool {
	if len(label) < 1 || len(label) > 63 {
		return false
	}
	for _, c := range label {
		if !isHostnameRune(c) {
			return false
		}
	}
	return label[0] != '-' && label[len(label)-1] != '-'
}

// isHostnameRune reports whether c is a character permitted in an RFC 1123
// hostname label: a letter, a digit, or a hyphen.
func isHostnameRune(c rune) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-'
}

// validEndpointIP reports whether ip is a public address safe to dial as a peer
// endpoint destination for non-.local hostnames. Private/reserved addresses
// (RFC 1918, RFC 1000 link-local, RFC 5737 documentation, IPv6 link-local,
// multicast, unspecified, reserved 240.0.0.0/4, and IPv6 documentation
// 2001::/23, 2002::/16, 2001:db8::/32) are rejected to prevent SSRF to
// internal services. Loopback is not considered public here; it is handled
// separately at dial time to preserve local development and testing.
func validEndpointIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if !ip.IsGlobalUnicast() || ip.IsPrivate() {
		return false
	}
	if isReservedOrDocumentationRange(ip) {
		return false
	}
	return true
}

// validEndpointLANIP reports whether ip is a valid LAN address for an mDNS/LAN
// peer endpoint, matching internal/discovery.usableAddress: private RFC 1918/
// ULA addresses that are global-unicast, excluding loopback, link-local,
// multicast, unspecified, reserved, and documentation addresses. This is the
// allowlist applied to resolved IPs when the endpoint hostname ends in .local.
func validEndpointLANIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	// Match discovery.usableAddress: private AND global-unicast, excluding
	// loopback, link-local, multicast, and unspecified.
	if !ip.IsPrivate() || !ip.IsGlobalUnicast() {
		return false
	}
	if ip.IsLoopback() || ip.IsMulticast() || ip.IsUnspecified() {
		return false
	}
	if isReservedOrDocumentationRange(ip) {
		return false
	}
	return true
}

// isReservedOrDocumentationRange reports whether ip is in a reserved or
// documentation address range that must not be used as a peer endpoint
// destination. It covers the reserved IPv4 range 240.0.0.0/4 and the RFC 5737
// documentation ranges (TEST-NET-1/2/3: 100.64.0.0/10, 198.51.100.0/24,
// 192.0.2.0/24, 203.0.113.0/24) and the IPv6 documentation ranges
// (2001::/23, 2002::/16, 2001:db8::/32), consistent with the special-range
// policy in internal/ingestion/download.go. It is shared by the endpoint IP
// policies so the identical allow/deny decision is made in one place.
func isReservedOrDocumentationRange(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if ip4 := ip.To4(); ip4 != nil {
		if ip4[0] >= 240 ||
			(ip4[0] == 100 && ip4[1] >= 64 && ip4[1] <= 127) ||
			(ip4[0] == 198 && ip4[1] == 51 && ip4[2] == 100) ||
			(ip4[0] == 192 && ip4[1] == 0 && ip4[2] == 2) ||
			(ip4[0] == 203 && ip4[1] == 0 && ip4[2] == 113) {
			return true
		}
	}
	return isDocumentationIPv6(ip)
}

// isDocumentationIPv6 reports whether ip is an IPv6 documentation address
// (2001::/23, 2002::/16, 2001:db8::/32), consistent with the special-range
// policy in internal/ingestion/download.go.
func isDocumentationIPv6(ip net.IP) bool {
	if len(ip) != 16 {
		return false
	}
	// 2001::/23: first 23 bits are 00100000 00000001 0000000.
	// Byte 3's first 7 bits must be zero; byte 3 is 0x00 or 0x01.
	if ip[0] == 0x20 && ip[1] == 0x01 && ip[2] <= 0x01 {
		return true
	}
	// 2002::/16: 00100000 00000010
	if ip[0] == 0x20 && ip[1] == 0x02 {
		return true
	}
	// 2001:db8::/32: 00100000 00000001 00001101 10111000
	if ip[0] == 0x20 && ip[1] == 0x01 && ip[2] == 0x0d && ip[3] == 0xb8 {
		return true
	}
	return false
}

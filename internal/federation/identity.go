package federation

import (
	"bytes"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"io"
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

func validEndpoint(value string) (string, error) {
	target, err := url.Parse(value)
	if err != nil || len(value) > 2048 || target.Scheme != "https" || target.Hostname() == "" || target.User != nil ||
		target.RawQuery != "" || target.Fragment != "" || (target.Path != "" && target.Path != "/") ||
		target.RawPath != "" || target.ForceQuery || strings.TrimSpace(value) != value {
		return "", errors.New("invalid peer endpoint")
	}
	target.Path = ""
	return strings.TrimSuffix(target.String(), "/"), nil
}

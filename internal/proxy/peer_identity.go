package proxy

import (
	"crypto/x509"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/federation"
)

func peerCertificateFingerprint(cert *x509.Certificate) string {
	return federation.CertificateFingerprint(cert)
}

func peerIdentityURI(federationID, nodeID string) string {
	return federation.PeerIdentityURI(federationID, nodeID)
}

func validatePeerIdentityCertificate(cert *x509.Certificate, federationID, nodeID string) error {
	return federation.ValidateIdentityCertificate(cert, federationID, nodeID)
}

func peerIdentityNodeID(cert *x509.Certificate, federationID string) (string, error) {
	return federation.PeerIdentityNodeID(cert, federationID)
}

func verifyPeerClientIdentity(certificates []*x509.Certificate, roots *x509.CertPool, crls []*x509.RevocationList, maxCRLAge time.Duration, federationID, nodeID string) error {
	return federation.VerifyPeerClientIdentity(certificates, roots, crls, maxCRLAge, federationID, nodeID)
}

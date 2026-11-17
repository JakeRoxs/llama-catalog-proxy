package federation

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newRevocationTestCA(t *testing.T, serial int64) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "revocation test CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(48 * time.Hour), IsCA: true,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert, key
}

func newRevocationTestLeaf(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, serial int64) *x509.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	uri, _ := url.Parse(PeerIdentityURI("lab.example", "node-a"))
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "node-a"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(29 * 24 * time.Hour), URIs: []*url.URL{uri},
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

func newRevocationTestCRL(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, number int64, thisUpdate, nextUpdate time.Time, serials ...*big.Int) *x509.RevocationList {
	t.Helper()
	entries := make([]x509.RevocationListEntry, 0, len(serials))
	for _, serial := range serials {
		entries = append(entries, x509.RevocationListEntry{SerialNumber: serial, RevocationTime: thisUpdate})
	}
	der, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
		Number: big.NewInt(number), ThisUpdate: thisUpdate, NextUpdate: nextUpdate, RevokedCertificateEntries: entries,
	}, ca, caKey)
	if err != nil {
		t.Fatal(err)
	}
	crl, err := x509.ParseRevocationList(der)
	if err != nil {
		t.Fatal(err)
	}
	return crl
}

func TestVerifyCertificateRevocationRequiresFreshIssuerCRLs(t *testing.T) {
	ca, caKey := newRevocationTestCA(t, 1)
	leaf := newRevocationTestLeaf(t, ca, caKey, 7)
	now := time.Now()
	chain := [][]*x509.Certificate{{leaf, ca}}
	fresh := newRevocationTestCRL(t, ca, caKey, 1, now.Add(-time.Minute), now.Add(time.Hour))
	if err := VerifyCertificateRevocation(chain, []*x509.RevocationList{fresh}, 24*time.Hour, now); err != nil {
		t.Fatalf("fresh unrevoked chain rejected: %v", err)
	}
	revoked := newRevocationTestCRL(t, ca, caKey, 2, now.Add(-time.Minute), now.Add(time.Hour), leaf.SerialNumber)
	if err := VerifyCertificateRevocation(chain, []*x509.RevocationList{revoked}, 24*time.Hour, now); err == nil {
		t.Fatal("revoked certificate accepted")
	}
	stale := newRevocationTestCRL(t, ca, caKey, 3, now.Add(-25*time.Hour), now.Add(-time.Hour))
	if err := VerifyCertificateRevocation(chain, []*x509.RevocationList{stale}, 24*time.Hour, now); err == nil {
		t.Fatal("stale CRL accepted")
	}
	future := newRevocationTestCRL(t, ca, caKey, 4, now.Add(10*time.Minute), now.Add(time.Hour))
	if err := VerifyCertificateRevocation(chain, []*x509.RevocationList{future}, 24*time.Hour, now); err == nil {
		t.Fatal("future-dated CRL accepted outside the clock-skew allowance")
	}
	longValidity := newRevocationTestCRL(t, ca, caKey, 5, now.Add(-time.Minute), now.Add(48*time.Hour))
	if err := VerifyCertificateRevocation(chain, []*x509.RevocationList{longValidity}, 24*time.Hour, now); err == nil {
		t.Fatal("CRL validity period longer than policy accepted")
	}
	if err := VerifyCertificateRevocation(chain, nil, 24*time.Hour, now); err == nil {
		t.Fatal("missing issuer CRL accepted")
	}
	otherCA, otherKey := newRevocationTestCA(t, 2)
	wrongSigner := newRevocationTestCRL(t, otherCA, otherKey, 4, now.Add(-time.Minute), now.Add(time.Hour))
	if err := VerifyCertificateRevocation(chain, []*x509.RevocationList{wrongSigner}, 24*time.Hour, now); err == nil {
		t.Fatal("CRL signed by an untrusted issuer accepted")
	}
}

func TestLoadCRLsAndShortCertificateLifetime(t *testing.T) {
	ca, caKey := newRevocationTestCA(t, 1)
	now := time.Now()
	crl := newRevocationTestCRL(t, ca, caKey, 1, now.Add(-time.Minute), now.Add(time.Hour))
	path := filepath.Join(t.TempDir(), "issuer.crl.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: crl.Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadCRLs(path, 24*time.Hour)
	if err != nil || len(loaded) != 1 {
		t.Fatalf("fresh CRL load = %d, %v", len(loaded), err)
	}
	leaf := newRevocationTestLeaf(t, ca, caKey, 8)
	if err := ValidateFederationCertificateLifetime(leaf); err != nil {
		t.Fatalf("short-lived identity certificate rejected: %v", err)
	}
	longLived := *leaf
	longLived.NotAfter = longLived.NotBefore.Add(31 * 24 * time.Hour)
	if err := ValidateFederationCertificateLifetime(&longLived); err == nil {
		t.Fatal("certificate lifetime longer than 30 days accepted")
	}
	expired := *leaf
	expired.NotAfter = time.Now().Add(-time.Minute)
	if err := ValidateIdentityCertificate(&expired, "lab.example", "node-a"); err == nil {
		t.Fatal("expired federation identity certificate accepted")
	}
	stalePath := filepath.Join(t.TempDir(), "stale.crl.pem")
	stale := newRevocationTestCRL(t, ca, caKey, 2, now.Add(-25*time.Hour), now.Add(-time.Hour))
	if err := os.WriteFile(stalePath, pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: stale.Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCRLs(stalePath, 24*time.Hour); err == nil {
		t.Fatal("stale CRL file accepted")
	}
}

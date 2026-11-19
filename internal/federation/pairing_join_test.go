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
	if _, err := store.JoinExpected(ctx, server.URL, "node-c", invitation.Code, clientCertificate, roots, []*x509.RevocationList{crl}, 24*time.Hour); err == nil {
		t.Fatal("pairing accepted a TLS identity different from the selected candidate")
	}
	if enrollRequests.Load() != 0 {
		t.Fatalf("enrollment endpoint received %d requests before node identity match", enrollRequests.Load())
	}
	if store.IsMember("node-b") {
		t.Fatal("identity-mismatched candidate was added to membership")
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

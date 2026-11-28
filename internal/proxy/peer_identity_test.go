package proxy

import (
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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/federation"
)

func newPeerIdentityTestCA(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test federation CA"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), IsCA: true,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	return ca, key, pool
}

func newPeerIdentityTestCRL(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, revoked ...*big.Int) *x509.RevocationList {
	t.Helper()
	now := time.Now()
	entries := make([]x509.RevocationListEntry, 0, len(revoked))
	for _, serial := range revoked {
		entries = append(entries, x509.RevocationListEntry{SerialNumber: serial, RevocationTime: now.Add(-time.Minute)})
	}
	template := &x509.RevocationList{
		Number: big.NewInt(1), ThisUpdate: now.Add(-time.Minute), NextUpdate: now.Add(time.Hour),
		RevokedCertificateEntries: entries,
	}
	der, err := x509.CreateRevocationList(rand.Reader, template, ca, caKey)
	if err != nil {
		t.Fatal(err)
	}
	crl, err := x509.ParseRevocationList(der)
	if err != nil {
		t.Fatal(err)
	}
	return crl
}

func writePeerTrustFiles(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, revoked ...*big.Int) (string, string) {
	t.Helper()
	directory := t.TempDir()
	caPath, crlPath := filepath.Join(directory, "ca.pem"), filepath.Join(directory, "ca.crl.pem")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	crl := newPeerIdentityTestCRL(t, ca, caKey, revoked...)
	if err := os.WriteFile(crlPath, pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: crl.Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	return caPath, crlPath
}

func newPeerIdentityTestPair(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, federationID, nodeID string) (tls.Certificate, *x509.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	uri, err := url.Parse(peerIdentityURI(federationID, nodeID))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(int64(len(nodeID) + 2)), Subject: pkix.Name{CommonName: nodeID},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
		DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, URIs: []*url.URL{uri},
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	pair.Leaf = leaf
	return pair, leaf
}

func writePeerIdentityTestPair(t *testing.T, pair tls.Certificate) (string, string) {
	t.Helper()
	certificatePath := filepath.Join(t.TempDir(), "node-cert.pem")
	keyPath := filepath.Join(filepath.Dir(certificatePath), "node-key.pem")
	keyBytes, err := x509.MarshalPKCS8PrivateKey(pair.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certificatePath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: pair.Certificate[0]}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certificatePath, keyPath
}

func TestPeerIdentityCertificateMustMatchFederationAndNode(t *testing.T) {
	ca, caKey, _ := newPeerIdentityTestCA(t)
	_, cert := newPeerIdentityTestPair(t, ca, caKey, "lab.example", "node-b")
	if err := validatePeerIdentityCertificate(cert, "lab.example", "node-b"); err != nil {
		t.Fatalf("valid identity certificate rejected: %v", err)
	}
	for _, pair := range [][2]string{{"other.example", "node-b"}, {"lab.example", "node-c"}} {
		if err := validatePeerIdentityCertificate(cert, pair[0], pair[1]); err == nil {
			t.Fatalf("identity certificate accepted for federation/node %q/%q", pair[0], pair[1])
		}
	}
}

// assertPeerAuthenticated verifies that authenticatedPeer returns the expected outcome for the
// given request and reloader state, failing the test with msg when it does not. When expectOk is
// true, the result must be successful and the node ID must match expectedNodeID. The failure
// message for a successful-but-wrong result includes the actual node ID and ok value.
func assertPeerAuthenticated(t *testing.T, request *http.Request, state *reloaderState, expectedNodeID string, expectOk bool, msg string) {
	t.Helper()
	nodeID, ok := authenticatedPeer(request, state, peerCatalog)
	if expectOk {
		if !ok || nodeID != expectedNodeID {
			t.Fatalf("%s: %q %v", msg, nodeID, ok)
		}
	} else if ok {
		t.Fatal(msg)
	}
}

// assertPeerGrantResult verifies that authenticatedPeer returns the expected outcome for the
// given request, state, and scope, failing the test with msg when it does not. When expectOk is
// true, the result must be successful and the node ID must match expectedNodeID. The failure
// message for a successful-but-wrong result includes the actual node ID and ok value.
func assertPeerGrantResult(t *testing.T, request *http.Request, state *reloaderState, scope peerScope, expectedNodeID string, expectOk bool, msg string) {
	t.Helper()
	nodeID, ok := authenticatedPeer(request, state, scope)
	if expectOk {
		if !ok || nodeID != expectedNodeID {
			t.Fatalf("%s: %q %v", msg, nodeID, ok)
		}
	} else if ok {
		t.Fatal(msg)
	}
}

type peerRotationContext struct {
	request *http.Request
	state   *reloaderState
	members *federation.PairingStore
	grants  *federation.CatalogGrantStore
	rotated *x509.Certificate
	ca      *x509.Certificate
	now     time.Time
}

// rotateAndRegrant replaces the node-b membership fingerprint with the rotated certificate's
// fingerprint, updates the request TLS state to the rotated certificate, asserts that rotation
// invalidates prior grants, then grants catalog read and inference to the rotated member and
// asserts the re-grant outcomes.
func rotateAndRegrant(t *testing.T, rotation peerRotationContext) {
	t.Helper()
	if _, err := rotation.members.AddMember("node-b", peerCertificateFingerprint(rotation.rotated), rotation.now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	rotation.request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{rotation.rotated}, VerifiedChains: [][]*x509.Certificate{{rotation.rotated, rotation.ca}}}
	if _, ok := authenticatedPeer(rotation.request, rotation.state, peerCatalog); ok {
		t.Fatal("certificate rotation inherited the previous catalog grant")
	}
	if _, ok := authenticatedPeer(rotation.request, rotation.state, peerInference); ok {
		t.Fatal("certificate rotation inherited the previous inference grant")
	}
	rotatedMember, ok, err := rotation.members.LookupMember("node-b")
	if err != nil || !ok {
		t.Fatalf("rotated membership lookup = %+v, %v, %v", rotatedMember, ok, err)
	}
	if _, err := rotation.grants.GrantCatalogRead(rotatedMember, rotation.now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if nodeID, ok := authenticatedPeer(rotation.request, rotation.state, peerCatalog); !ok || nodeID != "node-b" {
		t.Fatalf("re-granted rotated identity denied: %q %v", nodeID, ok)
	}
	if _, err := rotation.grants.GrantInference(rotatedMember, true, false, rotation.now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if nodeID, ok := authenticatedPeer(rotation.request, rotation.state, peerInference); !ok || nodeID != "node-b" {
		t.Fatalf("re-granted rotated inference identity denied: %q %v", nodeID, ok)
	}
}

func TestAuthenticatedPeerRequiresVerifiedMatchingClientIdentity(t *testing.T) {
	ca, caKey, roots := newPeerIdentityTestCA(t)
	_, valid := newPeerIdentityTestPair(t, ca, caKey, "lab.example", "node-b")
	_, wrong := newPeerIdentityTestPair(t, ca, caKey, "lab.example", "node-c")
	membershipDir := t.TempDir()
	if err := os.Chmod(membershipDir, 0o700); err != nil {
		t.Fatal(err)
	}
	members, err := federation.OpenPairingStore(membershipDir, "lab.example", "owner")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = members.Close() })
	if _, err := members.AddMember("node-b", peerCertificateFingerprint(valid), time.Now()); err != nil {
		t.Fatal(err)
	}
	currentCRL := newPeerIdentityTestCRL(t, ca, caKey)
	state := &reloaderState{peer: &config.PeerConfig{
		NodeID: "owner", FederationID: "lab.example", Peers: map[string]config.PeerGrant{
			"node-b": {Token: strings.Repeat("b", 32), CatalogRead: true},
		},
	}, peerClientCAs: roots, peerClientCRLs: []*x509.RevocationList{currentCRL}, pairingStore: members}
	request := httptest.NewRequest(http.MethodGet, "https://peer.example/peer/v1/report", nil)
	request.Header.Set("Authorization", "Bearer "+strings.Repeat("b", 32))
	request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{valid}, VerifiedChains: [][]*x509.Certificate{{valid, ca}}}
	assertPeerAuthenticated(t, request, state, "node-b", true, "verified matching identity rejected")
	state.peerClientCRLs = []*x509.RevocationList{newPeerIdentityTestCRL(t, ca, caKey, valid.SerialNumber)}
	assertPeerAuthenticated(t, request, state, "node-b", false, "CA-revoked certificate remained authorized")
	state.peerClientCRLs = []*x509.RevocationList{currentCRL}
	request.TLS.PeerCertificates[0] = wrong
	request.TLS.VerifiedChains[0][0] = wrong
	assertPeerAuthenticated(t, request, state, "node-b", false, "certificate for a different node was accepted with node-b's bearer token")
	_, rotated := newPeerIdentityTestPair(t, ca, caKey, "lab.example", "node-b")
	request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{rotated}, VerifiedChains: [][]*x509.Certificate{{rotated, ca}}}
	assertPeerAuthenticated(t, request, state, "node-b", false, "unpaired replacement certificate bypassed membership fingerprint")
	invite, err := members.CreateInvitation("node-b", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := members.ConsumeInvitation(invite.Code, "node-b", peerCertificateFingerprint(rotated), time.Now()); err != nil {
		t.Fatalf("explicit certificate rotation was rejected: %v", err)
	}
	assertPeerAuthenticated(t, request, state, "node-b", true, "explicitly paired replacement identity rejected")
	request.TLS.PeerCertificates[0] = valid
	request.TLS.VerifiedChains[0][0] = valid
	assertPeerAuthenticated(t, request, state, "node-b", false, "old identity certificate remained valid after replacement")
	request.TLS.PeerCertificates[0] = rotated
	request.TLS.VerifiedChains[0][0] = rotated
	_, _, rotatedRoots := newPeerIdentityTestCA(t)
	state.peerClientCAs = rotatedRoots
	assertPeerAuthenticated(t, request, state, "node-b", false, "certificate from a removed CA remained authorized on an existing connection")
	state.peerClientCAs = roots
	if err := members.RemoveMember("node-b", time.Now()); err != nil {
		t.Fatal(err)
	}
	assertPeerAuthenticated(t, request, state, "node-b", false, "revoked identity remained authorized")
	newInvite, err := members.CreateInvitation("node-b", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := members.ConsumeInvitation(newInvite.Code, "node-b", peerCertificateFingerprint(valid), time.Now()); err == nil {
		t.Fatal("previously revoked certificate was accepted during re-pairing")
	}
	request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{valid}}
	assertPeerAuthenticated(t, request, state, "node-b", false, "unverified client certificate was accepted")
}

func TestMtlSOnlyCatalogReadRequiresExplicitFingerprintBoundGrant(t *testing.T) {
	ca, caKey, roots := newPeerIdentityTestCA(t)
	_, valid := newPeerIdentityTestPair(t, ca, caKey, "lab.example", "node-b")
	_, rotated := newPeerIdentityTestPair(t, ca, caKey, "lab.example", "node-b")
	membershipDir, grantDir := t.TempDir(), t.TempDir()
	for _, directory := range []string{membershipDir, grantDir} {
		if err := os.Chmod(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	members, err := federation.OpenPairingStore(membershipDir, "lab.example", "owner")
	if err != nil {
		t.Fatal(err)
	}
	defer members.Close()
	grants, err := federation.OpenCatalogGrantStore(grantDir, "lab.example", "owner")
	if err != nil {
		t.Fatal(err)
	}
	defer grants.Close()
	now := time.Now().UTC()
	member, err := members.AddMember("node-b", peerCertificateFingerprint(valid), now)
	if err != nil {
		t.Fatal(err)
	}
	state := &reloaderState{
		peer:          &config.PeerConfig{NodeID: "owner", FederationID: "lab.example"},
		peerClientCAs: roots, peerClientCRLs: []*x509.RevocationList{newPeerIdentityTestCRL(t, ca, caKey)},
		pairingStore: members, catalogGrantStore: grants,
	}
	request := httptest.NewRequest(http.MethodGet, "https://peer.example/peer/v1/report", nil)
	request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{valid}, VerifiedChains: [][]*x509.Certificate{{valid, ca}}}

	// No catalog grant initially: paired membership alone must not authorize report access.
	if _, ok := authenticatedPeer(request, state, peerCatalog); ok {
		t.Fatal("paired membership without a catalog grant authorized report access")
	}

	// Explicit mTLS catalog grant allows only catalog.
	if _, err := grants.GrantCatalogRead(member, now); err != nil {
		t.Fatal(err)
	}
	assertPeerGrantResult(t, request, state, peerCatalog, "node-b", true, "explicit mTLS catalog grant denied")
	assertPeerGrantResult(t, request, state, peerInference, "", false, "catalog grant authorized inference without its separate grant")

	// Inference requires its distinct grant.
	if _, err := grants.GrantInference(member, true, false, now); err != nil {
		t.Fatal(err)
	}
	assertPeerGrantResult(t, request, state, peerInference, "node-b", true, "explicit inbound inference grant denied")

	// Invalid bearer never falls back to mTLS-only authorization.
	request.Header.Set("Authorization", "Bearer "+strings.Repeat("x", 32))
	assertPeerGrantResult(t, request, state, peerCatalog, "", false, "invalid bearer token fell back to mTLS-only authorization")
	request.Header.Del("Authorization")

	// Certificate rotation invalidates old grants; explicit grants restore scopes.
	rotateAndRegrant(t, peerRotationContext{
		request: request,
		state:   state,
		members: members,
		grants:  grants,
		rotated: rotated,
		ca:      ca,
		now:     now,
	})

	// Catalog revoke does not revoke independent inference.
	if err := grants.RevokeCatalogRead("node-b"); err != nil {
		t.Fatal(err)
	}
	assertPeerGrantResult(t, request, state, peerCatalog, "", false, "revoked catalog grant remained authorized")
	assertPeerGrantResult(t, request, state, peerInference, "node-b", true, "catalog-read revocation incorrectly revoked independent inbound inference grant")

	// Inference revoke denies.
	if err := grants.RevokeInference("node-b"); err != nil {
		t.Fatal(err)
	}
	assertPeerGrantResult(t, request, state, peerInference, "", false, "revoked inbound inference grant remained authorized")
}

func TestCurrentCRLBlocksIdentityFromRestoredOldMembershipSnapshot(t *testing.T) {
	ca, caKey, roots := newPeerIdentityTestCA(t)
	_, leaf := newPeerIdentityTestPair(t, ca, caKey, "lab.example", "node-b")
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := federation.OpenPairingStore(directory, "lab.example", "owner")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AddMember("node-b", peerCertificateFingerprint(leaf), time.Now()); err != nil {
		t.Fatal(err)
	}
	storePath := filepath.Join(directory, "pairing.json")
	oldSnapshot, err := os.ReadFile(storePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RemoveMember("node-b", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(storePath, oldSnapshot, 0o600); err != nil {
		t.Fatal(err)
	}
	staleStore, err := federation.OpenPairingStore(directory, "lab.example", "owner")
	if err != nil {
		t.Fatal(err)
	}
	defer staleStore.Close()
	revokingCRL := newPeerIdentityTestCRL(t, ca, caKey, leaf.SerialNumber)
	state := &reloaderState{
		peer: &config.PeerConfig{NodeID: "owner", FederationID: "lab.example", Peers: map[string]config.PeerGrant{
			"node-b": {Token: strings.Repeat("b", 32), CatalogRead: true},
		}},
		peerClientCAs: roots, peerClientCRLs: []*x509.RevocationList{revokingCRL}, pairingStore: staleStore,
	}
	request := httptest.NewRequest(http.MethodGet, "https://peer.example/peer/v1/report", nil)
	request.Header.Set("Authorization", "Bearer "+strings.Repeat("b", 32))
	request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}, VerifiedChains: [][]*x509.Certificate{{leaf, ca}}}
	if _, ok := authenticatedPeer(request, state, peerCatalog); ok {
		t.Fatal("restored membership snapshot overrode the current CA-signed revocation list")
	}
}

func TestPeerTLSRequiresClientCertificateWhenFederationIdentityEnabled(t *testing.T) {
	ca, caKey, roots := newPeerIdentityTestCA(t)
	serverPair, _ := newPeerIdentityTestPair(t, ca, caKey, "lab.example", "owner")
	clientPair, _ := newPeerIdentityTestPair(t, ca, caKey, "lab.example", "node-b")
	r := &Reloader{}
	r.state.Store(&reloaderState{
		peer:        &config.PeerConfig{NodeID: "owner", FederationID: "lab.example"},
		certificate: &serverPair, peerClientCAs: roots,
	})
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	server.TLS = r.PeerTLSConfig()
	server.StartTLS()
	defer server.Close()

	withoutIdentity := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}}}
	defer withoutIdentity.CloseIdleConnections()
	if response, err := withoutIdentity.Get(server.URL); err == nil {
		_ = response.Body.Close()
		t.Fatal("peer TLS accepted a connection without a client certificate")
	}
	withIdentity := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, Certificates: []tls.Certificate{clientPair}}}}
	defer withIdentity.CloseIdleConnections()
	response, err := withIdentity.Get(server.URL)
	if err != nil {
		t.Fatalf("peer TLS rejected a valid federation client certificate: %v", err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("mTLS test response status = %d", response.StatusCode)
	}
}

func TestNewPeerClientsPresentLocalFederationIdentity(t *testing.T) {
	ca, caKey, _ := newPeerIdentityTestCA(t)
	clientPair, _ := newPeerIdentityTestPair(t, ca, caKey, "lab.example", "node-a")
	_, remoteCert := newPeerIdentityTestPair(t, ca, caKey, "lab.example", "node-b")
	_, wrongRemoteCert := newPeerIdentityTestPair(t, ca, caKey, "lab.example", "node-c")
	certPath, keyPath := writePeerIdentityTestPair(t, clientPair)
	caPath, crlPath := writePeerTrustFiles(t, ca, caKey)
	membershipDir := t.TempDir()
	if err := os.Chmod(membershipDir, 0o700); err != nil {
		t.Fatal(err)
	}
	members, err := federation.OpenPairingStore(membershipDir, "lab.example", "node-a")
	if err != nil {
		t.Fatal(err)
	}
	defer members.Close()
	if _, err := members.AddMember("node-b", peerCertificateFingerprint(remoteCert), time.Now()); err != nil {
		t.Fatal(err)
	}
	peer := &config.PeerConfig{
		NodeID: "node-a", FederationID: "lab.example", TLSCertFile: certPath, TLSKeyFile: keyPath,
		Peers: map[string]config.PeerGrant{"node-b": {
			URL: "https://peer.example:9293", OutboundToken: strings.Repeat("b", 32), CAFile: caPath, CRLFile: crlPath,
		}},
	}
	clients, _, err := newPeerClientsWithMembership(peer, members)
	if err != nil {
		t.Fatal(err)
	}
	transport := clients["node-b"].client.Transport.(*http.Transport)
	if len(transport.TLSClientConfig.Certificates) != 1 || transport.TLSClientConfig.Certificates[0].Leaf == nil {
		t.Fatal("outbound peer transport did not load the local identity certificate")
	}
	if !transport.DisableKeepAlives || transport.TLSClientConfig.VerifyConnection == nil {
		t.Fatal("outbound mTLS client does not revalidate membership for every request")
	}
	connection := tls.ConnectionState{PeerCertificates: []*x509.Certificate{remoteCert}}
	connection.VerifiedChains = [][]*x509.Certificate{{remoteCert, ca}}
	if err := transport.TLSClientConfig.VerifyConnection(connection); err != nil {
		t.Fatalf("paired outbound endpoint rejected: %v", err)
	}
	connection.PeerCertificates[0] = wrongRemoteCert
	connection.VerifiedChains[0][0] = wrongRemoteCert
	if err := transport.TLSClientConfig.VerifyConnection(connection); err == nil {
		t.Fatal("outbound transport accepted a certificate with another node's URI identity")
	}
	if err := members.RemoveMember("node-b", time.Now()); err != nil {
		t.Fatal(err)
	}
	connection.PeerCertificates[0] = remoteCert
	connection.VerifiedChains[0][0] = remoteCert
	if err := transport.TLSClientConfig.VerifyConnection(connection); err == nil {
		t.Fatal("outbound transport accepted a revoked remote certificate")
	}
	peer.NodeID = "node-c"
	if _, _, err := newPeerClientsWithMembership(peer, members); err == nil {
		t.Fatal("outbound client accepted a certificate for a different local node ID")
	}
}

func TestLoadPeerClientCAsAcceptsOnlyCACertificates(t *testing.T) {
	ca, caKey, _ := newPeerIdentityTestCA(t)
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadPeerClientCAs(&config.PeerConfig{FederationID: "lab.example", ClientCAFile: caPath}); err != nil {
		t.Fatalf("valid CA bundle rejected: %v", err)
	}
	leafPair, _ := newPeerIdentityTestPair(t, ca, caKey, "lab.example", "node-a")
	leafPath := filepath.Join(t.TempDir(), "leaf.pem")
	if err := os.WriteFile(leafPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafPair.Certificate[0]}), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadPeerClientCAs(&config.PeerConfig{FederationID: "lab.example", ClientCAFile: leafPath}); err == nil {
		t.Fatal("client CA bundle accepted a leaf certificate")
	}
}

func TestAdminFederationPolicyReportsCRLFreshnessIndependently(t *testing.T) {
	ca, caKey, _ := newPeerIdentityTestCA(t)
	now := time.Now()
	crls := []*x509.RevocationList{newPeerIdentityTestCRL(t, ca, caKey)}
	state := &reloaderState{
		peer: &config.PeerConfig{FederationID: "lab.example"}, peerClientCRLs: crls,
		peerClients: map[string]*peerClient{"node-b": {serverCRLs: crls, crlMaxAge: 24 * time.Hour}},
	}
	policy := summarizeFederationCRLs(state, now)
	if policy.State != "fresh" || policy.ClientCRL.State != "fresh" || len(policy.PeerCRLs) != 1 || policy.PeerCRLs[0].State != "fresh" {
		t.Fatalf("fresh CRL policy = %+v", policy)
	}
	policy = summarizeFederationCRLs(state, now.Add(2*time.Hour))
	if policy.State != "stale" || policy.ClientCRL.State != "stale" || policy.PeerCRLs[0].State != "stale" {
		t.Fatalf("expired CRL policy = %+v", policy)
	}
}

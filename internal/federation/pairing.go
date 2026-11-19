package federation

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/identity"
)

const (
	storeFilename       = "pairing.json"
	lockFilename        = ".pairing.lock"
	maxPairingStoreSize = 512 << 10
	maxPairingInvites   = 128
	maxPairingMembers   = 512
	maxConsumedInvites  = 1024
	maxRevocations      = 2048
	maxRevocationsPerID = 8
	invitationLifetime  = 10 * time.Minute
)

var (
	ErrPairingStore   = errors.New("pairing store unavailable")
	ErrInvitation     = errors.New("invitation is invalid or expired")
	ErrMemberExists   = errors.New("node identity is already paired")
	ErrMemberNotFound = errors.New("node identity is not paired")
	ErrInvitationFull = errors.New("invitation limit reached")
)

type Membership struct {
	NodeID      string    `json:"node_id"`
	Fingerprint string    `json:"certificate_sha256"`
	PairedAt    time.Time `json:"paired_at"`
}

type Invitation struct {
	Code      string    `json:"code"`
	NodeID    string    `json:"node_id"`
	ExpiresAt time.Time `json:"expires_at"`
}

type PendingInvitation struct {
	NodeID    string    `json:"node_id"`
	ExpiresAt time.Time `json:"expires_at"`
}

type inviteRecord struct {
	NodeID    string    `json:"node_id"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

type consumedRecord struct {
	NodeID    string    `json:"node_id"`
	ExpiresAt time.Time `json:"expires_at"`
}

type pairingData struct {
	Version      int                       `json:"version"`
	FederationID string                    `json:"federation_id"`
	LocalNodeID  string                    `json:"local_node_id"`
	Members      map[string]Membership     `json:"members"`
	Invites      map[string]inviteRecord   `json:"invites"`
	Consumed     map[string]consumedRecord `json:"consumed"`
	Revoked      map[string][]string       `json:"revoked_certificates"`
}

type PairingStore struct {
	mu           sync.Mutex
	root         *os.Root
	lock         *os.File
	directory    string
	federationID string
	localNodeID  string
	data         pairingData
	closed       bool
	failed       bool
}

func OpenPairingStore(directory, federationID, localNodeID string) (*PairingStore, error) {
	if !filepath.IsAbs(directory) || filepath.Clean(directory) != directory ||
		!validFederationID(federationID) || !identity.ValidNodeID(localNodeID) {
		return nil, ErrPairingStore
	}
	info, err := os.Lstat(directory)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return nil, ErrPairingStore
	}
	if err := validatePairingStoreOwner(info); err != nil {
		return nil, ErrPairingStore
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, ErrPairingStore
	}
	store := &PairingStore{
		root: root, directory: directory, federationID: federationID, localNodeID: localNodeID,
		data: pairingData{
			Version: 1, FederationID: federationID, LocalNodeID: localNodeID,
			Members: make(map[string]Membership), Invites: make(map[string]inviteRecord), Consumed: make(map[string]consumedRecord),
			Revoked: make(map[string][]string),
		},
	}
	if err := store.openLock(); err != nil {
		_ = root.Close()
		return nil, err
	}
	unlock, err := lockPairingStore(store.lock)
	if err != nil {
		_ = store.Close()
		return nil, ErrPairingStore
	}
	readErr := store.read()
	unlockErr := unlock()
	if readErr != nil || unlockErr != nil {
		_ = store.Close()
		return nil, ErrPairingStore
	}
	return store, nil
}

func (s *PairingStore) CreateInvitation(nodeID string, now time.Time) (Invitation, error) {
	if !identity.ValidNodeID(nodeID) || nodeID == s.localNodeID || now.IsZero() {
		return Invitation{}, ErrInvitation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.failed {
		return Invitation{}, ErrPairingStore
	}
	unlock, err := s.lockAndRead()
	if err != nil {
		return Invitation{}, err
	}
	defer unlock()
	s.prune(now)
	for _, invite := range s.data.Invites {
		if invite.NodeID == nodeID {
			return Invitation{}, ErrInvitation
		}
	}
	if len(s.data.Invites) >= maxPairingInvites {
		return Invitation{}, ErrInvitationFull
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return Invitation{}, ErrPairingStore
	}
	code := base64.RawURLEncoding.EncodeToString(secret)
	hash := sha256.Sum256([]byte(code))
	key := hex.EncodeToString(hash[:])
	expires := now.Add(invitationLifetime).UTC()
	s.data.Invites[key] = inviteRecord{NodeID: nodeID, CreatedAt: now.UTC(), ExpiresAt: expires}
	if err := s.persist(); err != nil {
		return Invitation{}, err
	}
	return Invitation{Code: code, NodeID: nodeID, ExpiresAt: expires}, nil
}

func (s *PairingStore) ConsumeInvitation(code, nodeID, fingerprint string, now time.Time) (Membership, error) {
	if !identity.ValidNodeID(nodeID) || !validFingerprint(fingerprint) || !validInvitationCode(code) || now.IsZero() {
		return Membership{}, ErrInvitation
	}
	hash := sha256.Sum256([]byte(code))
	key := hex.EncodeToString(hash[:])
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.failed {
		return Membership{}, ErrPairingStore
	}
	unlock, err := s.lockAndRead()
	if err != nil {
		return Membership{}, err
	}
	defer unlock()
	s.prune(now)
	if consumed, ok := s.data.Consumed[key]; ok {
		member, paired := s.data.Members[consumed.NodeID]
		if paired && consumed.NodeID == nodeID && now.Before(consumed.ExpiresAt) {
			fingerprint = strings.ToLower(fingerprint)
			if member.Fingerprint != fingerprint {
				if fingerprintRevoked(s.data.Revoked[nodeID], fingerprint) || fingerprintInUse(s.data.Members, fingerprint) {
					return Membership{}, ErrInvitation
				}
				if err := recordRevokedFingerprint(s.data.Revoked, nodeID, member.Fingerprint); err != nil {
					return Membership{}, err
				}
				member.Fingerprint = fingerprint
				s.data.Members[nodeID] = member
				if err := s.persist(); err != nil {
					return Membership{}, err
				}
			}
			return member, nil
		}
		return Membership{}, ErrInvitation
	}
	invite, ok := s.data.Invites[key]
	if !ok || invite.NodeID != nodeID || !now.Before(invite.ExpiresAt) {
		return Membership{}, ErrInvitation
	}
	if fingerprintRevoked(s.data.Revoked[nodeID], fingerprint) {
		return Membership{}, ErrInvitation
	}
	if fingerprintInUseExcept(s.data.Members, fingerprint, nodeID) {
		return Membership{}, ErrMemberExists
	}
	existing, isExisting := s.data.Members[nodeID]
	if (!isExisting && len(s.data.Members) >= maxPairingMembers) || len(s.data.Consumed) >= maxConsumedInvites {
		return Membership{}, ErrInvitationFull
	}
	member := Membership{NodeID: nodeID, Fingerprint: strings.ToLower(fingerprint), PairedAt: now.UTC()}
	if isExisting {
		member.PairedAt = existing.PairedAt
		if existing.Fingerprint != member.Fingerprint {
			if err := recordRevokedFingerprint(s.data.Revoked, nodeID, existing.Fingerprint); err != nil {
				return Membership{}, err
			}
		}
	}
	s.data.Members[nodeID] = member
	delete(s.data.Invites, key)
	s.data.Consumed[key] = consumedRecord{NodeID: nodeID, ExpiresAt: now.Add(24 * time.Hour).UTC()}
	if err := s.persist(); err != nil {
		return Membership{}, err
	}
	return member, nil
}

func (s *PairingStore) AddMember(nodeID, fingerprint string, pairedAt time.Time) (Membership, error) {
	if !identity.ValidNodeID(nodeID) || nodeID == s.localNodeID || !validFingerprint(fingerprint) || pairedAt.IsZero() {
		return Membership{}, ErrInvitation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.failed {
		return Membership{}, ErrPairingStore
	}
	unlock, err := s.lockAndRead()
	if err != nil {
		return Membership{}, err
	}
	defer unlock()
	if existing, ok := s.data.Members[nodeID]; ok {
		if existing.Fingerprint == strings.ToLower(fingerprint) {
			return existing, nil
		}
		if fingerprintInUse(s.data.Members, fingerprint) || fingerprintRevoked(s.data.Revoked[nodeID], fingerprint) {
			return Membership{}, ErrMemberExists
		}
		if err := recordRevokedFingerprint(s.data.Revoked, nodeID, existing.Fingerprint); err != nil {
			return Membership{}, err
		}
		existing.Fingerprint = strings.ToLower(fingerprint)
		s.data.Members[nodeID] = existing
		if err := s.persist(); err != nil {
			return Membership{}, err
		}
		return existing, nil
	}
	if fingerprintRevoked(s.data.Revoked[nodeID], fingerprint) {
		return Membership{}, ErrInvitation
	}
	if fingerprintInUse(s.data.Members, fingerprint) {
		return Membership{}, ErrMemberExists
	}
	if len(s.data.Members) >= maxPairingMembers {
		return Membership{}, ErrInvitationFull
	}
	member := Membership{NodeID: nodeID, Fingerprint: strings.ToLower(fingerprint), PairedAt: pairedAt.UTC()}
	s.data.Members[nodeID] = member
	if err := s.persist(); err != nil {
		return Membership{}, err
	}
	return member, nil
}

func (s *PairingStore) RemoveMember(nodeID string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.failed {
		return ErrPairingStore
	}
	unlock, err := s.lockAndRead()
	if err != nil {
		return err
	}
	defer unlock()
	if _, ok := s.data.Members[nodeID]; !ok {
		return ErrMemberNotFound
	}
	if err := recordRevokedFingerprint(s.data.Revoked, nodeID, s.data.Members[nodeID].Fingerprint); err != nil {
		return err
	}
	delete(s.data.Members, nodeID)
	for hash, invite := range s.data.Invites {
		if invite.NodeID == nodeID {
			delete(s.data.Invites, hash)
		}
	}
	for hash, consumed := range s.data.Consumed {
		if consumed.NodeID == nodeID {
			delete(s.data.Consumed, hash)
		}
	}
	s.prune(now)
	return s.persist()
}

func (s *PairingStore) IsMember(nodeID string) bool {
	_, ok := s.GetMember(nodeID)
	return ok
}

func (s *PairingStore) GetMember(nodeID string) (Membership, bool) {
	member, ok, _ := s.LookupMember(nodeID)
	return member, ok
}

func (s *PairingStore) LookupMember(nodeID string) (Membership, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.failed {
		return Membership{}, false, ErrPairingStore
	}
	unlock, err := s.lockAndRead()
	if err != nil {
		return Membership{}, false, err
	}
	defer unlock()
	member, ok := s.data.Members[nodeID]
	return member, ok, nil
}

func (s *PairingStore) IsCertificateRevoked(nodeID, fingerprint string) (bool, error) {
	if !identity.ValidNodeID(nodeID) || !validFingerprint(fingerprint) {
		return true, ErrPairingStore
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.failed {
		return true, ErrPairingStore
	}
	unlock, err := s.lockAndRead()
	if err != nil {
		return true, err
	}
	defer unlock()
	return fingerprintRevoked(s.data.Revoked[nodeID], fingerprint), nil
}

func (s *PairingStore) ListMembers() ([]Membership, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.failed {
		return nil, ErrPairingStore
	}
	unlock, err := s.lockAndRead()
	if err != nil {
		return nil, err
	}
	defer unlock()
	members := make([]Membership, 0, len(s.data.Members))
	for _, member := range s.data.Members {
		members = append(members, member)
	}
	sort.Slice(members, func(i, j int) bool { return members[i].NodeID < members[j].NodeID })
	return members, nil
}

func (s *PairingStore) ListInvitations(now time.Time) ([]PendingInvitation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.failed || now.IsZero() {
		return nil, ErrPairingStore
	}
	unlock, err := s.lockAndRead()
	if err != nil {
		return nil, err
	}
	defer unlock()
	beforeInvites, beforeConsumed := len(s.data.Invites), len(s.data.Consumed)
	s.prune(now)
	if beforeInvites != len(s.data.Invites) || beforeConsumed != len(s.data.Consumed) {
		if err := s.persist(); err != nil {
			return nil, err
		}
	}
	invitations := make([]PendingInvitation, 0, len(s.data.Invites))
	for _, invite := range s.data.Invites {
		invitations = append(invitations, PendingInvitation{NodeID: invite.NodeID, ExpiresAt: invite.ExpiresAt})
	}
	sort.Slice(invitations, func(i, j int) bool { return invitations[i].NodeID < invitations[j].NodeID })
	return invitations, nil
}

func (s *PairingStore) RemoveInvitation(nodeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.failed {
		return ErrPairingStore
	}
	unlock, err := s.lockAndRead()
	if err != nil {
		return err
	}
	defer unlock()
	removed := false
	for hash, invite := range s.data.Invites {
		if invite.NodeID == nodeID {
			delete(s.data.Invites, hash)
			removed = true
		}
	}
	if !removed {
		return ErrInvitation
	}
	return s.persist()
}

func (s *PairingStore) Join(ctx context.Context, endpoint, code string, certificate tls.Certificate, roots *x509.CertPool, crls []*x509.RevocationList, maxCRLAge time.Duration) (Membership, error) {
	return s.JoinExpected(ctx, endpoint, "", code, certificate, roots, crls, maxCRLAge)
}

func (s *PairingStore) JoinExpected(ctx context.Context, endpoint, expectedNodeID, code string, certificate tls.Certificate, roots *x509.CertPool, crls []*x509.RevocationList, maxCRLAge time.Duration) (Membership, error) {
	if !validInvitationCode(code) || roots == nil || len(crls) == 0 || len(certificate.Certificate) == 0 {
		return Membership{}, ErrInvitation
	}
	if expectedNodeID != "" && !identity.ValidNodeID(expectedNodeID) {
		return Membership{}, ErrInvitation
	}
	origin, err := validEndpoint(endpoint)
	if err != nil {
		return Membership{}, ErrInvitation
	}
	leaf := certificate.Leaf
	if leaf == nil {
		leaf, err = x509.ParseCertificate(certificate.Certificate[0])
		if err != nil {
			return Membership{}, ErrInvitation
		}
	}
	if ValidateIdentityCertificate(leaf, s.federationID, s.localNodeID) != nil {
		return Membership{}, ErrInvitation
	}
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: []tls.Certificate{certificate},
			VerifyConnection: func(state tls.ConnectionState) error {
				if len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 {
					return ErrInvitation
				}
				if VerifyCertificateRevocation(state.VerifiedChains, crls, maxCRLAge, time.Now()) != nil {
					return ErrInvitation
				}
				remoteID, err := PeerIdentityNodeID(state.PeerCertificates[0], s.federationID)
				if err != nil || (expectedNodeID != "" && remoteID != expectedNodeID) {
					return ErrInvitation
				}
				revoked, err := s.IsCertificateRevoked(remoteID, CertificateFingerprint(state.PeerCertificates[0]))
				if err != nil || revoked {
					return ErrInvitation
				}
				return nil
			},
		},
		DisableCompression: true, MaxConnsPerHost: 1, MaxIdleConnsPerHost: 1,
		TLSHandshakeTimeout: 5 * time.Second, IdleConnTimeout: 10 * time.Second,
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, origin+"/peer/v1/enroll", nil)
	if err != nil {
		return Membership{}, ErrInvitation
	}
	request.Header.Set("X-Llama-Catalog-Invite", code)
	response, err := client.Do(request)
	if err != nil {
		return Membership{}, ErrInvitation
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.TLS == nil || len(response.TLS.VerifiedChains) == 0 || len(response.TLS.PeerCertificates) == 0 {
		return Membership{}, ErrInvitation
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 4097))
	if err != nil || len(body) > 4096 {
		return Membership{}, ErrInvitation
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var result struct {
		State        string    `json:"state"`
		FederationID string    `json:"federation_id"`
		LocalNodeID  string    `json:"local_node_id"`
		NodeID       string    `json:"node_id"`
		PairedAt     time.Time `json:"paired_at"`
	}
	if decoder.Decode(&result) != nil || decoder.Decode(new(any)) != io.EOF || result.State != "paired" ||
		result.FederationID != s.federationID || result.NodeID != s.localNodeID || !identity.ValidNodeID(result.LocalNodeID) ||
		(expectedNodeID != "" && result.LocalNodeID != expectedNodeID) {
		return Membership{}, ErrInvitation
	}
	serverCert := response.TLS.PeerCertificates[0]
	remoteNodeID, err := PeerIdentityNodeID(serverCert, s.federationID)
	if err != nil || remoteNodeID != result.LocalNodeID {
		return Membership{}, ErrInvitation
	}
	return s.AddMember(remoteNodeID, CertificateFingerprint(serverCert), time.Now().UTC())
}

func (s *PairingStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	closeErr := s.lock.Close()
	rootErr := s.root.Close()
	if closeErr != nil || rootErr != nil {
		return ErrPairingStore
	}
	return nil
}

func (s *PairingStore) openLock() error {
	if info, err := s.root.Lstat(lockFilename); err == nil {
		if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
			return ErrPairingStore
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return ErrPairingStore
	}
	lock, err := s.root.OpenFile(lockFilename, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return ErrPairingStore
	}
	if err := lock.Chmod(0o600); err != nil || lockInfoUnsafe(lock) {
		_ = lock.Close()
		return ErrPairingStore
	}
	s.lock = lock
	return nil
}

func (s *PairingStore) lockAndRead() (func() error, error) {
	unlock, err := lockPairingStore(s.lock)
	if err != nil {
		return nil, ErrPairingStore
	}
	if err := s.read(); err != nil {
		s.failed = true
		_ = unlock()
		return nil, ErrPairingStore
	}
	return unlock, nil
}

func (s *PairingStore) read() error {
	s.data = pairingData{
		Version: 1, FederationID: s.federationID, LocalNodeID: s.localNodeID,
		Members: make(map[string]Membership), Invites: make(map[string]inviteRecord), Consumed: make(map[string]consumedRecord),
		Revoked: make(map[string][]string),
	}
	file, err := s.root.Open(storeFilename)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return ErrPairingStore
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() > maxPairingStoreSize {
		return ErrPairingStore
	}
	data, err := io.ReadAll(io.LimitReader(file, maxPairingStoreSize+1))
	if err != nil || len(data) > maxPairingStoreSize {
		return ErrPairingStore
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var store pairingData
	if err := decoder.Decode(&store); err != nil {
		return ErrPairingStore
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return ErrPairingStore
	}
	if err := validatePairingData(store, s.federationID, s.localNodeID); err != nil {
		return ErrPairingStore
	}
	s.data = store
	return nil
}

func (s *PairingStore) persist() error {
	data, err := json.Marshal(s.data)
	if err != nil || len(data) > maxPairingStoreSize {
		s.failed = true
		return ErrPairingStore
	}
	secret := make([]byte, 12)
	if _, err := rand.Read(secret); err != nil {
		s.failed = true
		return ErrPairingStore
	}
	tempName := ".pairing-" + hex.EncodeToString(secret) + ".tmp"
	temp, err := s.root.OpenFile(tempName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		s.failed = true
		return ErrPairingStore
	}
	writeErr := temp.Chmod(0o600)
	if writeErr == nil {
		_, writeErr = temp.Write(data)
	}
	if writeErr == nil {
		writeErr = temp.Sync()
	}
	closeErr := temp.Close()
	if writeErr != nil || closeErr != nil {
		_ = s.root.Remove(tempName)
		s.failed = true
		return ErrPairingStore
	}
	if err := os.Rename(filepath.Join(s.directory, tempName), filepath.Join(s.directory, storeFilename)); err != nil {
		_ = s.root.Remove(tempName)
		s.failed = true
		return ErrPairingStore
	}
	dir, err := s.root.Open(".")
	if err == nil {
		err = dir.Sync()
		_ = dir.Close()
	}
	if err != nil {
		s.failed = true
		return ErrPairingStore
	}
	return nil
}

func (s *PairingStore) prune(now time.Time) {
	for hash, invite := range s.data.Invites {
		if !now.Before(invite.ExpiresAt) {
			delete(s.data.Invites, hash)
		}
	}
	for hash, consumed := range s.data.Consumed {
		if !now.Before(consumed.ExpiresAt) {
			delete(s.data.Consumed, hash)
		}
	}
}

func validatePairingData(data pairingData, federationID, localNodeID string) error {
	if data.Version != 1 || data.FederationID != federationID || data.LocalNodeID != localNodeID ||
		data.Members == nil || data.Invites == nil || data.Consumed == nil || data.Revoked == nil ||
		len(data.Members) > maxPairingMembers || len(data.Invites) > maxPairingInvites || len(data.Consumed) > maxConsumedInvites ||
		revokedFingerprintCount(data.Revoked) > maxRevocations {
		return ErrPairingStore
	}
	fingerprints := make(map[string]bool, len(data.Members))
	for id, member := range data.Members {
		fingerprint := strings.ToLower(member.Fingerprint)
		if !identity.ValidNodeID(id) || id == localNodeID || member.NodeID != id || !validFingerprint(fingerprint) ||
			member.PairedAt.IsZero() || fingerprints[fingerprint] {
			return ErrPairingStore
		}
		fingerprints[fingerprint] = true
	}
	inviteIDs := make(map[string]bool, len(data.Invites))
	for hash, invite := range data.Invites {
		if !validHash(hash) || !identity.ValidNodeID(invite.NodeID) || invite.NodeID == localNodeID ||
			inviteIDs[invite.NodeID] || invite.CreatedAt.IsZero() || !invite.ExpiresAt.After(invite.CreatedAt) || invite.ExpiresAt.Sub(invite.CreatedAt) > invitationLifetime {
			return ErrPairingStore
		}
		inviteIDs[invite.NodeID] = true
	}
	for hash, consumed := range data.Consumed {
		if !validHash(hash) || !identity.ValidNodeID(consumed.NodeID) || consumed.NodeID == localNodeID || consumed.ExpiresAt.IsZero() {
			return ErrPairingStore
		}
		if _, duplicate := data.Invites[hash]; duplicate {
			return ErrPairingStore
		}
	}
	for nodeID, revoked := range data.Revoked {
		if !identity.ValidNodeID(nodeID) || nodeID == localNodeID || len(revoked) == 0 || len(revoked) > maxRevocationsPerID {
			return ErrPairingStore
		}
		seen := make(map[string]bool, len(revoked))
		for _, fingerprint := range revoked {
			fingerprint = strings.ToLower(fingerprint)
			if !validFingerprint(fingerprint) || seen[fingerprint] {
				return ErrPairingStore
			}
			seen[fingerprint] = true
		}
	}
	return nil
}

func validInvitationCode(code string) bool {
	decoded, err := base64.RawURLEncoding.DecodeString(code)
	return err == nil && len(decoded) == 32 && len(code) <= 64
}

func validFingerprint(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func validHash(value string) bool { return validFingerprint(value) }

func lockInfoUnsafe(file *os.File) bool {
	info, err := file.Stat()
	return err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0
}

func validFederationID(value string) bool {
	if len(value) == 0 || len(value) > 253 || strings.ToLower(value) != value {
		return false
	}
	for _, label := range strings.Split(value, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, ch := range label {
			if !(ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' || ch == '-') {
				return false
			}
		}
	}
	return true
}

func fingerprintInUse(members map[string]Membership, fingerprint string) bool {
	return fingerprintInUseExcept(members, fingerprint, "")
}

func fingerprintInUseExcept(members map[string]Membership, fingerprint, excludedNode string) bool {
	fingerprint = strings.ToLower(fingerprint)
	for nodeID, member := range members {
		if nodeID != excludedNode && strings.EqualFold(member.Fingerprint, fingerprint) {
			return true
		}
	}
	return false
}

func fingerprintRevoked(fingerprints []string, fingerprint string) bool {
	for _, revoked := range fingerprints {
		if strings.EqualFold(revoked, fingerprint) {
			return true
		}
	}
	return false
}

func revokedFingerprintCount(revoked map[string][]string) int {
	count := 0
	for _, fingerprints := range revoked {
		count += len(fingerprints)
	}
	return count
}

func recordRevokedFingerprint(revoked map[string][]string, nodeID, fingerprint string) error {
	if fingerprintRevoked(revoked[nodeID], fingerprint) {
		return nil
	}
	if len(revoked[nodeID]) >= maxRevocationsPerID || revokedFingerprintCount(revoked) >= maxRevocations {
		return ErrInvitationFull
	}
	revoked[nodeID] = append(revoked[nodeID], strings.ToLower(fingerprint))
	return nil
}

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
	"net"
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
	// lookupNetIP resolves endpoint hostnames at dial time. Defaults to the
	// production resolver (net.DefaultResolver.LookupNetIP) but can be
	// overridden per store for deterministic testing without mutating
	// package-level state.
	lookupNetIP func(ctx context.Context, netw, name string) ([]net.IP, error)
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
		lookupNetIP: defaultLookupNetIP,
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
		return s.consumeInvitationReuse(consumed, nodeID, fingerprint, now)
	}
	invite, ok := s.data.Invites[key]
	if !ok || invite.NodeID != nodeID || !now.Before(invite.ExpiresAt) {
		return Membership{}, ErrInvitation
	}
	return s.consumeInvitationFresh(invite, nodeID, fingerprint, now, key)
}

// consumeInvitationReuse handles a previously consumed invitation whose consumed
// record is still fresh. It rotates the paired member fingerprint when the
// presented fingerprint differs from the stored one.
func (s *PairingStore) consumeInvitationReuse(consumed consumedRecord, nodeID, fingerprint string, now time.Time) (Membership, error) {
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

// consumeInvitationFresh handles a first-time invitation consume. It validates the
// fingerprint, registers the member, and persists the consumed record.
func (s *PairingStore) consumeInvitationFresh(invite inviteRecord, nodeID, fingerprint string, now time.Time, key string) (Membership, error) {
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

// JoinExpectedOptions groups the enrollment parameters of PairingStore.JoinExpected
// into a single options value so call sites use named fields instead of a positional
// argument list.
type JoinExpectedOptions struct {
	// ExpectedNodeID is the node ID the peer TLS certificate must identify. When empty,
	// any valid peer identity is accepted.
	ExpectedNodeID string
	// Code is the one-time invitation code presented in the enroll request.
	Code string
	// Certificate is the local peer identity certificate presented to the peer.
	Certificate tls.Certificate
	// Roots are the federation trust roots used to verify the peer.
	Roots *x509.CertPool
	// CRLs are the federation revocation lists used to verify the peer.
	CRLs []*x509.RevocationList
	// MaxCRLAge is the maximum allowed age of a CRL.
	MaxCRLAge time.Duration
}

func (s *PairingStore) Join(ctx context.Context, endpoint, code string, certificate tls.Certificate, roots *x509.CertPool, crls []*x509.RevocationList, maxCRLAge time.Duration) (Membership, error) {
	return s.JoinExpected(ctx, endpoint, JoinExpectedOptions{
		Code:        code,
		Certificate: certificate,
		Roots:       roots,
		CRLs:        crls,
		MaxCRLAge:   maxCRLAge,
	})
}

func (s *PairingStore) JoinExpected(ctx context.Context, endpoint string, options JoinExpectedOptions) (Membership, error) {
	if err := validateJoinOptions(options); err != nil {
		return Membership{}, ErrInvitation
	}
	if options.ExpectedNodeID != "" && !identity.ValidNodeID(options.ExpectedNodeID) {
		return Membership{}, ErrInvitation
	}
	origin, err := validEndpoint(endpoint)
	if err != nil {
		return Membership{}, ErrInvitation
	}
	leaf, err := localLeafCertificate(options.Certificate)
	if err != nil {
		return Membership{}, ErrInvitation
	}
	if err := ValidateIdentityCertificate(leaf, s.federationID, s.localNodeID); err != nil {
		return Membership{}, ErrInvitation
	}
	transport := newJoinTransport(s, options)
	defer transport.CloseIdleConnections()
	client := newJoinClient(transport)
	response, err := sendEnrollRequest(ctx, client, origin, options.Code)
	if err != nil {
		return Membership{}, ErrInvitation
	}
	if err := validateEnrollResponse(response); err != nil {
		return Membership{}, ErrInvitation
	}
	result, err := readAndDecodeEnrollResponse(response)
	if err != nil {
		return Membership{}, ErrInvitation
	}
	if err := validateEnrollResult(result, s, options.ExpectedNodeID); err != nil {
		return Membership{}, ErrInvitation
	}
	remoteNodeID, err := verifyServerIdentity(response, result, s.federationID)
	if err != nil {
		return Membership{}, ErrInvitation
	}
	return s.AddMember(remoteNodeID, CertificateFingerprint(response.TLS.PeerCertificates[0]), time.Now().UTC())
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
	if err := validatePairingDataRoot(data, federationID, localNodeID); err != nil {
		return ErrPairingStore
	}
	if err := validatePairingDataMembers(data.Members, localNodeID); err != nil {
		return ErrPairingStore
	}
	if err := validatePairingDataInvites(data.Invites, localNodeID); err != nil {
		return ErrPairingStore
	}
	if err := validatePairingDataConsumed(data.Consumed, data.Invites, localNodeID); err != nil {
		return ErrPairingStore
	}
	if err := validatePairingDataRevoked(data.Revoked, localNodeID); err != nil {
		return ErrPairingStore
	}
	return nil
}

// validatePairingDataRoot enforces the global store header and size limits.
func validatePairingDataRoot(data pairingData, federationID, localNodeID string) error {
	if data.Version != 1 || data.FederationID != federationID || data.LocalNodeID != localNodeID ||
		data.Members == nil || data.Invites == nil || data.Consumed == nil || data.Revoked == nil ||
		len(data.Members) > maxPairingMembers || len(data.Invites) > maxPairingInvites || len(data.Consumed) > maxConsumedInvites ||
		revokedFingerprintCount(data.Revoked) > maxRevocations {
		return ErrPairingStore
	}
	return nil
}

// validatePairingDataMembers validates each member row and duplicate fingerprints.
func validatePairingDataMembers(members map[string]Membership, localNodeID string) error {
	fingerprints := make(map[string]bool, len(members))
	for id, member := range members {
		fingerprint := strings.ToLower(member.Fingerprint)
		if !identity.ValidNodeID(id) || id == localNodeID || member.NodeID != id || !validFingerprint(fingerprint) ||
			member.PairedAt.IsZero() || fingerprints[fingerprint] {
			return ErrPairingStore
		}
		fingerprints[fingerprint] = true
	}
	return nil
}

// validatePairingDataInvites validates each invite row and duplicate node IDs.
func validatePairingDataInvites(invites map[string]inviteRecord, localNodeID string) error {
	inviteIDs := make(map[string]bool, len(invites))
	for hash, invite := range invites {
		if !validHash(hash) || !identity.ValidNodeID(invite.NodeID) || invite.NodeID == localNodeID ||
			inviteIDs[invite.NodeID] || invite.CreatedAt.IsZero() || !invite.ExpiresAt.After(invite.CreatedAt) || invite.ExpiresAt.Sub(invite.CreatedAt) > invitationLifetime {
			return ErrPairingStore
		}
		inviteIDs[invite.NodeID] = true
	}
	return nil
}

// validatePairingDataConsumed validates each consumed row and its exclusion from
// the active invite set.
func validatePairingDataConsumed(consumed map[string]consumedRecord, invites map[string]inviteRecord, localNodeID string) error {
	for hash, consumed := range consumed {
		if !validHash(hash) || !identity.ValidNodeID(consumed.NodeID) || consumed.NodeID == localNodeID || consumed.ExpiresAt.IsZero() {
			return ErrPairingStore
		}
		if _, duplicate := invites[hash]; duplicate {
			return ErrPairingStore
		}
	}
	return nil
}

// validatePairingDataRevoked validates each revoked fingerprint list.
func validatePairingDataRevoked(revoked map[string][]string, localNodeID string) error {
	for nodeID, revoked := range revoked {
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

// validateJoinOptions checks the invitation code, trust roots, CRLs and client
// certificate supplied to JoinExpected. It returns ErrInvitation for any unsafe input.
func validateJoinOptions(options JoinExpectedOptions) error {
	if !validInvitationCode(options.Code) || options.Roots == nil || len(options.CRLs) == 0 || len(options.Certificate.Certificate) == 0 {
		return ErrInvitation
	}
	return nil
}

// localLeafCertificate returns the leaf certificate of the local peer identity,
// parsing the first certificate of the chain when the transport has not pre-parsed it.
func localLeafCertificate(certificate tls.Certificate) (*x509.Certificate, error) {
	if certificate.Leaf != nil {
		return certificate.Leaf, nil
	}
	return x509.ParseCertificate(certificate.Certificate[0])
}

// newJoinTransport builds the HTTP transport used by JoinExpected, including the
// TLS VerifyConnection callback that validates the peer certificate, revocation
// state and expected node identity before any request is sent, and a DialContext
// that validates the resolved destination IP at dial time, pins DNS results to
// prevent rebinding, and preserves the mDNS/LAN pairing path (.local hostnames).
func newJoinTransport(s *PairingStore, options JoinExpectedOptions) *http.Transport {
	return &http.Transport{
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12, RootCAs: options.Roots, Certificates: []tls.Certificate{options.Certificate},
			VerifyConnection: func(state tls.ConnectionState) error {
				return s.verifyPeerConnection(state, options)
			},
		},
		DialContext:        s.joinDialContext,
		DisableCompression: true, MaxConnsPerHost: 1, MaxIdleConnsPerHost: 1,
		TLSHandshakeTimeout: 5 * time.Second, IdleConnTimeout: 10 * time.Second,
	}
}

// defaultLookupNetIP is the production DNS resolution function. It is set once
// at package initialization and never mutated, so it is safe for concurrent
// use across stores and goroutines.
var defaultLookupNetIP = func(ctx context.Context, netw, name string) ([]net.IP, error) {
	addrs, err := net.DefaultResolver.LookupNetIP(ctx, netw, name)
	if err != nil {
		return nil, err
	}
	var ips []net.IP
	for _, addr := range addrs {
		ips = append(ips, net.ParseIP(addr.String()))
	}
	return ips, nil
}

// joinDialContext is the DialContext for the join transport. It resolves the
// endpoint host to IP addresses at dial time, rejects private/reserved
// destinations unless the hostname is an mDNS/LAN peer (suffix .local) and the
// resolved IP matches the discovery LAN address policy, pins the DNS results by
// dialing the resolved IP directly (preventing DNS rebinding), and returns the
// first successful connection. Loopback is allowed for local development and
// testing; mTLS peer certificate verification authenticates the application
// data channel after the TCP connection is established.
func (s *PairingStore) joinDialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, ErrInvitation
	}
	// Strip IPv6 brackets so LookupNetIP receives a bare host.
	host = strings.Trim(host, "[]")
	// Resolve the host to IP addresses. This pins the DNS result so dialing the
	// resolved IP directly prevents DNS rebinding. The context deadline from
	// JoinExpected bounds the resolution.
	ips, err := s.lookupNetIP(ctx, "ip", host)
	if err != nil || len(ips) == 0 {
		return nil, ErrInvitation
	}
	isMDNS := strings.HasSuffix(strings.ToLower(host), ".local")
	var validIPs []string
	for _, ip := range ips {
		ipStr := ip.String()
		parsedIP := net.ParseIP(ipStr)
		if parsedIP == nil {
			continue
		}
		if validEndpointIP(parsedIP) {
			validIPs = append(validIPs, ipStr)
			continue
		}
		// Allow only LAN addresses per discovery.usableAddress for mDNS/LAN
		// peers (private RFC 1918/ULA that are global-unicast; excludes
		// loopback, link-local, multicast, unspecified, reserved, documentation).
		if isMDNS && validEndpointLANIP(parsedIP) {
			validIPs = append(validIPs, ipStr)
			continue
		}
		// Allow loopback for local development and testing.
		if parsedIP.IsLoopback() {
			validIPs = append(validIPs, ipStr)
			continue
		}
	}
	if len(validIPs) == 0 {
		return nil, ErrInvitation
	}
	// Dial the resolved IP directly (not the hostname) to prevent rebinding.
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	for _, ip := range validIPs {
		conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip, port))
		if err == nil {
			return conn, nil
		}
	}
	return nil, ErrInvitation
}

// verifyPeerConnection validates the peer TLS connection presented during enrollment.
// It is invoked by the transport's VerifyConnection callback.
func (s *PairingStore) verifyPeerConnection(state tls.ConnectionState, options JoinExpectedOptions) error {
	if len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 {
		return ErrInvitation
	}
	if err := VerifyCertificateRevocation(state.VerifiedChains, options.CRLs, options.MaxCRLAge, time.Now()); err != nil {
		return ErrInvitation
	}
	remoteID, err := PeerIdentityNodeID(state.PeerCertificates[0], s.federationID)
	if err != nil || (options.ExpectedNodeID != "" && remoteID != options.ExpectedNodeID) {
		return ErrInvitation
	}
	revoked, err := s.IsCertificateRevoked(remoteID, CertificateFingerprint(state.PeerCertificates[0]))
	if err != nil || revoked {
		return ErrInvitation
	}
	return nil
}

// newJoinClient builds the HTTP client used by JoinExpected. It rejects redirects and
// bounds the request lifetime.
func newJoinClient(transport *http.Transport) *http.Client {
	return &http.Client{Transport: transport, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

// sendEnrollRequest builds and executes the enroll request, presenting the invitation
// code as a request header. The origin is validated by validEndpoint before this
// function is called, so the outbound destination (scheme, host, port) is bounded
// to a user-provided but vetted peer endpoint; redirects are rejected by the
// client and TLS peer certificate verification is enforced by the transport.
func sendEnrollRequest(ctx context.Context, client *http.Client, origin, code string) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, origin+"/peer/v1/enroll", nil)
	if err != nil {
		return nil, ErrInvitation
	}
	request.Header.Set("X-Llama-Catalog-Invite", code)
	return client.Do(request)
}

// validateEnrollResponse checks the enroll response status and TLS peer evidence.
func validateEnrollResponse(response *http.Response) error {
	if response.StatusCode != http.StatusOK || response.TLS == nil || len(response.TLS.VerifiedChains) == 0 || len(response.TLS.PeerCertificates) == 0 {
		return ErrInvitation
	}
	return nil
}

// enrollResult is the decoded payload of the peer enroll endpoint.
type enrollResult struct {
	State        string    `json:"state"`
	FederationID string    `json:"federation_id"`
	LocalNodeID  string    `json:"local_node_id"`
	NodeID       string    `json:"node_id"`
	PairedAt     time.Time `json:"paired_at"`
}

// readAndDecodeEnrollResponse reads the enroll response body within the 4096 byte
// limit and decodes it, rejecting unknown and trailing fields.
func readAndDecodeEnrollResponse(response *http.Response) (enrollResult, error) {
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 4097))
	if err != nil || len(body) > 4096 {
		return enrollResult{}, ErrInvitation
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var result enrollResult
	if decoder.Decode(&result) != nil || decoder.Decode(new(any)) != io.EOF {
		return enrollResult{}, ErrInvitation
	}
	return result, nil
}

// validateEnrollResult checks the decoded enroll result against the local federation
// identity and the expected node ID.
func validateEnrollResult(result enrollResult, s *PairingStore, expectedNodeID string) error {
	if result.State != "paired" || result.FederationID != s.federationID || result.NodeID != s.localNodeID ||
		!identity.ValidNodeID(result.LocalNodeID) || (expectedNodeID != "" && result.LocalNodeID != expectedNodeID) {
		return ErrInvitation
	}
	return nil
}

// verifyServerIdentity checks that the peer TLS certificate identifies the same node
// the enroll response declared as the local node.
func verifyServerIdentity(response *http.Response, result enrollResult, federationID string) (string, error) {
	serverCert := response.TLS.PeerCertificates[0]
	remoteNodeID, err := PeerIdentityNodeID(serverCert, federationID)
	if err != nil || remoteNodeID != result.LocalNodeID {
		return "", ErrInvitation
	}
	return remoteNodeID, nil
}

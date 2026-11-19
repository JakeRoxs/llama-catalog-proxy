package federation

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/identity"
)

const (
	catalogGrantFilename = "catalog-grants.json"
	catalogGrantLockFile = ".catalog-grants.lock"
	maxCatalogGrantBytes = 128 << 10
	maxCatalogGrants     = 512
)

var (
	ErrCatalogGrantStore   = errors.New("catalog grant store unavailable")
	ErrCatalogGrantMissing = errors.New("catalog read grant not found")
)

type CatalogReadGrant struct {
	NodeID      string    `json:"node_id"`
	Fingerprint string    `json:"certificate_sha256"`
	GrantedAt   time.Time `json:"granted_at"`
}

type catalogGrantData struct {
	Version      int                         `json:"version"`
	FederationID string                      `json:"federation_id"`
	LocalNodeID  string                      `json:"local_node_id"`
	Grants       map[string]CatalogReadGrant `json:"catalog_read_grants"`
}

type CatalogGrantStore struct {
	mu           sync.Mutex
	root         *os.Root
	lock         *os.File
	directory    string
	federationID string
	localNodeID  string
	data         catalogGrantData
	closed       bool
	failed       bool
}

func OpenCatalogGrantStore(directory, federationID, localNodeID string) (*CatalogGrantStore, error) {
	if !filepath.IsAbs(directory) || filepath.Clean(directory) != directory ||
		!validFederationID(federationID) || !identity.ValidNodeID(localNodeID) {
		return nil, ErrCatalogGrantStore
	}
	info, err := os.Lstat(directory)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return nil, ErrCatalogGrantStore
	}
	if err := validatePairingStoreOwner(info); err != nil {
		return nil, ErrCatalogGrantStore
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, ErrCatalogGrantStore
	}
	store := &CatalogGrantStore{
		root: root, directory: directory, federationID: federationID, localNodeID: localNodeID,
		data: catalogGrantData{Version: 1, FederationID: federationID, LocalNodeID: localNodeID, Grants: make(map[string]CatalogReadGrant)},
	}
	if err := store.openLock(); err != nil {
		_ = root.Close()
		return nil, err
	}
	unlock, err := lockPairingStore(store.lock)
	if err != nil {
		_ = store.Close()
		return nil, ErrCatalogGrantStore
	}
	readErr := store.read()
	unlockErr := unlock()
	if readErr != nil || unlockErr != nil {
		_ = store.Close()
		return nil, ErrCatalogGrantStore
	}
	return store, nil
}

func (s *CatalogGrantStore) GrantCatalogRead(member Membership, now time.Time) (CatalogReadGrant, error) {
	if !identity.ValidNodeID(member.NodeID) || member.NodeID == s.localNodeID || !validFingerprint(member.Fingerprint) ||
		member.PairedAt.IsZero() || now.IsZero() {
		return CatalogReadGrant{}, ErrCatalogGrantStore
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.failed {
		return CatalogReadGrant{}, ErrCatalogGrantStore
	}
	unlock, err := s.lockAndRead()
	if err != nil {
		return CatalogReadGrant{}, err
	}
	defer unlock()
	if existing, ok := s.data.Grants[member.NodeID]; ok && strings.EqualFold(existing.Fingerprint, member.Fingerprint) {
		return existing, nil
	}
	for nodeID, existing := range s.data.Grants {
		if nodeID != member.NodeID && strings.EqualFold(existing.Fingerprint, member.Fingerprint) {
			return CatalogReadGrant{}, ErrCatalogGrantStore
		}
	}
	if _, ok := s.data.Grants[member.NodeID]; !ok && len(s.data.Grants) >= maxCatalogGrants {
		return CatalogReadGrant{}, ErrCatalogGrantStore
	}
	grant := CatalogReadGrant{NodeID: member.NodeID, Fingerprint: strings.ToLower(member.Fingerprint), GrantedAt: now.UTC()}
	s.data.Grants[member.NodeID] = grant
	if err := s.persist(); err != nil {
		return CatalogReadGrant{}, err
	}
	return grant, nil
}

func (s *CatalogGrantStore) RevokeCatalogRead(nodeID string) error {
	if !identity.ValidNodeID(nodeID) || nodeID == s.localNodeID {
		return ErrCatalogGrantMissing
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.failed {
		return ErrCatalogGrantStore
	}
	unlock, err := s.lockAndRead()
	if err != nil {
		return err
	}
	defer unlock()
	if _, ok := s.data.Grants[nodeID]; !ok {
		return ErrCatalogGrantMissing
	}
	delete(s.data.Grants, nodeID)
	return s.persist()
}

func (s *CatalogGrantStore) AllowsCatalogRead(nodeID, fingerprint string) (bool, error) {
	if !identity.ValidNodeID(nodeID) || !validFingerprint(fingerprint) {
		return false, ErrCatalogGrantStore
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.failed {
		return false, ErrCatalogGrantStore
	}
	unlock, err := s.lockAndRead()
	if err != nil {
		return false, err
	}
	defer unlock()
	grant, ok := s.data.Grants[nodeID]
	return ok && strings.EqualFold(grant.Fingerprint, fingerprint), nil
}

func (s *CatalogGrantStore) ListCatalogReadGrants() ([]CatalogReadGrant, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.failed {
		return nil, ErrCatalogGrantStore
	}
	unlock, err := s.lockAndRead()
	if err != nil {
		return nil, err
	}
	defer unlock()
	grants := make([]CatalogReadGrant, 0, len(s.data.Grants))
	for _, grant := range s.data.Grants {
		grants = append(grants, grant)
	}
	sort.Slice(grants, func(i, j int) bool { return grants[i].NodeID < grants[j].NodeID })
	return grants, nil
}

func (s *CatalogGrantStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	lockErr := s.lock.Close()
	rootErr := s.root.Close()
	if lockErr != nil || rootErr != nil {
		return ErrCatalogGrantStore
	}
	return nil
}

func (s *CatalogGrantStore) openLock() error {
	if info, err := s.root.Lstat(catalogGrantLockFile); err == nil {
		if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
			return ErrCatalogGrantStore
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return ErrCatalogGrantStore
	}
	lock, err := s.root.OpenFile(catalogGrantLockFile, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return ErrCatalogGrantStore
	}
	if err := lock.Chmod(0o600); err != nil || lockInfoUnsafe(lock) {
		_ = lock.Close()
		return ErrCatalogGrantStore
	}
	s.lock = lock
	return nil
}

func (s *CatalogGrantStore) lockAndRead() (func() error, error) {
	unlock, err := lockPairingStore(s.lock)
	if err != nil {
		return nil, ErrCatalogGrantStore
	}
	if err := s.read(); err != nil {
		s.failed = true
		_ = unlock()
		return nil, ErrCatalogGrantStore
	}
	return unlock, nil
}

func (s *CatalogGrantStore) read() error {
	s.data = catalogGrantData{
		Version: 1, FederationID: s.federationID, LocalNodeID: s.localNodeID,
		Grants: make(map[string]CatalogReadGrant),
	}
	pathInfo, err := s.root.Lstat(catalogGrantFilename)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || !pathInfo.Mode().IsRegular() || pathInfo.Mode().Perm()&0o077 != 0 || pathInfo.Size() > maxCatalogGrantBytes {
		return ErrCatalogGrantStore
	}
	file, err := s.root.Open(catalogGrantFilename)
	if err != nil {
		return ErrCatalogGrantStore
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() > maxCatalogGrantBytes {
		return ErrCatalogGrantStore
	}
	data, err := io.ReadAll(io.LimitReader(file, maxCatalogGrantBytes+1))
	if err != nil || len(data) > maxCatalogGrantBytes {
		return ErrCatalogGrantStore
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var stored catalogGrantData
	if err := decoder.Decode(&stored); err != nil {
		return ErrCatalogGrantStore
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) || validateCatalogGrantData(stored, s.federationID, s.localNodeID) != nil {
		return ErrCatalogGrantStore
	}
	s.data = stored
	return nil
}

func (s *CatalogGrantStore) persist() error {
	data, err := json.Marshal(s.data)
	if err != nil || len(data) > maxCatalogGrantBytes {
		s.failed = true
		return ErrCatalogGrantStore
	}
	suffix, err := newCatalogGrantTempSuffix()
	if err != nil {
		s.failed = true
		return ErrCatalogGrantStore
	}
	tempName := ".catalog-grants-" + suffix + ".tmp"
	temp, err := s.root.OpenFile(tempName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		s.failed = true
		return ErrCatalogGrantStore
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
		return ErrCatalogGrantStore
	}
	if err := os.Rename(filepath.Join(s.directory, tempName), filepath.Join(s.directory, catalogGrantFilename)); err != nil {
		_ = s.root.Remove(tempName)
		s.failed = true
		return ErrCatalogGrantStore
	}
	dir, err := s.root.Open(".")
	if err == nil {
		err = dir.Sync()
		_ = dir.Close()
	}
	if err != nil {
		s.failed = true
		return ErrCatalogGrantStore
	}
	return nil
}

func validateCatalogGrantData(data catalogGrantData, federationID, localNodeID string) error {
	if data.Version != 1 || data.FederationID != federationID || data.LocalNodeID != localNodeID ||
		data.Grants == nil || len(data.Grants) > maxCatalogGrants {
		return ErrCatalogGrantStore
	}
	fingerprints := make(map[string]struct{}, len(data.Grants))
	for nodeID, grant := range data.Grants {
		fingerprint := strings.ToLower(grant.Fingerprint)
		if !identity.ValidNodeID(nodeID) || nodeID == localNodeID || grant.NodeID != nodeID ||
			!validFingerprint(fingerprint) || grant.GrantedAt.IsZero() {
			return ErrCatalogGrantStore
		}
		if _, duplicate := fingerprints[fingerprint]; duplicate {
			return ErrCatalogGrantStore
		}
		fingerprints[fingerprint] = struct{}{}
	}
	return nil
}

func newCatalogGrantTempSuffix() (string, error) {
	secret := make([]byte, 12)
	if _, err := rand.Read(secret); err != nil {
		return "", err
	}
	return hex.EncodeToString(secret), nil
}

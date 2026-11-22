//go:build linux

package modelconfig

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/identity"
	"golang.org/x/sys/unix"
)

const delegatedProposalPrefix = "delegated-"
const delegatedProposalAuditFilename = "delegated-audit.json"
const MaxDelegatedProposalRecoveryAuditRecords = 256
const maxDelegatedProposalAuditBytes = 256 << 10

func delegatedProposalFilename(id string) string {
	return delegatedProposalPrefix + id + ".json"
}

// SaveDelegatedProposal stores a review in a private, dedicated directory.
// Storage does not authorize an action or prevent replay.
func SaveDelegatedProposal(directory string, proposal DelegatedProposal) error {
	if !validDelegatedProposal(proposal) {
		return ErrProposalInvalid
	}
	data, err := json.Marshal(proposal)
	if err != nil || len(data) > maxProposalBytes {
		return ErrProposalStore
	}
	return withProposalStore(directory, func(root *os.Root, dir *os.File) error {
		ids, err := delegatedProposalNames(root, dir)
		if err != nil {
			return err
		}
		if len(ids) >= MaxProposals {
			return ErrProposalStoreFull
		}
		return writePrivateRecord(root, dir, delegatedProposalFilename(proposal.Proposal.ID), data, ErrProposalStore)
	})
}

// LoadDelegatedProposal is a point-in-time read, not authorization. A future
// write must hold the store lock while rechecking the binding and applying.
func LoadDelegatedProposal(directory, id string) (DelegatedProposal, error) {
	if !validProposalID(id) {
		return DelegatedProposal{}, ErrProposalNotFound
	}
	var result DelegatedProposal
	err := withProposalStore(directory, func(root *os.Root, _ *os.File) error {
		var err error
		result, err = readDelegatedProposal(root, id)
		return err
	})
	if err != nil {
		return DelegatedProposal{}, err
	}
	return result, nil
}

func readDelegatedProposal(root *os.Root, id string) (DelegatedProposal, error) {
	if !validProposalID(id) {
		return DelegatedProposal{}, ErrProposalNotFound
	}
	data, err := readPrivateProposalFile(root, delegatedProposalFilename(id))
	if err != nil {
		return DelegatedProposal{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var result DelegatedProposal
	if err := decoder.Decode(&result); err != nil || result.Proposal.ID != id || !validDelegatedProposal(result) {
		return DelegatedProposal{}, ErrProposalStore
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return DelegatedProposal{}, ErrProposalStore
	}
	return result, nil
}

func ListDelegatedProposals(directory string) ([]string, error) {
	var ids []string
	err := withProposalStore(directory, func(root *os.Root, dir *os.File) error {
		var err error
		ids, err = delegatedProposalNames(root, dir)
		return err
	})
	return ids, err
}

// RecoverDelegatedProposal returns a unique exact request match and records a
// bounded audit event on the matched owner record. The caller must pass the
// authenticated requester and its established credential scope. No request
// identity or timestamp is inferred from proposal creation time.
func RecoverDelegatedProposal(directory string, query DelegatedProposalRecoveryQuery, credentialScope string, now time.Time) (DelegatedProposal, error) {
	if !validDelegatedIdentity(query.Owner, query.RequesterNode, query.BackendID) ||
		!identity.ValidNodeID(query.TargetID) || !identity.ValidModelComponent(query.ModelID) || !validDigest(query.Revision) ||
		!validRecoverySettings(query.Settings) || credentialScope != DelegatedSettingsCredentialScope || now.IsZero() {
		return DelegatedProposal{}, ErrProposalInvalid
	}
	requestBytes, err := json.Marshal(query)
	if err != nil {
		return DelegatedProposal{}, ErrProposalInvalid
	}
	requestDigest := sha256.Sum256(requestBytes)
	var result DelegatedProposal
	err = withProposalStore(directory, func(root *os.Root, dir *os.File) error {
		ids, err := delegatedProposalNames(root, dir)
		if err != nil {
			return err
		}
		matches := make([]DelegatedProposal, 0, 2)
		for _, id := range ids {
			proposal, err := readDelegatedProposal(root, id)
			if err != nil {
				return err
			}
			if proposal.Owner == query.Owner && proposal.RequesterNode == query.RequesterNode &&
				proposal.BackendID == query.BackendID && proposal.Proposal.ModelID == query.ModelID &&
				proposal.Proposal.TargetID == query.TargetID && proposal.Proposal.Revision == query.Revision &&
				reflect.DeepEqual(proposal.Proposal.Settings, query.Settings) {
				matches = append(matches, proposal)
			}
		}
		if len(matches) == 0 {
			return ErrProposalNotFound
		}
		outcome := "recovered"
		if len(matches) > 1 {
			outcome = "ambiguous"
		} else if !now.Before(matches[0].ExpiresAt) {
			outcome = "expired"
		}
		for _, proposal := range matches {
			proposal.RecoveryAudit = append(proposal.RecoveryAudit, DelegatedProposalRecoveryAudit{
				At: now.UTC(), ProposalID: proposal.Proposal.ID, RequesterNode: query.RequesterNode, CredentialScope: credentialScope,
				OwnerNode: query.Owner.NodeID, TargetID: proposal.Proposal.TargetID,
				BackendID: query.BackendID, ModelID: query.ModelID, SourceRevision: query.Revision,
				ReviewRevision: proposal.ReviewRevision, RequestDigest: hex.EncodeToString(requestDigest[:]), Outcome: outcome,
			})
			if len(proposal.RecoveryAudit) > MaxDelegatedProposalRecoveryAudit {
				proposal.RecoveryAudit = append([]DelegatedProposalRecoveryAudit(nil),
					proposal.RecoveryAudit[len(proposal.RecoveryAudit)-MaxDelegatedProposalRecoveryAudit:]...)
			}
			data, err := json.Marshal(proposal)
			if err != nil || len(data) > maxProposalBytes || !validDelegatedProposal(proposal) {
				return ErrProposalStore
			}
			if err := replaceDelegatedProposal(root, dir, proposal.Proposal.ID, data); err != nil {
				return err
			}
			if len(matches) == 1 {
				result = proposal
			}
		}
		if len(matches) > 1 {
			return ErrDelegatedProposalAmbiguous
		}
		return nil
	})
	if err != nil {
		return DelegatedProposal{}, err
	}
	return result, nil
}

func validRecoverySettings(settings CommandSettings) bool {
	if settings.ContextSize == nil && settings.Fit == nil && settings.FitContext == nil &&
		settings.FitTarget == nil && settings.CacheRAM == nil {
		return false
	}
	return (settings.ContextSize == nil || *settings.ContextSize >= 0) &&
		(settings.FitContext == nil || *settings.FitContext >= 0) &&
		(settings.FitTarget == nil || *settings.FitTarget >= 0) &&
		(settings.CacheRAM == nil || *settings.CacheRAM >= -1)
}

// DeleteDelegatedProposal removes a review, not any future replay or audit record.
func DeleteDelegatedProposal(directory, id string) error {
	if !validProposalID(id) {
		return ErrProposalNotFound
	}
	return withProposalStore(directory, func(root *os.Root, dir *os.File) error {
		if err := root.Remove(delegatedProposalFilename(id)); errors.Is(err, os.ErrNotExist) {
			return ErrProposalNotFound
		} else if err != nil {
			return ErrProposalStore
		}
		if err := dir.Sync(); err != nil {
			return ErrProposalStore
		}
		return nil
	})
}

// ListExpiredOrphanedDelegatedProposals returns reviews whose stored expiry
// has passed and which have no operation record. It holds both private-store
// locks in the same order as ApplyStoredDelegatedProposal and never changes the
// operation store.
func ListExpiredOrphanedDelegatedProposals(proposalDirectory, operationDirectory string, now time.Time) ([]DelegatedProposalCleanupCandidate, error) {
	if now.IsZero() {
		return nil, ErrProposalInvalid
	}
	var candidates []DelegatedProposalCleanupCandidate
	err := withDelegatedProposalStores(proposalDirectory, operationDirectory, func(proposals *os.Root, proposalDir *os.File, operations *os.Root, operationDir *os.File) error {
		operationIDs, err := delegatedOperationProposalIDsReadOnly(operations, operationDir)
		if err != nil {
			return err
		}
		ids, err := delegatedProposalNames(proposals, proposalDir)
		if err != nil {
			return err
		}
		for _, id := range ids {
			proposal, err := readDelegatedProposal(proposals, id)
			if err != nil {
				return err
			}
			if now.Before(proposal.ExpiresAt) || operationIDs[id] {
				continue
			}
			candidates = append(candidates, DelegatedProposalCleanupCandidate{
				ProposalID: id, RequesterNode: proposal.RequesterNode,
				BackendID: proposal.BackendID, ModelID: proposal.Proposal.ModelID,
				ExpiresAt: proposal.ExpiresAt,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return candidates, nil
}

// CleanupExpiredOrphanedDelegatedProposals explicitly removes only the named
// expired reviews that still have no operation record. Missing IDs are treated
// as already cleaned so callers can safely repeat a cleanup request. The
// operation store is inspected read-only and is never pruned or rewritten.
func CleanupExpiredOrphanedDelegatedProposals(proposalDirectory, operationDirectory string, proposalIDs []string, now time.Time) ([]string, error) {
	if now.IsZero() || len(proposalIDs) > MaxProposals {
		return nil, ErrProposalInvalid
	}
	ids := make([]string, 0, len(proposalIDs))
	seen := make(map[string]bool, len(proposalIDs))
	for _, id := range proposalIDs {
		if !validProposalID(id) {
			return nil, ErrProposalInvalid
		}
		if !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	if len(ids) == 0 {
		return []string{}, nil
	}

	var removed []string
	err := withDelegatedProposalStores(proposalDirectory, operationDirectory, func(proposals *os.Root, proposalDir *os.File, operations *os.Root, operationDir *os.File) error {
		if _, err := readDelegatedProposalRecoveryAudit(proposals, delegatedProposalAuditFilename); err != nil {
			return err
		}
		operationIDs, err := delegatedOperationProposalIDsReadOnly(operations, operationDir)
		if err != nil {
			return err
		}
		for _, id := range ids {
			proposal, err := readDelegatedProposal(proposals, id)
			if errors.Is(err, ErrProposalNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if now.Before(proposal.ExpiresAt) || operationIDs[id] {
				return ErrDelegatedProposalCleanup
			}
		}
		var toRemove []string
		var auditEvents []DelegatedProposalRecoveryAudit
		for _, id := range ids {
			proposal, err := readDelegatedProposal(proposals, id)
			if errors.Is(err, ErrProposalNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			toRemove = append(toRemove, id)
			auditEvents = append(auditEvents, proposal.RecoveryAudit...)
		}
		if len(auditEvents) != 0 {
			if err := appendDelegatedProposalRecoveryAudit(proposals, proposalDir, auditEvents); err != nil {
				return err
			}
		}
		for _, id := range toRemove {
			if err := proposals.Remove(delegatedProposalFilename(id)); err != nil {
				return ErrProposalStore
			}
			removed = append(removed, id)
		}
		if len(removed) != 0 {
			if err := proposalDir.Sync(); err != nil {
				return ErrProposalStore
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return removed, nil
}

func withDelegatedProposalStores(proposalDirectory, operationDirectory string, fn func(*os.Root, *os.File, *os.Root, *os.File) error) error {
	proposalInfo, err := os.Stat(proposalDirectory)
	if err != nil {
		return ErrProposalStore
	}
	operationInfo, err := os.Stat(operationDirectory)
	if err != nil || os.SameFile(proposalInfo, operationInfo) {
		return ErrProposalStore
	}
	return withProposalStore(proposalDirectory, func(proposals *os.Root, proposalDir *os.File) error {
		return withProposalStore(operationDirectory, func(operations *os.Root, operationDir *os.File) error {
			return fn(proposals, proposalDir, operations, operationDir)
		})
	})
}

// delegatedProposalNames lists the proposal IDs stored in the delegated
// proposal directory, sorted. It bounds the directory scan at MaxProposals+2
// entries and rejects the audit file, temporary files, unexpected names,
// invalid IDs, and directories. The audit file is validated but is never
// returned as a proposal ID.
func delegatedProposalNames(root *os.Root, dir *os.File) ([]string, error) {
	entries, err := dir.ReadDir(MaxProposals + 2)
	if err != nil && err != io.EOF || len(entries) > MaxProposals+1 {
		return nil, ErrProposalStore
	}
	ids := make([]string, 0, len(entries))
	for _, entry := range entries {
		if isDelegatedAuditEntry(entry) {
			if err := validateDelegatedAuditEntry(root, entry); err != nil {
				return nil, err
			}
			continue
		}
		id, ok := parseDelegatedProposalName(entry)
		if !ok {
			return nil, ErrProposalStore
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}

// isDelegatedAuditEntry reports whether a directory entry is the recovery
// audit file. The audit file is validated separately and is never returned
// as a proposal ID.
func isDelegatedAuditEntry(entry os.DirEntry) bool {
	return entry.Name() == delegatedProposalAuditFilename
}

// validateDelegatedAuditEntry rejects a directory that shares the audit file
// name and validates the audit file contents. Audit errors are propagated to
// the caller.
func validateDelegatedAuditEntry(root *os.Root, entry os.DirEntry) error {
	if entry.IsDir() {
		return ErrProposalStore
	}
	if _, err := readDelegatedProposalRecoveryAudit(root, entry.Name()); err != nil {
		return err
	}
	return nil
}

// parseDelegatedProposalName classifies a directory entry as a delegated
// proposal file and extracts its proposal ID. It rejects temporary files,
// names that do not match the exact delegated prefix and JSON suffix,
// invalid IDs, and directories.
func parseDelegatedProposalName(entry os.DirEntry) (string, bool) {
	name := entry.Name()
	if strings.HasPrefix(name, ".delegated-") && strings.HasSuffix(name, ".tmp") {
		return "", false
	}
	if !strings.HasPrefix(name, delegatedProposalPrefix) || !strings.HasSuffix(name, ".json") {
		return "", false
	}
	id := strings.TrimSuffix(strings.TrimPrefix(name, delegatedProposalPrefix), ".json")
	if !validProposalID(id) || entry.IsDir() {
		return "", false
	}
	return id, true
}

func replaceDelegatedProposal(root *os.Root, dir *os.File, id string, data []byte) (result error) {
	if len(data) > maxProposalBytes || !validProposalID(id) {
		return ErrProposalStore
	}
	return replaceDelegatedProposalFile(root, dir, delegatedProposalFilename(id), data, maxProposalBytes)
}

func readDelegatedProposalRecoveryAudit(root *os.Root, name string) ([]DelegatedProposalRecoveryAudit, error) {
	if name != delegatedProposalAuditFilename {
		return nil, ErrProposalStore
	}
	data, err := readPrivateProposalFileLimit(root, name, maxDelegatedProposalAuditBytes)
	if errors.Is(err, ErrProposalNotFound) {
		return []DelegatedProposalRecoveryAudit{}, nil
	}
	if err != nil {
		return nil, ErrProposalStore
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var records []DelegatedProposalRecoveryAudit
	if err := decoder.Decode(&records); err != nil || len(records) > MaxDelegatedProposalRecoveryAuditRecords {
		return nil, ErrProposalStore
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return nil, ErrProposalStore
	}
	for _, event := range records {
		if !validDelegatedProposalRecoveryAudit(event) {
			return nil, ErrProposalStore
		}
	}
	return records, nil
}

func validDelegatedProposalRecoveryAudit(event DelegatedProposalRecoveryAudit) bool {
	return !event.At.IsZero() && validProposalID(event.ProposalID) && identity.ValidNodeID(event.RequesterNode) &&
		event.CredentialScope == DelegatedSettingsCredentialScope && identity.ValidNodeID(event.OwnerNode) &&
		identity.ValidNodeID(event.TargetID) && event.BackendID != "" && identity.ValidModelComponent(event.BackendID) &&
		identity.ValidModelComponent(event.ModelID) && validDigest(event.SourceRevision) &&
		validDigest(event.ReviewRevision) &&
		(event.Outcome == "recovered" || event.Outcome == "expired" || event.Outcome == "ambiguous")
}

func appendDelegatedProposalRecoveryAudit(root *os.Root, dir *os.File, events []DelegatedProposalRecoveryAudit) error {
	records, err := readDelegatedProposalRecoveryAudit(root, delegatedProposalAuditFilename)
	if err != nil {
		return err
	}
	seen := make(map[DelegatedProposalRecoveryAudit]bool, len(records)+len(events))
	for _, event := range records {
		seen[event] = true
	}
	for _, event := range events {
		if !validDelegatedProposalRecoveryAudit(event) {
			return ErrProposalStore
		}
		if !seen[event] {
			records = append(records, event)
			seen[event] = true
		}
	}
	if len(records) > MaxDelegatedProposalRecoveryAuditRecords {
		records = append([]DelegatedProposalRecoveryAudit(nil),
			records[len(records)-MaxDelegatedProposalRecoveryAuditRecords:]...)
	}
	data, err := json.Marshal(records)
	if err != nil || len(data) > maxDelegatedProposalAuditBytes {
		return ErrProposalStore
	}
	return replaceDelegatedProposalFile(root, dir, delegatedProposalAuditFilename, data, maxDelegatedProposalAuditBytes)
}

func replaceDelegatedProposalFile(root *os.Root, dir *os.File, name string, data []byte, maxBytes int) (result error) {
	if len(data) > maxBytes || name == "" || strings.ContainsAny(name, "/\\") {
		return ErrProposalStore
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return ErrProposalStore
	}
	temp := ".delegated-" + hex.EncodeToString(random[:]) + ".tmp"
	file, err := root.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return ErrProposalStore
	}
	defer func() {
		if err := root.Remove(temp); err == nil {
			if err := dir.Sync(); err != nil {
				result = errors.Join(result, ErrProposalStore)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, ErrProposalStore)
		}
	}()
	written, writeErr := file.Write(data)
	if writeErr == nil && written != len(data) {
		writeErr = io.ErrShortWrite
	}
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		return ErrProposalStore
	}
	if err := unix.Renameat(int(dir.Fd()), temp, int(dir.Fd()), name); err != nil {
		return ErrProposalStore
	}
	if err := dir.Sync(); err != nil {
		return ErrProposalStore
	}
	return nil
}

//go:build linux

package modelconfig

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"slices"
	"sort"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/identity"
)

const (
	maxBackupLimit = MaxBackups
	maxBackupBytes = (maxConfigBytes+2)/3*4 + 16<<10
)

var errBackupStore = ErrBackupStore
var errRollbackStale = ErrRollbackStale

type backupInfo = BackupInfo

type backupRecord struct {
	Info   backupInfo `json:"info"`
	Source []byte     `json:"source"`
}

// The caller must hold the managed-write lock and an operator-controlled
// exclusive edit window through replacement.
func prepareBackup(directory, filename, targetID string, proposal Proposal, source []byte) (backupRecord, error) {
	if !validProposal(proposal) {
		return backupRecord{}, ErrProposalInvalid
	}
	fingerprint, err := targetFingerprint(directory, filename)
	if err != nil {
		return backupRecord{}, errBackupStore
	}
	if targetID != proposal.TargetID || fingerprint != proposal.TargetFingerprint {
		return backupRecord{}, ErrProposalTarget
	}
	if sourceRevision(source) != proposal.Revision {
		return backupRecord{}, ErrProposalStale
	}
	preview, err := PreviewDocument(targetID, proposal.ModelID, source, proposal.Settings)
	if err != nil {
		return backupRecord{}, err
	}
	if preview.Validation != proposal.Validation || !slices.Equal(preview.Changes, proposal.Changes) {
		return backupRecord{}, ErrProposalReview
	}
	return newBackupRecord(targetID, fingerprint, proposal.ModelID, proposal.ID, source, preview.candidate)
}

func newBackupRecord(targetID, fingerprint, modelID, proposalID string, source, candidate []byte) (backupRecord, error) {
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return backupRecord{}, errBackupStore
	}
	return backupRecord{
		Info: backupInfo{
			ID: hex.EncodeToString(id), CreatedAt: time.Now().UTC(), TargetID: targetID,
			TargetFingerprint: fingerprint, ModelID: modelID, ProposalID: proposalID,
			SourceRevision: sourceRevision(source), WrittenRevision: sourceRevision(candidate),
		},
		Source: append([]byte(nil), source...),
	}, nil
}

func validBackup(record backupRecord) bool {
	info := record.Info
	return validProposalID(info.ID) && !info.CreatedAt.IsZero() &&
		identity.ValidNodeID(info.TargetID) && info.TargetID != "." && info.TargetID != ".." &&
		info.ModelID != "" && identity.ValidModelComponent(info.ModelID) && validProposalID(info.ProposalID) &&
		validDigest(info.TargetFingerprint) && validDigest(info.SourceRevision) && validDigest(info.WrittenRevision) &&
		len(record.Source) > 0 && len(record.Source) <= maxConfigBytes && utf8.Valid(record.Source) &&
		sourceRevision(record.Source) == info.SourceRevision
}

// prepareRollback is only a content check; the caller must hold the
// managed-write lock and an operator-controlled exclusive edit window.
func prepareRollback(directory, filename, targetID string, record backupRecord, current []byte) ([]byte, error) {
	if !validBackup(record) {
		return nil, errBackupStore
	}
	fingerprint, err := targetFingerprint(directory, filename)
	if err != nil {
		return nil, errBackupStore
	}
	if targetID != record.Info.TargetID || fingerprint != record.Info.TargetFingerprint {
		return nil, ErrProposalTarget
	}
	if sourceRevision(current) != record.Info.WrittenRevision {
		return nil, errRollbackStale
	}
	return append([]byte(nil), record.Source...), nil
}

func backupFilename(id string) string { return "backup-" + id + ".json" }

func readBackup(root *os.Root, id string) (backupRecord, error) {
	if !validProposalID(id) {
		return backupRecord{}, errBackupStore
	}
	file, err := root.OpenFile(backupFilename(id), os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return backupRecord{}, errBackupStore
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() > maxBackupBytes {
		return backupRecord{}, errBackupStore
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) || stat.Nlink != 1 {
		return backupRecord{}, errBackupStore
	}
	decoder := json.NewDecoder(io.LimitReader(file, maxBackupBytes+1))
	decoder.DisallowUnknownFields()
	var record backupRecord
	if err := decoder.Decode(&record); err != nil || record.Info.ID != id || !validBackup(record) {
		return backupRecord{}, errBackupStore
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return backupRecord{}, errBackupStore
	}
	return record, nil
}

func loadBackup(directory, id string) (backupRecord, error) {
	var record backupRecord
	err := withPrivateStore(directory, errBackupStore, func(root *os.Root, _ *os.File) error {
		var err error
		record, err = readBackup(root, id)
		return err
	})
	if err != nil {
		return backupRecord{}, err
	}
	return record, nil
}

func saveBackup(directory string, record backupRecord, limit int) error {
	if limit < 1 || limit > maxBackupLimit || !validBackup(record) {
		return errBackupStore
	}
	return withPrivateStore(directory, errBackupStore, func(root *os.Root, dir *os.File) error {
		backups, err := backupInventory(root, dir, limit)
		if err != nil {
			return err
		}
		if err := writeBackup(root, dir, record); err != nil {
			return err
		}
		return rotateBackup(root, dir, backups, limit)
	})
}

func backupInventory(root *os.Root, dir *os.File, limit int) ([]backupInfo, error) {
	if limit < 1 || limit > maxBackupLimit {
		return nil, errBackupStore
	}
	entries, err := dir.ReadDir(maxBackupLimit + 2)
	if err != nil && err != io.EOF || len(entries) > limit {
		return nil, errBackupStore
	}
	backups := make([]backupInfo, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "backup-") || !strings.HasSuffix(name, ".json") {
			return nil, errBackupStore
		}
		id := strings.TrimSuffix(strings.TrimPrefix(name, "backup-"), ".json")
		existing, err := readBackup(root, id)
		if err != nil {
			return nil, errBackupStore
		}
		backups = append(backups, existing.Info)
	}
	sort.Slice(backups, func(i, j int) bool {
		if backups[i].CreatedAt.Equal(backups[j].CreatedAt) {
			return backups[i].ID < backups[j].ID
		}
		return backups[i].CreatedAt.Before(backups[j].CreatedAt)
	})
	return backups, nil
}

func writeBackup(root *os.Root, dir *os.File, record backupRecord) error {
	if !validBackup(record) {
		return errBackupStore
	}
	data, err := json.Marshal(record)
	if err != nil || len(data) > maxBackupBytes {
		return errBackupStore
	}
	return writePrivateRecord(root, dir, backupFilename(record.Info.ID), data, errBackupStore)
}

func rotateBackup(root *os.Root, dir *os.File, backups []backupInfo, limit int) error {
	if len(backups) != limit {
		return nil
	}
	if err := root.Remove(backupFilename(backups[0].ID)); err != nil {
		return errBackupStore
	}
	if err := dir.Sync(); err != nil {
		return errBackupStore
	}
	return nil
}

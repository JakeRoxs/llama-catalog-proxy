//go:build linux

package modelconfig

import (
	"errors"
	"io"
	"os"
	"sort"
	"strings"
)

// ListBackups also works when a single interrupted write exceeds the limit or
// a named record is unreadable, so an operator can choose what to remove.
func ListBackups(directory string) ([]BackupSummary, error) {
	var summaries []BackupSummary
	err := withPrivateStore(directory, ErrBackupStore, func(root *os.Root, dir *os.File) error {
		entries, err := dir.ReadDir(MaxBackups + 2)
		if err != nil && err != io.EOF || len(entries) > MaxBackups+1 {
			return ErrBackupStore
		}
		summaries = make([]BackupSummary, 0, len(entries))
		for _, entry := range entries {
			name := entry.Name()
			if !strings.HasPrefix(name, "backup-") || !strings.HasSuffix(name, ".json") {
				return ErrBackupStore
			}
			id := strings.TrimSuffix(strings.TrimPrefix(name, "backup-"), ".json")
			if !validProposalID(id) {
				return ErrBackupStore
			}
			summary := BackupSummary{ID: id, Status: "unreadable_or_invalid"}
			if record, err := readBackup(root, id); err == nil {
				info := record.Info
				summary.Status = "valid"
				summary.Info = &info
			}
			summaries = append(summaries, summary)
		}
		sort.Slice(summaries, func(i, j int) bool { return summaries[i].ID < summaries[j].ID })
		return nil
	})
	if err != nil {
		return nil, err
	}
	return summaries, nil
}

func ReadBackupInfo(directory, id string) (BackupInfo, error) {
	if !validProposalID(id) {
		return BackupInfo{}, ErrBackupNotFound
	}
	var result BackupInfo
	err := withPrivateStore(directory, ErrBackupStore, func(root *os.Root, _ *os.File) error {
		if _, err := root.Lstat(backupFilename(id)); errors.Is(err, os.ErrNotExist) {
			return ErrBackupNotFound
		} else if err != nil {
			return ErrBackupStore
		}
		record, err := readBackup(root, id)
		if err != nil {
			return ErrBackupStore
		}
		result = record.Info
		return nil
	})
	return result, err
}

// DeleteBackup removes only the named record entry, never its link target.
func DeleteBackup(directory, id string) error {
	if !validProposalID(id) {
		return ErrBackupNotFound
	}
	return withPrivateStore(directory, ErrBackupStore, func(root *os.Root, dir *os.File) error {
		name := backupFilename(id)
		info, err := root.Lstat(name)
		if errors.Is(err, os.ErrNotExist) {
			return ErrBackupNotFound
		}
		if err != nil || !(info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0) {
			return ErrBackupStore
		}
		if err := root.Remove(name); err != nil {
			return ErrBackupStore
		}
		if err := dir.Sync(); err != nil {
			return ErrBackupStore
		}
		return nil
	})
}

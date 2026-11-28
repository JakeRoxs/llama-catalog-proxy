//go:build linux

package modelconfig

import (
	"errors"
	"os"
	"syscall"
)

// openDelegatedStoreTemp creates the private temp file used by delegated
// proposal and operation store replacements. It performs the exact OpenFile
// flags and mode used by both callers. The returned cleanup function removes
// the temp file and syncs dir only when removal succeeds; os.ErrNotExist on
// removal is ignored (the file was already renamed away), while other
// removal/sync errors are surfaced as ErrProposalStore. The caller closes the
// file and defers the cleanup, joining any cleanup error with the named
// result.
func openDelegatedStoreTemp(root *os.Root, dir *os.File, temp string) (*os.File, func() error, error) {
	file, err := root.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, nil, ErrProposalStore
	}
	cleanup := func() error {
		removeErr := root.Remove(temp)
		if removeErr == nil {
			if err := dir.Sync(); err != nil {
				return ErrProposalStore
			}
			return nil
		}
		if errors.Is(removeErr, os.ErrNotExist) {
			return nil
		}
		return ErrProposalStore
	}
	return file, cleanup, nil
}

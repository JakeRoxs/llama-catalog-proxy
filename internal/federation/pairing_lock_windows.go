//go:build windows

package federation

import (
	"os"

	"golang.org/x/sys/windows"
)

func lockPairingStore(file *os.File) (func() error, error) {
	var overlapped windows.Overlapped
	flags := uint32(windows.LOCKFILE_EXCLUSIVE_LOCK)
	if err := windows.LockFileEx(windows.Handle(file.Fd()), flags, 0, 1, 0, &overlapped); err != nil {
		return nil, err
	}
	return func() error { return windows.UnlockFileEx(windows.Handle(file.Fd()), 0, 1, 0, &overlapped) }, nil
}

func validatePairingStoreOwner(os.FileInfo) error { return os.ErrPermission }

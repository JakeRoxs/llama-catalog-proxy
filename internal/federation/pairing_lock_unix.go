//go:build linux || darwin || freebsd || openbsd || netbsd

package federation

import (
	"os"
	"syscall"
)

func lockPairingStore(file *os.File) (func() error, error) {
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		return nil, err
	}
	return func() error { return syscall.Flock(int(file.Fd()), syscall.LOCK_UN) }, nil
}

func validatePairingStoreOwner(info os.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return os.ErrPermission
	}
	return nil
}

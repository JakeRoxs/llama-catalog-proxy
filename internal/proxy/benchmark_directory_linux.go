//go:build linux

package proxy

import (
	"errors"
	"os"
	"syscall"
)

func validateBenchmarkDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 {
		return errors.New("benchmark report directory must be an existing private directory")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return errors.New("benchmark report directory must be owned by the proxy user")
	}
	return nil
}

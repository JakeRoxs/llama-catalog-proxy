//go:build linux

package modelconfig

import (
	"os"
	"path/filepath"
	"syscall"
)

func withPrivateStore(directory string, storeError error, fn func(*os.Root, *os.File) error) error {
	if !filepath.IsAbs(directory) || filepath.Clean(directory) != directory {
		return storeError
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return storeError
	}
	defer root.Close()
	dir, err := root.Open(".")
	if err != nil {
		return storeError
	}
	defer dir.Close()
	info, err := dir.Stat()
	if err != nil || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return storeError
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return storeError
	}
	if err := syscall.Flock(int(dir.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return storeError
	}
	defer syscall.Flock(int(dir.Fd()), syscall.LOCK_UN)
	return fn(root, dir)
}

func writePrivateRecord(root *os.Root, dir *os.File, name string, data []byte, storeError error) error {
	file, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return storeError
	}
	writeErr := file.Chmod(0o600)
	if writeErr == nil {
		_, writeErr = file.Write(data)
	}
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		if err := root.Remove(name); err != nil {
			return storeError
		}
		if err := dir.Sync(); err != nil {
			return storeError
		}
		return storeError
	}
	if err := dir.Sync(); err != nil {
		return storeError
	}
	return nil
}

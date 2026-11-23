//go:build linux

package ingestion

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"

	"golang.org/x/sys/unix"
)

const (
	maxQuotaEntries = 100000
	maxQuotaDepth   = 64
)

func artifactDirectoryBytes(ctx context.Context, directory *os.File, limit int64) (int64, error) {
	var rootStat unix.Stat_t
	if err := unix.Fstat(int(directory.Fd()), &rootStat); err != nil {
		return 0, ErrStageDiskCheck
	}
	var total int64
	var entries int
	if err := walkQuotaDir(directory, 0, &rootStat, &total, &entries, limit, ctx); err != nil {
		return 0, err
	}
	return total, nil
}

// walkQuotaDir recursively descends a directory tree, accumulating regular-file
// sizes into *total and counting every visited entry into *entries. Depth and
// entry limits are enforced; violations return ErrStageDiskCheck.
func walkQuotaDir(parent *os.File, depth int, rootStat *unix.Stat_t, total *int64, entries *int, limit int64, ctx context.Context) error {
	if depth > maxQuotaDepth {
		return ErrStageDiskCheck
	}
	dir, err := openQuotaDir(parent)
	if err != nil {
		return ErrStageDiskCheck
	}
	defer dir.Close()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		batch, readErr := dir.ReadDir(128)
		for _, entry := range batch {
			*entries++
			if *entries > maxQuotaEntries {
				return ErrStageDiskCheck
			}
			if err := processQuotaEntry(dir, entry, depth, rootStat, total, entries, limit, ctx); err != nil {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return ErrStageDiskCheck
		}
	}
}

// openQuotaDir opens a directory file descriptor with the quota walk flags:
// O_RDONLY, O_DIRECTORY, O_CLOEXEC, and O_NOFOLLOW. The caller must close the
// returned *os.File.
func openQuotaDir(parent *os.File) (*os.File, error) {
	fd, err := unix.Openat(int(parent.Fd()), ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), "quota-directory"), nil
}

// processQuotaEntry inspects a single directory entry. Regular files are
// size-checked and accumulated; subdirectories are descended into. It returns
// ErrStageQuota when the size limit is exceeded and ErrStageDiskCheck for any
// filesystem or boundary violation.
func processQuotaEntry(dir *os.File, entry fs.DirEntry, depth int, rootStat *unix.Stat_t, total *int64, entries *int, limit int64, ctx context.Context) error {
	var stat unix.Stat_t
	if err := unix.Fstatat(int(dir.Fd()), entry.Name(), &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return ErrStageDiskCheck
	}
	switch stat.Mode & unix.S_IFMT {
	case unix.S_IFREG:
		if stat.Dev != rootStat.Dev || stat.Size < 0 {
			return ErrStageDiskCheck
		}
		if stat.Size > limit-*total {
			return ErrStageQuota
		}
		*total += stat.Size
	case unix.S_IFDIR:
		if stat.Dev != rootStat.Dev {
			return ErrStageDiskCheck
		}
		childFD, err := unix.Openat(int(dir.Fd()), entry.Name(), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if err != nil {
			return ErrStageDiskCheck
		}
		child := os.NewFile(uintptr(childFD), "quota-child")
		var opened unix.Stat_t
		statErr := unix.Fstat(childFD, &opened)
		if statErr != nil || opened.Dev != stat.Dev || opened.Ino != stat.Ino {
			child.Close()
			return ErrStageDiskCheck
		}
		if err := walkQuotaDir(child, depth+1, rootStat, total, entries, limit, ctx); err != nil {
			child.Close()
			return err
		}
		if closeErr := child.Close(); closeErr != nil {
			return ErrStageDiskCheck
		}
	}
	return nil
}

func checkFreeSpace(directory *os.File, reserve, additional int64) error {
	var stat unix.Statfs_t
	if err := unix.Fstatfs(int(directory.Fd()), &stat); err != nil || stat.Bsize <= 0 {
		return ErrStageDiskCheck
	}
	if !freeSpaceAvailable(stat.Bavail, uint64(stat.Bsize), reserve, additional) {
		return ErrStageFreeSpace
	}
	return nil
}

func freeSpaceAvailable(availableBlocks, blockSize uint64, reserve, additional int64) bool {
	blocks := func(bytes int64) uint64 {
		n := uint64(bytes)
		result := n / blockSize
		if n%blockSize != 0 {
			result++
		}
		return result
	}
	reserveBlocks, additionalBlocks := blocks(reserve), blocks(additional)
	return reserveBlocks <= availableBlocks && additionalBlocks <= availableBlocks-reserveBlocks
}

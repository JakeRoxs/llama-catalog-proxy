//go:build linux

package ingestion

import (
	"context"
	"errors"
	"io"
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
	var walk func(*os.File, int) error
	walk = func(parent *os.File, depth int) error {
		if depth > maxQuotaDepth {
			return ErrStageDiskCheck
		}
		fd, err := unix.Openat(int(parent.Fd()), ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if err != nil {
			return ErrStageDiskCheck
		}
		dir := os.NewFile(uintptr(fd), "quota-directory")
		defer dir.Close()
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			batch, readErr := dir.ReadDir(128)
			for _, entry := range batch {
				entries++
				if entries > maxQuotaEntries {
					return ErrStageDiskCheck
				}
				var stat unix.Stat_t
				if err := unix.Fstatat(int(dir.Fd()), entry.Name(), &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
					return ErrStageDiskCheck
				}
				switch stat.Mode & unix.S_IFMT {
				case unix.S_IFREG:
					if stat.Dev != rootStat.Dev || stat.Size < 0 {
						return ErrStageDiskCheck
					}
					if stat.Size > limit-total {
						return ErrStageQuota
					}
					total += stat.Size
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
					err = walk(child, depth+1)
					closeErr := child.Close()
					if err != nil {
						return err
					}
					if closeErr != nil {
						return ErrStageDiskCheck
					}
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
	if err := walk(directory, 0); err != nil {
		return 0, err
	}
	return total, nil
}

func checkFreeSpace(directory *os.File, reserve, additional int64) error {
	if reserve == 0 {
		return nil
	}
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

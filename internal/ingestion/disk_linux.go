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

// quotaWalkState carries the shared mutable state and cancellation context of a
// directory quota walk. Grouping these parameters into a struct reduces the
// parameter lists of the walk functions and keeps the recursion state explicit.
type quotaWalkState struct {
	rootStat *unix.Stat_t
	total    *int64
	entries  *int
	limit    int64
	ctx      context.Context
}

func artifactDirectoryBytes(ctx context.Context, directory *os.File, limit int64) (int64, error) {
	var rootStat unix.Stat_t
	if err := unix.Fstat(int(directory.Fd()), &rootStat); err != nil {
		return 0, ErrStageDiskCheck
	}
	var total int64
	var entries int
	state := &quotaWalkState{rootStat: &rootStat, total: &total, entries: &entries, limit: limit, ctx: ctx}
	if err := walkQuotaDir(directory, 0, state); err != nil {
		return 0, err
	}
	return total, nil
}

// walkQuotaDir recursively descends a directory tree, accumulating regular-file
// sizes into state.total and counting every visited entry into state.entries.
// Depth and entry limits are enforced; violations return ErrStageDiskCheck.
func walkQuotaDir(parent *os.File, depth int, state *quotaWalkState) error {
	if depth > maxQuotaDepth {
		return ErrStageDiskCheck
	}
	dir, err := openQuotaDir(parent)
	if err != nil {
		return ErrStageDiskCheck
	}
	defer dir.Close()
	return processQuotaBatch(dir, state, depth)
}

// processQuotaBatch reads a directory in batches and processes each entry until
// the directory is exhausted or the context is cancelled.
func processQuotaBatch(dir *os.File, state *quotaWalkState, depth int) error {
	for {
		if err := state.ctx.Err(); err != nil {
			return err
		}
		batch, readErr := dir.ReadDir(128)
		if err := processQuotaBatchEntries(dir, batch, state, depth); err != nil {
			return err
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return ErrStageDiskCheck
		}
	}
}

// processQuotaBatchEntries processes a single batch of directory entries.
func processQuotaBatchEntries(dir *os.File, batch []fs.DirEntry, state *quotaWalkState, depth int) error {
	for _, entry := range batch {
		*state.entries++
		if *state.entries > maxQuotaEntries {
			return ErrStageDiskCheck
		}
		if err := processQuotaEntry(dir, entry, depth, state); err != nil {
			return err
		}
	}
	return nil
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
func processQuotaEntry(dir *os.File, entry fs.DirEntry, depth int, state *quotaWalkState) error {
	var stat unix.Stat_t
	if err := unix.Fstatat(int(dir.Fd()), entry.Name(), &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return ErrStageDiskCheck
	}
	switch stat.Mode & unix.S_IFMT {
	case unix.S_IFREG:
		return processQuotaRegularFile(dir, &stat, state)
	case unix.S_IFDIR:
		return processQuotaSubdirectory(dir, entry, &stat, depth, state)
	}
	return nil
}

// processQuotaRegularFile size-checks a regular file against the walk state and
// accumulates its size. It returns ErrStageQuota when the size limit is exceeded
// and ErrStageDiskCheck for any filesystem or boundary violation.
func processQuotaRegularFile(dir *os.File, stat *unix.Stat_t, state *quotaWalkState) error {
	if stat.Dev != state.rootStat.Dev || stat.Size < 0 {
		return ErrStageDiskCheck
	}
	if stat.Size > state.limit-*state.total {
		return ErrStageQuota
	}
	*state.total += stat.Size
	return nil
}

// processQuotaSubdirectory descends into a subdirectory after validating it is
// on the same device as the root. It returns ErrStageDiskCheck for any
// filesystem or boundary violation.
func processQuotaSubdirectory(dir *os.File, entry fs.DirEntry, stat *unix.Stat_t, depth int, state *quotaWalkState) error {
	if stat.Dev != state.rootStat.Dev {
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
	if err := walkQuotaDir(child, depth+1, state); err != nil {
		child.Close()
		return err
	}
	if closeErr := child.Close(); closeErr != nil {
		return ErrStageDiskCheck
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

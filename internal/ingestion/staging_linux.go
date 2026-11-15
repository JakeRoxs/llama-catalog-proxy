//go:build linux

package ingestion

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// StageNewArtifact normally requires an absent filename; replacement requires
// explicit authorization and an exclusive edit window. The directory must not
// be modified by uncooperative writers while staging. The reader must unblock
// on context cancellation; HTTP callers must bind their request to ctx.
func StageNewArtifact(ctx context.Context, options StageOptions, input io.Reader) (result StageResult, returnErr error) {
	result = StageResult{Publication: PublicationNotPublished, Durability: "not_applicable"}
	if ctx == nil || input == nil || !options.AllowDownload || options.AdoptExisting || options.AllowAdoption ||
		(options.ReplaceExisting && (!options.AllowReplacement || !options.AcknowledgeExclusiveWindow)) ||
		(!options.ReplaceExisting && (options.AllowReplacement || options.AcknowledgeExclusiveWindow)) ||
		options.MaxBytes < 1 || options.MaxDirectoryBytes < 0 || options.MinFreeBytes < 0 ||
		(options.FileMode != 0o600 && options.FileMode != 0o640) ||
		!filepath.IsAbs(options.Directory) || filepath.Clean(options.Directory) != options.Directory {
		return result, ErrStageInvalid
	}
	source, err := ParseSource(SourceInput{
		Repository: options.Source.Repository, Filename: options.Source.Filename, Revision: options.Source.Revision,
	})
	if err != nil || source != options.Source {
		return result, ErrStageInvalid
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	root, directory, err := openArtifactDirectory(options.Directory)
	if err != nil {
		return result, ErrStageDirectory
	}
	defer root.Close()
	defer directory.Close()
	if err := unix.Flock(int(directory.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return result, ErrStageDirectory
	}
	defer unix.Flock(int(directory.Fd()), unix.LOCK_UN)
	var existing *os.File
	var existingIdentity artifactFileIdentity
	if options.ReplaceExisting {
		existing, existingIdentity, err = openExistingArtifact(root, source.Filename)
		if err != nil {
			return result, err
		}
		defer existing.Close()
	} else {
		if _, err := root.Lstat(source.Filename); err == nil {
			return result, ErrStageExists
		} else if !errors.Is(err, os.ErrNotExist) {
			return result, ErrStageDirectory
		}
	}
	var directoryBytes int64
	if options.MaxDirectoryBytes > 0 {
		directoryBytes, err = artifactDirectoryBytes(ctx, directory, options.MaxDirectoryBytes)
		if err != nil {
			return result, err
		}
	}
	if err := checkFreeSpace(directory, options.MinFreeBytes, 0); err != nil {
		return result, err
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return result, ErrStageIO
	}
	name := ".ingest-" + hex.EncodeToString(random[:]) + ".partial"
	file, err := root.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return result, ErrStageIO
	}
	renamed := false
	defer func() {
		closeErr := file.Close()
		var removeErr error
		if !renamed {
			removeErr = root.Remove(name)
			if removeErr == nil {
				removeErr = directory.Sync()
			}
		}
		if closeErr != nil || removeErr != nil {
			returnErr = errors.Join(returnErr, ErrStageCleanup)
		}
	}()

	hash := sha256.New()
	buffer := make([]byte, 32<<10)
	var written int64
	emptyReads := 0
	for {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		n, readErr := input.Read(buffer)
		if n < 0 || n > len(buffer) {
			return result, ErrStageIO
		}
		if int64(n) > options.MaxBytes-written {
			return result, ErrStageTooLarge
		}
		if n > 0 {
			if options.MaxDirectoryBytes > 0 && int64(n) > options.MaxDirectoryBytes-directoryBytes-written {
				return result, ErrStageQuota
			}
			if err := checkFreeSpace(directory, options.MinFreeBytes, int64(n)); err != nil {
				return result, err
			}
			emptyReads = 0
			if _, err := file.Write(buffer[:n]); err != nil {
				return result, fmt.Errorf("%w: writing temporary artifact", ErrStageIO)
			}
			_, _ = hash.Write(buffer[:n])
			written += int64(n)
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				if err := ctx.Err(); err != nil {
					return result, err
				}
				return result, fmt.Errorf("%w: reading artifact stream", ErrStageIO)
			}
			break
		}
		if n == 0 {
			emptyReads++
			if emptyReads >= 100 {
				return result, fmt.Errorf("%w: %w", ErrStageIO, io.ErrNoProgress)
			}
		}
	}
	if written == 0 {
		return result, ErrStageEmpty
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return result, fmt.Errorf("%w: seeking temporary artifact", ErrStageIO)
	}
	metadata, err := InspectGGUF(file)
	if err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := file.Chmod(options.FileMode); err != nil {
		return result, ErrStageIO
	}
	if err := file.Sync(); err != nil {
		return result, ErrStageIO
	}
	if err := checkFreeSpace(directory, options.MinFreeBytes, 0); err != nil {
		return result, err
	}
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || opened.Size() != written || opened.Mode().Perm() != options.FileMode {
		return result, ErrStageIO
	}
	current, err := root.Lstat(name)
	if err != nil || !os.SameFile(opened, current) {
		return result, ErrStageIO
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	result.Bytes, result.SHA256 = written, hex.EncodeToString(hash.Sum(nil))
	result.Integrity, result.Metadata = "calculated_unverified", &metadata
	if options.ReplaceExisting {
		if err := existingArtifactUnchanged(root, existing, source.Filename, existingIdentity); err != nil {
			return result, err
		}
		if err := artifactDirectoryUnchanged(directory, options.Directory); err != nil {
			return result, err
		}
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if err := unix.Renameat(int(directory.Fd()), name, int(directory.Fd()), source.Filename); err != nil {
			result.Publication, result.Durability = PublicationUnknown, "unknown"
			return result, ErrStageCommitUnknown
		}
		renamed = true
		result.Publication, result.Durability = PublicationPublished, "uncertain"
		if err := directory.Sync(); err != nil {
			return result, ErrStageDurability
		}
		result.Durability = "confirmed"
		return result, nil
	}
	if err := unix.Linkat(int(directory.Fd()), name, int(directory.Fd()), source.Filename, 0); err != nil {
		result.Publication, result.Durability = PublicationUnknown, "unknown"
		if errors.Is(err, unix.EEXIST) {
			return result, errors.Join(ErrStageExists, ErrStageCommitUnknown)
		}
		return result, ErrStageCommitUnknown
	}
	result.Publication, result.Durability = PublicationPublished, "uncertain"
	if err := directory.Sync(); err != nil {
		return result, ErrStageDurability
	}
	result.Durability = "confirmed"
	return result, nil
}

func openArtifactDirectory(path string) (*os.Root, *os.File, error) {
	before, err := os.Lstat(path)
	if err != nil || !before.IsDir() || before.Mode()&os.ModeSymlink != 0 || before.Mode().Perm()&0o022 != 0 {
		return nil, nil, ErrStageDirectory
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, nil, ErrStageDirectory
	}
	directory, err := root.Open(".")
	if err != nil {
		root.Close()
		return nil, nil, ErrStageDirectory
	}
	info, err := directory.Stat()
	var stat unix.Stat_t
	if err != nil || !os.SameFile(before, info) || !info.IsDir() || info.Mode().Perm()&0o022 != 0 ||
		unix.Fstat(int(directory.Fd()), &stat) != nil || stat.Uid != uint32(os.Geteuid()) {
		directory.Close()
		root.Close()
		return nil, nil, ErrStageDirectory
	}
	return root, directory, nil
}

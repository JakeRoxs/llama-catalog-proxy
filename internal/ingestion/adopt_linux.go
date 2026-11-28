//go:build linux

package ingestion

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
)

func openExistingArtifact(root *os.Root, name string) (*os.File, artifactFileIdentity, error) {
	file, err := root.OpenFile(name, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, artifactFileIdentity{}, ErrArtifactUnavailable
	}
	info, err := file.Stat()
	var stat unix.Stat_t
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o022 != 0 ||
		unix.Fstat(int(file.Fd()), &stat) != nil || stat.Mode&unix.S_IFMT != unix.S_IFREG ||
		stat.Mode&0o022 != 0 || stat.Uid != uint32(os.Geteuid()) || stat.Nlink != 1 {
		file.Close()
		return nil, artifactFileIdentity{}, ErrArtifactUnavailable
	}
	current, err := root.Lstat(name)
	if err != nil || !current.Mode().IsRegular() || !os.SameFile(info, current) {
		file.Close()
		return nil, artifactFileIdentity{}, ErrArtifactChanged
	}
	return file, artifactIdentity(stat), nil
}

func existingArtifactUnchanged(root *os.Root, file *os.File, name string, before artifactFileIdentity) error {
	info, err := file.Stat()
	var stat unix.Stat_t
	if err != nil || unix.Fstat(int(file.Fd()), &stat) != nil || artifactIdentity(stat) != before {
		return ErrArtifactChanged
	}
	current, err := root.Lstat(name)
	if err != nil || !current.Mode().IsRegular() || !os.SameFile(info, current) {
		return ErrArtifactChanged
	}
	return nil
}

func artifactDirectoryUnchanged(directory *os.File, path string) error {
	info, err := directory.Stat()
	var stat unix.Stat_t
	if err != nil || unix.Fstat(int(directory.Fd()), &stat) != nil || stat.Uid != uint32(os.Geteuid()) ||
		stat.Mode&0o022 != 0 {
		return ErrStageDirectory
	}
	current, err := os.Lstat(path)
	if err != nil || !current.IsDir() || current.Mode()&os.ModeSymlink != 0 ||
		current.Mode().Perm()&0o022 != 0 || !os.SameFile(info, current) {
		return ErrStageDirectory
	}
	return nil
}

func adoptArtifact(ctx context.Context, options StageOptions, duration time.Duration) (StageResult, error) {
	result := StageResult{Publication: PublicationNotPublished, Durability: "not_applicable"}
	if ctx == nil || duration <= 0 || options.MaxBytes < 1 || options.MaxDirectoryBytes < 1 || options.MinFreeBytes < 1 ||
		!options.AdoptExisting || !options.AllowAdoption ||
		options.AllowDownload || options.ReplaceExisting || options.AllowReplacement || options.AcknowledgeExclusiveWindow ||
		!filepath.IsAbs(options.Directory) || filepath.Clean(options.Directory) != options.Directory ||
		!validRecordedSource(options.Source, "adopt") {
		return result, ErrStageInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	root, directory, err := openArtifactDirectory(options.Directory)
	if err != nil {
		return result, err
	}
	defer root.Close()
	defer directory.Close()
	if err := unix.Flock(int(directory.Fd()), unix.LOCK_SH|unix.LOCK_NB); err != nil {
		return result, ErrArtifactBusy
	}
	defer unix.Flock(int(directory.Fd()), unix.LOCK_UN)
	file, before, err := openExistingArtifact(root, options.Source.Filename)
	if err != nil {
		return result, err
	}
	defer file.Close()
	if before.Size <= 0 {
		return result, ErrStageEmpty
	}
	if before.Size > options.MaxBytes {
		return result, ErrStageTooLarge
	}
	hash := sha256.New()
	buffer := make([]byte, 32<<10)
	var read int64
	for {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		n, readErr := file.Read(buffer)
		if int64(n) > before.Size-read {
			return result, ErrArtifactChanged
		}
		_, _ = hash.Write(buffer[:n])
		read += int64(n)
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				return result, ErrStageIO
			}
			break
		}
		if n == 0 {
			return result, ErrStageIO
		}
	}
	if read != before.Size {
		return result, ErrArtifactChanged
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return result, ErrStageIO
	}
	metadata, err := InspectGGUF(file)
	if err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := existingArtifactUnchanged(root, file, options.Source.Filename, before); err != nil {
		return result, err
	}
	if err := artifactDirectoryUnchanged(directory, options.Directory); err != nil {
		return result, err
	}
	if err := file.Close(); err != nil {
		return result, ErrStageIO
	}
	result.Publication, result.Bytes = PublicationExisting, read
	result.SHA256, result.Integrity, result.Metadata = hex.EncodeToString(hash.Sum(nil)), "calculated_unverified", &metadata
	return result, nil
}

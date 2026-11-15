//go:build linux

package ingestion

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

type artifactFileIdentity struct {
	Device, Inode uint64
	Size          int64
	Mode          uint32
	Owner, Group  uint32
	Links         uint64
	Mtime, Ctime  unix.Timespec
}

func artifactIdentity(stat unix.Stat_t) artifactFileIdentity {
	return artifactFileIdentity{
		Device: uint64(stat.Dev), Inode: stat.Ino, Size: stat.Size,
		Mode: stat.Mode, Owner: stat.Uid, Group: stat.Gid, Links: uint64(stat.Nlink),
		Mtime: stat.Mtim, Ctime: stat.Ctim,
	}
}

func withRecordedArtifactFile(directory string, record Record, fn func(ArtifactEvidence, func() (ArtifactEvidence, error)) error) error {
	if !filepath.IsAbs(directory) || filepath.Clean(directory) != directory {
		return ErrStageDirectory
	}
	source := record.Job.Source
	if !validRecordedSource(source, record.Job.Operation) {
		return ErrArtifactRecord
	}
	root, dir, err := openArtifactDirectory(directory)
	if err != nil {
		return err
	}
	defer root.Close()
	defer dir.Close()
	if err := unix.Flock(int(dir.Fd()), unix.LOCK_SH|unix.LOCK_NB); err != nil {
		return ErrArtifactBusy
	}
	defer unix.Flock(int(dir.Fd()), unix.LOCK_UN)
	recheck := func() (ArtifactEvidence, error) {
		return inspectRecordedArtifact(root, dir, directory, source, record)
	}
	evidence, err := recheck()
	if err != nil {
		return err
	}
	return fn(evidence, recheck)
}

func inspectRecordedArtifact(root *os.Root, dir *os.File, directory string, source Source, record Record) (ArtifactEvidence, error) {
	file, err := root.OpenFile(source.Filename, unix.O_PATH|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return ArtifactEvidence{}, ErrArtifactUnavailable
	}
	defer file.Close()
	info, err := file.Stat()
	var before, after, directoryStat unix.Stat_t
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o022 != 0 ||
		unix.Fstat(int(file.Fd()), &before) != nil || before.Mode&unix.S_IFMT != unix.S_IFREG ||
		before.Mode&0o022 != 0 || before.Uid != uint32(os.Geteuid()) || before.Nlink != 1 {
		return ArtifactEvidence{}, ErrArtifactUnavailable
	}
	if before.Size != record.Job.Result.Bytes {
		return ArtifactEvidence{}, ErrArtifactChanged
	}
	current, err := root.Lstat(source.Filename)
	if err != nil || !current.Mode().IsRegular() || !os.SameFile(info, current) {
		return ArtifactEvidence{}, ErrArtifactChanged
	}
	dirInfo, err := dir.Stat()
	if err != nil {
		return ArtifactEvidence{}, ErrStageDirectory
	}
	currentDir, err := os.Lstat(directory)
	if err != nil || !currentDir.IsDir() || currentDir.Mode()&os.ModeSymlink != 0 ||
		currentDir.Mode().Perm()&0o022 != 0 || !os.SameFile(dirInfo, currentDir) ||
		unix.Fstat(int(dir.Fd()), &directoryStat) != nil || directoryStat.Mode&0o022 != 0 ||
		directoryStat.Uid != uint32(os.Geteuid()) {
		return ArtifactEvidence{}, ErrStageDirectory
	}
	if unix.Fstat(int(file.Fd()), &after) != nil || artifactIdentity(before) != artifactIdentity(after) {
		return ArtifactEvidence{}, ErrArtifactChanged
	}
	binding, err := json.Marshal(struct {
		Directory       string
		DirectoryDevice uint64
		DirectoryInode  uint64
		DirectoryMode   uint32
		DirectoryOwner  uint32
		Artifact        artifactFileIdentity
		Record          Record
	}{
		Directory: directory, DirectoryDevice: uint64(directoryStat.Dev), DirectoryInode: directoryStat.Ino,
		DirectoryMode: directoryStat.Mode, DirectoryOwner: directoryStat.Uid,
		Artifact: artifactIdentity(after), Record: record,
	})
	if err != nil {
		return ArtifactEvidence{}, ErrArtifactUnavailable
	}
	sum := sha256.Sum256(binding)
	return ArtifactEvidence{
		RecordID: record.Job.ID, Source: source, Bytes: after.Size,
		Presence: "regular_file_when_checked", RecordMatch: "unknown", Integrity: "not_verified",
		BackendAccess: "unknown", Revision: hex.EncodeToString(sum[:]),
	}, nil
}

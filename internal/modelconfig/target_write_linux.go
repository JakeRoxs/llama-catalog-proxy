//go:build linux

package modelconfig

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"syscall"
	"unicode/utf8"

	"golang.org/x/sys/unix"
)

var (
	errTargetWrite           = ErrTargetWrite
	errTargetChanged         = ErrTargetChanged
	errTargetNoChange        = ErrTargetNoChange
	errTargetCleanup         = ErrTargetCleanup
	errTargetCommitUncertain = ErrTargetCommitUncertain
)

const (
	maxTargetAttributeNames = 64 << 10
	maxTargetAttributes     = 128
	maxTargetAttributeBytes = 256 << 10
)

type targetWrite struct {
	root       *os.Root
	directory  *os.File
	file       *os.File
	filename   string
	info       os.FileInfo
	source     []byte
	attributes map[string][]byte
}

// withTargetWrite serializes cooperating writers. The operator must also
// prevent uncooperative writers for the entire callback.
func withTargetWrite(directory, filename string, fn func(*targetWrite) error) error {
	if !validTargetPath(directory, filename) {
		return errTargetWrite
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return errTargetWrite
	}
	defer root.Close()
	dir, err := root.Open(".")
	if err != nil {
		return errTargetWrite
	}
	defer dir.Close()
	if err := unix.Flock(int(dir.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return errTargetWrite
	}
	defer unix.Flock(int(dir.Fd()), unix.LOCK_UN)
	file, err := openTargetFile(root, filename)
	if err != nil {
		return errTargetWrite
	}
	defer file.Close()
	data, info, err := readOpenedTarget(root, filename, file)
	if err != nil || !safeTargetMetadata(info) {
		return errTargetWrite
	}
	attributes, err := readTargetAttributes(file)
	if err != nil {
		return errTargetWrite
	}
	target := &targetWrite{root: root, directory: dir, file: file, filename: filename,
		info: info, source: data, attributes: attributes}
	if err := target.checkUnchanged(); err != nil {
		return err
	}
	return fn(target)
}

func safeTargetMetadata(info os.FileInfo) bool {
	if info == nil || !info.Mode().IsRegular() ||
		info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Nlink == 1
}

func sameTargetMetadata(a, b os.FileInfo) bool {
	if !safeTargetMetadata(a) || !safeTargetMetadata(b) || !os.SameFile(a, b) ||
		a.Mode() != b.Mode() || a.Size() != b.Size() || !a.ModTime().Equal(b.ModTime()) {
		return false
	}
	left, leftOK := a.Sys().(*syscall.Stat_t)
	right, rightOK := b.Sys().(*syscall.Stat_t)
	return leftOK && rightOK && left.Uid == right.Uid && left.Gid == right.Gid &&
		left.Nlink == right.Nlink && left.Ctim == right.Ctim
}

func (t *targetWrite) checkUnchanged() error {
	data, info, err := readOpenedTarget(t.root, t.filename, t.file)
	if err != nil || !sameTargetMetadata(t.info, info) || !bytes.Equal(data, t.source) {
		return errTargetChanged
	}
	attributes, err := readTargetAttributes(t.file)
	if err != nil || !sameTargetAttributes(t.attributes, attributes) {
		return errTargetChanged
	}
	current, err := t.root.Lstat(t.filename)
	if err != nil || !sameTargetMetadata(t.info, current) {
		return errTargetChanged
	}
	return nil
}

// replace reports confirmed rename; a rename error can leave the outcome unknown.
func (t *targetWrite) replace(candidate []byte) (bool, error) {
	if err := validateCandidate(candidate, t.source); err != nil {
		if errors.Is(err, errTargetNoChange) {
			return false, errTargetNoChange
		}
		return false, errTargetWrite
	}
	if err := t.checkUnchanged(); err != nil {
		return false, err
	}
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return false, errTargetWrite
	}
	name := ".llama-catalog-proxy-" + hex.EncodeToString(id) + ".tmp"
	temp, err := t.root.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return false, errTargetWrite
	}
	cleanup := func() error {
		closeErr := temp.Close()
		removeErr := t.removeTemporary(name)
		if closeErr != nil || removeErr != nil {
			return errTargetCleanup
		}
		return nil
	}
	fail := func() (bool, error) {
		if err := cleanup(); err != nil {
			return false, err
		}
		return false, errTargetWrite
	}
	tempInfo, err := stageTempFile(temp, t.info, t.attributes, candidate)
	if err != nil {
		return fail()
	}
	if err := t.checkUnchanged(); err != nil {
		if cleanupErr := cleanup(); cleanupErr != nil {
			return false, cleanupErr
		}
		return false, err
	}
	if err := validateStagedTemp(t.root, name, temp, tempInfo, candidate, t.attributes); err != nil {
		return fail()
	}
	return commitTemp(temp, name, t)
}

// validateCandidate bounds and validates the replacement payload. A no-op
// candidate is reported as errTargetNoChange so the caller can skip mutation.
func validateCandidate(candidate, source []byte) error {
	if len(candidate) == 0 || len(candidate) > maxConfigBytes || !utf8.Valid(candidate) {
		return errTargetWrite
	}
	if bytes.Equal(candidate, source) {
		return errTargetNoChange
	}
	return nil
}

// stageTempFile prepares the temporary target: ownership, mode, attributes,
// contents, metadata, and durability. It returns the staged file info.
func stageTempFile(temp *os.File, info os.FileInfo, attributes map[string][]byte, candidate []byte) (os.FileInfo, error) {
	stat := info.Sys().(*syscall.Stat_t)
	if uint64(int(stat.Uid)) != uint64(stat.Uid) || uint64(int(stat.Gid)) != uint64(stat.Gid) ||
		temp.Chown(int(stat.Uid), int(stat.Gid)) != nil || temp.Chmod(info.Mode().Perm()) != nil {
		return nil, errTargetWrite
	}
	if err := copyTargetAttributes(temp, attributes); err != nil {
		return nil, errTargetWrite
	}
	if _, err := temp.Write(candidate); err != nil {
		return nil, errTargetWrite
	}
	tempInfo, err := temp.Stat()
	if err != nil || !safeTargetMetadata(tempInfo) || tempInfo.Mode().Perm() != info.Mode().Perm() {
		return nil, errTargetWrite
	}
	tempStat := tempInfo.Sys().(*syscall.Stat_t)
	attrs, err := readTargetAttributes(temp)
	if err != nil || tempStat.Uid != stat.Uid || tempStat.Gid != stat.Gid ||
		!sameTargetAttributes(attrs, attributes) {
		return nil, errTargetWrite
	}
	if err := temp.Sync(); err != nil {
		return nil, errTargetWrite
	}
	return tempInfo, nil
}

// validateStagedTemp confirms the staged temp matches the candidate and the
// recorded attributes, and that its metadata matches the staged snapshot.
func validateStagedTemp(root *os.Root, name string, temp *os.File, tempInfo os.FileInfo, candidate []byte, attributes map[string][]byte) error {
	staged, stagedInfo, err := readOpenedTarget(root, name, temp)
	if err != nil {
		return errTargetWrite
	}
	stagedAttrs, attrErr := readTargetAttributes(temp)
	if attrErr != nil {
		return errTargetWrite
	}
	if !sameTargetMetadata(tempInfo, stagedInfo) || !bytes.Equal(staged, candidate) || !sameTargetAttributes(stagedAttrs, attributes) {
		return errTargetWrite
	}
	return nil
}

// commitTemp performs the atomic rename and post-rename directory sync. The
// rename block is kept verbatim so an uncertain rename outcome is preserved.
func commitTemp(temp *os.File, name string, t *targetWrite) (bool, error) {
	if err := temp.Close(); err != nil {
		if removeErr := t.removeTemporary(name); removeErr != nil {
			return false, removeErr
		}
		return false, errTargetWrite
	}
	if err := unix.Renameat(int(t.directory.Fd()), name, int(t.directory.Fd()), t.filename); err != nil {
		removeErr := t.root.Remove(name)
		if removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			return false, errors.Join(ErrTargetRenameUncertain, errTargetCleanup)
		}
		if removeErr == nil && t.directory.Sync() != nil {
			return false, errors.Join(ErrTargetRenameUncertain, errTargetCleanup)
		}
		return false, ErrTargetRenameUncertain
	}
	if err := t.directory.Sync(); err != nil {
		return true, errTargetCommitUncertain
	}
	return true, nil
}

func (t *targetWrite) removeTemporary(name string) error {
	if err := t.root.Remove(name); err != nil {
		return errTargetCleanup
	}
	if err := t.directory.Sync(); err != nil {
		return errTargetCleanup
	}
	return nil
}

func readTargetAttributes(file *os.File) (map[string][]byte, error) {
	fd := int(file.Fd())
	names, err := readTargetAttributeNames(fd)
	if err != nil {
		return nil, errTargetWrite
	}
	result, err := readTargetAttributeValues(fd, names)
	if err != nil {
		return nil, errTargetWrite
	}
	return result, nil
}

// readTargetAttributeNames performs the Flistxattr size, count, and NUL
// terminator checks. The caller maps any failure to errTargetWrite.
func readTargetAttributeNames(fd int) ([]byte, error) {
	size, err := unix.Flistxattr(fd, nil)
	if err != nil || size > maxTargetAttributeNames {
		return nil, errTargetWrite
	}
	names := make([]byte, size)
	if size > 0 {
		count, err := unix.Flistxattr(fd, names)
		if err != nil || count != size || names[len(names)-1] != 0 {
			return nil, errTargetWrite
		}
	}
	return names, nil
}

// readTargetAttributeValues parses NUL-separated names with duplicate and
// count limits and reads each value within the byte bound.
func readTargetAttributeValues(fd int, names []byte) (map[string][]byte, error) {
	result := make(map[string][]byte)
	total := 0
	for len(names) > 0 {
		pos := bytes.IndexByte(names, 0)
		if pos <= 0 || len(result) >= maxTargetAttributes {
			return nil, errTargetWrite
		}
		name := string(names[:pos])
		names = names[pos+1:]
		if _, exists := result[name]; exists {
			return nil, errTargetWrite
		}
		size, err := unix.Fgetxattr(fd, name, nil)
		if err != nil || size > maxTargetAttributeBytes-total {
			return nil, errTargetWrite
		}
		value := make([]byte, size)
		count, err := unix.Fgetxattr(fd, name, value)
		if err != nil || count != size {
			return nil, errTargetWrite
		}
		total += size
		result[name] = value
	}
	return result, nil
}

func sameTargetAttributes(a, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for name, value := range a {
		other, exists := b[name]
		if !exists || !bytes.Equal(value, other) {
			return false
		}
	}
	return true
}

func copyTargetAttributes(file *os.File, desired map[string][]byte) error {
	current, err := readTargetAttributes(file)
	if err != nil {
		return err
	}
	fd := int(file.Fd())
	for name := range current {
		if _, exists := desired[name]; !exists {
			if err := unix.Fremovexattr(fd, name); err != nil {
				return errTargetWrite
			}
		}
	}
	for name, value := range desired {
		if existing, exists := current[name]; !exists || !bytes.Equal(existing, value) {
			if err := unix.Fsetxattr(fd, name, value, 0); err != nil {
				return errTargetWrite
			}
		}
	}
	return nil
}

//go:build linux

package ingestion

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
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
	source, err := prepareStageRequest(ctx, input, options)
	if err != nil {
		return result, ErrStageInvalid
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	root, directory, err := openAndLockArtifactDirectory(options.Directory)
	if err != nil {
		return result, ErrStageDirectory
	}
	defer root.Close()
	defer directory.Close()

	var existing *os.File
	var existingIdentity artifactFileIdentity
	if options.ReplaceExisting {
		existing, existingIdentity, err = checkExistingDestination(root, options, source)
		if err != nil {
			return result, err
		}
		defer existing.Close()
	} else {
		if err := ensureDestinationAbsent(root, source.Filename); err != nil {
			return result, err
		}
	}

	directoryBytes, err := checkQuotaAndFreeSpace(ctx, directory, options)
	if err != nil {
		return result, err
	}

	file, name, err := createTempArtifactFile(root)
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

	stream := newStagingStream(ctx, file, input, options, directoryBytes, directory)
	if err := stream.read(); err != nil {
		return result, err
	}
	hash, written := stream.hash, stream.written

	staged := stagedArtifactFile{ctx: ctx, file: file, options: options, written: written, root: root, name: name, directory: directory}
	metadata, err := staged.finalize()
	if err != nil {
		return result, err
	}

	result.Bytes, result.SHA256 = written, hex.EncodeToString(hash.Sum(nil))
	result.Integrity, result.Metadata = "calculated_unverified", &metadata

	commit := stagedArtifactCommit{
		ctx: ctx, root: root, directory: directory, name: name, options: options,
		source: source, existing: existing, existingIdentity: existingIdentity, result: result,
	}
	if options.ReplaceExisting {
		result, err = commit.replace()
		if err != nil {
			// A durability error can occur after renameat has published the
			// destination. Do not try to remove the now-missing temp name.
			renamed = result.Publication == PublicationPublished
			return result, err
		}
		renamed = true
		return result, nil
	}
	return commit.publishNew()
}

// prepareStageRequest validates the staging request context, input, and options,
// then parses and confirms the artifact source. Validation failures are reported
// as ErrStageInvalid; a canceled context is reported directly.
func prepareStageRequest(ctx context.Context, input io.Reader, options StageOptions) (Source, error) {
	if ctx == nil || input == nil || !options.AllowDownload || options.AdoptExisting || options.AllowAdoption ||
		(options.ReplaceExisting && (!options.AllowReplacement || !options.AcknowledgeExclusiveWindow)) ||
		(!options.ReplaceExisting && (options.AllowReplacement || options.AcknowledgeExclusiveWindow)) ||
		options.MaxBytes < 1 || options.MaxDirectoryBytes < 1 || options.MinFreeBytes < 1 ||
		(options.FileMode != 0o600 && options.FileMode != 0o640) ||
		!filepath.IsAbs(options.Directory) || filepath.Clean(options.Directory) != options.Directory {
		return Source{}, ErrStageInvalid
	}
	source, err := ParseSource(SourceInput{
		Repository: options.Source.Repository, Filename: options.Source.Filename, Revision: options.Source.Revision,
	})
	if err != nil || source != options.Source {
		return Source{}, ErrStageInvalid
	}
	return source, nil
}

// openAndLockArtifactDirectory opens the artifact directory, verifies its
// safety, and acquires an exclusive non-blocking lock. The caller owns the
// returned file descriptors and must close them.
func openAndLockArtifactDirectory(path string) (*os.Root, *os.File, error) {
	root, directory, err := openArtifactDirectory(path)
	if err != nil {
		return nil, nil, err
	}
	if err := unix.Flock(int(directory.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		directory.Close()
		root.Close()
		return nil, nil, ErrStageDirectory
	}
	return root, directory, nil
}

// checkExistingDestination verifies the destination state: for replacement it
// opens the existing artifact under the exclusive lock; otherwise it ensures
// the destination is absent.
func checkExistingDestination(root *os.Root, options StageOptions, source Source) (*os.File, artifactFileIdentity, error) {
	if options.ReplaceExisting {
		return openExistingArtifact(root, source.Filename)
	}
	if _, err := root.Lstat(source.Filename); err == nil {
		return nil, artifactFileIdentity{}, ErrStageExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, artifactFileIdentity{}, ErrStageDirectory
	}
	return nil, artifactFileIdentity{}, nil
}

// ensureDestinationAbsent fails if the destination already exists.
func ensureDestinationAbsent(root *os.Root, name string) error {
	if _, err := root.Lstat(name); err == nil {
		return ErrStageExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return ErrStageDirectory
	}
	return nil
}

// checkQuotaAndFreeSpace measures the directory byte usage and reserves free
// space for the staged artifact.
func checkQuotaAndFreeSpace(ctx context.Context, directory *os.File, options StageOptions) (int64, error) {
	directoryBytes, err := artifactDirectoryBytes(ctx, directory, options.MaxDirectoryBytes)
	if err != nil {
		return 0, err
	}
	if err := checkFreeSpace(directory, options.MinFreeBytes, 0); err != nil {
		return 0, err
	}
	return directoryBytes, nil
}

// createTempArtifactFile creates a uniquely named, exclusive temporary artifact
// file in the artifact root with mode 0o600.
func createTempArtifactFile(root *os.Root) (*os.File, string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return nil, "", ErrStageIO
	}
	name := ".ingest-" + hex.EncodeToString(random[:]) + ".partial"
	file, err := root.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, "", ErrStageIO
	}
	return file, name, nil
}

type stagingStream struct {
	ctx            context.Context
	file           *os.File
	input          io.Reader
	options        StageOptions
	directoryBytes int64
	directory      *os.File
	buffer         []byte
	hash           hash.Hash
	written        int64
	emptyReads     int
}

func newStagingStream(ctx context.Context, file *os.File, input io.Reader, options StageOptions, directoryBytes int64, directory *os.File) *stagingStream {
	return &stagingStream{
		ctx: ctx, file: file, input: input, options: options, directoryBytes: directoryBytes,
		directory: directory, buffer: make([]byte, 32<<10), hash: sha256.New(),
	}
}

// read streams the bounded input into the temporary artifact, retaining its hash
// and byte count for finalization.
func (s *stagingStream) read() error {
	for {
		done, err := s.advance()
		if err != nil {
			return err
		}
		if done {
			break
		}
	}
	if s.written == 0 {
		return ErrStageEmpty
	}
	if err := s.ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (s *stagingStream) advance() (bool, error) {
	if err := s.ctx.Err(); err != nil {
		return false, err
	}
	n, readErr := s.input.Read(s.buffer)
	if n < 0 || n > len(s.buffer) {
		return false, ErrStageIO
	}
	if int64(n) > s.options.MaxBytes-s.written {
		return false, ErrStageTooLarge
	}
	if n > 0 {
		if err := s.writeChunk(n); err != nil {
			return false, err
		}
	}
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		if err := s.ctx.Err(); err != nil {
			return false, err
		}
		return false, fmt.Errorf("%w: reading artifact stream", ErrStageIO)
	}
	if readErr != nil {
		return true, nil
	}
	if n == 0 {
		s.emptyReads++
		if s.emptyReads >= 100 {
			return false, fmt.Errorf("%w: %w", ErrStageIO, io.ErrNoProgress)
		}
	}
	return false, nil
}

func (s *stagingStream) writeChunk(n int) error {
	if int64(n) > s.options.MaxDirectoryBytes-s.directoryBytes-s.written {
		return ErrStageQuota
	}
	if err := checkFreeSpace(s.directory, s.options.MinFreeBytes, int64(n)); err != nil {
		return err
	}
	s.emptyReads = 0
	if _, err := s.file.Write(s.buffer[:n]); err != nil {
		return fmt.Errorf("%w: writing temporary artifact", ErrStageIO)
	}
	_, _ = s.hash.Write(s.buffer[:n])
	s.written += int64(n)
	return nil
}

type stagedArtifactFile struct {
	ctx       context.Context
	file      *os.File
	options   StageOptions
	written   int64
	root      *os.Root
	name      string
	directory *os.File
}

// finalizeStagedFile seeks the temporary file to the start, inspects its GGUF
// metadata, applies the file mode, syncs, and verifies the on-disk identity and
// size of the staged artifact. It returns the inspected metadata.
func (s stagedArtifactFile) finalize() (GGUFMetadata, error) {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return GGUFMetadata{}, fmt.Errorf("%w: seeking temporary artifact", ErrStageIO)
	}
	metadata, err := InspectGGUF(s.file)
	if err != nil {
		return GGUFMetadata{}, err
	}
	if err := s.ctx.Err(); err != nil {
		return GGUFMetadata{}, err
	}
	if err := s.file.Chmod(s.options.FileMode); err != nil {
		return GGUFMetadata{}, ErrStageIO
	}
	if err := s.file.Sync(); err != nil {
		return GGUFMetadata{}, ErrStageIO
	}
	if err := checkFreeSpace(s.directory, s.options.MinFreeBytes, 0); err != nil {
		return GGUFMetadata{}, err
	}
	opened, err := s.file.Stat()
	if err != nil || !opened.Mode().IsRegular() || opened.Size() != s.written || opened.Mode().Perm() != s.options.FileMode {
		return GGUFMetadata{}, ErrStageIO
	}
	current, err := s.root.Lstat(s.name)
	if err != nil || !os.SameFile(opened, current) {
		return GGUFMetadata{}, ErrStageIO
	}
	return metadata, nil
}

type stagedArtifactCommit struct {
	ctx              context.Context
	root             *os.Root
	directory        *os.File
	name             string
	options          StageOptions
	source           Source
	existing         *os.File
	existingIdentity artifactFileIdentity
	result           StageResult
}

// commitReplacement atomically replaces the existing destination with the staged
// artifact, verifying that neither the artifact nor the directory changed during
// staging. It sets the publication and durability states on result.
func (s stagedArtifactCommit) replace() (StageResult, error) {
	if err := existingArtifactUnchanged(s.root, s.existing, s.source.Filename, s.existingIdentity); err != nil {
		return s.result, err
	}
	if err := artifactDirectoryUnchanged(s.directory, s.options.Directory); err != nil {
		return s.result, err
	}
	if err := s.ctx.Err(); err != nil {
		return s.result, err
	}
	if err := unix.Renameat(int(s.directory.Fd()), s.name, int(s.directory.Fd()), s.source.Filename); err != nil {
		s.result.Publication, s.result.Durability = PublicationUnknown, "unknown"
		return s.result, ErrStageCommitUnknown
	}
	s.result.Publication, s.result.Durability = PublicationPublished, "uncertain"
	if err := s.directory.Sync(); err != nil {
		return s.result, ErrStageDurability
	}
	s.result.Durability = "confirmed"
	return s.result, nil
}

// commitNewArtifact atomically publishes the staged artifact by linking it to
// the destination path, then syncs the directory. It sets the publication and
// durability states on result.
func (s stagedArtifactCommit) publishNew() (StageResult, error) {
	if err := unix.Linkat(int(s.directory.Fd()), s.name, int(s.directory.Fd()), s.source.Filename, 0); err != nil {
		s.result.Publication, s.result.Durability = PublicationUnknown, "unknown"
		if errors.Is(err, unix.EEXIST) {
			return s.result, errors.Join(ErrStageExists, ErrStageCommitUnknown)
		}
		return s.result, ErrStageCommitUnknown
	}
	s.result.Publication, s.result.Durability = PublicationPublished, "uncertain"
	if err := s.directory.Sync(); err != nil {
		return s.result, ErrStageDurability
	}
	s.result.Durability = "confirmed"
	return s.result, nil
}

// openArtifactDirectory opens the artifact directory, verifies that it is a
// regular directory owned by the caller with safe permissions, and returns a
// root handle plus an open directory file descriptor.
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

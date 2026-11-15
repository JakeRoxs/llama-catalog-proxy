package modelconfig

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// PreviewTarget reads a single explicitly configured file without modifying it.
// The target name supplied by a client is never used as a path.
func PreviewTarget(directory, filename, targetID, modelID string, settings CommandSettings) (Preview, error) {
	data, err := readTarget(directory, filename)
	if err != nil {
		return Preview{}, err
	}
	return PreviewDocument(targetID, modelID, data, settings)
}

// PreviewCreationTarget reads an explicitly configured target without modifying it.
func PreviewCreationTarget(directory, filename, targetID, modelID, templateID string, input CreationCommandInput) (CreationPreview, error) {
	data, err := readTarget(directory, filename)
	if err != nil {
		return CreationPreview{}, err
	}
	preview, err := PreviewCreationDocument(targetID, modelID, templateID, data, input)
	if err != nil {
		return CreationPreview{}, err
	}
	preview.TargetFingerprint, err = targetFingerprint(directory, filename)
	if err != nil {
		return CreationPreview{}, errors.New("cannot bind preview target")
	}
	return preview, nil
}

func ReadSettingsTarget(directory, filename, targetID, modelID string) (SettingsSnapshot, error) {
	data, err := readTarget(directory, filename)
	if err != nil {
		return SettingsSnapshot{}, err
	}
	return ReadSettingsDocument(targetID, modelID, data)
}

func readTarget(directory, filename string) ([]byte, error) {
	if !validTargetPath(directory, filename) {
		return nil, errors.New("invalid configured management target")
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, errors.New("management target directory unavailable")
	}
	defer root.Close()
	file, err := openTargetFile(root, filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, _, err := readOpenedTarget(root, filename, file)
	return data, err
}

func validTargetPath(directory, filename string) bool {
	return filepath.IsAbs(directory) && filepath.Clean(directory) == directory &&
		filename != "" && filename != "." && filename != ".." &&
		filepath.Base(filename) == filename && !strings.ContainsAny(filename, `/\`)
}

func readOpenedTarget(root *os.Root, filename string, file *os.File) ([]byte, os.FileInfo, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, nil, errors.New("management target could not be read")
	}
	before, err := file.Stat()
	if err != nil || !before.Mode().IsRegular() || before.Size() > maxConfigBytes {
		return nil, nil, errors.New("management target is not a bounded regular file")
	}
	data, err := io.ReadAll(io.LimitReader(file, maxConfigBytes+1))
	if err != nil || len(data) > maxConfigBytes {
		return nil, nil, errors.New("management target could not be read")
	}
	after, err := file.Stat()
	if err != nil || !os.SameFile(before, after) || before.Size() != after.Size() ||
		!before.ModTime().Equal(after.ModTime()) {
		return nil, nil, errors.New("management target changed while being read")
	}
	current, err := root.Lstat(filename)
	if err != nil || !current.Mode().IsRegular() || !os.SameFile(before, current) {
		return nil, nil, errors.New("management target changed while being read")
	}
	return data, after, nil
}

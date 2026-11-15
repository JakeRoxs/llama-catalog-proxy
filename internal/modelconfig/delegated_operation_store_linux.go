//go:build linux

package modelconfig

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sort"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const operationClockFilename = "operation-clock.json"

type operationClock struct {
	Since time.Time `json:"since"`
}

func operationInventory(root *os.Root, dir *os.File) ([]string, time.Time, error) {
	entries, err := dir.ReadDir(MaxProposals + 3)
	if err != nil && err != io.EOF || len(entries) > MaxProposals+2 {
		return nil, time.Time{}, ErrProposalStore
	}
	var ids []string
	var clock time.Time
	var temporary string
	for _, entry := range entries {
		name := entry.Name()
		switch {
		case name == operationClockFilename:
			data, err := readPrivateProposalFile(root, name)
			if err != nil {
				return nil, time.Time{}, ErrProposalStore
			}
			decoder := json.NewDecoder(bytes.NewReader(data))
			decoder.DisallowUnknownFields()
			var value operationClock
			var extra any
			if decoder.Decode(&value) != nil || value.Since.IsZero() || decoder.Decode(&extra) != io.EOF {
				return nil, time.Time{}, ErrProposalStore
			}
			clock = value.Since
		case strings.HasPrefix(name, delegatedOperationPrefix) && strings.HasSuffix(name, ".json"):
			id := strings.TrimSuffix(strings.TrimPrefix(name, delegatedOperationPrefix), ".json")
			if !validProposalID(id) || entry.IsDir() {
				return nil, time.Time{}, ErrProposalStore
			}
			ids = append(ids, id)
		case strings.HasPrefix(name, ".operation-") && strings.HasSuffix(name, ".tmp"):
			id := strings.TrimSuffix(strings.TrimPrefix(name, ".operation-"), ".tmp")
			if temporary != "" || !validProposalID(id) {
				return nil, time.Time{}, ErrProposalStore
			}
			if _, err := readPrivateProposalFile(root, name); err != nil {
				return nil, time.Time{}, ErrProposalStore
			}
			temporary = name
		default:
			return nil, time.Time{}, ErrProposalStore
		}
	}
	if len(ids) > MaxProposals || len(ids) != 0 && clock.IsZero() {
		return nil, time.Time{}, ErrProposalStore
	}
	if temporary != "" {
		if err := root.Remove(temporary); err != nil {
			return nil, time.Time{}, ErrProposalStore
		}
		if err := dir.Sync(); err != nil {
			return nil, time.Time{}, ErrProposalStore
		}
	}
	sort.Strings(ids)
	return ids, clock, nil
}

func writeOperationClock(root *os.Root, dir *os.File, now time.Time) error {
	data, err := json.Marshal(operationClock{Since: now.UTC()})
	if err != nil {
		return ErrProposalStore
	}
	return replaceOperationRecord(root, dir, operationClockFilename, data)
}

func replaceOperationRecord(root *os.Root, dir *os.File, name string, data []byte) (result error) {
	if len(data) > maxProposalBytes {
		return ErrProposalStore
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return ErrProposalStore
	}
	temp := ".operation-" + hex.EncodeToString(id[:]) + ".tmp"
	file, err := root.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return ErrProposalStore
	}
	defer func() {
		if err := root.Remove(temp); err == nil {
			if err := dir.Sync(); err != nil {
				result = errors.Join(result, ErrProposalStore)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, ErrProposalStore)
		}
	}()
	writeErr := file.Chmod(0o600)
	if writeErr == nil {
		var written int
		written, writeErr = file.Write(data)
		if writeErr == nil && written != len(data) {
			writeErr = io.ErrShortWrite
		}
	}
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		return ErrProposalStore
	}
	if err := unix.Renameat(int(dir.Fd()), temp, int(dir.Fd()), name); err != nil {
		return ErrProposalStore
	}
	if err := dir.Sync(); err != nil {
		return ErrProposalStore
	}
	return nil
}

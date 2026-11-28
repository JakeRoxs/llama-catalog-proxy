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
	"time"

	"golang.org/x/sys/unix"
)

const operationClockFilename = "operation-clock.json"

// delegatedOperationTempPrefix is the filename prefix for temporary operation
// replacement files created by replaceOperationRecord. Centralizing the
// ".operation-" literal here addresses go:S1192 (duplicated string literal x4)
// without changing temp filename or recovery semantics.
const delegatedOperationTempPrefix = ".operation-"

type operationClock struct {
	Since time.Time `json:"since"`
}

// readOperationClock reads and validates the operation clock file. It is shared
// by the inventory and read-only proposal listing paths so the same validation
// and failure behavior apply everywhere.
func readOperationClock(root *os.Root, name string) (time.Time, error) {
	data, err := readPrivateProposalFile(root, name)
	if err != nil {
		return time.Time{}, ErrProposalStore
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var value operationClock
	var extra any
	if decoder.Decode(&value) != nil || value.Since.IsZero() || decoder.Decode(&extra) != io.EOF {
		return time.Time{}, ErrProposalStore
	}
	return value.Since, nil
}

// processInventoryClock handles the operation-clock.json case of the inventory
// scan: decode the clock and store it in the caller's output.
func processInventoryClock(root *os.Root, name string, clock *time.Time) error {
	value, err := readOperationClock(root, name)
	if err != nil {
		return ErrProposalStore
	}
	*clock = value
	return nil
}

// processInventoryOperation handles the delegated-operation-<id>.json case:
// validate the id and append it to the inventory.
func processInventoryOperation(root *os.Root, name string, entry os.DirEntry, ids *[]string) error {
	id := strings.TrimSuffix(strings.TrimPrefix(name, delegatedOperationPrefix), ".json")
	if !validProposalID(id) || entry.IsDir() {
		return ErrProposalStore
	}
	*ids = append(*ids, id)
	return nil
}

// processInventoryTemp handles the delegated-operation-<id>.tmp case: validate
// uniqueness and id, then confirm the temp record is readable.
func processInventoryTemp(root *os.Root, name string, temporary *string) error {
	id := strings.TrimSuffix(strings.TrimPrefix(name, delegatedOperationTempPrefix), ".tmp")
	if *temporary != "" || !validProposalID(id) {
		return ErrProposalStore
	}
	if _, err := readPrivateProposalFile(root, name); err != nil {
		return ErrProposalStore
	}
	*temporary = name
	return nil
}

// processInventoryEntry dispatches one directory entry to the matching
// inventory handler. The switch and per-case guards live here instead of in
// the scan driver, keeping the driver's cognitive complexity low.
func processInventoryEntry(root *os.Root, entry os.DirEntry, ids *[]string, clock *time.Time, temporary *string) error {
	name := entry.Name()
	switch {
	case name == operationClockFilename:
		return processInventoryClock(root, name, clock)
	case strings.HasPrefix(name, delegatedOperationPrefix) && strings.HasSuffix(name, ".json"):
		return processInventoryOperation(root, name, entry, ids)
	case strings.HasPrefix(name, delegatedOperationTempPrefix) && strings.HasSuffix(name, ".tmp"):
		return processInventoryTemp(root, name, temporary)
	default:
		return ErrProposalStore
	}
}

// validateInventory enforces the proposal-count and clock invariants after the
// directory scan completes.
func validateInventory(count int, clock time.Time) error {
	if count > MaxProposals || count != 0 && clock.IsZero() {
		return ErrProposalStore
	}
	return nil
}

// recoverInventoryTemp removes an interrupted temporary replacement record and
// syncs the directory. It is a no-op when no temporary file is present.
func recoverInventoryTemp(root *os.Root, dir *os.File, temporary string) error {
	if temporary == "" {
		return nil
	}
	if err := root.Remove(temporary); err != nil {
		return ErrProposalStore
	}
	return dir.Sync()
}

// operationInventory validates and inventories the replay ledger. It recovers
// a temporary file left by an interrupted operation replacement, then returns
// the sorted proposal IDs and the durable clock.
func operationInventory(root *os.Root, dir *os.File) ([]string, time.Time, error) {
	entries, err := dir.ReadDir(MaxProposals + 3)
	if err != nil && err != io.EOF || len(entries) > MaxProposals+2 {
		return nil, time.Time{}, ErrProposalStore
	}
	var ids []string
	var clock time.Time
	var temporary string
	for _, entry := range entries {
		if err := processInventoryEntry(root, entry, &ids, &clock, &temporary); err != nil {
			return nil, time.Time{}, ErrProposalStore
		}
	}
	if err := validateInventory(len(ids), clock); err != nil {
		return nil, time.Time{}, ErrProposalStore
	}
	if err := recoverInventoryTemp(root, dir, temporary); err != nil {
		return nil, time.Time{}, ErrProposalStore
	}
	sort.Strings(ids)
	return ids, clock, nil
}

// processReadOnlyEntry dispatches one directory entry to the matching read-only
// listing handler. The operation store is inspected without recovery or
// mutation; a temporary file is treated as evidence of interruption.
func processReadOnlyEntry(root *os.Root, entry os.DirEntry, ids *map[string]bool, clock *time.Time) error {
	name := entry.Name()
	switch {
	case name == operationClockFilename:
		value, err := readOperationClock(root, name)
		if err != nil {
			return ErrProposalStore
		}
		*clock = value
	case strings.HasPrefix(name, delegatedOperationPrefix) && strings.HasSuffix(name, ".json"):
		id := strings.TrimSuffix(strings.TrimPrefix(name, delegatedOperationPrefix), ".json")
		if !validProposalID(id) || entry.IsDir() {
			return ErrProposalStore
		}
		if _, err := readDelegatedOperation(root, id); err != nil {
			return err
		}
		(*ids)[id] = true
	case strings.HasPrefix(name, delegatedOperationTempPrefix) && strings.HasSuffix(name, ".tmp"):
		// A temporary replacement may be evidence of an interrupted write.
		// Do not let proposal cleanup trigger the operation store's recovery.
		return ErrProposalStore
	default:
		return ErrProposalStore
	}
	return nil
}

// delegatedOperationProposalIDsReadOnly validates and inventories the replay
// ledger without recovering temporary files or otherwise changing it. Proposal
// cleanup fails closed if an interrupted operation replacement is present.
func delegatedOperationProposalIDsReadOnly(root *os.Root, dir *os.File) (map[string]bool, error) {
	entries, err := dir.ReadDir(MaxProposals + 3)
	if err != nil && err != io.EOF || len(entries) > MaxProposals+2 {
		return nil, ErrProposalStore
	}
	ids := make(map[string]bool)
	var clock time.Time
	for _, entry := range entries {
		if err := processReadOnlyEntry(root, entry, &ids, &clock); err != nil {
			return nil, err
		}
	}
	if err := validateInventory(len(ids), clock); err != nil {
		return nil, ErrProposalStore
	}
	return ids, nil
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
	temp := delegatedOperationTempPrefix + hex.EncodeToString(id[:]) + ".tmp"
	file, cleanup, err := openDelegatedStoreTemp(root, dir, temp)
	if err != nil {
		return ErrProposalStore
	}
	defer func() {
		if cleanupErr := cleanup(); cleanupErr != nil {
			result = errors.Join(result, cleanupErr)
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

//go:build linux || darwin || freebsd || openbsd || netbsd

package auditlog

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/identity"
)

const (
	auditLockName   = ".audit.lock"
	auditDataPrefix = "events-"
	auditDataSuffix = ".jsonl"
)

func Open(directory, localNodeID string) (*Store, error) {
	if !filepath.IsAbs(directory) || filepath.Clean(directory) != directory || !identity.ValidNodeID(localNodeID) {
		return nil, ErrUnavailable
	}
	info, err := os.Lstat(directory)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm()&0o077 != 0 || validateAuditDirectoryOwner(info) != nil {
		return nil, ErrUnavailable
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, ErrUnavailable
	}
	store := &Store{directory: directory, localID: localNodeID, root: root}
	if err := store.openLock(); err != nil {
		_ = root.Close()
		return nil, err
	}
	err = store.withLock(func() error {
		_, _, err := store.scan(time.Now().UTC(), true)
		return err
	})
	if err != nil {
		_ = store.Close()
		return nil, ErrUnavailable
	}
	return store, nil
}

func (s *Store) Begin(event Event) (*Transaction, error) {
	if event.ID == "" {
		id, err := NewEventID()
		if err != nil {
			return nil, err
		}
		event.ID = id
	}
	if event.LocalNodeID == "" {
		event.LocalNodeID = s.localID
	}
	if event.At.IsZero() {
		event.At = time.Now().UTC()
	}
	event.Phase, event.Outcome = "started", "started"
	if err := ValidateEvent(event); err != nil {
		return nil, err
	}
	if err := s.appendEvent(event); err != nil {
		return nil, err
	}
	return &Transaction{store: s, event: event}, nil
}

func (s *Store) Record(event Event) error {
	if event.ID == "" {
		id, err := NewEventID()
		if err != nil {
			return err
		}
		event.ID = id
	}
	if event.LocalNodeID == "" {
		event.LocalNodeID = s.localID
	}
	if event.At.IsZero() {
		event.At = time.Now().UTC()
	}
	if event.Phase == "" {
		event.Phase = "event"
	}
	if err := ValidateEvent(event); err != nil {
		return err
	}
	return s.appendEvent(event)
}

func (s *Store) finish(event Event) error {
	if err := ValidateEvent(event); err != nil {
		return err
	}
	return s.appendEvent(event)
}

func (s *Store) ListRecent(limit int) ([]Event, error) {
	if limit <= 0 || limit > MaxListEvents {
		return nil, ErrInvalid
	}
	var events []Event
	err := s.withLock(func() error {
		var err error
		events, _, err = s.scan(time.Now().UTC(), true)
		return err
	})
	if err != nil {
		return nil, err
	}
	byID := make(map[string]Event, len(events))
	for _, event := range events {
		if event.Phase == "started" {
			event.Outcome = "unknown"
		}
		byID[event.ID] = event
	}
	events = events[:0]
	for _, event := range byID {
		events = append(events, event)
	}
	sort.SliceStable(events, func(i, j int) bool { return events[i].At.Before(events[j].At) })
	if len(events) > limit {
		events = events[len(events)-limit:]
	}
	return events, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	lockErr := s.lock.Close()
	rootErr := s.root.Close()
	if lockErr != nil || rootErr != nil {
		return ErrUnavailable
	}
	return nil
}

func (s *Store) openLock() error {
	if info, err := s.root.Lstat(auditLockName); err == nil {
		if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
			return ErrUnavailable
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return ErrUnavailable
	}
	lock, err := s.root.OpenFile(auditLockName, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return ErrUnavailable
	}
	if err := lock.Chmod(0o600); err != nil {
		_ = lock.Close()
		return ErrUnavailable
	}
	info, err := lock.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || validateAuditFileOwner(info) != nil {
		_ = lock.Close()
		return ErrUnavailable
	}
	s.lock = lock
	return nil
}

func (s *Store) withLock(fn func() error) (resultErr error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.failed {
		return ErrUnavailable
	}
	unlock, err := lockAuditFile(s.lock)
	if err != nil {
		return ErrUnavailable
	}
	defer func() {
		if err := unlock(); err != nil {
			s.failed = true
			resultErr = ErrUnavailable
		}
	}()
	return fn()
}

func (s *Store) appendEvent(event Event) error {
	return s.withLock(func() error {
		events, totalBytes, err := s.scan(time.Now().UTC(), true)
		if err != nil {
			s.failed = true
			return ErrUnavailable
		}
		if !auditEventCanAppend(events, event) {
			if len(events) >= MaxEvents {
				return ErrCapacity
			}
			return ErrInvalid
		}
		line, err := json.Marshal(event)
		if err != nil || len(line)+1 > MaxEventBytes || totalBytes+int64(len(line)+1) > MaxBytes {
			return ErrCapacity
		}
		name := auditFilename(event.At)
		if info, err := s.root.Stat(name); err == nil {
			if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Size()+int64(len(line)+1) > MaxDayFileBytes {
				return ErrCapacity
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return ErrUnavailable
		}
		file, err := s.root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
		if err != nil {
			return ErrUnavailable
		}
		if err := file.Chmod(0o600); err != nil {
			_ = file.Close()
			s.failed = true
			return ErrUnavailable
		}
		data := append(line, '\n')
		written, writeErr := file.Write(data)
		if writeErr == nil && written != len(data) {
			writeErr = io.ErrShortWrite
		}
		if writeErr == nil {
			writeErr = file.Sync()
		}
		closeErr := file.Close()
		if writeErr != nil || closeErr != nil {
			s.failed = true
			return ErrUnavailable
		}
		directory, err := s.root.Open(".")
		if err != nil {
			s.failed = true
			return ErrUnavailable
		}
		err = directory.Sync()
		_ = directory.Close()
		if err != nil {
			s.failed = true
			return ErrUnavailable
		}
		return nil
	})
}

func (s *Store) scan(now time.Time, prune bool) ([]Event, int64, error) {
	directory, err := s.root.Open(".")
	if err != nil {
		return nil, 0, ErrUnavailable
	}
	entries, err := directory.ReadDir(-1)
	if err != nil {
		_ = directory.Close()
		return nil, 0, ErrUnavailable
	}
	cutoff := now.Add(-Retention + 24*time.Hour)
	var events []Event
	var totalBytes int64
	removed := false
	for _, entry := range entries {
		name := entry.Name()
		if name == auditLockName {
			continue
		}
		date, ok := parseAuditFilename(name)
		if !ok || entry.IsDir() {
			_ = directory.Close()
			return nil, 0, ErrUnavailable
		}
		if date.Before(time.Date(cutoff.Year(), cutoff.Month(), cutoff.Day(), 0, 0, 0, 0, time.UTC)) {
			if prune {
				if err := s.root.Remove(name); err != nil {
					_ = directory.Close()
					return nil, 0, ErrUnavailable
				}
				removed = true
			}
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 ||
			info.Size() > MaxDayFileBytes || validateAuditFileOwner(info) != nil {
			_ = directory.Close()
			return nil, 0, ErrUnavailable
		}
		totalBytes += info.Size()
		file, err := s.root.Open(name)
		if err != nil {
			_ = directory.Close()
			return nil, 0, ErrUnavailable
		}
		data, readErr := io.ReadAll(io.LimitReader(file, MaxDayFileBytes+1))
		closeErr := file.Close()
		if readErr != nil || closeErr != nil || len(data) > MaxDayFileBytes || (len(data) > 0 && data[len(data)-1] != '\n') {
			_ = directory.Close()
			return nil, 0, ErrUnavailable
		}
		for _, line := range bytes.Split(data, []byte{'\n'}) {
			if len(line) == 0 {
				continue
			}
			var event Event
			decoder := json.NewDecoder(bytes.NewReader(line))
			decoder.DisallowUnknownFields()
			if decoder.Decode(&event) != nil || decoder.Decode(new(any)) != io.EOF || ValidateEvent(event) != nil || auditFilename(event.At) != name {
				_ = directory.Close()
				return nil, 0, ErrUnavailable
			}
			events = append(events, event)
		}
	}
	if removed {
		if err := directory.Sync(); err != nil {
			_ = directory.Close()
			return nil, 0, ErrUnavailable
		}
	}
	if err := directory.Close(); err != nil || totalBytes > MaxBytes || len(events) > MaxEvents || !validAuditHistory(events) {
		return nil, 0, ErrUnavailable
	}
	return events, totalBytes, nil
}

func auditEventCanAppend(events []Event, event Event) bool {
	started := make(map[string]Event)
	finished := make(map[string]bool)
	for _, existing := range events {
		if existing.Phase == "started" {
			started[existing.ID] = existing
		} else if existing.Phase == "completed" {
			finished[existing.ID] = true
		}
	}
	switch event.Phase {
	case "started":
		for _, existing := range events {
			if existing.ID == event.ID {
				return false
			}
		}
		pending := 0
		for id := range started {
			if !finished[id] {
				pending++
			}
		}
		return len(events)+pending+2 <= MaxEvents
	case "event":
		for _, existing := range events {
			if existing.ID == event.ID {
				return false
			}
		}
		pending := 0
		for id := range started {
			if !finished[id] {
				pending++
			}
		}
		return len(events)+pending+1 <= MaxEvents
	case "completed":
		start, ok := started[event.ID]
		return ok && !finished[event.ID] && sameAuditAction(start, event)
	default:
		return false
	}
}

func validAuditHistory(events []Event) bool {
	started := make(map[string]Event)
	finished := make(map[string]bool)
	seen := make(map[string]bool)
	for _, event := range events {
		switch event.Phase {
		case "started":
			if seen[event.ID] {
				return false
			}
			seen[event.ID] = true
			started[event.ID] = event
		case "completed":
			start, ok := started[event.ID]
			if !ok || finished[event.ID] || !sameAuditAction(start, event) {
				return false
			}
			finished[event.ID] = true
		case "event":
			if seen[event.ID] {
				return false
			}
			seen[event.ID] = true
		default:
			return false
		}
	}
	return true
}

func sameAuditAction(start, completed Event) bool {
	if completed.TargetID != "" {
		start.TargetID = completed.TargetID
	}
	if completed.ModelID != "" {
		start.ModelID = completed.ModelID
	}
	if completed.CorrelationID != "" {
		start.CorrelationID = completed.CorrelationID
	}
	start.At, completed.At = time.Time{}, time.Time{}
	start.Phase, completed.Phase = "", ""
	start.Outcome, completed.Outcome = "", ""
	return start == completed
}

func auditFilename(at time.Time) string {
	return auditDataPrefix + at.UTC().Format("20060102") + auditDataSuffix
}

func parseAuditFilename(name string) (time.Time, bool) {
	if !strings.HasPrefix(name, auditDataPrefix) || !strings.HasSuffix(name, auditDataSuffix) {
		return time.Time{}, false
	}
	date, err := time.Parse("20060102", strings.TrimSuffix(strings.TrimPrefix(name, auditDataPrefix), auditDataSuffix))
	return date.UTC(), err == nil
}

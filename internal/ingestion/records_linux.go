//go:build linux

package ingestion

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"

	"golang.org/x/sys/unix"
)

const maxRecordBytes = 32 << 10

type linuxRecordStore struct {
	path  string
	root  *os.Root
	dir   *os.File
	limit int
}

func openRecordStore(directory string, limit int) (recordStore, error) {
	if !filepath.IsAbs(directory) || filepath.Clean(directory) != directory || limit < 1 || limit > 256 {
		return nil, ErrRecordStore
	}
	before, err := os.Lstat(directory)
	if err != nil || !before.IsDir() || before.Mode()&os.ModeSymlink != 0 || before.Mode().Perm()&0o077 != 0 {
		return nil, ErrRecordStore
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, ErrRecordStore
	}
	dir, err := root.Open(".")
	if err != nil {
		root.Close()
		return nil, ErrRecordStore
	}
	info, err := dir.Stat()
	var stat unix.Stat_t
	if err != nil || !os.SameFile(before, info) || !info.IsDir() || info.Mode().Perm()&0o077 != 0 ||
		unix.Fstat(int(dir.Fd()), &stat) != nil || stat.Uid != uint32(os.Geteuid()) ||
		unix.Flock(int(dir.Fd()), unix.LOCK_EX|unix.LOCK_NB) != nil {
		dir.Close()
		root.Close()
		return nil, ErrRecordStore
	}
	store := &linuxRecordStore{path: directory, root: root, dir: dir, limit: limit}
	records, err := store.List()
	if err == nil {
		for _, record := range records {
			if record.Job.State == "running" {
				record.Job.State = "interrupted"
				record.ReviewRequired = true
				if err = store.write(record, false); err != nil {
					break
				}
			}
		}
	}
	if err != nil {
		return nil, errors.Join(err, store.Close())
	}
	return store, nil
}

func (s *linuxRecordStore) checkDirectory() error {
	before, err := os.Lstat(s.path)
	if err != nil || !before.IsDir() || before.Mode()&os.ModeSymlink != 0 || before.Mode().Perm()&0o077 != 0 {
		return ErrRecordStore
	}
	info, err := s.dir.Stat()
	var stat unix.Stat_t
	if err != nil || !os.SameFile(before, info) || info.Mode().Perm()&0o077 != 0 ||
		unix.Fstat(int(s.dir.Fd()), &stat) != nil || stat.Uid != uint32(os.Geteuid()) {
		return ErrRecordStore
	}
	return nil
}

func (s *linuxRecordStore) Begin(job JobStatus) error {
	records, err := s.List()
	if err != nil {
		return err
	}
	for _, record := range records {
		if record.Job.State == "running" || record.ReviewRequired && !record.Acknowledged {
			return ErrRecordReview
		}
	}
	if len(records) >= s.limit {
		var oldest *Record
		for i := range records {
			record := &records[i]
			if record.Job.State != "running" && record.Job.State != "interrupted" &&
				(!record.ReviewRequired || record.Acknowledged) {
				oldest = record
				break
			}
		}
		if oldest == nil {
			return ErrRecordStoreFull
		}
		if err := s.root.Remove(oldest.Job.ID + ".json"); err != nil {
			return ErrRecordStore
		}
		if err := s.dir.Sync(); err != nil {
			return ErrRecordStore
		}
	}
	return s.write(Record{Version: 1, Job: job}, true)
}

func (s *linuxRecordStore) Finish(record Record) error {
	previous, err := s.Get(record.Job.ID)
	if err != nil || previous.Job.State != "running" || previous.Job.Source != record.Job.Source || previous.Job.Operation != record.Job.Operation ||
		!previous.Job.StartedAt.Equal(record.Job.StartedAt) || record.Job.State == "running" {
		return ErrRecordStore
	}
	return s.write(record, false)
}

func (s *linuxRecordStore) List() ([]Record, error) {
	if err := s.checkDirectory(); err != nil {
		return nil, err
	}
	if _, err := s.dir.Seek(0, io.SeekStart); err != nil {
		return nil, ErrRecordStore
	}
	entries, err := s.dir.ReadDir(s.limit + 1)
	if err != nil && err != io.EOF || len(entries) > s.limit {
		return nil, ErrRecordStore
	}
	records := make([]Record, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if len(name) < 6 || filepath.Ext(name) != ".json" {
			return nil, ErrRecordStore
		}
		id := name[:len(name)-5]
		if !jobIDPattern.MatchString(id) {
			return nil, ErrRecordStore
		}
		record, err := s.Get(id)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].Job.StartedAt.Equal(records[j].Job.StartedAt) {
			return records[i].Job.ID < records[j].Job.ID
		}
		return records[i].Job.StartedAt.Before(records[j].Job.StartedAt)
	})
	return records, nil
}

func (s *linuxRecordStore) Get(id string) (Record, error) {
	if !jobIDPattern.MatchString(id) {
		return Record{}, ErrRecordNotFound
	}
	if err := s.checkDirectory(); err != nil {
		return Record{}, err
	}
	file, err := s.root.OpenFile(id+".json", os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if errors.Is(err, os.ErrNotExist) {
		return Record{}, ErrRecordNotFound
	}
	if err != nil {
		return Record{}, ErrRecordStore
	}
	info, err := file.Stat()
	var stat unix.Stat_t
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Size() > maxRecordBytes ||
		unix.Fstat(int(file.Fd()), &stat) != nil || stat.Uid != uint32(os.Geteuid()) || stat.Nlink != 1 {
		file.Close()
		return Record{}, ErrRecordStore
	}
	data, err := io.ReadAll(io.LimitReader(file, maxRecordBytes+1))
	closeErr := file.Close()
	if err != nil || closeErr != nil || len(data) > maxRecordBytes {
		return Record{}, ErrRecordStore
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var record Record
	if err := decoder.Decode(&record); err != nil || record.Job.ID != id || !validRecord(record) {
		return Record{}, ErrRecordStore
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return Record{}, ErrRecordStore
	}
	return record, nil
}

func (s *linuxRecordStore) Acknowledge(id string) (Record, error) {
	record, err := s.Get(id)
	if err != nil {
		return Record{}, err
	}
	if !record.ReviewRequired || record.Acknowledged || record.Job.State == "running" {
		return Record{}, ErrRecordReview
	}
	record.Acknowledged = true
	if err := s.write(record, false); err != nil {
		return Record{}, err
	}
	return record, nil
}

func validRecord(record Record) bool {
	job := record.Job
	if record.Version != 1 || !jobIDPattern.MatchString(job.ID) || !validRecordedSource(job.Source, job.Operation) || job.StartedAt.IsZero() ||
		record.Acknowledged && !record.ReviewRequired || len(job.ErrorCodes) > 32 {
		return false
	}
	for _, code := range job.ErrorCodes {
		if !recordErrorCodePattern.MatchString(code) {
			return false
		}
		if code == "cleanup_failed" && !record.ReviewRequired {
			return false
		}
	}
	switch job.State {
	case "running", "interrupted":
		return job.CompletedAt.IsZero() && job.Result.Publication == PublicationUnknown &&
			job.Result.Durability == "unknown" && job.Result.Bytes == 0 && job.Result.SHA256 == "" &&
			job.Result.Metadata == nil && len(job.ErrorCodes) == 0 &&
			(job.State != "interrupted" || record.ReviewRequired) &&
			(job.State != "running" || !record.ReviewRequired && !record.Acknowledged)
	case "succeeded", "failed", "canceled", "timed_out":
		if job.CompletedAt.IsZero() || (job.State == "succeeded") != (len(job.ErrorCodes) == 0) {
			return false
		}
		switch job.Result.Publication {
		case PublicationNotPublished:
			return job.State != "succeeded" && job.Result.Durability == "not_applicable"
		case PublicationUnknown:
			return job.State == "failed" && job.Result.Durability == "unknown" && record.ReviewRequired
		case PublicationPublished:
			return job.Operation != "adopt" && job.State != "canceled" && job.State != "timed_out" &&
				(job.State != "failed" || record.ReviewRequired) &&
				(job.Result.Durability == "confirmed" || job.Result.Durability == "uncertain" && record.ReviewRequired) &&
				job.Result.Bytes > 0 && recordHashPattern.MatchString(job.Result.SHA256) &&
				job.Result.Integrity == "calculated_unverified" && job.Result.Metadata != nil
		case PublicationExisting:
			return job.Operation == "adopt" && job.State == "succeeded" && !record.ReviewRequired &&
				job.Result.Durability == "not_applicable" && job.Result.Bytes > 0 &&
				recordHashPattern.MatchString(job.Result.SHA256) && job.Result.Integrity == "calculated_unverified" &&
				job.Result.Metadata != nil
		default:
			return false
		}
	default:
		return false
	}
}

func (s *linuxRecordStore) write(record Record, create bool) (returnErr error) {
	if err := s.checkDirectory(); err != nil || !validRecord(record) {
		return ErrRecordStore
	}
	data, err := json.Marshal(record)
	if err != nil || len(data) > maxRecordBytes {
		return ErrRecordStore
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return ErrRecordStore
	}
	temp := ".record-" + hex.EncodeToString(random[:]) + ".tmp"
	file, err := s.root.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return ErrRecordStore
	}
	defer func() {
		if err := s.root.Remove(temp); err == nil {
			if err := s.dir.Sync(); err != nil {
				returnErr = errors.Join(returnErr, ErrRecordStore)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			returnErr = errors.Join(returnErr, ErrRecordStore)
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
		return ErrRecordStore
	}
	name := record.Job.ID + ".json"
	if create {
		err = unix.Renameat2(int(s.dir.Fd()), temp, int(s.dir.Fd()), name, unix.RENAME_NOREPLACE)
	} else {
		err = unix.Renameat(int(s.dir.Fd()), temp, int(s.dir.Fd()), name)
	}
	if err != nil {
		return ErrRecordStore
	}
	if err := s.dir.Sync(); err != nil {
		return ErrRecordStore
	}
	return nil
}

func (s *linuxRecordStore) Close() error {
	unlockErr := unix.Flock(int(s.dir.Fd()), unix.LOCK_UN)
	dirErr := s.dir.Close()
	rootErr := s.root.Close()
	if unlockErr != nil || dirErr != nil || rootErr != nil {
		return ErrRecordStore
	}
	return nil
}

package ingestion

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var (
	ErrJobActive   = errors.New("an ingestion job is already active")
	ErrJobNotFound = errors.New("ingestion job not found")
	ErrJobStopped  = errors.New("ingestion job manager is stopped")
	ErrJobFinished = errors.New("ingestion job is already finished")
)

// JobStatus describes a job; the manager retains only the latest in memory.
type JobStatus struct {
	ID          string      `json:"id"`
	Operation   string      `json:"operation,omitempty"`
	State       string      `json:"state"`
	Source      Source      `json:"source"`
	StartedAt   time.Time   `json:"started_at"`
	CompletedAt time.Time   `json:"completed_at,omitzero"`
	Result      StageResult `json:"result"`
	ErrorCodes  []string    `json:"error_codes,omitempty"`
}

type JobManager struct {
	mu      sync.Mutex
	parent  context.Context
	token   string
	current *JobStatus
	cancel  context.CancelFunc
	stopped bool
	blocked bool
	wg      sync.WaitGroup
	store   recordStore
	run     func(context.Context, StageOptions, time.Duration, string) (StageResult, error)
}

// NewPersistentJobManager acquires exclusive ownership of an existing private
// record directory until Stop. Interrupted jobs require acknowledgment.
func NewPersistentJobManager(parent context.Context, token, directory string, limit int) (*JobManager, error) {
	manager, err := NewJobManager(parent, token)
	if err != nil {
		return nil, err
	}
	store, err := openRecordStore(directory, limit)
	if err != nil {
		return nil, err
	}
	manager.store = store
	return manager, nil
}

func NewJobManager(parent context.Context, token string) (*JobManager, error) {
	if parent == nil || len(token) > 4096 || strings.ContainsAny(token, "\r\n") {
		return nil, ErrDownloadInvalid
	}
	return &JobManager{parent: parent, token: token, run: DownloadArtifact}, nil
}

func (m *JobManager) Start(options StageOptions, duration time.Duration) (JobStatus, error) {
	if options.MaxBytes < 1 || options.MaxDirectoryBytes < 0 || options.MinFreeBytes < 0 || duration <= 0 ||
		(options.FileMode != 0o600 && options.FileMode != 0o640) ||
		!filepath.IsAbs(options.Directory) || filepath.Clean(options.Directory) != options.Directory {
		return JobStatus{}, ErrStageInvalid
	}
	operation := ""
	switch {
	case options.AdoptExisting && options.AllowAdoption && !options.AllowDownload && !options.ReplaceExisting &&
		!options.AllowReplacement && !options.AcknowledgeExclusiveWindow:
		operation = "adopt"
	case options.ReplaceExisting && options.AllowDownload && options.AllowReplacement && options.AcknowledgeExclusiveWindow &&
		!options.AdoptExisting && !options.AllowAdoption:
		operation = "replace"
	case !options.AdoptExisting && !options.ReplaceExisting && options.AllowDownload && !options.AllowAdoption && !options.AllowReplacement && !options.AcknowledgeExclusiveWindow:
	default:
		return JobStatus{}, ErrStageInvalid
	}
	if !validRecordedSource(options.Source, operation) {
		return JobStatus{}, ErrStageInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped || m.parent.Err() != nil {
		return JobStatus{}, ErrJobStopped
	}
	if m.cancel != nil {
		return JobStatus{}, ErrJobActive
	}
	if m.blocked {
		return JobStatus{}, ErrRecordReview
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return JobStatus{}, errors.New("could not create ingestion job ID")
	}
	ctx, cancel := context.WithCancel(m.parent)
	status := JobStatus{
		ID: "ingest-" + hex.EncodeToString(id[:]), Operation: operation, State: "running", Source: options.Source, StartedAt: time.Now().UTC(),
		Result: StageResult{Publication: PublicationUnknown, Durability: "unknown"},
	}
	if m.store != nil {
		if err := m.store.Begin(status); err != nil {
			cancel()
			if errors.Is(err, ErrRecordStore) {
				m.blocked = true
			}
			return JobStatus{}, err
		}
	}
	m.current, m.cancel = &status, cancel
	m.wg.Add(1)
	go m.execute(ctx, options, duration)
	return status, nil
}

func (m *JobManager) Status(id string) (JobStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current == nil || m.current.ID != id {
		return JobStatus{}, ErrJobNotFound
	}
	return copyJobStatus(*m.current), nil
}

func (m *JobManager) Current() (JobStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current == nil {
		return JobStatus{}, ErrJobNotFound
	}
	return copyJobStatus(*m.current), nil
}

func (m *JobManager) Cancel(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current == nil || m.current.ID != id {
		return ErrJobNotFound
	}
	if m.cancel == nil {
		return ErrJobFinished
	}
	m.current.State = "canceling"
	m.cancel()
	return nil
}

func (m *JobManager) Stop() error {
	m.mu.Lock()
	m.stopped = true
	if m.cancel != nil {
		m.cancel()
	}
	m.mu.Unlock()
	m.wg.Wait()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.store != nil {
		err := m.store.Close()
		m.store = nil
		return err
	}
	return nil
}

func (m *JobManager) execute(ctx context.Context, options StageOptions, duration time.Duration) {
	defer m.wg.Done()
	var result StageResult
	var err error
	if options.AdoptExisting {
		result, err = adoptArtifact(ctx, options, duration)
	} else {
		result, err = m.run(ctx, options, duration, m.token)
	}
	state := "succeeded"
	if err != nil {
		state = "failed"
		if result.Publication == PublicationNotPublished {
			switch {
			case errors.Is(err, context.DeadlineExceeded):
				state = "timed_out"
			case errors.Is(err, context.Canceled):
				state = "canceled"
			}
		}
	}
	m.mu.Lock()
	m.current.State = state
	m.current.Result = result
	m.current.ErrorCodes = downloadErrorCodes(err)
	m.current.CompletedAt = time.Now().UTC()
	if m.store != nil {
		record := Record{
			Version: 1, Job: copyJobStatus(*m.current),
			ReviewRequired: result.Publication == PublicationUnknown ||
				errors.Is(err, ErrStageCleanup) || errors.Is(err, ErrStageDurability) ||
				err != nil && result.Publication == PublicationPublished,
		}
		if err := m.store.Finish(record); err != nil {
			m.current.State = "failed"
			m.current.ErrorCodes = append(m.current.ErrorCodes, "provenance_write_failed")
			m.blocked = true
		}
	}
	m.cancel()
	m.cancel = nil
	m.mu.Unlock()
}

func (m *JobManager) Records() ([]Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.store == nil {
		return nil, ErrRecordStore
	}
	return m.store.List()
}

func (m *JobManager) Record(id string) (Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.store == nil {
		return Record{}, ErrRecordStore
	}
	return m.store.Get(id)
}

func (m *JobManager) Acknowledge(id string) (Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.store == nil {
		return Record{}, ErrRecordStore
	}
	if m.cancel != nil || m.blocked {
		return Record{}, ErrRecordReview
	}
	return m.store.Acknowledge(id)
}

func downloadErrorCodes(err error) []string {
	if err == nil {
		return nil
	}
	var codes []string
	for _, entry := range []struct {
		err  error
		code string
	}{
		{context.Canceled, "canceled"}, {context.DeadlineExceeded, "timed_out"},
		{ErrStageInvalid, "invalid_request"}, {ErrStageDirectory, "unsafe_directory"},
		{ErrStageExists, "destination_exists"}, {ErrStageTooLarge, "too_large"},
		{ErrStageQuota, "directory_quota"}, {ErrStageFreeSpace, "free_space_reserve"},
		{ErrStageDiskCheck, "disk_check_failed"},
		{ErrStageEmpty, "empty"}, {ErrStageIO, "staging_io"},
		{ErrStageCleanup, "cleanup_failed"}, {ErrStageCommitUnknown, "publication_unknown"},
		{ErrStageDurability, "durability_uncertain"}, {ErrStageUnsupported, "unsupported_platform"},
		{ErrArtifactUnavailable, "artifact_unavailable"}, {ErrArtifactChanged, "artifact_changed"},
		{ErrArtifactBusy, "artifact_busy"},
		{ErrDownloadInvalid, "invalid_download"}, {ErrDownloadRedirect, "unsupported_redirect"},
		{ErrDownloadResponse, "unsupported_response"}, {ErrDownloadTransport, "transport_failed"},
		{ErrInvalidGGUF, "invalid_gguf"}, {ErrUnsupportedGGUF, "unsupported_gguf"},
		{ErrGGUFInspectLimit, "inspection_limit"}, {ErrGGUFRead, "inspection_read_failed"},
	} {
		if errors.Is(err, entry.err) {
			codes = append(codes, entry.code)
		}
	}
	if len(codes) == 0 {
		codes = append(codes, "internal_error")
	}
	return codes
}

func copyJobStatus(status JobStatus) JobStatus {
	status.ErrorCodes = append([]string(nil), status.ErrorCodes...)
	if status.Result.Metadata != nil {
		metadata := *status.Result.Metadata
		if metadata.Name != nil {
			value := *metadata.Name
			metadata.Name = &value
		}
		if metadata.Architecture != nil {
			value := *metadata.Architecture
			metadata.Architecture = &value
		}
		if metadata.FileType != nil {
			value := *metadata.FileType
			metadata.FileType = &value
		}
		status.Result.Metadata = &metadata
	}
	return status
}

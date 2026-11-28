package benchmark

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var (
	ErrJobActive   = errors.New("a benchmark job is already active")
	ErrJobNotFound = errors.New("benchmark job not found")
	ErrJobStopped  = errors.New("benchmark job manager is stopped")
)

type JobStatus struct {
	ID          string    `json:"id"`
	State       string    `json:"state"`
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at,omitzero"`
	RunID       string    `json:"run_id,omitempty"`
	ErrorCode   string    `json:"error_code,omitempty"`
}

type JobManager struct {
	mu        sync.Mutex
	parent    context.Context
	directory string
	logger    *slog.Logger
	current   *JobStatus
	cancel    context.CancelFunc
	stopped   bool
	wg        sync.WaitGroup
}

func NewJobManager(parent context.Context, directory string, logger *slog.Logger) (*JobManager, error) {
	if parent == nil || !filepath.IsAbs(directory) || logger == nil {
		return nil, errors.New("benchmark job manager requires a context, absolute report directory, and logger")
	}
	return &JobManager{parent: parent, directory: directory, logger: logger}, nil
}

func (m *JobManager) Start(options RunOptions) (JobStatus, error) {
	if _, err := requestedConfiguration(options); err != nil {
		return JobStatus{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped || m.parent.Err() != nil {
		return JobStatus{}, ErrJobStopped
	}
	if m.cancel != nil {
		return JobStatus{}, ErrJobActive
	}
	id, err := newRunID()
	if err != nil {
		return JobStatus{}, errors.New("could not create benchmark job ID")
	}
	id = "job-" + strings.TrimPrefix(id, "bench-")
	ctx, cancel := context.WithCancel(m.parent)
	status := JobStatus{ID: id, State: "running", StartedAt: time.Now().UTC()}
	m.current, m.cancel = &status, cancel
	m.wg.Add(1)
	go m.run(ctx, options)
	return status, nil
}

func (m *JobManager) Status(id string) (JobStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current == nil || m.current.ID != id {
		return JobStatus{}, ErrJobNotFound
	}
	return *m.current, nil
}

func (m *JobManager) Current() (JobStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current == nil {
		return JobStatus{}, ErrJobNotFound
	}
	return *m.current, nil
}

func (m *JobManager) Cancel(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current == nil || m.current.ID != id {
		return ErrJobNotFound
	}
	if m.cancel == nil {
		return errors.New("benchmark job is already finished")
	}
	m.current.State = "canceling"
	m.cancel()
	return nil
}

func (m *JobManager) Stop() {
	m.mu.Lock()
	m.stopped = true
	if m.cancel != nil {
		m.cancel()
	}
	m.mu.Unlock()
	m.wg.Wait()
}

func (m *JobManager) run(ctx context.Context, options RunOptions) {
	defer m.wg.Done()
	record, err := Run(ctx, options)
	state, errorCode, runID := "failed", "", ""
	if err != nil {
		if ctx.Err() != nil {
			state, errorCode = "canceled", "report_not_created"
		} else {
			errorCode = "run_failed"
			m.logger.Error("benchmark job failed", "error", err)
		}
	} else if _, err := WriteRun(m.directory, record); err != nil {
		errorCode, runID = "report_write_outcome_unknown", record.RunID
		m.logger.Error("benchmark report write failed", "error", err)
	} else {
		state, runID = string(record.Status), record.RunID
	}
	m.mu.Lock()
	m.current.State, m.current.ErrorCode, m.current.RunID = state, errorCode, runID
	m.current.CompletedAt = time.Now().UTC()
	m.cancel()
	m.cancel = nil
	m.mu.Unlock()
}

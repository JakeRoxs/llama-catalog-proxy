package ingestion

import (
	"errors"
	"regexp"
)

var (
	ErrRecordStore       = errors.New("ingestion record store unavailable or unsafe")
	ErrRecordNotFound    = errors.New("ingestion record not found")
	ErrRecordReview      = errors.New("ingestion record requires operator acknowledgment")
	ErrRecordStoreFull   = errors.New("ingestion record store has no removable completed records")
	ErrRecordUnsupported = errors.New("ingestion record storage is supported only on Linux")
)

var (
	jobIDPattern           = regexp.MustCompile(`^ingest-[0-9a-f]{32}$`)
	recordHashPattern      = regexp.MustCompile(`^[0-9a-f]{64}$`)
	recordErrorCodePattern = regexp.MustCompile(`^[a-z_]{1,64}$`)
)

// Record describes historical evidence, not the current state of an artifact.
type Record struct {
	Version        int       `json:"version"`
	Job            JobStatus `json:"job"`
	ReviewRequired bool      `json:"review_required"`
	Acknowledged   bool      `json:"acknowledged"`
}

type recordStore interface {
	Begin(JobStatus) error
	Finish(Record) error
	List() ([]Record, error)
	Get(string) (Record, error)
	Acknowledge(string) (Record, error)
	Close() error
}

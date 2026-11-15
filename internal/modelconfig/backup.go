package modelconfig

import (
	"errors"
	"time"
)

const MaxBackups = 32

var (
	ErrBackupStore    = errors.New("backup store unavailable or invalid")
	ErrBackupNotFound = errors.New("backup not found")
)

// BackupInfo contains metadata only; the original configuration is never returned.
type BackupInfo struct {
	ID                string    `json:"id"`
	CreatedAt         time.Time `json:"created_at"`
	TargetID          string    `json:"target_id"`
	TargetFingerprint string    `json:"target_fingerprint"`
	ModelID           string    `json:"model_id"`
	ProposalID        string    `json:"proposal_id"`
	SourceRevision    string    `json:"source_revision"`
	WrittenRevision   string    `json:"written_revision"`
}

type BackupSummary struct {
	ID     string      `json:"id"`
	Status string      `json:"status"`
	Info   *BackupInfo `json:"info,omitempty"`
}

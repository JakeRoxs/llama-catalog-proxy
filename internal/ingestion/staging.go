package ingestion

import (
	"errors"
	"os"
)

var (
	ErrStageInvalid       = errors.New("invalid or unauthorized artifact staging request")
	ErrStageDirectory     = errors.New("artifact directory unavailable or unsafe")
	ErrStageExists        = errors.New("artifact destination already exists")
	ErrStageTooLarge      = errors.New("artifact exceeds the configured byte limit")
	ErrStageQuota         = errors.New("artifact directory quota exceeded")
	ErrStageFreeSpace     = errors.New("filesystem free-space reserve would be exceeded")
	ErrStageDiskCheck     = errors.New("artifact disk usage could not be checked")
	ErrStageEmpty         = errors.New("artifact is empty")
	ErrStageIO            = errors.New("artifact staging failed")
	ErrStageCleanup       = errors.New("temporary artifact cleanup failed")
	ErrStageCommitUnknown = errors.New("artifact publication outcome is unknown")
	ErrStageDurability    = errors.New("artifact publication durability is uncertain")
	ErrStageUnsupported   = errors.New("artifact staging is supported only on Linux")
)

type PublicationState string

const (
	PublicationNotPublished PublicationState = "not_published"
	PublicationPublished    PublicationState = "published"
	PublicationExisting     PublicationState = "existing"
	PublicationUnknown      PublicationState = "unknown"
)

type StageOptions struct {
	Directory                  string
	Source                     Source
	MaxBytes                   int64
	MaxDirectoryBytes          int64
	MinFreeBytes               int64
	FileMode                   os.FileMode
	AllowDownload              bool
	AdoptExisting              bool
	ReplaceExisting            bool
	AllowAdoption              bool
	AllowReplacement           bool
	AcknowledgeExclusiveWindow bool
}

type StageResult struct {
	Publication PublicationState `json:"publication"`
	Durability  string           `json:"durability"`
	Bytes       int64            `json:"bytes,omitempty"`
	SHA256      string           `json:"sha256,omitempty"`
	Integrity   string           `json:"integrity,omitempty"`
	Metadata    *GGUFMetadata    `json:"metadata,omitempty"`
}

package modelconfig

import "errors"

var (
	ErrTargetWrite           = errors.New("target write unavailable or unsafe")
	ErrTargetChanged         = errors.New("target changed during managed write")
	ErrTargetNoChange        = errors.New("target content is unchanged")
	ErrTargetCleanup         = errors.New("temporary target cleanup failed")
	ErrTargetCommitUncertain = errors.New("target replaced but directory sync failed")
	ErrTargetRenameUncertain = errors.New("target rename outcome is uncertain")
	ErrRollbackStale         = errors.New("rollback source revision changed")
)

type MutationResult struct {
	BackupID       string
	Written        bool
	Durable        bool
	OutcomeUnknown bool
	Revision       string
}

// CreationArtifactBinding is supplied by the caller while it holds artifact
// and record locks. Recheck must revalidate that same observation.
type CreationArtifactBinding struct {
	RecordID  string
	Revision  string
	ModelPath string
	Recheck   func() error
}

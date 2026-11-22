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

// MutationTarget identifies a target file within a directory. It is shared by
// the delegated and creation proposal apply requests so callers group the
// directory and filename together instead of threading them as separate
// parameters.
type MutationTarget struct {
	// Directory is the parent directory of the target file.
	Directory string
	// Filename is the target file name within the directory.
	Filename string
}

// DelegatedProposalApplyRequest groups all arguments of
// ApplyStoredDelegatedProposal into a single cohesive request. The caller must
// still enforce authorization, directory separation, and an exclusive edit
// window under its lifecycle lock.
type DelegatedProposalApplyRequest struct {
	// ProposalDirectory is the directory containing stored delegated proposals.
	ProposalDirectory string
	// OperationDirectory is the directory containing stored delegated operations.
	OperationDirectory string
	// ProposalID is the stored proposal identifier.
	ProposalID string
	// OperationID is the delegated operation identifier.
	OperationID string
	// Target identifies the target file to mutate.
	Target MutationTarget
	// BackupDirectory is the backup store directory.
	BackupDirectory string
	// Limit is the maximum number of retained backups.
	Limit int
	// Binding is the delegated apply binding used for validation and outcome
	// persistence.
	Binding DelegatedApplyBinding
}

// CreationProposalApplyRequest groups all arguments of
// ApplyStoredCreationProposal into a single cohesive request. The caller must
// still enforce authorization, directory separation, an exclusive edit window,
// and the artifact binding's lifetime.
type CreationProposalApplyRequest struct {
	// ProposalDirectory is the directory containing stored creation proposals.
	ProposalDirectory string
	// ProposalID is the stored proposal identifier.
	ProposalID string
	// Target identifies the target file to mutate.
	Target MutationTarget
	// TargetID is the management target identifier.
	TargetID string
	// BackupDirectory is the backup store directory.
	BackupDirectory string
	// Limit is the maximum number of retained backups.
	Limit int
	// Artifact is the artifact binding used for validation and recheck.
	Artifact CreationArtifactBinding
}

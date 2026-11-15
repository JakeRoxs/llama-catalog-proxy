//go:build !linux

package modelconfig

func ApplyStoredDelegatedProposal(string, string, string, string, string, string, string, int, DelegatedApplyBinding) (DelegatedOperation, bool, error) {
	return DelegatedOperation{}, false, ErrProposalStore
}

func ApplyStoredProposal(string, string, string, string, string, string, int) (MutationResult, error) {
	return MutationResult{}, ErrTargetWrite
}

func ApplyStoredCreationProposal(string, string, string, string, string, string, int, CreationArtifactBinding) (MutationResult, error) {
	return MutationResult{}, ErrTargetWrite
}

func RollbackStoredBackup(string, string, string, string, string, int) (MutationResult, error) {
	return MutationResult{}, ErrTargetWrite
}

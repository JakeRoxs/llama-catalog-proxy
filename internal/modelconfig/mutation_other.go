//go:build !linux

package modelconfig

func ApplyStoredDelegatedProposal(req DelegatedProposalApplyRequest) (DelegatedOperation, bool, error) {
	return DelegatedOperation{}, false, ErrProposalStore
}

func ApplyStoredProposal(string, string, string, string, string, string, int) (MutationResult, error) {
	return MutationResult{}, ErrTargetWrite
}

func ApplyStoredCreationProposal(req CreationProposalApplyRequest) (MutationResult, error) {
	return MutationResult{}, ErrTargetWrite
}

func RollbackStoredBackup(string, string, string, string, string, int) (MutationResult, error) {
	return MutationResult{}, ErrTargetWrite
}

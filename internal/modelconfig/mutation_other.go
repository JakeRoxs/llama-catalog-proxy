//go:build !linux

package modelconfig

func ApplyStoredProposal(string, string, string, string, string, string, int) (MutationResult, error) {
	return MutationResult{}, ErrTargetWrite
}

func ApplyStoredCreationProposal(string, string, string, string, string, string, int, CreationArtifactBinding) (MutationResult, error) {
	return MutationResult{}, ErrTargetWrite
}

func RollbackStoredBackup(string, string, string, string, string, int) (MutationResult, error) {
	return MutationResult{}, ErrTargetWrite
}

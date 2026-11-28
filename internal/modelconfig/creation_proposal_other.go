//go:build !linux

package modelconfig

func SaveCreationProposal(string, string, string, CreationPreview, string, string, CreationCommandInput) (CreationProposal, error) {
	return CreationProposal{}, ErrProposalStore
}

func LoadCreationProposal(string, string) (CreationProposal, error) {
	return CreationProposal{}, ErrProposalStore
}

func RevalidateStoredCreationProposal(string, string, string, string, string, string, string) (CreationPreview, error) {
	return CreationPreview{}, ErrProposalStore
}

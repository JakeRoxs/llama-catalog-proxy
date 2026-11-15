//go:build !linux

package modelconfig

func SaveDelegatedProposal(string, DelegatedProposal) error { return ErrProposalStore }

func LoadDelegatedProposal(string, string) (DelegatedProposal, error) {
	return DelegatedProposal{}, ErrProposalStore
}

func ListDelegatedProposals(string) ([]string, error) { return nil, ErrProposalStore }

func DeleteDelegatedProposal(string, string) error { return ErrProposalStore }

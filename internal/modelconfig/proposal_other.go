//go:build !linux

package modelconfig

func SaveProposal(string, Proposal) error { return ErrProposalStore }

func ListProposals(string) ([]string, error) { return nil, ErrProposalStore }

func LoadProposal(string, string) (Proposal, error) { return Proposal{}, ErrProposalStore }

func LoadStoredProposal(string, string) (any, error) { return nil, ErrProposalStore }

func DeleteProposal(string, string) error { return ErrProposalStore }

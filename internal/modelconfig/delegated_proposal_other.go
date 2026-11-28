//go:build !linux

package modelconfig

import "time"

func SaveDelegatedProposal(string, DelegatedProposal) error { return ErrProposalStore }

func LoadDelegatedProposal(string, string) (DelegatedProposal, error) {
	return DelegatedProposal{}, ErrProposalStore
}

func ListDelegatedProposals(string) ([]string, error) { return nil, ErrProposalStore }

func DeleteDelegatedProposal(string, string) error { return ErrProposalStore }

func RecoverDelegatedProposal(string, DelegatedProposalRecoveryQuery, string, time.Time) (DelegatedProposal, error) {
	return DelegatedProposal{}, ErrProposalStore
}

func ListExpiredOrphanedDelegatedProposals(string, string, time.Time) ([]DelegatedProposalCleanupCandidate, error) {
	return nil, ErrProposalStore
}

func CleanupExpiredOrphanedDelegatedProposals(string, string, []string, time.Time) ([]string, error) {
	return nil, ErrProposalStore
}

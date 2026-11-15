//go:build !linux

package modelconfig

import "time"

func ReserveDelegatedOperation(string, DelegatedProposal, string, time.Time) (DelegatedOperation, bool, error) {
	return DelegatedOperation{}, false, ErrProposalStore
}

func LoadDelegatedOperation(string, string) (DelegatedOperation, error) {
	return DelegatedOperation{}, ErrProposalStore
}

func FinishDelegatedOperation(string, string, string, string, MutationResult, error, time.Time) (DelegatedOperation, error) {
	return DelegatedOperation{}, ErrProposalStore
}

func ReviewUnknownDelegatedOperation(string, string, string, string, time.Time) (DelegatedOperation, error) {
	return DelegatedOperation{}, ErrProposalStore
}

func PruneDelegatedOperations(string, time.Time) (int, error) {
	return 0, ErrProposalStore
}

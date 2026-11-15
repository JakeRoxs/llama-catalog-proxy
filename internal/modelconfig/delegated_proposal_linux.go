//go:build linux

package modelconfig

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
)

const delegatedProposalPrefix = "delegated-"

func delegatedProposalFilename(id string) string {
	return delegatedProposalPrefix + id + ".json"
}

// SaveDelegatedProposal stores a review in a private, dedicated directory.
// Storage does not authorize an action or prevent replay.
func SaveDelegatedProposal(directory string, proposal DelegatedProposal) error {
	if !validDelegatedProposal(proposal) {
		return ErrProposalInvalid
	}
	data, err := json.Marshal(proposal)
	if err != nil || len(data) > maxProposalBytes {
		return ErrProposalStore
	}
	return withProposalStore(directory, func(root *os.Root, dir *os.File) error {
		ids, err := privateProposalNames(dir, delegatedProposalPrefix)
		if err != nil {
			return err
		}
		if len(ids) >= MaxProposals {
			return ErrProposalStoreFull
		}
		return writePrivateRecord(root, dir, delegatedProposalFilename(proposal.Proposal.ID), data, ErrProposalStore)
	})
}

// LoadDelegatedProposal is a point-in-time read, not authorization. A future
// write must hold the store lock while rechecking the binding and applying.
func LoadDelegatedProposal(directory, id string) (DelegatedProposal, error) {
	if !validProposalID(id) {
		return DelegatedProposal{}, ErrProposalNotFound
	}
	var result DelegatedProposal
	err := withProposalStore(directory, func(root *os.Root, _ *os.File) error {
		var err error
		result, err = readDelegatedProposal(root, id)
		return err
	})
	if err != nil {
		return DelegatedProposal{}, err
	}
	return result, nil
}

func readDelegatedProposal(root *os.Root, id string) (DelegatedProposal, error) {
	if !validProposalID(id) {
		return DelegatedProposal{}, ErrProposalNotFound
	}
	data, err := readPrivateProposalFile(root, delegatedProposalFilename(id))
	if err != nil {
		return DelegatedProposal{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var result DelegatedProposal
	if err := decoder.Decode(&result); err != nil || result.Proposal.ID != id || !validDelegatedProposal(result) {
		return DelegatedProposal{}, ErrProposalStore
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return DelegatedProposal{}, ErrProposalStore
	}
	return result, nil
}

func ListDelegatedProposals(directory string) ([]string, error) {
	var ids []string
	err := withProposalStore(directory, func(_ *os.Root, dir *os.File) error {
		var err error
		ids, err = privateProposalNames(dir, delegatedProposalPrefix)
		return err
	})
	return ids, err
}

// DeleteDelegatedProposal removes a review, not any future replay or audit record.
func DeleteDelegatedProposal(directory, id string) error {
	if !validProposalID(id) {
		return ErrProposalNotFound
	}
	return withProposalStore(directory, func(root *os.Root, dir *os.File) error {
		if err := root.Remove(delegatedProposalFilename(id)); errors.Is(err, os.ErrNotExist) {
			return ErrProposalNotFound
		} else if err != nil {
			return ErrProposalStore
		}
		if err := dir.Sync(); err != nil {
			return ErrProposalStore
		}
		return nil
	})
}

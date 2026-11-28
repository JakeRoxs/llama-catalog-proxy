//go:build linux

package modelconfig

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
)

func SaveCreationProposal(store, directory, filename string, preview CreationPreview, recordID, artifactRevision string, input CreationCommandInput) (CreationProposal, error) {
	record, err := newCreationProposalRecord(directory, filename, preview, recordID, artifactRevision, input)
	if err != nil {
		return CreationProposal{}, err
	}
	data, err := json.Marshal(record)
	if err != nil || len(data) > maxProposalBytes {
		return CreationProposal{}, ErrProposalStore
	}
	err = withProposalStore(store, func(root *os.Root, dir *os.File) error {
		ids, err := proposalNames(dir)
		if err != nil {
			return err
		}
		if len(ids) >= MaxProposals {
			return ErrProposalStoreFull
		}
		return writePrivateRecord(root, dir, proposalFilename(record.Proposal.ID), data, ErrProposalStore)
	})
	if err != nil {
		return CreationProposal{}, err
	}
	return record.Proposal, nil
}

func readCreationProposal(root *os.Root, id string) (creationProposalRecord, error) {
	data, err := readProposalFile(root, id)
	if err != nil {
		return creationProposalRecord{}, err
	}
	return decodeCreationProposal(data, id)
}

func decodeCreationProposal(data []byte, id string) (creationProposalRecord, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var record creationProposalRecord
	if err := decoder.Decode(&record); err != nil || record.Proposal.ID != id || !validCreationProposalRecord(record) {
		return creationProposalRecord{}, ErrProposalStore
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return creationProposalRecord{}, ErrProposalStore
	}
	return record, nil
}

// LoadCreationProposal returns only the redacted review.
func LoadCreationProposal(store, id string) (CreationProposal, error) {
	var result CreationProposal
	err := withProposalStore(store, func(root *os.Root, _ *os.File) error {
		record, err := readCreationProposal(root, id)
		if err == nil {
			result = record.Proposal
		}
		return err
	})
	return result, err
}

// RevalidateStoredCreationProposal is a point-in-time read, not authorization to apply.
func RevalidateStoredCreationProposal(store, id, directory, filename, targetID, artifactRevision, modelPath string) (CreationPreview, error) {
	var result CreationPreview
	err := withProposalStore(store, func(root *os.Root, _ *os.File) error {
		record, err := readCreationProposal(root, id)
		if err != nil {
			return err
		}
		if err := checkCreationProposalBinding(directory, filename, targetID, artifactRevision, record.Proposal); err != nil {
			return err
		}
		data, err := readTarget(directory, filename)
		if err != nil {
			return err
		}
		result, err = revalidateCreationRecord(directory, filename, targetID, artifactRevision, modelPath, record, data)
		return err
	})
	return result, err
}

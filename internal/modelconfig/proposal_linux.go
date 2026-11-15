//go:build linux

package modelconfig

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sort"
	"strings"
	"syscall"
)

const maxProposalBytes = 16 << 10

func withProposalStore(directory string, fn func(*os.Root, *os.File) error) error {
	return withPrivateStore(directory, ErrProposalStore, fn)
}

func proposalNames(dir *os.File) ([]string, error) {
	entries, err := dir.ReadDir(MaxProposals + 1)
	if err != nil && err != io.EOF {
		return nil, ErrProposalStore
	}
	if len(entries) > MaxProposals {
		return nil, ErrProposalStore
	}
	ids := make([]string, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "proposal-") || !strings.HasSuffix(name, ".json") {
			return nil, ErrProposalStore
		}
		id := strings.TrimSuffix(strings.TrimPrefix(name, "proposal-"), ".json")
		if !validProposalID(id) || entry.IsDir() {
			return nil, ErrProposalStore
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}

func proposalFilename(id string) string {
	return "proposal-" + id + ".json"
}

func SaveProposal(directory string, proposal Proposal) error {
	if !validProposal(proposal) {
		return ErrProposalStore
	}
	data, err := json.Marshal(proposal)
	if err != nil || len(data) > maxProposalBytes {
		return ErrProposalStore
	}
	return withProposalStore(directory, func(root *os.Root, dir *os.File) error {
		ids, err := proposalNames(dir)
		if err != nil {
			return err
		}
		if len(ids) >= MaxProposals {
			return ErrProposalStoreFull
		}
		return writePrivateRecord(root, dir, proposalFilename(proposal.ID), data, ErrProposalStore)
	})
}

func ListProposals(directory string) ([]string, error) {
	var ids []string
	err := withProposalStore(directory, func(_ *os.Root, dir *os.File) error {
		var err error
		ids, err = proposalNames(dir)
		return err
	})
	return ids, err
}

func LoadProposal(directory, id string) (Proposal, error) {
	if !validProposalID(id) {
		return Proposal{}, ErrProposalNotFound
	}
	var result Proposal
	err := withProposalStore(directory, func(root *os.Root, _ *os.File) error {
		var err error
		result, err = readProposal(root, id)
		return err
	})
	if err != nil {
		return Proposal{}, err
	}
	return result, nil
}

func readProposal(root *os.Root, id string) (Proposal, error) {
	data, err := readProposalFile(root, id)
	if err != nil {
		return Proposal{}, err
	}
	return decodeProposal(data, id)
}

func decodeProposal(data []byte, id string) (Proposal, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var result Proposal
	if err := decoder.Decode(&result); err != nil || result.ID != id || !validProposal(result) {
		return Proposal{}, ErrProposalStore
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return Proposal{}, ErrProposalStore
	}
	return result, nil
}

// LoadStoredProposal returns either an existing-settings proposal or a redacted
// new-entry proposal from the shared store.
func LoadStoredProposal(directory, id string) (any, error) {
	var result any
	err := withProposalStore(directory, func(root *os.Root, _ *os.File) error {
		data, err := readProposalFile(root, id)
		if err != nil {
			return err
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(data, &fields); err != nil {
			return ErrProposalStore
		}
		if _, ok := fields["proposal"]; ok {
			record, err := decodeCreationProposal(data, id)
			if err != nil {
				return err
			}
			result = record.Proposal
		} else {
			result, err = decodeProposal(data, id)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return result, err
}

func readProposalFile(root *os.Root, id string) ([]byte, error) {
	if !validProposalID(id) {
		return nil, ErrProposalNotFound
	}
	file, err := root.OpenFile(proposalFilename(id), os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrProposalNotFound
	}
	if err != nil {
		return nil, ErrProposalStore
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() > maxProposalBytes {
		return nil, ErrProposalStore
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) || stat.Nlink != 1 {
		return nil, ErrProposalStore
	}
	data, err := io.ReadAll(io.LimitReader(file, maxProposalBytes+1))
	if err != nil || len(data) > maxProposalBytes {
		return nil, ErrProposalStore
	}
	return data, nil
}

func DeleteProposal(directory, id string) error {
	if !validProposalID(id) {
		return ErrProposalNotFound
	}
	return withProposalStore(directory, func(root *os.Root, dir *os.File) error {
		if err := root.Remove(proposalFilename(id)); errors.Is(err, os.ErrNotExist) {
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

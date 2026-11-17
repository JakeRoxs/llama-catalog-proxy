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
	"time"
)

const maxOwnerGrantRecordBytes = 32 << 10

func CreateOwnerGrant(directory string, grant OwnerGrant) error {
	if !ValidOwnerGrant(grant) || !grant.ExpiresAt.After(time.Now()) {
		return ErrOwnerGrantInvalid
	}
	return withPrivateStore(directory, ErrOwnerGrantStore, func(root *os.Root, dir *os.File) error {
		grants, err := readOwnerGrants(root)
		if err != nil {
			return ErrOwnerGrantStore
		}
		if len(grants) >= MaxOwnerGrants {
			return ErrOwnerGrantStore
		}
		for _, existing := range grants {
			if existing.ExpiresAt.After(grant.CreatedAt) && ownerGrantsOverlap(existing, grant) {
				return ErrOwnerGrantConflict
			}
		}
		data, err := json.Marshal(grant)
		if err != nil || len(data) > maxOwnerGrantRecordBytes {
			return ErrOwnerGrantInvalid
		}
		return writePrivateRecord(root, dir, ownerGrantFilename(grant.ID), data, ErrOwnerGrantStore)
	})
}

func ListOwnerGrants(directory string) ([]OwnerGrant, error) {
	var grants []OwnerGrant
	err := withPrivateStore(directory, ErrOwnerGrantStore, func(root *os.Root, _ *os.File) error {
		loaded, err := readOwnerGrants(root)
		if err != nil {
			return ErrOwnerGrantStore
		}
		grants = loaded
		return nil
	})
	if err != nil {
		return nil, err
	}
	return grants, nil
}

func OwnerGrantAuthorizes(directory, requesterNodeID, action, backendID, targetID, modelID string) (bool, error) {
	return OwnerGrantAuthorizesAt(directory, requesterNodeID, action, backendID, targetID, modelID, time.Now())
}

func OwnerGrantAuthorizesAt(directory, requesterNodeID, action, backendID, targetID, modelID string, now time.Time) (bool, error) {
	var allowed bool
	err := withPrivateStore(directory, ErrOwnerGrantStore, func(root *os.Root, _ *os.File) error {
		grants, err := readOwnerGrants(root)
		if err != nil {
			return ErrOwnerGrantStore
		}
		for _, grant := range grants {
			if OwnerGrantAllows(grant, requesterNodeID, action, backendID, targetID, modelID, now) {
				allowed = true
				break
			}
		}
		return nil
	})
	return allowed, err
}

func RevokeOwnerGrant(directory, grantID string) error {
	if !ValidDelegatedOperationID(grantID) {
		return ErrOwnerGrantInvalid
	}
	return withPrivateStore(directory, ErrOwnerGrantStore, func(root *os.Root, dir *os.File) error {
		name := ownerGrantFilename(grantID)
		if _, err := root.Stat(name); errors.Is(err, os.ErrNotExist) {
			return ErrOwnerGrantNotFound
		} else if err != nil {
			return ErrOwnerGrantStore
		}
		if err := root.Remove(name); err != nil {
			return ErrOwnerGrantStore
		}
		if err := dir.Sync(); err != nil {
			return ErrOwnerGrantStore
		}
		return nil
	})
}

func ValidateOwnerGrantStore(directory string) error {
	return withPrivateStore(directory, ErrOwnerGrantStore, func(root *os.Root, _ *os.File) error {
		_, err := readOwnerGrants(root)
		if err != nil {
			return ErrOwnerGrantStore
		}
		return nil
	})
}

func ownerGrantFilename(id string) string { return "owner-grant-" + id + ".json" }

func readOwnerGrants(root *os.Root) ([]OwnerGrant, error) {
	directory, err := root.Open(".")
	if err != nil {
		return nil, ErrOwnerGrantStore
	}
	defer directory.Close()
	entries, err := directory.ReadDir(-1)
	if err != nil {
		return nil, ErrOwnerGrantStore
	}
	if len(entries) > MaxOwnerGrants {
		return nil, ErrOwnerGrantStore
	}
	grants := make([]OwnerGrant, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "owner-grant-") || !strings.HasSuffix(name, ".json") || entry.IsDir() {
			return nil, ErrOwnerGrantStore
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Size() > maxOwnerGrantRecordBytes {
			return nil, ErrOwnerGrantStore
		}
		file, err := root.Open(name)
		if err != nil {
			return nil, ErrOwnerGrantStore
		}
		data, readErr := io.ReadAll(io.LimitReader(file, maxOwnerGrantRecordBytes+1))
		closeErr := file.Close()
		if readErr != nil || closeErr != nil || len(data) > maxOwnerGrantRecordBytes {
			return nil, ErrOwnerGrantStore
		}
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		var grant OwnerGrant
		if decoder.Decode(&grant) != nil || decoder.Decode(new(any)) != io.EOF || !ValidOwnerGrant(grant) || name != ownerGrantFilename(grant.ID) {
			return nil, ErrOwnerGrantStore
		}
		grants = append(grants, grant)
	}
	sort.Slice(grants, func(i, j int) bool { return grants[i].ID < grants[j].ID })
	for i := range grants {
		for j := 0; j < i; j++ {
			if ownerGrantsOverlap(grants[i], grants[j]) {
				return nil, ErrOwnerGrantStore
			}
		}
	}
	return grants, nil
}

func ownerGrantsOverlap(a, b OwnerGrant) bool {
	if a.RequesterNodeID != b.RequesterNodeID || a.Action != b.Action || a.BackendID != b.BackendID || a.TargetID != b.TargetID {
		return false
	}
	for _, modelA := range a.ModelIDs {
		for _, modelB := range b.ModelIDs {
			if modelA == modelB {
				return true
			}
		}
	}
	return false
}

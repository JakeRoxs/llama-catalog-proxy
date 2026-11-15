package modelconfig

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/identity"
)

const MaxProposals = 256

var (
	ErrProposalNotFound  = errors.New("proposal not found")
	ErrProposalStoreFull = errors.New("proposal store is full")
	ErrProposalStore     = errors.New("proposal store unavailable or invalid")
	ErrProposalInvalid   = errors.New("invalid proposal")
	ErrProposalTarget    = errors.New("proposal target changed")
	ErrProposalStale     = errors.New("proposal source revision changed")
	ErrProposalReview    = errors.New("proposal no longer matches reviewed changes")
)

// Proposal contains only the reviewed settings and their source binding.
// It is not authorization to apply the change.
type Proposal struct {
	ID                string          `json:"id"`
	CreatedAt         time.Time       `json:"created_at"`
	TargetID          string          `json:"target_id"`
	TargetFingerprint string          `json:"target_fingerprint"`
	ModelID           string          `json:"model_id"`
	Revision          string          `json:"revision"`
	Settings          CommandSettings `json:"settings"`
	Changes           []SettingChange `json:"changes"`
	Validation        string          `json:"validation"`
}

func NewProposal(directory, filename string, preview Preview, settings CommandSettings) (Proposal, error) {
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return Proposal{}, errors.New("cannot generate proposal identity")
	}
	fingerprint, err := targetFingerprint(directory, filename)
	if err != nil {
		return Proposal{}, errors.New("cannot bind proposal target")
	}
	return Proposal{
		ID: hex.EncodeToString(id), CreatedAt: time.Now().UTC(),
		TargetID: preview.TargetID, TargetFingerprint: fingerprint,
		ModelID: preview.ModelID, Revision: preview.Revision, Settings: settings,
		Changes: preview.Changes, Validation: preview.Validation,
	}, nil
}

func targetFingerprint(directory, filename string) (string, error) {
	target, err := json.Marshal([2]string{directory, filename})
	if err != nil {
		return "", err
	}
	fingerprint := sha256.Sum256(target)
	return hex.EncodeToString(fingerprint[:]), nil
}

// RevalidateProposalTarget rechecks the proposal against the currently read target.
// Callers performing writes must hold the managed-write lock and require an
// operator-controlled exclusive edit window throughout validation and replacement.
func RevalidateProposalTarget(directory, filename, targetID string, proposal Proposal) (Preview, error) {
	if !validProposal(proposal) {
		return Preview{}, ErrProposalInvalid
	}
	fingerprint, err := targetFingerprint(directory, filename)
	if err != nil {
		return Preview{}, errors.New("cannot bind proposal target")
	}
	if proposal.TargetID != targetID || proposal.TargetFingerprint != fingerprint {
		return Preview{}, ErrProposalTarget
	}
	data, err := readTarget(directory, filename)
	if err != nil {
		return Preview{}, err
	}
	if sourceRevision(data) != proposal.Revision {
		return Preview{}, ErrProposalStale
	}
	preview, err := PreviewDocument(targetID, proposal.ModelID, data, proposal.Settings)
	if err != nil {
		return Preview{}, err
	}
	if preview.Validation != proposal.Validation || !slices.Equal(preview.Changes, proposal.Changes) {
		return Preview{}, ErrProposalReview
	}
	return preview, nil
}

func validProposalID(id string) bool {
	if len(id) != 32 {
		return false
	}
	for _, c := range id {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func validProposal(p Proposal) bool {
	if !validProposalID(p.ID) || p.CreatedAt.IsZero() || !identity.ValidNodeID(p.TargetID) ||
		p.TargetID == "." || p.TargetID == ".." || p.ModelID == "" || !identity.ValidModelComponent(p.ModelID) ||
		!validDigest(p.Revision) || !validDigest(p.TargetFingerprint) ||
		p.Validation != "yaml_syntax_and_supported_command_only" {
		return false
	}
	requested := make(map[string]string)
	addNumber := func(name string, value *int32, minimum int32) bool {
		if value == nil {
			return true
		}
		if *value < minimum {
			return false
		}
		requested[name] = strconv.FormatInt(int64(*value), 10)
		return true
	}
	if !addNumber("context", p.Settings.ContextSize, 0) ||
		!addNumber("fit_context", p.Settings.FitContext, 0) ||
		!addNumber("fit_target", p.Settings.FitTarget, 0) ||
		!addNumber("cache_ram", p.Settings.CacheRAM, -1) {
		return false
	}
	if p.Settings.Fit != nil {
		requested["fit"] = "off"
		if *p.Settings.Fit {
			requested["fit"] = "on"
		}
	}
	if len(requested) == 0 || len(requested) != len(p.Changes) {
		return false
	}
	for _, change := range p.Changes {
		after, ok := requested[change.Setting]
		if !ok || after != change.After || validateExistingSetting(change.Setting, change.Before) != nil {
			return false
		}
		delete(requested, change.Setting)
	}
	return len(requested) == 0
}

func validDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, c := range value {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

package modelconfig

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/identity"
)

var ErrProposalArtifact = errors.New("proposal artifact observation changed")

// CreationProposal is the redacted review of a new entry. It does not authorize a write.
type CreationProposal struct {
	ID                string                `json:"id"`
	Kind              string                `json:"kind"`
	CreatedAt         time.Time             `json:"created_at"`
	TargetID          string                `json:"target_id"`
	TargetFingerprint string                `json:"target_fingerprint"`
	ModelID           string                `json:"model_id"`
	TemplateID        string                `json:"template_id,omitempty"`
	Revision          string                `json:"revision"`
	CandidateRevision string                `json:"candidate_revision"`
	ArtifactRecordID  string                `json:"artifact_record_id"`
	ArtifactRevision  string                `json:"artifact_revision"`
	Command           CreationCommandReview `json:"command"`
	Validation        string                `json:"validation"`
}

// The executable is stored privately so a proposal can be revalidated after restart.
// Neither the original configuration nor the generated command is stored here.
type creationProposalRecord struct {
	Proposal          CreationProposal `json:"proposal"`
	Executable        string           `json:"executable,omitempty"`
	Settings          CommandSettings  `json:"settings"`
	GPULayers         *int32           `json:"gpu_layers,omitempty"`
	DeviceEnvironment []string         `json:"device_environment,omitempty"`
}

func newCreationProposalRecord(directory, filename string, preview CreationPreview, recordID, artifactRevision string, input CreationCommandInput) (creationProposalRecord, error) {
	if !validTargetPath(directory, filename) || input.ModelPath != "" || input.TemplateCommand != "" ||
		len(input.TemplateEnvironment) != 0 || len(preview.candidate) == 0 || preview.Artifact != "regular_file_when_checked" {
		return creationProposalRecord{}, ErrProposalInvalid
	}
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return creationProposalRecord{}, errors.New("cannot generate proposal identity")
	}
	fingerprint, err := targetFingerprint(directory, filename)
	if err != nil {
		return creationProposalRecord{}, errors.New("cannot bind proposal target")
	}
	record := creationProposalRecord{
		Proposal: CreationProposal{
			ID: hex.EncodeToString(id), Kind: "create_model", CreatedAt: time.Now().UTC(),
			TargetID: preview.TargetID, TargetFingerprint: fingerprint, ModelID: preview.ModelID,
			TemplateID: preview.TemplateID, Revision: preview.Revision,
			CandidateRevision: sourceRevision(preview.candidate), ArtifactRecordID: recordID,
			ArtifactRevision: artifactRevision, Command: preview.Command, Validation: preview.Validation,
		},
		Executable: input.Executable, Settings: input.Settings, GPULayers: input.GPULayers,
		DeviceEnvironment: append([]string(nil), input.DeviceEnvironment...),
	}
	if !validCreationProposalRecord(record) {
		return creationProposalRecord{}, ErrProposalInvalid
	}
	return record, nil
}

func validCreationProposalRecord(record creationProposalRecord) bool {
	p := record.Proposal
	if !validProposalID(p.ID) || p.Kind != "create_model" || p.CreatedAt.IsZero() ||
		!identity.ValidNodeID(p.TargetID) || p.TargetID == "." || p.TargetID == ".." ||
		!validCreationModelID(p.ModelID) || p.TemplateID != "" && !validCreationModelID(p.TemplateID) ||
		!validDigest(p.TargetFingerprint) || !validDigest(p.Revision) || !validDigest(p.CandidateRevision) ||
		!strings.HasPrefix(p.ArtifactRecordID, "ingest-") || !validProposalID(strings.TrimPrefix(p.ArtifactRecordID, "ingest-")) ||
		!validDigest(p.ArtifactRevision) || p.Validation != "yaml_syntax_and_supported_command_only" ||
		!validCreationReview(p.Command) {
		return false
	}
	if p.TemplateID != "" {
		return record.Executable == "" && !hasCreationSettings(record.Settings) &&
			record.GPULayers == nil && len(record.DeviceEnvironment) == 0
	}
	command, err := BuildCreationCommand(CreationCommandInput{
		ModelPath: "/artifact/model.gguf", Executable: record.Executable, Settings: record.Settings,
		GPULayers: record.GPULayers, DeviceEnvironment: record.DeviceEnvironment,
	})
	return err == nil && sameCreationReview(command, p.Command)
}

func validCreationReview(review CreationCommandReview) bool {
	if len(review.Options) == 0 || len(review.Options) > 128 {
		return false
	}
	if _, err := creationEnvironment(review.DeviceEnvironment); err != nil {
		return false
	}
	seen := make(map[string]bool)
	for _, option := range review.Options {
		if seen[option.Name] {
			return false
		}
		seen[option.Name] = true
		if option.Name == "reasoning_preserve" {
			if option.Value != "enabled" {
				return false
			}
		} else if option.Name == "model" || !validCreationValue(option.Name, option.Value) {
			return false
		}
	}
	return seen["port"]
}

func sameCreationReview(a, b CreationCommandReview) bool {
	return slices.Equal(a.Options, b.Options) && slices.Equal(a.DeviceEnvironment, b.DeviceEnvironment)
}

func checkCreationProposalBinding(directory, filename, targetID, artifactRevision string, proposal CreationProposal) error {
	fingerprint, err := targetFingerprint(directory, filename)
	if err != nil || targetID != proposal.TargetID || fingerprint != proposal.TargetFingerprint {
		return ErrProposalTarget
	}
	if artifactRevision != proposal.ArtifactRevision {
		return ErrProposalArtifact
	}
	return nil
}

// The artifact revision and path must come from a fresh trusted artifact check.
// A caller that writes must hold the proposal/target locks and exclude outside writers.
func revalidateCreationRecord(directory, filename, targetID, artifactRevision, modelPath string, record creationProposalRecord, data []byte) (CreationPreview, error) {
	if !validCreationProposalRecord(record) {
		return CreationPreview{}, ErrProposalInvalid
	}
	p := record.Proposal
	if err := checkCreationProposalBinding(directory, filename, targetID, artifactRevision, p); err != nil {
		return CreationPreview{}, err
	}
	if sourceRevision(data) != p.Revision {
		return CreationPreview{}, ErrProposalStale
	}
	preview, err := PreviewCreationDocument(targetID, p.ModelID, p.TemplateID, data, CreationCommandInput{
		ModelPath: modelPath, Executable: record.Executable, Settings: record.Settings,
		GPULayers: record.GPULayers, DeviceEnvironment: record.DeviceEnvironment,
	})
	if err != nil {
		return CreationPreview{}, err
	}
	if preview.Validation != p.Validation || sourceRevision(preview.candidate) != p.CandidateRevision ||
		!sameCreationReview(preview.Command, p.Command) {
		return CreationPreview{}, ErrProposalReview
	}
	preview.Artifact = "regular_file_when_checked"
	return preview, nil
}

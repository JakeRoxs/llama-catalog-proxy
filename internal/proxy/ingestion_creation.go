package proxy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/ingestion"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
)

type recordedCreationPreview struct {
	Preview        modelconfig.CreationPreview `json:"preview"`
	Artifact       ingestion.ArtifactEvidence  `json:"artifact"`
	ReviewRevision string                      `json:"review_revision"`
}

// The caller must keep ingestion and management configuration stable.
func previewRecordedCreation(jobs *ingestion.JobManager, modelDirectory string, target config.ManagementTarget,
	targetID, modelID, templateID, recordID string, input modelconfig.CreationCommandInput) (recordedCreationPreview, error) {
	if jobs == nil || input.ModelPath != "" {
		return recordedCreationPreview{}, errors.New("invalid recorded creation request")
	}
	before, err := jobs.CheckRecordedArtifact(modelDirectory, recordID)
	if err != nil {
		return recordedCreationPreview{}, err
	}
	input.ModelPath = filepath.Join(modelDirectory, before.Source.Filename)
	preview, err := modelconfig.PreviewCreationTarget(target.Directory, target.Filename, targetID, modelID, templateID, input)
	if err != nil {
		return recordedCreationPreview{}, err
	}
	after, err := jobs.CheckRecordedArtifact(modelDirectory, recordID)
	if err != nil {
		return recordedCreationPreview{}, err
	}
	if before.Revision != after.Revision {
		return recordedCreationPreview{}, ingestion.ErrArtifactChanged
	}
	preview.Artifact = after.Presence
	result := recordedCreationPreview{Preview: preview, Artifact: after}
	review, err := json.Marshal(struct {
		Preview  modelconfig.CreationPreview `json:"preview"`
		Artifact ingestion.ArtifactEvidence  `json:"artifact"`
	}{Preview: preview, Artifact: after})
	if err != nil {
		return recordedCreationPreview{}, err
	}
	digest := sha256.Sum256(review)
	result.ReviewRevision = hex.EncodeToString(digest[:])
	return result, nil
}

func storeRecordedCreation(jobs *ingestion.JobManager, modelDirectory, store string, target config.ManagementTarget,
	targetID, modelID, templateID, recordID, expectedRevision, expectedArtifactRevision, expectedReviewRevision string,
	input modelconfig.CreationCommandInput) (modelconfig.CreationProposal, error) {
	preview, err := previewRecordedCreation(jobs, modelDirectory, target, targetID, modelID, templateID, recordID, input)
	if err != nil {
		return modelconfig.CreationProposal{}, err
	}
	if preview.Preview.Revision != expectedRevision {
		return modelconfig.CreationProposal{}, modelconfig.ErrProposalStale
	}
	if preview.Artifact.Revision != expectedArtifactRevision {
		return modelconfig.CreationProposal{}, modelconfig.ErrProposalArtifact
	}
	if preview.ReviewRevision != expectedReviewRevision {
		return modelconfig.CreationProposal{}, modelconfig.ErrProposalReview
	}
	proposal, err := modelconfig.SaveCreationProposal(store, target.Directory, target.Filename, preview.Preview,
		recordID, preview.Artifact.Revision, input)
	if err != nil && !errors.Is(err, modelconfig.ErrProposalInvalid) {
		return modelconfig.CreationProposal{}, errors.Join(modelconfig.ErrProposalStore, err)
	}
	return proposal, err
}

type creationRequest struct {
	TargetID          string                      `json:"target_id"`
	ModelID           string                      `json:"model_id"`
	TemplateID        string                      `json:"template_id,omitempty"`
	ArtifactRecordID  string                      `json:"artifact_record_id"`
	Executable        string                      `json:"executable,omitempty"`
	Settings          modelconfig.CommandSettings `json:"settings,omitempty"`
	GPULayers         *int32                      `json:"gpu_layers,omitempty"`
	DeviceEnvironment []string                    `json:"device_environment,omitempty"`
}

func (input creationRequest) command() modelconfig.CreationCommandInput {
	return modelconfig.CreationCommandInput{
		Executable: input.Executable, Settings: input.Settings,
		GPULayers: input.GPULayers, DeviceEnvironment: input.DeviceEnvironment,
	}
}

func validCreationDigest(value string) bool {
	digest, err := hex.DecodeString(value)
	return err == nil && len(digest) == sha256.Size && value == strings.ToLower(value)
}

func (r *Reloader) serveCreation(w http.ResponseWriter, req *http.Request, state *reloaderState, create bool) {
	if req.URL.RawQuery != "" || req.URL.ForceQuery || !sameIngestionOrigin(req) {
		http.Error(w, "invalid request origin or query", http.StatusForbidden)
		return
	}
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	contentType, _, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
	if err != nil || contentType != "application/json" || req.Header.Get("Content-Encoding") != "" {
		http.Error(w, "JSON request required", http.StatusUnsupportedMediaType)
		return
	}
	if req.Body == nil {
		http.Error(w, "request body required", http.StatusBadRequest)
		return
	}
	data, err := io.ReadAll(io.LimitReader(req.Body, maxManagementRequest+1))
	if err != nil {
		http.Error(w, "request could not be read", http.StatusBadRequest)
		return
	}
	if len(data) > maxManagementRequest {
		http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		return
	}
	if validateManagementJSON(data) != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var input creationRequest
	var sourceRevision, artifactRevision, reviewRevision string
	if create {
		var proposal struct {
			creationRequest
			Revision         string `json:"revision"`
			ArtifactRevision string `json:"artifact_revision"`
			ReviewRevision   string `json:"review_revision"`
		}
		if decoder.Decode(&proposal) != nil || !validCreationDigest(proposal.Revision) ||
			!validCreationDigest(proposal.ArtifactRevision) || !validCreationDigest(proposal.ReviewRevision) {
			http.Error(w, "invalid request or review revisions", http.StatusBadRequest)
			return
		}
		input, sourceRevision, artifactRevision, reviewRevision = proposal.creationRequest, proposal.Revision, proposal.ArtifactRevision, proposal.ReviewRevision
	} else if decoder.Decode(&input) != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	target, ok := state.management.Targets[input.TargetID]
	if !ok {
		http.NotFound(w, req)
		return
	}
	r.lifecycleMu.Lock()
	if r.stopped || r.state.Load() != state || r.ingestion == nil || r.ingestion.jobs == nil ||
		state.management.Ingestion == nil || r.ingestion.config.Directory != state.management.Ingestion.Directory ||
		r.ingestion.config.RecordDirectory != state.management.Ingestion.RecordDirectory ||
		validateIngestionDirectories(state.management, nil) != nil {
		r.lifecycleMu.Unlock()
		http.Error(w, "management configuration changed or ingestion unavailable", http.StatusServiceUnavailable)
		return
	}
	var result any
	if create {
		result, err = storeRecordedCreation(r.ingestion.jobs, r.ingestion.config.Directory, state.management.ProposalStore.Directory,
			target, input.TargetID, input.ModelID, input.TemplateID, input.ArtifactRecordID,
			sourceRevision, artifactRevision, reviewRevision, input.command())
	} else {
		result, err = previewRecordedCreation(r.ingestion.jobs, r.ingestion.config.Directory, target,
			input.TargetID, input.ModelID, input.TemplateID, input.ArtifactRecordID, input.command())
	}
	r.lifecycleMu.Unlock()
	if err != nil {
		switch {
		case errors.Is(err, modelconfig.ErrProposalStoreFull):
			http.Error(w, "proposal store is full", http.StatusConflict)
		case errors.Is(err, modelconfig.ErrProposalStale), errors.Is(err, modelconfig.ErrProposalArtifact),
			errors.Is(err, modelconfig.ErrProposalReview), errors.Is(err, ingestion.ErrArtifactChanged),
			errors.Is(err, ingestion.ErrArtifactUnavailable), errors.Is(err, ingestion.ErrArtifactRecord),
			errors.Is(err, ingestion.ErrRecordNotFound):
			http.Error(w, "creation review no longer matches or is unavailable", http.StatusConflict)
		case errors.Is(err, modelconfig.ErrProposalStore):
			http.Error(w, "proposal store unavailable; creation outcome may be uncertain", http.StatusServiceUnavailable)
		case errors.Is(err, ingestion.ErrRecordStore), errors.Is(err, ingestion.ErrArtifactBusy), errors.Is(err, ingestion.ErrJobActive),
			errors.Is(err, ingestion.ErrStageDirectory):
			http.Error(w, "creation service unavailable", http.StatusServiceUnavailable)
		default:
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		return
	}
	status := http.StatusOK
	if create {
		status = http.StatusCreated
	}
	writeJSON(r.logger, w, status, result)
}

func verifyRecordedCreation(jobs *ingestion.JobManager, modelDirectory, store string, target config.ManagementTarget,
	targetID, proposalID string) (recordedCreationPreview, error) {
	if jobs == nil {
		return recordedCreationPreview{}, errors.New("invalid recorded creation request")
	}
	proposal, err := modelconfig.LoadCreationProposal(store, proposalID)
	if err != nil {
		return recordedCreationPreview{}, err
	}
	before, err := jobs.CheckRecordedArtifact(modelDirectory, proposal.ArtifactRecordID)
	if err != nil {
		return recordedCreationPreview{}, err
	}
	preview, err := modelconfig.RevalidateStoredCreationProposal(store, proposalID, target.Directory, target.Filename,
		targetID, before.Revision, filepath.Join(modelDirectory, before.Source.Filename))
	if err != nil {
		return recordedCreationPreview{}, err
	}
	after, err := jobs.CheckRecordedArtifact(modelDirectory, proposal.ArtifactRecordID)
	if err != nil {
		return recordedCreationPreview{}, err
	}
	if before.Revision != after.Revision {
		return recordedCreationPreview{}, ingestion.ErrArtifactChanged
	}
	return recordedCreationPreview{Preview: preview, Artifact: after}, nil
}

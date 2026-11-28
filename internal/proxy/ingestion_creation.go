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

// recordedCreationPreviewRequest groups the arguments used to record a creation preview.
type recordedCreationPreviewRequest struct {
	jobs           *ingestion.JobManager
	modelDirectory string
	target         config.ManagementTarget
	targetID       string
	modelID        string
	templateID     string
	recordID       string
	input          modelconfig.CreationCommandInput
}

// withStableRecordedArtifact checks the recorded artifact before and after an
// operation. The operation receives the pre-operation artifact evidence. When
// the operation errors, the post-operation check is skipped and the error is
// returned immediately. On success the artifact is rechecked and
// ingestion.ErrArtifactChanged is returned when the revision changed; otherwise
// the result's Artifact field is replaced with the post-operation evidence.
func withStableRecordedArtifact(jobs *ingestion.JobManager, modelDirectory, recordID string,
	operation func(before ingestion.ArtifactEvidence) (recordedCreationPreview, error)) (recordedCreationPreview, error) {
	before, err := jobs.CheckRecordedArtifact(modelDirectory, recordID)
	if err != nil {
		return recordedCreationPreview{}, err
	}
	result, err := operation(before)
	if err != nil {
		return result, err
	}
	after, err := jobs.CheckRecordedArtifact(modelDirectory, recordID)
	if err != nil {
		return result, err
	}
	if before.Revision != after.Revision {
		return result, ingestion.ErrArtifactChanged
	}
	result.Artifact = after
	return result, nil
}

// The caller must keep ingestion and management configuration stable.
func previewRecordedCreation(req recordedCreationPreviewRequest) (recordedCreationPreview, error) {
	if req.jobs == nil || req.input.ModelPath != "" {
		return recordedCreationPreview{}, errors.New("invalid recorded creation request")
	}
	return withStableRecordedArtifact(req.jobs, req.modelDirectory, req.recordID, func(before ingestion.ArtifactEvidence) (recordedCreationPreview, error) {
		input := req.input
		input.ModelPath = filepath.Join(req.modelDirectory, before.Source.Filename)
		preview, err := modelconfig.PreviewCreationTarget(req.target.Directory, req.target.Filename, req.targetID, req.modelID, req.templateID, input)
		if err != nil {
			return recordedCreationPreview{}, err
		}
		preview.Artifact = before.Presence
		review, err := json.Marshal(struct {
			Preview  modelconfig.CreationPreview `json:"preview"`
			Artifact ingestion.ArtifactEvidence  `json:"artifact"`
		}{Preview: preview, Artifact: before})
		if err != nil {
			return recordedCreationPreview{}, err
		}
		digest := sha256.Sum256(review)
		return recordedCreationPreview{Preview: preview, ReviewRevision: hex.EncodeToString(digest[:])}, nil
	})
}

// storeRecordedCreationRequest groups the preview request, proposal store, and expected revisions.
type storeRecordedCreationRequest struct {
	previewRequest           recordedCreationPreviewRequest
	store                    string
	expectedRevision         string
	expectedArtifactRevision string
	expectedReviewRevision   string
}

func storeRecordedCreation(req storeRecordedCreationRequest) (modelconfig.CreationProposal, error) {
	preview, err := previewRecordedCreation(req.previewRequest)
	if err != nil {
		return modelconfig.CreationProposal{}, err
	}
	if preview.Preview.Revision != req.expectedRevision {
		return modelconfig.CreationProposal{}, modelconfig.ErrProposalStale
	}
	if preview.Artifact.Revision != req.expectedArtifactRevision {
		return modelconfig.CreationProposal{}, modelconfig.ErrProposalArtifact
	}
	if preview.ReviewRevision != req.expectedReviewRevision {
		return modelconfig.CreationProposal{}, modelconfig.ErrProposalReview
	}
	proposal, err := modelconfig.SaveCreationProposal(req.store, req.previewRequest.target.Directory, req.previewRequest.target.Filename, preview.Preview,
		req.previewRequest.recordID, preview.Artifact.Revision, req.previewRequest.input)
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
	data, handled := serveCreationGuards(w, req)
	if handled {
		return
	}
	input, sourceRevision, artifactRevision, reviewRevision, err := decodeCreationBody(data, create)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	target, ok := state.management.Targets[input.TargetID]
	if !ok {
		http.NotFound(w, req)
		return
	}
	result, ready, err := r.runCreationOperation(state, input, target, create, sourceRevision, artifactRevision, reviewRevision)
	if !ready {
		http.Error(w, "management configuration changed or ingestion unavailable", http.StatusServiceUnavailable)
		return
	}
	if err != nil {
		writeCreationError(w, err)
		return
	}
	status := http.StatusOK
	if create {
		status = http.StatusCreated
	}
	writeJSON(r.logger, w, status, result)
}

// serveCreationGuards validates the request origin, method, content type, body,
// size, and JSON shape. When handled is true the caller must not write a
// response; the guard has already done so. It preserves every early status and
// body of the previous inline implementation.
func serveCreationGuards(w http.ResponseWriter, req *http.Request) (data []byte, handled bool) {
	if req.URL.RawQuery != "" || req.URL.ForceQuery || !sameIngestionOrigin(req) {
		http.Error(w, "invalid request origin or query", http.StatusForbidden)
		return nil, true
	}
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return nil, true
	}
	contentType, _, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
	if err != nil || contentType != "application/json" || req.Header.Get("Content-Encoding") != "" {
		http.Error(w, "JSON request required", http.StatusUnsupportedMediaType)
		return nil, true
	}
	if req.Body == nil {
		http.Error(w, "request body required", http.StatusBadRequest)
		return nil, true
	}
	data, err = io.ReadAll(io.LimitReader(req.Body, maxManagementRequest+1))
	if err != nil {
		http.Error(w, "request could not be read", http.StatusBadRequest)
		return nil, true
	}
	if len(data) > maxManagementRequest {
		http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		return nil, true
	}
	if validateManagementJSON(data) != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return nil, true
	}
	return data, false
}

// decodeCreationBody decodes the validated management request body. When create
// is true it requires the create wrapper with valid revision digests; otherwise
// it decodes a plain creation request. The returned revisions are empty when
// create is false. Error messages match the previous inline implementation.
func decodeCreationBody(data []byte, create bool) (creationRequest, string, string, string, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if create {
		var proposal struct {
			creationRequest
			Revision         string `json:"revision"`
			ArtifactRevision string `json:"artifact_revision"`
			ReviewRevision   string `json:"review_revision"`
		}
		if decoder.Decode(&proposal) != nil || !validCreationDigest(proposal.Revision) ||
			!validCreationDigest(proposal.ArtifactRevision) || !validCreationDigest(proposal.ReviewRevision) {
			return creationRequest{}, "", "", "", errors.New("invalid request or review revisions")
		}
		return proposal.creationRequest, proposal.Revision, proposal.ArtifactRevision, proposal.ReviewRevision, nil
	}
	var input creationRequest
	if decoder.Decode(&input) != nil {
		return creationRequest{}, "", "", "", errors.New("invalid request")
	}
	return input, "", "", "", nil
}

// runCreationOperation performs the lifecycle-guarded creation operation. The
// lifecycle lock covers the availability validation and the store/preview call
// and is released before the caller maps errors to HTTP responses. When the
// runtime is unavailable it returns ready=false and no error.
func (r *Reloader) runCreationOperation(state *reloaderState, input creationRequest, target config.ManagementTarget, create bool, revisions ...string) (result any, ready bool, err error) {
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.stopped || r.state.Load() != state || r.ingestion == nil || r.ingestion.jobs == nil ||
		state.management.Ingestion == nil || r.ingestion.config.Directory != state.management.Ingestion.Directory ||
		r.ingestion.config.RecordDirectory != state.management.Ingestion.RecordDirectory ||
		validateIngestionDirectories(state.management, nil) != nil {
		return nil, false, nil
	}
	sourceRevision, artifactRevision, reviewRevision := revisions[0], revisions[1], revisions[2]
	previewRequest := recordedCreationPreviewRequest{
		jobs:           r.ingestion.jobs,
		modelDirectory: r.ingestion.config.Directory,
		target:         target,
		targetID:       input.TargetID,
		modelID:        input.ModelID,
		templateID:     input.TemplateID,
		recordID:       input.ArtifactRecordID,
		input:          input.command(),
	}
	if create {
		result, err = storeRecordedCreation(storeRecordedCreationRequest{
			previewRequest:           previewRequest,
			store:                    state.management.ProposalStore.Directory,
			expectedRevision:         sourceRevision,
			expectedArtifactRevision: artifactRevision,
			expectedReviewRevision:   reviewRevision,
		})
	} else {
		result, err = previewRecordedCreation(previewRequest)
	}
	return result, true, err
}

// writeCreationError maps creation operation errors to HTTP responses,
// preserving the exact status codes and bodies of the previous implementation.
func writeCreationError(w http.ResponseWriter, err error) {
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
	return withStableRecordedArtifact(jobs, modelDirectory, proposal.ArtifactRecordID, func(before ingestion.ArtifactEvidence) (recordedCreationPreview, error) {
		if before.Revision != proposal.ArtifactRevision {
			return recordedCreationPreview{}, modelconfig.ErrProposalArtifact
		}
		var preview modelconfig.CreationPreview
		preview, err = modelconfig.RevalidateStoredCreationProposal(store, proposalID, target.Directory, target.Filename,
			targetID, before.Revision, filepath.Join(modelDirectory, before.Source.Filename))
		if err != nil {
			return recordedCreationPreview{}, err
		}
		return recordedCreationPreview{Preview: preview}, nil
	})
}

package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/ingestion"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
)

type mutationResponse struct {
	Status     string `json:"status"`
	Durability string `json:"durability"`
	BackupID   string `json:"backup_id,omitempty"`
	Revision   string `json:"candidate_revision,omitempty"`
	Observed   string `json:"observed"`
	Effective  string `json:"effective"`
	Error      string `json:"error,omitempty"`
}

func (r *Reloader) serveMutation(w http.ResponseWriter, req *http.Request, state *reloaderState) {
	if req.URL.RawQuery != "" {
		http.NotFound(w, req)
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
	select {
	case r.managementSlots <- struct{}{}:
		defer func() { <-r.managementSlots }()
	default:
		http.Error(w, "management service busy", http.StatusServiceUnavailable)
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
	var input struct {
		AcknowledgeReview          bool `json:"acknowledge_review"`
		AcknowledgeDisruption      bool `json:"acknowledge_disruption"`
		AcknowledgeExclusiveWindow bool `json:"acknowledge_exclusive_window"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if validateManagementJSON(data) != nil || decoder.Decode(&input) != nil ||
		!input.AcknowledgeReview || !input.AcknowledgeDisruption || !input.AcknowledgeExclusiveWindow {
		http.Error(w, "review, disruption, and exclusive-window acknowledgements are required", http.StatusBadRequest)
		return
	}
	const proposals = "/management/v1/proposals/"
	const backups = "/management/v1/backups/"
	operation := "apply"
	id := strings.TrimSuffix(strings.TrimPrefix(req.URL.Path, proposals), "/apply")
	if strings.HasPrefix(req.URL.Path, backups) {
		operation = "rollback"
		id = strings.TrimSuffix(strings.TrimPrefix(req.URL.Path, backups), "/rollback")
	}

	r.lifecycleMu.Lock()
	if r.stopped || r.state.Load() != state {
		r.lifecycleMu.Unlock()
		http.Error(w, "management configuration changed", http.StatusServiceUnavailable)
		return
	}
	result, err := r.runMutation(state, operation, id)
	r.lifecycleMu.Unlock()

	response, code := mutationOutcome(result, err)
	r.logger.Info("management mutation", "operation", operation, "status", response.Status,
		"durability", response.Durability, "error", response.Error)
	writeJSON(r.logger, w, code, response)
}

func (r *Reloader) runMutation(state *reloaderState, operation, id string) (modelconfig.MutationResult, error) {
	management := state.management
	if management == nil || management.ProposalStore == nil || management.BackupStore == nil {
		return modelconfig.MutationResult{}, modelconfig.ErrTargetWrite
	}
	proposals, backups := management.ProposalStore.Directory, management.BackupStore.Directory
	if !separateProposalDirectory(proposals, management.Targets) ||
		!separateProposalDirectory(backups, management.Targets) || !separateDirectories(proposals, backups) {
		return modelconfig.MutationResult{}, modelconfig.ErrTargetWrite
	}
	var targetID string
	var creation *modelconfig.CreationProposal
	if operation == "apply" {
		proposal, err := modelconfig.LoadStoredProposal(proposals, id)
		if err != nil {
			return modelconfig.MutationResult{}, err
		}
		switch p := proposal.(type) {
		case modelconfig.Proposal:
			targetID = p.TargetID
		case modelconfig.CreationProposal:
			targetID, creation = p.TargetID, &p
		default:
			return modelconfig.MutationResult{}, modelconfig.ErrProposalStore
		}
	} else {
		backup, err := modelconfig.ReadBackupInfo(backups, id)
		if err != nil {
			return modelconfig.MutationResult{}, err
		}
		targetID = backup.TargetID
	}
	target, ok := management.Targets[targetID]
	if !ok {
		return modelconfig.MutationResult{}, modelconfig.ErrProposalTarget
	}
	if operation == "apply" {
		if creation != nil {
			if r.ingestion == nil || r.ingestion.jobs == nil || management.Ingestion == nil ||
				r.ingestion.config.Directory != management.Ingestion.Directory ||
				r.ingestion.config.RecordDirectory != management.Ingestion.RecordDirectory ||
				validateIngestionDirectories(management, nil) != nil {
				return modelconfig.MutationResult{}, modelconfig.ErrTargetWrite
			}
			var result modelconfig.MutationResult
			err := r.ingestion.jobs.WithRecordedArtifact(r.ingestion.config.Directory, creation.ArtifactRecordID,
				func(evidence ingestion.ArtifactEvidence, recheck func() (ingestion.ArtifactEvidence, error)) error {
					binding := modelconfig.CreationArtifactBinding{
						RecordID: evidence.RecordID, Revision: evidence.Revision,
						ModelPath: filepath.Join(r.ingestion.config.Directory, evidence.Source.Filename),
						Recheck: func() error {
							current, err := recheck()
							if err != nil {
								return err
							}
							if current.Revision != evidence.Revision {
								return modelconfig.ErrProposalArtifact
							}
							return nil
						},
					}
					var err error
					result, err = modelconfig.ApplyStoredCreationProposal(modelconfig.CreationProposalApplyRequest{
						ProposalDirectory: proposals,
						ProposalID:        id,
						Target: modelconfig.MutationTarget{
							Directory: target.Directory,
							Filename:  target.Filename,
						},
						TargetID:        targetID,
						BackupDirectory: backups,
						Limit:           management.BackupStore.MaxBackups,
						Artifact:        binding,
					})
					return err
				})
			return result, err
		}
		return modelconfig.ApplyStoredProposal(proposals, id, target.Directory, target.Filename, targetID, backups, management.BackupStore.MaxBackups)
	}
	return modelconfig.RollbackStoredBackup(target.Directory, target.Filename, targetID, backups, id, management.BackupStore.MaxBackups)
}

func mutationOutcome(result modelconfig.MutationResult, err error) (mutationResponse, int) {
	response := mutationResponse{
		Status: "not_written", Durability: "not_applicable", BackupID: result.BackupID,
		Revision: result.Revision, Observed: "unknown", Effective: "unknown",
	}
	if result.OutcomeUnknown {
		response.Status, response.Durability = "unknown", "unknown"
	} else if result.Written {
		response.Status, response.Durability = "written", "uncertain"
		if result.Durable {
			response.Durability = "confirmed"
		}
	}
	if err == nil && result.Written && result.Durable && !result.OutcomeUnknown &&
		result.BackupID != "" && result.Revision != "" {
		return response, http.StatusOK
	}
	if err == nil {
		response.Error = "unavailable"
		return response, http.StatusServiceUnavailable
	}
	switch {
	case errors.Is(err, modelconfig.ErrTargetCleanup):
		response.Error = "temporary_cleanup_failed"
	case errors.Is(err, modelconfig.ErrTargetRenameUncertain):
		response.Error = "rename_outcome_unknown"
	case errors.Is(err, modelconfig.ErrTargetCommitUncertain):
		response.Error = "directory_sync_failed"
	case errors.Is(err, modelconfig.ErrBackupStore):
		response.Error = "backup_store_unavailable"
	case errors.Is(err, modelconfig.ErrProposalNotFound), errors.Is(err, modelconfig.ErrBackupNotFound):
		response.Error = "not_found"
		return response, http.StatusNotFound
	case errors.Is(err, modelconfig.ErrProposalTarget), errors.Is(err, modelconfig.ErrProposalStale),
		errors.Is(err, modelconfig.ErrProposalReview), errors.Is(err, modelconfig.ErrProposalArtifact),
		errors.Is(err, ingestion.ErrArtifactChanged), errors.Is(err, ingestion.ErrArtifactUnavailable),
		errors.Is(err, ingestion.ErrArtifactRecord), errors.Is(err, ingestion.ErrRecordNotFound),
		errors.Is(err, modelconfig.ErrTargetChanged),
		errors.Is(err, modelconfig.ErrRollbackStale), errors.Is(err, modelconfig.ErrTargetNoChange):
		response.Error = "conflict"
		return response, http.StatusConflict
	default:
		response.Error = "unavailable"
	}
	return response, http.StatusServiceUnavailable
}

package proxy

import (
	"errors"
	"net/http"
	"slices"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
)

var (
	errDelegationUnauthorized   = errors.New("delegated settings not authorized")
	errDelegationTarget         = errors.New("delegated settings target unavailable")
	errDelegationChanged        = errors.New("delegated settings owner or source changed")
	errDelegationAcknowledgment = errors.New("delegated settings acknowledgments required")
)

type delegatedSettingsRequest struct {
	Owner    modelconfig.DelegatedOwner  `json:"owner"`
	Backend  string                      `json:"backend_id"`
	TargetID string                      `json:"target_id,omitempty"`
	Model    string                      `json:"model_id"`
	Revision string                      `json:"revision"`
	Settings modelconfig.CommandSettings `json:"settings"`
}

type delegatedSettingsApply struct {
	Backend                    string `json:"backend_id"`
	ProposalID                 string `json:"proposal_id"`
	OperationID                string `json:"operation_id"`
	ReviewRevision             string `json:"review_revision"`
	AcknowledgeReview          bool   `json:"acknowledge_review"`
	AcknowledgeDisruption      bool   `json:"acknowledge_disruption"`
	AcknowledgeExclusiveWindow bool   `json:"acknowledge_exclusive_window"`
}

// A future forwarding operation must recheck the state under the lifecycle
// lock before taking effect; this point-in-time check grants no peer access.
func (r *Reloader) delegatedCallerAuthorized(req *http.Request, state *reloaderState) bool {
	if req == nil || req.TLS == nil || state == nil || state.management == nil || r.state.Load() != state {
		return false
	}
	if managementBearerAuthorized(req, state.management.DelegationToken) {
		return true
	}
	if r.adminAuth == nil {
		return false
	}
	if _, ok := r.adminAuth.RequireRole(req, "configure_models"); !ok {
		return false
	}
	return req.Method == http.MethodGet || r.adminAuth.RequireCSRF(req)
}

func delegatedOwnerGrantAllows(state *reloaderState, requesterNodeID, backendID, targetID, modelID string) bool {
	if state == nil || state.peer == nil {
		return false
	}
	if state.peer.FederationID == "" {
		return true
	}
	if state.management == nil || state.management.DelegatedStore == nil || state.management.DelegatedStore.GrantDirectory == "" {
		return false
	}
	allowed, err := modelconfig.OwnerGrantAuthorizes(
		state.management.DelegatedStore.GrantDirectory, requesterNodeID, "manage_settings", backendID, targetID, modelID)
	return err == nil && allowed
}

// A receiving node must separately authorize its local caller.
func (r *Reloader) createDelegatedSettingsProposal(req *http.Request, state *reloaderState, input delegatedSettingsRequest, lifetime time.Duration) (result modelconfig.DelegatedProposal, retErr error) {
	if req == nil || req.TLS == nil || state == nil {
		return modelconfig.DelegatedProposal{}, errDelegationUnauthorized
	}
	requester, ok := authenticatedPeer(req, state, peerManageSettings)
	if !ok {
		return modelconfig.DelegatedProposal{}, errDelegationUnauthorized
	}
	auditTx, auditErr := beginPeerAudit(state, state.auditStore, requester, "settings.proposal.create", "", input.Model, "")
	if auditErr != nil {
		return modelconfig.DelegatedProposal{}, auditErr
	}
	auditTarget, auditCorrelation := "", ""
	defer func() {
		if err := finishAudit(auditTx, auditOutcomeForError(retErr), auditTarget, input.Model, auditCorrelation); err != nil {
			retErr = errDelegationOutcomeUnknown
		}
	}()

	proposal, targetID, stores, err := r.validateCreateSettingsPreLock(state, requester, input, lifetime)
	auditTarget = targetID
	if err != nil {
		return modelconfig.DelegatedProposal{}, err
	}
	auditCorrelation = proposal.Proposal.ID

	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.stopped || r.state.Load() != state {
		return modelconfig.DelegatedProposal{}, errDelegationChanged
	}
	if err := req.Context().Err(); err != nil {
		return modelconfig.DelegatedProposal{}, err
	}
	if err := r.validateCreateSettingsPostLock(req, state, requester, input, proposal, stores, targetID); err != nil {
		return modelconfig.DelegatedProposal{}, err
	}
	if err := modelconfig.SaveDelegatedProposal(stores.ProposalDirectory, proposal); err != nil {
		return modelconfig.DelegatedProposal{}, err
	}
	return proposal, nil
}

// validateCreateSettingsPreLock validates all pre-lock conditions for creating
// a delegated settings proposal. It constructs the proposal and returns it with
// the resolved target ID and stores, or an error if any validation fails.
func (r *Reloader) validateCreateSettingsPreLock(state *reloaderState, requester string, input delegatedSettingsRequest, lifetime time.Duration) (modelconfig.DelegatedProposal, string, *config.DelegatedStoreConfig, error) {
	if r.state.Load() != state {
		return modelconfig.DelegatedProposal{}, "", nil, errDelegationChanged
	}
	owner := modelconfig.DelegatedOwner{
		NodeID:           state.peer.NodeID,
		ProxyInstanceID:  r.instanceID,
		ConfigGeneration: state.generation,
	}
	if input.Owner != owner {
		return modelconfig.DelegatedProposal{}, "", nil, errDelegationChanged
	}
	if state.management == nil || !slices.Contains(state.peer.LocalBackends, input.Backend) {
		return modelconfig.DelegatedProposal{}, "", nil, errDelegationTarget
	}
	targetID, ok := state.peer.ManagementTargets[input.Backend]
	if !ok {
		return modelconfig.DelegatedProposal{}, "", nil, errDelegationTarget
	}
	target, targetExists := state.management.Targets[targetID]
	if !targetExists || input.TargetID != "" && input.TargetID != targetID {
		return modelconfig.DelegatedProposal{}, "", nil, errDelegationTarget
	}
	if !delegatedOwnerGrantAllows(state, requester, input.Backend, targetID, input.Model) {
		return modelconfig.DelegatedProposal{}, "", nil, errDelegationUnauthorized
	}
	stores := state.management.DelegatedStore
	if stores == nil || !r.separateDelegatedStores(state, stores.ProposalDirectory, stores.OperationDirectory) {
		return modelconfig.DelegatedProposal{}, "", nil, modelconfig.ErrProposalStore
	}
	preview, err := modelconfig.PreviewTarget(target.Directory, target.Filename, targetID, input.Model, input.Settings)
	if err != nil {
		return modelconfig.DelegatedProposal{}, "", nil, err
	}
	if preview.Revision != input.Revision {
		return modelconfig.DelegatedProposal{}, "", nil, errDelegationChanged
	}
	local, err := modelconfig.NewProposal(target.Directory, target.Filename, preview, input.Settings)
	if err != nil {
		return modelconfig.DelegatedProposal{}, "", nil, err
	}
	proposal, err := modelconfig.NewDelegatedProposal(owner, requester, input.Backend, local, lifetime)
	if err != nil {
		return modelconfig.DelegatedProposal{}, "", nil, err
	}
	return proposal, targetID, stores, nil
}

// validateCreateSettingsPostLock validates post-lock conditions for creating a
// delegated settings proposal. It runs under the lifecycle lock and returns an
// error if any validation fails.
func (r *Reloader) validateCreateSettingsPostLock(req *http.Request, state *reloaderState, requester string, input delegatedSettingsRequest, proposal modelconfig.DelegatedProposal, stores *config.DelegatedStoreConfig, targetID string) error {
	if r.stopped || r.state.Load() != state {
		return errDelegationChanged
	}
	if err := req.Context().Err(); err != nil {
		return err
	}
	if !r.separateDelegatedStores(state, stores.ProposalDirectory, stores.OperationDirectory) {
		return modelconfig.ErrProposalStore
	}
	if !delegatedOwnerGrantAllows(state, requester, input.Backend, targetID, input.Model) {
		return errDelegationUnauthorized
	}
	return nil
}

// recoverDelegatedSettingsProposal returns a unique exact match for the
// currently authenticated management peer. The shared peer credential proves
// scope possession, not an individual or the original request's delivery state.
func (r *Reloader) recoverDelegatedSettingsProposal(req *http.Request, state *reloaderState, input delegatedSettingsRequest) (result modelconfig.DelegatedProposal, retErr error) {
	if req == nil || req.TLS == nil || state == nil {
		return modelconfig.DelegatedProposal{}, errDelegationUnauthorized
	}
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.stopped || r.state.Load() != state {
		return modelconfig.DelegatedProposal{}, errDelegationChanged
	}
	requester, ok := authenticatedPeer(req, state, peerManageSettings)
	if !ok {
		return modelconfig.DelegatedProposal{}, errDelegationUnauthorized
	}
	auditTx, auditErr := beginPeerAudit(state, state.auditStore, requester, "settings.proposal.recover", input.TargetID, input.Model, "")
	if auditErr != nil {
		return modelconfig.DelegatedProposal{}, auditErr
	}
	auditTarget, auditCorrelation := input.TargetID, ""
	defer func() {
		if err := finishAudit(auditTx, auditOutcomeForError(retErr), auditTarget, input.Model, auditCorrelation); err != nil {
			retErr = errDelegationOutcomeUnknown
		}
	}()
	if err := req.Context().Err(); err != nil {
		return modelconfig.DelegatedProposal{}, err
	}
	owner := modelconfig.DelegatedOwner{NodeID: state.peer.NodeID, ProxyInstanceID: r.instanceID, ConfigGeneration: state.generation}
	if input.Owner != owner {
		return modelconfig.DelegatedProposal{}, errDelegationChanged
	}
	if state.management == nil || !slices.Contains(state.peer.LocalBackends, input.Backend) {
		return modelconfig.DelegatedProposal{}, errDelegationTarget
	}
	targetID, ok := state.peer.ManagementTargets[input.Backend]
	if !ok || input.TargetID != "" && input.TargetID != targetID {
		return modelconfig.DelegatedProposal{}, errDelegationTarget
	}
	auditTarget = targetID
	if _, ok := state.management.Targets[targetID]; !ok {
		return modelconfig.DelegatedProposal{}, errDelegationTarget
	}
	if !delegatedOwnerGrantAllows(state, requester, input.Backend, targetID, input.Model) {
		return modelconfig.DelegatedProposal{}, errDelegationUnauthorized
	}
	stores := state.management.DelegatedStore
	if stores == nil || !r.separateDelegatedStores(state, stores.ProposalDirectory, stores.OperationDirectory) {
		return modelconfig.DelegatedProposal{}, modelconfig.ErrProposalStore
	}
	query := modelconfig.DelegatedProposalRecoveryQuery{
		Owner: owner, RequesterNode: requester, BackendID: input.Backend, TargetID: targetID,
		ModelID: input.Model, Revision: input.Revision, Settings: input.Settings,
	}
	result, retErr = modelconfig.RecoverDelegatedProposal(stores.ProposalDirectory, query,
		modelconfig.DelegatedSettingsCredentialScope, time.Now())
	if retErr == nil {
		auditCorrelation = result.Proposal.ID
	}
	return result, retErr
}

func (r *Reloader) separateDelegatedStore(store string, state *reloaderState) bool {
	if state.management == nil || !separateProposalDirectory(store, state.management.Targets) {
		return false
	}
	if state.management.ProposalStore != nil && !separateDirectories(store, state.management.ProposalStore.Directory) ||
		state.management.BackupStore != nil && !separateDirectories(store, state.management.BackupStore.Directory) {
		return false
	}
	if state.management.Ingestion != nil &&
		(!separateDirectories(store, state.management.Ingestion.Directory) ||
			!separateDirectories(store, state.management.Ingestion.RecordDirectory)) {
		return false
	}
	return r.benchmark == nil || separateDirectories(store, r.benchmark.config.ReportDirectory)
}

// The receiving node must separately authorize its caller and collect the
// acknowledgments; peer authentication does not identify an individual.
func (r *Reloader) applyDelegatedSettings(req *http.Request, state *reloaderState, input delegatedSettingsApply) (record modelconfig.DelegatedOperation, committed bool, retErr error) {
	if req == nil || req.TLS == nil || state == nil {
		return modelconfig.DelegatedOperation{}, false, errDelegationUnauthorized
	}
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.stopped || r.state.Load() != state {
		return modelconfig.DelegatedOperation{}, false, errDelegationChanged
	}
	requester, ok := authenticatedPeer(req, state, peerManageSettings)
	if !ok {
		return modelconfig.DelegatedOperation{}, false, errDelegationUnauthorized
	}
	auditTx, auditErr := beginPeerAudit(state, state.auditStore, requester, "settings.apply", "", "", "")
	if auditErr != nil {
		return modelconfig.DelegatedOperation{}, false, auditErr
	}
	auditTarget, auditModel, auditCorrelation := "", "", ""
	defer func() {
		if err := finishAudit(auditTx, auditOutcomeForError(retErr), auditTarget, auditModel, auditCorrelation); err != nil {
			retErr = errDelegationOutcomeUnknown
		}
	}()
	if err := req.Context().Err(); err != nil {
		return modelconfig.DelegatedOperation{}, false, err
	}
	if !input.AcknowledgeReview || !input.AcknowledgeDisruption || !input.AcknowledgeExclusiveWindow {
		return modelconfig.DelegatedOperation{}, false, errDelegationAcknowledgment
	}

	proposal, targetID, target, stores, management, err := r.validateApplySettingsPreCommit(state, requester, input)
	if err != nil {
		return modelconfig.DelegatedOperation{}, false, err
	}
	auditTarget, auditModel = targetID, proposal.Proposal.ModelID
	if modelconfig.ValidDelegatedOperationID(input.OperationID) {
		auditCorrelation = input.OperationID
	}
	if !delegatedOwnerGrantAllows(state, requester, input.Backend, targetID, proposal.Proposal.ModelID) {
		return modelconfig.DelegatedOperation{}, false, errDelegationUnauthorized
	}
	binding := modelconfig.DelegatedApplyBinding{
		Owner: modelconfig.DelegatedOwner{
			NodeID: state.peer.NodeID, ProxyInstanceID: r.instanceID, ConfigGeneration: state.generation,
		},
		RequesterNode: requester, BackendID: input.Backend, TargetID: targetID, ReviewRevision: input.ReviewRevision,
	}
	record, committed, retErr = modelconfig.ApplyStoredDelegatedProposal(modelconfig.DelegatedProposalApplyRequest{
		ProposalDirectory:  stores.ProposalDirectory,
		OperationDirectory: stores.OperationDirectory,
		ProposalID:         input.ProposalID,
		OperationID:        input.OperationID,
		Target: modelconfig.MutationTarget{
			Directory: target.Directory,
			Filename:  target.Filename,
		},
		BackupDirectory: management.BackupStore.Directory,
		Limit:           management.BackupStore.MaxBackups,
		Binding:         binding,
	})
	return record, committed, retErr
}

// validateApplySettingsPreCommit validates all pre-commit conditions for
// applying a delegated settings proposal. It returns the loaded proposal,
// target, stores, and management config, or an error if any validation fails.
func (r *Reloader) validateApplySettingsPreCommit(state *reloaderState, requester string, input delegatedSettingsApply) (modelconfig.DelegatedProposal, string, config.ManagementTarget, *config.DelegatedStoreConfig, *config.ManagementConfig, error) {
	management := state.management
	if management == nil || management.WriteToken == "" || management.ProposalStore == nil || management.BackupStore == nil {
		return modelconfig.DelegatedProposal{}, "", config.ManagementTarget{}, nil, nil, modelconfig.ErrTargetWrite
	}
	stores := management.DelegatedStore
	if stores == nil {
		return modelconfig.DelegatedProposal{}, "", config.ManagementTarget{}, nil, nil, modelconfig.ErrProposalStore
	}
	if !slices.Contains(state.peer.LocalBackends, input.Backend) {
		return modelconfig.DelegatedProposal{}, "", config.ManagementTarget{}, nil, nil, errDelegationTarget
	}
	targetID, ok := state.peer.ManagementTargets[input.Backend]
	if !ok {
		return modelconfig.DelegatedProposal{}, "", config.ManagementTarget{}, nil, nil, errDelegationTarget
	}
	target, exists := management.Targets[targetID]
	if !ok || !exists {
		return modelconfig.DelegatedProposal{}, "", config.ManagementTarget{}, nil, nil, errDelegationTarget
	}
	if !r.separateDelegatedExecutionStores(state, stores.ProposalDirectory, stores.OperationDirectory) {
		return modelconfig.DelegatedProposal{}, "", config.ManagementTarget{}, nil, nil, modelconfig.ErrProposalStore
	}
	proposal, err := modelconfig.LoadDelegatedProposal(stores.ProposalDirectory, input.ProposalID)
	if err != nil {
		return modelconfig.DelegatedProposal{}, "", config.ManagementTarget{}, nil, nil, err
	}
	if proposal.RequesterNode != requester || proposal.BackendID != input.Backend || proposal.Proposal.TargetID != targetID {
		return modelconfig.DelegatedProposal{}, "", config.ManagementTarget{}, nil, nil, modelconfig.ErrDelegatedProposalBinding
	}
	return proposal, targetID, target, stores, management, nil
}

func (r *Reloader) separateDelegatedExecutionStores(state *reloaderState, proposals, operations string) bool {
	management := state.management
	if management == nil || management.ProposalStore == nil || management.BackupStore == nil ||
		!r.separateDelegatedStores(state, proposals, operations) {
		return false
	}
	localProposals, backups := management.ProposalStore.Directory, management.BackupStore.Directory
	if !separateProposalDirectory(localProposals, management.Targets) ||
		!separateProposalDirectory(backups, management.Targets) || !separateDirectories(localProposals, backups) {
		return false
	}
	var benchmark *config.BenchmarkConfig
	if r.benchmark != nil {
		benchmark = &r.benchmark.config
		if !separateProposalDirectory(benchmark.ReportDirectory, management.Targets) ||
			!separateDirectories(benchmark.ReportDirectory, localProposals) ||
			!separateDirectories(benchmark.ReportDirectory, backups) {
			return false
		}
	}
	return management.Ingestion == nil || validateIngestionDirectories(management, benchmark) == nil
}

func (r *Reloader) separateDelegatedStores(state *reloaderState, proposals, operations string) bool {
	if !r.separateDelegatedStore(proposals, state) || !r.separateDelegatedStore(operations, state) ||
		!separateDirectories(proposals, operations) {
		return false
	}
	if state.management != nil && state.management.DelegatedStore != nil && state.management.DelegatedStore.GrantDirectory != "" {
		grants := state.management.DelegatedStore.GrantDirectory
		if !r.separateDelegatedStore(grants, state) || !separateDirectories(grants, proposals) || !separateDirectories(grants, operations) {
			return false
		}
	}
	return true
}

// Only the currently authorized originating peer may inspect its operation.
// Historical records remain readable after an owner restart or target change.
func (r *Reloader) peerDelegatedOperation(req *http.Request, state *reloaderState, proposalID, operationID string) (record modelconfig.DelegatedOperation, retErr error) {
	if req == nil || req.TLS == nil || state == nil {
		return modelconfig.DelegatedOperation{}, errDelegationUnauthorized
	}
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.stopped || r.state.Load() != state {
		return modelconfig.DelegatedOperation{}, errDelegationChanged
	}
	requester, ok := authenticatedPeer(req, state, peerManageSettings)
	if !ok {
		return modelconfig.DelegatedOperation{}, errDelegationUnauthorized
	}
	auditTx, auditErr := beginPeerAudit(state, state.auditStore, requester, "settings.operation.read", "", "", "")
	if auditErr != nil {
		return modelconfig.DelegatedOperation{}, auditErr
	}
	auditTarget, auditModel, auditCorrelation := "", "", ""
	defer func() {
		if err := finishAudit(auditTx, auditOutcomeForError(retErr), auditTarget, auditModel, auditCorrelation); err != nil {
			retErr = errDelegationOutcomeUnknown
		}
	}()
	if err := req.Context().Err(); err != nil {
		return modelconfig.DelegatedOperation{}, err
	}
	if state.management == nil || state.management.DelegatedStore == nil {
		return modelconfig.DelegatedOperation{}, modelconfig.ErrProposalStore
	}
	stores := state.management.DelegatedStore
	if !r.separateDelegatedStores(state, stores.ProposalDirectory, stores.OperationDirectory) {
		return modelconfig.DelegatedOperation{}, modelconfig.ErrProposalStore
	}
	record, err := modelconfig.LoadDelegatedOperation(stores.OperationDirectory, proposalID)
	if err != nil {
		return modelconfig.DelegatedOperation{}, err
	}
	if record.Owner.NodeID != state.peer.NodeID || record.RequesterNode != requester || record.OperationID != operationID {
		return modelconfig.DelegatedOperation{}, modelconfig.ErrProposalNotFound
	}
	auditTarget, auditModel = record.TargetID, record.ModelID
	if modelconfig.ValidDelegatedOperationID(operationID) {
		auditCorrelation = operationID
	}
	if !delegatedOwnerGrantAllows(state, requester, record.BackendID, record.TargetID, record.ModelID) {
		return modelconfig.DelegatedOperation{}, errDelegationUnauthorized
	}
	if !delegatedOwnerGrantAllows(state, requester, record.BackendID, record.TargetID, record.ModelID) {
		return modelconfig.DelegatedOperation{}, errDelegationUnauthorized
	}
	return record, nil
}

// The owner's backup credential permits inspection or explicit acknowledgement.
// Possession identifies neither an individual nor a physical location and does
// not grant permission to replay an operation.
func (r *Reloader) ownerDelegatedOperation(req *http.Request, state *reloaderState, proposalID, operationID string, acknowledgeReview bool) (modelconfig.DelegatedOperation, error) {
	if req == nil || req.TLS == nil || state == nil {
		return modelconfig.DelegatedOperation{}, errDelegationUnauthorized
	}
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.stopped || r.state.Load() != state {
		return modelconfig.DelegatedOperation{}, errDelegationChanged
	}
	if state.management == nil || state.management.BackupStore == nil ||
		!managementBearerAuthorized(req, state.management.BackupStore.Token) {
		return modelconfig.DelegatedOperation{}, errDelegationUnauthorized
	}
	if err := req.Context().Err(); err != nil {
		return modelconfig.DelegatedOperation{}, err
	}
	if r.peerNodeID == "" || state.peer != nil && state.peer.NodeID != r.peerNodeID {
		return modelconfig.DelegatedOperation{}, errDelegationChanged
	}
	stores := state.management.DelegatedStore
	if stores == nil || !r.separateDelegatedStores(state, stores.ProposalDirectory, stores.OperationDirectory) {
		return modelconfig.DelegatedOperation{}, modelconfig.ErrProposalStore
	}
	if acknowledgeReview {
		return modelconfig.ReviewUnknownDelegatedOperation(stores.OperationDirectory, proposalID, operationID, r.peerNodeID, time.Now())
	}
	record, err := modelconfig.LoadDelegatedOperation(stores.OperationDirectory, proposalID)
	if err != nil {
		return modelconfig.DelegatedOperation{}, err
	}
	if record.Owner.NodeID != r.peerNodeID || record.OperationID != operationID {
		return modelconfig.DelegatedOperation{}, modelconfig.ErrProposalNotFound
	}
	return record, nil
}

// The owner's backup credential permits inspection and explicit cleanup of
// expired delegated reviews. Operation-associated proposals are never eligible;
// cleanup is not operation review, resolution, or replay authorization.
func (r *Reloader) ownerDelegatedProposals(req *http.Request, state *reloaderState, cleanupIDs []string, cleanup bool) ([]modelconfig.DelegatedProposalCleanupCandidate, []string, error) {
	if req == nil || req.TLS == nil || state == nil {
		return nil, nil, errDelegationUnauthorized
	}
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.stopped || r.state.Load() != state {
		return nil, nil, errDelegationChanged
	}
	if state.management == nil || state.management.BackupStore == nil ||
		!managementBearerAuthorized(req, state.management.BackupStore.Token) {
		return nil, nil, errDelegationUnauthorized
	}
	if err := req.Context().Err(); err != nil {
		return nil, nil, err
	}
	if r.peerNodeID == "" || state.peer != nil && state.peer.NodeID != r.peerNodeID {
		return nil, nil, errDelegationChanged
	}
	stores := state.management.DelegatedStore
	if stores == nil || !r.separateDelegatedStores(state, stores.ProposalDirectory, stores.OperationDirectory) {
		return nil, nil, modelconfig.ErrProposalStore
	}
	if !cleanup {
		candidates, err := modelconfig.ListExpiredOrphanedDelegatedProposals(stores.ProposalDirectory, stores.OperationDirectory, time.Now())
		return candidates, nil, err
	}
	removed, err := modelconfig.CleanupExpiredOrphanedDelegatedProposals(stores.ProposalDirectory, stores.OperationDirectory, cleanupIDs, time.Now())
	return nil, removed, err
}

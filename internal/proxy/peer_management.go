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
	return req != nil && req.TLS != nil && state != nil && state.management != nil &&
		r.state.Load() == state && managementBearerAuthorized(req, state.management.DelegationToken)
}

// A receiving node must separately authorize its local caller.
func (r *Reloader) createDelegatedSettingsProposal(req *http.Request, state *reloaderState, input delegatedSettingsRequest, lifetime time.Duration) (modelconfig.DelegatedProposal, error) {
	if req == nil || req.TLS == nil || state == nil {
		return modelconfig.DelegatedProposal{}, errDelegationUnauthorized
	}
	requester, ok := authenticatedPeer(req, state, peerManageSettings)
	if !ok {
		return modelconfig.DelegatedProposal{}, errDelegationUnauthorized
	}
	if r.state.Load() != state {
		return modelconfig.DelegatedProposal{}, errDelegationChanged
	}
	owner := modelconfig.DelegatedOwner{
		NodeID: state.peer.NodeID, ProxyInstanceID: r.instanceID, ConfigGeneration: state.generation,
	}
	if input.Owner != owner {
		return modelconfig.DelegatedProposal{}, errDelegationChanged
	}
	if state.management == nil || !slices.Contains(state.peer.LocalBackends, input.Backend) {
		return modelconfig.DelegatedProposal{}, errDelegationTarget
	}
	targetID, ok := state.peer.ManagementTargets[input.Backend]
	target, targetExists := state.management.Targets[targetID]
	if !ok || !targetExists {
		return modelconfig.DelegatedProposal{}, errDelegationTarget
	}
	stores := state.management.DelegatedStore
	if stores == nil || !r.separateDelegatedStores(state, stores.ProposalDirectory, stores.OperationDirectory) {
		return modelconfig.DelegatedProposal{}, modelconfig.ErrProposalStore
	}
	preview, err := modelconfig.PreviewTarget(target.Directory, target.Filename, targetID, input.Model, input.Settings)
	if err != nil {
		return modelconfig.DelegatedProposal{}, err
	}
	if preview.Revision != input.Revision {
		return modelconfig.DelegatedProposal{}, errDelegationChanged
	}
	local, err := modelconfig.NewProposal(target.Directory, target.Filename, preview, input.Settings)
	if err != nil {
		return modelconfig.DelegatedProposal{}, err
	}
	proposal, err := modelconfig.NewDelegatedProposal(owner, requester, input.Backend, local, lifetime)
	if err != nil {
		return modelconfig.DelegatedProposal{}, err
	}
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.stopped || r.state.Load() != state {
		return modelconfig.DelegatedProposal{}, errDelegationChanged
	}
	if err := req.Context().Err(); err != nil {
		return modelconfig.DelegatedProposal{}, err
	}
	if !r.separateDelegatedStores(state, stores.ProposalDirectory, stores.OperationDirectory) {
		return modelconfig.DelegatedProposal{}, modelconfig.ErrProposalStore
	}
	if err := modelconfig.SaveDelegatedProposal(stores.ProposalDirectory, proposal); err != nil {
		return modelconfig.DelegatedProposal{}, err
	}
	return proposal, nil
}

// recoverDelegatedSettingsProposal returns a unique exact match for the
// currently authenticated management peer. The shared peer credential proves
// scope possession, not an individual or the original request's delivery state.
func (r *Reloader) recoverDelegatedSettingsProposal(req *http.Request, state *reloaderState, input delegatedSettingsRequest) (modelconfig.DelegatedProposal, error) {
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
	if !ok {
		return modelconfig.DelegatedProposal{}, errDelegationTarget
	}
	if _, ok := state.management.Targets[targetID]; !ok {
		return modelconfig.DelegatedProposal{}, errDelegationTarget
	}
	stores := state.management.DelegatedStore
	if stores == nil || !r.separateDelegatedStores(state, stores.ProposalDirectory, stores.OperationDirectory) {
		return modelconfig.DelegatedProposal{}, modelconfig.ErrProposalStore
	}
	query := modelconfig.DelegatedProposalRecoveryQuery{
		Owner: owner, RequesterNode: requester, BackendID: input.Backend, TargetID: targetID,
		ModelID: input.Model, Revision: input.Revision, Settings: input.Settings,
	}
	return modelconfig.RecoverDelegatedProposal(stores.ProposalDirectory, query,
		modelconfig.DelegatedSettingsCredentialScope, time.Now())
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
func (r *Reloader) applyDelegatedSettings(req *http.Request, state *reloaderState, input delegatedSettingsApply) (modelconfig.DelegatedOperation, bool, error) {
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
	if err := req.Context().Err(); err != nil {
		return modelconfig.DelegatedOperation{}, false, err
	}
	if !input.AcknowledgeReview || !input.AcknowledgeDisruption || !input.AcknowledgeExclusiveWindow {
		return modelconfig.DelegatedOperation{}, false, errDelegationAcknowledgment
	}
	management := state.management
	if management == nil || management.WriteToken == "" || management.ProposalStore == nil || management.BackupStore == nil {
		return modelconfig.DelegatedOperation{}, false, modelconfig.ErrTargetWrite
	}
	stores := management.DelegatedStore
	if stores == nil {
		return modelconfig.DelegatedOperation{}, false, modelconfig.ErrProposalStore
	}
	if !slices.Contains(state.peer.LocalBackends, input.Backend) {
		return modelconfig.DelegatedOperation{}, false, errDelegationTarget
	}
	targetID, ok := state.peer.ManagementTargets[input.Backend]
	target, exists := management.Targets[targetID]
	if !ok || !exists {
		return modelconfig.DelegatedOperation{}, false, errDelegationTarget
	}
	if !r.separateDelegatedExecutionStores(state, stores.ProposalDirectory, stores.OperationDirectory) {
		return modelconfig.DelegatedOperation{}, false, modelconfig.ErrProposalStore
	}
	binding := modelconfig.DelegatedApplyBinding{
		Owner: modelconfig.DelegatedOwner{
			NodeID: state.peer.NodeID, ProxyInstanceID: r.instanceID, ConfigGeneration: state.generation,
		},
		RequesterNode: requester, BackendID: input.Backend, TargetID: targetID, ReviewRevision: input.ReviewRevision,
	}
	return modelconfig.ApplyStoredDelegatedProposal(stores.ProposalDirectory, stores.OperationDirectory, input.ProposalID, input.OperationID,
		target.Directory, target.Filename, management.BackupStore.Directory, management.BackupStore.MaxBackups, binding)
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
	return r.separateDelegatedStore(proposals, state) && r.separateDelegatedStore(operations, state) &&
		separateDirectories(proposals, operations)
}

// Only the currently authorized originating peer may inspect its operation.
// Historical records remain readable after an owner restart or target change.
func (r *Reloader) peerDelegatedOperation(req *http.Request, state *reloaderState, proposalID, operationID string) (modelconfig.DelegatedOperation, error) {
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

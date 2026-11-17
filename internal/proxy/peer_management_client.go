package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"reflect"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
)

var (
	errDelegationBusy            = errors.New("delegated management service busy")
	errDelegationPeerUnavailable = errors.New("delegated management peer unavailable")
	errDelegationPeerResponse    = errors.New("invalid delegated management peer response")
	errDelegationOutcomeUnknown  = errors.New("delegated apply outcome unknown; inspect the operation before further action")
	errDelegationProposalUnknown = errors.New("delegated proposal creation outcome unknown; owner-side review may require cleanup")
)

// forwardDelegatedOperation is an internal read-only transport foundation.
// The owner endpoint and receiving management route are not registered yet.
func (r *Reloader) forwardDelegatedOperation(req *http.Request, state *reloaderState, owner, proposalID, operationID string) (modelconfig.DelegatedOperation, error) {
	if req == nil || req.TLS == nil || state == nil {
		return modelconfig.DelegatedOperation{}, errDelegationUnauthorized
	}
	select {
	case r.managementSlots <- struct{}{}:
		defer func() { <-r.managementSlots }()
	default:
		return modelconfig.DelegatedOperation{}, errDelegationBusy
	}
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.stopped || r.state.Load() != state {
		return modelconfig.DelegatedOperation{}, errDelegationChanged
	}
	if !r.delegatedCallerAuthorized(req, state) {
		return modelconfig.DelegatedOperation{}, errDelegationUnauthorized
	}
	if err := req.Context().Err(); err != nil {
		return modelconfig.DelegatedOperation{}, err
	}
	if !modelconfig.ValidDelegatedOperationID(proposalID) || !modelconfig.ValidDelegatedOperationID(operationID) {
		return modelconfig.DelegatedOperation{}, modelconfig.ErrProposalInvalid
	}
	if state.peer == nil || owner == state.peer.NodeID {
		return modelconfig.DelegatedOperation{}, errDelegationPeerUnavailable
	}
	client := state.peerClients[owner]
	if client == nil || !client.routeManagement || client.id != owner {
		return modelconfig.DelegatedOperation{}, errDelegationPeerUnavailable
	}
	remote, err := http.NewRequestWithContext(req.Context(), http.MethodGet,
		client.origin+peerOperationPath+proposalID+"/"+operationID, nil)
	if err != nil {
		return modelconfig.DelegatedOperation{}, errDelegationPeerUnavailable
	}
	remote.Header.Set("Authorization", "Bearer "+client.token)
	remote.Header.Set("Accept", "application/json")
	response, err := client.client.Do(remote)
	if err != nil {
		return modelconfig.DelegatedOperation{}, errDelegationPeerUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return modelconfig.DelegatedOperation{}, errDelegationPeerUnavailable
	}
	record, err := decodeDelegatedOperationResponse(response)
	if err != nil || record.Owner.NodeID != owner || record.RequesterNode != state.peer.NodeID ||
		record.ProposalID != proposalID || record.OperationID != operationID {
		return modelconfig.DelegatedOperation{}, errDelegationPeerResponse
	}
	return record, nil
}

func decodeDelegatedOperationResponse(response *http.Response) (modelconfig.DelegatedOperation, error) {
	var record modelconfig.DelegatedOperation
	if err := decodeDelegatedPeerJSON(response, &record); err != nil || !modelconfig.ValidDelegatedOperation(record) {
		return modelconfig.DelegatedOperation{}, errDelegationPeerResponse
	}
	return record, nil
}

func decodeDelegatedPeerJSON(response *http.Response, dst any) error {
	contentType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || contentType != "application/json" || response.Header.Get("Content-Encoding") != "" {
		return errDelegationPeerResponse
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxManagementRequest+1))
	if err != nil || len(data) > maxManagementRequest || validateManagementJSON(data) != nil {
		return errDelegationPeerResponse
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return errDelegationPeerResponse
	}
	return nil
}

// Creation can persist a review even if its response is lost. No request is
// automatically retried, and the owner-generated ID may then be unavailable.
func (r *Reloader) forwardDelegatedSettingsProposal(req *http.Request, state *reloaderState, input delegatedSettingsRequest) (modelconfig.DelegatedProposal, error) {
	if req == nil || req.TLS == nil || state == nil {
		return modelconfig.DelegatedProposal{}, errDelegationUnauthorized
	}
	select {
	case r.managementSlots <- struct{}{}:
		defer func() { <-r.managementSlots }()
	default:
		return modelconfig.DelegatedProposal{}, errDelegationBusy
	}
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.stopped || r.state.Load() != state {
		return modelconfig.DelegatedProposal{}, errDelegationChanged
	}
	if !r.delegatedCallerAuthorized(req, state) {
		return modelconfig.DelegatedProposal{}, errDelegationUnauthorized
	}
	if err := req.Context().Err(); err != nil {
		return modelconfig.DelegatedProposal{}, err
	}
	if state.peer == nil || input.Owner.NodeID == state.peer.NodeID {
		return modelconfig.DelegatedProposal{}, errDelegationPeerUnavailable
	}
	client := state.peerClients[input.Owner.NodeID]
	if client == nil || !client.routeManagement || client.id != input.Owner.NodeID {
		return modelconfig.DelegatedProposal{}, errDelegationPeerUnavailable
	}
	payload, err := json.Marshal(input)
	if err != nil || len(payload) > maxBenchmarkJSONBytes {
		return modelconfig.DelegatedProposal{}, modelconfig.ErrDelegatedProposalInvalid
	}
	remote, err := http.NewRequestWithContext(req.Context(), http.MethodPost, client.origin+peerSettingsProposalPath, bytes.NewReader(payload))
	if err != nil {
		return modelconfig.DelegatedProposal{}, errDelegationPeerUnavailable
	}
	remote.Header.Set("Authorization", "Bearer "+client.token)
	remote.Header.Set("Content-Type", "application/json")
	remote.Header.Set("Accept", "application/json")
	response, err := client.client.Do(remote)
	if err != nil {
		return modelconfig.DelegatedProposal{}, errDelegationProposalUnknown
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return modelconfig.DelegatedProposal{}, errDelegationProposalUnknown
	}
	var proposal modelconfig.DelegatedProposal
	if err := decodeDelegatedPeerJSON(response, &proposal); err != nil ||
		modelconfig.ValidateDelegatedProposal(proposal, input.Owner, state.peer.NodeID, input.Backend, proposal.Proposal.TargetID, time.Now()) != nil ||
		proposal.Proposal.ModelID != input.Model || proposal.Proposal.Revision != input.Revision ||
		!reflect.DeepEqual(proposal.Proposal.Settings, input.Settings) ||
		proposal.ExpiresAt.Sub(proposal.Proposal.CreatedAt) > delegatedProposalLifetime {
		return modelconfig.DelegatedProposal{}, errDelegationProposalUnknown
	}
	return proposal, nil
}

func (r *Reloader) forwardDelegatedSettingsGrants(req *http.Request, state *reloaderState, owner string) (modelconfig.DelegatedOwner, []modelconfig.OwnerGrant, error) {
	if req == nil || req.TLS == nil || state == nil {
		return modelconfig.DelegatedOwner{}, nil, errDelegationUnauthorized
	}
	select {
	case r.managementSlots <- struct{}{}:
		defer func() { <-r.managementSlots }()
	default:
		return modelconfig.DelegatedOwner{}, nil, errDelegationBusy
	}
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.stopped || r.state.Load() != state {
		return modelconfig.DelegatedOwner{}, nil, errDelegationChanged
	}
	if !r.delegatedCallerAuthorized(req, state) {
		return modelconfig.DelegatedOwner{}, nil, errDelegationUnauthorized
	}
	if state.peer == nil || owner == "" || owner == state.peer.NodeID {
		return modelconfig.DelegatedOwner{}, nil, errDelegationPeerUnavailable
	}
	client := state.peerClients[owner]
	if client == nil || !client.routeManagement || client.id != owner {
		return modelconfig.DelegatedOwner{}, nil, errDelegationPeerUnavailable
	}
	remote, err := http.NewRequestWithContext(req.Context(), http.MethodGet, client.origin+peerSettingsGrantsPath, nil)
	if err != nil {
		return modelconfig.DelegatedOwner{}, nil, errDelegationPeerUnavailable
	}
	remote.Header.Set("Authorization", "Bearer "+client.token)
	remote.Header.Set("Accept", "application/json")
	response, err := client.client.Do(remote)
	if err != nil {
		return modelconfig.DelegatedOwner{}, nil, errDelegationPeerUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return modelconfig.DelegatedOwner{}, nil, errDelegationPeerUnavailable
	}
	var result struct {
		Owner  modelconfig.DelegatedOwner `json:"owner"`
		Grants []modelconfig.OwnerGrant   `json:"grants"`
	}
	if decodeDelegatedPeerJSON(response, &result) != nil || !validDelegatedOwnerIdentity(result.Owner, owner) || len(result.Grants) > modelconfig.MaxOwnerGrants {
		return modelconfig.DelegatedOwner{}, nil, errDelegationPeerResponse
	}
	for _, grant := range result.Grants {
		if !modelconfig.ValidOwnerGrant(grant) || grant.RequesterNodeID != state.peer.NodeID || grant.Action != "manage_settings" || !time.Now().Before(grant.ExpiresAt) {
			return modelconfig.DelegatedOwner{}, nil, errDelegationPeerResponse
		}
	}
	return result.Owner, result.Grants, nil
}

func (r *Reloader) forwardDelegatedSettingsSnapshot(req *http.Request, state *reloaderState, owner string, input delegatedSettingsTargetRequest) (modelconfig.DelegatedOwner, modelconfig.SettingsSnapshot, error) {
	if req == nil || req.TLS == nil || state == nil {
		return modelconfig.DelegatedOwner{}, modelconfig.SettingsSnapshot{}, errDelegationUnauthorized
	}
	select {
	case r.managementSlots <- struct{}{}:
		defer func() { <-r.managementSlots }()
	default:
		return modelconfig.DelegatedOwner{}, modelconfig.SettingsSnapshot{}, errDelegationBusy
	}
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.stopped || r.state.Load() != state {
		return modelconfig.DelegatedOwner{}, modelconfig.SettingsSnapshot{}, errDelegationChanged
	}
	if !r.delegatedCallerAuthorized(req, state) {
		return modelconfig.DelegatedOwner{}, modelconfig.SettingsSnapshot{}, errDelegationUnauthorized
	}
	client := state.peerClients[owner]
	if state.peer == nil || owner == "" || owner == state.peer.NodeID || client == nil || !client.routeManagement || client.id != owner {
		return modelconfig.DelegatedOwner{}, modelconfig.SettingsSnapshot{}, errDelegationPeerUnavailable
	}
	payload, err := json.Marshal(input)
	if err != nil || len(payload) > maxManagementRequest {
		return modelconfig.DelegatedOwner{}, modelconfig.SettingsSnapshot{}, modelconfig.ErrDelegatedProposalInvalid
	}
	remote, err := http.NewRequestWithContext(req.Context(), http.MethodPost, client.origin+peerSettingsSnapshotPath, bytes.NewReader(payload))
	if err != nil {
		return modelconfig.DelegatedOwner{}, modelconfig.SettingsSnapshot{}, errDelegationPeerUnavailable
	}
	remote.Header.Set("Authorization", "Bearer "+client.token)
	remote.Header.Set("Content-Type", "application/json")
	remote.Header.Set("Accept", "application/json")
	response, err := client.client.Do(remote)
	if err != nil {
		return modelconfig.DelegatedOwner{}, modelconfig.SettingsSnapshot{}, errDelegationPeerUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return modelconfig.DelegatedOwner{}, modelconfig.SettingsSnapshot{}, errDelegationPeerUnavailable
	}
	var result struct {
		Owner    modelconfig.DelegatedOwner   `json:"owner"`
		Snapshot modelconfig.SettingsSnapshot `json:"snapshot"`
	}
	if decodeDelegatedPeerJSON(response, &result) != nil || !validDelegatedOwnerIdentity(result.Owner, owner) ||
		result.Snapshot.TargetID != input.TargetID || result.Snapshot.ModelID != input.ModelID || result.Snapshot.Revision == "" ||
		result.Snapshot.Validation != "yaml_syntax_and_supported_command_only" {
		return modelconfig.DelegatedOwner{}, modelconfig.SettingsSnapshot{}, errDelegationPeerResponse
	}
	return result.Owner, result.Snapshot, nil
}

func (r *Reloader) forwardDelegatedSettingsPreview(req *http.Request, state *reloaderState, owner string, input delegatedSettingsPreviewRequest) (modelconfig.DelegatedOwner, modelconfig.Preview, error) {
	if req == nil || req.TLS == nil || state == nil {
		return modelconfig.DelegatedOwner{}, modelconfig.Preview{}, errDelegationUnauthorized
	}
	select {
	case r.managementSlots <- struct{}{}:
		defer func() { <-r.managementSlots }()
	default:
		return modelconfig.DelegatedOwner{}, modelconfig.Preview{}, errDelegationBusy
	}
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.stopped || r.state.Load() != state {
		return modelconfig.DelegatedOwner{}, modelconfig.Preview{}, errDelegationChanged
	}
	if !r.delegatedCallerAuthorized(req, state) {
		return modelconfig.DelegatedOwner{}, modelconfig.Preview{}, errDelegationUnauthorized
	}
	client := state.peerClients[owner]
	if state.peer == nil || owner == "" || owner == state.peer.NodeID || client == nil || !client.routeManagement || client.id != owner {
		return modelconfig.DelegatedOwner{}, modelconfig.Preview{}, errDelegationPeerUnavailable
	}
	payload, err := json.Marshal(input)
	if err != nil || len(payload) > maxManagementRequest {
		return modelconfig.DelegatedOwner{}, modelconfig.Preview{}, modelconfig.ErrDelegatedProposalInvalid
	}
	remote, err := http.NewRequestWithContext(req.Context(), http.MethodPost, client.origin+peerSettingsPreviewPath, bytes.NewReader(payload))
	if err != nil {
		return modelconfig.DelegatedOwner{}, modelconfig.Preview{}, errDelegationPeerUnavailable
	}
	remote.Header.Set("Authorization", "Bearer "+client.token)
	remote.Header.Set("Content-Type", "application/json")
	remote.Header.Set("Accept", "application/json")
	response, err := client.client.Do(remote)
	if err != nil {
		return modelconfig.DelegatedOwner{}, modelconfig.Preview{}, errDelegationPeerUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return modelconfig.DelegatedOwner{}, modelconfig.Preview{}, errDelegationPeerUnavailable
	}
	var result struct {
		Owner   modelconfig.DelegatedOwner `json:"owner"`
		Preview modelconfig.Preview        `json:"preview"`
	}
	if decodeDelegatedPeerJSON(response, &result) != nil || !validDelegatedOwnerIdentity(result.Owner, owner) ||
		result.Preview.TargetID != input.TargetID || result.Preview.ModelID != input.ModelID || result.Preview.Revision == "" ||
		result.Preview.Validation != "yaml_syntax_and_supported_command_only" || !validDelegatedSettingChanges(result.Preview.Changes) {
		return modelconfig.DelegatedOwner{}, modelconfig.Preview{}, errDelegationPeerResponse
	}
	return result.Owner, result.Preview, nil
}

func validDelegatedOwnerIdentity(owner modelconfig.DelegatedOwner, expectedNodeID string) bool {
	return owner.NodeID == expectedNodeID && modelconfig.ValidDelegatedOperationID(owner.ProxyInstanceID) && owner.ConfigGeneration > 0
}

func validDelegatedSettingChanges(changes []modelconfig.SettingChange) bool {
	if len(changes) > 5 {
		return false
	}
	allowed := map[string]bool{"context": true, "fit": true, "fit_context": true, "fit_target": true, "cache_ram": true}
	for _, change := range changes {
		if !allowed[change.Setting] || len(change.Before) > 256 || len(change.After) > 256 {
			return false
		}
	}
	return true
}

// Recovery is a read-only exact-match lookup; repeating it does not reserve an
// operation or authorize a write. Each successful owner lookup appends a
// bounded audit event, so transport failure is reported without auto-retry.
func (r *Reloader) forwardDelegatedSettingsProposalRecovery(req *http.Request, state *reloaderState, input delegatedSettingsRequest) (modelconfig.DelegatedProposal, error) {
	if req == nil || req.TLS == nil || state == nil {
		return modelconfig.DelegatedProposal{}, errDelegationUnauthorized
	}
	select {
	case r.managementSlots <- struct{}{}:
		defer func() { <-r.managementSlots }()
	default:
		return modelconfig.DelegatedProposal{}, errDelegationBusy
	}
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.stopped || r.state.Load() != state {
		return modelconfig.DelegatedProposal{}, errDelegationChanged
	}
	if !r.delegatedCallerAuthorized(req, state) {
		return modelconfig.DelegatedProposal{}, errDelegationUnauthorized
	}
	if err := req.Context().Err(); err != nil {
		return modelconfig.DelegatedProposal{}, err
	}
	if state.peer == nil || input.Owner.NodeID == state.peer.NodeID {
		return modelconfig.DelegatedProposal{}, errDelegationPeerUnavailable
	}
	client := state.peerClients[input.Owner.NodeID]
	if client == nil || !client.routeManagement || client.id != input.Owner.NodeID {
		return modelconfig.DelegatedProposal{}, errDelegationPeerUnavailable
	}
	payload, err := json.Marshal(input)
	if err != nil || len(payload) > maxBenchmarkJSONBytes {
		return modelconfig.DelegatedProposal{}, modelconfig.ErrDelegatedProposalInvalid
	}
	remote, err := http.NewRequestWithContext(req.Context(), http.MethodPost,
		client.origin+peerSettingsProposalRecoveryPath, bytes.NewReader(payload))
	if err != nil {
		return modelconfig.DelegatedProposal{}, errDelegationPeerUnavailable
	}
	remote.Header.Set("Authorization", "Bearer "+client.token)
	remote.Header.Set("Content-Type", "application/json")
	remote.Header.Set("Accept", "application/json")
	response, err := client.client.Do(remote)
	if err != nil {
		return modelconfig.DelegatedProposal{}, errDelegationPeerUnavailable
	}
	defer response.Body.Close()
	switch response.StatusCode {
	case http.StatusNotFound:
		return modelconfig.DelegatedProposal{}, modelconfig.ErrProposalNotFound
	case http.StatusConflict:
		return modelconfig.DelegatedProposal{}, modelconfig.ErrDelegatedProposalAmbiguous
	case http.StatusOK:
	default:
		return modelconfig.DelegatedProposal{}, errDelegationPeerUnavailable
	}
	var proposal modelconfig.DelegatedProposal
	if err := decodeDelegatedPeerJSON(response, &proposal); err != nil ||
		modelconfig.ValidateDelegatedProposal(proposal, input.Owner, state.peer.NodeID, input.Backend,
			proposal.Proposal.TargetID, proposal.Proposal.CreatedAt) != nil ||
		proposal.Proposal.ModelID != input.Model || proposal.Proposal.Revision != input.Revision ||
		!reflect.DeepEqual(proposal.Proposal.Settings, input.Settings) ||
		proposal.ExpiresAt.Sub(proposal.Proposal.CreatedAt) > delegatedProposalLifetime ||
		len(proposal.RecoveryAudit) == 0 {
		return modelconfig.DelegatedProposal{}, errDelegationPeerResponse
	}
	audit := proposal.RecoveryAudit[len(proposal.RecoveryAudit)-1]
	if audit.RequesterNode != state.peer.NodeID || audit.CredentialScope != modelconfig.DelegatedSettingsCredentialScope ||
		audit.OwnerNode != input.Owner.NodeID || audit.Outcome != "recovered" && audit.Outcome != "expired" {
		return modelconfig.DelegatedProposal{}, errDelegationPeerResponse
	}
	return proposal, nil
}

// The supplied review is not authorization. The owner reloads its stored
// proposal and rechecks every binding before any write.
func (r *Reloader) forwardDelegatedSettingsApply(req *http.Request, state *reloaderState, review modelconfig.DelegatedProposal, operationID string, acknowledgeReview, acknowledgeDisruption, acknowledgeExclusiveWindow bool) (modelconfig.DelegatedOperation, error) {
	if req == nil || req.TLS == nil || state == nil {
		return modelconfig.DelegatedOperation{}, errDelegationUnauthorized
	}
	select {
	case r.managementSlots <- struct{}{}:
		defer func() { <-r.managementSlots }()
	default:
		return modelconfig.DelegatedOperation{}, errDelegationBusy
	}
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.stopped || r.state.Load() != state {
		return modelconfig.DelegatedOperation{}, errDelegationChanged
	}
	if !r.delegatedCallerAuthorized(req, state) {
		return modelconfig.DelegatedOperation{}, errDelegationUnauthorized
	}
	if err := req.Context().Err(); err != nil {
		return modelconfig.DelegatedOperation{}, err
	}
	if !acknowledgeReview || !acknowledgeDisruption || !acknowledgeExclusiveWindow {
		return modelconfig.DelegatedOperation{}, errDelegationAcknowledgment
	}
	if state.peer == nil || review.Owner.NodeID == state.peer.NodeID {
		return modelconfig.DelegatedOperation{}, errDelegationPeerUnavailable
	}
	if !modelconfig.ValidDelegatedOperationID(operationID) {
		return modelconfig.DelegatedOperation{}, modelconfig.ErrProposalInvalid
	}
	if err := modelconfig.ValidateDelegatedProposal(review, review.Owner, state.peer.NodeID, review.BackendID, review.Proposal.TargetID, time.Now()); err != nil {
		return modelconfig.DelegatedOperation{}, err
	}
	client := state.peerClients[review.Owner.NodeID]
	if client == nil || !client.routeManagement || client.id != review.Owner.NodeID {
		return modelconfig.DelegatedOperation{}, errDelegationPeerUnavailable
	}
	payload, err := json.Marshal(delegatedSettingsApply{
		Backend: review.BackendID, ProposalID: review.Proposal.ID, OperationID: operationID,
		ReviewRevision: review.ReviewRevision, AcknowledgeReview: acknowledgeReview,
		AcknowledgeDisruption: acknowledgeDisruption, AcknowledgeExclusiveWindow: acknowledgeExclusiveWindow,
	})
	if err != nil {
		return modelconfig.DelegatedOperation{}, modelconfig.ErrProposalInvalid
	}
	remote, err := http.NewRequestWithContext(req.Context(), http.MethodPost, client.origin+peerSettingsApplyPath, bytes.NewReader(payload))
	if err != nil {
		return modelconfig.DelegatedOperation{}, errDelegationPeerUnavailable
	}
	remote.Header.Set("Authorization", "Bearer "+client.token)
	remote.Header.Set("Content-Type", "application/json")
	remote.Header.Set("Accept", "application/json")
	response, err := client.client.Do(remote)
	if err != nil {
		return modelconfig.DelegatedOperation{}, errDelegationOutcomeUnknown
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return modelconfig.DelegatedOperation{}, errDelegationOutcomeUnknown
	}
	record, err := decodeDelegatedOperationResponse(response)
	if err != nil || record.Owner != review.Owner || record.RequesterNode != state.peer.NodeID ||
		record.ProposalID != review.Proposal.ID || record.OperationID != operationID ||
		record.BackendID != review.BackendID || record.TargetID != review.Proposal.TargetID ||
		record.ModelID != review.Proposal.ModelID || record.ReviewRevision != review.ReviewRevision ||
		!record.ProposalExpiresAt.Equal(review.ExpiresAt) {
		return modelconfig.DelegatedOperation{}, errDelegationOutcomeUnknown
	}
	return record, nil
}

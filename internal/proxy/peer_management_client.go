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

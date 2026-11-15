package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
)

var (
	errDelegationBusy            = errors.New("delegated management service busy")
	errDelegationPeerUnavailable = errors.New("delegated management peer unavailable")
	errDelegationPeerResponse    = errors.New("invalid delegated management peer response")
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
	contentType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || contentType != "application/json" || response.Header.Get("Content-Encoding") != "" {
		return modelconfig.DelegatedOperation{}, errDelegationPeerResponse
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxManagementRequest+1))
	if err != nil || len(data) > maxManagementRequest || validateManagementJSON(data) != nil {
		return modelconfig.DelegatedOperation{}, errDelegationPeerResponse
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var record modelconfig.DelegatedOperation
	if err := decoder.Decode(&record); err != nil || !modelconfig.ValidDelegatedOperation(record) ||
		record.Owner.NodeID != owner || record.RequesterNode != state.peer.NodeID ||
		record.ProposalID != proposalID || record.OperationID != operationID {
		return modelconfig.DelegatedOperation{}, errDelegationPeerResponse
	}
	return record, nil
}

package modelconfig

import (
	"errors"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/identity"
)

const DelegatedOperationRetention = 7 * 24 * time.Hour

var (
	ErrDelegatedOperationConflict = errors.New("delegated operation identity or state conflicts with an existing operation")
	ErrDelegatedClockRollback     = errors.New("delegated operation clock moved backwards")
)

// DelegatedOperation does not establish effective runtime settings.
type DelegatedOperation struct {
	OperationID       string         `json:"operation_id"`
	ProposalID        string         `json:"proposal_id"`
	Owner             DelegatedOwner `json:"owner"`
	RequesterNode     string         `json:"requester_node_id"`
	CallerScope       string         `json:"caller_scope"`
	BackendID         string         `json:"backend_id"`
	TargetID          string         `json:"target_id"`
	ModelID           string         `json:"model_id"`
	ReviewRevision    string         `json:"review_revision"`
	ProposalExpiresAt time.Time      `json:"proposal_expires_at"`
	AcceptedAt        time.Time      `json:"accepted_at"`
	Outcome           string         `json:"outcome"`
	FinishedAt        *time.Time     `json:"finished_at,omitempty"`
	ReviewedAt        *time.Time     `json:"reviewed_at,omitempty"`
}

// ValidDelegatedOperationID accepts the opaque IDs used for operations and proposals.
func ValidDelegatedOperationID(id string) bool {
	return validProposalID(id)
}

// ValidDelegatedOperation checks the shape of an operation, not its origin or authorization.
func ValidDelegatedOperation(record DelegatedOperation) bool {
	return validDelegatedOperation(record)
}

func validDelegatedOperation(record DelegatedOperation) bool {
	if !(validProposalID(record.OperationID) && validProposalID(record.ProposalID) &&
		validDelegatedIdentity(record.Owner, record.RequesterNode, record.BackendID) &&
		record.CallerScope == "unknown" &&
		identity.ValidNodeID(record.TargetID) && record.TargetID != "." && record.TargetID != ".." &&
		record.ModelID != "" && identity.ValidModelComponent(record.ModelID) &&
		validDigest(record.ReviewRevision) && !record.AcceptedAt.IsZero() &&
		record.ProposalExpiresAt.After(record.AcceptedAt) &&
		record.ProposalExpiresAt.Sub(record.AcceptedAt) <= MaxDelegatedProposalLifetime) {
		return false
	}
	if record.FinishedAt != nil && (record.FinishedAt.IsZero() || record.FinishedAt.Before(record.AcceptedAt)) ||
		record.ReviewedAt != nil && (record.ReviewedAt.IsZero() || record.ReviewedAt.Before(record.AcceptedAt) ||
			record.FinishedAt != nil && record.ReviewedAt.Before(*record.FinishedAt)) {
		return false
	}
	switch record.Outcome {
	case "unknown":
		return true
	case "written", "not_written":
		return record.FinishedAt != nil && record.ReviewedAt == nil
	default:
		return false
	}
}

func operationMatchesProposal(record DelegatedOperation, proposal DelegatedProposal) bool {
	return record.ProposalID == proposal.Proposal.ID && record.Owner == proposal.Owner &&
		record.RequesterNode == proposal.RequesterNode && record.BackendID == proposal.BackendID &&
		record.TargetID == proposal.Proposal.TargetID && record.ModelID == proposal.Proposal.ModelID &&
		record.ReviewRevision == proposal.ReviewRevision && record.ProposalExpiresAt.Equal(proposal.ExpiresAt)
}

func newDelegatedOperation(proposal DelegatedProposal, operationID string, now time.Time) DelegatedOperation {
	return DelegatedOperation{
		OperationID: operationID, ProposalID: proposal.Proposal.ID,
		Owner: proposal.Owner, RequesterNode: proposal.RequesterNode, CallerScope: "unknown",
		BackendID: proposal.BackendID, TargetID: proposal.Proposal.TargetID, ModelID: proposal.Proposal.ModelID,
		ReviewRevision: proposal.ReviewRevision, ProposalExpiresAt: proposal.ExpiresAt,
		AcceptedAt: now.UTC(), Outcome: "unknown",
	}
}

package modelconfig

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/identity"
)

const MaxDelegatedProposalLifetime = 24 * time.Hour

var (
	ErrDelegatedProposalInvalid = errors.New("invalid delegated proposal")
	ErrDelegatedProposalBinding = errors.New("delegated proposal binding changed")
	ErrDelegatedProposalExpired = errors.New("delegated proposal expired or is not yet valid")
)

type DelegatedOwner struct {
	NodeID           string `json:"node_id"`
	ProxyInstanceID  string `json:"proxy_instance_id"`
	ConfigGeneration uint64 `json:"config_generation"`
}

// DelegatedProposal binds an owner-created settings proposal to the peer that
// requested it. ReviewRevision is a digest, not a signature or authorization.
type DelegatedProposal struct {
	Owner          DelegatedOwner `json:"owner"`
	RequesterNode  string         `json:"requester_node_id"`
	BackendID      string         `json:"backend_id"`
	Proposal       Proposal       `json:"proposal"`
	ExpiresAt      time.Time      `json:"expires_at"`
	ReviewRevision string         `json:"review_revision"`
}

// DelegatedApplyBinding is supplied by the owner after authenticating the peer
// and resolving its currently authorized backend and target.
type DelegatedApplyBinding struct {
	Owner          DelegatedOwner
	RequesterNode  string
	BackendID      string
	TargetID       string
	ReviewRevision string
}

// NewDelegatedProposal binds an existing owner-created proposal. The caller
// must establish peer authorization and the backend-to-target association.
func NewDelegatedProposal(owner DelegatedOwner, requesterNode, backendID string, proposal Proposal, lifetime time.Duration) (DelegatedProposal, error) {
	if !validDelegatedIdentity(owner, requesterNode, backendID) || !validProposal(proposal) ||
		lifetime <= 0 || lifetime > MaxDelegatedProposalLifetime {
		return DelegatedProposal{}, ErrDelegatedProposalInvalid
	}
	result := DelegatedProposal{
		Owner: owner, RequesterNode: requesterNode, BackendID: backendID,
		Proposal: proposal, ExpiresAt: proposal.CreatedAt.Add(lifetime),
	}
	revision, err := delegatedReviewRevision(result)
	if err != nil {
		return DelegatedProposal{}, ErrDelegatedProposalInvalid
	}
	result.ReviewRevision = revision
	return result, nil
}

// ValidateDelegatedProposal checks an owner-controlled proposal against a current
// owner incarnation and configured association. Never trust a peer-supplied copy:
// its digest can be recomputed. The caller must independently recheck permissions
// and revalidate the local proposal against the target before any write.
func ValidateDelegatedProposal(proposal DelegatedProposal, owner DelegatedOwner, requesterNode, backendID, targetID string, now time.Time) error {
	if !validDelegatedProposal(proposal) {
		return ErrDelegatedProposalInvalid
	}
	if proposal.Owner != owner || proposal.RequesterNode != requesterNode || proposal.BackendID != backendID ||
		proposal.Proposal.TargetID != targetID {
		return ErrDelegatedProposalBinding
	}
	if now.IsZero() || now.Before(proposal.Proposal.CreatedAt) || !now.Before(proposal.ExpiresAt) {
		return ErrDelegatedProposalExpired
	}
	return nil
}

func validDelegatedProposal(proposal DelegatedProposal) bool {
	if !validDelegatedIdentity(proposal.Owner, proposal.RequesterNode, proposal.BackendID) ||
		!validProposal(proposal.Proposal) || !validDigest(proposal.ReviewRevision) ||
		!proposal.ExpiresAt.After(proposal.Proposal.CreatedAt) ||
		proposal.ExpiresAt.Sub(proposal.Proposal.CreatedAt) > MaxDelegatedProposalLifetime {
		return false
	}
	revision, err := delegatedReviewRevision(proposal)
	return err == nil && revision == proposal.ReviewRevision
}

func validDelegatedIdentity(owner DelegatedOwner, requesterNode, backendID string) bool {
	instance, err := hex.DecodeString(owner.ProxyInstanceID)
	return identity.ValidNodeID(owner.NodeID) && identity.ValidNodeID(requesterNode) &&
		owner.NodeID != requesterNode && owner.ConfigGeneration > 0 &&
		err == nil && len(instance) == 16 && owner.ProxyInstanceID == strings.ToLower(owner.ProxyInstanceID) &&
		backendID != "" && identity.ValidModelComponent(backendID)
}

func delegatedReviewRevision(proposal DelegatedProposal) (string, error) {
	binding := struct {
		Owner         DelegatedOwner `json:"owner"`
		RequesterNode string         `json:"requester_node_id"`
		BackendID     string         `json:"backend_id"`
		Proposal      Proposal       `json:"proposal"`
		ExpiresAt     time.Time      `json:"expires_at"`
	}{
		Owner: proposal.Owner, RequesterNode: proposal.RequesterNode,
		BackendID: proposal.BackendID, Proposal: proposal.Proposal, ExpiresAt: proposal.ExpiresAt,
	}
	data, err := json.Marshal(binding)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

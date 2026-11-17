package modelconfig

import (
	"errors"
	"strings"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/identity"
)

const (
	MaxOwnerGrants        = 512
	MaxOwnerGrantModels   = 64
	MaxOwnerGrantLifetime = 90 * 24 * time.Hour
)

var (
	ErrOwnerGrantStore    = errors.New("owner grant store unavailable")
	ErrOwnerGrantInvalid  = errors.New("owner grant invalid")
	ErrOwnerGrantConflict = errors.New("owner grant conflicts with an existing grant")
	ErrOwnerGrantNotFound = errors.New("owner grant not found")
)

type OwnerGrant struct {
	ID              string    `json:"id"`
	RequesterNodeID string    `json:"requester_node_id"`
	Action          string    `json:"action"`
	BackendID       string    `json:"backend_id"`
	TargetID        string    `json:"target_id"`
	ModelIDs        []string  `json:"model_ids"`
	CreatedAt       time.Time `json:"created_at"`
	ExpiresAt       time.Time `json:"expires_at"`
	ApprovedBy      string    `json:"approved_by"`
}

func ValidOwnerGrant(grant OwnerGrant) bool {
	if !ValidDelegatedOperationID(grant.ID) || !identity.ValidNodeID(grant.RequesterNodeID) ||
		grant.Action != "manage_settings" || !identity.ValidNodeID(grant.BackendID) || !identity.ValidNodeID(grant.TargetID) ||
		len(grant.ModelIDs) == 0 || len(grant.ModelIDs) > MaxOwnerGrantModels || grant.CreatedAt.IsZero() ||
		!grant.ExpiresAt.After(grant.CreatedAt) || grant.ExpiresAt.Sub(grant.CreatedAt) > MaxOwnerGrantLifetime ||
		(grant.ApprovedBy != "oidc_admin" && grant.ApprovedBy != "local_cli") {
		return false
	}
	seen := make(map[string]struct{}, len(grant.ModelIDs))
	for _, modelID := range grant.ModelIDs {
		if !validOwnerGrantModelID(modelID) {
			return false
		}
		if _, duplicate := seen[modelID]; duplicate {
			return false
		}
		seen[modelID] = struct{}{}
	}
	return true
}

func validOwnerGrantModelID(modelID string) bool {
	if !identity.ValidModelComponent(modelID) || strings.TrimSpace(modelID) == "" || strings.ContainsAny(modelID, "\\\x00") {
		return false
	}
	for _, component := range strings.Split(modelID, "/") {
		if component == "" || component == "." || component == ".." {
			return false
		}
	}
	return true
}

func OwnerGrantAllows(grant OwnerGrant, requesterNodeID, action, backendID, targetID, modelID string, now time.Time) bool {
	if !ValidOwnerGrant(grant) || now.Before(grant.CreatedAt) || !now.Before(grant.ExpiresAt) || grant.RequesterNodeID != requesterNodeID ||
		grant.Action != action || grant.BackendID != backendID || grant.TargetID != targetID {
		return false
	}
	for _, allowedModel := range grant.ModelIDs {
		if allowedModel == modelID {
			return true
		}
	}
	return false
}

package auditlog

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/identity"
)

const (
	Retention       = 30 * 24 * time.Hour
	MaxEvents       = 10_000
	MaxBytes        = 16 << 20
	MaxEventBytes   = 4096
	MaxListEvents   = 200
	MaxDayFileBytes = 2 << 20
)

var (
	ErrUnavailable = errors.New("audit store unavailable")
	ErrCapacity    = errors.New("audit store capacity reached")
	ErrInvalid     = errors.New("audit event invalid")
)

type Event struct {
	ID                  string    `json:"id"`
	At                  time.Time `json:"at"`
	LocalNodeID         string    `json:"local_node_id"`
	RequesterNodeID     string    `json:"requester_node_id,omitempty"`
	PrincipalType       string    `json:"principal_type"`
	UserIdentityState   string    `json:"user_identity_state"`
	IdentityIssuer      string    `json:"identity_issuer,omitempty"`
	VerifiedUserSubject string    `json:"verified_user_subject,omitempty"`
	AssertedUserSubject string    `json:"asserted_user_subject,omitempty"`
	LocalAccount        string    `json:"local_account,omitempty"`
	Role                string    `json:"role,omitempty"`
	Action              string    `json:"action"`
	TargetID            string    `json:"target_id,omitempty"`
	ModelID             string    `json:"model_id,omitempty"`
	CorrelationID       string    `json:"correlation_id,omitempty"`
	Phase               string    `json:"phase"`
	Outcome             string    `json:"outcome"`
}

type Store struct {
	mu        sync.Mutex
	directory string
	localID   string
	root      *os.Root
	lock      *os.File
	closed    bool
	failed    bool
}

type Transaction struct {
	mu    sync.Mutex
	store *Store
	event Event
	done  bool
}

func NewEventID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", ErrUnavailable
	}
	return hex.EncodeToString(value), nil
}

func ValidateEvent(event Event) error {
	if !identity.ValidNodeID(event.LocalNodeID) || !validAuditID(event.ID) || event.At.IsZero() ||
		!validAuditAction(event.Action) || !validAuditOutcome(event.Outcome) || event.AssertedUserSubject != "" ||
		len(event.VerifiedUserSubject) > 256 || len(event.IdentityIssuer) > 256 || len(event.LocalAccount) > 128 ||
		len(event.Role) > 64 || len(event.TargetID) > 128 || len(event.ModelID) > 256 || len(event.CorrelationID) > 64 {
		return ErrInvalid
	}
	if event.RequesterNodeID != "" && !identity.ValidNodeID(event.RequesterNodeID) {
		return ErrInvalid
	}
	if event.TargetID != "" && !identity.ValidNodeID(event.TargetID) {
		return ErrInvalid
	}
	if event.ModelID != "" && !identity.ValidModelComponent(event.ModelID) {
		return ErrInvalid
	}
	if event.CorrelationID != "" && !validAuditID(event.CorrelationID) {
		return ErrInvalid
	}
	switch event.PrincipalType {
	case "oidc":
		if event.UserIdentityState != "verified_local" || event.VerifiedUserSubject == "" || event.IdentityIssuer == "" || event.LocalAccount != "" {
			return ErrInvalid
		}
	case "local_cli":
		if event.UserIdentityState != "local_process" || event.LocalAccount == "" || event.VerifiedUserSubject != "" || event.IdentityIssuer != "" {
			return ErrInvalid
		}
	case "delegation_token", "machine_token", "peer_node", "unknown":
		if event.UserIdentityState != "unknown" || event.VerifiedUserSubject != "" || event.IdentityIssuer != "" || event.LocalAccount != "" {
			return ErrInvalid
		}
		if event.PrincipalType == "peer_node" && event.RequesterNodeID == "" {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	if event.Phase != "started" && event.Phase != "completed" && event.Phase != "event" {
		return ErrInvalid
	}
	if event.Phase == "started" && event.Outcome != "started" {
		return ErrInvalid
	}
	if event.Phase != "started" && event.Outcome == "started" {
		return ErrInvalid
	}
	return nil
}

func validAuditID(value string) bool {
	if len(value) != 32 {
		return false
	}
	for _, char := range value {
		if !(char >= '0' && char <= '9' || char >= 'a' && char <= 'f') {
			return false
		}
	}
	return true
}

func validAuditAction(value string) bool {
	if len(value) == 0 || len(value) > 64 {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || strings.ContainsRune("._-", char)) {
			return false
		}
	}
	return true
}

func validAuditOutcome(value string) bool {
	switch value {
	case "started", "succeeded", "denied", "failed", "unknown":
		return true
	default:
		return false
	}
}

func (tx *Transaction) Finish(outcome string) error {
	return tx.FinishWith(outcome, "", "", "")
}

func (tx *Transaction) FinishWith(outcome, targetID, modelID, correlationID string) error {
	if tx == nil || tx.store == nil || !validAuditOutcome(outcome) || outcome == "started" {
		return ErrInvalid
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return ErrInvalid
	}
	event := tx.event
	event.At = time.Now().UTC()
	event.Phase = "completed"
	event.Outcome = outcome
	if (targetID != "" && tx.event.TargetID != "" && targetID != tx.event.TargetID) ||
		(modelID != "" && tx.event.ModelID != "" && modelID != tx.event.ModelID) ||
		(correlationID != "" && tx.event.CorrelationID != "" && correlationID != tx.event.CorrelationID) {
		return ErrInvalid
	}
	if targetID != "" {
		event.TargetID = targetID
	}
	if modelID != "" {
		event.ModelID = modelID
	}
	if correlationID != "" {
		event.CorrelationID = correlationID
	}
	if err := tx.store.finish(event); err != nil {
		return err
	}
	tx.done = true
	return nil
}

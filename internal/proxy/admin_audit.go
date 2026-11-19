package proxy

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/auditlog"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/federation"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
)

const adminAuditPath = "/admin/api/v1/audit/events"

type adminAuditPrincipalKey struct{}

type adminAuditPrincipal struct {
	subject string
	role    string
}

func withAdminAuditPrincipal(req *http.Request, subject, role string) *http.Request {
	return req.WithContext(context.WithValue(req.Context(), adminAuditPrincipalKey{}, adminAuditPrincipal{subject: subject, role: role}))
}

func (r *Reloader) serveAdminAuditEvents(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if req.Method != http.MethodGet || req.URL.RawPath != "" || req.URL.ForceQuery {
		http.Error(w, "invalid audit request", http.StatusBadRequest)
		return
	}
	if _, ok := r.adminAuth.RequireRole(req, "administer_federation"); !ok {
		r.auditDeniedAdminRole(req)
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	limit := 100
	query := req.URL.Query()
	if len(query) > 1 || len(query["limit"]) > 1 {
		http.Error(w, "invalid audit query", http.StatusBadRequest)
		return
	}
	if raw := query.Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > auditlog.MaxListEvents {
			http.Error(w, "invalid audit limit", http.StatusBadRequest)
			return
		}
		limit = parsed
	}
	if r.auditStore == nil {
		http.Error(w, "audit store unavailable", http.StatusServiceUnavailable)
		return
	}
	events, err := r.auditStore.ListRecent(limit)
	if err != nil {
		http.Error(w, "audit store unavailable", http.StatusServiceUnavailable)
		return
	}
	writeJSON(r.logger, w, http.StatusOK, map[string]any{"state": "available", "retention_days": int(auditlog.Retention / (24 * time.Hour)), "events": events})
}

func (r *Reloader) beginAdminAudit(req *http.Request, action, targetID, modelID, correlationID string) (*auditlog.Transaction, error) {
	if req == nil || r.auditStore == nil || r.adminAuth == nil {
		return nil, auditlog.ErrUnavailable
	}
	principal, ok := req.Context().Value(adminAuditPrincipalKey{}).(adminAuditPrincipal)
	if !ok || principal.subject == "" || principal.role == "" {
		return nil, auditlog.ErrUnavailable
	}
	state := r.state.Load()
	if state == nil || state.peer == nil {
		return nil, auditlog.ErrUnavailable
	}
	return r.auditStore.Begin(auditlog.Event{
		LocalNodeID: state.peer.NodeID, PrincipalType: "oidc", UserIdentityState: "verified_local",
		IdentityIssuer: r.adminAuth.config.IssuerURL, VerifiedUserSubject: principal.subject, Role: principal.role,
		Action: action, TargetID: targetID, ModelID: modelID, CorrelationID: correlationID,
	})
}

func (r *Reloader) auditDeniedAdminRole(req *http.Request) {
	if req == nil || r.auditStore == nil || r.adminAuth == nil {
		return
	}
	subject, roles, ok := r.adminAuth.AuthenticatedPrincipal(req)
	state := r.state.Load()
	if !ok || subject == "" || state == nil || state.peer == nil {
		return
	}
	role := "none"
	if len(roles) > 0 {
		role = strings.Join(roles, ",")
	}
	tx, err := r.auditStore.Begin(auditlog.Event{
		LocalNodeID: state.peer.NodeID, PrincipalType: "oidc", UserIdentityState: "verified_local",
		IdentityIssuer: r.adminAuth.config.IssuerURL, VerifiedUserSubject: subject, Role: role,
		Action: "admin.authorization.denied", Phase: "started", Outcome: "started",
	})
	if err != nil {
		return
	}
	if err := tx.Finish("denied"); err != nil {
		r.logger.Error("audit authorization denial completion failed", "error", err)
	}
}

func beginPeerAudit(state *reloaderState, auditStore *auditlog.Store, requesterNodeID, action, targetID, modelID, correlationID string) (*auditlog.Transaction, error) {
	if state == nil || state.peer == nil || requesterNodeID == "" {
		return nil, auditlog.ErrUnavailable
	}
	if auditStore == nil && state.peer.FederationID == "" {
		return nil, nil
	}
	if auditStore == nil {
		return nil, auditlog.ErrUnavailable
	}
	return auditStore.Begin(auditlog.Event{
		LocalNodeID: state.peer.NodeID, RequesterNodeID: requesterNodeID, PrincipalType: "peer_node",
		UserIdentityState: "unknown", Action: action, TargetID: targetID, ModelID: modelID, CorrelationID: correlationID,
	})
}

func finishAudit(tx *auditlog.Transaction, outcome, targetID, modelID, correlationID string) error {
	if tx == nil {
		return nil
	}
	return tx.FinishWith(outcome, targetID, modelID, correlationID)
}

func auditOutcomeForError(err error) string {
	if err == nil {
		return "succeeded"
	}
	if errors.Is(err, errDelegationUnauthorized) {
		return "denied"
	}
	if errors.Is(err, auditlog.ErrUnavailable) || errors.Is(err, auditlog.ErrCapacity) ||
		errors.Is(err, federation.ErrPairingStore) || errors.Is(err, federation.ErrInvitation) || errors.Is(err, federation.ErrCatalogGrantStore) ||
		errors.Is(err, modelconfig.ErrOwnerGrantStore) || errors.Is(err, errDelegationPeerUnavailable) ||
		errors.Is(err, errDelegationPeerResponse) || errors.Is(err, errDelegationProposalUnknown) ||
		errors.Is(err, errDelegationOutcomeUnknown) {
		return "unknown"
	}
	return "failed"
}

func (r *Reloader) finishAdminMutation(w http.ResponseWriter, tx *auditlog.Transaction, operationErr error, targetID, modelID, correlationID string) bool {
	if err := finishAudit(tx, auditOutcomeForError(operationErr), targetID, modelID, correlationID); err != nil {
		http.Error(w, "audit completion unavailable; outcome unknown", http.StatusServiceUnavailable)
		return false
	}
	return true
}

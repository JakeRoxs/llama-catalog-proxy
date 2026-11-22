package proxy

import (
	"bufio"
	"bytes"
	"crypto/subtle"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"strings"
	"unicode"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/auditlog"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/identity"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
)

const maxManagementRequest = 16 << 10

const (
	invalidRequestMsg = "invalid request"
	previewPath       = "/management/v1/preview"
)

func (r *Reloader) ManagementTLSConfig() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			certificate := r.state.Load().managementCertificate
			if certificate == nil {
				return nil, errors.New("management listener disabled")
			}
			return certificate, nil
		},
	}
}

func (r *Reloader) ManagementHandler() http.Handler {
	return http.HandlerFunc(r.serveManagementAudited)
}

type auditResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *auditResponseWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *auditResponseWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(data)
}

func (w *auditResponseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *auditResponseWriter) Flush() {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}

func (w *auditResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return http.NewResponseController(w.ResponseWriter).Hijack()
}

func (w *auditResponseWriter) Push(target string, options *http.PushOptions) error {
	pusher, ok := w.ResponseWriter.(http.Pusher)
	if !ok {
		return http.ErrNotSupported
	}
	return pusher.Push(target, options)
}

func (r *Reloader) serveManagementAudited(w http.ResponseWriter, req *http.Request) {
	state := r.state.Load()
	tx, err := r.beginManagementAudit(req, state)
	if err != nil {
		http.Error(w, "audit store unavailable", http.StatusServiceUnavailable)
		return
	}
	tracked := &auditResponseWriter{ResponseWriter: w}
	r.serveManagement(tracked, req)
	r.finishManagementAudit(tx, tracked)
}

// beginManagementAudit returns the audit transaction for the request. A non-nil
// error means the caller must fail closed with 503 "audit store unavailable".
// It returns (nil, nil) when auditing is not applicable (no state, no store,
// or a non-federated peer).
func (r *Reloader) beginManagementAudit(req *http.Request, state *reloaderState) (*auditlog.Transaction, error) {
	if state == nil || state.auditStore == nil {
		if state != nil && state.peer != nil && state.peer.FederationID != "" {
			return nil, errors.New("audit store unavailable")
		}
		return nil, nil
	}
	tx, err := state.auditStore.Begin(r.managementAuditEvent(req, state))
	if err != nil {
		return nil, err
	}
	return tx, nil
}

// managementAuditOutcome maps an HTTP response status to an audit outcome.
func managementAuditOutcome(status int) string {
	switch {
	case status >= 200 && status < 300:
		return "succeeded"
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return "denied"
	case status < 500:
		return "failed"
	}
	return "unknown"
}

// finishManagementAudit completes the transaction exactly once after the handler,
// mapping the tracked response status to an audit outcome and logging any finish
// failure using the same fields as before.
func (r *Reloader) finishManagementAudit(tx *auditlog.Transaction, tracked *auditResponseWriter) {
	if tx == nil {
		return
	}
	if err := tx.Finish(managementAuditOutcome(tracked.status)); err != nil {
		r.logger.Error("management audit completion failed", "outcome", "unknown", "error", err)
	}
}

func (r *Reloader) managementAuditEvent(req *http.Request, state *reloaderState) auditlog.Event {
	event := auditlog.Event{
		LocalNodeID:       r.peerNodeID,
		PrincipalType:     "unknown",
		UserIdentityState: "unknown",
		Action:            managementAuditAction(req),
	}
	if state.peer != nil {
		event.LocalNodeID = state.peer.NodeID
	}
	if req == nil || state.management == nil {
		return event
	}
	event = r.classifyManagementAuditPrincipal(req, state.management, event)
	event = extractManagementAuditQueryIDs(req, event)
	return event
}

// classifyManagementAuditPrincipal resolves the audit principal using the priority:
// delegation token > machine principal > verified OIDC configure_models.
func (r *Reloader) classifyManagementAuditPrincipal(req *http.Request, management *config.ManagementConfig, event auditlog.Event) auditlog.Event {
	if token := management.DelegationToken; managementBearerAuthorized(req, token) {
		event.PrincipalType = "delegation_token"
		return event
	}
	if managementMachinePrincipal(req, management) {
		event.PrincipalType = "machine_token"
		return event
	}
	if r.adminAuth == nil {
		return event
	}
	if subject, ok := r.adminAuth.RequireRole(req, "configure_models"); ok {
		event.PrincipalType = "oidc"
		event.UserIdentityState = "verified_local"
		event.IdentityIssuer = r.adminAuth.config.IssuerURL
		event.VerifiedUserSubject = subject
		event.Role = "configure_models"
	}
	return event
}

// extractManagementAuditQueryIDs safely extracts single-valued query IDs for target, model, and operation.
func extractManagementAuditQueryIDs(req *http.Request, event auditlog.Event) auditlog.Event {
	if len(req.URL.Query()["target_id"]) == 1 {
		if candidate := req.URL.Query().Get("target_id"); identity.ValidNodeID(candidate) {
			event.TargetID = candidate
		}
	}
	if len(req.URL.Query()["model_id"]) == 1 {
		if candidate := req.URL.Query().Get("model_id"); identity.ValidModelComponent(candidate) {
			event.ModelID = candidate
		}
	}
	if len(req.URL.Query()["operation_id"]) == 1 {
		if candidate := req.URL.Query().Get("operation_id"); modelconfig.ValidDelegatedOperationID(candidate) {
			event.CorrelationID = candidate
		}
	}
	return event
}

func managementMachinePrincipal(req *http.Request, management *config.ManagementConfig) bool {
	if req == nil || management == nil {
		return false
	}
	if managementBearerAuthorized(req, management.PreviewToken) || managementBearerAuthorized(req, management.WriteToken) {
		return true
	}
	if management.BackupStore != nil && managementBearerAuthorized(req, management.BackupStore.Token) {
		return true
	}
	return management.ProposalStore != nil && managementBearerAuthorized(req, management.ProposalStore.Token)
}

func managementAuditAction(req *http.Request) string {
	if req == nil {
		return "management.unknown"
	}
	path := req.URL.Path
	switch {
	case strings.Contains(path, "/preview"):
		return "management.preview"
	case strings.Contains(path, "/proposal"):
		return "management.proposal"
	case strings.Contains(path, "/operation"):
		return "management.operation"
	case strings.Contains(path, "/backup"):
		return "management.backup"
	case strings.Contains(path, "/ingestion"):
		return "management.ingestion"
	case strings.Contains(path, "/apply"):
		return "management.apply"
	default:
		return "management.request"
	}
}

// managementRoute classifies the exact management sub-resource a request targets,
// preserving the precedence used by serveManagement: delegated routes first, then
// write, proposal, backup, ingestion, creation preview, preview, settings, and
// finally unknown.
type managementRoute int

const (
	routeUnknown managementRoute = iota
	routeDelegated
	routeWrite
	routeProposal
	routeBackup
	routeIngestion
	routeCreationPreview
	routePreview
	routeSettings
)

// classifyManagementRoute returns the route matched by req with exact precedence.
// Delegated routes take priority over all other management routes.
func classifyManagementRoute(req *http.Request) managementRoute {
	if strings.HasPrefix(req.URL.Path, receivedSettingsPathPrefix) ||
		req.URL.Path == receivedOperationPath[:len(receivedOperationPath)-1] ||
		strings.HasPrefix(req.URL.Path, receivedOperationPath) ||
		req.URL.Path == ownerProposalsPath ||
		strings.HasPrefix(req.URL.Path, ownerOperationPath) {
		return routeDelegated
	}
	if isWriteRoute(req) {
		return routeWrite
	}
	if isProposalRoute(req) {
		return routeProposal
	}
	if isBackupRoute(req) {
		return routeBackup
	}
	if isIngestionRoute(req) {
		return routeIngestion
	}
	if req.URL.Path == "/management/v1/creation/preview" {
		return routeCreationPreview
	}
	if req.URL.Path == previewPath {
		return routePreview
	}
	if req.URL.Path == "/management/v1/settings" {
		return routeSettings
	}
	return routeUnknown
}

func isWriteRoute(req *http.Request) bool {
	return (strings.HasPrefix(req.URL.Path, "/management/v1/proposals/") && strings.HasSuffix(req.URL.Path, "/apply")) ||
		(strings.HasPrefix(req.URL.Path, "/management/v1/backups/") && strings.HasSuffix(req.URL.Path, "/rollback"))
}

func isProposalRoute(req *http.Request) bool {
	return req.URL.Path == "/management/v1/proposals" || strings.HasPrefix(req.URL.Path, "/management/v1/proposals/")
}

func isBackupRoute(req *http.Request) bool {
	return req.URL.Path == "/management/v1/backups" || strings.HasPrefix(req.URL.Path, "/management/v1/backups/")
}

func isIngestionRoute(req *http.Request) bool {
	return req.URL.Path == "/management/v1/ingestion" || strings.HasPrefix(req.URL.Path, "/management/v1/ingestion/")
}

// selectManagementToken returns the bearer token required for the classified route,
// following the precedence: write token > proposal/backup/ingestion store token >
// preview token. It returns an empty string when management is nil or no store token
// applies, which causes the caller's authorization check to fail closed.
func selectManagementToken(management *config.ManagementConfig, route managementRoute) string {
	if management == nil {
		return ""
	}
	switch route {
	case routeWrite:
		return management.WriteToken
	case routeProposal:
		if management.ProposalStore != nil {
			return management.ProposalStore.Token
		}
		return ""
	case routeBackup:
		if management.BackupStore != nil {
			return management.BackupStore.Token
		}
		return ""
	case routeIngestion:
		if management.Ingestion != nil {
			return management.Ingestion.Token
		}
		return ""
	}
	return management.PreviewToken
}

// serveCreationPreview acquires the management slot and serves the creation preview.
// It returns true when the request was handled, false when the slot was unavailable
// (and a 503 was already written).
func (r *Reloader) serveCreationPreview(w http.ResponseWriter, req *http.Request, state *reloaderState) bool {
	select {
	case r.managementSlots <- struct{}{}:
		defer func() { <-r.managementSlots }()
	default:
		http.Error(w, "management service busy", http.StatusServiceUnavailable)
		return false
	}
	r.serveCreation(w, req, state, false)
	return true
}

// managementTargetInput is the decoded target/model selection for preview and settings
// requests. It is intentionally distinct from the preview payload, which adds a
// settings object.
type managementTargetInput struct {
	TargetID string `json:"target_id"`
	ModelID  string `json:"model_id"`
}

// servePreviewOrSettings handles the /management/v1/preview and /management/v1/settings
// endpoints: method, content-type, slot, body, and JSON validation guards, then body
// decode, target lookup, and the preview/settings operation. It returns true when the
// request was handled, false when a guard already wrote a response.
func (r *Reloader) servePreviewOrSettings(w http.ResponseWriter, req *http.Request, state *reloaderState, management *config.ManagementConfig) bool {
	if (req.URL.Path != previewPath && req.URL.Path != "/management/v1/settings") || req.URL.RawQuery != "" {
		http.NotFound(w, req)
		return false
	}
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return false
	}
	contentType, _, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
	if err != nil || contentType != "application/json" || req.Header.Get("Content-Encoding") != "" {
		http.Error(w, "JSON request required", http.StatusUnsupportedMediaType)
		return false
	}
	select {
	case r.managementSlots <- struct{}{}:
		defer func() { <-r.managementSlots }()
	default:
		http.Error(w, "management service busy", http.StatusServiceUnavailable)
		return false
	}
	if req.Body == nil {
		http.Error(w, "request body required", http.StatusBadRequest)
		return false
	}
	data, err := io.ReadAll(io.LimitReader(req.Body, maxManagementRequest+1))
	if err != nil {
		http.Error(w, "request could not be read", http.StatusBadRequest)
		return false
	}
	if len(data) > maxManagementRequest {
		http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		return false
	}
	if err := validateManagementJSON(data); err != nil {
		http.Error(w, invalidRequestMsg, http.StatusBadRequest)
		return false
	}
	return r.decodeAndExecutePreviewOrSettings(w, req, state, management, data)
}

// decodeAndExecutePreviewOrSettings decodes the preview/settings body, resolves the
// target, runs the preview or settings operation, rechecks the snapshot, and writes
// the JSON result. It returns true when the request was handled, false when a response
// was already written.
func (r *Reloader) decodeAndExecutePreviewOrSettings(w http.ResponseWriter, req *http.Request, state *reloaderState, management *config.ManagementConfig, data []byte) bool {
	var input managementTargetInput
	var settings modelconfig.CommandSettings
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if req.URL.Path == previewPath {
		var previewInput struct {
			managementTargetInput
			Settings modelconfig.CommandSettings `json:"settings"`
		}
		if err := decoder.Decode(&previewInput); err != nil {
			http.Error(w, invalidRequestMsg, http.StatusBadRequest)
			return false
		}
		input, settings = previewInput.managementTargetInput, previewInput.Settings
	} else {
		if err := decoder.Decode(&input); err != nil {
			http.Error(w, invalidRequestMsg, http.StatusBadRequest)
			return false
		}
	}
	target, ok := management.Targets[input.TargetID]
	if !ok {
		http.NotFound(w, req)
		return false
	}
	var result any
	var err error
	if req.URL.Path == previewPath {
		result, err = modelconfig.PreviewTarget(target.Directory, target.Filename, input.TargetID, input.ModelID, settings)
	} else {
		result, err = modelconfig.ReadSettingsTarget(target.Directory, target.Filename, input.TargetID, input.ModelID)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return false
	}
	if r.state.Load() != state {
		http.Error(w, "management configuration changed", http.StatusServiceUnavailable)
		return false
	}
	writeJSON(r.logger, w, http.StatusOK, result)
	return true
}

func (r *Reloader) serveManagement(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if req.TLS == nil {
		http.Error(w, "TLS required", http.StatusForbidden)
		return
	}
	state := r.state.Load()
	route := classifyManagementRoute(req)
	if route == routeDelegated {
		r.serveDelegatedManagement(w, req, state)
		return
	}
	management := state.management
	token := selectManagementToken(management, route)
	if management == nil || !managementBearerAuthorized(req, token) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	switch route {
	case routeWrite:
		r.serveMutation(w, req, state)
		return
	case routeIngestion:
		r.serveIngestion(w, req, state)
		return
	case routeProposal:
		r.serveProposals(w, req, state)
		return
	case routeBackup:
		r.serveBackups(w, req, state)
		return
	case routeCreationPreview:
		if !r.serveCreationPreview(w, req, state) {
			return
		}
		return
	case routePreview, routeSettings:
		if !r.servePreviewOrSettings(w, req, state, management) {
			return
		}
		return
	default:
		http.NotFound(w, req)
		return
	}
}

func managementBearerAuthorized(req *http.Request, token string) bool {
	if req == nil || token == "" {
		return false
	}
	auth := req.Header.Values("Authorization")
	return len(auth) == 1 && strings.HasPrefix(auth[0], "Bearer ") &&
		subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(auth[0], "Bearer ")), []byte(token)) == 1
}

func validateManagementJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := readManagementJSONValue(decoder, 0); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}

func readManagementJSONValue(decoder *json.Decoder, depth int) error {
	if depth > 8 {
		return errors.New("JSON nesting exceeds limit")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	switch token {
	case json.Delim('{'):
		keys := make(map[string]bool)
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok {
				return errors.New("invalid object key")
			}
			folded := strings.Map(func(r rune) rune {
				lowestFolded := r
				for next := unicode.SimpleFold(r); next != r; next = unicode.SimpleFold(next) {
					if next < lowestFolded {
						lowestFolded = next
					}
				}
				return lowestFolded
			}, name)
			if keys[folded] {
				return errors.New("duplicate object key")
			}
			keys[folded] = true
			if err := readManagementJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	case json.Delim('['):
		for decoder.More() {
			if err := readManagementJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	}
	return nil
}

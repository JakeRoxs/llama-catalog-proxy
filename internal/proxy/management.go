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
	var tx *auditlog.Transaction
	if state != nil && state.auditStore != nil {
		var err error
		tx, err = state.auditStore.Begin(r.managementAuditEvent(req, state))
		if err != nil {
			http.Error(w, "audit store unavailable", http.StatusServiceUnavailable)
			return
		}
	} else if state != nil && state.peer != nil && state.peer.FederationID != "" {
		http.Error(w, "audit store unavailable", http.StatusServiceUnavailable)
		return
	}
	tracked := &auditResponseWriter{ResponseWriter: w}
	r.serveManagement(tracked, req)
	if tx != nil {
		outcome := "unknown"
		if tracked.status >= 200 && tracked.status < 300 {
			outcome = "succeeded"
		} else if tracked.status == http.StatusUnauthorized || tracked.status == http.StatusForbidden {
			outcome = "denied"
		} else if tracked.status < 500 {
			outcome = "failed"
		}
		if err := tx.Finish(outcome); err != nil {
			r.logger.Error("management audit completion failed", "outcome", "unknown", "error", err)
		}
	}
}

func (r *Reloader) managementAuditEvent(req *http.Request, state *reloaderState) auditlog.Event {
	event := auditlog.Event{LocalNodeID: r.peerNodeID, PrincipalType: "unknown", UserIdentityState: "unknown", Action: managementAuditAction(req)}
	if state.peer != nil {
		event.LocalNodeID = state.peer.NodeID
	}
	if req == nil || state.management == nil {
		return event
	}
	if token := state.management.DelegationToken; managementBearerAuthorized(req, token) {
		event.PrincipalType = "delegation_token"
	} else if managementMachinePrincipal(req, state.management) {
		event.PrincipalType = "machine_token"
	} else if r.adminAuth != nil {
		if subject, ok := r.adminAuth.RequireRole(req, "configure_models"); ok {
			event.PrincipalType = "oidc"
			event.UserIdentityState = "verified_local"
			event.IdentityIssuer = r.adminAuth.config.IssuerURL
			event.VerifiedUserSubject = subject
			event.Role = "configure_models"
		}
	}
	if len(req.URL.Query()["target_id"]) == 1 {
		candidate := req.URL.Query().Get("target_id")
		if identity.ValidNodeID(candidate) {
			event.TargetID = candidate
		}
	}
	if len(req.URL.Query()["model_id"]) == 1 {
		candidate := req.URL.Query().Get("model_id")
		if identity.ValidModelComponent(candidate) {
			event.ModelID = candidate
		}
	}
	if len(req.URL.Query()["operation_id"]) == 1 {
		candidate := req.URL.Query().Get("operation_id")
		if modelconfig.ValidDelegatedOperationID(candidate) {
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

func (r *Reloader) serveManagement(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if req.TLS == nil {
		http.Error(w, "TLS required", http.StatusForbidden)
		return
	}
	state := r.state.Load()
	switch {
	case strings.HasPrefix(req.URL.Path, receivedSettingsPathPrefix),
		req.URL.Path == receivedOperationPath[:len(receivedOperationPath)-1],
		strings.HasPrefix(req.URL.Path, receivedOperationPath),
		req.URL.Path == ownerProposalsPath,
		strings.HasPrefix(req.URL.Path, ownerOperationPath):
		r.serveDelegatedManagement(w, req, state)
		return
	}
	management := state.management
	isProposal := req.URL.Path == "/management/v1/proposals" || strings.HasPrefix(req.URL.Path, "/management/v1/proposals/")
	isBackup := req.URL.Path == "/management/v1/backups" || strings.HasPrefix(req.URL.Path, "/management/v1/backups/")
	isIngestion := req.URL.Path == "/management/v1/ingestion" || strings.HasPrefix(req.URL.Path, "/management/v1/ingestion/")
	isWrite := strings.HasPrefix(req.URL.Path, "/management/v1/proposals/") && strings.HasSuffix(req.URL.Path, "/apply") ||
		strings.HasPrefix(req.URL.Path, "/management/v1/backups/") && strings.HasSuffix(req.URL.Path, "/rollback")
	token := ""
	if management != nil {
		if isWrite {
			token = management.WriteToken
		} else if isProposal && management.ProposalStore != nil {
			token = management.ProposalStore.Token
		} else if isBackup && management.BackupStore != nil {
			token = management.BackupStore.Token
		} else if isIngestion && management.Ingestion != nil {
			token = management.Ingestion.Token
		} else if !isProposal && !isBackup && !isIngestion {
			token = management.PreviewToken
		}
	}
	if management == nil || !managementBearerAuthorized(req, token) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if isWrite {
		r.serveMutation(w, req, state)
		return
	}
	if isIngestion {
		r.serveIngestion(w, req, state)
		return
	}
	if isProposal {
		r.serveProposals(w, req, state)
		return
	}
	if isBackup {
		r.serveBackups(w, req, state)
		return
	}
	if req.URL.Path == "/management/v1/creation/preview" {
		select {
		case r.managementSlots <- struct{}{}:
			defer func() { <-r.managementSlots }()
		default:
			http.Error(w, "management service busy", http.StatusServiceUnavailable)
			return
		}
		r.serveCreation(w, req, state, false)
		return
	}
	if (req.URL.Path != "/management/v1/preview" && req.URL.Path != "/management/v1/settings") || req.URL.RawQuery != "" {
		http.NotFound(w, req)
		return
	}
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	contentType, _, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
	if err != nil || contentType != "application/json" || req.Header.Get("Content-Encoding") != "" {
		http.Error(w, "JSON request required", http.StatusUnsupportedMediaType)
		return
	}
	select {
	case r.managementSlots <- struct{}{}:
		defer func() { <-r.managementSlots }()
	default:
		http.Error(w, "management service busy", http.StatusServiceUnavailable)
		return
	}
	if req.Body == nil {
		http.Error(w, "request body required", http.StatusBadRequest)
		return
	}
	data, err := io.ReadAll(io.LimitReader(req.Body, maxManagementRequest+1))
	if err != nil {
		http.Error(w, "request could not be read", http.StatusBadRequest)
		return
	}
	if len(data) > maxManagementRequest {
		http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		return
	}
	if err := validateManagementJSON(data); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	type targetInput struct {
		TargetID string `json:"target_id"`
		ModelID  string `json:"model_id"`
	}
	var input targetInput
	var settings modelconfig.CommandSettings
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if req.URL.Path == "/management/v1/preview" {
		var previewInput struct {
			targetInput
			Settings modelconfig.CommandSettings `json:"settings"`
		}
		if err := decoder.Decode(&previewInput); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		input, settings = previewInput.targetInput, previewInput.Settings
	} else {
		if err := decoder.Decode(&input); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
	}
	target, ok := management.Targets[input.TargetID]
	if !ok {
		http.NotFound(w, req)
		return
	}
	var result any
	if req.URL.Path == "/management/v1/preview" {
		result, err = modelconfig.PreviewTarget(target.Directory, target.Filename, input.TargetID, input.ModelID, settings)
	} else {
		result, err = modelconfig.ReadSettingsTarget(target.Directory, target.Filename, input.TargetID, input.ModelID)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if r.state.Load() != state {
		http.Error(w, "management configuration changed", http.StatusServiceUnavailable)
		return
	}
	writeJSON(r.logger, w, http.StatusOK, result)
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
				min := r
				for next := unicode.SimpleFold(r); next != r; next = unicode.SimpleFold(next) {
					if next < min {
						min = next
					}
				}
				return min
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

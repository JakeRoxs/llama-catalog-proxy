package proxy

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

const (
	adminOIDCLoginPath    = "/admin/auth/login"
	adminOIDCCallbackPath = "/admin/auth/callback"
	adminOIDCLogoutPath   = "/admin/auth/logout"
	adminSessionPath      = "/admin/api/v1/session"
	adminSessionCookie    = "__Host-lcp_admin"
	adminStateCookie      = "__Host-lcp_oidc_state"
	adminCSRFHeader       = "X-Llama-Catalog-CSRF"
	maxAdminPending       = 128
	maxAdminSessions      = 256
	adminTransactionTTL   = 5 * time.Minute
	adminOIDCTimeout      = 10 * time.Second
)

type adminAuth struct {
	config     config.ManagementUIConfig
	logger     *slog.Logger
	httpClient *http.Client
	sessionTTL time.Duration
	idleTTL    time.Duration

	providerMu sync.Mutex
	provider   *oidc.Provider
	oauth      oauth2.Config
	verifier   *oidc.IDTokenVerifier

	mu       sync.Mutex
	pending  map[string]oidcLoginTransaction
	sessions map[string]adminSession
}

type oidcLoginTransaction struct {
	nonce     string
	verifier  string
	expiresAt time.Time
}

type adminSession struct {
	subject   string
	roles     []string
	csrf      string
	lastSeen  time.Time
	expiresAt time.Time
}

func newAdminAuth(cfg *config.ManagementUIConfig, logger *slog.Logger, client *http.Client) (*adminAuth, error) {
	if cfg == nil {
		return nil, nil
	}
	if client == nil {
		client = http.DefaultClient
	}
	ttl := 8 * time.Hour
	if cfg.SessionTTL != "" {
		parsed, err := time.ParseDuration(cfg.SessionTTL)
		if err != nil {
			return nil, errors.New("invalid management UI session lifetime")
		}
		ttl = parsed
	}
	idleTTL := 30 * time.Minute
	if cfg.SessionIdleTTL != "" {
		parsed, err := time.ParseDuration(cfg.SessionIdleTTL)
		if err != nil {
			return nil, errors.New("invalid management UI idle session lifetime")
		}
		idleTTL = parsed
	}
	copy := *cfg
	copy.RoleGroups = make(map[string][]string, len(cfg.RoleGroups))
	for role, groups := range cfg.RoleGroups {
		copy.RoleGroups[role] = append([]string(nil), groups...)
	}
	copy.Scopes = append([]string(nil), cfg.Scopes...)
	if logger == nil {
		logger = slog.Default()
	}
	return &adminAuth{
		config: copy, logger: logger, httpClient: client, sessionTTL: ttl, idleTTL: idleTTL,
		pending: make(map[string]oidcLoginTransaction), sessions: make(map[string]adminSession),
	}, nil
}

func (a *adminAuth) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	switch req.URL.Path {
	case "/admin":
		if req.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		http.Redirect(w, req, "/admin/", http.StatusPermanentRedirect)
	case "/admin/":
		serveAdminAsset(w, req, "admin.html")
	case "/admin/admin.css":
		serveAdminAsset(w, req, "admin.css")
	case "/admin/admin.js":
		serveAdminAsset(w, req, "admin.js")
	case adminOIDCLoginPath:
		a.login(w, req)
	case adminOIDCCallbackPath:
		a.callback(w, req)
	case adminOIDCLogoutPath:
		a.logout(w, req)
	case adminSessionPath:
		a.session(w, req)
	default:
		http.NotFound(w, req)
	}
}

func (a *adminAuth) login(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet || req.URL.RawQuery != "" || req.URL.RawPath != "" {
		http.Error(w, "invalid login request", http.StatusBadRequest)
		return
	}
	provider, oauth, _, err := a.oidc(req.Context())
	if err != nil {
		a.logger.Warn("management OIDC provider unavailable", "error", err)
		http.Error(w, "identity provider unavailable", http.StatusServiceUnavailable)
		return
	}
	_ = provider
	state, err := newAdminRandomToken(32)
	if err != nil {
		http.Error(w, "login unavailable", http.StatusInternalServerError)
		return
	}
	nonce, err := newAdminRandomToken(32)
	if err != nil {
		http.Error(w, "login unavailable", http.StatusInternalServerError)
		return
	}
	verifier := oauth2.GenerateVerifier()
	now := time.Now()
	a.mu.Lock()
	a.purgeExpiredLocked(now)
	if len(a.pending) >= maxAdminPending {
		a.mu.Unlock()
		http.Error(w, "too many pending logins", http.StatusServiceUnavailable)
		return
	}
	a.pending[state] = oidcLoginTransaction{nonce: nonce, verifier: verifier, expiresAt: now.Add(adminTransactionTTL)}
	a.mu.Unlock()
	http.SetCookie(w, &http.Cookie{
		Name: adminStateCookie, Value: state, Path: "/", Secure: true, HttpOnly: true,
		SameSite: http.SameSiteLaxMode, MaxAge: int(adminTransactionTTL.Seconds()),
	})
	url := oauth.AuthCodeURL(state,
		oauth2.SetAuthURLParam("nonce", nonce),
		oauth2.S256ChallengeOption(verifier),
	)
	http.Redirect(w, req, url, http.StatusFound)
}

func (a *adminAuth) callback(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet || req.URL.RawPath != "" {
		http.Error(w, "invalid authentication callback", http.StatusBadRequest)
		return
	}
	query := req.URL.Query()
	stateValues, codeValues := query["state"], query["code"]
	stateCookie, err := req.Cookie(adminStateCookie)
	if err != nil || len(stateValues) != 1 || len(codeValues) != 1 || codeValues[0] == "" ||
		stateValues[0] == "" || subtle.ConstantTimeCompare([]byte(stateValues[0]), []byte(stateCookie.Value)) != 1 {
		a.clearCookie(w, adminStateCookie, "/")
		http.Error(w, "invalid authentication callback", http.StatusBadRequest)
		return
	}
	if query.Get("error") != "" {
		a.removeTransaction(stateValues[0])
		a.clearCookie(w, adminStateCookie, "/")
		http.Error(w, "authentication was not completed", http.StatusUnauthorized)
		return
	}
	transaction, ok := a.takeTransaction(stateValues[0], time.Now())
	a.clearCookie(w, adminStateCookie, "/")
	if !ok {
		http.Error(w, "authentication transaction expired", http.StatusBadRequest)
		return
	}
	provider, oauth, verifier, err := a.oidc(req.Context())
	if err != nil {
		a.logger.Warn("management OIDC provider unavailable", "error", err)
		http.Error(w, "identity provider unavailable", http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(req.Context(), adminOIDCTimeout)
	defer cancel()
	ctx = context.WithValue(ctx, oauth2.HTTPClient, a.httpClient)
	token, err := oauth.Exchange(ctx, codeValues[0], oauth2.VerifierOption(transaction.verifier))
	if err != nil {
		var retrieveErr *oauth2.RetrieveError
		if errors.As(err, &retrieveErr) {
			status := 0
			if retrieveErr.Response != nil {
				status = retrieveErr.Response.StatusCode
			}
			a.logger.Warn("management OIDC code exchange failed", "error_code", retrieveErr.ErrorCode, "status", status)
		} else {
			a.logger.Warn("management OIDC code exchange failed", "error_type", fmt.Sprintf("%T", err))
		}
		http.Error(w, "authentication failed", http.StatusUnauthorized)
		return
	}
	rawIDToken, ok := token.Extra("id_token").(string)
	if !ok || rawIDToken == "" {
		a.logger.Warn("management OIDC token response omitted ID token")
		http.Error(w, "authentication failed", http.StatusUnauthorized)
		return
	}
	idToken, err := verifier.Verify(ctx, rawIDToken)
	if err != nil {
		a.logger.Warn("management OIDC ID-token verification failed")
		http.Error(w, "authentication failed", http.StatusUnauthorized)
		return
	}
	if subtle.ConstantTimeCompare([]byte(idToken.Nonce), []byte(transaction.nonce)) != 1 {
		a.logger.Warn("management OIDC nonce validation failed")
		http.Error(w, "authentication failed", http.StatusUnauthorized)
		return
	}
	groups, err := oidcGroups(idToken, a.config.GroupsClaim)
	if err != nil {
		a.logger.Warn("management OIDC group claim invalid")
		http.Error(w, "authentication failed", http.StatusUnauthorized)
		return
	}
	roles := a.rolesForGroups(groups)
	if len(roles) == 0 {
		http.Error(w, "administrator role required", http.StatusForbidden)
		return
	}
	sessionID, err := newAdminRandomToken(32)
	if err != nil {
		http.Error(w, "session unavailable", http.StatusInternalServerError)
		return
	}
	csrf, err := newAdminRandomToken(32)
	if err != nil {
		http.Error(w, "session unavailable", http.StatusInternalServerError)
		return
	}
	now := time.Now()
	a.mu.Lock()
	a.purgeExpiredLocked(now)
	if len(a.sessions) >= maxAdminSessions {
		a.mu.Unlock()
		http.Error(w, "too many active admin sessions", http.StatusServiceUnavailable)
		return
	}
	a.sessions[sessionID] = adminSession{subject: idToken.Subject, roles: roles, csrf: csrf, lastSeen: now, expiresAt: now.Add(a.sessionTTL)}
	a.mu.Unlock()
	http.SetCookie(w, &http.Cookie{
		Name: adminSessionCookie, Value: sessionID, Path: "/", Secure: true, HttpOnly: true,
		SameSite: http.SameSiteLaxMode, MaxAge: int(a.sessionTTL.Seconds()),
	})
	http.Redirect(w, req, "/admin/", http.StatusSeeOther)
	_ = provider
}

func (a *adminAuth) session(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet || req.URL.RawQuery != "" || req.URL.RawPath != "" {
		http.Error(w, "invalid session request", http.StatusBadRequest)
		return
	}
	session, ok := a.sessionForRequest(req)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	roles := append([]string(nil), session.roles...)
	sort.Strings(roles)
	writeJSON(a.logger, w, http.StatusOK, map[string]any{
		"subject": session.subject, "roles": roles, "csrf_token": session.csrf,
		"expires_at": session.expiresAt.UTC(),
	})
}

func (a *adminAuth) logout(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost || !sameBenchmarkOrigin(req) {
		http.Error(w, "invalid logout request", http.StatusForbidden)
		return
	}
	requestSession, ok := a.sessionForRequest(req)
	if !ok || !a.validCSRF(req, requestSession) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	cookie, err := req.Cookie(adminSessionCookie)
	if err == nil {
		a.mu.Lock()
		delete(a.sessions, cookie.Value)
		a.mu.Unlock()
	}
	a.clearCookie(w, adminSessionCookie, "/")
	w.WriteHeader(http.StatusNoContent)
}

func (a *adminAuth) RequireRole(req *http.Request, role string) (string, bool) {
	session, ok := a.sessionForRequest(req)
	if !ok {
		return "", false
	}
	for _, granted := range session.roles {
		if granted == role {
			return session.subject, true
		}
	}
	return "", false
}

func (a *adminAuth) AuthenticatedPrincipal(req *http.Request) (string, []string, bool) {
	session, ok := a.sessionForRequest(req)
	if !ok {
		return "", nil, false
	}
	return session.subject, append([]string(nil), session.roles...), true
}

func (a *adminAuth) RequireCSRF(req *http.Request) bool {
	session, ok := a.sessionForRequest(req)
	return ok && sameBenchmarkOrigin(req) && a.validCSRF(req, session)
}

func (a *adminAuth) sessionForRequest(req *http.Request) (adminSession, bool) {
	cookie, err := req.Cookie(adminSessionCookie)
	if err != nil || cookie.Value == "" {
		return adminSession{}, false
	}
	now := time.Now()
	a.mu.Lock()
	defer a.mu.Unlock()
	a.purgeExpiredLocked(now)
	session, ok := a.sessions[cookie.Value]
	if !ok || !now.Before(session.expiresAt) || now.Sub(session.lastSeen) >= a.idleTTL {
		delete(a.sessions, cookie.Value)
		return adminSession{}, false
	}
	session.lastSeen = now
	a.sessions[cookie.Value] = session
	return session, true
}

func (a *adminAuth) validCSRF(req *http.Request, session adminSession) bool {
	values := req.Header.Values(adminCSRFHeader)
	return len(values) == 1 && subtle.ConstantTimeCompare([]byte(values[0]), []byte(session.csrf)) == 1
}

func (a *adminAuth) rolesForGroups(groups []string) []string {
	member := make(map[string]bool, len(groups))
	for _, group := range groups {
		member[group] = true
	}
	var roles []string
	for role, allowed := range a.config.RoleGroups {
		for _, group := range allowed {
			if member[group] {
				roles = append(roles, role)
				break
			}
		}
	}
	sort.Strings(roles)
	return roles
}

func (a *adminAuth) oidc(ctx context.Context) (*oidc.Provider, oauth2.Config, *oidc.IDTokenVerifier, error) {
	a.providerMu.Lock()
	defer a.providerMu.Unlock()
	if a.provider != nil {
		return a.provider, a.oauth, a.verifier, nil
	}
	ctx, cancel := context.WithTimeout(ctx, adminOIDCTimeout)
	defer cancel()
	ctx = context.WithValue(ctx, oauth2.HTTPClient, a.httpClient)
	provider, err := oidc.NewProvider(ctx, a.config.IssuerURL)
	if err != nil {
		return nil, oauth2.Config{}, nil, err
	}
	scopes := []string{oidc.ScopeOpenID, "profile", "email"}
	scopes = append(scopes, a.config.Scopes...)
	a.oauth = oauth2.Config{
		ClientID: a.config.ClientID, ClientSecret: a.config.ClientSecret, RedirectURL: a.config.RedirectURL,
		Endpoint: provider.Endpoint(), Scopes: scopes,
	}
	a.verifier = provider.Verifier(&oidc.Config{ClientID: a.config.ClientID})
	a.provider = provider
	return provider, a.oauth, a.verifier, nil
}

func (a *adminAuth) purgeExpiredLocked(now time.Time) {
	for state, transaction := range a.pending {
		if !now.Before(transaction.expiresAt) {
			delete(a.pending, state)
		}
	}
	for sessionID, session := range a.sessions {
		if !now.Before(session.expiresAt) || now.Sub(session.lastSeen) >= a.idleTTL {
			delete(a.sessions, sessionID)
		}
	}
}

func (a *adminAuth) takeTransaction(state string, now time.Time) (oidcLoginTransaction, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	transaction, ok := a.pending[state]
	delete(a.pending, state)
	return transaction, ok && now.Before(transaction.expiresAt)
}

func (a *adminAuth) removeTransaction(state string) {
	a.mu.Lock()
	delete(a.pending, state)
	a.mu.Unlock()
}

func (a *adminAuth) clearCookie(w http.ResponseWriter, name, cookiePath string) {
	http.SetCookie(w, &http.Cookie{
		Name: name, Value: "", Path: cookiePath, Secure: true, HttpOnly: true,
		SameSite: http.SameSiteLaxMode, MaxAge: -1, Expires: time.Unix(1, 0).UTC(),
	})
}

func newAdminRandomToken(size int) (string, error) {
	value := make([]byte, size)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}

func oidcGroups(token *oidc.IDToken, claim string) ([]string, error) {
	if claim == "" {
		claim = "groups"
	}
	var claims map[string]json.RawMessage
	if err := token.Claims(&claims); err != nil {
		return nil, err
	}
	raw, ok := claims[claim]
	if !ok {
		return nil, nil
	}
	var groups []string
	if err := json.Unmarshal(raw, &groups); err == nil {
		return groups, nil
	}
	var group string
	if err := json.Unmarshal(raw, &group); err == nil && group != "" {
		return []string{group}, nil
	}
	return nil, errors.New("invalid OIDC group claim")
}

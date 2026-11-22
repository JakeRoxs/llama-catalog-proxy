package proxy

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

type fakeOIDCProvider struct {
	mu        sync.Mutex
	key       *rsa.PrivateKey
	server    *httptest.Server
	nonce     string
	challenge string
	groups    []string
}

func newFakeOIDCProvider(t *testing.T) *fakeOIDCProvider {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeOIDCProvider{key: key, groups: []string{"operators", "model-admins"}}
	fake.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/.well-known/openid-configuration":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"issuer":                                fake.server.URL,
				"authorization_endpoint":                fake.server.URL + "/authorize",
				"token_endpoint":                        fake.server.URL + "/token",
				"jwks_uri":                              fake.server.URL + "/jwks",
				"response_types_supported":              []string{"code"},
				"subject_types_supported":               []string{"public"},
				"id_token_signing_alg_values_supported": []string{"RS256"},
				"token_endpoint_auth_methods_supported": []string{"client_secret_basic"},
			})
		case "/jwks":
			w.Header().Set("Content-Type", "application/json")
			modulus := base64.RawURLEncoding.EncodeToString(fake.key.N.Bytes())
			exponent := base64.RawURLEncoding.EncodeToString(big.NewInt(int64(fake.key.E)).Bytes())
			_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]string{
				"kty": "RSA", "use": "sig", "alg": "RS256", "kid": "test-key", "n": modulus, "e": exponent,
			}}})
		case "/token":
			if err := req.ParseForm(); err != nil || req.Form.Get("code") != "valid-code" {
				http.Error(w, "invalid test code", http.StatusBadRequest)
				return
			}
			clientID, clientSecret, authenticated := req.BasicAuth()
			if !authenticated || clientID != "catalog-client" || clientSecret != strings.Repeat("c", 32) {
				http.Error(w, "invalid OIDC client authentication", http.StatusUnauthorized)
				return
			}
			fake.mu.Lock()
			verifier, nonce := req.Form.Get("code_verifier"), fake.nonce
			groups := append([]string(nil), fake.groups...)
			challenge := fake.challenge
			fake.mu.Unlock()
			hash := sha256.Sum256([]byte(verifier))
			if base64.RawURLEncoding.EncodeToString(hash[:]) != challenge || verifier == "" {
				http.Error(w, "invalid test verifier", http.StatusBadRequest)
				return
			}
			token, err := fake.signIDToken(nonce, groups)
			if err != nil {
				http.Error(w, "could not sign test token", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token": "test-access-token", "token_type": "Bearer", "expires_in": 3600, "id_token": token,
			})
		default:
			http.NotFound(w, req)
		}
	}))
	t.Cleanup(fake.server.Close)
	return fake
}

func (f *fakeOIDCProvider) signIDToken(nonce string, groups []string) (string, error) {
	header, err := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT", "kid": "test-key"})
	if err != nil {
		return "", err
	}
	now := time.Now()
	claims, err := json.Marshal(map[string]any{
		"iss": f.server.URL, "sub": "user-123", "aud": "catalog-client",
		"iat": now.Unix(), "exp": now.Add(time.Hour).Unix(), "nonce": nonce, "groups": groups,
	})
	if err != nil {
		return "", err
	}
	input := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(claims)
	digest := sha256.Sum256([]byte(input))
	signature, err := rsa.SignPKCS1v15(rand.Reader, f.key, crypto.SHA256, digest[:])
	if err != nil {
		return "", err
	}
	return input + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

func TestAdminOIDCLoginSessionRolesAndLogout(t *testing.T) {
	idp := newFakeOIDCProvider(t)
	auth, err := newAdminAuth(&config.ManagementUIConfig{
		IssuerURL: idp.server.URL, ClientID: "catalog-client", ClientSecret: strings.Repeat("c", 32),
		RedirectURL: "https://catalog.example/admin/auth/callback", GroupsClaim: "groups",
		RoleGroups: map[string][]string{"view": {"operators"}, "configure_models": {"model-admins"}}, SessionTTL: "1h",
	}, discardLogger(), idp.server.Client())
	if err != nil {
		t.Fatal(err)
	}

	_, state, nonce, challenge, stateCookie := loginStart(t, auth)
	idp.mu.Lock()
	idp.nonce, idp.challenge = nonce, challenge
	idp.mu.Unlock()
	sessionCookie := oidcCallback(t, auth, stateCookie, state)
	sessionRequest := httptest.NewRequest(http.MethodGet, "https://catalog.example"+adminSessionPath, nil)
	csrf := checkSession(t, auth, sessionRequest, sessionCookie)
	adminLogout(t, auth, sessionRequest, sessionCookie, csrf)
}

// loginStart performs the OIDC login initiation and asserts the redirect
// carries state, nonce, and PKCE S256 challenge with a protected state cookie.
// It returns the authorize URL, state, nonce, code_challenge, and state cookie.
func loginStart(t *testing.T, auth *adminAuth) (*url.URL, string, string, string, *http.Cookie) {
	t.Helper()
	login := httptest.NewRequest(http.MethodGet, "https://catalog.example/admin/auth/login", nil)
	login.AddCookie(&http.Cookie{Name: adminSessionCookie, Value: "attacker-fixed-session"})
	loginResponse := httptest.NewRecorder()
	auth.ServeHTTP(loginResponse, login)
	if loginResponse.Code != http.StatusFound {
		t.Fatalf("login start = %d %s", loginResponse.Code, loginResponse.Body.String())
	}
	u, err := url.Parse(loginResponse.Header().Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	state, nonce := u.Query().Get("state"), u.Query().Get("nonce")
	challenge := u.Query().Get("code_challenge")
	if state == "" || nonce == "" || challenge == "" || u.Query().Get("code_challenge_method") != "S256" {
		t.Fatalf("OIDC redirect lacks state/nonce/PKCE: %s", u.Redacted())
	}
	var stateCookie *http.Cookie
	for _, cookie := range loginResponse.Result().Cookies() {
		if cookie.Name == adminStateCookie {
			stateCookie = cookie
		}
	}
	if stateCookie == nil || !stateCookie.Secure || !stateCookie.HttpOnly || stateCookie.SameSite != http.SameSiteLaxMode {
		t.Fatalf("OIDC state cookie is not protected: %+v", stateCookie)
	}
	return u, state, nonce, challenge, stateCookie
}

// oidcCallback performs the callback request with the state cookie and asserts
// the issued session cookie is protected and not attacker-fixed.
func oidcCallback(t *testing.T, auth *adminAuth, stateCookie *http.Cookie, state string) *http.Cookie {
	t.Helper()
	callbackURL := "https://catalog.example" + adminOIDCCallbackPath + "?state=" + url.QueryEscape(state) + "&code=valid-code"
	callback := httptest.NewRequest(http.MethodGet, callbackURL, nil)
	callback.AddCookie(stateCookie)
	callbackResponse := httptest.NewRecorder()
	auth.ServeHTTP(callbackResponse, callback)
	if callbackResponse.Code != http.StatusSeeOther || callbackResponse.Header().Get("Location") != "/admin/" {
		t.Fatalf("OIDC callback = %d %s", callbackResponse.Code, callbackResponse.Body.String())
	}
	var sessionCookie *http.Cookie
	for _, cookie := range callbackResponse.Result().Cookies() {
		if cookie.Name == adminSessionCookie {
			sessionCookie = cookie
		}
	}
	if sessionCookie == nil || sessionCookie.Value == "attacker-fixed-session" || !sessionCookie.Secure || !sessionCookie.HttpOnly || sessionCookie.SameSite != http.SameSiteLaxMode {
		t.Fatalf("admin session cookie is not protected: %+v", sessionCookie)
	}
	return sessionCookie
}

// checkSession asserts the session endpoint returns the expected subject and
// role mapping, keeps the access token secret, and that role gating matches the
// configured mapping. It returns the session CSRF token.
func checkSession(t *testing.T, auth *adminAuth, sessionRequest *http.Request, sessionCookie *http.Cookie) string {
	t.Helper()
	sessionRequest.AddCookie(sessionCookie)
	sessionResponse := httptest.NewRecorder()
	auth.ServeHTTP(sessionResponse, sessionRequest)
	var session struct {
		Subject string   `json:"subject"`
		Roles   []string `json:"roles"`
		CSRF    string   `json:"csrf_token"`
	}
	if sessionResponse.Code != http.StatusOK || json.Unmarshal(sessionResponse.Body.Bytes(), &session) != nil ||
		session.Subject != "user-123" || !slices.Contains(session.Roles, "view") || !slices.Contains(session.Roles, "configure_models") ||
		strings.Contains(sessionResponse.Body.String(), "test-access-token") {
		t.Fatalf("admin session = %d %s", sessionResponse.Code, sessionResponse.Body.String())
	}
	if subject, ok := auth.RequireRole(sessionRequest, "configure_models"); !ok || subject != "user-123" {
		t.Fatal("configured OIDC role was not applied")
	}
	if _, ok := auth.RequireRole(sessionRequest, "administer_federation"); ok {
		t.Fatal("unassigned federation role was granted")
	}
	return session.CSRF
}

// adminLogout asserts foreign-origin logout is rejected, a valid CSRF logout
// succeeds, and the session is invalidated afterward.
func adminLogout(t *testing.T, auth *adminAuth, sessionRequest *http.Request, sessionCookie *http.Cookie, csrf string) {
	t.Helper()
	foreignLogout := httptest.NewRequest(http.MethodPost, "https://catalog.example"+adminOIDCLogoutPath, nil)
	foreignLogout.AddCookie(sessionCookie)
	foreignLogout.Header.Set("Origin", "https://attacker.example")
	foreignLogout.Header.Set(adminCSRFHeader, csrf)
	foreignResponse := httptest.NewRecorder()
	auth.ServeHTTP(foreignResponse, foreignLogout)
	if foreignResponse.Code != http.StatusForbidden {
		t.Fatalf("cross-origin logout accepted: %d", foreignResponse.Code)
	}
	logout := httptest.NewRequest(http.MethodPost, "https://catalog.example"+adminOIDCLogoutPath, nil)
	logout.AddCookie(sessionCookie)
	logout.Header.Set("Origin", "https://catalog.example")
	logout.Header.Set(adminCSRFHeader, csrf)
	logoutResponse := httptest.NewRecorder()
	auth.ServeHTTP(logoutResponse, logout)
	if logoutResponse.Code != http.StatusNoContent {
		t.Fatalf("logout = %d %s", logoutResponse.Code, logoutResponse.Body.String())
	}
	if _, ok := auth.RequireRole(sessionRequest, "view"); ok {
		t.Fatal("logged-out session remained valid")
	}
}

func TestAdminOIDCRejectsForgedHeadersStateNonceAndCSRF(t *testing.T) {
	idp := newFakeOIDCProvider(t)
	auth, err := newAdminAuth(&config.ManagementUIConfig{
		IssuerURL: idp.server.URL, ClientID: "catalog-client", ClientSecret: strings.Repeat("c", 32),
		RedirectURL: "https://catalog.example/admin/auth/callback", RoleGroups: map[string][]string{"view": {"operators"}},
	}, discardLogger(), idp.server.Client())
	if err != nil {
		t.Fatal(err)
	}
	forged := httptest.NewRequest(http.MethodGet, "https://catalog.example"+adminSessionPath, nil)
	forged.Header.Set("X-Forwarded-User", "admin")
	forgedResponse := httptest.NewRecorder()
	auth.ServeHTTP(forgedResponse, forged)
	if forgedResponse.Code != http.StatusUnauthorized {
		t.Fatalf("forwarded identity header authorized admin API: %d", forgedResponse.Code)
	}
	badLogout := httptest.NewRequest(http.MethodPost, "https://catalog.example"+adminOIDCLogoutPath, nil)
	badLogout.Header.Set("Origin", "https://catalog.example")
	badLogout.Header.Set(adminCSRFHeader, "forged")
	badLogoutResponse := httptest.NewRecorder()
	auth.ServeHTTP(badLogoutResponse, badLogout)
	if badLogoutResponse.Code != http.StatusUnauthorized {
		t.Fatalf("CSRF-less logout accepted: %d", badLogoutResponse.Code)
	}

	login := httptest.NewRequest(http.MethodGet, "https://catalog.example/admin/auth/login", nil)
	loginResponse := httptest.NewRecorder()
	auth.ServeHTTP(loginResponse, login)
	authorizeURL, err := url.Parse(loginResponse.Header().Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	state, nonce := authorizeURL.Query().Get("state"), authorizeURL.Query().Get("nonce")
	idp.mu.Lock()
	idp.nonce, idp.challenge = "wrong-nonce", authorizeURL.Query().Get("code_challenge")
	idp.mu.Unlock()
	var cookie *http.Cookie
	for _, candidate := range loginResponse.Result().Cookies() {
		if candidate.Name == adminStateCookie {
			cookie = candidate
		}
	}
	callback := httptest.NewRequest(http.MethodGet, "https://catalog.example"+adminOIDCCallbackPath+"?state="+url.QueryEscape(state)+"&code=valid-code", nil)
	callback.AddCookie(cookie)
	callbackResponse := httptest.NewRecorder()
	auth.ServeHTTP(callbackResponse, callback)
	if callbackResponse.Code != http.StatusUnauthorized {
		t.Fatalf("nonce mismatch accepted: %d", callbackResponse.Code)
	}
	replay := httptest.NewRequest(http.MethodGet, "https://catalog.example"+adminOIDCCallbackPath+"?state="+url.QueryEscape(state)+"&code=valid-code", nil)
	replay.AddCookie(cookie)
	replayResponse := httptest.NewRecorder()
	auth.ServeHTTP(replayResponse, replay)
	if replayResponse.Code != http.StatusBadRequest {
		t.Fatalf("replayed OIDC state accepted: %d", replayResponse.Code)
	}
	_ = nonce
}

func TestReloaderAdminAPIRejectsUnauthenticatedDirectAccess(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	configText := `listen: "0.0.0.0:8080"
default_backend: local
backends: {local: {url: "http://127.0.0.1:8188"}}
management_ui:
  issuer_url: "https://identity.example"
  client_id: "catalog-client"
  client_secret: "ssssssssssssssssssssssssssssssss"
  redirect_url: "https://catalog.example/admin/auth/callback"
  role_groups: {view: ["operators"]}
`
	if err := os.WriteFile(configPath, []byte(configText), 0o600); err != nil {
		t.Fatal(err)
	}
	reloader, _, err := NewReloader(configPath, discardLogger(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reloader.Stop()
	request := httptest.NewRequest(http.MethodGet, "http://192.0.2.10/admin/api/v1/session", nil)
	request.Header.Set("X-Forwarded-User", "forged-admin")
	response := httptest.NewRecorder()
	reloader.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("forwarded header authorized direct admin access: %d", response.Code)
	}
}

func TestAdminSessionExpiresAfterIdleOrAbsoluteLifetime(t *testing.T) {
	auth, err := newAdminAuth(&config.ManagementUIConfig{
		IssuerURL: "https://identity.example", ClientID: "catalog-client", ClientSecret: strings.Repeat("c", 32),
		RedirectURL: "https://catalog.example/admin/auth/callback", RoleGroups: map[string][]string{"view": {"operators"}},
		SessionTTL: "1h", SessionIdleTTL: "1m",
	}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	auth.sessions["idle-session"] = adminSession{subject: "user", roles: []string{"view"}, csrf: "csrf",
		lastSeen: now.Add(-2 * time.Minute), expiresAt: now.Add(time.Hour)}
	request := httptest.NewRequest(http.MethodGet, "https://catalog.example"+adminSessionPath, nil)
	request.AddCookie(&http.Cookie{Name: adminSessionCookie, Value: "idle-session"})
	if _, ok := auth.sessionForRequest(request); ok {
		t.Fatal("idle session remained valid")
	}
	auth.sessions["absolute-session"] = adminSession{subject: "user", roles: []string{"view"}, csrf: "csrf",
		lastSeen: now, expiresAt: now.Add(-time.Second)}
	request = httptest.NewRequest(http.MethodGet, "https://catalog.example"+adminSessionPath, nil)
	request.AddCookie(&http.Cookie{Name: adminSessionCookie, Value: "absolute-session"})
	if _, ok := auth.sessionForRequest(request); ok {
		t.Fatal("absolute-expired session remained valid")
	}
}

func TestAdminOIDCProviderOutageDoesNotFallBackToAnonymous(t *testing.T) {
	idp := httptest.NewTLSServer(http.NotFoundHandler())
	issuer := idp.URL
	idp.Close()
	auth, err := newAdminAuth(&config.ManagementUIConfig{
		IssuerURL: issuer, ClientID: "catalog-client", ClientSecret: strings.Repeat("c", 32),
		RedirectURL: "https://catalog.example/admin/auth/callback", RoleGroups: map[string][]string{"view": {"operators"}},
	}, discardLogger(), http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "https://catalog.example/admin/auth/login", nil)
	response := httptest.NewRecorder()
	auth.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("OIDC provider outage fell back to access: %d", response.Code)
	}
}

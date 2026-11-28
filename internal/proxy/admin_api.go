package proxy

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/correlation"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/federation"
)

type adminModelSummary struct {
	ID      string `json:"id"`
	Object  string `json:"object,omitempty"`
	OwnedBy string `json:"owned_by,omitempty"`
}

type adminPeerSummary struct {
	NodeID              string     `json:"node_id"`
	Status              string     `json:"status"`
	ReceivedAt          *time.Time `json:"received_at,omitempty"`
	IdentityState       string     `json:"identity_state"`
	AuthorizationState  string     `json:"authorization_state"`
	ConfiguredScopes    []string   `json:"configured_scopes"`
	ModelReadinessState string     `json:"model_readiness_state"`
}

type adminRequestSummary struct {
	Count               int            `json:"count"`
	Outcomes            map[string]int `json:"outcomes"`
	GenerationOutcomes  map[string]int `json:"generation_outcomes"`
	DurationMS          map[string]any `json:"duration_ms"`
	DurationSampleCount int            `json:"duration_sample_count"`
}

type adminCRLTimeSummary struct {
	State         string     `json:"state"`
	UpdatedAt     *time.Time `json:"updated_at,omitempty"`
	ExpiresAt     *time.Time `json:"expires_at,omitempty"`
	MaxAgeSeconds int64      `json:"max_age_seconds"`
}

type adminPeerCRLSummary struct {
	NodeID string `json:"node_id"`
	adminCRLTimeSummary
}

type adminFederationPolicySummary struct {
	State     string                `json:"state"`
	ClientCRL adminCRLTimeSummary   `json:"client_crl"`
	PeerCRLs  []adminPeerCRLSummary `json:"peer_crls"`
}

type adminOverview struct {
	NodeID              string                       `json:"node_id,omitempty"`
	Models              []adminModelSummary          `json:"models"`
	Peers               []adminPeerSummary           `json:"peers"`
	FederationPolicy    adminFederationPolicySummary `json:"federation_policy"`
	RequestSummary      *adminRequestSummary         `json:"request_summary,omitempty"`
	RequestSummaryState string                       `json:"request_summary_state"`
	Updated             time.Time                    `json:"updated_at"`
}

type adminActivityEvent struct {
	Time       time.Time `json:"time"`
	BackendID  string    `json:"backend_id"`
	Status     string    `json:"status,omitempty"`
	DurationMS *float64  `json:"duration_ms,omitempty"`
}

type adminActivity struct {
	State   string               `json:"state"`
	Events  []adminActivityEvent `json:"events"`
	Updated time.Time            `json:"updated_at"`
}

// adminAPIGetGuard applies the shared read-only admin API guard. Cache-Control
// and nosniff headers are always set, then GET-only, session, and role checks
// run in method->session->role order. It returns true when the request is
// authorized to proceed; otherwise an error response has already been written.
func (r *Reloader) adminAPIGetGuard(w http.ResponseWriter, req *http.Request) bool {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if req.Method != http.MethodGet || req.URL.RawQuery != "" || req.URL.RawPath != "" {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return false
	}
	if _, ok := r.adminAuth.sessionForRequest(req); !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	if _, ok := r.adminAuth.RequireRole(req, "view"); !ok {
		http.Error(w, "forbidden", http.StatusForbidden)
		return false
	}
	return true
}

func (r *Reloader) serveAdminOverview(w http.ResponseWriter, req *http.Request) {
	if !r.adminAPIGetGuard(w, req) {
		return
	}
	state := r.state.Load()
	if state == nil {
		http.Error(w, "dashboard data unavailable", http.StatusServiceUnavailable)
		return
	}
	handler, ok := state.handler.(*Handler)
	if !ok || handler.catalog == nil {
		http.Error(w, "dashboard data unavailable", http.StatusServiceUnavailable)
		return
	}
	models, ok := fetchModelSummaries(handler, req)
	if !ok {
		http.Error(w, "model catalog unavailable", http.StatusServiceUnavailable)
		return
	}
	now := time.Now()
	overview := adminOverview{
		Models:              models,
		Peers:               []adminPeerSummary{},
		Updated:             now.UTC(),
		RequestSummaryState: "not_configured",
		FederationPolicy:    adminFederationPolicySummary{State: "not_configured", PeerCRLs: []adminPeerCRLSummary{}},
	}
	if state.peer != nil {
		overview.NodeID = state.peer.NodeID
		if state.peer.FederationID != "" {
			overview.FederationPolicy = summarizeFederationCRLs(state, now)
		}
	}
	if state.peerCache != nil {
		overview.Peers = constructPeerSummaries(state)
	}
	if handler.diagnosticsAPIKey != "" {
		summary, state := fetchRequestSummary(handler)
		overview.RequestSummary = summary
		overview.RequestSummaryState = state
	}
	writeJSON(r.logger, w, http.StatusOK, overview)
}

// fetchModelSummaries returns the model catalog summaries sorted by ID, skipping
// records that fail to unmarshal or carry an empty ID. A false ok means the
// catalog could not be fetched or decoded.
func fetchModelSummaries(handler *Handler, req *http.Request) ([]adminModelSummary, bool) {
	ctx, cancel := context.WithTimeout(req.Context(), handler.catalogTimeout)
	defer cancel()
	catalogResponse, _, err := handler.modelCatalog(ctx, make(http.Header))
	if err != nil {
		return nil, false
	}
	var envelope struct {
		Data []json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(catalogResponse.Body, &envelope); err != nil {
		return nil, false
	}
	models := make([]adminModelSummary, 0, len(envelope.Data))
	for _, raw := range envelope.Data {
		var model struct {
			ID      string `json:"id"`
			Object  string `json:"object"`
			OwnedBy string `json:"owned_by"`
		}
		if err := json.Unmarshal(raw, &model); err != nil || model.ID == "" {
			continue
		}
		models = append(models, adminModelSummary{ID: model.ID, Object: model.Object, OwnedBy: model.OwnedBy})
	}
	sort.Slice(models, func(i, j int) bool { return models[i].ID < models[j].ID })
	return models, true
}

// constructPeerSummaries builds peer summaries from the peer cache views,
// applying identity-store lookup, configured scopes, and readiness defaults.
func constructPeerSummaries(state *reloaderState) []adminPeerSummary {
	peers := []adminPeerSummary{}
	if state.peerCache != nil {
		views, _ := state.peerCache.views(time.Now(), "")
		for _, view := range views {
			peer := adminPeerSummary{
				NodeID:              view.NodeID,
				Status:              view.Status,
				ReceivedAt:          view.ReceivedAt,
				IdentityState:       peerIdentityState(state, view),
				AuthorizationState:  "not_configured",
				ConfiguredScopes:    []string{},
				ModelReadinessState: "unknown",
			}
			if state.peer != nil {
				if grant, ok := state.peer.Peers[view.NodeID]; ok {
					peer.ConfiguredScopes = configuredPeerScopes(grant)
					peer.AuthorizationState = peerAuthorizationState(grant, true)
				}
			}
			peers = append(peers, peer)
		}
	}
	return peers
}

// peerIdentityState reports the peer membership identity state from the pairing
// store, defaulting to not_configured or legacy_bearer when not federated.
func peerIdentityState(state *reloaderState, view peerView) string {
	if state.peer == nil {
		return "not_configured"
	}
	if state.peer.FederationID == "" {
		return "legacy_bearer"
	}
	if state.pairingStore == nil {
		return "unavailable"
	}
	member, paired, err := state.pairingStore.LookupMember(view.NodeID)
	switch {
	case err != nil:
		return "unavailable"
	case !paired:
		return "not_paired"
	case member.NodeID != "":
		return "paired"
	}
	return "not_configured"
}

// peerAuthorizationState reports the peer authorization state derived from the
// configured grant scopes.
func peerAuthorizationState(grant config.PeerGrant, ok bool) string {
	if !ok {
		return "not_configured"
	}
	scopes := configuredPeerScopes(grant)
	if len(scopes) == 0 {
		return "no_scopes"
	}
	return "scopes_configured"
}

// fetchRequestSummary queries the local diagnostics summary endpoint and returns
// the parsed summary together with the request-summary state to assign:
// status 503 -> unavailable; status 200 + valid JSON -> available; status 200
// + invalid JSON -> unavailable; every other status -> not_configured.
func fetchRequestSummary(handler *Handler) (*adminRequestSummary, string) {
	internalRequest := httptest.NewRequest(http.MethodGet, "http://internal/diagnostics/v1/summary", nil)
	internalRequest.Header.Set("Authorization", "Bearer "+handler.diagnosticsAPIKey)
	recorder := httptest.NewRecorder()
	handler.serveDiagnosticsAPI(recorder, internalRequest, &routeInfo{kind: routeInfoKindLocal})
	switch recorder.Code {
	case http.StatusServiceUnavailable:
		return nil, "unavailable"
	case http.StatusOK:
		var summary adminRequestSummary
		if json.Unmarshal(recorder.Body.Bytes(), &summary) != nil {
			return nil, "unavailable"
		}
		return &summary, "available"
	default:
		return nil, "not_configured"
	}
}

func configuredPeerScopes(grant config.PeerGrant) []string {
	var scopes []string
	if grant.CatalogRead {
		scopes = append(scopes, "catalog_read")
	}
	if grant.DiagnosticsRead {
		scopes = append(scopes, "diagnostics_read")
	}
	if grant.Inference {
		scopes = append(scopes, "inference")
	}
	if grant.ManageSettings {
		scopes = append(scopes, "manage_settings")
	}
	if len(grant.PathAccess) > 0 {
		scopes = append(scopes, "path_access")
	}
	sort.Strings(scopes)
	return scopes
}

func summarizeFederationCRLs(state *reloaderState, now time.Time) adminFederationPolicySummary {
	policy := adminFederationPolicySummary{State: "fresh", PeerCRLs: []adminPeerCRLSummary{}}
	maxAge := state.peer.RevocationMaxAge()
	policy.ClientCRL = summarizeCRLTime(state.peerClientCRLs, maxAge, now)
	if policy.ClientCRL.State != "fresh" {
		policy.State = policy.ClientCRL.State
	}
	for nodeID, client := range state.peerClients {
		status := summarizeCRLTime(client.serverCRLs, client.crlMaxAge, now)
		policy.PeerCRLs = append(policy.PeerCRLs, adminPeerCRLSummary{NodeID: nodeID, adminCRLTimeSummary: status})
		if status.State != "fresh" && policy.State == "fresh" {
			policy.State = status.State
		}
	}
	sort.Slice(policy.PeerCRLs, func(i, j int) bool { return policy.PeerCRLs[i].NodeID < policy.PeerCRLs[j].NodeID })
	return policy
}

func summarizeCRLTime(crls []*x509.RevocationList, maxAge time.Duration, now time.Time) adminCRLTimeSummary {
	status := federation.CRLPolicyTime(crls, maxAge, now)
	summary := adminCRLTimeSummary{State: status.State, MaxAgeSeconds: int64(status.MaxAge.Seconds())}
	if !status.UpdatedAt.IsZero() {
		updated := status.UpdatedAt.UTC()
		summary.UpdatedAt = &updated
	}
	if !status.ExpiresAt.IsZero() {
		expires := status.ExpiresAt.UTC()
		summary.ExpiresAt = &expires
	}
	return summary
}

func (r *Reloader) serveAdminActivity(w http.ResponseWriter, req *http.Request) {
	if !r.adminAPIGetGuard(w, req) {
		return
	}
	state := r.state.Load()
	if state == nil {
		http.Error(w, "activity data unavailable", http.StatusServiceUnavailable)
		return
	}
	handler, ok := state.handler.(*Handler)
	if !ok {
		http.Error(w, "activity data unavailable", http.StatusServiceUnavailable)
		return
	}
	result := adminActivity{State: "not_configured", Events: []adminActivityEvent{}, Updated: time.Now().UTC()}
	if handler.history == nil {
		writeJSON(r.logger, w, http.StatusOK, result)
		return
	}
	now := time.Now()
	records := handler.history.Query(correlation.QueryFilter{TimeStart: now.Add(-24 * time.Hour), TimeEnd: now, Limit: 50})
	result.State = "available"
	for _, record := range records {
		if record == nil {
			continue
		}
		result.Events = append(result.Events, adminActivityEventFromRecord(record))
	}
	writeJSON(r.logger, w, http.StatusOK, result)
}

// adminActivityEventFromRecord projects a correlation record into a sanitized
// admin activity event. Duration is taken from ProxyObservation.TotalDurationMs
// with a Metadata["duration_ms"] int64 fallback. Private or raw metadata is
// never copied into the output.
func adminActivityEventFromRecord(record *correlation.CorrelationRecord) adminActivityEvent {
	event := adminActivityEvent{
		Time:      record.RequestTime,
		BackendID: record.BackendID,
		Status:    record.Status,
	}
	if record.ProxyObservation != nil {
		event.DurationMS = record.ProxyObservation.TotalDurationMs
	}
	if event.DurationMS == nil {
		if duration, ok := record.Metadata["duration_ms"].(int64); ok {
			value := float64(duration)
			event.DurationMS = &value
		}
	}
	return event
}

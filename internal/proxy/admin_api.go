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

func (r *Reloader) serveAdminOverview(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if req.Method != http.MethodGet || req.URL.RawQuery != "" || req.URL.RawPath != "" {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if _, ok := r.adminAuth.sessionForRequest(req); !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if _, ok := r.adminAuth.RequireRole(req, "view"); !ok {
		http.Error(w, "forbidden", http.StatusForbidden)
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
	ctx, cancel := context.WithTimeout(req.Context(), handler.catalogTimeout)
	defer cancel()
	catalogResponse, _, err := handler.modelCatalog(ctx, make(http.Header))
	if err != nil {
		http.Error(w, "model catalog unavailable", http.StatusServiceUnavailable)
		return
	}
	var envelope struct {
		Data []json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(catalogResponse.Body, &envelope); err != nil {
		http.Error(w, "model catalog unavailable", http.StatusServiceUnavailable)
		return
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
	now := time.Now()
	overview := adminOverview{
		Models: models, Peers: []adminPeerSummary{}, Updated: now.UTC(), RequestSummaryState: "not_configured",
		FederationPolicy: adminFederationPolicySummary{State: "not_configured", PeerCRLs: []adminPeerCRLSummary{}},
	}
	if state.peer != nil {
		overview.NodeID = state.peer.NodeID
		if state.peer.FederationID != "" {
			overview.FederationPolicy = summarizeFederationCRLs(state, now)
		}
	}
	if state.peerCache != nil {
		views, _ := state.peerCache.views(time.Now(), "")
		for _, view := range views {
			peer := adminPeerSummary{
				NodeID: view.NodeID, Status: view.Status, ReceivedAt: view.ReceivedAt,
				IdentityState: "not_configured", AuthorizationState: "not_configured",
				ConfiguredScopes: []string{}, ModelReadinessState: "unknown",
			}
			if state.peer != nil {
				if state.peer.FederationID != "" {
					if state.pairingStore == nil {
						peer.IdentityState = "unavailable"
					} else {
						member, paired, err := state.pairingStore.LookupMember(view.NodeID)
						switch {
						case err != nil:
							peer.IdentityState = "unavailable"
						case !paired:
							peer.IdentityState = "not_paired"
						case member.NodeID != "":
							peer.IdentityState = "paired"
						}
					}
				} else {
					peer.IdentityState = "legacy_bearer"
				}
				if grant, ok := state.peer.Peers[view.NodeID]; ok {
					peer.ConfiguredScopes = configuredPeerScopes(grant)
					if len(peer.ConfiguredScopes) == 0 {
						peer.AuthorizationState = "no_scopes"
					} else {
						peer.AuthorizationState = "scopes_configured"
					}
				}
			}
			overview.Peers = append(overview.Peers, peer)
		}
	}
	if handler.diagnosticsAPIKey != "" {
		internalRequest := httptest.NewRequest(http.MethodGet, "http://internal/diagnostics/v1/summary", nil)
		internalRequest.Header.Set("Authorization", "Bearer "+handler.diagnosticsAPIKey)
		recorder := httptest.NewRecorder()
		handler.serveDiagnosticsAPI(recorder, internalRequest, &routeInfo{kind: routeInfoKindLocal})
		if recorder.Code == http.StatusOK {
			var summary adminRequestSummary
			if json.Unmarshal(recorder.Body.Bytes(), &summary) == nil {
				overview.RequestSummary = &summary
				overview.RequestSummaryState = "available"
			} else {
				overview.RequestSummaryState = "unavailable"
			}
		} else if recorder.Code == http.StatusServiceUnavailable {
			overview.RequestSummaryState = "unavailable"
		}
	}
	writeJSON(r.logger, w, http.StatusOK, overview)
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
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if req.Method != http.MethodGet || req.URL.RawQuery != "" || req.URL.RawPath != "" {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if _, ok := r.adminAuth.sessionForRequest(req); !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if _, ok := r.adminAuth.RequireRole(req, "view"); !ok {
		http.Error(w, "forbidden", http.StatusForbidden)
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
		event := adminActivityEvent{Time: record.RequestTime, BackendID: record.BackendID, Status: record.Status}
		if record.ProxyObservation != nil {
			event.DurationMS = record.ProxyObservation.TotalDurationMs
		}
		if event.DurationMS == nil {
			if duration, ok := record.Metadata["duration_ms"].(int64); ok {
				value := float64(duration)
				event.DurationMS = &value
			}
		}
		result.Events = append(result.Events, event)
	}
	writeJSON(r.logger, w, http.StatusOK, result)
}

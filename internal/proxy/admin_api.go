package proxy

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"sort"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/correlation"
)

type adminModelSummary struct {
	ID      string `json:"id"`
	Object  string `json:"object,omitempty"`
	OwnedBy string `json:"owned_by,omitempty"`
}

type adminPeerSummary struct {
	NodeID     string     `json:"node_id"`
	Status     string     `json:"status"`
	ReceivedAt *time.Time `json:"received_at,omitempty"`
}

type adminRequestSummary struct {
	Count               int            `json:"count"`
	Outcomes            map[string]int `json:"outcomes"`
	GenerationOutcomes  map[string]int `json:"generation_outcomes"`
	DurationMS          map[string]any `json:"duration_ms"`
	DurationSampleCount int            `json:"duration_sample_count"`
}

type adminOverview struct {
	NodeID              string               `json:"node_id,omitempty"`
	Models              []adminModelSummary  `json:"models"`
	Peers               []adminPeerSummary   `json:"peers"`
	RequestSummary      *adminRequestSummary `json:"request_summary,omitempty"`
	RequestSummaryState string               `json:"request_summary_state"`
	Updated             time.Time            `json:"updated_at"`
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
	overview := adminOverview{Models: models, Peers: []adminPeerSummary{}, Updated: time.Now().UTC(), RequestSummaryState: "not_configured"}
	if state.peer != nil {
		overview.NodeID = state.peer.NodeID
	}
	if state.peerCache != nil {
		views, _ := state.peerCache.views(time.Now(), "")
		for _, view := range views {
			overview.Peers = append(overview.Peers, adminPeerSummary{
				NodeID: view.NodeID, Status: view.Status, ReceivedAt: view.ReceivedAt,
			})
		}
	}
	if handler.history != nil {
		now := time.Now()
		records := handler.history.Query(correlation.QueryFilter{
			TimeStart: now.Add(-24 * time.Hour), TimeEnd: now, Limit: maxDiagnosticSummaryLimit,
		})
		overview.RequestSummary = summarizeAdminRequestHistory(records)
		overview.RequestSummaryState = "available"
	}
	writeJSON(r.logger, w, http.StatusOK, overview)
}

func summarizeAdminRequestHistory(records []*correlation.CorrelationRecord) *adminRequestSummary {
	summary := &adminRequestSummary{
		Count: len(records), Outcomes: make(map[string]int), GenerationOutcomes: make(map[string]int),
		DurationMS: map[string]any{"sample_count": 0},
	}
	var durations []float64
	for _, record := range records {
		if record == nil {
			continue
		}
		outcome := record.Status
		if outcome == "" {
			outcome = "unknown"
		}
		summary.Outcomes[outcome]++
		if record.ProxyObservation != nil {
			if record.ProxyObservation.GenerationOutcome != "" {
				summary.GenerationOutcomes[record.ProxyObservation.GenerationOutcome]++
			}
			if record.ProxyObservation.TotalDurationMs != nil {
				durations = append(durations, *record.ProxyObservation.TotalDurationMs)
				continue
			}
		}
		if duration, ok := record.Metadata["duration_ms"].(int64); ok && duration >= 0 {
			durations = append(durations, float64(duration))
		}
	}
	if len(durations) == 0 {
		return summary
	}
	sort.Float64s(durations)
	total := 0.0
	for _, duration := range durations {
		total += duration
	}
	p50 := int(math.Ceil(0.5*float64(len(durations)))) - 1
	if p50 < 0 {
		p50 = 0
	}
	if p50 >= len(durations) {
		p50 = len(durations) - 1
	}
	sampleCount := len(durations)
	summary.DurationMS = map[string]any{
		"mean": total / float64(sampleCount), "p50": durations[p50], "sample_count": sampleCount,
	}
	summary.DurationSampleCount = sampleCount
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

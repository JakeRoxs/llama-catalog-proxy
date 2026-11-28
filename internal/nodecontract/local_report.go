package nodecontract

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/catalog"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/provenance"
)

const maxLocalBackends = 256

type observationHeap []Observation

func (h observationHeap) Len() int { return len(h) }
func (h observationHeap) Less(i, j int) bool {
	a, b := h[i].Identity, h[j].Identity
	if a.BackendID != b.BackendID {
		return a.BackendID > b.BackendID
	}
	return a.LocalID > b.LocalID
}
func (h observationHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *observationHeap) Push(x any)   { *h = append(*h, x.(Observation)) }
func (h *observationHeap) Pop() any {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}

// localReportBuilder accumulates a local report during the backend/cache visit
// phase. Backend visits bind the current backend identity and freshness; model
// visits dedupe identities and maintain the capped top-K observation heap.
type localReportBuilder struct {
	source          Source
	generatedAt     time.Time
	freshnessWindow time.Duration
	selected        map[string]bool

	report            Report
	seenBackends      map[string]bool
	seenModels        map[ModelIdentity]bool
	observations      observationHeap
	availableBackends int
	truncated         bool
	backendID         string
	freshness         string
	fetchedAt         time.Time
}

func newLocalReportBuilder(source Source, generatedAt time.Time, freshnessWindow time.Duration, selected map[string]bool) *localReportBuilder {
	b := &localReportBuilder{
		source:          source,
		generatedAt:     generatedAt,
		freshnessWindow: freshnessWindow,
		selected:        selected,
	}
	b.report = Report{
		ProtocolVersion:  Version{Major: 1, Minor: 0},
		RequiredFeatures: []string{},
		Source:           source,
		GeneratedAt:      generatedAt,
		Capabilities: Capabilities{
			"catalog_read": "enabled", "diagnostics_read": "disabled", "federation_exchange": "disabled",
			"artifact_read": "disabled", "artifact_write": "disabled", "config_write": "disabled",
		},
		Coverage:     "complete",
		Observations: []Observation{},
	}
	b.seenBackends = make(map[string]bool, len(selected))
	b.seenModels = make(map[ModelIdentity]bool)
	b.observations = make(observationHeap, 0, MaxObservations)
	return b
}

// visitBackend is the backend visit callback. It binds the current backend and
// records freshness/staleness evidence without emitting observations.
func (b *localReportBuilder) visitBackend(id string, at time.Time) error {
	b.backendID, b.fetchedAt = id, at
	if !b.selected[id] || b.seenBackends[id] {
		return errors.New("unexpected or duplicate selected backend cache")
	}
	b.seenBackends[id] = true
	if at.IsZero() {
		b.report.Coverage = "partial"
		return nil
	}
	b.availableBackends++
	b.freshness = "fresh"
	if at.After(b.generatedAt) {
		b.freshness = "unknown"
		b.report.Coverage = "partial"
	} else if b.generatedAt.Sub(at) > b.freshnessWindow {
		b.freshness = "stale"
		b.report.Coverage = "partial"
	}
	return nil
}

// visitModel is the model visit callback. It dedupes model identities and
// maintains the capped observation heap.
func (b *localReportBuilder) visitModel(id string, aliases []string) error {
	if b.fetchedAt.IsZero() {
		return nil
	}
	if id == "" || !(provenance.ModelIdentity{
		BackendID: b.backendID, LocalID: id, PublicID: id,
	}).Valid() {
		b.report.Coverage = "partial"
		return nil
	}
	identity := ModelIdentity{OwnerNodeID: b.source.NodeID, BackendID: b.backendID, LocalID: id}
	if b.seenModels[identity] {
		return errors.New("ambiguous duplicate local model identity")
	}
	observedAt := b.fetchedAt
	expiresAt := b.fetchedAt.Add(b.freshnessWindow)
	observation := Observation{
		Identity:   identity,
		State:      "supported",
		ObservedAt: &observedAt,
		ExpiresAt:  &expiresAt,
		Aliases:    append([]string(nil), aliases...),
		Freshness:  b.freshness,
		Coverage:   "unknown",
	}
	if len(b.observations) == MaxObservations {
		b.truncated = true
		largest := b.observations[0].Identity
		if identity.BackendID > largest.BackendID ||
			(identity.BackendID == largest.BackendID && identity.LocalID >= largest.LocalID) {
			return nil
		}
		delete(b.seenModels, heap.Pop(&b.observations).(Observation).Identity)
	}
	b.seenModels[identity] = true
	heap.Push(&b.observations, observation)
	return nil
}

// finalize applies coverage precedence, sorts observations, and emits the report.
func (b *localReportBuilder) finalize() (Report, error) {
	if len(b.seenBackends) != len(b.selected) {
		b.report.Coverage = "partial"
	}
	if len(b.selected) > 0 {
		if b.availableBackends == 0 {
			b.report.Coverage = "unknown"
		} else {
			b.report.Coverage = "partial"
		}
		b.report.Observations = b.observations
	}
	sort.Slice(b.report.Observations, func(i, j int) bool {
		a, b := b.report.Observations[i].Identity, b.report.Observations[j].Identity
		if a.BackendID != b.BackendID {
			return a.BackendID < b.BackendID
		}
		return a.LocalID < b.LocalID
	})
	if b.truncated {
		b.report.Coverage = "truncated"
	}
	return finishLocalReport(b.report)
}

// LocalReport projects explicitly selected backend caches without issuing
// upstream requests. Callers must establish ownership of selected backends.
func LocalReport(source Source, localBackends []string, caches []catalog.BackendModel, generatedAt time.Time, freshnessWindow time.Duration) (Report, error) {
	return localReport(source, localBackends, generatedAt, freshnessWindow, func(selected map[string]bool, visitBackend func(string, time.Time) error, visitModel func(string, []string) error) error {
		for _, backend := range caches {
			if !selected[backend.BackendID] {
				continue
			}
			if err := visitBackend(backend.BackendID, backend.FetchedAt); err != nil {
				return err
			}
			for _, model := range backend.Models {
				if catalog.IsAliasModelRow(model) {
					continue
				}
				id, _ := model["id"].(string)
				if err := visitModel(id, catalog.ModelAliases(model)); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// LocalReportFromCatalog projects selected cache entries without cloning full
// catalogs. Callers must establish ownership and pass the live collector.
func LocalReportFromCatalog(ctx context.Context, source Source, localBackends []string, service *catalog.Service, generatedAt time.Time, freshnessWindow time.Duration) (Report, error) {
	if service == nil {
		return Report{}, errors.New("catalog service is required")
	}
	return localReport(source, localBackends, generatedAt, freshnessWindow, func(selected map[string]bool, backend func(string, time.Time) error, model func(string, []string) error) error {
		return service.VisitCachedModelIDs(ctx, selected, backend, model)
	})
}

func localReport(source Source, localBackends []string, generatedAt time.Time, freshnessWindow time.Duration, visit func(map[string]bool, func(string, time.Time) error, func(string, []string) error) error) (Report, error) {
	if generatedAt.IsZero() || freshnessWindow <= 0 || len(localBackends) > maxLocalBackends {
		return Report{}, errors.New("invalid local report time, freshness window, or backend count")
	}
	selected := make(map[string]bool, len(localBackends))
	for _, id := range localBackends {
		if id == "" || !(provenance.ModelIdentity{BackendID: id, LocalID: "x", PublicID: "x"}).Valid() || selected[id] {
			return Report{}, errors.New("invalid or duplicate local backend")
		}
		selected[id] = true
	}
	b := newLocalReportBuilder(source, generatedAt, freshnessWindow, selected)
	if err := visit(selected, b.visitBackend, b.visitModel); err != nil {
		return Report{}, err
	}
	return b.finalize()
}

// AddBackendReadiness adds authorized, cache-only health-check evidence.
func AddBackendReadiness(report Report, checks []catalog.BackendReadiness, freshnessWindow time.Duration) (Report, error) {
	if freshnessWindow <= 0 || len(checks) > maxLocalBackends {
		return Report{}, errors.New("invalid backend readiness snapshot")
	}
	readiness := make([]BackendReadiness, 0, len(checks))
	for _, check := range checks {
		entry := BackendReadiness{BackendID: check.BackendID, Status: check.Status, Freshness: "unknown"}
		if !check.ObservedAt.IsZero() {
			observedAt := check.ObservedAt
			entry.ObservedAt = &observedAt
			if age := report.GeneratedAt.Sub(observedAt); age >= 0 {
				entry.Freshness = "fresh"
				if age > freshnessWindow {
					entry.Freshness = "stale"
				}
			}
		}
		readiness = append(readiness, entry)
	}
	report.Capabilities["diagnostics_read"] = "enabled"
	report.BackendReadiness = &readiness
	return finishLocalReport(report)
}

func finishLocalReport(report Report) (Report, error) {
	data, err := json.Marshal(report)
	if err != nil {
		return Report{}, err
	}
	if len(data) > MaxReportBytes {
		report.Coverage = "truncated"
		observations := report.Observations
		low, high := 0, len(observations)
		for low < high {
			mid := low + (high-low+1)/2
			report.Observations = observations[:mid]
			data, err = json.Marshal(report)
			if err != nil {
				return Report{}, err
			}
			if len(data) <= MaxReportBytes {
				low = mid
			} else {
				high = mid - 1
			}
		}
		report.Observations = observations[:low]
		data, err = json.Marshal(report)
		if err != nil {
			return Report{}, err
		}
	}
	if _, err := DecodeReport(bytes.NewReader(data), report.Source.NodeID, report.GeneratedAt); err != nil {
		return Report{}, err
	}
	return report, nil
}

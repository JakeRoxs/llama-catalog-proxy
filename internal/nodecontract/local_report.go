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

// LocalReport projects explicitly selected backend caches without issuing
// upstream requests. Callers must establish ownership of selected backends.
func LocalReport(source Source, localBackends []string, caches []catalog.BackendModel, generatedAt time.Time, freshnessWindow time.Duration) (Report, error) {
	return localReport(source, localBackends, generatedAt, freshnessWindow, func(selected map[string]bool, visitBackend func(string, time.Time) error, visitModel func(string) error) error {
		for _, backend := range caches {
			if !selected[backend.BackendID] {
				continue
			}
			if err := visitBackend(backend.BackendID, backend.FetchedAt); err != nil {
				return err
			}
			for _, model := range backend.Models {
				id, _ := model["id"].(string)
				if err := visitModel(id); err != nil {
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
	return localReport(source, localBackends, generatedAt, freshnessWindow, func(selected map[string]bool, backend func(string, time.Time) error, model func(string) error) error {
		return service.VisitCachedModelIDs(ctx, selected, backend, model)
	})
}

func localReport(source Source, localBackends []string, generatedAt time.Time, freshnessWindow time.Duration, visit func(map[string]bool, func(string, time.Time) error, func(string) error) error) (Report, error) {
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

	report := Report{
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
	seenBackends := make(map[string]bool, len(selected))
	seenModels := make(map[ModelIdentity]bool)
	observations := make(observationHeap, 0, MaxObservations)
	availableBackends := 0
	truncated := false
	var backendID, freshness string
	var fetchedAt time.Time
	err := visit(selected, func(id string, at time.Time) error {
		backendID, fetchedAt = id, at
		if !selected[id] || seenBackends[id] {
			return errors.New("unexpected or duplicate selected backend cache")
		}
		seenBackends[id] = true
		if at.IsZero() {
			report.Coverage = "partial"
			return nil
		}
		availableBackends++
		freshness = "fresh"
		if at.After(generatedAt) {
			freshness = "unknown"
			report.Coverage = "partial"
		} else if generatedAt.Sub(at) > freshnessWindow {
			freshness = "stale"
			report.Coverage = "partial"
		}
		return nil
	}, func(id string) error {
		if fetchedAt.IsZero() {
			return nil
		}
		if id == "" || !(provenance.ModelIdentity{
			BackendID: backendID, LocalID: id, PublicID: id,
		}).Valid() {
			report.Coverage = "partial"
			return nil
		}
		identity := ModelIdentity{OwnerNodeID: source.NodeID, BackendID: backendID, LocalID: id}
		if seenModels[identity] {
			return errors.New("ambiguous duplicate local model identity")
		}
		observedAt := fetchedAt
		expiresAt := fetchedAt.Add(freshnessWindow)
		observation := Observation{
			Identity: identity, State: "supported", ObservedAt: &observedAt, ExpiresAt: &expiresAt,
			Freshness: freshness, Coverage: "unknown",
		}
		if len(observations) == MaxObservations {
			truncated = true
			largest := observations[0].Identity
			if identity.BackendID > largest.BackendID ||
				(identity.BackendID == largest.BackendID && identity.LocalID >= largest.LocalID) {
				return nil
			}
			delete(seenModels, heap.Pop(&observations).(Observation).Identity)
		}
		seenModels[identity] = true
		heap.Push(&observations, observation)
		return nil
	})
	if err != nil {
		return Report{}, err
	}
	if len(seenBackends) != len(selected) {
		report.Coverage = "partial"
	}
	if len(selected) > 0 {
		if availableBackends == 0 {
			report.Coverage = "unknown"
		} else {
			// BackendModels does not expose whether catalog rows were discarded.
			report.Coverage = "partial"
		}
		report.Observations = observations
	}
	sort.Slice(report.Observations, func(i, j int) bool {
		a, b := report.Observations[i].Identity, report.Observations[j].Identity
		if a.BackendID != b.BackendID {
			return a.BackendID < b.BackendID
		}
		return a.LocalID < b.LocalID
	})
	if truncated {
		report.Coverage = "truncated"
	}
	return finishLocalReport(report)
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

// Package modeldiagnostics provides unified, per-model diagnostic snapshots
// that combine catalog metadata with backend diagnostics observations. It uses
// cached data only and never issues upstream requests, so it cannot cause model
// loads, inference, or swaps.
package modeldiagnostics

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/correlation"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/diagnostics"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/provenance"
)

// MeasurementState is the finite state of a per-model measurement.
type MeasurementState string

const (
	// MeasurementSupported means the observation is fresh, complete, and
	// attributed to a uniquely catalog_resolved identity.
	MeasurementSupported MeasurementState = "supported"
	// MeasurementPartial means the observation is fresh but coverage or data
	// is incomplete (partial collection, missing metrics, or duplicates).
	MeasurementPartial MeasurementState = "partial"
	// MeasurementStale means the observation is not fresh (backend collection
	// non-supported or the diagnostics observation is stale).
	MeasurementStale MeasurementState = "stale"
	// MeasurementDisabled means the backend collection is disabled.
	MeasurementDisabled MeasurementState = "disabled"
	// MeasurementUnavailable means the backend collection is unavailable.
	MeasurementUnavailable MeasurementState = "unavailable"
	// MeasurementUnsupported means the backend collection is unsupported.
	MeasurementUnsupported MeasurementState = "unsupported"
	// MeasurementUnknown means no attribution or no data is available.
	MeasurementUnknown MeasurementState = "unknown"
	// MeasurementAmbiguous means duplicate or conflicting identities were
	// observed and no winner was picked.
	MeasurementAmbiguous MeasurementState = "ambiguous"
)

// catalogResolvedState is the alias resolution state meaning the reported
// reference maps to a unique canonical catalog reference.
const catalogResolvedState = "catalog_resolved"

// maxAggregateCounter keeps sums of at most 1,000 source rows inside int64.
const maxAggregateCounter int64 = 1_000_000_000_000_000

// validCatalogStatus is the finite set of safe catalog status values. Raw
// strings outside this set and arbitrary objects are omitted from the
// projection.
var validCatalogStatus = map[string]bool{
	"loaded":   true,
	"unloaded": true,
}

// maxIdentityRunes bounds the byte and rune length of every identity
// component (backend ID, local ID, prefix) and of the composed public ID
// (prefix + local ID). It is enforced before any public ID is materialized,
// hashed, or used as a map key.
const maxIdentityRunes = 256

// modelRefPrefix is the opaque bounded model reference prefix. It is applied
// consistently by isModelReference; changing it changes the public ID scheme.
const modelRefPrefix = "model-"

// modelCatalogSource is the source label used in provenance and lifecycle
// records for model catalog observations from the /v1/models endpoint.
const modelCatalogSource = "/v1/models"

// ModelIdentity stably and boundedly identifies a model. The public ID is the
// catalog-visible ID (backend prefix + local ID). Backend ID and local ID are
// the stable internal identifiers; prefix collisions are resolved by the
// catalog layer (default backend wins, then backend ID order).
type ModelIdentity struct {
	PublicID  string `json:"public_id"`
	BackendID string `json:"backend_id"`
	LocalID   string `json:"local_id"`
	Prefix    string `json:"prefix,omitempty"`
}

// Valid reports whether the identity is bounded and well formed. It enforces
// UTF-8, per-component byte/rune bounds, and composition length
// (public ID == prefix + local ID) by delegating to the shared provenance
// validator, so forged or oversized identities are rejected before any public
// ID is materialized or used as a map key.
func (i ModelIdentity) Valid() bool {
	p := provenance.ModelIdentity{
		PublicID:  i.PublicID,
		BackendID: i.BackendID,
		LocalID:   i.LocalID,
		Prefix:    i.Prefix,
	}
	return p.Valid()
}

// validIdentityComponent reports whether value is a bounded, UTF-8 valid
// identifier component (at most 256 bytes and 256 runes). Empty components are
// allowed here; callers that require a non-empty key (backend, activity,
// device IDs) check non-emptiness separately. This mirrors the shared
// provenance validator so bounds are enforced before any public ID is
// materialized or used as a map key.
func validIdentityComponent(value string) bool {
	return utf8.ValidString(value) && len(value) <= maxIdentityRunes && len([]rune(value)) <= maxIdentityRunes
}

// validCatalogIdentityParts checks each component and the composed public ID
// before constructing or indexing that ID.
func validCatalogIdentityParts(backendID, prefix, localID string) bool {
	return backendID != "" && localID != "" && validIdentityComponent(backendID) &&
		validIdentityComponent(prefix) && validIdentityComponent(localID) && len(prefix)+len(localID) <= maxIdentityRunes
}

// ModelCatalogProjection is a safe, typed projection of catalog metadata. It
// intentionally excludes raw metadata (meta, command, desc, URLs, names,
// aliases, unknown keys) to avoid leaking untrusted catalog data.
type ModelCatalogProjection struct {
	// ContextLength is the catalog-advertised context length. Only nonnegative
	// integral values from the model map are reported. The source is catalog
	// (/v1/models) and the value is declared, not verified. Nil when missing,
	// fractional, negative, or non-integral.
	ContextLength *int64 `json:"context_length,omitempty"`
}

// MeasurementSummary summarizes a numeric measurement with mean/p50 and a
// sample denominator. Missing values are never summed as zero.
type MeasurementSummary struct {
	// Mean is the arithmetic mean of known values.
	Mean *float64 `json:"mean,omitempty"`
	// P50 is the 50th percentile of known values (nearest rank).
	P50 *float64 `json:"p50,omitempty"`
	// SampleCount is the number of known values (the denominator).
	SampleCount int `json:"sample_count"`
	// MissingCount is the number of missing values.
	MissingCount int `json:"missing_count"`
}

// MetricSummary is a normalized aggregate of one optional integer metric.
// Missing values are never summed as zero; known and missing counts are
// reported separately.
type MetricSummary struct {
	// Sum is the sum of known values. Nil when no values are known.
	Sum *int64 `json:"sum,omitempty"`
	// KnownCount is the number of known (non-nil) values.
	KnownCount int `json:"known_count"`
	// MissingCount is the number of missing (nil) values.
	MissingCount int `json:"missing_count"`
}

// RateSummary is a normalized aggregate of one optional per-second rate.
type RateSummary struct {
	// Mean is the arithmetic mean of known values.
	Mean *float64 `json:"mean,omitempty"`
	// SampleCount is the number of known values.
	SampleCount int `json:"sample_count"`
	// MissingCount is the number of missing values.
	MissingCount int `json:"missing_count"`
}

// DraftRateSummary is the aggregate draft acceptance rate. Rate is an
// aggregate across observed requests, not a per-position rate. It is computed
// only when the aggregate denominator is positive and every attributed pair
// is known.
type DraftRateSummary struct {
	// AcceptedTokens is the sum of known accepted draft tokens.
	AcceptedTokens *int64 `json:"accepted_tokens,omitempty"`
	// ProposedTokens is the sum of known proposed draft tokens.
	ProposedTokens *int64 `json:"proposed_tokens,omitempty"`
	// KnownPairs is the number of attributed activities for which both draft
	// and accepted draft tokens are known.
	KnownPairs int `json:"known_pairs"`
	// Rate is the aggregate acceptance rate. Nil when the denominator is not
	// positive or any pair is unknown.
	Rate *float64 `json:"rate,omitempty"`
	// State is unknown/conflicting when accepted exceeds proposed.
	State MeasurementState `json:"state"`
}

// ActivityMeasurement is a normalized view of one model's activity metrics,
// aggregated across the activities attributed to that model.
type ActivityMeasurement struct {
	State MeasurementState `json:"state"`
	// Source is the fixed upstream collection path.
	Source string `json:"source,omitempty"`
	// CollectionState is the finite outcome for the backend collection round.
	CollectionState string `json:"collection_state,omitempty"`
	// ObservedAt is the local collector observation time.
	ObservedAt time.Time `json:"observed_at,omitempty"`
	// FirstActivityAt and LastActivityAt are upstream row timestamps, not
	// proxy request-start/completion timestamps.
	FirstActivityAt time.Time `json:"first_activity_at,omitempty"`
	LastActivityAt  time.Time `json:"last_activity_at,omitempty"`
	// Freshness describes whether values are fresh.
	Freshness string `json:"freshness,omitempty"`
	// Truncated is true when the underlying collection was truncated.
	Truncated bool `json:"truncated,omitempty"`
	// RowCount is the number of activity rows attributed to the model.
	RowCount int `json:"row_count"`
	// FailureCount is the number of transport failures (not generation
	// outcomes).
	FailureCount int `json:"failure_count"`
	// DurationMS summarizes duration in milliseconds.
	DurationMS *MeasurementSummary `json:"duration_ms,omitempty"`
	// InputTokens sums known input tokens.
	InputTokens *MetricSummary `json:"input_tokens,omitempty"`
	// OutputTokens sums known output tokens.
	OutputTokens *MetricSummary `json:"output_tokens,omitempty"`
	// CachedTokens sums known cached tokens.
	CachedTokens *MetricSummary `json:"cache_tokens,omitempty"`
	// ReasoningTokens sums known reasoning tokens.
	ReasoningTokens *MetricSummary `json:"reasoning_tokens,omitempty"`
	// DraftTokens sums known proposed draft tokens.
	DraftTokens *MetricSummary `json:"draft_tokens,omitempty"`
	// DraftAcceptedTokens sums known accepted draft tokens.
	DraftAcceptedTokens *MetricSummary `json:"draft_accepted_tokens,omitempty"`
	// DraftRate is the aggregate draft acceptance rate.
	DraftRate *DraftRateSummary `json:"draft_rate,omitempty"`
	// PrefillTokensPerSecond is the arithmetic mean of source-reported
	// prefill rates; SampleCount is the denominator.
	PrefillTokensPerSecond *RateSummary `json:"prefill_tokens_per_second,omitempty"`
	// DecodeTokensPerSecond is the mean of source-reported decode rates;
	// SampleCount is the denominator.
	DecodeTokensPerSecond *RateSummary `json:"decode_tokens_per_second,omitempty"`
}

// SharedGPUWindow reports GPU sample values observed in the shared time window
// of an activity's device associations. Adjacency is not proof of ownership,
// so values are never represented as model-owned.
type SharedGPUWindow struct {
	State                     MeasurementState `json:"state"`
	Source                    string           `json:"source"`
	CollectionState           string           `json:"collection_state"`
	Freshness                 string           `json:"freshness"`
	Reason                    string           `json:"reason,omitempty"`
	SharedDeviceOnly          bool             `json:"shared_device_only"`
	ExecutionDeviceUnverified bool             `json:"execution_device_unverified"`
	ClockAlignmentUnverified  bool             `json:"clock_alignment_unverified"`
	// Window is the association time window.
	WindowStart   time.Time `json:"window_start,omitempty"`
	WindowEnd     time.Time `json:"window_end,omitempty"`
	FirstSampleAt time.Time `json:"first_sample_at,omitempty"`
	LastSampleAt  time.Time `json:"last_sample_at,omitempty"`
	ObservedAt    time.Time `json:"observed_at,omitempty"`
	// SampleCount is the number of matched GPU samples (the denominator).
	SampleCount           int `json:"sample_count"`
	GpuUtilKnownSamples   int `json:"gpu_util_known_samples"`
	GpuUtilMissingSamples int `json:"gpu_util_missing_samples"`
	MemUtilKnownSamples   int `json:"mem_util_known_samples"`
	MemUtilMissingSamples int `json:"mem_util_missing_samples"`
	MemUsedKnownSamples   int `json:"mem_used_known_samples"`
	MemUsedMissingSamples int `json:"mem_used_missing_samples"`
	// MaxGpuUtilPct / MinGpuUtilPct across matched samples.
	MaxGpuUtilPct *float64 `json:"max_gpu_util_pct,omitempty"`
	MinGpuUtilPct *float64 `json:"min_gpu_util_pct,omitempty"`
	// MaxMemUtilPct / MinMemUtilPct across matched samples.
	MaxMemUtilPct *float64 `json:"max_mem_util_pct,omitempty"`
	MinMemUtilPct *float64 `json:"min_mem_util_pct,omitempty"`
	// MaxMemUsedMB / MinMemUsedMB across matched samples.
	MaxMemUsedMB *int `json:"max_mem_used_mb,omitempty"`
	MinMemUsedMB *int `json:"min_mem_used_mb,omitempty"`
}

// ModelLifecycleSnapshot is the current lifecycle snapshot for a model. The
// state at the activity is always unknown; absence never implies unloaded.
type ModelLifecycleSnapshot struct {
	// CurrentState is the observed lifecycle state of the matched model when
	// the identity is uniquely catalog_resolved and a unique current lifecycle
	// row exists; otherwise unknown. It is an observed enum, never a derivation
	// of prior state.
	CurrentState string `json:"current_state,omitempty"`
	// Source identifies the fixed current-state API that supplied the status.
	Source string `json:"source,omitempty"`
	// Observed is true when a unique current lifecycle row matched.
	Observed bool `json:"observed"`
	// ObservedAt points to the matched lifecycle row's ObservedAt.
	ObservedAt *time.Time `json:"observed_at,omitempty"`
	// StateAtActivity is always unknown.
	StateAtActivity string `json:"state_at_activity"`
	// TransitionEvidence is unsupported because the existing adapter's events
	// endpoint exposes readiness snapshots, not timestamped state transitions.
	TransitionEvidence string `json:"transition_evidence"`
	// Kind identifies this as a current metadata snapshot, never a historical join.
	Kind string `json:"kind,omitempty"`
}

// ModelSnapshot is a unified diagnostic view of a single model. It combines
// catalog metadata with the owning backend's diagnostics observations and
// declared vs observed provenance.
type ModelSnapshot struct {
	Identity ModelIdentity `json:"identity"`
	// Catalog safe projection (no raw maps/metadata).
	Catalog *ModelCatalogProjection `json:"catalog,omitempty"`
	// Backend diagnostics observations (legacy view).
	Diagnostics BackendDiagnostics `json:"diagnostics,omitempty"`
	// Per-model activity measurement (new normalized view).
	Activity *ActivityMeasurement `json:"activity,omitempty"`
	// Shared GPU window (never model-owned).
	GPUWindow *SharedGPUWindow `json:"approximate_shared_gpu_window,omitempty"`
	// Current lifecycle snapshot.
	Lifecycle *ModelLifecycleSnapshot `json:"lifecycle,omitempty"`
	// Provenance carries declared vs observed provenance for the model.
	Provenance *Provenance `json:"provenance,omitempty"`
}

// State represents the lifecycle state of a diagnostics observation.
type State string

const (
	Supported    State = "supported"
	Unsupported  State = "unsupported"
	Disabled     State = "disabled"
	Unavailable  State = "unavailable"
	Unauthorized State = "unauthorized"
	Unknown      State = "unknown"
)

// ObservationSummary is a normalized view of a single diagnostics observation.
// Missing, unsupported, disabled, stale, and zero-valued measurements are
// distinguishable through State and Stale; no rates are invented from
// insufficient data.
type ObservationSummary struct {
	State State `json:"state"`
	// ObservedAt is when the observation was last successfully captured.
	ObservedAt time.Time `json:"observed_at,omitempty"`
	// FreshUntil is when the observation is considered fresh.
	FreshUntil time.Time `json:"fresh_until,omitempty"`
	// ExpiresAt is when the observation expires.
	ExpiresAt time.Time `json:"expires_at,omitempty"`
	Reason    string    `json:"reason,omitempty"`
	// Stale is true when the observation is past its freshness window.
	Stale bool          `json:"stale"`
	Build *BuildSummary `json:"build,omitempty"`
}

// BuildSummary is a sanitized build identity from a version observation.
type BuildSummary struct {
	Version string `json:"version,omitempty"`
	Commit  string `json:"commit,omitempty"`
	Date    string `json:"date,omitempty"`
}

// BackendDiagnostics summarizes a backend's diagnostics observations relevant
// to model lifecycle and performance. All fields are optional; absent fields
// mean the backend does not expose that capability.
type BackendDiagnostics struct {
	Version     *ObservationSummary `json:"version,omitempty"`
	Activity    *ObservationSummary `json:"activity,omitempty"`
	Stats       *ObservationSummary `json:"stats,omitempty"`
	Performance *ObservationSummary `json:"performance,omitempty"`
	Hardware    *ObservationSummary `json:"hardware,omitempty"`
}

// Provenance carries per-model provenance from the provenance builder. It
// wraps provenance.FieldProvenance with modeldiagnostics.ModelIdentity.
type Provenance struct {
	Identity ModelIdentity `json:"identity"`
	// Fields is a per-field provenance record keyed by field name. Only
	// fields with evidence from a documented source are recorded.
	Fields map[string]provenance.FieldProvenance `json:"fields,omitempty"`
}

// BackendModel describes the models served by one backend, using cached catalog
// data only. ObservedAt is the time the backend catalog was last successfully
// fetched; it is zero when no successful fetch has occurred.
type BackendModel struct {
	BackendID  string
	Prefix     string
	Models     []map[string]any
	ObservedAt time.Time
}

// SnapshotSources is the bounded set of cached inputs for SnapshotWithSources.
// All inputs are pure cached data; no requests are issued and no hidden
// inferences are made.
type SnapshotSources struct {
	// Catalog models (model records total <= 1000).
	BackendModels []BackendModel
	// Backend diagnostics snapshots (per-backend capabilities).
	BackendSnapshots []*diagnostics.Snapshot
	// Activity records (<= 1000).
	Activities []*correlation.ActivityRecord
	// Resolved activity identities (<= 1000).
	Identities []correlation.ActivityModelIdentity
	// Backend collection states keyed by backend ID (<= 256).
	BackendStates map[string]correlation.BackendState
	// GPU report (samples <= 4096).
	GPUReport *correlation.GPUReport
	// Activity-device associations (<= 8000).
	DeviceAssociations []correlation.ActivityDeviceAssociation
	// Lifecycle report (models <= 1000).
	LifecycleReport *correlation.LifecycleReport
	// Lifecycle associations (<= 8000).
	LifecycleAssociations []correlation.ActivityLifecycleAssociation
}

// Service produces unified model diagnostic snapshots. It is a pure
// function of its inputs: no state, no upstream requests, no background
// polling.
type Service struct{}

// New returns a Service.
func New() *Service {
	return &Service{}
}

// Snapshot produces per-model snapshots combining catalog metadata with
// backend diagnostics observations. It uses cached data only and never
// issues upstream requests. Models without a matching diagnostics snapshot
// still appear with an empty diagnostics block. Provenance is populated
// via the shared provenance.Build API. The catalog is projected to a safe
// typed view (no raw maps/metadata).
func (s *Service) Snapshot(ctx context.Context, backendModels []BackendModel, backendSnapshots []*diagnostics.Snapshot) ([]*ModelSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	snapByBackend := make(map[string]*diagnostics.Snapshot, len(backendSnapshots))
	duplicateBackends := make(map[string]bool)
	snapCounts := make(map[string]int)
	for _, snap := range backendSnapshots {
		if snap == nil || snap.Backend == "" || !validIdentityComponent(snap.Backend) {
			continue
		}
		snapCounts[snap.Backend]++
		if snapCounts[snap.Backend] > 1 {
			duplicateBackends[snap.Backend] = true
		}
		snapByBackend[snap.Backend] = snap
	}

	models := append([]BackendModel(nil), backendModels...)
	for i := range models {
		models[i].Models = append([]map[string]any(nil), models[i].Models...)
		sort.SliceStable(models[i].Models, func(a, b int) bool {
			left, _ := models[i].Models[a]["id"].(string)
			right, _ := models[i].Models[b]["id"].(string)
			return left < right
		})
	}
	sort.SliceStable(models, func(i, j int) bool {
		if models[i].BackendID != models[j].BackendID {
			return models[i].BackendID < models[j].BackendID
		}
		return models[i].Prefix < models[j].Prefix
	})
	publicCounts := make(map[string]int)
	for _, bm := range models {
		for _, model := range bm.Models {
			localID, _ := model["id"].(string)
			if !validCatalogIdentityParts(bm.BackendID, bm.Prefix, localID) {
				continue
			}
			identity := ModelIdentity{PublicID: bm.Prefix + localID, BackendID: bm.BackendID, LocalID: localID, Prefix: bm.Prefix}
			if identity.Valid() {
				publicCounts[identity.PublicID]++
			}
		}
	}

	var result []*ModelSnapshot
	for _, bm := range models {
		diag := summarizeDiagnostics(snapByBackend[bm.BackendID])

		for _, model := range bm.Models {
			localID, _ := model["id"].(string)
			if !validCatalogIdentityParts(bm.BackendID, bm.Prefix, localID) {
				continue
			}
			publicID := bm.Prefix + localID

			// Build provenance via the shared provenance.Build API.
			provIdentity := provenance.ModelIdentity{
				PublicID:  publicID,
				BackendID: bm.BackendID,
				LocalID:   localID,
				Prefix:    bm.Prefix,
			}
			snap := snapByBackend[bm.BackendID]
			if duplicateBackends[bm.BackendID] {
				snap = nil
			}
			prov := provenance.Build(provIdentity, model, snap, bm.ObservedAt)
			if !provIdentity.Valid() {
				continue
			}
			if publicCounts[publicID] > 1 {
				prov.Fields["public_id_resolution"] = provenance.FieldProvenance{Value: "ambiguous_catalog_id", Source: modelCatalogSource, Verification: provenance.Conflicting}
			} else {
				prov.Fields["public_id_resolution"] = provenance.FieldProvenance{Value: "unique_catalog_id", Source: modelCatalogSource, Verification: provenance.Declared}
			}
			modelRef, ok := correlation.ModelReference(bm.BackendID, localID)
			if !ok {
				continue
			}
			lifecycle := buildLifecycle(bm.BackendID, modelRef, map[refKey][]correlation.LifecycleModel{}, map[refKey]bool{}, catalogStatus(model["status"]), bm.ObservedAt)

			result = append(result, &ModelSnapshot{
				Identity: ModelIdentity{
					PublicID:  publicID,
					BackendID: bm.BackendID,
					LocalID:   localID,
					Prefix:    bm.Prefix,
				},
				Catalog:     projectCatalog(model),
				Diagnostics: diag,
				Lifecycle:   lifecycle,
				Provenance: &Provenance{
					Identity: ModelIdentity{
						PublicID:  publicID,
						BackendID: bm.BackendID,
						LocalID:   localID,
						Prefix:    bm.Prefix,
					},
					Fields: prov.Fields,
				},
			})
		}
	}

	return result, nil
}

// SnapshotWithSources produces per-model snapshots combining catalog metadata
// with all cached proxy-collected sources: activity metrics, resolved alias
// identities, backend collection states, GPU samples, device associations,
// and the current lifecycle snapshot. It is a pure function of its inputs: no
// state, no upstream requests, no hidden inferences. Inputs are bounded before
// any allocation; exceeding a bound returns an error.
func (s *Service) SnapshotWithSources(ctx context.Context, backendModels []BackendModel, backendSnapshots []*diagnostics.Snapshot, sources SnapshotSources) ([]*ModelSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Validate input bounds before any allocation.
	if len(sources.Activities) > 1000 {
		return nil, errors.New("too many activities: max 1000")
	}
	if len(sources.Identities) > 1000 {
		return nil, errors.New("too many identities: max 1000")
	}
	if len(sources.BackendStates) > 256 {
		return nil, errors.New("too many backend states: max 256")
	}
	if sources.GPUReport != nil && len(sources.GPUReport.Samples) > 4096 {
		return nil, errors.New("too many GPU samples: max 4096")
	}
	if len(sources.DeviceAssociations) > 8000 {
		return nil, errors.New("too many device associations: max 8000")
	}
	if len(sources.LifecycleAssociations) > 8000 {
		return nil, errors.New("too many lifecycle associations: max 8000")
	}
	if sources.LifecycleReport != nil && len(sources.LifecycleReport.Models) > 1000 {
		return nil, errors.New("too many lifecycle models: max 1000")
	}
	modelCount := 0
	for _, bm := range backendModels {
		modelCount += len(bm.Models)
	}
	if modelCount > 1000 {
		return nil, errors.New("too many model records: max 1000")
	}

	// Validate all untrusted input identifiers and opaque refs before any
	// indexing map is created or any public ID is materialized. Invalid
	// scopes are rejected with a bounded generic error; no raw values are
	// echoed.
	for _, id := range sources.Identities {
		if id.BackendID == "" || !validIdentityComponent(id.BackendID) ||
			id.ActivityID == "" || !validIdentityComponent(id.ActivityID) || id.Epoch <= 0 {
			return nil, errors.New("invalid activity identity key")
		}
		if id.ReportedModelRef != "" && !isModelReference(id.ReportedModelRef) {
			return nil, errors.New("invalid reported model ref")
		}
		if id.CanonicalRef != "" && !isModelReference(id.CanonicalRef) {
			return nil, errors.New("invalid canonical model ref")
		}
	}
	for backendID := range sources.BackendStates {
		if backendID == "" || !validIdentityComponent(backendID) {
			return nil, errors.New("invalid activity backend state key")
		}
	}
	for _, r := range sources.Activities {
		if r == nil {
			return nil, errors.New("nil activity record")
		}
		if r.BackendID == "" || !validIdentityComponent(r.BackendID) ||
			r.ActivityID == "" || !validIdentityComponent(r.ActivityID) || r.Epoch <= 0 || r.Timestamp.IsZero() {
			return nil, errors.New("invalid activity key")
		}
		if !validActivityMetrics(r) {
			return nil, errors.New("invalid activity measurement")
		}
	}
	if sources.GPUReport != nil {
		if len(sources.GPUReport.BackendStates) > 256 {
			return nil, errors.New("too many GPU backend states: max 256")
		}
		for backendID := range sources.GPUReport.BackendStates {
			if backendID == "" || !validIdentityComponent(backendID) {
				return nil, errors.New("invalid GPU backend state key")
			}
		}
		for _, s := range sources.GPUReport.Samples {
			if s.BackendID == "" || !validIdentityComponent(s.BackendID) ||
				s.DeviceID == "" || !validIdentityComponent(s.DeviceID) || s.Epoch <= 0 || s.Timestamp.IsZero() {
				return nil, errors.New("invalid GPU sample key")
			}
			if !isGPUDeviceID(s.DeviceID) {
				return nil, errors.New("invalid GPU device id")
			}
			if !validGPUValue(s) {
				return nil, errors.New("invalid GPU measurement")
			}
		}
	}
	for _, a := range sources.DeviceAssociations {
		if a.BackendID == "" || !validIdentityComponent(a.BackendID) ||
			a.ActivityID == "" || !validIdentityComponent(a.ActivityID) || a.Epoch <= 0 {
			return nil, errors.New("invalid device association key")
		}
		if !a.WindowStart.IsZero() && !a.WindowEnd.IsZero() && a.WindowEnd.Before(a.WindowStart) {
			return nil, errors.New("invalid association window order")
		}
	}
	for _, a := range sources.LifecycleAssociations {
		if a.BackendID == "" || !validIdentityComponent(a.BackendID) ||
			a.ActivityID == "" || !validIdentityComponent(a.ActivityID) || a.Epoch <= 0 {
			return nil, errors.New("invalid lifecycle association key")
		}
	}
	if sources.LifecycleReport != nil {
		for _, m := range sources.LifecycleReport.Models {
			if m.BackendID == "" || !validIdentityComponent(m.BackendID) || m.Epoch <= 0 {
				return nil, errors.New("invalid lifecycle model key")
			}
			if m.ModelRef != "" && !isModelReference(m.ModelRef) {
				return nil, errors.New("invalid lifecycle model ref")
			}
		}
	}

	// Backend diagnostics snapshots by backend. Duplicate snapshots are
	// ambiguous: treat such backends as having no diagnostics.
	snapByBackend := make(map[string]*diagnostics.Snapshot, len(backendSnapshots))
	duplicateBackends := make(map[string]bool)
	snapCounts := make(map[string]int)
	for _, snap := range backendSnapshots {
		snapCounts[snap.Backend]++
		if snapCounts[snap.Backend] > 1 {
			duplicateBackends[snap.Backend] = true
		}
		snapByBackend[snap.Backend] = snap
	}

	// Index identities by (backendID, epoch, activityID). Detect ambiguous
	// duplicates (same key, different identity) -> ambiguous.
	identitiesByKey := make(map[identityKey][]correlation.ActivityModelIdentity, len(sources.Identities))
	identityAmbiguous := make(map[identityKey]bool)
	for i := range sources.Identities {
		id := &sources.Identities[i]
		key := identityKey{id.BackendID, id.Epoch, id.ActivityID}
		if identityAmbiguous[key] {
			continue
		}
		if _, ok := identitiesByKey[key]; ok {
			identityAmbiguous[key] = true
			continue
		}
		identitiesByKey[key] = append(identitiesByKey[key], *id)
	}

	// Index identities by (backendID, canonicalRef) for model attribution.
	identitiesByRef := make(map[refKey][]correlation.ActivityModelIdentity, len(sources.Identities))
	for i := range sources.Identities {
		id := &sources.Identities[i]
		if id.State != catalogResolvedState || id.CanonicalRef == "" {
			continue
		}
		key := refKey{id.BackendID, id.CanonicalRef}
		identitiesByRef[key] = append(identitiesByRef[key], *id)
	}

	// Index activities by (backendID, epoch, activityID). Detect conflicts.
	activitiesByKey := make(map[activityKey]*correlation.ActivityRecord, len(sources.Activities))
	activityConflict := make(map[activityKey]bool)
	for _, r := range sources.Activities {
		if r == nil {
			continue
		}
		key := activityKey{r.BackendID, r.Epoch, r.ActivityID}
		if activityConflict[key] {
			continue
		}
		if existing, ok := activitiesByKey[key]; ok {
			// Replayed source IDs with changed identity or measurement payloads
			// are conflicting, not silently merged into one model aggregate.
			if !reflect.DeepEqual(existing, r) {
				activityConflict[key] = true
				continue
			}
			// Identical replay: keep the first.
			continue
		}
		activitiesByKey[key] = r
	}

	// Index GPU samples by (backendID, epoch, deviceID).
	gpuSamplesByDevice := make(map[gpuSampleKey][]correlation.GPUSample, 4096)
	if sources.GPUReport != nil {
		for _, s := range sources.GPUReport.Samples {
			key := gpuSampleKey{s.BackendID, s.Epoch, s.DeviceID}
			gpuSamplesByDevice[key] = append(gpuSamplesByDevice[key], s)
		}
	}
	for key := range gpuSamplesByDevice {
		sort.SliceStable(gpuSamplesByDevice[key], func(i, j int) bool {
			return gpuSamplesByDevice[key][i].Timestamp.Before(gpuSamplesByDevice[key][j].Timestamp)
		})
	}

	// Index device associations by (backendID, epoch, activityID).
	deviceAssocsByActivity := make(map[activityKey][]correlation.ActivityDeviceAssociation, len(sources.DeviceAssociations))
	for _, a := range sources.DeviceAssociations {
		key := activityKey{a.BackendID, a.Epoch, a.ActivityID}
		deviceAssocsByActivity[key] = append(deviceAssocsByActivity[key], a)
	}

	// Index current lifecycle models by (backendID, modelRef). The existing
	// /running API returns current canonical model snapshots, not transitions.
	lifecycleModelsByKey := make(map[refKey][]correlation.LifecycleModel, 1000)
	lifecycleModelDuplicate := make(map[refKey]bool)
	if sources.LifecycleReport != nil {
		for i := range sources.LifecycleReport.Models {
			m := &sources.LifecycleReport.Models[i]
			key := refKey{m.BackendID, m.ModelRef}
			if lifecycleModelDuplicate[key] {
				continue
			}
			if _, ok := lifecycleModelsByKey[key]; ok {
				lifecycleModelDuplicate[key] = true
				continue
			}
			lifecycleModelsByKey[key] = append(lifecycleModelsByKey[key], *m)
		}
	}

	// Sort and deduplicate catalog models.
	models := append([]BackendModel(nil), backendModels...)
	for i := range models {
		models[i].Models = append([]map[string]any(nil), models[i].Models...)
		sort.SliceStable(models[i].Models, func(a, b int) bool {
			left, _ := models[i].Models[a]["id"].(string)
			right, _ := models[i].Models[b]["id"].(string)
			return left < right
		})
	}
	sort.SliceStable(models, func(i, j int) bool {
		if models[i].BackendID != models[j].BackendID {
			return models[i].BackendID < models[j].BackendID
		}
		return models[i].Prefix < models[j].Prefix
	})

	// Public ID duplication for provenance resolution.
	publicCounts := make(map[string]int)
	for _, bm := range models {
		for _, model := range bm.Models {
			localID, _ := model["id"].(string)
			if localID == "" || !validIdentityComponent(localID) ||
				!validIdentityComponent(bm.BackendID) || !validIdentityComponent(bm.Prefix) {
				continue
			}
			publicID := bm.Prefix + localID
			identity := ModelIdentity{PublicID: publicID, BackendID: bm.BackendID, LocalID: localID, Prefix: bm.Prefix}
			if identity.Valid() {
				publicCounts[publicID]++
			}
		}
	}

	var result []*ModelSnapshot
	for _, bm := range models {
		snap := snapByBackend[bm.BackendID]
		if duplicateBackends[bm.BackendID] {
			snap = nil
		}
		bs := sources.BackendStates[bm.BackendID]

		for _, model := range bm.Models {
			localID, _ := model["id"].(string)
			// Validate identity components before the public ID is
			// materialized (prefix + local ID) so oversized or invalid
			// identifiers are skipped without concatenation.
			if localID == "" || !validIdentityComponent(localID) ||
				!validIdentityComponent(bm.BackendID) || !validIdentityComponent(bm.Prefix) {
				continue
			}
			publicID := bm.Prefix + localID

			identity := ModelIdentity{
				PublicID:  publicID,
				BackendID: bm.BackendID,
				LocalID:   localID,
				Prefix:    bm.Prefix,
			}
			if !identity.Valid() {
				continue
			}

			// Provenance via the shared provenance.Build API.
			provIdentity := provenance.ModelIdentity{
				PublicID:  publicID,
				BackendID: bm.BackendID,
				LocalID:   localID,
				Prefix:    bm.Prefix,
			}
			prov := provenance.Build(provIdentity, model, snap, bm.ObservedAt)
			if !provIdentity.Valid() {
				continue
			}
			if publicCounts[publicID] > 1 {
				prov.Fields["public_id_resolution"] = provenance.FieldProvenance{Value: "ambiguous_catalog_id", Source: modelCatalogSource, Verification: provenance.Conflicting}
			} else {
				prov.Fields["public_id_resolution"] = provenance.FieldProvenance{Value: "unique_catalog_id", Source: modelCatalogSource, Verification: provenance.Declared}
			}

			// Attribute the model to activities whose resolved identity has a
			// catalog_resolved canonical reference equal to
			// ModelReference(backendID, localModelID). No name/modelID or time
			// coincidence is used.
			expectedRef, ok := correlation.ModelReference(bm.BackendID, localID)
			if !ok {
				// Invalid local model ID: skip.
				continue
			}
			matchingIdentities := identitiesByRef[refKey{bm.BackendID, expectedRef}]
			attributed := false
			ambiguous := false
			seenActivity := make(map[activityKey]bool)
			var attributedRecords []*correlation.ActivityRecord
			for _, id := range matchingIdentities {
				if id.State != catalogResolvedState || id.CanonicalRef != expectedRef {
					continue
				}
				if identityAmbiguous[identityKey{id.BackendID, id.Epoch, id.ActivityID}] {
					ambiguous = true
					continue
				}
				key := activityKey{id.BackendID, id.Epoch, id.ActivityID}
				if activityConflict[key] {
					ambiguous = true
					continue
				}
				r, ok := activitiesByKey[key]
				if !ok || r == nil {
					continue
				}
				if id.ReportedModelRef != "" && r.ReportedModelRef != "" && id.ReportedModelRef != r.ReportedModelRef {
					ambiguous = true
					continue
				}
				if r.IdentityConflict {
					ambiguous = true
					continue
				}
				activityIdentity := activityKey{r.BackendID, r.Epoch, r.ActivityID}
				if seenActivity[activityIdentity] {
					continue // dedupe by unique backend/epoch/activity ID
				}
				seenActivity[activityIdentity] = true
				attributedRecords = append(attributedRecords, r)
				attributed = true
			}

			// Activity measurement.
			activity := buildActivityMeasurement(attributedRecords, attributed, ambiguous, bs)

			// Shared GPU window (adjacency is not proof of ownership).
			gpuWindow := buildGPUWindow(attributedRecords, deviceAssocsByActivity, gpuSamplesByDevice, sources.GPUReport)

			// Current lifecycle snapshot.
			lifecycle := buildLifecycle(bm.BackendID, expectedRef, lifecycleModelsByKey, lifecycleModelDuplicate, catalogStatus(model["status"]), bm.ObservedAt)

			result = append(result, &ModelSnapshot{
				Identity:  identity,
				Catalog:   projectCatalog(model),
				Activity:  activity,
				GPUWindow: gpuWindow,
				Lifecycle: lifecycle,
				Provenance: &Provenance{
					Identity: identity,
					Fields:   prov.Fields,
				},
			})
		}
	}

	return result, nil
}

func validActivityMetrics(r *correlation.ActivityRecord) bool {
	for _, value := range []*int64{r.DurationMS, r.InputTokens, r.OutputTokens, r.CachedTokens, r.DraftTokens, r.DraftAcceptedTokens, r.ReasoningTokens} {
		if value != nil && (*value < 0 || *value > maxAggregateCounter) {
			return false
		}
	}
	for _, value := range []*float64{r.PromptTokensPerSecond, r.TokensPerSecond} {
		if value != nil && (math.IsNaN(*value) || math.IsInf(*value, 0) || *value < 0 || *value > 1e9) {
			return false
		}
	}
	return true
}

func validGPUValue(sample correlation.GPUSample) bool {
	for _, value := range []*float64{sample.GpuUtilPct, sample.MemUtilPct} {
		if value != nil && (math.IsNaN(*value) || math.IsInf(*value, 0) || *value < 0 || *value > 100) {
			return false
		}
	}
	for _, value := range []*int{sample.MemUsedMB, sample.MemTotalMB} {
		if value != nil && (*value < 0 || int64(*value) > maxAggregateCounter) {
			return false
		}
	}
	return true
}

// summarizeDiagnostics converts a diagnostics snapshot into a normalized
// backend diagnostics summary. Unknown or missing observations are simply
// omitted; no values are invented.
func summarizeDiagnostics(snap *diagnostics.Snapshot) BackendDiagnostics {
	if snap == nil {
		return BackendDiagnostics{}
	}

	diag := BackendDiagnostics{}
	for name, obs := range snap.Capabilities {
		summary := ObservationSummary{
			State:      State(obs.State),
			ObservedAt: obs.ObservedAt,
			FreshUntil: obs.FreshUntil,
			ExpiresAt:  obs.ExpiresAt,
			Reason:     obs.Reason,
			Stale:      obs.Stale,
		}
		if obs.Build != nil {
			summary.Build = &BuildSummary{
				Version: obs.Build.Version,
				Commit:  obs.Build.Commit,
				Date:    obs.Build.Date,
			}
		}
		switch name {
		case "version":
			diag.Version = &summary
		case "activity":
			diag.Activity = &summary
		case "stats":
			diag.Stats = &summary
		case "performance":
			diag.Performance = &summary
		case "hardware":
			diag.Hardware = &summary
		}
	}
	return diag
}

// projectCatalog produces a safe, typed projection of catalog metadata. Raw
// metadata (meta, command, desc, URLs, names, aliases, unknown keys) is
// excluded.
func projectCatalog(model map[string]any) *ModelCatalogProjection {
	proj := &ModelCatalogProjection{}
	if cl, ok := model["context_length"]; ok {
		if v, ok := contextLengthValue(cl); ok {
			proj.ContextLength = &v
		}
	}
	return proj
}

// contextLengthValue parses a JSON-decoded value into a finite, nonnegative
// integral int64. Fractional floats, NaN/Inf, negatives, and overflowing
// values are invalid.
func contextLengthValue(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		if n >= 0 {
			return int64(n), true
		}
	case int64:
		if n >= 0 {
			return n, true
		}
	case float64:
		if math.IsNaN(n) || math.IsInf(n, 0) || n < 0 {
			return 0, false
		}
		if n != float64(int64(n)) {
			return 0, false // fractional
		}
		if n >= float64(math.MaxInt64) {
			return 0, false
		}
		return int64(n), true
	case json.Number:
		if i, err := n.Int64(); err == nil && i >= 0 {
			return i, true
		}
	}
	return 0, false
}

// catalogStatus returns the finite catalog status for a status value. It
// accepts a top-level string in the finite status set or a nested object with
// a finite "value" string. Raw strings outside the set and arbitrary objects
// yield an empty string (omitted).
func catalogStatus(v any) string {
	switch x := v.(type) {
	case string:
		if validCatalogStatus[x] {
			return x
		}
	case map[string]any:
		if val, ok := x["value"]; ok {
			if s, ok := val.(string); ok && validCatalogStatus[s] {
				return s
			}
		}
	}
	return ""
}

// activityAggregates holds the accumulated metrics of a set of activity
// records. Missing values are never summed as zero.
type activityAggregates struct {
	rowCount          int
	failureCount      int
	durationValues    []float64
	durationMissing   int
	inputSum          *int64
	inputKnown        int
	inputMissing      int
	outputSum         *int64
	outputKnown       int
	outputMissing     int
	cacheSum          *int64
	cacheKnown        int
	cacheMissing      int
	reasoningSum      *int64
	reasoningKnown    int
	reasoningMissing  int
	draftSum          *int64
	draftKnown        int
	draftMissing      int
	draftAccSum       *int64
	draftAccKnown     int
	draftAccMissing   int
	promptRateValues  []float64
	promptRateKnown   int
	promptRateMissing int
	tokensRateValues  []float64
	tokensRateKnown   int
	tokensRateMissing int
}

func addInt64(base *int64, v int64) *int64 {
	if base == nil {
		base = &v
	} else {
		*base += v
	}
	return base
}

// aggregateActivityRecords accumulates metrics from a set of activity records.
func aggregateActivityRecords(records []*correlation.ActivityRecord) activityAggregates {
	var agg activityAggregates
	agg.rowCount = len(records)
	for _, r := range records {
		if isTransportFailure(r.Status) {
			agg.failureCount++
		}
		if r.DurationMS != nil {
			agg.durationValues = append(agg.durationValues, float64(*r.DurationMS))
		} else {
			agg.durationMissing++
		}
		if r.InputTokens != nil {
			agg.inputSum = addInt64(agg.inputSum, *r.InputTokens)
			agg.inputKnown++
		} else {
			agg.inputMissing++
		}
		if r.OutputTokens != nil {
			agg.outputSum = addInt64(agg.outputSum, *r.OutputTokens)
			agg.outputKnown++
		} else {
			agg.outputMissing++
		}
		if r.CachedTokens != nil {
			agg.cacheSum = addInt64(agg.cacheSum, *r.CachedTokens)
			agg.cacheKnown++
		} else {
			agg.cacheMissing++
		}
		if r.ReasoningTokens != nil {
			agg.reasoningSum = addInt64(agg.reasoningSum, *r.ReasoningTokens)
			agg.reasoningKnown++
		} else {
			agg.reasoningMissing++
		}
		if r.DraftTokens != nil {
			agg.draftSum = addInt64(agg.draftSum, *r.DraftTokens)
			agg.draftKnown++
		} else {
			agg.draftMissing++
		}
		if r.DraftAcceptedTokens != nil {
			agg.draftAccSum = addInt64(agg.draftAccSum, *r.DraftAcceptedTokens)
			agg.draftAccKnown++
		} else {
			agg.draftAccMissing++
		}
		if r.PromptTokensPerSecond != nil {
			agg.promptRateValues = append(agg.promptRateValues, *r.PromptTokensPerSecond)
			agg.promptRateKnown++
		} else {
			agg.promptRateMissing++
		}
		if r.TokensPerSecond != nil {
			agg.tokensRateValues = append(agg.tokensRateValues, *r.TokensPerSecond)
			agg.tokensRateKnown++
		} else {
			agg.tokensRateMissing++
		}
	}
	return agg
}

func isTransportFailure(status string) bool {
	switch status {
	case "http_client_error", "http_server_error", "http_other", "transport_error":
		return true
	default:
		return false
	}
}

func meanFloat(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var sum float64
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func p50Float(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	n := len(sorted)
	idx := int(math.Ceil(0.5*float64(n))) - 1
	if idx < 0 {
		idx = 0
	}
	return sorted[idx]
}

// metricSummary builds a MetricSummary from a sum and known/missing counts.
func metricSummary(sum *int64, known, missing int) *MetricSummary {
	return &MetricSummary{
		Sum:          sum,
		KnownCount:   known,
		MissingCount: missing,
	}
}

// buildActivityMeasurement builds an ActivityMeasurement from aggregated
// activity records and the backend collection state.
func buildActivityMeasurement(records []*correlation.ActivityRecord, attributed bool, ambiguous bool, bs correlation.BackendState) *ActivityMeasurement {
	agg := aggregateActivityRecords(records)

	// Determine freshness and truncated from the backend collection state.
	freshness := "not_fresh"
	truncated := bs.Truncated
	switch bs.State {
	case "collected":
		freshness = "fresh"
	case "unknown":
		freshness = "not_fresh"
	case "unavailable":
		freshness = "not_fresh"
	case "unsupported":
		freshness = "not_fresh"
	case "disabled":
		freshness = "not_fresh"
	case "unauthorized":
		freshness = "not_fresh"
	}

	state, freshness := measurementState(attributed, ambiguous, agg.rowCount, bs, freshness)

	m := &ActivityMeasurement{
		State:           state,
		Source:          "/api/metrics/activity",
		CollectionState: finiteActivityCollectionState(bs.State),
		ObservedAt:      bs.ObservedAt,
		Freshness:       freshness,
		Truncated:       truncated,
		RowCount:        agg.rowCount,
		FailureCount:    agg.failureCount,
	}
	for _, r := range records {
		if r.Timestamp.IsZero() {
			continue
		}
		if m.FirstActivityAt.IsZero() || r.Timestamp.Before(m.FirstActivityAt) {
			m.FirstActivityAt = r.Timestamp
		}
		if m.LastActivityAt.IsZero() || r.Timestamp.After(m.LastActivityAt) {
			m.LastActivityAt = r.Timestamp
		}
	}

	// Duration MS: mean/p50 with sample denominator.
	if len(agg.durationValues) > 0 {
		mn := meanFloat(agg.durationValues)
		p50 := p50Float(agg.durationValues)
		m.DurationMS = &MeasurementSummary{
			Mean:         &mn,
			P50:          &p50,
			SampleCount:  len(agg.durationValues),
			MissingCount: agg.durationMissing,
		}
	} else {
		m.DurationMS = &MeasurementSummary{
			SampleCount:  0,
			MissingCount: agg.durationMissing,
		}
	}

	// Integer metric sums (missing never summed as zero).
	m.InputTokens = metricSummary(agg.inputSum, agg.inputKnown, agg.inputMissing)
	m.OutputTokens = metricSummary(agg.outputSum, agg.outputKnown, agg.outputMissing)
	m.CachedTokens = metricSummary(agg.cacheSum, agg.cacheKnown, agg.cacheMissing)
	m.ReasoningTokens = metricSummary(agg.reasoningSum, agg.reasoningKnown, agg.reasoningMissing)
	m.DraftTokens = metricSummary(agg.draftSum, agg.draftKnown, agg.draftMissing)
	m.DraftAcceptedTokens = metricSummary(agg.draftAccSum, agg.draftAccKnown, agg.draftAccMissing)

	// Draft rate: aggregate across observed requests. Rate only when the
	// aggregate denominator (proposed) is positive and every attributed pair
	// is known (knownPairs == rowCount). When accepted exceeds proposed, the
	// state is unknown/conflicting.
	m.DraftRate = buildDraftRate(agg.draftAccSum, agg.draftAccKnown, agg.draftSum, agg.draftKnown, agg.rowCount)

	// Throughput rates: mean with sample counts.
	if agg.promptRateKnown > 0 {
		mn := meanFloat(agg.promptRateValues)
		m.PrefillTokensPerSecond = &RateSummary{
			Mean:         &mn,
			SampleCount:  agg.promptRateKnown,
			MissingCount: agg.promptRateMissing,
		}
	} else {
		m.PrefillTokensPerSecond = &RateSummary{
			SampleCount:  0,
			MissingCount: agg.promptRateMissing,
		}
	}
	if agg.tokensRateKnown > 0 {
		mn := meanFloat(agg.tokensRateValues)
		m.DecodeTokensPerSecond = &RateSummary{
			Mean:         &mn,
			SampleCount:  agg.tokensRateKnown,
			MissingCount: agg.tokensRateMissing,
		}
	} else {
		m.DecodeTokensPerSecond = &RateSummary{
			SampleCount:  0,
			MissingCount: agg.tokensRateMissing,
		}
	}
	// Backend collection can be fresh while individual row metrics remain
	// unknown (the upstream activity schema has no Usage.Found bit). Keep
	// freshness separate, but mark the per-model metric aggregate partial.
	if m.State == MeasurementSupported && activityHasMissingMetricValues(m) {
		m.State = MeasurementPartial
	}

	return m
}

func activityHasMissingMetricValues(m *ActivityMeasurement) bool {
	if m == nil {
		return false
	}
	metrics := []*MetricSummary{m.InputTokens, m.OutputTokens, m.CachedTokens, m.ReasoningTokens, m.DraftTokens, m.DraftAcceptedTokens}
	for _, metric := range metrics {
		if metric != nil && metric.MissingCount > 0 {
			return true
		}
	}
	if m.DurationMS != nil && m.DurationMS.MissingCount > 0 {
		return true
	}
	if m.PrefillTokensPerSecond != nil && m.PrefillTokensPerSecond.MissingCount > 0 {
		return true
	}
	if m.DecodeTokensPerSecond != nil && m.DecodeTokensPerSecond.MissingCount > 0 {
		return true
	}
	return false
}

// buildDraftRate builds the aggregate draft acceptance rate. Rate is computed
// only when the aggregate denominator (proposed) is positive and every
// attributed pair is known (knownPairs == rowCount). When accepted exceeds
// proposed, the state is unknown/conflicting.
func buildDraftRate(acceptedSum *int64, acceptedKnown int, proposedSum *int64, proposedKnown int, rowCount int) *DraftRateSummary {
	if rowCount == 0 {
		return &DraftRateSummary{State: MeasurementUnknown}
	}
	// No draft tokens known at all: nothing to report.
	if acceptedSum == nil && proposedSum == nil {
		return &DraftRateSummary{KnownPairs: 0, State: MeasurementUnknown}
	}
	rate := &DraftRateSummary{
		AcceptedTokens: acceptedSum,
		ProposedTokens: proposedSum,
		KnownPairs:     0,
		State:          MeasurementUnknown,
	}
	// Every attributed pair must be known for the rate to be computed.
	knownPairs := 0
	if acceptedKnown == rowCount && proposedKnown == rowCount && rowCount > 0 {
		knownPairs = rowCount
	}
	rate.KnownPairs = knownPairs
	// Rate only when the denominator is positive and every pair is known.
	if proposedSum != nil && *proposedSum > 0 && knownPairs == rowCount {
		if acceptedSum != nil {
			v := float64(*acceptedSum) / float64(*proposedSum)
			rate.Rate = &v
		}
		// numerator > denominator => unknown/conflicting.
		if acceptedSum != nil && proposedSum != nil && *acceptedSum > *proposedSum {
			rate.State = MeasurementUnknown
			rate.Rate = nil // unreliable when conflicting
		} else {
			rate.State = MeasurementSupported
		}
	} else {
		rate.State = MeasurementUnknown
		rate.Rate = nil
	}
	return rate
}

// measurementState maps attribution, ambiguity, row count, and backend state
// to a measurement state and freshness.
func measurementState(attributed bool, ambiguous bool, rowCount int, bs correlation.BackendState, freshness string) (MeasurementState, string) {
	if ambiguous {
		return MeasurementAmbiguous, freshness
	}
	if !attributed {
		return MeasurementUnknown, freshness
	}
	if rowCount == 0 {
		return MeasurementUnknown, freshness
	}
	// Backend non-collected: values are not fresh.
	switch bs.State {
	case "collected":
		// freshness already "fresh"; staleness from coverage below.
	case "unavailable":
		return MeasurementUnavailable, "not_fresh"
	case "unsupported":
		return MeasurementUnsupported, "not_fresh"
	case "disabled":
		return MeasurementDisabled, "not_fresh"
	case "unknown":
		return MeasurementUnknown, "not_fresh"
	case "unauthorized":
		return MeasurementUnavailable, "not_fresh"
	default:
		return MeasurementUnknown, "not_fresh"
	}
	// Backend collected. Coverage informs the state. Only a complete
	// coverage supports the observation; partial/truncated yields partial,
	// and unknown/empty coverage yields unknown (never supported).
	switch bs.Coverage {
	case "complete":
		return MeasurementSupported, freshness
	case "partial", "truncated":
		freshness = "stale"
		return MeasurementPartial, freshness
	default:
		// Unknown or empty coverage: the observation cannot be supported.
		return MeasurementUnknown, "not_fresh"
	}
}

// buildGPUWindow builds the shared GPU window from activity device
// associations. Adjacency is not proof of ownership, so values are reported
// separately under approximate_shared_gpu_window with uncertainty flags and
// are never represented as model-owned.
func buildGPUWindow(records []*correlation.ActivityRecord, deviceAssocsByActivity map[activityKey][]correlation.ActivityDeviceAssociation, gpuSamplesByDevice map[gpuSampleKey][]correlation.GPUSample, report *correlation.GPUReport) *SharedGPUWindow {
	var matched []correlation.GPUSample
	// sampleKey bounds a deduplication key: backend + epoch + device + source
	// timestamp. Overlapping associations that reference the same sample are
	// counted once.
	type sampleKey struct {
		backendID string
		epoch     int64
		deviceID  string
		// UTC strips the monotonic clock reading so equal wall-clock source
		// timestamps dedupe regardless of collection order.
		timestamp time.Time
	}
	seen := make(map[sampleKey]struct{})
	var windowStart, windowEnd time.Time
	hasWindow := false
	backendID := ""
	for _, r := range records {
		backendID = r.BackendID
		key := activityKey{r.BackendID, r.Epoch, r.ActivityID}
		for _, assoc := range deviceAssocsByActivity[key] {
			if assoc.Kind != "approximate_time_window" {
				continue
			}
			if !hasWindow {
				windowStart = assoc.WindowStart
				windowEnd = assoc.WindowEnd
				hasWindow = true
			} else {
				if assoc.WindowStart.Before(windowStart) {
					windowStart = assoc.WindowStart
				}
				if assoc.WindowEnd.After(windowEnd) {
					windowEnd = assoc.WindowEnd
				}
			}
			// Join actual samples within the association's explicit window.
			skey := gpuSampleKey{assoc.BackendID, assoc.Epoch, assoc.DeviceID}
			for _, s := range gpuSamplesByDevice[skey] {
				if !s.Timestamp.Before(assoc.WindowStart) && !s.Timestamp.After(assoc.WindowEnd) {
					key := sampleKey{s.BackendID, s.Epoch, s.DeviceID, s.Timestamp.UTC()}
					if _, exists := seen[key]; exists {
						continue // duplicate sample: count once
					}
					seen[key] = struct{}{}
					matched = append(matched, s)
				}
			}
		}
	}
	if !hasWindow {
		return nil
	}
	backendState := correlation.GPUBackendState{State: gpuUnknownBackendState, Coverage: "unknown"}
	globalTruncated := false
	if report != nil {
		if state, ok := report.BackendStates[backendID]; ok {
			backendState = state
		}
		globalTruncated = report.Truncated || report.Coverage == "truncated"
	}
	measurementState := gpuMeasurementState(backendState, globalTruncated)
	freshness := "not_fresh"
	reason := ""
	if len(matched) > 0 && backendState.State == "enabled" && backendState.Coverage == "complete" && !globalTruncated {
		// The source supplies sample timestamps but no maximum sample age.
		// Show collection and source time separately; don't claim fresh samples.
		freshness = "source_timestamp_only"
	} else if len(matched) == 0 && measurementState == MeasurementSupported {
		measurementState = MeasurementUnknown
		reason = "no_matching_samples"
	}
	w := &SharedGPUWindow{
		State:                     measurementState,
		Source:                    "/api/performance",
		CollectionState:           finiteGPUCollectionState(backendState.State),
		Freshness:                 freshness,
		Reason:                    reason,
		SharedDeviceOnly:          true,
		ExecutionDeviceUnverified: true,
		ClockAlignmentUnverified:  true,
		WindowStart:               windowStart,
		WindowEnd:                 windowEnd,
		SampleCount:               len(matched),
	}
	if !backendState.ObservedAt.IsZero() {
		w.ObservedAt = backendState.ObservedAt
	}
	var maxGpu, minGpu *float64
	var maxMem, minMem *float64
	var maxMemUsed, minMemUsed *int
	for _, s := range matched {
		if s.Timestamp.IsZero() {
			continue
		}
		if w.FirstSampleAt.IsZero() || s.Timestamp.Before(w.FirstSampleAt) {
			w.FirstSampleAt = s.Timestamp
		}
		if w.LastSampleAt.IsZero() || s.Timestamp.After(w.LastSampleAt) {
			w.LastSampleAt = s.Timestamp
		}
		// ObservedAt is the local collector observation time (set from the
		// backend state above); it is deliberately separate from the source
		// sample timestamps and is not derived from samples.
		if s.GpuUtilPct != nil {
			w.GpuUtilKnownSamples++
			if maxGpu == nil || *s.GpuUtilPct > *maxGpu {
				maxGpu = s.GpuUtilPct
			}
			if minGpu == nil || *s.GpuUtilPct < *minGpu {
				minGpu = s.GpuUtilPct
			}
		} else {
			w.GpuUtilMissingSamples++
		}
		if s.MemUtilPct != nil {
			w.MemUtilKnownSamples++
			if maxMem == nil || *s.MemUtilPct > *maxMem {
				maxMem = s.MemUtilPct
			}
			if minMem == nil || *s.MemUtilPct < *minMem {
				minMem = s.MemUtilPct
			}
		} else {
			w.MemUtilMissingSamples++
		}
		if s.MemUsedMB != nil {
			w.MemUsedKnownSamples++
			if maxMemUsed == nil || *s.MemUsedMB > *maxMemUsed {
				maxMemUsed = s.MemUsedMB
			}
			if minMemUsed == nil || *s.MemUsedMB < *minMemUsed {
				minMemUsed = s.MemUsedMB
			}
		} else {
			w.MemUsedMissingSamples++
		}
	}
	w.MaxGpuUtilPct = maxGpu
	w.MinGpuUtilPct = minGpu
	w.MaxMemUtilPct = maxMem
	w.MinMemUtilPct = minMem
	w.MaxMemUsedMB = maxMemUsed
	w.MinMemUsedMB = minMemUsed
	return w
}

const gpuUnknownBackendState = "unknown"

// finiteActivityCollectionState returns the finite activity backend collection
// state. Arbitrary supplied strings are normalized to "unknown" so the output
// never echoes raw backend state values.
func finiteActivityCollectionState(state string) string {
	switch state {
	case "collected", "disabled", "unsupported", "unavailable", "unauthorized", "unknown":
		return state
	default:
		return "unknown"
	}
}

func finiteGPUCollectionState(state string) string {
	switch state {
	case "enabled", "disabled", "unsupported", "unauthorized", "unavailable", "unknown":
		return state
	default:
		return "unknown"
	}
}

func gpuMeasurementState(backend correlation.GPUBackendState, globallyTruncated bool) MeasurementState {
	switch backend.State {
	case "disabled":
		return MeasurementDisabled
	case "unsupported":
		return MeasurementUnsupported
	case "unavailable", "unauthorized":
		return MeasurementUnavailable
	case "unknown", "":
		return MeasurementUnknown
	case "enabled":
		if globallyTruncated || backend.Truncated || backend.Coverage != "complete" {
			return MeasurementPartial
		}
		return MeasurementSupported
	default:
		return MeasurementUnknown
	}
}

// buildLifecycle builds the current lifecycle snapshot for a model from the
// exact configured-backend/canonical-model reference. It never links current
// state to an activity's historical time; transition evidence is unsupported.
func buildLifecycle(backendID, canonicalRef string, lifecycleModelsByKey map[refKey][]correlation.LifecycleModel, duplicates map[refKey]bool, catalogState string, catalogObservedAt time.Time) *ModelLifecycleSnapshot {
	lifecycle := &ModelLifecycleSnapshot{
		CurrentState:       "unknown",
		StateAtActivity:    "unknown",
		TransitionEvidence: "unsupported",
		Kind:               "current_metadata_only",
	}
	if !isModelReference(canonicalRef) {
		return lifecycle
	}
	key := refKey{backendID, canonicalRef}
	models := lifecycleModelsByKey[key]
	if duplicates[key] {
		return lifecycle
	}
	if len(models) != 1 {
		// /v1/models explicitly reports loaded/unloaded from its current
		// RunningModels snapshot. Use that observation only when its cached
		// catalog timestamp exists; never infer unloaded from omission.
		if (catalogState == "loaded" || catalogState == "unloaded") && !catalogObservedAt.IsZero() {
			lifecycle.CurrentState = catalogState
			lifecycle.Source = modelCatalogSource
			lifecycle.Observed = true
			copied := catalogObservedAt
			lifecycle.ObservedAt = &copied
		}
		return lifecycle
	}
	m := models[0]
	switch m.State {
	case "ready", "starting", "stopping":
		lifecycle.CurrentState = m.State
	default:
		return lifecycle
	}
	lifecycle.Observed = true
	lifecycle.Source = "/running"
	if !m.ObservedAt.IsZero() {
		copied := m.ObservedAt
		lifecycle.ObservedAt = &copied
	}
	return lifecycle
}

// Package-level key types for bounded lookups.

type identityKey struct {
	backendID  string
	epoch      int64
	activityID string
}

type refKey struct {
	backendID    string
	canonicalRef string
}

type activityKey struct {
	backendID  string
	epoch      int64
	activityID string
}

type gpuSampleKey struct {
	backendID string
	epoch     int64
	deviceID  string
}

// isModelReference reports whether ref is a bounded opaque model reference:
// the modelRefPrefix prefix followed by exactly 32 lowercase hex digits.
func isModelReference(ref string) bool {
	if len(ref) != len(modelRefPrefix)+32 || !strings.HasPrefix(ref, modelRefPrefix) {
		return false
	}
	for _, c := range ref[len(modelRefPrefix):] {
		if !(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// isGPUDeviceID reports whether id is a canonical opaque 'gpu-' device
// identifier (the raw name, UUID, driver, and PCI address are never used).
func isGPUDeviceID(id string) bool {
	if !strings.HasPrefix(id, "gpu-") {
		return false
	}
	rest := id[4:]
	if len(rest) == 0 || len(rest) > 64 {
		return false
	}
	for _, c := range rest {
		if !(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

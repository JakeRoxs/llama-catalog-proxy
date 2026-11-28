package modeldiagnostics

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/correlation"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/diagnostics"
)

const (
	fixtureTimeSec = 1794374400 // 2026-11-11T10:00:00Z
	epoch1         = int64(1)
)

var fixtureTime = time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC)

func ptr64(v int64) *int64      { return &v }
func ptrF64(v float64) *float64 { return &v }
func ptrInt(v int) *int         { return &v }

// identityFor builds a catalog_resolved identity whose canonical ref equals
// ModelReference(backendID, localModelID).
func identityFor(backendID, localID string, epoch int64, activityID string) correlation.ActivityModelIdentity {
	ref, _ := correlation.ModelReference(backendID, localID)
	return correlation.ActivityModelIdentity{
		BackendID:        backendID,
		Epoch:            epoch,
		ActivityID:       activityID,
		ReportedModelRef: ref,
		CanonicalRef:     ref,
		State:            catalogResolvedState,
		Source:           "catalog_metadata",
	}
}

// activityMetrics groups the five optional integer metrics of an activity
// record. Each field is nil when the metric is missing; the -1 sentinel is
// represented as nil (missing).
type activityMetrics struct {
	DurationMS          *int64
	InputTokens         *int64
	OutputTokens        *int64
	DraftTokens         *int64
	DraftAcceptedTokens *int64
}

// activityFor builds an activity record with the given transport status and
// optional integer metrics grouped in activityMetrics. -1 sentinel is
// represented as nil (missing).
func activityFor(backendID string, epoch int64, activityID, status string, metrics activityMetrics) *correlation.ActivityRecord {
	return &correlation.ActivityRecord{
		BackendID:           backendID,
		Epoch:               epoch,
		ActivityID:          activityID,
		Timestamp:           fixtureTime,
		Status:              status,
		DurationMS:          metrics.DurationMS,
		InputTokens:         metrics.InputTokens,
		OutputTokens:        metrics.OutputTokens,
		DraftTokens:         metrics.DraftTokens,
		DraftAcceptedTokens: metrics.DraftAcceptedTokens,
	}
}

// backendState returns a backend collection state.
func backendState(state, coverage string, truncated bool, observedAt time.Time) correlation.BackendState {
	return correlation.BackendState{
		State:      state,
		ObservedAt: observedAt,
		Coverage:   coverage,
		Truncated:  truncated,
	}
}

// gpuSample returns a GPU sample.
func gpuSample(backendID string, epoch int64, deviceID string, ts time.Time,
	gpuUtil, memUtil *float64, memUsed *int) correlation.GPUSample {
	return correlation.GPUSample{
		BackendID:  backendID,
		Epoch:      epoch,
		DeviceID:   deviceID,
		Timestamp:  ts,
		GpuUtilPct: gpuUtil,
		MemUtilPct: memUtil,
		MemUsedMB:  memUsed,
	}
}

// deviceAssoc returns a device association within the given window.
func deviceAssoc(backendID string, epoch int64, activityID, deviceID string,
	windowStart, windowEnd time.Time) correlation.ActivityDeviceAssociation {
	return correlation.ActivityDeviceAssociation{
		Kind:               "approximate_time_window",
		BackendID:          backendID,
		Epoch:              epoch,
		ActivityID:         activityID,
		DeviceID:           deviceID,
		WindowStart:        windowStart,
		WindowEnd:          windowEnd,
		MatchedSampleCount: 1,
	}
}

// lifecycleModel returns a current lifecycle model row.
func lifecycleModel(backendID string, epoch int64, modelRef, state string, observedAt time.Time) correlation.LifecycleModel {
	return correlation.LifecycleModel{
		BackendID:  backendID,
		Epoch:      epoch,
		ModelRef:   modelRef,
		State:      state,
		ObservedAt: observedAt,
	}
}

// lifecycleAssoc returns a lifecycle association.
func lifecycleAssoc(backendID string, epoch int64, activityID, kind string, matchedCount int) correlation.ActivityLifecycleAssociation {
	return correlation.ActivityLifecycleAssociation{
		Kind:              kind,
		BackendID:         backendID,
		Epoch:             epoch,
		ActivityID:        activityID,
		MatchedEventCount: matchedCount,
	}
}

func newDiagnosticsSnapshot(backendID string, capabilities map[string]diagnostics.Observation) *diagnostics.Snapshot {
	return &diagnostics.Snapshot{
		Backend:      backendID,
		Adapter:      "llama_swap",
		GeneratedAt:  fixtureTime,
		Capabilities: capabilities,
	}
}

func observation(state diagnostics.State, observedAt time.Time, stale bool, reason string) diagnostics.Observation {
	return diagnostics.Observation{
		State:         state,
		Source:        "/api/version",
		ObservedAt:    observedAt,
		FreshUntil:    observedAt.Add(30 * time.Second),
		ExpiresAt:     observedAt.Add(5 * time.Minute),
		LastAttemptAt: observedAt,
		Reason:        reason,
		Stale:         stale,
	}
}

// basicSources returns a minimal source set for one backend with one model.
func basicSources() ([]BackendModel, []*diagnostics.Snapshot, SnapshotSources) {
	backendModels := []BackendModel{
		{BackendID: "default", Prefix: "", Models: []map[string]any{{"id": "local-model"}}},
	}
	snap := newDiagnosticsSnapshot("default", map[string]diagnostics.Observation{})
	sources := SnapshotSources{
		BackendModels:    backendModels,
		BackendSnapshots: []*diagnostics.Snapshot{snap},
		BackendStates: map[string]correlation.BackendState{
			"default": backendState("collected", "complete", false, fixtureTime),
		},
	}
	return backendModels, []*diagnostics.Snapshot{snap}, sources
}

// assertSnapshotIdentityAndCatalog verifies the model identity and catalog
// projection for the basic attribution test. The catalog projection must be
// present with a nil context length (not advertised).
func assertSnapshotIdentityAndCatalog(t *testing.T, m *ModelSnapshot, publicID, backendID string) {
	t.Helper()
	if m.Identity.PublicID != publicID || m.Identity.BackendID != backendID {
		t.Fatalf("identity = %#v", m.Identity)
	}
	if m.Catalog == nil || m.Catalog.ContextLength != nil {
		t.Fatalf("context_length should be nil (not advertised), got %#v", m.Catalog)
	}
}

// assertActivityBasics verifies activity state, freshness, row count, and
// failure count for the basic attribution test.
func assertActivityBasics(t *testing.T, act *ActivityMeasurement) {
	t.Helper()
	if act == nil {
		t.Fatal("activity measurement should be present")
	}
	if act.State != MeasurementPartial || act.Freshness != "fresh" {
		t.Fatalf("state/freshness = %s/%s, want partial/fresh (complete row collection with unreported metrics)", act.State, act.Freshness)
	}
	if act.RowCount != 1 {
		t.Fatalf("row_count = %d, want 1", act.RowCount)
	}
	if act.FailureCount != 0 {
		t.Fatalf("failure_count = %d, want 0", act.FailureCount)
	}
}

// assertActivityMetrics verifies duration, input, and output metric aggregates
// for the basic attribution test. Missing values are never summed as zero.
func assertActivityMetrics(t *testing.T, act *ActivityMeasurement) {
	t.Helper()
	if act.DurationMS == nil || act.DurationMS.Mean == nil || *act.DurationMS.Mean != 100 {
		t.Fatalf("duration mean = %#v, want 100", act.DurationMS)
	}
	if act.DurationMS.SampleCount != 1 {
		t.Fatalf("duration sample_count = %d, want 1", act.DurationMS.SampleCount)
	}
	if act.InputTokens == nil || act.InputTokens.Sum == nil || *act.InputTokens.Sum != 10 {
		t.Fatalf("input sum = %#v, want 10", act.InputTokens)
	}
	if act.InputTokens.KnownCount != 1 || act.InputTokens.MissingCount != 0 {
		t.Fatalf("input counts = %#v", act.InputTokens)
	}
	if act.OutputTokens == nil || act.OutputTokens.Sum == nil || *act.OutputTokens.Sum != 5 {
		t.Fatalf("output sum = %#v, want 5", act.OutputTokens)
	}
}

// assertDraftRateUnknown verifies the draft rate is explicitly unknown when
// speculative acceptance tokens are unreported.
func assertDraftRateUnknown(t *testing.T, act *ActivityMeasurement) {
	t.Helper()
	if act.DraftRate == nil || act.DraftRate.State != MeasurementUnknown || act.DraftRate.Rate != nil {
		t.Fatal("unreported speculative-acceptance rate should be explicitly unknown")
	}
}

// snapshotWithSourcesOneActivity assembles the standard single-activity
// (act-1, http_success, no metrics) sources, invokes SnapshotWithSources, and
// returns the validated snapshot. It fails the test on error or unexpected
// count. localID selects the model and activity identity. countFormat formats
// the unexpected-count failure message.
func snapshotWithSourcesOneActivity(t *testing.T, backendModels []BackendModel, sources SnapshotSources, localID string, countFormat string) []*ModelSnapshot {
	t.Helper()
	_, _ = correlation.ModelReference("default", localID)
	identities := []correlation.ActivityModelIdentity{
		identityFor("default", localID, epoch1, "act-1"),
	}
	activities := []*correlation.ActivityRecord{
		activityFor("default", epoch1, "act-1", "http_success", activityMetrics{DurationMS: nil, InputTokens: nil, OutputTokens: nil, DraftTokens: nil, DraftAcceptedTokens: nil}),
	}
	sources.Activities = activities
	sources.Identities = identities

	result, err := New().SnapshotWithSources(context.Background(), backendModels, nil, sources)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf(countFormat, len(result))
	}
	return result
}

func TestSnapshotWithSourcesBasicAttribution(t *testing.T) {
	backendModels, _, sources := basicSources()
	localID := "local-model"
	identities := []correlation.ActivityModelIdentity{
		identityFor("default", localID, epoch1, "act-1"),
	}
	activities := []*correlation.ActivityRecord{
		activityFor("default", epoch1, "act-1", "http_success", activityMetrics{DurationMS: ptr64(100), InputTokens: ptr64(10), OutputTokens: ptr64(5), DraftTokens: nil, DraftAcceptedTokens: nil}),
	}
	sources.Activities = activities
	sources.Identities = identities

	result, err := New().SnapshotWithSources(context.Background(), backendModels, nil, sources)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(result))
	}
	m := result[0]
	assertSnapshotIdentityAndCatalog(t, m, "local-model", "default")
	assertActivityBasics(t, m.Activity)
	assertActivityMetrics(t, m.Activity)
	assertDraftRateUnknown(t, m.Activity)
}

func TestSnapshotWithSourcesDraftRateAggregate(t *testing.T) {
	backendModels, _, sources := basicSources()
	localID := "local-model"
	_, _ = correlation.ModelReference("default", localID)
	identities := []correlation.ActivityModelIdentity{
		identityFor("default", localID, epoch1, "act-1"),
		identityFor("default", localID, epoch1, "act-2"),
	}
	// act-1: proposed 10, accepted 8. act-2: proposed 10, accepted 5.
	activities := []*correlation.ActivityRecord{
		activityFor("default", epoch1, "act-1", "http_success", activityMetrics{DurationMS: nil, InputTokens: nil, OutputTokens: nil, DraftTokens: ptr64(10), DraftAcceptedTokens: ptr64(8)}),
		activityFor("default", epoch1, "act-2", "http_success", activityMetrics{DurationMS: nil, InputTokens: nil, OutputTokens: nil, DraftTokens: ptr64(10), DraftAcceptedTokens: ptr64(5)}),
	}
	sources.Activities = activities
	sources.Identities = identities

	result, err := New().SnapshotWithSources(context.Background(), backendModels, nil, sources)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(result))
	}
	rate := result[0].Activity.DraftRate
	if rate == nil {
		t.Fatal("draft rate should be present")
	}
	if rate.AcceptedTokens == nil || *rate.AcceptedTokens != 13 {
		t.Fatalf("accepted_tokens = %#v, want 13", rate.AcceptedTokens)
	}
	if rate.ProposedTokens == nil || *rate.ProposedTokens != 20 {
		t.Fatalf("proposed_tokens = %#v, want 20", rate.ProposedTokens)
	}
	if rate.KnownPairs != 2 {
		t.Fatalf("known_pairs = %d, want 2", rate.KnownPairs)
	}
	if rate.Rate == nil || *rate.Rate != 0.65 {
		t.Fatalf("rate = %#v, want 0.65", rate.Rate)
	}
	if rate.State != MeasurementSupported {
		t.Fatalf("rate state = %s, want supported", rate.State)
	}
	// The rate is an aggregate across observed requests, not per-position.
}

func TestSnapshotWithSourcesDraftRateConflicting(t *testing.T) {
	backendModels, _, sources := basicSources()
	localID := "local-model"
	_, _ = correlation.ModelReference("default", localID)
	identities := []correlation.ActivityModelIdentity{
		identityFor("default", localID, epoch1, "act-1"),
	}
	// accepted 12 > proposed 10 -> conflicting.
	activities := []*correlation.ActivityRecord{
		activityFor("default", epoch1, "act-1", "http_success", activityMetrics{DurationMS: nil, InputTokens: nil, OutputTokens: nil, DraftTokens: ptr64(10), DraftAcceptedTokens: ptr64(12)}),
	}
	sources.Activities = activities
	sources.Identities = identities

	result, err := New().SnapshotWithSources(context.Background(), backendModels, nil, sources)
	if err != nil {
		t.Fatal(err)
	}
	rate := result[0].Activity.DraftRate
	if rate == nil {
		t.Fatal("draft rate should be present")
	}
	if rate.State != MeasurementUnknown {
		t.Fatalf("rate state = %s, want unknown (conflicting)", rate.State)
	}
	if rate.Rate != nil {
		t.Fatal("rate should be nil when conflicting")
	}
}

func TestSnapshotWithSourcesDraftRateMissingPair(t *testing.T) {
	backendModels, _, sources := basicSources()
	localID := "local-model"
	_, _ = correlation.ModelReference("default", localID)
	identities := []correlation.ActivityModelIdentity{
		identityFor("default", localID, epoch1, "act-1"),
	}
	// proposed known, accepted missing -> pair not known -> rate nil.
	activities := []*correlation.ActivityRecord{
		activityFor("default", epoch1, "act-1", "http_success", activityMetrics{DurationMS: nil, InputTokens: nil, OutputTokens: nil, DraftTokens: ptr64(10), DraftAcceptedTokens: nil}),
	}
	sources.Activities = activities
	sources.Identities = identities

	result, err := New().SnapshotWithSources(context.Background(), backendModels, nil, sources)
	if err != nil {
		t.Fatal(err)
	}
	rate := result[0].Activity.DraftRate
	if rate == nil {
		t.Fatal("draft rate should be present")
	}
	if rate.Rate != nil {
		t.Fatal("rate should be nil when a pair is missing")
	}
	if rate.KnownPairs != 0 {
		t.Fatalf("known_pairs = %d, want 0", rate.KnownPairs)
	}
}

func TestSnapshotWithSourcesMissingMetricsNotSummedAsZero(t *testing.T) {
	backendModels, _, sources := basicSources()
	localID := "local-model"
	_, _ = correlation.ModelReference("default", localID)
	identities := []correlation.ActivityModelIdentity{
		identityFor("default", localID, epoch1, "act-1"),
	}
	// input known, output missing, cache missing.
	activities := []*correlation.ActivityRecord{
		activityFor("default", epoch1, "act-1", "http_success", activityMetrics{DurationMS: nil, InputTokens: ptr64(10), OutputTokens: nil, DraftTokens: nil, DraftAcceptedTokens: nil}),
	}
	sources.Activities = activities
	sources.Identities = identities

	result, err := New().SnapshotWithSources(context.Background(), backendModels, nil, sources)
	if err != nil {
		t.Fatal(err)
	}
	act := result[0].Activity
	if act.InputTokens == nil || act.InputTokens.Sum == nil || *act.InputTokens.Sum != 10 {
		t.Fatalf("input sum = %#v, want 10", act.InputTokens)
	}
	if act.InputTokens.KnownCount != 1 || act.InputTokens.MissingCount != 0 {
		t.Fatalf("input counts = %#v", act.InputTokens)
	}
	if act.OutputTokens == nil || act.OutputTokens.Sum != nil {
		t.Fatal("output sum should be nil (missing not summed as zero)")
	}
	if act.OutputTokens.KnownCount != 0 || act.OutputTokens.MissingCount != 1 {
		t.Fatalf("output counts = %#v, want known=0 missing=1", act.OutputTokens)
	}
}

func TestSnapshotWithSourcesTransportFailuresOnly(t *testing.T) {
	backendModels, _, sources := basicSources()
	localID := "local-model"
	_, _ = correlation.ModelReference("default", localID)
	identities := []correlation.ActivityModelIdentity{
		identityFor("default", localID, epoch1, "act-1"),
		identityFor("default", localID, epoch1, "act-2"),
	}
	// Success is excluded; finite HTTP failure and transport outcomes count.
	identities = append(identities, identityFor("default", localID, epoch1, "act-3"),
		identityFor("default", localID, epoch1, "act-4"), identityFor("default", localID, epoch1, "act-5"))
	activities := []*correlation.ActivityRecord{
		activityFor("default", epoch1, "act-1", "http_success", activityMetrics{DurationMS: nil, InputTokens: nil, OutputTokens: nil, DraftTokens: nil, DraftAcceptedTokens: nil}),
		activityFor("default", epoch1, "act-2", "http_server_error", activityMetrics{DurationMS: nil, InputTokens: nil, OutputTokens: nil, DraftTokens: nil, DraftAcceptedTokens: nil}),
		activityFor("default", epoch1, "act-3", "http_client_error", activityMetrics{DurationMS: nil, InputTokens: nil, OutputTokens: nil, DraftTokens: nil, DraftAcceptedTokens: nil}),
		activityFor("default", epoch1, "act-4", "http_other", activityMetrics{DurationMS: nil, InputTokens: nil, OutputTokens: nil, DraftTokens: nil, DraftAcceptedTokens: nil}),
		activityFor("default", epoch1, "act-5", "transport_error", activityMetrics{DurationMS: nil, InputTokens: nil, OutputTokens: nil, DraftTokens: nil, DraftAcceptedTokens: nil}),
	}
	sources.Activities = activities
	sources.Identities = identities

	result, err := New().SnapshotWithSources(context.Background(), backendModels, nil, sources)
	if err != nil {
		t.Fatal(err)
	}
	act := result[0].Activity
	if act.RowCount != 5 {
		t.Fatalf("row_count = %d, want 5", act.RowCount)
	}
	if act.FailureCount != 4 {
		t.Fatalf("failure_count = %d, want 4", act.FailureCount)
	}
}

func TestSnapshotWithSourcesThroughputMeans(t *testing.T) {
	backendModels, _, sources := basicSources()
	localID := "local-model"
	_, _ = correlation.ModelReference("default", localID)
	identities := []correlation.ActivityModelIdentity{
		identityFor("default", localID, epoch1, "act-1"),
		identityFor("default", localID, epoch1, "act-2"),
	}
	// prompt rates 2.0 and 4.0 -> mean 3.0, sample_count 2.
	// token rates 1.0 and 3.0 -> mean 2.0, sample_count 2.
	activities := []*correlation.ActivityRecord{
		activityFor("default", epoch1, "act-1", "http_success", activityMetrics{DurationMS: nil, InputTokens: nil, OutputTokens: nil, DraftTokens: nil, DraftAcceptedTokens: nil}),
		activityFor("default", epoch1, "act-2", "http_success", activityMetrics{DurationMS: nil, InputTokens: nil, OutputTokens: nil, DraftTokens: nil, DraftAcceptedTokens: nil}),
	}
	activities[0].PromptTokensPerSecond = ptrF64(2.0)
	activities[0].TokensPerSecond = ptrF64(1.0)
	activities[1].PromptTokensPerSecond = ptrF64(4.0)
	activities[1].TokensPerSecond = ptrF64(3.0)
	sources.Activities = activities
	sources.Identities = identities

	result, err := New().SnapshotWithSources(context.Background(), backendModels, nil, sources)
	if err != nil {
		t.Fatal(err)
	}
	act := result[0].Activity
	if act.PrefillTokensPerSecond == nil || act.PrefillTokensPerSecond.Mean == nil || *act.PrefillTokensPerSecond.Mean != 3.0 {
		t.Fatalf("prefill rate mean = %#v, want 3.0", act.PrefillTokensPerSecond)
	}
	if act.PrefillTokensPerSecond.SampleCount != 2 {
		t.Fatalf("prefill sample_count = %d, want 2", act.PrefillTokensPerSecond.SampleCount)
	}
	if act.DecodeTokensPerSecond == nil || act.DecodeTokensPerSecond.Mean == nil || *act.DecodeTokensPerSecond.Mean != 2.0 {
		t.Fatalf("decode rate mean = %#v, want 2.0", act.DecodeTokensPerSecond)
	}
	if act.DecodeTokensPerSecond.SampleCount != 2 {
		t.Fatalf("decode sample_count = %d, want 2", act.DecodeTokensPerSecond.SampleCount)
	}
}

func TestSnapshotWithSourcesAliasResolutionPositive(t *testing.T) {
	backendModels := []BackendModel{
		{BackendID: "default", Prefix: "", Models: []map[string]any{{"id": "canonical-model"}}},
	}
	sources := SnapshotSources{
		BackendModels: backendModels,
		BackendStates: map[string]correlation.BackendState{
			"default": backendState("collected", "complete", false, fixtureTime),
		},
	}
	localID := "canonical-model"
	_, _ = correlation.ModelReference("default", localID)
	// Two aliases (alias-A, alias-B) both resolve to the same canonical ref.
	identities := []correlation.ActivityModelIdentity{
		identityFor("default", localID, epoch1, "act-alias-a"),
		identityFor("default", localID, epoch1, "act-alias-b"),
	}
	activities := []*correlation.ActivityRecord{
		activityFor("default", epoch1, "act-alias-a", "http_success", activityMetrics{DurationMS: ptr64(100), InputTokens: ptr64(10), OutputTokens: nil, DraftTokens: nil, DraftAcceptedTokens: nil}),
		activityFor("default", epoch1, "act-alias-b", "http_success", activityMetrics{DurationMS: ptr64(100), InputTokens: ptr64(10), OutputTokens: nil, DraftTokens: nil, DraftAcceptedTokens: nil}),
	}
	sources.Activities = activities
	sources.Identities = identities

	result, err := New().SnapshotWithSources(context.Background(), backendModels, nil, sources)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(result))
	}
	// Unique activity IDs counted; no alias double count.
	if result[0].Activity.RowCount != 2 {
		t.Fatalf("row_count = %d, want 2 (unique activity IDs)", result[0].Activity.RowCount)
	}
	if result[0].Activity.InputTokens == nil || *result[0].Activity.InputTokens.Sum != 20 {
		t.Fatalf("input sum = %#v, want 20", result[0].Activity.InputTokens)
	}
}

func TestSnapshotWithSourcesAliasAmbiguousUnknown(t *testing.T) {
	backendModels := []BackendModel{
		{BackendID: "default", Prefix: "", Models: []map[string]any{{"id": "model"}}},
	}
	sources := SnapshotSources{
		BackendModels: backendModels,
		BackendStates: map[string]correlation.BackendState{
			"default": backendState("collected", "complete", false, fixtureTime),
		},
	}
	localID := "model"
	expectedRef, _ := correlation.ModelReference("default", localID)
	// Ambiguous identity: no canonical ref.
	identities := []correlation.ActivityModelIdentity{
		{
			BackendID:        "default",
			Epoch:            epoch1,
			ActivityID:       "act-1",
			ReportedModelRef: expectedRef,
			State:            "ambiguous",
			Source:           "catalog_metadata",
		},
	}
	activities := []*correlation.ActivityRecord{
		activityFor("default", epoch1, "act-1", "http_success", activityMetrics{DurationMS: nil, InputTokens: nil, OutputTokens: nil, DraftTokens: nil, DraftAcceptedTokens: nil}),
	}
	sources.Activities = activities
	sources.Identities = identities

	result, err := New().SnapshotWithSources(context.Background(), backendModels, nil, sources)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(result))
	}
	if result[0].Activity.State != MeasurementUnknown {
		t.Fatalf("state = %s, want unknown (ambiguous identity)", result[0].Activity.State)
	}
}

func TestSnapshotWithSourcesCrossBackendEpochUnknown(t *testing.T) {
	backendModels := []BackendModel{
		{BackendID: "default", Prefix: "", Models: []map[string]any{{"id": "model"}}},
		{BackendID: "other", Prefix: "", Models: []map[string]any{{"id": "model"}}},
	}
	sources := SnapshotSources{
		BackendModels: backendModels,
		BackendStates: map[string]correlation.BackendState{
			"default": backendState("collected", "complete", false, fixtureTime),
			"other":   backendState("collected", "complete", false, fixtureTime),
		},
	}
	// Identity for "default" backend but canonical ref of "other" backend's model.
	// No matching identity for default's model -> unknown.
	localID := "model"
	_, _ = correlation.ModelReference("default", localID)
	_, _ = correlation.ModelReference("other", localID)
	identities := []correlation.ActivityModelIdentity{
		identityFor("other", localID, epoch1, "act-1"), // ref of other backend
	}
	activities := []*correlation.ActivityRecord{
		activityFor("other", epoch1, "act-1", "http_success", activityMetrics{DurationMS: nil, InputTokens: nil, OutputTokens: nil, DraftTokens: nil, DraftAcceptedTokens: nil}),
	}
	sources.Activities = activities
	sources.Identities = identities

	result, err := New().SnapshotWithSources(context.Background(), backendModels, nil, sources)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(result))
	}
	// default model: no matching identity (ref mismatch) -> unknown activity.
	defaultSnap := result[0]
	if defaultSnap.Activity != nil && defaultSnap.Activity.State != MeasurementUnknown {
		t.Fatalf("default model state = %s, want unknown (ref mismatch)", defaultSnap.Activity.State)
	}
	// other model: attributed.
	otherSnap := result[1]
	if otherSnap.Activity == nil || otherSnap.Activity.State != MeasurementPartial || otherSnap.Activity.Freshness != "fresh" {
		t.Fatalf("other model state/freshness = %v, want partial/fresh", otherSnap.Activity)
	}
}

func TestSnapshotWithSourcesDuplicateRowsDedup(t *testing.T) {
	backendModels, _, sources := basicSources()
	localID := "local-model"
	_, _ = correlation.ModelReference("default", localID)
	identities := []correlation.ActivityModelIdentity{
		identityFor("default", localID, epoch1, "act-1"),
	}
	// Same activity ID appears twice with the same timestamp (replay) -> dedup,
	// count once.
	activities := []*correlation.ActivityRecord{
		activityFor("default", epoch1, "act-1", "http_success", activityMetrics{DurationMS: ptr64(100), InputTokens: ptr64(10), OutputTokens: nil, DraftTokens: nil, DraftAcceptedTokens: nil}),
		activityFor("default", epoch1, "act-1", "http_success", activityMetrics{DurationMS: ptr64(100), InputTokens: ptr64(10), OutputTokens: nil, DraftTokens: nil, DraftAcceptedTokens: nil}),
	}
	sources.Activities = activities
	sources.Identities = identities

	result, err := New().SnapshotWithSources(context.Background(), backendModels, nil, sources)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(result))
	}
	if result[0].Activity.RowCount != 1 {
		t.Fatalf("row_count = %d, want 1 (dedup)", result[0].Activity.RowCount)
	}
	if result[0].Activity.InputTokens == nil || *result[0].Activity.InputTokens.Sum != 10 {
		t.Fatalf("input sum = %#v, want 10 (no double count)", result[0].Activity.InputTokens)
	}
}

func TestSnapshotWithSourcesDuplicateConflictAmbiguous(t *testing.T) {
	backendModels, _, sources := basicSources()
	localID := "local-model"
	_, _ = correlation.ModelReference("default", localID)
	identities := []correlation.ActivityModelIdentity{
		identityFor("default", localID, epoch1, "act-1"),
	}
	// Same activity ID with conflicting timestamps -> conflict -> ambiguous.
	activities := []*correlation.ActivityRecord{
		activityFor("default", epoch1, "act-1", "http_success", activityMetrics{DurationMS: ptr64(100), InputTokens: ptr64(10), OutputTokens: nil, DraftTokens: nil, DraftAcceptedTokens: nil}),
		activityFor("default", epoch1, "act-1", "http_success", activityMetrics{DurationMS: ptr64(200), InputTokens: ptr64(10), OutputTokens: nil, DraftTokens: nil, DraftAcceptedTokens: nil}),
	}
	activities[1].Timestamp = fixtureTime.Add(time.Minute) // conflicting timestamp
	sources.Activities = activities
	sources.Identities = identities

	result, err := New().SnapshotWithSources(context.Background(), backendModels, nil, sources)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(result))
	}
	if result[0].Activity.State != MeasurementAmbiguous {
		t.Fatalf("state = %s, want ambiguous (duplicate conflict)", result[0].Activity.State)
	}
}

func TestSnapshotWithSourcesStalePartialBackendNotFresh(t *testing.T) {
	backendModels, _, sources := basicSources()
	localID := "local-model"
	_, _ = correlation.ModelReference("default", localID)
	identities := []correlation.ActivityModelIdentity{
		identityFor("default", localID, epoch1, "act-1"),
	}
	activities := []*correlation.ActivityRecord{
		activityFor("default", epoch1, "act-1", "http_success", activityMetrics{DurationMS: ptr64(100), InputTokens: ptr64(10), OutputTokens: nil, DraftTokens: nil, DraftAcceptedTokens: nil}),
	}
	sources.Activities = activities
	sources.Identities = identities

	// Backend unavailable -> not fresh, state unavailable.
	sources.BackendStates["default"] = backendState("unavailable", "complete", false, fixtureTime)
	result, err := New().SnapshotWithSources(context.Background(), backendModels, nil, sources)
	if err != nil {
		t.Fatal(err)
	}
	if result[0].Activity.State != MeasurementUnavailable {
		t.Fatalf("state = %s, want unavailable", result[0].Activity.State)
	}
	if result[0].Activity.Freshness != "not_fresh" {
		t.Fatalf("freshness = %s, want not_fresh", result[0].Activity.Freshness)
	}

	// Backend collected but partial coverage -> partial, stale freshness.
	sources.BackendStates["default"] = backendState("collected", "partial", false, fixtureTime)
	result, err = New().SnapshotWithSources(context.Background(), backendModels, nil, sources)
	if err != nil {
		t.Fatal(err)
	}
	if result[0].Activity.State != MeasurementPartial {
		t.Fatalf("state = %s, want partial", result[0].Activity.State)
	}
	if result[0].Activity.Freshness != "stale" {
		t.Fatalf("freshness = %s, want stale", result[0].Activity.Freshness)
	}
}

func TestSnapshotWithSourcesNoRawMetadataOutput(t *testing.T) {
	backendModels := []BackendModel{
		{BackendID: "default", Prefix: "", Models: []map[string]any{
			{
				"id": "model",
				"meta": map[string]any{
					"llamaswap": map[string]any{
						"template_version": "v1",
						"quantization":     "command --secret",
					},
				},
				"command": "/usr/bin/foo",
				"desc":    "a description",
				"aliases": []string{"alias"},
			},
		}},
	}
	sources := SnapshotSources{
		BackendModels: backendModels,
		BackendStates: map[string]correlation.BackendState{
			"default": backendState("collected", "complete", false, fixtureTime),
		},
	}
	m := snapshotWithSourcesOneActivity(t, backendModels, sources, "model", "expected 1 snapshot, got %d")[0]
	// Catalog projection must not contain raw metadata keys.
	_ = m
	// Verify via JSON that no raw secrets/meta/command/aliases appear.
	enc, err := json.Marshal(map[string]any{
		"identity": m.Identity,
		"catalog":  m.Catalog,
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(enc), "meta") || strings.Contains(string(enc), "llamaswap") ||
		strings.Contains(string(enc), "command") || strings.Contains(string(enc), "desc") ||
		strings.Contains(string(enc), "aliases") || strings.Contains(string(enc), "--secret") {
		t.Fatalf("raw metadata leaked in output: %s", string(enc))
	}
}

func TestSnapshotWithSourcesMalformedContextUnknown(t *testing.T) {
	backendModels := []BackendModel{
		{BackendID: "default", Prefix: "", Models: []map[string]any{
			{"id": "model", "context_length": 8192.5}, // fractional -> unknown
		}},
	}
	sources := SnapshotSources{
		BackendModels: backendModels,
		BackendStates: map[string]correlation.BackendState{
			"default": backendState("collected", "complete", false, fixtureTime),
		},
	}
	m := snapshotWithSourcesOneActivity(t, backendModels, sources, "model", "expected 1 snapshot, got %d")[0]
	if m.Catalog == nil || m.Catalog.ContextLength != nil {
		t.Fatal("fractional context_length should be unknown (nil)")
	}
}

func TestSnapshotWithSourcesContextNegativeUnknown(t *testing.T) {
	backendModels := []BackendModel{
		{BackendID: "default", Prefix: "", Models: []map[string]any{
			{"id": "model", "context_length": -1},
		}},
	}
	sources := SnapshotSources{
		BackendModels: backendModels,
		BackendStates: map[string]correlation.BackendState{
			"default": backendState("collected", "complete", false, fixtureTime),
		},
	}
	m := snapshotWithSourcesOneActivity(t, backendModels, sources, "model", "expected 1 snapshot, got %d")[0]
	if m.Catalog == nil || m.Catalog.ContextLength != nil {
		t.Fatal("negative context_length should be unknown (nil)")
	}
}

func TestSnapshotWithSourcesGPUSharedUnknownOwnership(t *testing.T) {
	backendModels, _, sources := basicSources()

	// GPU sample for a device within the association window, but adjacency is
	// not proof of ownership.
	ws := fixtureTime.Add(-time.Second)
	we := fixtureTime.Add(time.Second)
	sampleTS := fixtureTime
	sources.GPUReport = &correlation.GPUReport{
		Samples: []correlation.GPUSample{
			gpuSample("default", epoch1, "gpu-abc", sampleTS, ptrF64(50.0), ptrF64(60.0), ptrInt(1024)),
		},
	}
	sources.DeviceAssociations = []correlation.ActivityDeviceAssociation{
		deviceAssoc("default", epoch1, "act-1", "gpu-abc", ws, we),
	}

	gw := snapshotWithSourcesOneActivity(t, backendModels, sources, "local-model", "expected 1 snapshot, got %d")[0].GPUWindow
	if gw == nil {
		t.Fatal("GPU window should be present")
	}
	if !gw.SharedDeviceOnly || !gw.ExecutionDeviceUnverified || !gw.ClockAlignmentUnverified {
		t.Fatal("GPU window must carry shared/unknown ownership flags")
	}
	if gw.SampleCount != 1 {
		t.Fatalf("sample_count = %d, want 1", gw.SampleCount)
	}
	if gw.MaxGpuUtilPct == nil || *gw.MaxGpuUtilPct != 50.0 {
		t.Fatalf("max gpu util = %#v, want 50.0", gw.MaxGpuUtilPct)
	}
	if gw.MinMemUsedMB == nil || *gw.MinMemUsedMB != 1024 {
		t.Fatalf("min mem used = %#v, want 1024", gw.MinMemUsedMB)
	}
}

func TestSnapshotWithSourcesGPUNoTimestampOnlyJoin(t *testing.T) {
	backendModels, _, sources := basicSources()

	// GPU sample outside the association window -> no match.
	ws := fixtureTime.Add(-time.Second)
	we := fixtureTime.Add(time.Second)
	outerTS := fixtureTime.Add(10 * time.Second) // outside window
	sources.GPUReport = &correlation.GPUReport{
		Samples: []correlation.GPUSample{
			gpuSample("default", epoch1, "gpu-abc", outerTS, ptrF64(50.0), nil, nil),
		},
	}
	sources.DeviceAssociations = []correlation.ActivityDeviceAssociation{
		deviceAssoc("default", epoch1, "act-1", "gpu-abc", ws, we),
	}

	gw := snapshotWithSourcesOneActivity(t, backendModels, sources, "local-model", "expected 1 snapshot, got %d")[0].GPUWindow
	if gw == nil || gw.SampleCount != 0 || gw.State != MeasurementUnknown || gw.GpuUtilKnownSamples != 0 {
		t.Fatalf("missing matched sample should be an explicit unknown, not a fabricated reading: %#v", gw)
	}
}

func TestSnapshotWithSourcesGPUAssociationUnavailable(t *testing.T) {
	backendModels, _, sources := basicSources()

	// Association kind unavailable -> no GPU measurements.
	sources.DeviceAssociations = []correlation.ActivityDeviceAssociation{
		{
			Kind:               "unavailable",
			BackendID:          "default",
			Epoch:              epoch1,
			ActivityID:         "act-1",
			DeviceID:           "gpu-abc",
			WindowStart:        fixtureTime,
			WindowEnd:          fixtureTime,
			MatchedSampleCount: 0,
		},
	}

	if gw := snapshotWithSourcesOneActivity(t, backendModels, sources, "local-model", "expected 1 snapshot, got %d")[0].GPUWindow; gw != nil {
		t.Fatal("GPU window should be nil when association kind is unavailable")
	}
}

func TestSnapshotWithSourcesLifecycleCurrentSnapshot(t *testing.T) {
	backendModels, _, sources := basicSources()
	localID := "local-model"
	expectedRef, _ := correlation.ModelReference("default", localID)

	// Current lifecycle model matches the canonical ref.
	sources.LifecycleReport = &correlation.LifecycleReport{
		Models: []correlation.LifecycleModel{
			lifecycleModel("default", epoch1, expectedRef, "ready", fixtureTime),
		},
	}
	lc := snapshotWithSourcesOneActivity(t, backendModels, sources, "local-model", "expected 1 snapshot, got %d")[0].Lifecycle
	if lc == nil {
		t.Fatal("lifecycle should be present")
	}
	if lc.CurrentState != "ready" {
		t.Fatalf("current_state = %s, want ready", lc.CurrentState)
	}
	if lc.Source != "/running" {
		t.Fatalf("lifecycle source = %s, want /running", lc.Source)
	}
	if !lc.Observed {
		t.Fatal("observed should be true")
	}
	if lc.StateAtActivity != "unknown" {
		t.Fatalf("state_at_activity = %s, want unknown", lc.StateAtActivity)
	}
	if lc.Kind != "current_metadata_only" || lc.TransitionEvidence != "unsupported" {
		t.Fatalf("lifecycle evidence = %#v, want current-only unsupported transitions", lc)
	}
}

func TestSnapshotCurrentCatalogStatusIsTimedSnapshotNotHistory(t *testing.T) {
	fetchedAt := fixtureTime.Add(time.Second)
	models := []BackendModel{{BackendID: "default", Prefix: "", ObservedAt: fetchedAt, Models: []map[string]any{{
		"id": "local-model", "status": map[string]any{"value": "unloaded"},
	}}}}
	got, err := New().SnapshotWithSources(context.Background(), models, nil, SnapshotSources{})
	if err != nil || len(got) != 1 {
		t.Fatalf("snapshots=%d, err=%v", len(got), err)
	}
	lc := got[0].Lifecycle
	if lc == nil || !lc.Observed || lc.CurrentState != "unloaded" || lc.Source != "/v1/models" || lc.ObservedAt == nil || !lc.ObservedAt.Equal(fetchedAt) {
		t.Fatalf("explicit catalog current state = %#v", lc)
	}
	if lc.StateAtActivity != "unknown" || lc.TransitionEvidence != "unsupported" || lc.Kind != "current_metadata_only" {
		t.Fatalf("current snapshot claimed transition history: %#v", lc)
	}
}

func TestSnapshotWithSourcesLifecycleAbsentNotUnloaded(t *testing.T) {
	backendModels, _, sources := basicSources()

	// Lifecycle report has a different (valid) model ref (absent for our model).
	otherRef, _ := correlation.ModelReference("default", "other-model")
	sources.LifecycleReport = &correlation.LifecycleReport{
		Models: []correlation.LifecycleModel{
			lifecycleModel("default", epoch1, otherRef, "ready", fixtureTime),
		},
	}
	sources.LifecycleAssociations = []correlation.ActivityLifecycleAssociation{
		lifecycleAssoc("default", epoch1, "act-1", "approximate_lifecycle_window", 1),
	}

	lc := snapshotWithSourcesOneActivity(t, backendModels, sources, "local-model", "expected 1 snapshot, got %d")[0].Lifecycle
	if lc.CurrentState != "unknown" {
		t.Fatalf("current_state = %s, want unknown (absent)", lc.CurrentState)
	}
	if lc.Observed {
		t.Fatal("observed should be false when absent")
	}
	if lc.StateAtActivity != "unknown" {
		t.Fatalf("state_at_activity = %s, want unknown", lc.StateAtActivity)
	}
}

func TestSnapshotWithSourcesDeterministic(t *testing.T) {
	backendModels, _, sources := basicSources()
	localID := "local-model"
	_, _ = correlation.ModelReference("default", localID)
	identities := []correlation.ActivityModelIdentity{
		identityFor("default", localID, epoch1, "act-1"),
	}
	activities := []*correlation.ActivityRecord{
		activityFor("default", epoch1, "act-1", "http_success", activityMetrics{DurationMS: ptr64(100), InputTokens: ptr64(10), OutputTokens: nil, DraftTokens: nil, DraftAcceptedTokens: nil}),
	}
	sources.Activities = activities
	sources.Identities = identities

	service := New()
	r1, err := service.SnapshotWithSources(context.Background(), backendModels, nil, sources)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := service.SnapshotWithSources(context.Background(), backendModels, nil, sources)
	if err != nil {
		t.Fatal(err)
	}
	if len(r1) != len(r2) {
		t.Fatal("snapshot is not deterministic")
	}
	for i := range r1 {
		if r1[i].Identity.PublicID != r2[i].Identity.PublicID {
			t.Fatal("snapshot is not deterministic")
		}
	}
}

func TestSnapshotWithSourcesBoundsBeforeAllocate(t *testing.T) {
	// Too many activities.
	backendModels := []BackendModel{
		{BackendID: "default", Prefix: "", Models: []map[string]any{{"id": "model"}}},
	}
	sources := SnapshotSources{
		BackendModels: backendModels,
		BackendStates: map[string]correlation.BackendState{
			"default": backendState("collected", "complete", false, fixtureTime),
		},
	}
	activities := make([]*correlation.ActivityRecord, 1001)
	for i := range activities {
		activities[i] = activityFor("default", epoch1, "act-"+string(rune('0'+i%10)), "http_success", activityMetrics{DurationMS: nil, InputTokens: nil, OutputTokens: nil, DraftTokens: nil, DraftAcceptedTokens: nil})
	}
	sources.Activities = activities
	if _, err := New().SnapshotWithSources(context.Background(), backendModels, nil, sources); err == nil {
		t.Fatal("expected error for too many activities")
	}

	// Too many GPU samples.
	sources2 := SnapshotSources{
		BackendModels: backendModels,
		BackendStates: map[string]correlation.BackendState{
			"default": backendState("collected", "complete", false, fixtureTime),
		},
		GPUReport: &correlation.GPUReport{
			Samples: make([]correlation.GPUSample, 4097),
		},
	}
	if _, err := New().SnapshotWithSources(context.Background(), backendModels, nil, sources2); err == nil {
		t.Fatal("expected error for too many GPU samples")
	}

	// Too many model records.
	sources3 := SnapshotSources{
		BackendModels: []BackendModel{
			{BackendID: "default", Prefix: "", Models: make([]map[string]any, 1001)},
		},
		BackendStates: map[string]correlation.BackendState{
			"default": backendState("collected", "complete", false, fixtureTime),
		},
	}
	for i := range sources3.BackendModels[0].Models {
		sources3.BackendModels[0].Models[i] = map[string]any{"id": "model"}
	}
	if _, err := New().SnapshotWithSources(context.Background(), sources3.BackendModels, nil, sources3); err == nil {
		t.Fatal("expected error for too many model records")
	}
}

func TestSnapshotWithSourcesCancellation(t *testing.T) {
	backendModels := []BackendModel{
		{BackendID: "default", Prefix: "", Models: []map[string]any{{"id": "model"}}},
	}
	sources := SnapshotSources{
		BackendModels: backendModels,
		BackendStates: map[string]correlation.BackendState{
			"default": backendState("collected", "complete", false, fixtureTime),
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := New().SnapshotWithSources(ctx, backendModels, nil, sources); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestSnapshotOldAPICompatibility(t *testing.T) {
	backendModels := []BackendModel{
		{BackendID: "default", Prefix: "", ObservedAt: fixtureTime, Models: []map[string]any{
			{"id": "model", "context_length": 8192, "status": map[string]any{"value": "loaded"}},
		}},
	}
	snap := newDiagnosticsSnapshot("default", map[string]diagnostics.Observation{
		"version": observation(diagnostics.Supported, fixtureTime, false, ""),
	})

	result, err := New().Snapshot(context.Background(), backendModels, []*diagnostics.Snapshot{snap})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(result))
	}
	m := result[0]
	if m.Identity.PublicID != "model" || m.Identity.BackendID != "default" {
		t.Fatalf("identity = %#v", m.Identity)
	}
	// Safe catalog projection: context_length advertised, no raw meta.
	if m.Catalog == nil || m.Catalog.ContextLength == nil || *m.Catalog.ContextLength != 8192 {
		t.Fatalf("catalog context_length = %#v", m.Catalog)
	}
	if m.Lifecycle == nil || !m.Lifecycle.Observed || m.Lifecycle.CurrentState != "loaded" || m.Lifecycle.Source != "/v1/models" {
		t.Fatalf("catalog current status should be represented with source in lifecycle snapshot: %#v", m.Lifecycle)
	}
	// Diagnostics legacy view preserved.
	if m.Diagnostics.Version == nil || m.Diagnostics.Version.State != Supported {
		t.Fatalf("version diagnostics = %#v", m.Diagnostics.Version)
	}
	// Provenance nonnil.
	if m.Provenance == nil {
		t.Fatal("provenance should be nonnil")
	}
}

func TestSnapshotOldAPISafeCatalogNoRawMetadata(t *testing.T) {
	backendModels := []BackendModel{
		{BackendID: "default", Prefix: "", Models: []map[string]any{
			{"id": "model",
				"meta": map[string]any{
					"llamaswap": map[string]any{
						"template_version": "v1",
					},
				}},
		}},
	}
	snap := newDiagnosticsSnapshot("default", map[string]diagnostics.Observation{})

	result, err := New().Snapshot(context.Background(), backendModels, []*diagnostics.Snapshot{snap})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(result))
	}
	m := result[0]
	if m.Catalog == nil {
		t.Fatal("catalog should be present")
	}
	// No raw meta keys in the typed projection (it has no meta field).
	_ = m.Catalog.ContextLength
	// Provenance still reports declared metadata separately (sanitized by Build).
	if m.Provenance == nil {
		t.Fatal("provenance should be nonnil")
	}
}

func TestSnapshotWithSourcesNoAliasDefensive(t *testing.T) {
	// No identities supplied at all -> model appears with unknown activity.
	backendModels, _, sources := basicSources()
	localID := "local-model"
	_, _ = correlation.ModelReference("default", localID)
	identities := []correlation.ActivityModelIdentity{} // empty
	activities := []*correlation.ActivityRecord{
		activityFor("default", epoch1, "act-1", "http_success", activityMetrics{DurationMS: ptr64(100), InputTokens: ptr64(10), OutputTokens: nil, DraftTokens: nil, DraftAcceptedTokens: nil}),
	}
	sources.Activities = activities
	sources.Identities = identities

	result, err := New().SnapshotWithSources(context.Background(), backendModels, nil, sources)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(result))
	}
	if result[0].Activity.State != MeasurementUnknown {
		t.Fatalf("state = %s, want unknown (no alias)", result[0].Activity.State)
	}
}

func TestSnapshotWithSourcesConcurrentReads(t *testing.T) {
	backendModels, snapshots, sources := basicSources()
	activityIdentity := identityFor("default", "local-model", epoch1, "act-1")
	sources.Identities = []correlation.ActivityModelIdentity{activityIdentity}
	sources.Activities = []*correlation.ActivityRecord{activityFor("default", epoch1, "act-1", "http_success", activityMetrics{DurationMS: ptr64(10), InputTokens: ptr64(5), OutputTokens: ptr64(3), DraftTokens: ptr64(4), DraftAcceptedTokens: ptr64(2)})}
	svc := New()
	const readers = 12
	var wg sync.WaitGroup
	errCh := make(chan error, readers)
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := svc.SnapshotWithSources(context.Background(), backendModels, snapshots, sources)
			if err != nil {
				errCh <- err
				return
			}
			if len(got) != 1 || got[0].Activity == nil || got[0].Activity.DraftRate == nil || got[0].Activity.DraftRate.Rate == nil || *got[0].Activity.DraftRate.Rate != 0.5 {
				errCh <- errors.New("concurrent snapshot result mismatch")
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

func TestSnapshotWithSourcesReusedServiceDoesNotRetainOldGeneration(t *testing.T) {
	svc := New()
	backendModels, oldSnapshots, oldSources := basicSources()
	oldSources.Identities = []correlation.ActivityModelIdentity{identityFor("default", "local-model", epoch1, "old-activity")}
	oldSources.Activities = []*correlation.ActivityRecord{activityFor("default", epoch1, "old-activity", "http_success", activityMetrics{DurationMS: ptr64(10), InputTokens: ptr64(5), OutputTokens: nil, DraftTokens: nil, DraftAcceptedTokens: nil})}
	oldResult, err := svc.SnapshotWithSources(context.Background(), backendModels, oldSnapshots, oldSources)
	if err != nil {
		t.Fatal(err)
	}
	if oldResult[0].Activity.RowCount != 1 {
		t.Fatal("old generation fixture missing activity")
	}

	// Simulate proxy configuration reload: re-use the stateless service with a
	// fresh backend identity and current measurements. No old record may bleed.
	newBackend := []BackendModel{{BackendID: "reloaded", Prefix: "new/", ObservedAt: fixtureTime.Add(time.Minute), Models: []map[string]any{{"id": "fresh-model", "context_length": 16384}}}}
	newSources := SnapshotSources{BackendStates: map[string]correlation.BackendState{"reloaded": backendState("collected", "complete", false, fixtureTime.Add(time.Minute))}}
	newResult, err := svc.SnapshotWithSources(context.Background(), newBackend, nil, newSources)
	if err != nil {
		t.Fatal(err)
	}
	if len(newResult) != 1 || newResult[0].Identity.BackendID != "reloaded" || newResult[0].Identity.PublicID != "new/fresh-model" {
		t.Fatalf("reloaded snapshot identity = %#v", newResult)
	}
	if newResult[0].Activity == nil || newResult[0].Activity.RowCount != 0 || newResult[0].Activity.State != MeasurementUnknown {
		t.Fatalf("old generation activity leaked into new snapshot: %#v", newResult[0].Activity)
	}
	if newResult[0].Provenance.Fields["catalog_context_length"].Value != 16384 {
		t.Fatalf("reloaded catalog provenance = %#v", newResult[0].Provenance.Fields["catalog_context_length"])
	}
}

func TestSnapshotWithSourcesHugeIdentifierSkippedBeforePublicID(t *testing.T) {
	backendModels := []BackendModel{
		{BackendID: "default", Prefix: "", Models: []map[string]any{
			{"id": strings.Repeat("a", 300)}, // huge: skipped before publicID materialization
			{"id": "valid-model"},
		}},
	}
	sources := SnapshotSources{
		BackendModels: backendModels,
		BackendStates: map[string]correlation.BackendState{
			"default": backendState("collected", "complete", false, fixtureTime),
		},
	}

	m := snapshotWithSourcesOneActivity(t, backendModels, sources, "valid-model", "expected 1 snapshot (huge id skipped), got %d")[0]
	if m.Identity.PublicID != "valid-model" {
		t.Fatalf("public id = %q, want valid-model", m.Identity.PublicID)
	}
}

func TestSnapshotWithSourcesInvalidInputRejected(t *testing.T) {
	backendModels := []BackendModel{
		{BackendID: "default", Prefix: "", Models: []map[string]any{{"id": "model"}}},
	}
	sources := SnapshotSources{
		BackendModels: backendModels,
		BackendStates: map[string]correlation.BackendState{
			"default": backendState("collected", "complete", false, fixtureTime),
		},
	}
	_, _ = correlation.ModelReference("default", "model")
	identities := []correlation.ActivityModelIdentity{
		identityFor("default", "model", epoch1, "act-1"),
	}
	// Invalid activity backend ID (empty) must be rejected before indexing;
	// the error is generic and echoes no raw value.
	activities := []*correlation.ActivityRecord{
		activityFor("", epoch1, "act-1", "http_success", activityMetrics{DurationMS: nil, InputTokens: nil, OutputTokens: nil, DraftTokens: nil, DraftAcceptedTokens: nil}),
	}
	sources.Activities = activities
	sources.Identities = identities

	if _, err := New().SnapshotWithSources(context.Background(), backendModels, nil, sources); err == nil {
		t.Fatal("expected error for invalid activity backend id")
	}

	// Invalid canonical ref in an identity is also rejected.
	sources2 := SnapshotSources{
		BackendModels: backendModels,
		BackendStates: map[string]correlation.BackendState{
			"default": backendState("collected", "complete", false, fixtureTime),
		},
		Activities: []*correlation.ActivityRecord{
			activityFor("default", epoch1, "act-1", "http_success", activityMetrics{DurationMS: nil, InputTokens: nil, OutputTokens: nil, DraftTokens: nil, DraftAcceptedTokens: nil}),
		},
		Identities: []correlation.ActivityModelIdentity{
			{
				BackendID:        "default",
				Epoch:            epoch1,
				ActivityID:       "act-1",
				ReportedModelRef: "model-" + strings.Repeat("a", 32),
				CanonicalRef:     "not-a-valid-ref",
				State:            catalogResolvedState,
				Source:           "catalog_metadata",
			},
		},
	}
	if _, err := New().SnapshotWithSources(context.Background(), backendModels, nil, sources2); err == nil {
		t.Fatal("expected error for invalid canonical model ref")
	}
}

func TestSnapshotWithSourcesEmptyCoverageNotSupported(t *testing.T) {
	backendModels, _, sources := basicSources()
	localID := "local-model"
	_, _ = correlation.ModelReference("default", localID)
	identities := []correlation.ActivityModelIdentity{
		identityFor("default", localID, epoch1, "act-1"),
	}
	activities := []*correlation.ActivityRecord{
		activityFor("default", epoch1, "act-1", "http_success", activityMetrics{DurationMS: ptr64(100), InputTokens: ptr64(10), OutputTokens: nil, DraftTokens: nil, DraftAcceptedTokens: nil}),
	}
	sources.Activities = activities
	sources.Identities = identities

	// Collected but empty coverage -> unknown, never supported.
	sources.BackendStates["default"] = backendState("collected", "", false, fixtureTime)
	result, err := New().SnapshotWithSources(context.Background(), backendModels, nil, sources)
	if err != nil {
		t.Fatal(err)
	}
	if result[0].Activity.State != MeasurementUnknown {
		t.Fatalf("state = %s, want unknown (empty coverage not supported)", result[0].Activity.State)
	}
	if result[0].Activity.Freshness != "not_fresh" {
		t.Fatalf("freshness = %s, want not_fresh", result[0].Activity.Freshness)
	}
}

func TestSnapshotWithSourcesHttpOtherCountedAsFailure(t *testing.T) {
	backendModels, _, sources := basicSources()
	localID := "local-model"
	_, _ = correlation.ModelReference("default", localID)
	identities := []correlation.ActivityModelIdentity{
		identityFor("default", localID, epoch1, "act-1"),
		identityFor("default", localID, epoch1, "act-2"),
	}
	activities := []*correlation.ActivityRecord{
		activityFor("default", epoch1, "act-1", "http_success", activityMetrics{DurationMS: nil, InputTokens: nil, OutputTokens: nil, DraftTokens: nil, DraftAcceptedTokens: nil}),
		activityFor("default", epoch1, "act-2", "http_other", activityMetrics{DurationMS: nil, InputTokens: nil, OutputTokens: nil, DraftTokens: nil, DraftAcceptedTokens: nil}),
	}
	sources.Activities = activities
	sources.Identities = identities

	result, err := New().SnapshotWithSources(context.Background(), backendModels, nil, sources)
	if err != nil {
		t.Fatal(err)
	}
	act := result[0].Activity
	if act.RowCount != 2 {
		t.Fatalf("row_count = %d, want 2", act.RowCount)
	}
	if act.FailureCount != 1 {
		t.Fatalf("failure_count = %d, want 1 (http_other is a failure)", act.FailureCount)
	}
}

func TestSnapshotWithSourcesMissingCounts(t *testing.T) {
	backendModels, _, sources := basicSources()
	localID := "local-model"
	_, _ = correlation.ModelReference("default", localID)
	identities := []correlation.ActivityModelIdentity{
		identityFor("default", localID, epoch1, "act-1"),
	}
	// duration missing, input missing, output known.
	activities := []*correlation.ActivityRecord{
		activityFor("default", epoch1, "act-1", "http_success", activityMetrics{DurationMS: nil, InputTokens: nil, OutputTokens: ptr64(5), DraftTokens: nil, DraftAcceptedTokens: nil}),
	}
	sources.Activities = activities
	sources.Identities = identities

	result, err := New().SnapshotWithSources(context.Background(), backendModels, nil, sources)
	if err != nil {
		t.Fatal(err)
	}
	act := result[0].Activity
	if act.DurationMS == nil {
		t.Fatal("duration should be present")
	}
	if act.DurationMS.SampleCount != 0 || act.DurationMS.MissingCount != 1 {
		t.Fatalf("duration counts = %#v, want sample=0 missing=1", act.DurationMS)
	}
	if act.InputTokens == nil || act.InputTokens.Sum != nil {
		t.Fatal("input sum should be nil (missing, not zero)")
	}
	if act.InputTokens.KnownCount != 0 || act.InputTokens.MissingCount != 1 {
		t.Fatalf("input counts = %#v", act.InputTokens)
	}
	if act.OutputTokens == nil || act.OutputTokens.Sum == nil || *act.OutputTokens.Sum != 5 {
		t.Fatalf("output sum = %#v, want 5", act.OutputTokens)
	}
}

func TestSnapshotWithSourcesGPUOverlappingSampleDedup(t *testing.T) {
	backendModels, _, sources := basicSources()
	localID := "local-model"
	_, _ = correlation.ModelReference("default", localID)
	identities := []correlation.ActivityModelIdentity{
		identityFor("default", localID, epoch1, "act-1"),
		identityFor("default", localID, epoch1, "act-2"),
	}
	activities := []*correlation.ActivityRecord{
		activityFor("default", epoch1, "act-1", "http_success", activityMetrics{DurationMS: nil, InputTokens: nil, OutputTokens: nil, DraftTokens: nil, DraftAcceptedTokens: nil}),
		activityFor("default", epoch1, "act-2", "http_success", activityMetrics{DurationMS: nil, InputTokens: nil, OutputTokens: nil, DraftTokens: nil, DraftAcceptedTokens: nil}),
	}
	sources.Activities = activities
	sources.Identities = identities

	// Both activities associate to the same device within overlapping windows;
	// a single shared sample must be counted once.
	ws := fixtureTime.Add(-time.Second)
	we := fixtureTime.Add(time.Second)
	sampleTS := fixtureTime
	sources.GPUReport = &correlation.GPUReport{
		Samples: []correlation.GPUSample{
			gpuSample("default", epoch1, "gpu-abc", sampleTS, ptrF64(50.0), ptrF64(60.0), ptrInt(1024)),
		},
	}
	sources.DeviceAssociations = []correlation.ActivityDeviceAssociation{
		deviceAssoc("default", epoch1, "act-1", "gpu-abc", ws, we),
		deviceAssoc("default", epoch1, "act-2", "gpu-abc", ws, we),
	}

	result, err := New().SnapshotWithSources(context.Background(), backendModels, nil, sources)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(result))
	}
	gw := result[0].GPUWindow
	if gw == nil {
		t.Fatal("GPU window should be present")
	}
	if gw.SampleCount != 1 {
		t.Fatalf("sample_count = %d, want 1 (overlapping sample deduped)", gw.SampleCount)
	}
	if gw.GpuUtilKnownSamples != 1 {
		t.Fatalf("gpu util known = %d, want 1", gw.GpuUtilKnownSamples)
	}
	if gw.MemUtilKnownSamples != 1 {
		t.Fatalf("mem util known = %d, want 1", gw.MemUtilKnownSamples)
	}
	if gw.MemUsedKnownSamples != 1 {
		t.Fatalf("mem used known = %d, want 1", gw.MemUsedKnownSamples)
	}
}

func TestSnapshotWithSourcesGPUExplicitZeroPreserved(t *testing.T) {
	backendModels, _, sources := basicSources()

	ws := fixtureTime.Add(-time.Second)
	we := fixtureTime.Add(time.Second)
	sampleTS := fixtureTime
	// Explicit zero values are known readings, not missing.
	sources.GPUReport = &correlation.GPUReport{
		Samples: []correlation.GPUSample{
			gpuSample("default", epoch1, "gpu-abc", sampleTS, ptrF64(0.0), ptrF64(0.0), ptrInt(0)),
		},
	}
	sources.DeviceAssociations = []correlation.ActivityDeviceAssociation{
		deviceAssoc("default", epoch1, "act-1", "gpu-abc", ws, we),
	}

	gw := snapshotWithSourcesOneActivity(t, backendModels, sources, "local-model", "expected 1 snapshot, got %d")[0].GPUWindow
	if gw == nil {
		t.Fatal("GPU window should be present")
	}
	if gw.SampleCount != 1 {
		t.Fatalf("sample_count = %d, want 1", gw.SampleCount)
	}
	if gw.GpuUtilKnownSamples != 1 || gw.GpuUtilMissingSamples != 0 {
		t.Fatalf("gpu util counts = %d/%d, want 1/0 (explicit zero is known)", gw.GpuUtilKnownSamples, gw.GpuUtilMissingSamples)
	}
	if gw.MaxGpuUtilPct == nil || *gw.MaxGpuUtilPct != 0.0 {
		t.Fatalf("max gpu util = %#v, want 0.0", gw.MaxGpuUtilPct)
	}
	if gw.MinMemUsedMB == nil || *gw.MinMemUsedMB != 0 {
		t.Fatalf("min mem used = %#v, want 0", gw.MinMemUsedMB)
	}
}

func TestSnapshotWithSourcesGPUBackendStateEpochIsolation(t *testing.T) {
	backendModels, _, sources := basicSources()

	ws := fixtureTime.Add(-time.Second)
	we := fixtureTime.Add(time.Second)
	sampleTS := fixtureTime
	sources.GPUReport = &correlation.GPUReport{
		BackendStates: map[string]correlation.GPUBackendState{
			"default": {State: "enabled", Coverage: "complete", ObservedAt: fixtureTime},
			"other":   {State: "enabled", Coverage: "complete", ObservedAt: fixtureTime},
		},
		// Samples for both backends; only the "default" backend/epoch/device
		// referenced by the association is matched.
		Samples: []correlation.GPUSample{
			gpuSample("default", epoch1, "gpu-abc", sampleTS, ptrF64(50.0), nil, nil),
			gpuSample("other", epoch1, "gpu-def", sampleTS, ptrF64(90.0), nil, nil),
		},
	}
	sources.DeviceAssociations = []correlation.ActivityDeviceAssociation{
		deviceAssoc("default", epoch1, "act-1", "gpu-abc", ws, we),
	}

	gw := snapshotWithSourcesOneActivity(t, backendModels, sources, "local-model", "expected 1 snapshot, got %d")[0].GPUWindow
	if gw == nil {
		t.Fatal("GPU window should be present")
	}
	// The window uses the "default" backend state (enabled+complete).
	if gw.State != MeasurementSupported {
		t.Fatalf("gpu state = %s, want supported (default backend enabled+complete)", gw.State)
	}
	if gw.Freshness != "source_timestamp_only" {
		t.Fatalf("freshness = %s, want source_timestamp_only", gw.Freshness)
	}
	if gw.SampleCount != 1 {
		t.Fatalf("sample_count = %d, want 1 (other backend sample isolated)", gw.SampleCount)
	}
	if !gw.ObservedAt.Equal(fixtureTime) {
		t.Fatalf("observed_at = %v, want fixtureTime (backend state observed at, separate from sample times)", gw.ObservedAt)
	}
	if gw.FirstSampleAt.IsZero() || gw.LastSampleAt.IsZero() {
		t.Fatal("first/last sample at should be set from source sample timestamps")
	}
}

func TestSnapshotWithSourcesGPUMissingSamplesUnknown(t *testing.T) {
	backendModels, _, sources := basicSources()

	// Enabled+complete report but no matching sample -> unknown, explicit zero
	// counts (not fabricated values).
	ws := fixtureTime.Add(-time.Second)
	we := fixtureTime.Add(time.Second)
	sources.GPUReport = &correlation.GPUReport{
		BackendStates: map[string]correlation.GPUBackendState{
			"default": {State: "enabled", Coverage: "complete", ObservedAt: fixtureTime},
		},
		Samples: []correlation.GPUSample{
			gpuSample("default", epoch1, "gpu-abc", fixtureTime.Add(10*time.Second), ptrF64(50.0), nil, nil),
		},
	}
	sources.DeviceAssociations = []correlation.ActivityDeviceAssociation{
		deviceAssoc("default", epoch1, "act-1", "gpu-abc", ws, we),
	}

	gw := snapshotWithSourcesOneActivity(t, backendModels, sources, "local-model", "expected 1 snapshot, got %d")[0].GPUWindow
	if gw == nil {
		t.Fatal("GPU window should be present")
	}
	if gw.State != MeasurementUnknown {
		t.Fatalf("gpu state = %s, want unknown (missing samples)", gw.State)
	}
	if gw.Reason != "no_matching_samples" {
		t.Fatalf("reason = %q, want no_matching_samples", gw.Reason)
	}
	if gw.SampleCount != 0 || gw.GpuUtilKnownSamples != 0 || gw.GpuUtilMissingSamples != 0 {
		t.Fatalf("missing samples should be explicit zero counts: %#v", gw)
	}
}

func TestSnapshotWithSourcesGPUBackendStateDrivesState(t *testing.T) {
	backendModels, _, sources := basicSources()

	ws := fixtureTime.Add(-time.Second)
	we := fixtureTime.Add(time.Second)
	sampleTS := fixtureTime
	sources.DeviceAssociations = []correlation.ActivityDeviceAssociation{
		deviceAssoc("default", epoch1, "act-1", "gpu-abc", ws, we),
	}
	newReport := func(state correlation.GPUBackendState) *correlation.GPUReport {
		return &correlation.GPUReport{
			BackendStates: map[string]correlation.GPUBackendState{"default": state},
			Samples:       []correlation.GPUSample{gpuSample("default", epoch1, "gpu-abc", sampleTS, ptrF64(50.0), nil, nil)},
		}
	}

	// unavailable -> unavailable
	sources.GPUReport = newReport(correlation.GPUBackendState{State: "unavailable", Coverage: "complete"})
	result := snapshotWithSourcesOneActivity(t, backendModels, sources, "local-model", "expected 1 snapshot, got %d")
	if result[0].GPUWindow.State != MeasurementUnavailable {
		t.Fatalf("unavailable -> %s, want unavailable", result[0].GPUWindow.State)
	}

	// partial coverage -> partial
	sources.GPUReport = newReport(correlation.GPUBackendState{State: "enabled", Coverage: "partial"})
	result = snapshotWithSourcesOneActivity(t, backendModels, sources, "local-model", "expected 1 snapshot, got %d")
	if result[0].GPUWindow.State != MeasurementPartial {
		t.Fatalf("partial -> %s, want partial", result[0].GPUWindow.State)
	}

	// global truncation -> partial (conservative)
	sources.GPUReport = newReport(correlation.GPUBackendState{State: "enabled", Coverage: "complete"})
	sources.GPUReport.Truncated = true
	result = snapshotWithSourcesOneActivity(t, backendModels, sources, "local-model", "expected 1 snapshot, got %d")
	if result[0].GPUWindow.State != MeasurementPartial {
		t.Fatalf("truncated -> %s, want partial", result[0].GPUWindow.State)
	}
}

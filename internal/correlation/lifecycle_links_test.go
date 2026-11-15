// Package correlation tests for LinkCurrentLifecycle.
package correlation

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

var lifecycleLinksFixtureTime = time.Date(2026, 11, 11, 10, 30, 0, 0, time.UTC)

func lifecycleLinksModel(backend, model, state string) LifecycleModel {
	ref, _ := ModelReference(backend, model)
	return LifecycleModel{
		BackendID:  backend,
		Epoch:      1,
		ModelRef:   ref,
		State:      state,
		ObservedAt: lifecycleLinksFixtureTime,
		// Attribution fields are always serialized.
		UpstreamGeneration:        activityLifecycleUpstreamGenerationUnknown,
		AliasResolution:           lifecycleAliasResolutionUnsupported,
		RequestIDMapping:          lifecycleRequestIDMappingUnsupported,
		ModelExecutionAttribution: lifecycleModelExecutionAttributionUnverified,
	}
}

func TestLinkCurrentLifecycleExactResolutionLinksCanonicalRef(t *testing.T) {
	canonRef, _ := ModelReference("backend", "canon")
	aliasRef, _ := ModelReference("backend", "shortalias")
	identities := []ActivityModelIdentity{
		{
			BackendID:        "backend",
			Epoch:            1,
			ActivityID:       "act-1",
			ReportedModelRef: aliasRef,
			CanonicalRef:     canonRef,
			State:            aliasStateExact,
			Source:           aliasSourceCatalogMetadata,
			Provenance:       activityProvenanceCatalogDeclared,
		},
	}
	report := LifecycleReport{
		Models: []LifecycleModel{
			lifecycleLinksModel("backend", "canon", lifecycleStateReady),
		},
	}
	obs, err := LinkCurrentLifecycle(identities, report)
	if err != nil {
		t.Fatal(err)
	}
	if len(obs) != 1 {
		t.Fatalf("expected 1 observation, got %d", len(obs))
	}
	o := obs[0]
	// Canonical ref agrees between identity and lifecycle.
	if o.CanonicalRef != canonRef {
		t.Fatalf("canonical_ref = %q, want %q", o.CanonicalRef, canonRef)
	}
	if o.ReportedModelRef != aliasRef {
		t.Fatalf("reported_model_ref = %q, want %q", o.ReportedModelRef, aliasRef)
	}
	// CurrentState is the observed lifecycle enum.
	if o.CurrentState != lifecycleStateReady {
		t.Fatalf("current_state = %q, want ready", o.CurrentState)
	}
	// ObservedAt points to the actual row.
	if o.ObservedAt == nil || !o.ObservedAt.Equal(lifecycleLinksFixtureTime) {
		t.Fatalf("observed_at = %v, want fixture time", o.ObservedAt)
	}
	if !o.Observed {
		t.Fatal("observed should be true")
	}
	// Fixed attribution fields.
	if o.StateAtActivity != activityLifecycleStateAtActivityUnknown {
		t.Fatalf("state_at_activity = %q, want unknown", o.StateAtActivity)
	}
	if o.Kind != activityLifecycleKind {
		t.Fatalf("kind = %q, want %q", o.Kind, activityLifecycleKind)
	}
	if o.ClockAlignmentUnverified != activityLifecycleClockAlignmentUnverified {
		t.Fatalf("clock_alignment_unverified = %v, want %v", o.ClockAlignmentUnverified, activityLifecycleClockAlignmentUnverified)
	}
	if o.UpstreamGeneration != activityLifecycleUpstreamGenerationUnknown {
		t.Fatalf("upstream_generation = %q", o.UpstreamGeneration)
	}
	if o.RequestIDMapping != activityLifecycleRequestIDMappingUnsupported {
		t.Fatalf("request_id_mapping = %q", o.RequestIDMapping)
	}
	if o.ExecutionAttribution != activityLifecycleExecutionAttributionUnverified {
		t.Fatalf("execution_attribution = %q", o.ExecutionAttribution)
	}
	// No raw labels leaked.
	if strings.Contains(o.CanonicalRef, "canon") || strings.Contains(o.ReportedModelRef, "shortalias") {
		t.Fatal("opaque ref must not leak raw model ID")
	}
}

func TestLinkCurrentLifecycleAbsentModelUnknownNotUnloaded(t *testing.T) {
	canonRef, _ := ModelReference("backend", "canon")
	identities := []ActivityModelIdentity{
		{
			BackendID:        "backend",
			Epoch:            1,
			ActivityID:       "act-1",
			ReportedModelRef: canonRef,
			CanonicalRef:     canonRef,
			State:            aliasStateExact,
		},
	}
	// No lifecycle model for canon -> absent.
	report := LifecycleReport{Models: []LifecycleModel{
		lifecycleLinksModel("backend", "other", lifecycleStateReady),
	}}
	obs, err := LinkCurrentLifecycle(identities, report)
	if err != nil {
		t.Fatal(err)
	}
	if len(obs) != 1 {
		t.Fatalf("expected 1 observation, got %d", len(obs))
	}
	o := obs[0]
	if o.CurrentState != aliasStateUnknown {
		t.Fatalf("current_state = %q, want unknown (absent)", o.CurrentState)
	}
	if o.Observed {
		t.Fatal("observed should be false for absent model")
	}
	if o.ObservedAt != nil {
		t.Fatalf("observed_at should be nil for absent model, got %v", o.ObservedAt)
	}
	// Never infer unloaded/stopped from absence.
	if o.CurrentState == "unloaded" || o.CurrentState == "stopped" || o.CurrentState == "offline" {
		t.Fatalf("must not infer unloaded/stopped from absence: %q", o.CurrentState)
	}
}

func TestLinkCurrentLifecycleAmbiguousIdentityUnknown(t *testing.T) {
	aliasRef, _ := ModelReference("backend", "alias1")
	identities := []ActivityModelIdentity{
		{
			BackendID:        "backend",
			Epoch:            1,
			ActivityID:       "act-1",
			ReportedModelRef: aliasRef,
			State:            aliasStateAmbiguous,
		},
	}
	report := LifecycleReport{
		Models: []LifecycleModel{
			lifecycleLinksModel("backend", "canon", lifecycleStateReady),
		},
	}
	obs, err := LinkCurrentLifecycle(identities, report)
	if err != nil {
		t.Fatal(err)
	}
	if len(obs) != 1 {
		t.Fatalf("expected 1 observation, got %d", len(obs))
	}
	o := obs[0]
	if o.CurrentState != aliasStateUnknown {
		t.Fatalf("current_state = %q, want unknown (ambiguous)", o.CurrentState)
	}
	if o.Observed {
		t.Fatal("observed should be false for ambiguous identity")
	}
	if o.CanonicalRef != "" {
		t.Fatal("ambiguous identity must omit canonical_ref")
	}
}

func TestLinkCurrentLifecycleDuplicateModelNoWinner(t *testing.T) {
	canonRef, _ := ModelReference("backend", "canon")
	identities := []ActivityModelIdentity{
		{
			BackendID:        "backend",
			Epoch:            1,
			ActivityID:       "act-1",
			ReportedModelRef: canonRef,
			CanonicalRef:     canonRef,
			State:            aliasStateExact,
		},
	}
	// Two lifecycle rows with the same backend+epoch+ModelRef: duplicate, no
	// winner.
	report := LifecycleReport{
		Models: []LifecycleModel{
			lifecycleLinksModel("backend", "canon", lifecycleStateReady),
			lifecycleLinksModel("backend", "canon", lifecycleStateStarting),
		},
	}
	obs, err := LinkCurrentLifecycle(identities, report)
	if err != nil {
		t.Fatal(err)
	}
	if len(obs) != 1 {
		t.Fatalf("expected 1 observation, got %d", len(obs))
	}
	o := obs[0]
	if o.CurrentState != aliasStateUnknown {
		t.Fatalf("current_state = %q, want unknown (duplicate, no winner)", o.CurrentState)
	}
	if o.Observed {
		t.Fatal("observed should be false for duplicate")
	}
}

func TestLinkCurrentLifecycleCrossBackendAndEpochGuard(t *testing.T) {
	canonRef1, _ := ModelReference("b1", "canon")
	canonRef2, _ := ModelReference("b2", "canon")
	// Same canonical ref string across backends must not match (scoped by
	// backend+epoch+ref).
	identities := []ActivityModelIdentity{
		{
			BackendID:        "b1",
			Epoch:            1,
			ActivityID:       "act-1",
			ReportedModelRef: canonRef1,
			CanonicalRef:     canonRef1,
			State:            aliasStateExact,
		},
		{
			BackendID:        "b1",
			Epoch:            2, // wrong epoch
			ActivityID:       "act-2",
			ReportedModelRef: canonRef1,
			CanonicalRef:     canonRef1,
			State:            aliasStateExact,
		},
		{
			BackendID:        "b2", // wrong backend
			Epoch:            1,
			ActivityID:       "act-3",
			ReportedModelRef: canonRef2,
			CanonicalRef:     canonRef2,
			State:            aliasStateExact,
		},
	}
	report := LifecycleReport{
		Models: []LifecycleModel{
			lifecycleLinksModel("b1", "canon", lifecycleStateReady),
		},
	}
	obs, err := LinkCurrentLifecycle(identities, report)
	if err != nil {
		t.Fatal(err)
	}
	if len(obs) != 3 {
		t.Fatalf("expected 3 observations, got %d", len(obs))
	}
	// First matches (b1, epoch 1, canon).
	if obs[0].CurrentState != lifecycleStateReady || !obs[0].Observed {
		t.Fatalf("first should match: %#v", obs[0])
	}
	// Second (wrong epoch) and third (wrong backend) are unknown.
	if obs[1].CurrentState != aliasStateUnknown || obs[1].Observed {
		t.Fatalf("second (wrong epoch) should be unknown: %#v", obs[1])
	}
	if obs[2].CurrentState != aliasStateUnknown || obs[2].Observed {
		t.Fatalf("third (wrong backend) should be unknown: %#v", obs[2])
	}
}

func TestLinkCurrentLifecycleBounds(t *testing.T) {
	// Too many identities.
	tooMany := make([]ActivityModelIdentity, lifecycleLinksMaxIdentities+1)
	for i := range tooMany {
		tooMany[i] = ActivityModelIdentity{BackendID: "b", Epoch: 1, ActivityID: fmt.Sprintf("%d", i)}
	}
	if _, err := LinkCurrentLifecycle(tooMany, LifecycleReport{}); err == nil {
		t.Fatal("expected error for over-bound identities")
	}
	// Too many models.
	tooManyModels := make([]LifecycleModel, lifecycleLinksMaxModels+1)
	for i := range tooManyModels {
		ref, _ := ModelReference("b", fmt.Sprintf("model-%d", i))
		tooManyModels[i] = LifecycleModel{BackendID: "b", Epoch: 1, ModelRef: ref, State: lifecycleStateReady, ObservedAt: lifecycleLinksFixtureTime}
	}
	if _, err := LinkCurrentLifecycle([]ActivityModelIdentity{{BackendID: "b", Epoch: 1, ActivityID: "a"}}, LifecycleReport{Models: tooManyModels}); err == nil {
		t.Fatal("expected error for over-bound models")
	}
}

func TestLinkCurrentLifecycleInvalidScope(t *testing.T) {
	cases := []struct {
		name       string
		identities []ActivityModelIdentity
		models     []LifecycleModel
	}{
		{"empty backend id", []ActivityModelIdentity{{BackendID: "", Epoch: 1, ActivityID: "a"}}, nil},
		{"empty activity id", []ActivityModelIdentity{{BackendID: "b", Epoch: 1, ActivityID: ""}}, nil},
		{"zero epoch", []ActivityModelIdentity{{BackendID: "b", Epoch: 0, ActivityID: "a"}}, nil},
		{"negative epoch", []ActivityModelIdentity{{BackendID: "b", Epoch: -1, ActivityID: "a"}}, nil},
		{"invalid model backend", nil, []LifecycleModel{{BackendID: "", Epoch: 1, ModelRef: "model-x", State: lifecycleStateReady, ObservedAt: lifecycleLinksFixtureTime}}},
		{"invalid model epoch", nil, []LifecycleModel{{BackendID: "b", Epoch: 0, ModelRef: "model-x", State: lifecycleStateReady, ObservedAt: lifecycleLinksFixtureTime}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := LinkCurrentLifecycle(tc.identities, LifecycleReport{Models: tc.models}); err == nil {
				t.Fatalf("%s: expected error, got nil", tc.name)
			}
		})
	}
}

func TestLinkCurrentLifecyclePreservesInputAndOrder(t *testing.T) {
	canonRef, _ := ModelReference("backend", "canon")
	aliasRef, _ := ModelReference("backend", "alias1")
	identities := []ActivityModelIdentity{
		{BackendID: "backend", Epoch: 1, ActivityID: "first", ReportedModelRef: aliasRef, CanonicalRef: canonRef, State: aliasStateExact},
		{BackendID: "backend", Epoch: 1, ActivityID: "second", ReportedModelRef: canonRef, CanonicalRef: canonRef, State: aliasStateExact},
	}
	report := LifecycleReport{
		Models: []LifecycleModel{
			lifecycleLinksModel("backend", "canon", lifecycleStateReady),
		},
	}
	obs, err := LinkCurrentLifecycle(identities, report)
	if err != nil {
		t.Fatal(err)
	}
	if len(obs) != 2 {
		t.Fatalf("expected 2 observations, got %d", len(obs))
	}
	// Order preserved.
	if obs[0].ActivityID != "first" || obs[1].ActivityID != "second" {
		t.Fatal("observation order not preserved")
	}
	// Purity: mutating inputs after the call must not affect output.
	identities[0].CanonicalRef = "mutated"
	report.Models[0].State = "mutated"
	if obs[0].CanonicalRef != canonRef || obs[0].CurrentState != lifecycleStateReady {
		t.Fatal("output changed after input mutation")
	}
}

func TestLinkCurrentLifecycleUnknownAndEmptyRef(t *testing.T) {
	unknownRef, _ := ModelReference("backend", "unknown")
	identities := []ActivityModelIdentity{
		{BackendID: "backend", Epoch: 1, ActivityID: "a", ReportedModelRef: unknownRef}, // no match
		{BackendID: "backend", Epoch: 1, ActivityID: "b", ReportedModelRef: ""},         // empty ref
	}
	report := LifecycleReport{
		Models: []LifecycleModel{
			lifecycleLinksModel("backend", "canon", lifecycleStateReady),
		},
	}
	obs, err := LinkCurrentLifecycle(identities, report)
	if err != nil {
		t.Fatal(err)
	}
	if len(obs) != 2 {
		t.Fatalf("expected 2 observations, got %d", len(obs))
	}
	for _, o := range obs {
		if o.CurrentState != aliasStateUnknown || o.Observed || o.ObservedAt != nil {
			t.Fatalf("unknown/empty should be unknown and unobserved: %#v", o)
		}
	}
}

func TestLinkCurrentLifecycleObservedAtPointerToRow(t *testing.T) {
	canonRef, _ := ModelReference("backend", "canon")
	identities := []ActivityModelIdentity{
		{BackendID: "backend", Epoch: 1, ActivityID: "a", ReportedModelRef: canonRef, CanonicalRef: canonRef, State: aliasStateExact},
	}
	report := LifecycleReport{
		Models: []LifecycleModel{
			lifecycleLinksModel("backend", "canon", lifecycleStateStarting),
		},
	}
	obs, err := LinkCurrentLifecycle(identities, report)
	if err != nil {
		t.Fatal(err)
	}
	if len(obs) != 1 {
		t.Fatalf("expected 1 observation, got %d", len(obs))
	}
	o := obs[0]
	if o.CurrentState != lifecycleStateStarting {
		t.Fatalf("current_state = %q, want starting", o.CurrentState)
	}
	if o.ObservedAt == nil || !o.ObservedAt.Equal(report.Models[0].ObservedAt) {
		t.Fatal("observed_at should point to the actual row's ObservedAt")
	}
	if !o.Observed {
		t.Fatal("observed should be true")
	}
}

func TestLinkCurrentLifecycleNoEcho(t *testing.T) {
	// The function must not echo the input back; it produces fresh
	// observations. Verify no raw input is reflected beyond the fixed fields.
	canonRef, _ := ModelReference("backend", "canon")
	identities := []ActivityModelIdentity{
		{BackendID: "backend", Epoch: 1, ActivityID: "a", ReportedModelRef: canonRef, CanonicalRef: canonRef, State: aliasStateExact},
	}
	report := LifecycleReport{
		Models: []LifecycleModel{
			lifecycleLinksModel("backend", "canon", lifecycleStateReady),
		},
	}
	obs, err := LinkCurrentLifecycle(identities, report)
	if err != nil {
		t.Fatal(err)
	}
	if len(obs) != 1 {
		t.Fatalf("expected 1 observation, got %d", len(obs))
	}
	o := obs[0]
	// Only fixed fields and opaque refs; no raw model ID, no source metadata
	// echoed as state.
	if o.StateAtActivity != activityLifecycleStateAtActivityUnknown {
		t.Fatalf("state_at_activity = %q", o.StateAtActivity)
	}
	if o.Kind != activityLifecycleKind {
		t.Fatalf("kind = %q", o.Kind)
	}
	if strings.Contains(o.CanonicalRef, "canon") {
		t.Fatal("canonical_ref must be an opaque hash")
	}
}

func TestLinkCurrentLifecycleInvalidRefRejected(t *testing.T) {
	canonRef, _ := ModelReference("backend", "canon")
	// Invalid canonical ref (not a bounded opaque ModelReference) is rejected.
	identities := []ActivityModelIdentity{
		{
			BackendID:        "backend",
			Epoch:            1,
			ActivityID:       "act-1",
			ReportedModelRef: canonRef,
			CanonicalRef:     "model-short", // invalid: not 32 hex chars
			State:            aliasStateExact,
		},
	}
	report := LifecycleReport{
		Models: []LifecycleModel{
			lifecycleLinksModel("backend", "canon", lifecycleStateReady),
		},
	}
	if _, err := LinkCurrentLifecycle(identities, report); err == nil {
		t.Fatal("expected error for invalid canonical ref")
	}
	// Invalid reported ref is rejected.
	identities[0].CanonicalRef = canonRef
	identities[0].ReportedModelRef = "model-short"
	if _, err := LinkCurrentLifecycle(identities, report); err == nil {
		t.Fatal("expected error for invalid reported model ref")
	}
}

func TestLinkCurrentLifecycleInvalidModelRefRejected(t *testing.T) {
	identities := []ActivityModelIdentity{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1"},
	}
	// Invalid lifecycle model ref is rejected.
	report := LifecycleReport{
		Models: []LifecycleModel{
			{BackendID: "backend", Epoch: 1, ModelRef: "model-short", State: lifecycleStateReady, ObservedAt: lifecycleLinksFixtureTime},
		},
	}
	if _, err := LinkCurrentLifecycle(identities, report); err == nil {
		t.Fatal("expected error for invalid lifecycle model ref")
	}
}

func TestLinkCurrentLifecycleObservedAtIsolation(t *testing.T) {
	canonRef, _ := ModelReference("backend", "canon")
	newTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	// Input mutation after the call must not change the output.
	{
		identities := []ActivityModelIdentity{
			{BackendID: "backend", Epoch: 1, ActivityID: "a", ReportedModelRef: canonRef, CanonicalRef: canonRef, State: aliasStateExact},
		}
		report := LifecycleReport{
			Models: []LifecycleModel{
				lifecycleLinksModel("backend", "canon", lifecycleStateReady),
			},
		}
		obs, err := LinkCurrentLifecycle(identities, report)
		if err != nil {
			t.Fatal(err)
		}
		if len(obs) != 1 {
			t.Fatalf("expected 1 observation, got %d", len(obs))
		}
		o := obs[0]
		if o.ObservedAt == nil || !o.ObservedAt.Equal(lifecycleLinksFixtureTime) {
			t.Fatal("observed_at should point to fixture time")
		}
		report.Models[0].ObservedAt = newTime
		if !o.ObservedAt.Equal(lifecycleLinksFixtureTime) {
			t.Fatal("output observed_at changed after input mutation")
		}
	}

	// Output mutation after the call must not change the input report.
	{
		identities := []ActivityModelIdentity{
			{BackendID: "backend", Epoch: 1, ActivityID: "a", ReportedModelRef: canonRef, CanonicalRef: canonRef, State: aliasStateExact},
		}
		report := LifecycleReport{
			Models: []LifecycleModel{
				lifecycleLinksModel("backend", "canon", lifecycleStateReady),
			},
		}
		obs, err := LinkCurrentLifecycle(identities, report)
		if err != nil {
			t.Fatal(err)
		}
		if len(obs) != 1 {
			t.Fatalf("expected 1 observation, got %d", len(obs))
		}
		o := obs[0]
		if o.ObservedAt != nil {
			*o.ObservedAt = newTime
		}
		if !report.Models[0].ObservedAt.Equal(lifecycleLinksFixtureTime) {
			t.Fatal("input report changed after output mutation")
		}
	}
}

func TestLinkCurrentLifecycleUnknownIdentityWithCanonicalDoesNotMatch(t *testing.T) {
	canonRef, _ := ModelReference("backend", "canon")
	// Ambiguous identity with a populated canonical ref must not attach it
	// or claim a match, even when a lifecycle row shares the ref.
	identities := []ActivityModelIdentity{
		{
			BackendID:        "backend",
			Epoch:            1,
			ActivityID:       "act-1",
			ReportedModelRef: canonRef,
			CanonicalRef:     canonRef,
			State:            aliasStateAmbiguous,
		},
	}
	report := LifecycleReport{
		Models: []LifecycleModel{
			lifecycleLinksModel("backend", "canon", lifecycleStateReady),
		},
	}
	obs, err := LinkCurrentLifecycle(identities, report)
	if err != nil {
		t.Fatal(err)
	}
	if len(obs) != 1 {
		t.Fatalf("expected 1 observation, got %d", len(obs))
	}
	o := obs[0]
	if o.CurrentState != aliasStateUnknown {
		t.Fatalf("current_state = %q, want unknown (ambiguous)", o.CurrentState)
	}
	if o.Observed {
		t.Fatal("observed should be false for ambiguous identity")
	}
	if o.CanonicalRef != "" {
		t.Fatal("ambiguous identity with canonical ref must not attach it")
	}
	if o.ObservedAt != nil {
		t.Fatal("observed_at should be nil for ambiguous identity")
	}
}

func TestLinkCurrentLifecycleStateAtActivityUnknownEvenReady(t *testing.T) {
	canonRef, _ := ModelReference("backend", "canon")
	identities := []ActivityModelIdentity{
		{BackendID: "backend", Epoch: 1, ActivityID: "a", ReportedModelRef: canonRef, CanonicalRef: canonRef, State: aliasStateExact},
	}
	report := LifecycleReport{
		Models: []LifecycleModel{
			lifecycleLinksModel("backend", "canon", lifecycleStateReady),
		},
	}
	obs, err := LinkCurrentLifecycle(identities, report)
	if err != nil {
		t.Fatal(err)
	}
	if len(obs) != 1 {
		t.Fatalf("expected 1 observation, got %d", len(obs))
	}
	o := obs[0]
	if o.CurrentState != lifecycleStateReady {
		t.Fatalf("current_state = %q, want ready", o.CurrentState)
	}
	if o.StateAtActivity != activityLifecycleStateAtActivityUnknown {
		t.Fatalf("state_at_activity = %q, want unknown even when current_state is ready", o.StateAtActivity)
	}
}

// Package correlation tests for AssociateLifecycleEvents.
package correlation

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

var lifecycleAssociationFixtureTime = time.Date(2026, 11, 11, 11, 0, 0, 0, time.UTC)

func lifecycleAssociationEvent(backend, model, oldState, newState string, observedAt time.Time, sourceTime *time.Time) LifecycleTransitionEvidence {
	ref, _ := ModelReference(backend, model)
	return LifecycleTransitionEvidence{
		BackendID:  backend,
		Epoch:      1,
		ModelRef:   ref,
		OldState:   oldState,
		NewState:   newState,
		ObservedAt: observedAt,
		SourceTime: sourceTime,
	}
}

func TestLifecycleAssociationUsesVerifiedCanonicalReference(t *testing.T) {
	alias, _ := ModelReference("backend", "alias")
	canonical, _ := ModelReference("backend", "canonical")
	at := lifecycleAssociationFixtureTime
	activity := &ActivityRecord{BackendID: "backend", Epoch: 1, ActivityID: "1", Timestamp: at, ReportedModelRef: alias}
	identity := ActivityModelIdentity{BackendID: "backend", Epoch: 1, ActivityID: "1", State: aliasStateExact, ReportedModelRef: alias, CanonicalRef: canonical}
	event := LifecycleTransitionEvidence{BackendID: "backend", Epoch: 1, ModelRef: canonical, OldState: "starting", NewState: "ready", ObservedAt: at, SourceTime: &at}
	rows, err := AssociateLifecycleEvents([]*ActivityRecord{activity}, []ActivityModelIdentity{identity}, []LifecycleTransitionEvidence{event}, DefaultAssociationWindow())
	if err != nil || len(rows) != 1 || rows[0].MatchedEventCount != 1 || rows[0].Kind != lifecycleAssociationKind {
		t.Fatalf("verified alias/canonical association failed: rows=%+v error=%v", rows, err)
	}
	if rows[0].LifecycleState != "unknown" || rows[0].RequestIDMapping != "unsupported" {
		t.Fatal("catalog identity implied historical state or request identity")
	}
}

func TestAssociateLifecycleEventsSourceTimeWithinWindow(t *testing.T) {
	ref, _ := ModelReference("backend", "canon")
	actTime := lifecycleAssociationFixtureTime
	eventTime := actTime.Add(time.Second)
	activities := []*ActivityRecord{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", Timestamp: actTime},
	}
	identities := []ActivityModelIdentity{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", ReportedModelRef: ref, CanonicalRef: ref, State: aliasStateExact},
	}
	events := []LifecycleTransitionEvidence{
		{BackendID: "backend", Epoch: 1, ModelRef: ref, OldState: lifecycleStateStarting, NewState: lifecycleStateReady, ObservedAt: eventTime, SourceTime: &eventTime},
	}
	rows, err := AssociateLifecycleEvents(activities, identities, events, DefaultAssociationWindow())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 association, got %d", len(rows))
	}
	r := rows[0]
	if r.Kind != lifecycleAssociationKind {
		t.Fatalf("kind = %q, want %q", r.Kind, lifecycleAssociationKind)
	}
	if r.MatchedEventCount != 1 {
		t.Fatalf("matched count = %d, want 1", r.MatchedEventCount)
	}
	if !r.FirstEventTime.Equal(eventTime) || !r.LastEventTime.Equal(eventTime) {
		t.Fatalf("event times = %v %v, want %v", r.FirstEventTime, r.LastEventTime, eventTime)
	}
	if r.LifecycleState != "unknown" {
		t.Fatalf("lifecycle_state = %q, want unknown", r.LifecycleState)
	}
	if r.ClockAlignmentUnverified != true {
		t.Fatal("clock_alignment_unverified should be true")
	}
	if r.UpstreamGeneration != "unknown" {
		t.Fatalf("upstream_generation = %q", r.UpstreamGeneration)
	}
	if r.RequestIDMapping != "unsupported" {
		t.Fatalf("request_id_mapping = %q", r.RequestIDMapping)
	}
	if r.ExecutionAttribution != "unverified" {
		t.Fatalf("execution_attribution = %q", r.ExecutionAttribution)
	}
	if r.Attribution != "catalog_declared" {
		t.Fatalf("attribution = %q", r.Attribution)
	}
	if r.Reason != "" {
		t.Fatalf("reason should be empty for valid association, got %q", r.Reason)
	}
	// No raw model ID leaked.
	if strings.Contains(r.CanonicalRef, "canon") {
		t.Fatal("canonical_ref must be opaque hash")
	}
}

func TestAssociateLifecycleEventsObservedAtOnlyUnavailable(t *testing.T) {
	ref, _ := ModelReference("backend", "canon")
	actTime := lifecycleAssociationFixtureTime
	observedTime := actTime.Add(time.Second)
	// No SourceTime: only local ObservedAt -> unavailable.
	events := []LifecycleTransitionEvidence{
		{BackendID: "backend", Epoch: 1, ModelRef: ref, OldState: lifecycleStateStarting, NewState: lifecycleStateReady, ObservedAt: observedTime},
	}
	activities := []*ActivityRecord{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", Timestamp: actTime},
	}
	identities := []ActivityModelIdentity{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", ReportedModelRef: ref, CanonicalRef: ref, State: aliasStateExact},
	}
	rows, err := AssociateLifecycleEvents(activities, identities, events, DefaultAssociationWindow())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 association, got %d", len(rows))
	}
	r := rows[0]
	if r.Kind != lifecycleAssociationUnavailable {
		t.Fatalf("kind = %q, want unavailable", r.Kind)
	}
	if r.Reason != lifecycleAssociationReasonSourceTimeUnavailable {
		t.Fatalf("reason = %q, want source_time_unavailable", r.Reason)
	}
	// MatchedEventCount is zero when no valid SourceTime is in the window;
	// the observed transition count is reported separately.
	if r.MatchedEventCount != 0 {
		t.Fatalf("matched count = %d, want 0 (no valid source clock in window)", r.MatchedEventCount)
	}
	if r.ObservedEventCount != 1 {
		t.Fatalf("observed count = %d, want 1 (scoped transition metadata)", r.ObservedEventCount)
	}
	if !r.FirstEventTime.IsZero() || !r.LastEventTime.IsZero() {
		t.Fatal("first/last event time should be zero when no source clock")
	}
}

func TestAssociateLifecycleEventsWrongModelNoMatch(t *testing.T) {
	ref, _ := ModelReference("backend", "canon")
	actTime := lifecycleAssociationFixtureTime
	// Event for a different model ref: no match.
	otherRef, _ := ModelReference("backend", "other")
	events := []LifecycleTransitionEvidence{
		{BackendID: "backend", Epoch: 1, ModelRef: otherRef, OldState: lifecycleStateStarting, NewState: lifecycleStateReady, ObservedAt: actTime, SourceTime: &actTime},
	}
	activities := []*ActivityRecord{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", Timestamp: actTime},
	}
	identities := []ActivityModelIdentity{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", ReportedModelRef: ref, CanonicalRef: ref, State: aliasStateExact},
	}
	rows, err := AssociateLifecycleEvents(activities, identities, events, DefaultAssociationWindow())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 association, got %d", len(rows))
	}
	r := rows[0]
	if r.Kind != lifecycleAssociationUnavailable {
		t.Fatalf("kind = %q, want unavailable", r.Kind)
	}
	if r.Reason != lifecycleAssociationReasonNoMatchingEvent {
		t.Fatalf("reason = %q, want no_matching_event", r.Reason)
	}
}

func TestAssociateLifecycleEventsWrongBackendOrEpochNoMatch(t *testing.T) {
	ref, _ := ModelReference("backend", "canon")
	actTime := lifecycleAssociationFixtureTime
	events := []LifecycleTransitionEvidence{
		{BackendID: "backend", Epoch: 1, ModelRef: ref, OldState: lifecycleStateStarting, NewState: lifecycleStateReady, ObservedAt: actTime, SourceTime: &actTime},
	}
	activities := []*ActivityRecord{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", Timestamp: actTime},
	}
	// Identity with wrong epoch: not found by (backend, epoch, ActivityID) key.
	identities := []ActivityModelIdentity{
		{BackendID: "backend", Epoch: 2, ActivityID: "act-1", ReportedModelRef: ref, CanonicalRef: ref, State: aliasStateExact},
	}
	rows, err := AssociateLifecycleEvents(activities, identities, events, DefaultAssociationWindow())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 association, got %d", len(rows))
	}
	r := rows[0]
	if r.Kind != lifecycleAssociationUnavailable {
		t.Fatalf("kind = %q, want unavailable", r.Kind)
	}
	// Wrong epoch means the identity is not resolved by key lookup.
	if r.Reason != lifecycleAssociationReasonIdentityUnresolved {
		t.Fatalf("reason = %q, want identity_unresolved", r.Reason)
	}
}

func TestAssociateLifecycleEventsAmbiguousIdentityUnknown(t *testing.T) {
	ref, _ := ModelReference("backend", "canon")
	actTime := lifecycleAssociationFixtureTime
	events := []LifecycleTransitionEvidence{
		{BackendID: "backend", Epoch: 1, ModelRef: ref, OldState: lifecycleStateStarting, NewState: lifecycleStateReady, ObservedAt: actTime, SourceTime: &actTime},
	}
	activities := []*ActivityRecord{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", Timestamp: actTime},
	}
	// Ambiguous identity: no winner, no match.
	identities := []ActivityModelIdentity{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", ReportedModelRef: ref, State: aliasStateAmbiguous},
	}
	rows, err := AssociateLifecycleEvents(activities, identities, events, DefaultAssociationWindow())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 association, got %d", len(rows))
	}
	r := rows[0]
	if r.Kind != lifecycleAssociationUnavailable {
		t.Fatalf("kind = %q, want unavailable", r.Kind)
	}
	if r.CanonicalRef != "" {
		t.Fatal("ambiguous identity must omit canonical_ref")
	}
}

func TestAssociateLifecycleEventsMultipleTransitions(t *testing.T) {
	ref, _ := ModelReference("backend", "canon")
	actTime := lifecycleAssociationFixtureTime
	// Two transitions for the same model, both SourceTimes in window.
	earlier := actTime.Add(-time.Second)
	later := actTime.Add(time.Second)
	events := []LifecycleTransitionEvidence{
		{BackendID: "backend", Epoch: 1, ModelRef: ref, OldState: lifecycleStateStarting, NewState: lifecycleStateReady, ObservedAt: earlier, SourceTime: &earlier},
		{BackendID: "backend", Epoch: 1, ModelRef: ref, OldState: lifecycleStateReady, NewState: lifecycleStateStopping, ObservedAt: later, SourceTime: &later},
	}
	activities := []*ActivityRecord{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", Timestamp: actTime},
	}
	identities := []ActivityModelIdentity{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", ReportedModelRef: ref, CanonicalRef: ref, State: aliasStateExact},
	}
	rows, err := AssociateLifecycleEvents(activities, identities, events, DefaultAssociationWindow())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 association, got %d", len(rows))
	}
	r := rows[0]
	if r.Kind != lifecycleAssociationKind {
		t.Fatalf("kind = %q, want %q", r.Kind, lifecycleAssociationKind)
	}
	if r.MatchedEventCount != 2 {
		t.Fatalf("matched count = %d, want 2", r.MatchedEventCount)
	}
}

func TestAssociateLifecycleEventsSourceConflictNotMerged(t *testing.T) {
	ref, _ := ModelReference("backend", "canon")
	actTime := lifecycleAssociationFixtureTime
	// Same SourceEventID with different payloads: conflict, not merged.
	events := []LifecycleTransitionEvidence{
		{BackendID: "backend", Epoch: 1, ModelRef: ref, OldState: lifecycleStateStarting, NewState: lifecycleStateReady, ObservedAt: actTime, SourceTime: &actTime, SourceEventID: "evt-1"},
		{BackendID: "backend", Epoch: 1, ModelRef: ref, OldState: lifecycleStateReady, NewState: lifecycleStateStopping, ObservedAt: actTime, SourceTime: &actTime, SourceEventID: "evt-1"},
	}
	activities := []*ActivityRecord{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", Timestamp: actTime},
	}
	identities := []ActivityModelIdentity{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", ReportedModelRef: ref, CanonicalRef: ref, State: aliasStateExact},
	}
	rows, err := AssociateLifecycleEvents(activities, identities, events, DefaultAssociationWindow())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 association, got %d", len(rows))
	}
	r := rows[0]
	// A conflicting identity never emits an approximate association.
	if r.Kind != lifecycleAssociationUnavailable {
		t.Fatalf("kind = %q, want unavailable (conflicting identity)", r.Kind)
	}
	if !r.Conflict {
		t.Fatal("conflict should be flagged for duplicate source event ID with different payload")
	}
	if r.MatchedEventCount != 0 {
		t.Fatalf("matched count = %d, want 0 (conflict, no approx)", r.MatchedEventCount)
	}
	if r.ObservedEventCount != 1 {
		t.Fatalf("observed count = %d, want 1 (distinct scoped event)", r.ObservedEventCount)
	}
}

func TestAssociateLifecycleEventsReceiptVsSourceClockOrder(t *testing.T) {
	ref, _ := ModelReference("backend", "canon")
	actTime := lifecycleAssociationFixtureTime
	// Receipt order: event B (later SourceTime) received before event A
	// (earlier SourceTime). Out-of-order SourceTimes sort stably for
	// first/last.
	later := actTime.Add(time.Second)
	earlier := actTime.Add(-time.Second)
	events := []LifecycleTransitionEvidence{
		{BackendID: "backend", Epoch: 1, ModelRef: ref, OldState: lifecycleStateStarting, NewState: lifecycleStateReady, ObservedAt: actTime, SourceTime: &later},
		{BackendID: "backend", Epoch: 1, ModelRef: ref, OldState: lifecycleStateReady, NewState: lifecycleStateStopping, ObservedAt: actTime, SourceTime: &earlier},
	}
	activities := []*ActivityRecord{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", Timestamp: actTime},
	}
	identities := []ActivityModelIdentity{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", ReportedModelRef: ref, CanonicalRef: ref, State: aliasStateExact},
	}
	rows, err := AssociateLifecycleEvents(activities, identities, events, DefaultAssociationWindow())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 association, got %d", len(rows))
	}
	r := rows[0]
	if r.Kind != lifecycleAssociationKind {
		t.Fatalf("kind = %q, want %q", r.Kind, lifecycleAssociationKind)
	}
	// FirstEventTime should be the earliest SourceTime (actTime - 1s), not the
	// first-received (actTime + 1s).
	if !r.FirstEventTime.Equal(actTime.Add(-time.Second)) {
		t.Fatalf("first event time = %v, want %v (stable sort by SourceTime)", r.FirstEventTime, actTime.Add(-time.Second))
	}
	if !r.LastEventTime.Equal(actTime.Add(time.Second)) {
		t.Fatalf("last event time = %v, want %v", r.LastEventTime, actTime.Add(time.Second))
	}
}

func TestAssociateLifecycleEventsInvalidFieldsReturnError(t *testing.T) {
	ref, _ := ModelReference("backend", "canon")
	actTime := lifecycleAssociationFixtureTime
	activities := []*ActivityRecord{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", Timestamp: actTime},
	}
	identities := []ActivityModelIdentity{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", ReportedModelRef: ref, CanonicalRef: ref, State: aliasStateExact},
	}
	// Invalid old state.
	events := []LifecycleTransitionEvidence{
		{BackendID: "backend", Epoch: 1, ModelRef: ref, OldState: "invalid_state", NewState: lifecycleStateReady, ObservedAt: actTime, SourceTime: &actTime},
	}
	if _, err := AssociateLifecycleEvents(activities, identities, events, DefaultAssociationWindow()); err == nil {
		t.Fatal("expected error for invalid event old state")
	}
	// Invalid model ref (non-empty, malformed).
	events[0].OldState = lifecycleStateStarting
	events[0].ModelRef = "model-short"
	if _, err := AssociateLifecycleEvents(activities, identities, events, DefaultAssociationWindow()); err == nil {
		t.Fatal("expected error for invalid event model ref")
	}
}

func TestAssociateLifecycleEventsPointerCopies(t *testing.T) {
	ref, _ := ModelReference("backend", "canon")
	actTime := lifecycleAssociationFixtureTime
	eventTime := actTime.Add(time.Second)
	activities := []*ActivityRecord{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", Timestamp: actTime},
	}
	identities := []ActivityModelIdentity{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", ReportedModelRef: ref, CanonicalRef: ref, State: aliasStateExact},
	}
	events := []LifecycleTransitionEvidence{
		{BackendID: "backend", Epoch: 1, ModelRef: ref, OldState: lifecycleStateStarting, NewState: lifecycleStateReady, ObservedAt: actTime, SourceTime: &eventTime},
	}
	rows, err := AssociateLifecycleEvents(activities, identities, events, DefaultAssociationWindow())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 association, got %d", len(rows))
	}
	r := rows[0]
	// Save original values before mutating inputs.
	originalEventTime := eventTime
	originalWindowStart := actTime.Add(-2 * time.Second)
	// Output times are copies: mutating input after the call must not change
	// output.
	newTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	events[0].ObservedAt = newTime
	*events[0].SourceTime = newTime
	activities[0].Timestamp = newTime
	if !r.FirstEventTime.Equal(originalEventTime) {
		t.Fatal("output first event time changed after input mutation")
	}
	if !r.LastEventTime.Equal(originalEventTime) {
		t.Fatal("output last event time changed after input mutation")
	}
	if !r.WindowStart.Equal(originalWindowStart) {
		t.Fatal("output window start changed after input mutation")
	}
}

func TestAssociateLifecycleEventsBounds(t *testing.T) {
	// Too many events.
	tooMany := make([]LifecycleTransitionEvidence, lifecycleAssociationMaxEvents+1)
	for i := range tooMany {
		ref, _ := ModelReference("b", fmt.Sprintf("model-%d", i))
		tooMany[i] = LifecycleTransitionEvidence{
			BackendID: "b", Epoch: 1, ModelRef: ref,
			OldState: lifecycleStateStarting, NewState: lifecycleStateReady,
			ObservedAt: lifecycleAssociationFixtureTime,
		}
	}
	if _, err := AssociateLifecycleEvents(nil, nil, tooMany, DefaultAssociationWindow()); err == nil {
		t.Fatal("expected error for over-bound events")
	}
	// Too many activities.
	tooManyAct := make([]*ActivityRecord, 1001)
	for i := range tooManyAct {
		tooManyAct[i] = &ActivityRecord{BackendID: "b", Epoch: 1, ActivityID: fmt.Sprintf("%d", i), Timestamp: lifecycleAssociationFixtureTime}
	}
	if _, err := AssociateLifecycleEvents(tooManyAct, nil, nil, DefaultAssociationWindow()); err == nil {
		t.Fatal("expected error for over-bound activities")
	}
	// Window out of range.
	if _, err := AssociateLifecycleEvents(nil, nil, nil, AssociationWindow{Before: -1, After: 2 * time.Second}); err == nil {
		t.Fatal("expected error for negative before window")
	}
	if _, err := AssociateLifecycleEvents(nil, nil, nil, AssociationWindow{Before: 2 * time.Second, After: time.Minute + time.Second}); err == nil {
		t.Fatal("expected error for after window > 1 minute")
	}
}

func TestAssociateLifecycleEventsConcurrentPure(t *testing.T) {
	ref, _ := ModelReference("backend", "canon")
	actTime := lifecycleAssociationFixtureTime
	eventTime := actTime.Add(time.Second)
	activities := []*ActivityRecord{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", Timestamp: actTime},
	}
	identities := []ActivityModelIdentity{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", ReportedModelRef: ref, CanonicalRef: ref, State: aliasStateExact},
	}
	events := []LifecycleTransitionEvidence{
		{BackendID: "backend", Epoch: 1, ModelRef: ref, OldState: lifecycleStateStarting, NewState: lifecycleStateReady, ObservedAt: actTime, SourceTime: &eventTime},
	}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rows, err := AssociateLifecycleEvents(activities, identities, events, DefaultAssociationWindow())
			if err != nil || len(rows) != 1 {
				t.Errorf("concurrent call failed: err=%v len=%d", err, len(rows))
			}
		}()
	}
	wg.Wait()
}

func TestAssociateLifecycleEventsNoHistoricalInference(t *testing.T) {
	ref, _ := ModelReference("backend", "canon")
	actTime := lifecycleAssociationFixtureTime
	// A ready event near the activity must not set state at activity to
	// ready; it remains unknown.
	events := []LifecycleTransitionEvidence{
		{BackendID: "backend", Epoch: 1, ModelRef: ref, OldState: lifecycleStateStopping, NewState: lifecycleStateReady, ObservedAt: actTime, SourceTime: &actTime},
	}
	activities := []*ActivityRecord{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", Timestamp: actTime},
	}
	identities := []ActivityModelIdentity{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", ReportedModelRef: ref, CanonicalRef: ref, State: aliasStateExact},
	}
	rows, err := AssociateLifecycleEvents(activities, identities, events, DefaultAssociationWindow())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 association, got %d", len(rows))
	}
	r := rows[0]
	if r.Kind != lifecycleAssociationKind {
		t.Fatalf("kind = %q, want %q", r.Kind, lifecycleAssociationKind)
	}
	if r.LifecycleState != "unknown" {
		t.Fatalf("lifecycle_state = %q, want unknown even with nearby ready event", r.LifecycleState)
	}
}

func TestAssociateLifecycleEventsEmptyModelRefUnknown(t *testing.T) {
	// Empty event ModelRef: cannot match any identity.
	actTime := lifecycleAssociationFixtureTime
	activities := []*ActivityRecord{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", Timestamp: actTime},
	}
	identities := []ActivityModelIdentity{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", ReportedModelRef: "", CanonicalRef: "", State: aliasStateExact},
	}
	events := []LifecycleTransitionEvidence{
		{BackendID: "backend", Epoch: 1, ModelRef: "", OldState: lifecycleStateStarting, NewState: lifecycleStateReady, ObservedAt: actTime, SourceTime: &actTime},
	}
	rows, err := AssociateLifecycleEvents(activities, identities, events, DefaultAssociationWindow())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 association, got %d", len(rows))
	}
	r := rows[0]
	if r.Kind != lifecycleAssociationUnavailable {
		t.Fatalf("kind = %q, want unavailable", r.Kind)
	}
}

func TestAssociateLifecycleEventsJSONNoSecretLeak(t *testing.T) {
	ref, _ := ModelReference("backend", "canon")
	actTime := lifecycleAssociationFixtureTime
	activities := []*ActivityRecord{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", Timestamp: actTime},
	}
	identities := []ActivityModelIdentity{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", ReportedModelRef: ref, CanonicalRef: ref, State: aliasStateExact},
	}
	events := []LifecycleTransitionEvidence{
		{BackendID: "backend", Epoch: 1, ModelRef: ref, OldState: lifecycleStateStarting, NewState: lifecycleStateReady, ObservedAt: actTime, SourceTime: &actTime, SourceEventID: "secret-event-id"},
	}
	rows, err := AssociateLifecycleEvents(activities, identities, events, DefaultAssociationWindow())
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	// Unmarshal into maps so we inspect values, not field names.
	var raw []map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	for _, row := range raw {
		for _, v := range row {
			if s, ok := v.(string); ok {
				if strings.Contains(s, "secret-event-id") {
					t.Fatal("source event id (secret) must not appear in JSON output")
				}
				if strings.Contains(s, "canon") {
					t.Fatal("raw model ID must not appear in JSON output")
				}
			}
		}
	}
}

func TestAssociateLifecycleEventsSourceTimeOutsideWindow(t *testing.T) {
	ref, _ := ModelReference("backend", "canon")
	actTime := lifecycleAssociationFixtureTime
	// SourceTime far outside the window.
	outside := actTime.Add(time.Hour)
	events := []LifecycleTransitionEvidence{
		{BackendID: "backend", Epoch: 1, ModelRef: ref, OldState: lifecycleStateStarting, NewState: lifecycleStateReady, ObservedAt: actTime, SourceTime: &outside},
	}
	activities := []*ActivityRecord{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", Timestamp: actTime},
	}
	identities := []ActivityModelIdentity{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", ReportedModelRef: ref, CanonicalRef: ref, State: aliasStateExact},
	}
	rows, err := AssociateLifecycleEvents(activities, identities, events, DefaultAssociationWindow())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 association, got %d", len(rows))
	}
	r := rows[0]
	if r.Kind != lifecycleAssociationUnavailable {
		t.Fatalf("kind = %q, want unavailable", r.Kind)
	}
	if r.Reason != lifecycleAssociationReasonOutsideWindow {
		t.Fatalf("reason = %q, want outside_window", r.Reason)
	}
}

func TestAssociateLifecycleEventsShuffledIdentityOrder(t *testing.T) {
	ref, _ := ModelReference("backend", "canon")
	actTime := lifecycleAssociationFixtureTime
	eventTime := actTime.Add(time.Second)
	// Identities are in reverse order relative to activities. Map lookup by
	// (backend, epoch, ActivityID) must match each activity to its own
	// identity, not by positional index.
	activities := []*ActivityRecord{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-2", Timestamp: actTime},
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", Timestamp: actTime},
	}
	identities := []ActivityModelIdentity{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", ReportedModelRef: ref, CanonicalRef: ref, State: aliasStateExact},
		{BackendID: "backend", Epoch: 1, ActivityID: "act-2", ReportedModelRef: ref, CanonicalRef: ref, State: aliasStateExact},
	}
	events := []LifecycleTransitionEvidence{
		{BackendID: "backend", Epoch: 1, ModelRef: ref, OldState: lifecycleStateStarting, NewState: lifecycleStateReady, ObservedAt: actTime, SourceTime: &eventTime},
	}
	rows, err := AssociateLifecycleEvents(activities, identities, events, DefaultAssociationWindow())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 associations, got %d", len(rows))
	}
	for _, r := range rows {
		if r.Kind != lifecycleAssociationKind {
			t.Fatalf("kind = %q for %s, want %q", r.Kind, r.ActivityID, lifecycleAssociationKind)
		}
		if r.MatchedEventCount != 1 {
			t.Fatalf("matched count = %d for %s, want 1", r.MatchedEventCount, r.ActivityID)
		}
		if r.ObservedEventCount != 1 {
			t.Fatalf("observed count = %d for %s, want 1", r.ObservedEventCount, r.ActivityID)
		}
		if !r.FirstEventTime.Equal(eventTime) || !r.LastEventTime.Equal(eventTime) {
			t.Fatalf("event times = %v %v for %s, want %v", r.FirstEventTime, r.LastEventTime, r.ActivityID, eventTime)
		}
	}
}

func TestAssociateLifecycleEventsCrossBackendNoBorrow(t *testing.T) {
	ref, _ := ModelReference("backend", "canon")
	actTime := lifecycleAssociationFixtureTime
	eventTime := actTime.Add(time.Second)
	// Activity on backend-a, identity on backend-b (same ActivityID). Must not
	// borrow the identity across backends.
	activities := []*ActivityRecord{
		{BackendID: "backend-a", Epoch: 1, ActivityID: "act-1", Timestamp: actTime},
	}
	identities := []ActivityModelIdentity{
		{BackendID: "backend-b", Epoch: 1, ActivityID: "act-1", ReportedModelRef: ref, CanonicalRef: ref, State: aliasStateExact},
	}
	events := []LifecycleTransitionEvidence{
		{BackendID: "backend-a", Epoch: 1, ModelRef: ref, OldState: lifecycleStateStarting, NewState: lifecycleStateReady, ObservedAt: actTime, SourceTime: &eventTime},
	}
	rows, err := AssociateLifecycleEvents(activities, identities, events, DefaultAssociationWindow())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 association, got %d", len(rows))
	}
	r := rows[0]
	if r.Kind != lifecycleAssociationUnavailable {
		t.Fatalf("kind = %q, want unavailable (cross-backend borrow)", r.Kind)
	}
	if r.Reason != lifecycleAssociationReasonIdentityUnresolved {
		t.Fatalf("reason = %q, want identity_unresolved", r.Reason)
	}
}

func TestAssociateLifecycleEventsDuplicateIdentityAmbiguous(t *testing.T) {
	ref, _ := ModelReference("backend", "canon")
	actTime := lifecycleAssociationFixtureTime
	// Two identical identities for the same (backend, epoch, ActivityID):
	// ambiguous, no winner is picked even if identical.
	activities := []*ActivityRecord{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", Timestamp: actTime},
	}
	identities := []ActivityModelIdentity{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", ReportedModelRef: ref, CanonicalRef: ref, State: aliasStateExact},
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", ReportedModelRef: ref, CanonicalRef: ref, State: aliasStateExact},
	}
	events := []LifecycleTransitionEvidence{
		{BackendID: "backend", Epoch: 1, ModelRef: ref, OldState: lifecycleStateStarting, NewState: lifecycleStateReady, ObservedAt: actTime, SourceTime: &actTime},
	}
	rows, err := AssociateLifecycleEvents(activities, identities, events, DefaultAssociationWindow())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 association, got %d", len(rows))
	}
	r := rows[0]
	if r.Kind != lifecycleAssociationUnavailable {
		t.Fatalf("kind = %q, want unavailable (ambiguous identity)", r.Kind)
	}
	if r.Reason != lifecycleAssociationReasonIdentityUnresolved {
		t.Fatalf("reason = %q, want identity_unresolved", r.Reason)
	}
	if r.CanonicalRef != "" {
		t.Fatal("ambiguous identity must omit canonical_ref")
	}
}

func TestAssociateLifecycleEventsConflictingActivityUnavailable(t *testing.T) {
	ref, _ := ModelReference("backend", "canon")
	actTime := lifecycleAssociationFixtureTime
	// An activity with IdentityConflict is never merged.
	activities := []*ActivityRecord{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", Timestamp: actTime, IdentityConflict: true},
	}
	identities := []ActivityModelIdentity{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", ReportedModelRef: ref, CanonicalRef: ref, State: aliasStateExact},
	}
	events := []LifecycleTransitionEvidence{
		{BackendID: "backend", Epoch: 1, ModelRef: ref, OldState: lifecycleStateStarting, NewState: lifecycleStateReady, ObservedAt: actTime, SourceTime: &actTime},
	}
	rows, err := AssociateLifecycleEvents(activities, identities, events, DefaultAssociationWindow())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 association, got %d", len(rows))
	}
	r := rows[0]
	if r.Kind != lifecycleAssociationUnavailable {
		t.Fatalf("kind = %q, want unavailable (conflicted activity)", r.Kind)
	}
	if r.Reason != lifecycleAssociationReasonIdentityUnresolved {
		t.Fatalf("reason = %q, want identity_unresolved", r.Reason)
	}
}

func TestAssociateLifecycleEventsRawReportedModelRefRejected(t *testing.T) {
	ref, _ := ModelReference("backend", "canon")
	actTime := lifecycleAssociationFixtureTime
	// The activity carries a raw model label (secret) instead of an opaque
	// ref. It must be validated before output and not copied raw.
	activities := []*ActivityRecord{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", Timestamp: actTime, ReportedModelRef: "raw-model-label"},
	}
	identities := []ActivityModelIdentity{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", ReportedModelRef: ref, CanonicalRef: ref, State: aliasStateExact},
	}
	events := []LifecycleTransitionEvidence{
		{BackendID: "backend", Epoch: 1, ModelRef: ref, OldState: lifecycleStateStarting, NewState: lifecycleStateReady, ObservedAt: actTime, SourceTime: &actTime},
	}
	rows, err := AssociateLifecycleEvents(activities, identities, events, DefaultAssociationWindow())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 association, got %d", len(rows))
	}
	r := rows[0]
	if r.Kind != lifecycleAssociationUnavailable {
		t.Fatalf("kind = %q, want unavailable (invalid reported model ref)", r.Kind)
	}
	// Raw secret must not be copied to output.
	if r.ReportedModelRef != "" {
		t.Fatal("reported_model_ref must be blank for invalid raw ref")
	}
	data, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "raw-model-label") {
		t.Fatal("raw model label must not appear in JSON output")
	}
}

func TestAssociateLifecycleEventsZeroSourceTimeRejected(t *testing.T) {
	ref, _ := ModelReference("backend", "canon")
	actTime := lifecycleAssociationFixtureTime
	// A non-nil SourceTime that is zero is invalid and rejected.
	zeroTime := time.Time{}
	events := []LifecycleTransitionEvidence{
		{BackendID: "backend", Epoch: 1, ModelRef: ref, OldState: lifecycleStateStarting, NewState: lifecycleStateReady, ObservedAt: actTime, SourceTime: &zeroTime},
	}
	activities := []*ActivityRecord{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", Timestamp: actTime},
	}
	identities := []ActivityModelIdentity{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", ReportedModelRef: ref, CanonicalRef: ref, State: aliasStateExact},
	}
	if _, err := AssociateLifecycleEvents(activities, identities, events, DefaultAssociationWindow()); err == nil {
		t.Fatal("expected error for zero source time")
	}
}

func TestAssociateLifecycleEventsOversizedSourceEventIDRejected(t *testing.T) {
	ref, _ := ModelReference("backend", "canon")
	actTime := lifecycleAssociationFixtureTime
	// SourceEventID over 256 bytes is rejected.
	oversized := strings.Repeat("a", 257)
	events := []LifecycleTransitionEvidence{
		{BackendID: "backend", Epoch: 1, ModelRef: ref, OldState: lifecycleStateStarting, NewState: lifecycleStateReady, ObservedAt: actTime, SourceTime: &actTime, SourceEventID: oversized},
	}
	activities := []*ActivityRecord{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", Timestamp: actTime},
	}
	identities := []ActivityModelIdentity{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", ReportedModelRef: ref, CanonicalRef: ref, State: aliasStateExact},
	}
	if _, err := AssociateLifecycleEvents(activities, identities, events, DefaultAssociationWindow()); err == nil {
		t.Fatal("expected error for oversized source event id")
	}
}

func TestAssociateLifecycleEventsDuplicateSourceEventIDCountsOnce(t *testing.T) {
	ref, _ := ModelReference("backend", "canon")
	actTime := lifecycleAssociationFixtureTime
	eventTime := actTime.Add(time.Second)
	// Same SourceEventID with identical payload: exact replay is deduplicated
	// and must not inflate the count.
	events := []LifecycleTransitionEvidence{
		{BackendID: "backend", Epoch: 1, ModelRef: ref, OldState: lifecycleStateStarting, NewState: lifecycleStateReady, ObservedAt: actTime, SourceTime: &eventTime, SourceEventID: "evt-1"},
		{BackendID: "backend", Epoch: 1, ModelRef: ref, OldState: lifecycleStateStarting, NewState: lifecycleStateReady, ObservedAt: actTime, SourceTime: &eventTime, SourceEventID: "evt-1"},
	}
	activities := []*ActivityRecord{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", Timestamp: actTime},
	}
	identities := []ActivityModelIdentity{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", ReportedModelRef: ref, CanonicalRef: ref, State: aliasStateExact},
	}
	rows, err := AssociateLifecycleEvents(activities, identities, events, DefaultAssociationWindow())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 association, got %d", len(rows))
	}
	r := rows[0]
	if r.Kind != lifecycleAssociationKind {
		t.Fatalf("kind = %q, want %q", r.Kind, lifecycleAssociationKind)
	}
	if r.MatchedEventCount != 1 {
		t.Fatalf("matched count = %d, want 1 (exact replay deduped)", r.MatchedEventCount)
	}
	if r.ObservedEventCount != 1 {
		t.Fatalf("observed count = %d, want 1 (distinct scoped event)", r.ObservedEventCount)
	}
	// Source event id (secret) must not be serialized.
	data, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "evt-1") {
		t.Fatal("source event id must not appear in JSON output")
	}
}

func TestAssociateLifecycleEventsConflictingSourceEventIDNoApprox(t *testing.T) {
	ref, _ := ModelReference("backend", "canon")
	actTime := lifecycleAssociationFixtureTime
	eventTime := actTime.Add(time.Second)
	// Same SourceEventID with different SourceTime: conflicting payload,
	// mark conflict and never emit an approximate association.
	events := []LifecycleTransitionEvidence{
		{BackendID: "backend", Epoch: 1, ModelRef: ref, OldState: lifecycleStateStarting, NewState: lifecycleStateReady, ObservedAt: actTime, SourceTime: &actTime, SourceEventID: "evt-1"},
		{BackendID: "backend", Epoch: 1, ModelRef: ref, OldState: lifecycleStateReady, NewState: lifecycleStateStopping, ObservedAt: actTime, SourceTime: &eventTime, SourceEventID: "evt-1"},
	}
	activities := []*ActivityRecord{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", Timestamp: actTime},
	}
	identities := []ActivityModelIdentity{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", ReportedModelRef: ref, CanonicalRef: ref, State: aliasStateExact},
	}
	rows, err := AssociateLifecycleEvents(activities, identities, events, DefaultAssociationWindow())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 association, got %d", len(rows))
	}
	r := rows[0]
	if r.Kind != lifecycleAssociationUnavailable {
		t.Fatalf("kind = %q, want unavailable (conflict, no approx)", r.Kind)
	}
	if !r.Conflict {
		t.Fatal("conflict should be flagged for conflicting source event ID payload")
	}
	if r.MatchedEventCount != 0 {
		t.Fatalf("matched count = %d, want 0 (conflict)", r.MatchedEventCount)
	}
}

func TestAssociateLifecycleEventsOneInWindowMatchedOneObservedTwo(t *testing.T) {
	ref, _ := ModelReference("backend", "canon")
	actTime := lifecycleAssociationFixtureTime
	inWindow := actTime.Add(time.Second)
	outWindow := actTime.Add(time.Hour)
	// Two events: one SourceTime in the window, one outside. Matched count is
	// one (only the in-window event), observed count is two.
	events := []LifecycleTransitionEvidence{
		{BackendID: "backend", Epoch: 1, ModelRef: ref, OldState: lifecycleStateStarting, NewState: lifecycleStateReady, ObservedAt: actTime, SourceTime: &inWindow},
		{BackendID: "backend", Epoch: 1, ModelRef: ref, OldState: lifecycleStateReady, NewState: lifecycleStateStopping, ObservedAt: actTime, SourceTime: &outWindow},
	}
	activities := []*ActivityRecord{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", Timestamp: actTime},
	}
	identities := []ActivityModelIdentity{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", ReportedModelRef: ref, CanonicalRef: ref, State: aliasStateExact},
	}
	rows, err := AssociateLifecycleEvents(activities, identities, events, DefaultAssociationWindow())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 association, got %d", len(rows))
	}
	r := rows[0]
	if r.Kind != lifecycleAssociationKind {
		t.Fatalf("kind = %q, want %q", r.Kind, lifecycleAssociationKind)
	}
	if r.MatchedEventCount != 1 {
		t.Fatalf("matched count = %d, want 1 (one event in window)", r.MatchedEventCount)
	}
	if r.ObservedEventCount != 2 {
		t.Fatalf("observed count = %d, want 2 (two scoped events)", r.ObservedEventCount)
	}
	if !r.FirstEventTime.Equal(inWindow) || !r.LastEventTime.Equal(inWindow) {
		t.Fatalf("event times = %v %v, want %v", r.FirstEventTime, r.LastEventTime, inWindow)
	}
}

func TestAssociateLifecycleEventsNoSourceTimeMatchedZeroObservedN(t *testing.T) {
	ref, _ := ModelReference("backend", "canon")
	actTime := lifecycleAssociationFixtureTime
	// Three events, none with SourceTime. Matched count is zero; observed
	// count reports the scoped transition metadata.
	events := []LifecycleTransitionEvidence{
		{BackendID: "backend", Epoch: 1, ModelRef: ref, OldState: lifecycleStateStarting, NewState: lifecycleStateReady, ObservedAt: actTime},
		{BackendID: "backend", Epoch: 1, ModelRef: ref, OldState: lifecycleStateReady, NewState: lifecycleStateStopping, ObservedAt: actTime},
		{BackendID: "backend", Epoch: 1, ModelRef: ref, OldState: lifecycleStateStopping, NewState: lifecycleStateUnknown, ObservedAt: actTime},
	}
	activities := []*ActivityRecord{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", Timestamp: actTime},
	}
	identities := []ActivityModelIdentity{
		{BackendID: "backend", Epoch: 1, ActivityID: "act-1", ReportedModelRef: ref, CanonicalRef: ref, State: aliasStateExact},
	}
	rows, err := AssociateLifecycleEvents(activities, identities, events, DefaultAssociationWindow())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 association, got %d", len(rows))
	}
	r := rows[0]
	if r.Kind != lifecycleAssociationUnavailable {
		t.Fatalf("kind = %q, want unavailable", r.Kind)
	}
	if r.Reason != lifecycleAssociationReasonSourceTimeUnavailable {
		t.Fatalf("reason = %q, want source_time_unavailable", r.Reason)
	}
	if r.MatchedEventCount != 0 {
		t.Fatalf("matched count = %d, want 0 (no source time)", r.MatchedEventCount)
	}
	if r.ObservedEventCount != 3 {
		t.Fatalf("observed count = %d, want 3 (scoped transition metadata)", r.ObservedEventCount)
	}
	if !r.FirstEventTime.IsZero() || !r.LastEventTime.IsZero() {
		t.Fatal("first/last event time should be zero when no source clock")
	}
}

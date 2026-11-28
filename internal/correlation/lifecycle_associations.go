// Package correlation provides bounded, normalized collection of backend
// lifecycle (readiness) snapshots from llama-swap backends configured with
// Diagnostics == "llama_swap". It is independent of the network reader and
// never issues requests.
package correlation

import (
	"errors"
	"sort"
	"time"
)

// LifecycleTransitionEvidence is a normalized, safe record of one backend
// lifecycle transition observed by the collector. ModelRef is an opaque
// SHA256-derived reference of the source canonical model ID; the raw ID is
// never retained. SourceTime is the upstream/source clock time of the
// transition; it is optional and may be nil when only the local collection
// time (ObservedAt) is available. SourceEventID may contain secrets and is
// never copied to output.
type LifecycleTransitionEvidence struct {
	BackendID     string     `json:"backend_id"`
	Epoch         int64      `json:"epoch"`
	ModelRef      string     `json:"model_ref"`             // opaque model reference
	OldState      string     `json:"old_state"`             // finite lifecycle enum
	NewState      string     `json:"new_state"`             // finite lifecycle enum
	ObservedAt    time.Time  `json:"observed_at"`           // local collection time
	SourceTime    *time.Time `json:"source_time,omitempty"` // upstream clock, optional
	SourceEventID string     `json:"-"`                     // may contain secrets; never serialized
}

// ActivityLifecycleAssociation is the result of associating one activity with
// nearby lifecycle transition events. It reports only bounded opaque IDs and
// fixed attribution fields: the raw model ID, command, proxy, TTL, name,
// description, URLs, and source event IDs are never output.
type ActivityLifecycleAssociation struct {
	Kind                     string    `json:"kind"`                          // "approximate_lifecycle_window" or "unavailable"
	Attribution              string    `json:"attribution,omitempty"`         // "catalog_declared"
	RequestIDMapping         string    `json:"request_id_mapping,omitempty"`  // "unsupported"
	UpstreamGeneration       string    `json:"upstream_generation,omitempty"` // "unknown"
	LifecycleState           string    `json:"lifecycle_state"`               // state at activity, always "unknown"
	ClockAlignmentUnverified bool      `json:"clock_alignment_unverified,omitempty"`
	ExecutionAttribution     string    `json:"execution_attribution,omitempty"` // "unverified"
	BackendID                string    `json:"backend_id"`
	Epoch                    int64     `json:"epoch"`
	ActivityID               string    `json:"activity_id"`
	ReportedModelRef         string    `json:"reported_model_ref,omitempty"`
	CanonicalRef             string    `json:"canonical_ref,omitempty"`
	WindowStart              time.Time `json:"window_start"`
	WindowEnd                time.Time `json:"window_end"`
	MatchedEventCount        int       `json:"matched_event_count,omitempty"`
	ObservedEventCount       int       `json:"observed_event_count,omitempty"`
	FirstEventTime           time.Time `json:"first_event_time,omitzero"`
	LastEventTime            time.Time `json:"last_event_time,omitzero"`
	Reason                   string    `json:"reason,omitempty"`
	Conflict                 bool      `json:"conflict,omitempty"`
}

const (
	// lifecycleAssociationMaxEvents bounds the number of transition events the
	// associator can process in one call.
	lifecycleAssociationMaxEvents = 256
	// lifecycleAssociationMaxAssociations bounds the number of association
	// rows emitted in one call.
	lifecycleAssociationMaxAssociations = 4096
	// lifecycleAssociationWindowMax is the maximum allowed look-behind /
	// look-ahead duration.
	lifecycleAssociationWindowMax = time.Minute
)

// Fixed kind and reason values for the association output.
const (
	lifecycleAssociationKind        = "approximate_lifecycle_window"
	lifecycleAssociationUnavailable = "unavailable"

	lifecycleAssociationReasonSourceTimeUnavailable = "source_time_unavailable"
	lifecycleAssociationReasonOutsideWindow         = "outside_window"
	lifecycleAssociationReasonIdentityUnresolved    = "identity_unresolved"
	lifecycleAssociationReasonNoMatchingEvent       = "no_matching_event"

	lifecycleAssociationAttribution          = "catalog_declared"
	lifecycleAssociationExecutionAttribution = "unverified"
)

// Model-level strict source finite enum. Values mirror the source ProcessState
// enum as observed through /running and related lifecycle endpoints. The
// observed states include ready, starting, stopping, stopped, and shutdown.
// Absent models are not reported; we never infer "unloaded" from absence.
// ready/starting/stopping/unknown are declared in lifecycle.go; stopped and
// shutdown are added here so the strict finite enum covers all source states.
const (
	lifecycleStateStopped  = "stopped"
	lifecycleStateShutdown = "shutdown"
)

// isLifecycleTransitionState reports whether s is one of the finite lifecycle
// states observed through lifecycle endpoints.
func isLifecycleTransitionState(s string) bool {
	switch s {
	case lifecycleStateReady, lifecycleStateStarting, lifecycleStateStopping, lifecycleStateStopped, lifecycleStateShutdown, lifecycleStateUnknown:
		return true
	}
	return false
}

// eventPayload captures the observable payload of a transition event for
// SourceEventID conflict detection. Two events with the same SourceEventID but
// a differing payload (SourceTime, OldState, NewState) are a conservative
// conflict; exact replays are deduplicated.
type eventPayload struct {
	oldState   string
	newState   string
	sourceTime *time.Time
}

func payloadEqual(a, b eventPayload) bool {
	if a.oldState != b.oldState || a.newState != b.newState {
		return false
	}
	if (a.sourceTime == nil) != (b.sourceTime == nil) {
		return false
	}
	if a.sourceTime != nil && b.sourceTime != nil && !a.sourceTime.Equal(*b.sourceTime) {
		return false
	}
	return true
}

// identityKey and eventKey are package-level so the indexing and per-activity
// association helpers can share the lookup keys without redefining them.
type identityKey struct {
	backendID  string
	epoch      int64
	activityID string
}

type eventKey struct {
	backendID string
	epoch     int64
	modelRef  string
}

// AssociateLifecycleEvents associates activities with nearby lifecycle
// transition events within a time window. It is pure: it does not mutate the
// inputs and is safe for concurrent calls. Identities are looked up by
// (backendID, epoch, ActivityID) - never by positional index - so activities
// are never matched to the wrong identity. Duplicate identities for the same
// (backend, epoch, ActivityID) are ambiguous even if identical. An activity
// with IdentityConflict is never merged.
// Events are indexed by (backendID, epoch, ModelRef) only; the canonical ref
// is never borrowed as the model ref. The activity's ReportedModelRef must
// match the identity's ReportedModelRef when both are present.
// The kind is approximate_lifecycle_window only when a matching event's
// SourceTime is non-nil and falls within [activity.Timestamp - Before,
// activity.Timestamp + After] (inclusive). Local ObservedAt is never
// substituted for the source clock. When SourceTime is unavailable, the kind
// is unavailable with reason source_time_unavailable and the observed
// transition count is reported as metadata without comparing clock domains.
// The state at the activity is always unknown: a transition observation
// cannot prove the activity's own state, transition, restart, cold start,
// queue, or execution.
func AssociateLifecycleEvents(activities []*ActivityRecord, identities []ActivityModelIdentity, events []LifecycleTransitionEvidence, window AssociationWindow) ([]ActivityLifecycleAssociation, error) {
	// Validate the window upfront.
	if err := validateAssociationWindow(window); err != nil {
		return nil, err
	}

	// Validate input bounds upfront.
	if err := validateAssociationInputBounds(activities, identities, events); err != nil {
		return nil, err
	}

	// Validate activities: nil, invalid IDs, zero timestamps, non-positive
	// epochs are rejected with explicit unavailable errors.
	if err := validateActivities(activities); err != nil {
		return nil, err
	}

	// Validate identities: scope must be finite; reported/canonical refs,
	// when present, must be bounded opaque ModelReferences.
	if err := validateIdentities(identities); err != nil {
		return nil, err
	}

	// Validate events: scope and finite enum fields must be valid; zero
	// observation times are rejected. A non-nil SourceTime must be non-zero.
	// SourceEventID is capped at 256 bytes; it may contain secrets and is
	// never emitted.
	if err := validateEvents(events); err != nil {
		return nil, err
	}

	// Index identities by (backendID, epoch, ActivityID). Duplicates are
	// ambiguous even if identical: no winner is picked.
	identityByKey, identityAmbiguous := indexIdentities(identities)

	// Index events by (backendID, epoch, ModelRef). Track SourceEventID
	// conflicts per scope: the same source event ID with a different payload
	// (SourceTime, OldState, NewState) is a conservative conflict; exact
	// replays are deduplicated and never inflate the count. Events are never
	// merged.
	eventsByKey, conflictKeys := indexEvents(events)

	// Sort each event list stably by SourceTime (nil last) so FirstEventTime /
	// LastEventTime are deterministic regardless of receipt order. Receipt
	// order itself is preserved for processing; SourceTime order is only used
	// for reporting first/last bounds.
	sortEventListsBySourceTime(eventsByKey)

	result := make([]ActivityLifecycleAssociation, 0, len(activities))

	for i := range activities {
		a := activities[i]
		result = append(result, associateOneActivity(a, identityByKey, identityAmbiguous, eventsByKey, conflictKeys, window))
	}

	// Cap association count defensively.
	if len(result) > lifecycleAssociationMaxAssociations {
		result = result[:lifecycleAssociationMaxAssociations]
	}

	return result, nil
}

// validateAssociationWindow validates the association look-behind / look-ahead
// window bounds.
func validateAssociationWindow(window AssociationWindow) error {
	if window.Before < 0 || window.Before > lifecycleAssociationWindowMax {
		return errors.New("association window before out of range: must be 0..1 minute")
	}
	if window.After < 0 || window.After > lifecycleAssociationWindowMax {
		return errors.New("association window after out of range: must be 0..1 minute")
	}
	return nil
}

// validateAssociationInputBounds validates the input size bounds upfront.
func validateAssociationInputBounds(activities []*ActivityRecord, identities []ActivityModelIdentity, events []LifecycleTransitionEvidence) error {
	if len(activities) > 1000 {
		return errors.New("too many activities: max 1000")
	}
	if len(identities) > 1000 {
		return errors.New("too many identities: max 1000")
	}
	if len(events) > lifecycleAssociationMaxEvents {
		return errors.New("too many events: max 256")
	}
	return nil
}

// validateActivities validates each activity record.
func validateActivities(activities []*ActivityRecord) error {
	for _, a := range activities {
		if err := validateActivity(a); err != nil {
			return err
		}
	}
	return nil
}

// validateIdentities validates each activity identity.
func validateIdentities(identities []ActivityModelIdentity) error {
	for _, id := range identities {
		if !validIdentifier(id.BackendID) {
			return errors.New("invalid identity backend id: explicit unavailable")
		}
		if !validIdentifier(id.ActivityID) {
			return errors.New("invalid identity activity id: explicit unavailable")
		}
		if id.Epoch <= 0 {
			return errors.New("non-positive identity epoch: explicit unavailable")
		}
		if id.ReportedModelRef != "" && !isModelReference(id.ReportedModelRef) {
			return errors.New("invalid identity reported model ref: explicit unavailable")
		}
		if id.CanonicalRef != "" && !isModelReference(id.CanonicalRef) {
			return errors.New("invalid identity canonical ref: explicit unavailable")
		}
	}
	return nil
}

// validateEvents validates each lifecycle transition event.
func validateEvents(events []LifecycleTransitionEvidence) error {
	for _, e := range events {
		if !validIdentifier(e.BackendID) {
			return errors.New("invalid event backend id: explicit unavailable")
		}
		if e.Epoch <= 0 {
			return errors.New("non-positive event epoch: explicit unavailable")
		}
		if e.ModelRef != "" && !isModelReference(e.ModelRef) {
			return errors.New("invalid event model ref: explicit unavailable")
		}
		if !isLifecycleTransitionState(e.OldState) {
			return errors.New("invalid event old state: explicit unavailable")
		}
		if !isLifecycleTransitionState(e.NewState) {
			return errors.New("invalid event new state: explicit unavailable")
		}
		if e.ObservedAt.IsZero() {
			return errors.New("zero event observed at: explicit unavailable")
		}
		if e.SourceTime != nil && e.SourceTime.IsZero() {
			return errors.New("zero event source time: explicit unavailable")
		}
		if len(e.SourceEventID) > 256 {
			return errors.New("source event id too long: explicit unavailable")
		}
	}
	return nil
}

// indexIdentities indexes identities by (backendID, epoch, ActivityID).
// Duplicates are ambiguous even if identical: no winner is picked.
func indexIdentities(identities []ActivityModelIdentity) (map[identityKey]*ActivityModelIdentity, map[identityKey]bool) {
	identityByKey := make(map[identityKey]*ActivityModelIdentity, len(identities))
	identityAmbiguous := make(map[identityKey]bool)
	for i := range identities {
		id := &identities[i]
		key := identityKey{id.BackendID, id.Epoch, id.ActivityID}
		if identityAmbiguous[key] {
			continue
		}
		if _, ok := identityByKey[key]; ok {
			identityAmbiguous[key] = true
			continue
		}
		identityByKey[key] = id
	}
	return identityByKey, identityAmbiguous
}

// indexEvents indexes events by (backendID, epoch, ModelRef) and tracks
// SourceEventID conflicts per scope. The same source event ID with a
// differing payload (SourceTime, OldState, NewState) is a conservative
// conflict; exact replays are deduplicated and never inflate the count.
// Events are never merged.
func indexEvents(events []LifecycleTransitionEvidence) (map[eventKey][]*LifecycleTransitionEvidence, map[eventKey]bool) {
	eventsByKey := make(map[eventKey][]*LifecycleTransitionEvidence, len(events))
	seenSourceEvent := make(map[eventKey]map[string]eventPayload)
	conflictKeys := make(map[eventKey]bool)

	for i := range events {
		e := &events[i] // input pointer; only read, never mutate
		key := eventKey{e.BackendID, e.Epoch, e.ModelRef}

		if e.SourceEventID != "" {
			if seenSourceEvent[key] == nil {
				seenSourceEvent[key] = make(map[string]eventPayload)
			}
			prev, ok := seenSourceEvent[key][e.SourceEventID]
			if ok {
				if !payloadEqual(prev, eventPayload{e.OldState, e.NewState, e.SourceTime}) {
					conflictKeys[key] = true
				}
				// Exact duplicate or conflicting replay: do not inflate count.
				continue
			}
			eventsByKey[key] = append(eventsByKey[key], e)
			seenSourceEvent[key][e.SourceEventID] = eventPayload{e.OldState, e.NewState, e.SourceTime}
		} else {
			eventsByKey[key] = append(eventsByKey[key], e)
		}
	}

	return eventsByKey, conflictKeys
}

// sortEventListsBySourceTime sorts each indexed event list stably by
// SourceTime (nil last) so FirstEventTime / LastEventTime are deterministic
// regardless of receipt order. Receipt order itself is preserved for
// processing; SourceTime order is only used for reporting first/last bounds.
func sortEventListsBySourceTime(eventsByKey map[eventKey][]*LifecycleTransitionEvidence) {
	for key := range eventsByKey {
		evts := eventsByKey[key]
		sort.SliceStable(evts, func(i, j int) bool {
			ti, tj := evts[i].SourceTime, evts[j].SourceTime
			if ti == nil && tj == nil {
				return false
			}
			if ti == nil {
				return false // nil sorts after non-nil
			}
			if tj == nil {
				return true
			}
			return ti.Before(*tj)
		})
	}
}

// buildBaseAssoc builds the unavailable base association for one activity:
// fixed attribution fields, the activity's association window, and the state
// at the activity is always unknown.
func buildBaseAssoc(a *ActivityRecord, window AssociationWindow) ActivityLifecycleAssociation {
	return ActivityLifecycleAssociation{
		Kind:                     lifecycleAssociationUnavailable,
		Attribution:              lifecycleAssociationAttribution,
		RequestIDMapping:         "unsupported",
		UpstreamGeneration:       "unknown",
		LifecycleState:           "unknown",
		ClockAlignmentUnverified: true,
		ExecutionAttribution:     lifecycleAssociationExecutionAttribution,
		BackendID:                a.BackendID,
		Epoch:                    a.Epoch,
		ActivityID:               a.ActivityID,
		WindowStart:              a.Timestamp.Add(-window.Before),
		WindowEnd:                a.Timestamp.Add(window.After),
	}
}

// associateOneActivity associates one activity with nearby lifecycle transition
// events. It starts from the unavailable base association and applies the
// identity resolution, conflict, and source-clock window rules. The result is
// returned without mutating the inputs.
func associateOneActivity(
	a *ActivityRecord,
	identityByKey map[identityKey]*ActivityModelIdentity,
	identityAmbiguous map[identityKey]bool,
	eventsByKey map[eventKey][]*LifecycleTransitionEvidence,
	conflictKeys map[eventKey]bool,
	window AssociationWindow,
) ActivityLifecycleAssociation {
	assoc := buildBaseAssoc(a, window)

	// Validate the activity's reported model ref before output: it must
	// be blank or an opaque model reference. Raw labels are never copied.
	if a.ReportedModelRef != "" && !isModelReference(a.ReportedModelRef) {
		assoc.Reason = lifecycleAssociationReasonIdentityUnresolved
		return assoc
	}
	assoc.ReportedModelRef = a.ReportedModelRef

	// Look up the identity by (backendID, epoch, ActivityID) - never by
	// positional index. Duplicates are ambiguous; a conflicting activity
	// is never merged.
	key := identityKey{a.BackendID, a.Epoch, a.ActivityID}
	id, ok := identityByKey[key]
	if !ok || identityAmbiguous[key] || a.IdentityConflict {
		assoc.Reason = lifecycleAssociationReasonIdentityUnresolved
		return assoc
	}

	// The activity's reported model ref must match the identity's when
	// both are present; never borrow the canonical ref as the model ref.
	if a.ReportedModelRef != "" && id.ReportedModelRef != "" && a.ReportedModelRef != id.ReportedModelRef {
		assoc.Reason = lifecycleAssociationReasonIdentityUnresolved
		return assoc
	}

	// Only a uniquely catalog_resolved identity with a canonical ref and
	// positive epoch is matched.
	identityResolved := false
	if id.State == aliasStateExact && id.CanonicalRef != "" && id.Epoch > 0 {
		identityResolved = true
		assoc.CanonicalRef = id.CanonicalRef
	}

	if !identityResolved {
		assoc.Reason = lifecycleAssociationReasonIdentityUnresolved
		return assoc
	}

	// Transition events name configured models. Match the independently
	// resolved canonical reference, never the possibly aliased reported name.
	ekey := eventKey{id.BackendID, id.Epoch, id.CanonicalRef}
	matching := eventsByKey[ekey]
	if len(matching) == 0 {
		assoc.Reason = lifecycleAssociationReasonNoMatchingEvent
		return assoc
	}

	// Conservative conflict flag: duplicate source event IDs with
	// differing payloads are not merged. A conflicting identity never
	// emits an approximate association.
	if conflictKeys[ekey] {
		assoc.Conflict = true
		assoc.Reason = lifecycleAssociationReasonIdentityUnresolved
		assoc.ObservedEventCount = len(matching)
		return assoc
	}

	// Source clock window: [activity.Timestamp - Before,
	// activity.Timestamp + After] inclusive. Local ObservedAt is never
	// substituted for the source clock. The base association already
	// computed the window from the activity timestamp.
	ws := assoc.WindowStart
	we := assoc.WindowEnd

	var validSourceTimes []time.Time
	hasSourceTime := false
	for _, e := range matching {
		if e.SourceTime != nil {
			hasSourceTime = true
			st := *e.SourceTime
			if !st.Before(ws) && !st.After(we) {
				validSourceTimes = append(validSourceTimes, st)
			}
		}
	}

	// Observed event count is scoped metadata: the number of distinct
	// transition events observed for this backend/epoch/model ref.
	assoc.ObservedEventCount = len(matching)

	if len(validSourceTimes) > 0 {
		// Valid source-clock association.
		assoc.Kind = lifecycleAssociationKind
		assoc.MatchedEventCount = len(validSourceTimes)
		sort.SliceStable(validSourceTimes, func(i, j int) bool {
			return validSourceTimes[i].Before(validSourceTimes[j])
		})
		assoc.FirstEventTime = validSourceTimes[0]
		assoc.LastEventTime = validSourceTimes[len(validSourceTimes)-1]
	} else {
		// No valid source-clock association. Matched count is zero;
		// report observed count as metadata but do not compare clock
		// domains.
		assoc.MatchedEventCount = 0
		if !hasSourceTime {
			assoc.Reason = lifecycleAssociationReasonSourceTimeUnavailable
		} else {
			assoc.Reason = lifecycleAssociationReasonOutsideWindow
		}
	}

	return assoc
}

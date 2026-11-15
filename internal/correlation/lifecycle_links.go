// Package correlation provides linking between resolved activity identities
// and the current lifecycle snapshot. It is pure: it does not mutate the
// identities or the report, never issues requests, and never copies raw
// upstream labels, commands, proxies, TTLs, names, descriptions, or URLs.
package correlation

import (
	"errors"
	"strconv"
	"time"
)

const (
	// lifecycleLinksMaxIdentities bounds the number of activity identities
	// LinkCurrentLifecycle can process in one call.
	lifecycleLinksMaxIdentities = 1000
	// lifecycleLinksMaxModels bounds the number of lifecycle models the link
	// can process in one call.
	lifecycleLinksMaxModels = 1000
	// activityLifecycleKind is the fixed kind of the observation: a current
	// metadata-only snapshot of the readiness source.
	activityLifecycleKind = "current_metadata_only"
	// activityLifecycleStateAtActivityUnknown is the fixed state reported for
	// the activity's own state. A readiness snapshot cannot prove the
	// activity's state, transition, restart, cold start, queue, or execution.
	activityLifecycleStateAtActivityUnknown = "unknown"
	// activityLifecycleClockAlignmentUnverified is the fixed attribution for
	// the observation clock: the collector clock and the upstream clock are
	// not synchronized, so alignment is unverified.
	activityLifecycleClockAlignmentUnverified = true
	// activityLifecycleUpstreamGenerationUnknown is the fixed attribution:
	// /running carries no upstream generation/restart identifier.
	activityLifecycleUpstreamGenerationUnknown = "unknown"
	// activityLifecycleRequestIDMappingUnsupported is the fixed attribution:
	// /running carries no X-Request-ID, so lifecycle observations cannot be
	// mapped to a proxy request.
	activityLifecycleRequestIDMappingUnsupported = "unsupported"
	// activityLifecycleExecutionAttributionUnverified is the fixed
	// attribution: one readiness observation does not prove that the backend
	// actually executed a model or booted.
	activityLifecycleExecutionAttributionUnverified = "unverified"
)

// ActivityLifecycleObservation is the result of linking one activity's
// resolved alias identity to the current lifecycle snapshot. It reports only
// bounded opaque IDs and fixed states: the raw model ID, command, proxy, TTL,
// name, description, and URLs are never output.
type ActivityLifecycleObservation struct {
	BackendID        string `json:"backend_id"`
	Epoch            int64  `json:"epoch"`
	ActivityID       string `json:"activity_id"`
	ReportedModelRef string `json:"reported_model_ref,omitempty"`
	CanonicalRef     string `json:"canonical_ref,omitempty"`
	// CurrentState is the lifecycle state of the matched model when the
	// identity is uniquely catalog_resolved; otherwise unknown. It is an
	// observed enum, never a derivation of prior state.
	CurrentState string `json:"current_state"` // ready, starting, stopping, unknown
	// ObservedAt points to the matched lifecycle row's ObservedAt when a
	// unique match exists; otherwise nil.
	ObservedAt *time.Time `json:"observed_at,omitempty"`
	Observed   bool       `json:"observed"`
	// StateAtActivity is always unknown: a readiness snapshot cannot prove
	// the activity's own state, transition, restart, cold start, queue, or
	// execution.
	StateAtActivity          string `json:"state_at_activity"`
	Kind                     string `json:"kind"`
	ClockAlignmentUnverified bool   `json:"clock_alignment_unverified"`
	UpstreamGeneration       string `json:"upstream_generation"`
	RequestIDMapping         string `json:"request_id_mapping"`
	ExecutionAttribution     string `json:"execution_attribution"`
}

// LinkCurrentLifecycle links each activity identity to the current lifecycle
// snapshot. It is pure: it does not mutate the identities or the report.
// Only identities with a unique catalog_resolved canonical reference are
// matched to a lifecycle model with the same backend ID, collection epoch,
// and ModelRef. Duplicates (multiple lifecycle rows with the same
// backend+epoch+ModelRef) and ambiguous identities yield unknown with no
// winner. Absent models are reported unknown with Observed=false; we never
// infer "unloaded" or "stopped" from absence.
func LinkCurrentLifecycle(identities []ActivityModelIdentity, report LifecycleReport) ([]ActivityLifecycleObservation, error) {
	if len(identities) > lifecycleLinksMaxIdentities {
		return nil, errors.New("identity count exceeds bound")
	}
	if len(report.Models) > lifecycleLinksMaxModels {
		return nil, errors.New("model count exceeds bound")
	}

	// Validate scope upfront: backend ID, epoch, and activity ID must be
	// finite and in scope. Reported/canonical refs, when present, must be
	// bounded opaque ModelReferences. Lifecycle model refs must be real
	// opaque ModelReferences.
	for _, id := range identities {
		if !validIdentifier(id.BackendID) || !validIdentifier(id.ActivityID) {
			return nil, errors.New("invalid activity identity scope")
		}
		if id.Epoch <= 0 {
			return nil, errors.New("invalid activity identity epoch")
		}
		if id.ReportedModelRef != "" && !isModelReference(id.ReportedModelRef) {
			return nil, errors.New("invalid activity identity reported model ref")
		}
		if id.CanonicalRef != "" && !isModelReference(id.CanonicalRef) {
			return nil, errors.New("invalid activity identity canonical ref")
		}
	}
	for _, m := range report.Models {
		if !validIdentifier(m.BackendID) {
			return nil, errors.New("invalid lifecycle model scope")
		}
		if m.Epoch <= 0 {
			return nil, errors.New("invalid lifecycle model epoch")
		}
		if !isModelReference(m.ModelRef) {
			return nil, errors.New("invalid lifecycle model ref")
		}
	}

	// Index lifecycle models by (backendID, epoch, ModelRef). Detect
	// duplicates: the same ref observed more than once in a scope has no
	// unique winner.
	modelByKey := make(map[string]*LifecycleModel, len(report.Models))
	duplicateKeys := make(map[string]struct{}, len(report.Models))
	for i := range report.Models {
		m := &report.Models[i]
		key := lifecycleModelKey(m.BackendID, m.Epoch, m.ModelRef)
		if _, exists := modelByKey[key]; exists {
			duplicateKeys[key] = struct{}{}
			continue
		}
		modelByKey[key] = m
	}

	observations := make([]ActivityLifecycleObservation, 0, len(identities))
	for _, id := range identities {
		obs := ActivityLifecycleObservation{
			BackendID:                id.BackendID,
			Epoch:                    id.Epoch,
			ActivityID:               id.ActivityID,
			ReportedModelRef:         id.ReportedModelRef,
			StateAtActivity:          activityLifecycleStateAtActivityUnknown,
			Kind:                     activityLifecycleKind,
			ClockAlignmentUnverified: activityLifecycleClockAlignmentUnverified,
			UpstreamGeneration:       activityLifecycleUpstreamGenerationUnknown,
			RequestIDMapping:         activityLifecycleRequestIDMappingUnsupported,
			ExecutionAttribution:     activityLifecycleExecutionAttributionUnverified,
		}
		// Only a uniquely catalog_resolved identity with a canonical ref is
		// matched to a lifecycle model. Ambiguous or unknown identities are
		// reported unknown with no winner.
		if id.State == aliasStateExact && id.CanonicalRef != "" {
			key := lifecycleModelKey(id.BackendID, id.Epoch, id.CanonicalRef)
			if _, dup := duplicateKeys[key]; dup {
				// Duplicate canonical ref in the same scope: no winner.
				obs.CurrentState = aliasStateUnknown
				obs.Observed = false
				obs.ObservedAt = nil
				obs.CanonicalRef = id.CanonicalRef
				observations = append(observations, obs)
				continue
			}
			m, ok := modelByKey[key]
			if !ok {
				// Absent model: unknown, Observed=false. Never infer
				// unloaded/stopped from absence. Preserve the known catalog
				// ref when there is no false match.
				obs.CurrentState = aliasStateUnknown
				obs.Observed = false
				obs.ObservedAt = nil
				obs.CanonicalRef = id.CanonicalRef
				observations = append(observations, obs)
				continue
			}
			// Unique match: observed enum from the source row. Only known
			// lifecycle states are copied; arbitrary raw text yields unknown.
			switch m.State {
			case lifecycleStateReady, lifecycleStateStarting, lifecycleStateStopping:
				obs.CurrentState = m.State
			default:
				obs.CurrentState = aliasStateUnknown
			}
			obs.Observed = true
			// Copy the time value so the output never aliases the input
			// report and never fabricates a timestamp from a zero value.
			if !m.ObservedAt.IsZero() {
				copied := m.ObservedAt
				obs.ObservedAt = &copied
			}
			obs.CanonicalRef = id.CanonicalRef
			observations = append(observations, obs)
			continue
		}
		// Ambiguous or unknown: no canonical ref to match; never attach one.
		obs.CurrentState = aliasStateUnknown
		obs.Observed = false
		obs.ObservedAt = nil
		observations = append(observations, obs)
	}
	return observations, nil
}

// lifecycleModelKey builds a deterministic lookup key for a
// (backendID, epoch, modelRef) triple.
func lifecycleModelKey(backendID string, epoch int64, modelRef string) string {
	return backendID + "\x00" + strconv.FormatInt(epoch, 10) + "\x00" + modelRef
}

// Package correlation provides lifecycle event support description for the
// inspected adapter contract. It is pure: no HTTP, no goroutines, no backend
// probing. It reports what the adapter contract exposes (or does not expose)
// rather than what a deployed backend currently emits.
package correlation

import (
	"errors"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

// lifecycleEventSupportMaxBackends bounds the number of backends described
// in one report.
const lifecycleEventSupportMaxBackends = 1024

// Source and scope constants for the report.
const (
	// sourceAdapterCapability is the serialized Source value. The report
	// describes the inspected adapter contract, not a probed backend.
	sourceAdapterCapability = "adapter_capability"
	// epochScopeCollector is defined in activity.go as "collector"; the
	// epoch is a collector-local collection scope, not proof of an upstream
	// restart. It is reused here so the report's scope is consistent with
	// the activity and alias reports.
)

// Backend-level states.
const (
	lifecycleEventSupportUnsupported = "unsupported"
	lifecycleEventSupportDisabled    = "disabled"
)

// Reasons.
const (
	// lifecycleEventReasonTransitionPayloadNotExposed is the serialized
	// Reason for llama-swap backends: the adapter contract does not expose
	// transition payloads (old/new state with source timestamp, source event
	// ID, or replay).
	lifecycleEventReasonTransitionPayloadNotExposed = "transition_payload_not_exposed"
	// lifecycleEventReasonNotEnabled is the serialized Reason for backends
	// that are not configured for lifecycle diagnostics.
	lifecycleEventReasonNotEnabled = "not_enabled"
)

// Explicit unavailability statements for the inspected adapter contract. The
// llama-swap /running endpoint and wire event expose neither old/new state
// pairs with source timestamps, nor source event IDs, nor a replay mechanism.
// These are contract observations, not backend errors.
const (
	lifecycleEventSourceTimeUnavailable    = "source_time_unavailable"
	lifecycleEventSourceEventIDUnavailable = "source_event_id_unavailable"
	lifecycleEventReplayUnsupported        = "replay_unsupported"
)

// Explanations (human-readable, no URLs, model IDs, headers, or upstream
// error strings copied).
const (
	lifecycleEventExplainTransitionPayloadNotExposed = "the adapter contract does not expose transition payloads: no old/new state pairs with source timestamps, source event IDs, or replay"
	lifecycleEventExplainNotEnabled                  = "lifecycle diagnostics are not enabled for this backend"
)

// LifecycleEventSupportReport describes the lifecycle event support contract
// for the inspected adapter. It is pure: no HTTP, no goroutines, no backend
// probing. It reports what the adapter contract exposes (or does not expose)
// rather than what a deployed backend currently emits.
type LifecycleEventSupportReport struct {
	Source string `json:"source"`
	// EpochScope is the collector-local collection scope, not proof of an
	// upstream restart.
	EpochScope string `json:"epoch_scope"`
	// BackendStatuses reports one entry per valid configured backend ID.
	// Invalid IDs are omitted without leaking their identity.
	BackendStatuses map[string]LifecycleBackendEventSupport `json:"backend_statuses"`
	// Attribution: structural constraints of the inspected adapter contract.
	SourceTimeUnavailable    string `json:"source_time_unavailable,omitempty"`
	SourceEventIDUnavailable string `json:"source_event_id_unavailable,omitempty"`
	ReplayUnsupported        string `json:"replay_unsupported,omitempty"`
	// No collection took place. Null means unknown, not an observed empty stream.
	ObservedTransitions []LifecycleTransitionEvidence `json:"observed_transitions"`
	CollectionPerformed bool                          `json:"collection_performed"`
}

// LifecycleBackendEventSupport reports the lifecycle event support for one
// configured backend. It never leaks upstream URLs, credentials, or model IDs.
type LifecycleBackendEventSupport struct {
	State       string `json:"state"` // unsupported, disabled
	Reason      string `json:"reason,omitempty"`
	Explanation string `json:"explanation,omitempty"`
}

// DescribeLifecycleEventSupport describes the lifecycle event support contract
// for the given backends. It is pure: no HTTP, no goroutines, no backend
// probing. The epoch must be positive; it is a collector-local collection
// scope, not proof of an upstream restart. Only valid configured backend IDs
// are reported; invalid IDs are omitted without leaking their identity.
// For backends with Diagnostics == "llama_swap", the adapter contract is
// inspected: the llama-swap /running endpoint and wire event expose neither
// old/new state pairs with source timestamps, nor source event IDs, nor a
// replay mechanism, so support is unsupported with reason
// transition_payload_not_exposed. This pertains to the inspected adapter
// contract, not a claim that a deployed backend was probed. All other
// backends are reported disabled. No transition collection is performed and
// ObservedTransitions remains unknown (null), not an observed count of zero.
func DescribeLifecycleEventSupport(backends map[string]config.Backend, epoch int64) (LifecycleEventSupportReport, error) {
	if epoch <= 0 {
		return LifecycleEventSupportReport{}, errors.New("epoch must be positive")
	}
	if len(backends) > lifecycleEventSupportMaxBackends {
		return LifecycleEventSupportReport{}, errors.New("too many backends: max 1024")
	}

	report := LifecycleEventSupportReport{
		Source:                   sourceAdapterCapability,
		EpochScope:               epochScopeCollector,
		BackendStatuses:          make(map[string]LifecycleBackendEventSupport, len(backends)),
		SourceTimeUnavailable:    lifecycleEventSourceTimeUnavailable,
		SourceEventIDUnavailable: lifecycleEventSourceEventIDUnavailable,
		ReplayUnsupported:        lifecycleEventReplayUnsupported,
		ObservedTransitions:      nil,
		CollectionPerformed:      false,
	}

	for id, cfg := range backends {
		if !validIdentifier(id) {
			// Invalid IDs are omitted without leaking their identity.
			continue
		}
		var support LifecycleBackendEventSupport
		if cfg.Diagnostics == "llama_swap" {
			// The adapter contract is inspected, not the backend. The
			// llama-swap /running endpoint and wire event expose neither
			// old/new state pairs with source timestamps, nor source event
			// IDs, nor a replay mechanism. Support is unsupported; this
			// pertains to the inspected adapter contract, not a claim that a
			// deployed backend was probed.
			support = LifecycleBackendEventSupport{
				State:       lifecycleEventSupportUnsupported,
				Reason:      lifecycleEventReasonTransitionPayloadNotExposed,
				Explanation: lifecycleEventExplainTransitionPayloadNotExposed,
			}
		} else {
			// Unknown or disabled backends: no lifecycle diagnostics.
			support = LifecycleBackendEventSupport{
				State:       lifecycleEventSupportDisabled,
				Reason:      lifecycleEventReasonNotEnabled,
				Explanation: lifecycleEventExplainNotEnabled,
			}
		}
		report.BackendStatuses[id] = support
	}

	return report, nil
}

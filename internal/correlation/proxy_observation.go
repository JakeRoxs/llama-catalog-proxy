// Package correlation provides bounded request-to-activity correlation.
// proxy_observation.go implements the typed, bounded projection of a
// timing.Summary onto a CorrelationRecord. The projection retains only fixed
// numeric/enum fields; no unbounded strings, maps, or slices are ever added,
// so the retained size is hard-bounded.

package correlation

import (
	"math"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/timing"
)

// ProxyObservation is a bounded, typed projection of a timing.Summary onto a
// correlation record. It contains only fixed numeric/enum fields and no
// unbounded strings, maps, or slices, so its serialized size is hard-bounded
// by proxyObservationBudget.
//
// Source, Scope, and UpstreamRequestMapping are fixed proxy labels and are
// never copied from the summary. Wall phases and totals are optional pointers:
// a nil pointer means unknown, a pointer to zero means zero (zero is
// preserved, never coerced to nil). ReportedUsage fields are nonnegative int
// pointers separate from ReportedEngineTimings finite float pointers.
// Transport, Generation, and Inspection states are validated against finite
// allowlists; unknown values are omitted, never copied. FinishReason is
// validated against the finish-reason allowlist; unknown values are omitted.
//
// The projection rejects unfinished summaries (zero EndTime), invalid time
// horizons (EndTime before StartTime), NaN/Inf/negative phases, phases that
// exceed the total or violate phase order, and negative counters or engine
// timings. It does not derive rates and never forces the summary total to
// equal the root record duration.
type ProxyObservation struct {
	Source                 string   `json:"source,omitempty"`
	Scope                  string   `json:"scope,omitempty"`
	UpstreamRequestMapping string   `json:"upstream_request_mapping,omitempty"`
	UpstreamHeaderMs       *float64 `json:"upstream_header_ms,omitempty"`
	FirstByteMs            *float64 `json:"first_byte_ms,omitempty"`
	FirstOutputMs          *float64 `json:"first_output_ms,omitempty"`
	FirstAnswerMs          *float64 `json:"first_answer_ms,omitempty"`
	TotalDurationMs        *float64 `json:"total_duration_ms,omitempty"`
	PromptTokens           *int64   `json:"prompt_tokens,omitempty"`
	CompletionTokens       *int64   `json:"completion_tokens,omitempty"`
	TotalTokens            *int64   `json:"total_tokens,omitempty"`
	CachedTokens           *int64   `json:"cached_tokens,omitempty"`
	ReasoningTokens        *int64   `json:"reasoning_tokens,omitempty"`
	PromptMS               *float64 `json:"prompt_ms,omitempty"`
	PredictedMS            *float64 `json:"predicted_ms,omitempty"`
	PromptN                *int64   `json:"prompt_n,omitempty"`
	PredictedN             *int64   `json:"predicted_n,omitempty"`
	TransportOutcome       string   `json:"transport_outcome,omitempty"`
	GenerationOutcome      string   `json:"generation_outcome,omitempty"`
	InspectionState        string   `json:"inspection_state,omitempty"`
	FinishReason           string   `json:"finish_reason,omitempty"`
}

const (
	// proxySource is the fixed source label; never copied from the summary.
	proxySource = "proxy"
	// proxyScope is the fixed scope label; never copied from the summary.
	proxyScope = "proxy_request_only"
	// upstreamRequestMapping is the fixed mapping label; never copied from
	// the summary.
	upstreamRequestMapping = "not_established"

	// proxyObservationBudget is the hard byte budget for a serialized
	// ProxyObservation. Fixed numeric/enum fields keep the size well below
	// this bound; no arbitrary maps or unbounded strings are allowed.
	proxyObservationBudget = 4 * 1024
)

// transportOutcomeAllowlist is the finite set of transport outcomes.
var transportOutcomeAllowlist = map[string]bool{
	"returned":        true,
	"aborted":         true,
	"canceled":        true,
	"timeout":         true,
	"transport_error": true,
}

// generationOutcomeAllowlist is the finite set of generation outcomes.
var generationOutcomeAllowlist = map[string]bool{
	"uninspected": true,
	"incomplete":  true,
	"unknown":     true,
	"truncated":   true,
	"completed":   true,
}

// inspectionStateAllowlist is the finite set of inspection states.
var inspectionStateAllowlist = map[string]bool{
	"uninspected": true,
	"full":        true,
	"partial":     true,
}

// finishReasonAllowlist must match the producer (timing/parser.go): the finish
// reasons the parser recognizes and retains. Unknown values are omitted.
var finishReasonAllowlist = map[string]bool{
	"stop":           true,
	"length":         true,
	"tool_calls":     true,
	"function_call":  true,
	"content_filter": true,
}

// Clone returns a deep defensive copy of the observation. Pointer fields are
// copied so caller mutations cannot affect the retained value.
func (o *ProxyObservation) Clone() *ProxyObservation {
	clone := &ProxyObservation{
		Source:                 o.Source,
		Scope:                  o.Scope,
		UpstreamRequestMapping: o.UpstreamRequestMapping,
		TransportOutcome:       o.TransportOutcome,
		GenerationOutcome:      o.GenerationOutcome,
		InspectionState:        o.InspectionState,
		FinishReason:           o.FinishReason,
	}
	clone.UpstreamHeaderMs = copyFloatPtr(o.UpstreamHeaderMs)
	clone.FirstByteMs = copyFloatPtr(o.FirstByteMs)
	clone.FirstOutputMs = copyFloatPtr(o.FirstOutputMs)
	clone.FirstAnswerMs = copyFloatPtr(o.FirstAnswerMs)
	clone.TotalDurationMs = copyFloatPtr(o.TotalDurationMs)
	clone.PromptTokens = copyIntPtr(o.PromptTokens)
	clone.CompletionTokens = copyIntPtr(o.CompletionTokens)
	clone.TotalTokens = copyIntPtr(o.TotalTokens)
	clone.CachedTokens = copyIntPtr(o.CachedTokens)
	clone.ReasoningTokens = copyIntPtr(o.ReasoningTokens)
	clone.PromptMS = copyFloatPtr(o.PromptMS)
	clone.PredictedMS = copyFloatPtr(o.PredictedMS)
	clone.PromptN = copyIntPtr(o.PromptN)
	clone.PredictedN = copyIntPtr(o.PredictedN)
	return clone
}

// copyFloatPtr returns a copy of v, or nil if v is nil. Zero values are
// preserved (a pointer to zero is not nil).
func copyFloatPtr(v *float64) *float64 {
	if v == nil {
		return nil
	}
	x := *v
	return &x
}

// copyIntPtr returns a copy of v, or nil if v is nil. Zero values are
// preserved (a pointer to zero is not nil).
func copyIntPtr(v *int64) *int64 {
	if v == nil {
		return nil
	}
	x := *v
	return &x
}

// validPhase reports true if v is unknown (nil) or a finite, nonnegative
// phase value. NaN, Inf, and negative phases are invalid.
func validPhase(v *float64) bool {
	if v == nil {
		return true
	}
	f := *v
	return !math.IsNaN(f) && !math.IsInf(f, 0) && f >= 0
}

// copyPhase copies a phase pointer. The caller must ensure validPhase(v) is
// true; otherwise the result is nil (invalid).
func copyPhase(v *float64) *float64 {
	if v == nil || !validPhase(v) {
		return nil
	}
	return copyFloatPtr(v)
}

// validInt64 reports true if v is unknown (nil) or a nonnegative integer.
func validInt64(v *int64) bool {
	if v == nil {
		return true
	}
	return *v >= 0
}

// copyInt64 copies an int64 pointer. The caller must ensure validInt64(v) is
// true; otherwise the result is nil (invalid).
func copyInt64(v *int64) *int64 {
	if v == nil || !validInt64(v) {
		return nil
	}
	return copyIntPtr(v)
}

// validFiniteFloat reports true if v is unknown (nil) or a finite, nonnegative
// float. NaN, Inf, and negative values are invalid.
func validFiniteFloat(v *float64) bool {
	if v == nil {
		return true
	}
	f := *v
	return !math.IsNaN(f) && !math.IsInf(f, 0) && f >= 0
}

// copyFiniteFloat copies a finite float pointer. The caller must ensure
// validFiniteFloat(v) is true; otherwise the result is nil (invalid).
func copyFiniteFloat(v *float64) *float64 {
	if v == nil || !validFiniteFloat(v) {
		return nil
	}
	return copyFloatPtr(v)
}

// sanitizeString returns s if it is a member of the allowlist, otherwise "".
// Unknown values are omitted (never copied).
func sanitizeString(s string, allowlist map[string]bool) string {
	if allowlist[s] {
		return s
	}
	return ""
}

// validatePhases checks that all non-nil phases are finite, nonnegative, each
// phase does not exceed the total (when total is set), and pairwise phase
// order holds whenever both sides are known: header<=firstbyte,
// byte<=output, output<=answer, byte<=answer (even if the middle is missing).
// It returns false if any check fails.
func validatePhases(o *ProxyObservation) bool {
	// Each phase must be finite and nonnegative.
	for _, p := range []*float64{
		o.UpstreamHeaderMs, o.FirstByteMs, o.FirstOutputMs, o.FirstAnswerMs,
		o.TotalDurationMs,
	} {
		if p != nil && !validPhase(p) {
			return false
		}
	}

	// A phase must not exceed the total (when total is established).
	if o.TotalDurationMs != nil {
		total := *o.TotalDurationMs
		for _, p := range []*float64{
			o.UpstreamHeaderMs, o.FirstByteMs, o.FirstOutputMs, o.FirstAnswerMs,
		} {
			if p != nil && *p > total {
				return false
			}
		}
	}

	// Pairwise order when both sides are known.
	if o.UpstreamHeaderMs != nil && o.FirstByteMs != nil && *o.UpstreamHeaderMs > *o.FirstByteMs {
		return false
	}
	if o.FirstByteMs != nil && o.FirstOutputMs != nil && *o.FirstByteMs > *o.FirstOutputMs {
		return false
	}
	if o.FirstOutputMs != nil && o.FirstAnswerMs != nil && *o.FirstOutputMs > *o.FirstAnswerMs {
		return false
	}
	if o.FirstByteMs != nil && o.FirstAnswerMs != nil && *o.FirstByteMs > *o.FirstAnswerMs {
		return false
	}

	return true
}

// projectSummary returns a validated ProxyObservation projection of summary,
// or nil if the summary is unfinished or malformed. The returned observation
// is a fresh value; callers may mutate it without affecting the summary.
func projectSummary(summary timing.Summary) *ProxyObservation {
	// Unfinished: a zero EndTime means the observation is not complete.
	if summary.EndTime.IsZero() {
		return nil
	}
	// Invalid time: zero StartTime or a year JSON cannot represent (<1 or >9999).
	if summary.StartTime.IsZero() {
		return nil
	}
	if !validJSONTime(summary.StartTime) || !validJSONTime(summary.EndTime) {
		return nil
	}
	// Invalid time horizon: end must not precede start.
	if summary.EndTime.Before(summary.StartTime) {
		return nil
	}
	// A finished summary must establish a total duration.
	if summary.TotalDurationMs == nil {
		return nil
	}

	obs := &ProxyObservation{
		Source:                 proxySource,
		Scope:                  proxyScope,
		UpstreamRequestMapping: upstreamRequestMapping,
	}

	// Wall phases: copy and validate.
	obs.UpstreamHeaderMs = copyPhase(summary.UpstreamHeaderMs)
	obs.FirstByteMs = copyPhase(summary.FirstByteMs)
	obs.FirstOutputMs = copyPhase(summary.FirstOutputMs)
	obs.FirstAnswerMs = copyPhase(summary.FirstAnswerMs)
	obs.TotalDurationMs = copyPhase(summary.TotalDurationMs)

	// Detect invalid phases: a non-nil original that copied to nil is invalid.
	if (summary.UpstreamHeaderMs != nil && obs.UpstreamHeaderMs == nil) ||
		(summary.FirstByteMs != nil && obs.FirstByteMs == nil) ||
		(summary.FirstOutputMs != nil && obs.FirstOutputMs == nil) ||
		(summary.FirstAnswerMs != nil && obs.FirstAnswerMs == nil) ||
		(summary.TotalDurationMs != nil && obs.TotalDurationMs == nil) {
		return nil
	}
	if !validatePhases(obs) {
		return nil
	}

	// ReportedUsage: nonnegative int pointers. Negative counters invalidate.
	obs.PromptTokens = copyInt64(summary.TokenUsage.PromptTokens)
	obs.CompletionTokens = copyInt64(summary.TokenUsage.CompletionTokens)
	obs.TotalTokens = copyInt64(summary.TokenUsage.TotalTokens)
	obs.CachedTokens = copyInt64(summary.TokenUsage.CachedTokens)
	obs.ReasoningTokens = copyInt64(summary.TokenUsage.ReasoningTokens)
	if (summary.TokenUsage.PromptTokens != nil && obs.PromptTokens == nil) ||
		(summary.TokenUsage.CompletionTokens != nil && obs.CompletionTokens == nil) ||
		(summary.TokenUsage.TotalTokens != nil && obs.TotalTokens == nil) ||
		(summary.TokenUsage.CachedTokens != nil && obs.CachedTokens == nil) ||
		(summary.TokenUsage.ReasoningTokens != nil && obs.ReasoningTokens == nil) {
		return nil
	}

	// ReportedEngineTimings: finite float pointers. NaN/Inf/negative
	// invalidates.
	obs.PromptMS = copyFiniteFloat(summary.EngineTimings.PromptMS)
	obs.PredictedMS = copyFiniteFloat(summary.EngineTimings.PredictedMS)
	obs.PromptN = copyInt64(summary.EngineTimings.PromptN)
	obs.PredictedN = copyInt64(summary.EngineTimings.PredictedN)
	if (summary.EngineTimings.PromptMS != nil && obs.PromptMS == nil) ||
		(summary.EngineTimings.PredictedMS != nil && obs.PredictedMS == nil) ||
		(summary.EngineTimings.PromptN != nil && obs.PromptN == nil) ||
		(summary.EngineTimings.PredictedN != nil && obs.PredictedN == nil) {
		return nil
	}

	// Finite states: unknown values are omitted, never copied.
	obs.TransportOutcome = sanitizeString(summary.TransportOutcome, transportOutcomeAllowlist)
	obs.GenerationOutcome = sanitizeString(summary.GenerationOutcome, generationOutcomeAllowlist)
	obs.InspectionState = sanitizeString(summary.InspectionState, inspectionStateAllowlist)
	obs.FinishReason = sanitizeString(summary.TokenUsage.FinishReason, finishReasonAllowlist)

	// Cross-outcome constraints: interrupted transport cannot retain
	// completed/truncated generation; partial inspection cannot be completed;
	// unknown transport cannot confidently be completed; uninspected inspection
	// forces generation uninspected.
	applyCrossOutcomeConstraints(obs)

	if estimatedProxyObservationSize(obs) > proxyObservationBudget {
		return nil
	}
	return obs
}

// applyCrossOutcomeConstraints enforces consistency between transport,
// generation, and inspection outcomes. It mutates obs in place.
func applyCrossOutcomeConstraints(obs *ProxyObservation) {
	// Interrupted transport cannot have completed/truncated generation.
	interrupted := obs.TransportOutcome == "canceled" || obs.TransportOutcome == "timeout" ||
		obs.TransportOutcome == "aborted" || obs.TransportOutcome == "transport_error"
	if interrupted {
		if obs.GenerationOutcome == "completed" || obs.GenerationOutcome == "truncated" {
			obs.GenerationOutcome = "incomplete"
		}
		// "uninspected" is preserved.
	}

	// Partial inspection cannot be completed.
	if obs.InspectionState == "partial" && obs.GenerationOutcome == "completed" {
		obs.GenerationOutcome = "unknown"
	}

	// Unknown transport cannot confidently be completed.
	if (obs.TransportOutcome == "" || obs.InspectionState == "") && obs.GenerationOutcome == "completed" {
		obs.GenerationOutcome = "unknown"
	}

	// Uninspected inspection forces generation uninspected.
	if obs.InspectionState == "uninspected" {
		obs.GenerationOutcome = "uninspected"
	}
	// Inspection did not establish body-derived evidence. Preserve wall times,
	// but never attach forged usage/output/engine values to an uninspected body.
	if obs.InspectionState == "uninspected" || obs.InspectionState == "" {
		obs.FirstOutputMs, obs.FirstAnswerMs = nil, nil
		obs.PromptTokens, obs.CompletionTokens, obs.TotalTokens = nil, nil, nil
		obs.CachedTokens, obs.ReasoningTokens = nil, nil
		obs.PromptMS, obs.PredictedMS, obs.PromptN, obs.PredictedN = nil, nil, nil, nil
		obs.FinishReason = ""
	}
}

// validJSONTime reports true if t is a non-zero time whose year JSON can
// represent (years 1..9999). Years outside this range fail JSON marshaling.
func validJSONTime(t time.Time) bool {
	return t.Year() >= 1 && t.Year() <= 9999
}

// estimatedProxyObservationSize estimates the serialized size of an
// observation. Fixed fields keep the size bounded well under the budget.
func estimatedProxyObservationSize(o *ProxyObservation) int64 {
	size := int64(len(o.Source) + len(o.Scope) + len(o.UpstreamRequestMapping) +
		len(o.TransportOutcome) + len(o.GenerationOutcome) + len(o.InspectionState) +
		len(o.FinishReason))
	fields := []*float64{
		o.UpstreamHeaderMs, o.FirstByteMs, o.FirstOutputMs, o.FirstAnswerMs,
		o.TotalDurationMs, o.PromptMS, o.PredictedMS,
	}
	for _, f := range fields {
		if f != nil {
			size += 8
		}
	}
	intFields := []*int64{
		o.PromptTokens, o.CompletionTokens, o.TotalTokens, o.CachedTokens,
		o.ReasoningTokens, o.PromptN, o.PredictedN,
	}
	for _, f := range intFields {
		if f != nil {
			size += 8
		}
	}
	return size + 64 // overhead
}

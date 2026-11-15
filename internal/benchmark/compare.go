package benchmark

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"
)

type MetricStatistics struct {
	SampleCount int      `json:"sample_count"`
	Mean        *float64 `json:"mean,omitempty"`
	StdDev      *float64 `json:"sample_stddev,omitempty"`
	Min         *float64 `json:"min,omitempty"`
	Max         *float64 `json:"max,omitempty"`
}

type MetricComparison struct {
	Name          string           `json:"name"`
	Baseline      MetricStatistics `json:"baseline"`
	Candidate     MetricStatistics `json:"candidate"`
	ChangePercent *float64         `json:"change_percent,omitempty"`
	Attribution   string           `json:"attribution"`
}

type CorrectnessCounts struct {
	Passed       int `json:"passed"`
	Failed       int `json:"failed"`
	NotEvaluated int `json:"not_evaluated"`
}

type PairedCorrectnessCounts struct {
	Baseline  CorrectnessCounts `json:"baseline"`
	Candidate CorrectnessCounts `json:"candidate"`
}

type FailureCounts struct {
	FailedSamples        int `json:"failed_samples"`
	CanceledSamples      int `json:"canceled_samples"`
	IncompleteTransport  int `json:"incomplete_transport"`
	UnknownTransport     int `json:"unknown_transport"`
	IncompleteGeneration int `json:"incomplete_generation"`
	UnknownGeneration    int `json:"unknown_generation"`
	TruncatedGeneration  int `json:"truncated_generation"`
}

type PairedFailureCounts struct {
	Baseline  FailureCounts `json:"baseline"`
	Candidate FailureCounts `json:"candidate"`
}

type WorkloadCoverage struct {
	Recorded        bool `json:"recorded"`
	SamplesRecorded int  `json:"samples_recorded"`
	SamplesExpected int  `json:"samples_expected"`
}

type PairedWorkloadCoverage struct {
	Baseline  WorkloadCoverage `json:"baseline"`
	Candidate WorkloadCoverage `json:"candidate"`
}

type WorkloadComparison struct {
	Name        string                  `json:"name"`
	Coverage    PairedWorkloadCoverage  `json:"coverage"`
	Correctness PairedCorrectnessCounts `json:"correctness"`
	Failures    PairedFailureCounts     `json:"failures"`
	Metrics     []MetricComparison      `json:"metrics"`
}

type ContextMetrics struct {
	ModelState                   string   `json:"model_state"`
	ObservedAt                   string   `json:"observed_at,omitempty"`
	ActivityState                string   `json:"activity_state,omitempty"`
	ActivityFreshness            string   `json:"activity_freshness,omitempty"`
	ActivityTruncated            bool     `json:"activity_truncated,omitempty"`
	ActivityRowCount             int      `json:"activity_row_count"`
	AggregateAcceptanceRate      *float64 `json:"aggregate_acceptance_rate,omitempty"`
	AggregateDraftTokens         *int64   `json:"aggregate_draft_tokens,omitempty"`
	AggregateAcceptedDraftTokens *int64   `json:"aggregate_accepted_draft_tokens,omitempty"`
	GPUState                     string   `json:"shared_gpu_state,omitempty"`
	GPUFreshness                 string   `json:"shared_gpu_freshness,omitempty"`
	GPUSource                    string   `json:"shared_gpu_source,omitempty"`
	GPUMemoryMinMB               *int     `json:"approximate_shared_gpu_memory_min_mb,omitempty"`
	GPUMemoryMaxMB               *int     `json:"approximate_shared_gpu_memory_max_mb,omitempty"`
	GPUSampleCount               int      `json:"approximate_shared_gpu_sample_count"`
	GPUSharedDeviceOnly          bool     `json:"shared_device_only,omitempty"`
	GPUExecutionDeviceUnverified bool     `json:"execution_device_unverified,omitempty"`
	GPUClockAlignmentUnverified  bool     `json:"clock_alignment_unverified,omitempty"`
	Attribution                  string   `json:"attribution"`
}

type ContextComparison struct {
	BaselineBefore          ContextMetrics `json:"baseline_before"`
	BaselineAfter           ContextMetrics `json:"baseline_after"`
	CandidateBefore         ContextMetrics `json:"candidate_before"`
	CandidateAfter          ContextMetrics `json:"candidate_after"`
	PerRoundAcceptanceState string         `json:"per_round_acceptance_state"`
}

type ComparisonReport struct {
	SchemaVersion  int                  `json:"schema_version"`
	BaselineRunID  string               `json:"baseline_run_id"`
	CandidateRunID string               `json:"candidate_run_id"`
	Confounders    []string             `json:"confounders"`
	Workloads      []WorkloadComparison `json:"workloads"`
	Context        ContextComparison    `json:"context"`
	Interpretation string               `json:"interpretation"`
}

func Compare(baseline, candidate RunRecord) (ComparisonReport, error) {
	if err := baseline.Validate(); err != nil {
		return ComparisonReport{}, fmt.Errorf("invalid baseline: %w", err)
	}
	if err := candidate.Validate(); err != nil {
		return ComparisonReport{}, fmt.Errorf("invalid candidate: %w", err)
	}
	if baseline.WorkloadVersion != candidate.WorkloadVersion {
		return ComparisonReport{}, errors.New("runs use different workload versions")
	}

	report := ComparisonReport{
		SchemaVersion:  SchemaVersion,
		BaselineRunID:  baseline.RunID,
		CandidateRunID: candidate.RunID,
		Confounders:    compareConfounders(baseline, candidate),
		Workloads:      make([]WorkloadComparison, 0, len(syntheticWorkloads)),
		Context: ContextComparison{
			BaselineBefore:          contextMetrics(baseline.ModelBefore),
			BaselineAfter:           contextMetrics(baseline.ModelAfter),
			CandidateBefore:         contextMetrics(candidate.ModelBefore),
			CandidateAfter:          contextMetrics(candidate.ModelAfter),
			PerRoundAcceptanceState: "unsupported: available diagnostics expose aggregate draft counters, not per-round records",
		},
		Interpretation: "Performance metrics are descriptive; correctness is reported separately. Aggregate backend metrics and shared GPU samples are contextual, not attributed to this run. A higher acceptance percentage alone is not evidence of faster performance.",
	}

	for i, expected := range syntheticWorkloads {
		base := WorkloadResult{Name: expected.Name, Category: expected.Category}
		candidateResult := base
		baseRecorded := i < len(baseline.Workloads)
		candidateRecorded := i < len(candidate.Workloads)
		if baseRecorded {
			base = baseline.Workloads[i]
		}
		if candidateRecorded {
			candidateResult = candidate.Workloads[i]
		}
		comparison := compareWorkload(base, candidateResult)
		comparison.Coverage = PairedWorkloadCoverage{
			Baseline:  WorkloadCoverage{Recorded: baseRecorded, SamplesRecorded: len(base.Samples), SamplesExpected: baseline.Requested.Repetitions},
			Candidate: WorkloadCoverage{Recorded: candidateRecorded, SamplesRecorded: len(candidateResult.Samples), SamplesExpected: candidate.Requested.Repetitions},
		}
		report.Workloads = append(report.Workloads, comparison)
	}
	return report, nil
}

func compareWorkload(baseline, candidate WorkloadResult) WorkloadComparison {
	out := WorkloadComparison{
		Name: baseline.Name,
	}
	baseMetrics := collectMetrics(baseline.Samples)
	candidateMetrics := collectMetrics(candidate.Samples)
	names := make([]string, 0, len(baseMetrics)+len(candidateMetrics))
	seen := make(map[string]bool, len(baseMetrics)+len(candidateMetrics))
	for name := range baseMetrics {
		names = append(names, name)
		seen[name] = true
	}
	for name := range candidateMetrics {
		if !seen[name] {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		baseStats := summarize(baseMetrics[name])
		candidateStats := summarize(candidateMetrics[name])
		var change *float64
		if baseStats.Mean != nil && candidateStats.Mean != nil && *baseStats.Mean != 0 {
			value := (*candidateStats.Mean - *baseStats.Mean) / *baseStats.Mean * 100
			change = &value
		}
		out.Metrics = append(out.Metrics, MetricComparison{
			Name:          name,
			Baseline:      baseStats,
			Candidate:     candidateStats,
			ChangePercent: change,
			Attribution:   metricAttribution(name),
		})
	}
	for side, samples := range [][]SampleResult{baseline.Samples, candidate.Samples} {
		correctness := &out.Correctness.Baseline
		failures := &out.Failures.Baseline
		if side == 1 {
			correctness = &out.Correctness.Candidate
			failures = &out.Failures.Candidate
		}
		for _, sample := range samples {
			switch sample.Correctness {
			case CorrectnessPassed:
				correctness.Passed++
			case CorrectnessFailed:
				correctness.Failed++
			default:
				correctness.NotEvaluated++
			}
			switch sample.Status {
			case SampleFailed:
				failures.FailedSamples++
			case SampleCanceled:
				failures.CanceledSamples++
			}
			for _, request := range sample.Requests {
				if request.ProxyObservation == nil {
					failures.UnknownTransport++
					failures.UnknownGeneration++
					continue
				}
				if request.ProxyObservation.TransportOutcome != "" && request.ProxyObservation.TransportOutcome != "returned" {
					if request.ProxyObservation.TransportOutcome == "unknown" {
						failures.UnknownTransport++
					} else {
						failures.IncompleteTransport++
					}
				} else if request.ProxyObservation.TransportOutcome == "" {
					failures.UnknownTransport++
				}
				switch request.ProxyObservation.GenerationOutcome {
				case "incomplete", "uninspected":
					failures.IncompleteGeneration++
				case "", "unknown":
					failures.UnknownGeneration++
				case "truncated":
					failures.TruncatedGeneration++
				}
			}
		}
	}
	return out
}

func collectMetrics(samples []SampleResult) map[string][]float64 {
	metrics := map[string][]float64{}
	add := func(name string, value *float64) {
		if value != nil && !math.IsNaN(*value) && !math.IsInf(*value, 0) && *value >= 0 {
			metrics[name] = append(metrics[name], *value)
		}
	}
	for _, sample := range samples {
		if sample.Status != SampleCompleted {
			continue
		}
		add("end_to_end_ms", sample.DurationMS)
		inputBytes := float64(sample.InputBytes)
		outputBytes := float64(sample.OutputBytes)
		metrics["input_bytes"] = append(metrics["input_bytes"], inputBytes)
		metrics["output_bytes"] = append(metrics["output_bytes"], outputBytes)
		for _, request := range sample.Requests {
			addObservationMetrics(add, request)
			if request.ProxyObservation != nil {
				add("prompt_tokens", int64PointerFloat(request.ProxyObservation.PromptTokens))
				add("completion_tokens", int64PointerFloat(request.ProxyObservation.CompletionTokens))
				add("total_tokens", int64PointerFloat(request.ProxyObservation.TotalTokens))
				add("cached_tokens", int64PointerFloat(request.ProxyObservation.CachedTokens))
				add("reasoning_tokens", int64PointerFloat(request.ProxyObservation.ReasoningTokens))
			}
		}
	}
	return metrics
}

func addObservationMetrics(add func(string, *float64), request RequestMeasurement) {
	observation := request.ProxyObservation
	if observation == nil {
		return
	}
	add("upstream_header_ms", observation.UpstreamHeaderMs)
	add("first_byte_ms", observation.FirstByteMs)
	add("first_output_ms", observation.FirstOutputMs)
	add("first_answer_ms", observation.FirstAnswerMs)
	add("proxy_observed_total_ms", observation.TotalDurationMs)
	add("prefill_tokens_per_second", reportedRate(observation.PromptN, observation.PromptMS))
	add("decode_tokens_per_second", reportedRate(observation.PredictedN, observation.PredictedMS))
}

func reportedRate(tokens *int64, milliseconds *float64) *float64 {
	if tokens == nil || milliseconds == nil || *tokens < 0 || *milliseconds <= 0 ||
		math.IsNaN(*milliseconds) || math.IsInf(*milliseconds, 0) {
		return nil
	}
	rate := float64(*tokens) * 1000 / *milliseconds
	if math.IsNaN(rate) || math.IsInf(rate, 0) {
		return nil
	}
	return &rate
}

func int64PointerFloat(value *int64) *float64 {
	if value == nil {
		return nil
	}
	number := float64(*value)
	return &number
}

func summarize(values []float64) MetricStatistics {
	out := MetricStatistics{SampleCount: len(values)}
	if len(values) == 0 {
		return out
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	mean := 0.0
	for _, value := range sorted {
		mean += value
	}
	mean /= float64(len(sorted))
	minimum, maximum := sorted[0], sorted[len(sorted)-1]
	out.Mean, out.Min, out.Max = &mean, &minimum, &maximum
	if len(sorted) > 1 {
		var squared float64
		for _, value := range sorted {
			delta := value - mean
			squared += delta * delta
		}
		stddev := math.Sqrt(squared / float64(len(sorted)-1))
		out.StdDev = &stddev
	}
	return out
}

func metricAttribution(name string) string {
	switch name {
	case "end_to_end_ms":
		return "measured by the benchmark client around the request sequence"
	case "input_bytes", "output_bytes", "prompt_tokens", "completion_tokens",
		"total_tokens", "cached_tokens", "reasoning_tokens":
		return "measured or reported for the benchmark request; response content is not persisted"
	case "prefill_tokens_per_second", "decode_tokens_per_second":
		return "derived from backend-reported token counts and duration in the exact proxy request observation"
	default:
		return "exact proxy request observation retrieved by the returned proxy request ID"
	}
}

func compareConfounders(a, b RunRecord) []string {
	var out []string
	if a.Status != RunCompleted {
		out = append(out, "baseline run is "+string(a.Status))
	}
	if b.Status != RunCompleted {
		out = append(out, "candidate run is "+string(b.Status))
	}
	if a.Requested.Backend != b.Requested.Backend {
		out = append(out, "backend differs")
	}
	if a.Requested.Model != b.Requested.Model {
		out = append(out, "model differs")
	}
	if a.Requested.Temperature != b.Requested.Temperature || a.Requested.TopP != b.Requested.TopP {
		out = append(out, "sampling settings differ")
	}
	if a.Requested.MaxOutput != b.Requested.MaxOutput {
		out = append(out, "maximum output token budgets differ")
	}
	if a.Requested.Reasoning != b.Requested.Reasoning {
		out = append(out, "reasoning effort differs")
	}
	if a.Requested.CacheState != b.Requested.CacheState {
		out = append(out, "declared cache states differ")
	}
	if a.Requested.Repetitions != b.Requested.Repetitions {
		out = append(out, "repetition counts differ")
	}
	if a.Requested.Concurrency != b.Requested.Concurrency {
		out = append(out, "concurrency differs")
	}
	if a.Requested.CacheEvidence != "verified" || b.Requested.CacheEvidence != "verified" {
		out = append(out, "cache state is declared, not independently verified")
	}
	out = append(out, "ambient backend load and active inference are not detectable through the available API")
	return out
}

func contextMetrics(observation *ModelObservation) ContextMetrics {
	out := ContextMetrics{
		ModelState:  "unknown",
		Attribution: "model-level aggregate backend activity and approximate shared-device GPU window; neither is joined to an individual benchmark request or model-owned execution",
	}
	if observation == nil {
		return out
	}
	out.ModelState = observation.State
	if !observation.ObservedAt.IsZero() {
		out.ObservedAt = observation.ObservedAt.UTC().Format(time.RFC3339Nano)
	}
	if observation.Activity != nil {
		out.ActivityState = string(observation.Activity.State)
		out.ActivityFreshness = observation.Activity.Freshness
		out.ActivityTruncated = observation.Activity.Truncated
		out.ActivityRowCount = observation.Activity.RowCount
		if observation.Activity.DraftRate != nil {
			out.AggregateAcceptanceRate = observation.Activity.DraftRate.Rate
			out.AggregateDraftTokens = observation.Activity.DraftRate.ProposedTokens
			out.AggregateAcceptedDraftTokens = observation.Activity.DraftRate.AcceptedTokens
		}
	}
	if observation.GPUWindow != nil {
		out.GPUState = string(observation.GPUWindow.State)
		out.GPUFreshness = observation.GPUWindow.Freshness
		out.GPUSource = observation.GPUWindow.Source
		out.GPUMemoryMinMB = observation.GPUWindow.MinMemUsedMB
		out.GPUMemoryMaxMB = observation.GPUWindow.MaxMemUsedMB
		out.GPUSampleCount = observation.GPUWindow.SampleCount
		out.GPUSharedDeviceOnly = observation.GPUWindow.SharedDeviceOnly
		out.GPUExecutionDeviceUnverified = observation.GPUWindow.ExecutionDeviceUnverified
		out.GPUClockAlignmentUnverified = observation.GPUWindow.ClockAlignmentUnverified
	}
	return out
}

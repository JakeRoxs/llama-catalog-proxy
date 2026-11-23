package benchmark

import (
	"errors"
	"math"
	"regexp"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/correlation"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modeldiagnostics"
)

const (
	SchemaVersion       = 1
	WorkloadVersion     = "1"
	MaxRepetitions      = 3
	MaxOutputTokens     = 128
	MaxRunDuration      = 5 * time.Minute
	MaxReportBytes      = 2 << 20
	MaxRequests         = 21
	MaxTotalTokenBudget = 4096
	DefaultOutputDir    = "benchmark-runs"
	RequiredConcurrency = 1
)

var (
	runIDPattern     = regexp.MustCompile(`^bench-[0-9a-f]{32}$`)
	requestIDPattern = regexp.MustCompile(`^(?:[0-9a-f]{32}|req-[0-9]{1,64})$`)
)

type CacheState string

const (
	CacheUnknown CacheState = "unknown"
	CacheWarm    CacheState = "warm"
	CacheCold    CacheState = "cold"
)

type RunStatus string

const (
	RunCompleted RunStatus = "completed"
	RunCanceled  RunStatus = "canceled"
	RunPartial   RunStatus = "partial"
)

type SampleStatus string

const (
	SampleCompleted SampleStatus = "completed"
	SampleFailed    SampleStatus = "failed"
	SampleCanceled  SampleStatus = "canceled"
)

type CorrectnessStatus string

const (
	CorrectnessPassed       CorrectnessStatus = "passed"
	CorrectnessFailed       CorrectnessStatus = "failed"
	CorrectnessNotEvaluated CorrectnessStatus = "not_evaluated"
)

type RequestedConfiguration struct {
	Backend       string     `json:"backend"`
	Model         string     `json:"model"`
	Temperature   float64    `json:"temperature"`
	TopP          float64    `json:"top_p"`
	MaxOutput     int        `json:"max_output_tokens"`
	Reasoning     string     `json:"reasoning_effort,omitempty"`
	CacheState    CacheState `json:"declared_cache_state"`
	CacheEvidence string     `json:"cache_state_verification"`
	Repetitions   int        `json:"repetitions"`
	Concurrency   int        `json:"concurrency"`
	TimeBudget    string     `json:"time_budget"`
	TokenBudget   int        `json:"max_output_token_budget"`
}

type ModelObservation struct {
	State       string                                   `json:"state"`
	ObservedAt  time.Time                                `json:"observed_at,omitempty"`
	Coverage    string                                   `json:"coverage,omitempty"`
	Identity    modeldiagnostics.ModelIdentity           `json:"identity"`
	Lifecycle   *modeldiagnostics.ModelLifecycleSnapshot `json:"lifecycle,omitempty"`
	Provenance  *modeldiagnostics.Provenance             `json:"provenance,omitempty"`
	Activity    *modeldiagnostics.ActivityMeasurement    `json:"activity,omitempty"`
	GPUWindow   *modeldiagnostics.SharedGPUWindow        `json:"approximate_shared_gpu_window,omitempty"`
	ConfigState string                                   `json:"applied_inference_configuration_state"`
}

type RequestMeasurement struct {
	RequestID        string                        `json:"request_id,omitempty"`
	StartedAt        time.Time                     `json:"started_at"`
	EndedAt          time.Time                     `json:"ended_at"`
	HTTPStatus       int                           `json:"http_status,omitempty"`
	RequestBytes     int                           `json:"request_bytes"`
	PromptBytes      int                           `json:"prompt_bytes"`
	ResponseBytes    int                           `json:"response_bytes,omitempty"`
	OutputBytes      int                           `json:"output_bytes,omitempty"`
	MaxTokens        int                           `json:"max_output_tokens"`
	ProxyObservation *correlation.ProxyObservation `json:"proxy_observation,omitempty"`
	DiagnosticState  string                        `json:"diagnostic_state"`
	ErrorClass       string                        `json:"error_class,omitempty"`
}

type WorkloadResult struct {
	Name            string         `json:"name"`
	Category        string         `json:"category"`
	Version         string         `json:"version"`
	ReasoningEffort string         `json:"reasoning_effort,omitempty"`
	Samples         []SampleResult `json:"samples"`
}

type SampleResult struct {
	Iteration    int                  `json:"iteration"`
	Status       SampleStatus         `json:"status"`
	Correctness  CorrectnessStatus    `json:"correctness"`
	DurationMS   *float64             `json:"duration_ms,omitempty"`
	RequestCount int                  `json:"request_count"`
	InputBytes   int                  `json:"input_bytes"`
	OutputBytes  int                  `json:"output_bytes"`
	Requests     []RequestMeasurement `json:"requests"`
	ErrorClass   string               `json:"error_class,omitempty"`
}

type RunRecord struct {
	SchemaVersion        int                    `json:"schema_version"`
	RunID                string                 `json:"run_id"`
	WorkloadVersion      string                 `json:"workload_version"`
	StartedAt            time.Time              `json:"started_at"`
	CompletedAt          time.Time              `json:"completed_at,omitzero"`
	Status               RunStatus              `json:"status"`
	Requested            RequestedConfiguration `json:"requested"`
	OutputTokensReserved int                    `json:"output_tokens_reserved"`
	ModelBefore          *ModelObservation      `json:"model_before,omitempty"`
	ModelAfter           *ModelObservation      `json:"model_after,omitempty"`
	Workloads            []WorkloadResult       `json:"workloads"`
	Limitations          []string               `json:"limitations"`
}

func (r RunRecord) Validate() error {
	if r.SchemaVersion != SchemaVersion || !runIDPattern.MatchString(r.RunID) ||
		r.WorkloadVersion != WorkloadVersion || r.StartedAt.IsZero() {
		return errors.New("invalid benchmark run metadata")
	}
	if err := r.Requested.Validate(); err != nil {
		return err
	}
	if r.OutputTokensReserved < 0 || r.OutputTokensReserved > r.Requested.TokenBudget {
		return errors.New("benchmark output token budget exceeded")
	}
	if r.CompletedAt.IsZero() && r.Status != RunCanceled {
		return errors.New("incomplete benchmark run must be marked canceled")
	}
	if !r.CompletedAt.IsZero() && r.CompletedAt.Before(r.StartedAt) {
		return errors.New("benchmark run completion precedes start")
	}
	if r.Status != RunCompleted && r.Status != RunCanceled && r.Status != RunPartial {
		return errors.New("invalid benchmark run status")
	}
	if !validModelObservation(r.ModelBefore, r.Requested.Backend, r.Requested.Model) {
		return errors.New("benchmark run contains an invalid pre-run model observation")
	}
	if r.ModelAfter != nil && !validModelObservation(r.ModelAfter, r.Requested.Backend, r.Requested.Model) {
		return errors.New("benchmark run contains an invalid post-run model observation")
	}
	if len(r.Limitations) > 16 {
		return errors.New("benchmark run contains too many limitations")
	}
	for _, limitation := range r.Limitations {
		if limitation == "" || len(limitation) > 256 || !utf8.ValidString(limitation) {
			return errors.New("benchmark run contains an invalid limitation")
		}
	}
	if len(r.Workloads) > len(syntheticWorkloads) ||
		(r.Status == RunCompleted && len(r.Workloads) != len(syntheticWorkloads)) {
		return errors.New("benchmark run has an unexpected workload count")
	}
	requests := 0
	reservedTokens := 0
	for i := range r.Workloads {
		if err := r.validateWorkload(i, &requests, &reservedTokens); err != nil {
			return err
		}
	}
	if requests > MaxRequests || reservedTokens != r.OutputTokensReserved ||
		reservedTokens > r.Requested.TokenBudget {
		return errors.New("benchmark request or token budget exceeded")
	}
	return nil
}

// validateWorkload validates a single workload's metadata and all of its samples.
// It accumulates request and reserved-token counts into the provided counters.
func (r RunRecord) validateWorkload(i int, requests *int, reservedTokens *int) error {
	expected := syntheticWorkloads[i]
	workload := r.Workloads[i]
	if workload.Name != expected.Name || workload.Category != expected.Category ||
		workload.Version != expected.Version || workload.ReasoningEffort != expected.ReasoningEffort ||
		len(workload.Samples) > r.Requested.Repetitions {
		return errors.New("benchmark run contains an invalid workload")
	}
	iterations := make(map[int]bool, len(workload.Samples))
	for _, sample := range workload.Samples {
		if err := r.validateSample(sample, iterations); err != nil {
			return err
		}
		iterations[sample.Iteration] = true
		if sample.DurationMS != nil && (math.IsNaN(*sample.DurationMS) || math.IsInf(*sample.DurationMS, 0) || *sample.DurationMS < 0) {
			return errors.New("benchmark run contains an invalid sample duration")
		}
		*requests += sample.RequestCount
		if err := r.validateRequests(sample.Requests, reservedTokens); err != nil {
			return err
		}
	}
	if r.Status == RunCompleted && len(workload.Samples) != r.Requested.Repetitions {
		return errors.New("completed benchmark run is missing samples")
	}
	return nil
}

// validateSample validates a single sample's fields and request list. The iterations
// map tracks seen iteration indices to detect duplicates within a workload.
func (r RunRecord) validateSample(sample SampleResult, iterations map[int]bool) error {
	if sample.Iteration < 1 || sample.Iteration > r.Requested.Repetitions ||
		iterations[sample.Iteration] ||
		(sample.Status != SampleCompleted && sample.Status != SampleFailed && sample.Status != SampleCanceled) ||
		(sample.Correctness != CorrectnessPassed && sample.Correctness != CorrectnessFailed && sample.Correctness != CorrectnessNotEvaluated) ||
		sample.RequestCount != len(sample.Requests) || sample.RequestCount > 2 ||
		sample.InputBytes < 0 || sample.OutputBytes < 0 || !validErrorClass(sample.ErrorClass) ||
		(r.Status == RunCompleted && sample.Status != SampleCompleted) {
		return errors.New("benchmark run contains an invalid sample")
	}
	if sample.DurationMS != nil && (math.IsNaN(*sample.DurationMS) || math.IsInf(*sample.DurationMS, 0) || *sample.DurationMS < 0) {
		return errors.New("benchmark run contains an invalid sample duration")
	}
	return nil
}

// validateRequests validates all request measurements in a sample and accumulates
// reserved tokens. It shares the same error identity as the original inline check.
func (r RunRecord) validateRequests(requests []RequestMeasurement, reservedTokens *int) error {
	for _, request := range requests {
		if request.RequestBytes < 0 || request.PromptBytes < 0 || request.ResponseBytes < 0 ||
			request.OutputBytes < 0 || request.MaxTokens < 1 || request.MaxTokens > r.Requested.MaxOutput ||
			request.StartedAt.IsZero() || (!request.EndedAt.IsZero() && request.EndedAt.Before(request.StartedAt)) ||
			(request.RequestID != "" && !validRequestID(request.RequestID)) ||
			(request.HTTPStatus != 0 && (request.HTTPStatus < 100 || request.HTTPStatus > 599)) ||
			!validErrorClass(request.ErrorClass) || !validDiagnosticState(request.DiagnosticState) {
			return errors.New("benchmark run contains invalid request measurements")
		}
		*reservedTokens += request.MaxTokens
	}
	return nil
}

func (c RequestedConfiguration) Validate() error {
	timeBudget, timeErr := time.ParseDuration(c.TimeBudget)
	if !validIdentifier(c.Backend) || !validIdentifier(c.Model) ||
		math.IsNaN(c.Temperature) || math.IsInf(c.Temperature, 0) ||
		c.Temperature < 0 || c.Temperature > 2 ||
		math.IsNaN(c.TopP) || math.IsInf(c.TopP, 0) ||
		c.TopP <= 0 || c.TopP > 1 ||
		c.MaxOutput < 1 || c.MaxOutput > MaxOutputTokens ||
		c.Repetitions < 1 || c.Repetitions > MaxRepetitions ||
		c.Concurrency != RequiredConcurrency || c.TokenBudget < 1 ||
		c.TokenBudget > MaxTotalTokenBudget || timeErr != nil ||
		timeBudget <= 0 || timeBudget > MaxRunDuration || !validCacheState(c.CacheState) ||
		c.CacheEvidence != "declared_unverified" || c.Reasoning != "per_workload" {
		return errors.New("invalid benchmark run configuration")
	}
	return nil
}

func validIdentifier(value string) bool {
	if value == "" || len(value) > 256 || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func validModelObservation(observation *ModelObservation, backend, model string) bool {
	return observation != nil && observation.Identity.Valid() &&
		observation.Identity.BackendID == backend && observation.Identity.PublicID == model &&
		observation.State == "ready" && observation.ConfigState == "not_exposed_by_diagnostics_api" &&
		observation.Lifecycle != nil && observation.Lifecycle.Observed &&
		observation.Lifecycle.CurrentState == "ready" && observation.Lifecycle.ObservedAt != nil &&
		!observation.Lifecycle.ObservedAt.IsZero()
}

func validRequestID(value string) bool {
	return requestIDPattern.MatchString(value)
}

func validDiagnosticState(value string) bool {
	switch value {
	case "pending", "available", "request_id_unavailable", "history_unavailable",
		"unauthorized", "query_failed", "observation_unavailable":
		return true
	default:
		return false
	}
}

func validErrorClass(value string) bool {
	switch value {
	case "", "request_failed", "response_invalid", "request_budget_exhausted",
		"diagnostics_unavailable", "model_not_ready", "invalid_tool_call",
		"unexpected_tool_output", "request_canceled", "request_timeout":
		return true
	default:
		return false
	}
}

func validCacheState(state CacheState) bool {
	return state == CacheUnknown || state == CacheWarm || state == CacheCold
}

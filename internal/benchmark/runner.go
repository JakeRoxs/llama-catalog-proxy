package benchmark

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

type RunOptions struct {
	Client            *APIClient
	Backend           string
	Model             string
	Temperature       float64
	TopP              float64
	MaxOutput         int
	Repetitions       int
	CacheState        CacheState
	TimeBudget        time.Duration
	TokenBudget       int
	AllowInterference bool
}

func Run(ctx context.Context, options RunOptions) (RunRecord, error) {
	config, err := requestedConfiguration(options)
	if err != nil {
		return RunRecord{}, err
	}
	if options.TimeBudget == 0 {
		options.TimeBudget = MaxRunDuration
	}
	runCtx, cancel := context.WithTimeout(ctx, options.TimeBudget)
	defer cancel()
	modelBefore, err := options.Client.ObserveReadyModel(runCtx, options.Backend, options.Model)
	if err != nil {
		return RunRecord{}, fmt.Errorf("preflight failed: %w", err)
	}
	runID, err := newRunID()
	if err != nil {
		return RunRecord{}, errors.New("could not create benchmark run ID")
	}
	record := newRunRecord(runID, config, modelBefore)
	workloads, partial := runWorkloads(runCtx, options.Client, config, &record)
	record.Workloads = workloads
	record.Status = determineRunStatus(runCtx, partial, len(workloads))
	if runCtx.Err() == nil {
		observePostflightModel(runCtx, options.Client, options.Backend, options.Model, &record)
	}
	record.CompletedAt = time.Now().UTC()
	if err := record.Validate(); err != nil {
		return RunRecord{}, fmt.Errorf("benchmark produced an invalid report: %w", err)
	}
	return record, nil
}

func requestedConfiguration(options RunOptions) (RequestedConfiguration, error) {
	if !options.AllowInterference {
		return RequestedConfiguration{}, errors.New("live benchmark requires --allow-active-inference-interference")
	}
	if options.Client == nil {
		return RequestedConfiguration{}, errors.New("benchmark diagnostics client is required")
	}
	if options.TimeBudget == 0 {
		options.TimeBudget = MaxRunDuration
	}
	if options.TokenBudget == 0 {
		options.TokenBudget = MaxTotalTokenBudget
	}
	config := RequestedConfiguration{
		Backend:       options.Backend,
		Model:         options.Model,
		Temperature:   options.Temperature,
		TopP:          options.TopP,
		MaxOutput:     options.MaxOutput,
		Reasoning:     "per_workload",
		CacheState:    options.CacheState,
		CacheEvidence: "declared_unverified",
		Repetitions:   options.Repetitions,
		Concurrency:   RequiredConcurrency,
		TimeBudget:    options.TimeBudget.String(),
		TokenBudget:   options.TokenBudget,
	}
	if config.MaxOutput == 0 {
		config.MaxOutput = 64
	}
	if config.Repetitions == 0 {
		config.Repetitions = 1
	}
	if config.CacheState == "" {
		config.CacheState = CacheUnknown
	}
	if err := validateRequestedConfiguration(config); err != nil {
		return RequestedConfiguration{}, err
	}
	return config, nil
}

func validateRequestedConfiguration(config RequestedConfiguration) error {
	return config.Validate()
}

func runSample(ctx context.Context, client *APIClient, workload syntheticWorkload, config RequestedConfiguration, iteration int, record *RunRecord) (SampleResult, bool) {
	started := time.Now()
	sample, messages := newSampleAndMessages(workload, iteration)
	runner := &sampleRequestRunner{
		client:   client,
		workload: workload,
		config:   config,
		record:   record,
		sample:   &sample,
	}
	first, failed, exhausted := sendRequest(ctx, runner, messages, workload.ToolRoundTrip)
	if failed || exhausted {
		setSampleFailure(&sample, ctx)
		finalizeSample(&sample, started, ctx, client, config)
		return sample, exhausted
	}
	if workload.ToolRoundTrip {
		exhausted = runToolRoundTrip(ctx, runner, &sample, messages, first, workload)
	} else {
		recordTextCorrectness(&sample, first.Content, workload.Expected, "")
	}
	finalizeSample(&sample, started, ctx, client, config)
	return sample, exhausted
}

// newSampleAndMessages constructs a fresh SampleResult and the initial chat
// message sequence for a workload. It preserves the system prompt when present.
func newSampleAndMessages(workload syntheticWorkload, iteration int) (SampleResult, []chatMessage) {
	sample := SampleResult{
		Iteration:   iteration,
		Status:      SampleCompleted,
		Correctness: CorrectnessNotEvaluated,
		Requests:    make([]RequestMeasurement, 0, 2),
	}
	messages := make([]chatMessage, 0, 4)
	if workload.SystemPrompt != "" {
		messages = append(messages, chatMessage{Role: "system", Content: workload.SystemPrompt})
	}
	messages = append(messages, chatMessage{Role: "user", Content: workload.UserPrompt})
	return sample, messages
}

// runToolRoundTrip executes the assistant tool call and the subsequent tool
// response request. It returns whether the sample's token budget was exhausted.
func runToolRoundTrip(ctx context.Context, runner *sampleRequestRunner, sample *SampleResult, messages []chatMessage, first chatResult, workload syntheticWorkload) bool {
	toolOutput, ok := toolRoundTripOutput(first.ToolCalls, "TOOL_ROUND_TRIP_OK")
	if !ok {
		sample.Correctness = CorrectnessFailed
		sample.ErrorClass = "invalid_tool_call"
		return false
	}
	messages = append(messages, chatMessage{Role: "assistant", ToolCalls: first.ToolCalls})
	messages = append(messages, chatMessage{Role: "tool", ToolCallID: first.ToolCalls[0].ID, Content: toolOutput})
	second, secondFailed, secondExhausted := sendRequest(ctx, runner, messages, false)
	if secondFailed || secondExhausted {
		setSampleFailure(sample, ctx)
		return secondExhausted
	}
	recordTextCorrectness(sample, second.Content, workload.Expected, "unexpected_tool_output")
	return false
}

// finalizeSample stamps the sample duration, attaches proxy observations for
// every recorded request, and sets the request count.
func finalizeSample(sample *SampleResult, started time.Time, ctx context.Context, client *APIClient, config RequestedConfiguration) {
	sample.DurationMS = floatPointer(float64(time.Since(started)) / float64(time.Millisecond))
	for i := range sample.Requests {
		client.attachObservation(ctx, &sample.Requests[i], config.Backend)
	}
	sample.RequestCount = len(sample.Requests)
}

// recordTextCorrectness normalizes a trimmed response against the expected text.
// On a match it marks the sample CorrectnessPassed. On a mismatch it marks
// CorrectnessFailed and sets ErrorClass only when a nonempty mismatch error class
// is passed; a plain mismatch leaves ErrorClass unchanged. This consolidates the
// previously duplicated correctness branch across the tool-round-trip and normal
// expected-content success paths.
func recordTextCorrectness(sample *SampleResult, content, expected, mismatchErrorClass string) {
	if strings.TrimSpace(content) == expected {
		sample.Correctness = CorrectnessPassed
		return
	}
	sample.Correctness = CorrectnessFailed
	if mismatchErrorClass != "" {
		sample.ErrorClass = mismatchErrorClass
	}
}

// toolRoundTripOutput validates that exactly one tool call was made and that its
// benchmark_echo payload matches the expected tool message. It returns the echoed
// message and whether the round trip is valid. A non-single tool call or a
// malformed/incorrect payload is treated as one invalid round trip; toolMessage is
// only invoked when exactly one call is present, matching the prior short-circuit.
func toolRoundTripOutput(toolCalls []toolCall, expectedToolMessage string) (string, bool) {
	if len(toolCalls) != 1 {
		return "", false
	}
	return toolMessage(toolCalls[0], expectedToolMessage)
}

// newRunRecord constructs the base RunRecord with metadata, the pre-flight model
// observation, and the standard limitations list.
func newRunRecord(runID string, config RequestedConfiguration, modelBefore *ModelObservation) RunRecord {
	return RunRecord{
		SchemaVersion:   SchemaVersion,
		RunID:           runID,
		WorkloadVersion: WorkloadVersion,
		StartedAt:       time.Now().UTC(),
		Status:          RunCompleted,
		Requested:       config,
		ModelBefore:     modelBefore,
		Workloads:       make([]WorkloadResult, 0, len(syntheticWorkloads)),
		Limitations: []string{
			"active inference state is not exposed and cannot be verified",
			"model lifecycle is observed before and after the run only; lifecycle at each inference request is unknown",
			"applied sampling and draft configuration is not exposed by the diagnostics API",
			"cache state is declared by the operator and is not independently verified",
			"per-round draft acceptance is unsupported; available activity evidence is aggregate",
			"GPU samples are shared-device approximations and are not model-owned measurements",
			"response bodies and reasoning text are never persisted",
		},
	}
}

// determineRunStatus maps run context and workload outcomes to the final run
// status. Cancellation takes precedence; otherwise a partial workload count or
// budget exhaustion yields a partial run.
func determineRunStatus(runCtx context.Context, partial bool, workloadCount int) RunStatus {
	if runCtx.Err() != nil {
		return RunCanceled
	}
	if partial || workloadCount != len(syntheticWorkloads) {
		return RunPartial
	}
	return RunCompleted
}

// observePostflightModel observes the model after the run completes. On failure
// it records the unavailability as a limitation rather than failing the run.
func observePostflightModel(runCtx context.Context, client *APIClient, backend, model string, record *RunRecord) {
	modelAfter, observationErr := client.ObserveReadyModel(runCtx, backend, model)
	if observationErr == nil {
		record.ModelAfter = modelAfter
	} else {
		record.Limitations = append(record.Limitations, "post_run_model_observation_unavailable")
	}
}

// runWorkloads executes every synthetic workload in order, repeating each per
// the configured repetitions. It preserves workload/iteration order and stops at
// the first budget exhaustion (the current workload is still appended) or on
// context cancellation. It returns the workload results and whether any sample
// failed or the budget exhausted.
func runWorkloads(runCtx context.Context, client *APIClient, config RequestedConfiguration, record *RunRecord) ([]WorkloadResult, bool) {
	partial := false
	workloads := make([]WorkloadResult, 0, len(syntheticWorkloads))
	for _, workload := range syntheticWorkloads {
		if runCtx.Err() != nil {
			break
		}
		workloadResult := WorkloadResult{
			Name:            workload.Name,
			Category:        workload.Category,
			Version:         workload.Version,
			ReasoningEffort: workload.ReasoningEffort,
			Samples:         make([]SampleResult, 0, config.Repetitions),
		}
		budgetExhausted := false
		for iteration := 1; iteration <= config.Repetitions; iteration++ {
			if runCtx.Err() != nil {
				break
			}
			sample, exhausted := runSample(runCtx, client, workload, config, iteration, record)
			workloadResult.Samples = append(workloadResult.Samples, sample)
			if sample.Status != SampleCompleted {
				partial = true
			}
			if exhausted {
				partial = true
				budgetExhausted = true
				break
			}
		}
		workloads = append(workloads, workloadResult)
		if budgetExhausted {
			break
		}
	}
	return workloads, partial
}

// sampleRequestRunner holds the request-scoped state shared by sendRequest and
// runSample: the client, workload, configuration, run record, and sample result.
type sampleRequestRunner struct {
	client   *APIClient
	workload syntheticWorkload
	config   RequestedConfiguration
	record   *RunRecord
	sample   *SampleResult
}

func sendRequest(ctx context.Context, runner *sampleRequestRunner, messages []chatMessage, allowTools bool) (chatResult, bool, bool) {
	remaining := runner.config.TokenBudget - runner.record.OutputTokensReserved
	if remaining < 1 {
		runner.sample.ErrorClass = "request_budget_exhausted"
		return chatResult{}, true, true
	}
	maxTokens := runner.config.MaxOutput
	if maxTokens > remaining {
		maxTokens = remaining
	}
	runner.record.OutputTokensReserved += maxTokens
	result, errorClass := runner.client.Complete(ctx, runner.config.Model, messages, runner.workload, runner.config, maxTokens, allowTools)
	runner.sample.Requests = append(runner.sample.Requests, result.Measure)
	runner.sample.InputBytes += result.Measure.PromptBytes
	runner.sample.OutputBytes += result.Measure.OutputBytes
	if errorClass != "" {
		runner.sample.ErrorClass = errorClass
		return result, true, false
	}
	return result, false, false
}

func setSampleFailure(sample *SampleResult, ctx context.Context) {
	if ctx.Err() != nil {
		sample.Status = SampleCanceled
		sample.ErrorClass = "request_canceled"
		return
	}
	sample.Status = SampleFailed
	if sample.ErrorClass == "" {
		sample.ErrorClass = "request_failed"
	}
}

func newRunID() (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	return "bench-" + hex.EncodeToString(random[:]), nil
}

func floatPointer(value float64) *float64 {
	return &value
}

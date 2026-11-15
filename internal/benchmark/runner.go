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
	record := RunRecord{
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

	partial := false
	stop := false
	for _, workload := range syntheticWorkloads {
		if stop || runCtx.Err() != nil {
			break
		}
		workloadResult := WorkloadResult{
			Name:            workload.Name,
			Category:        workload.Category,
			Version:         workload.Version,
			ReasoningEffort: workload.ReasoningEffort,
			Samples:         make([]SampleResult, 0, config.Repetitions),
		}
		for iteration := 1; iteration <= config.Repetitions; iteration++ {
			if runCtx.Err() != nil {
				break
			}
			sample, budgetExhausted := runSample(runCtx, options.Client, workload, config, iteration, &record)
			workloadResult.Samples = append(workloadResult.Samples, sample)
			if sample.Status != SampleCompleted {
				partial = true
			}
			if budgetExhausted {
				partial = true
				stop = true
				break
			}
		}
		record.Workloads = append(record.Workloads, workloadResult)
	}

	if runCtx.Err() != nil {
		record.Status = RunCanceled
	} else if partial || len(record.Workloads) != len(syntheticWorkloads) {
		record.Status = RunPartial
	}
	if runCtx.Err() == nil {
		modelAfter, observationErr := options.Client.ObserveReadyModel(runCtx, options.Backend, options.Model)
		if observationErr == nil {
			record.ModelAfter = modelAfter
		} else {
			record.Limitations = append(record.Limitations, "post_run_model_observation_unavailable")
		}
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

	first, failed, exhausted := sendRequest(ctx, client, workload, config, messages, workload.ToolRoundTrip, record, &sample)
	if failed || exhausted {
		setSampleFailure(&sample, ctx)
	} else if workload.ToolRoundTrip {
		if len(first.ToolCalls) != 1 {
			sample.Correctness = CorrectnessFailed
			sample.ErrorClass = "invalid_tool_call"
		} else if toolOutput, ok := toolMessage(first.ToolCalls[0], "TOOL_ROUND_TRIP_OK"); !ok {
			sample.Correctness = CorrectnessFailed
			sample.ErrorClass = "invalid_tool_call"
		} else {
			messages = append(messages, chatMessage{Role: "assistant", ToolCalls: first.ToolCalls})
			messages = append(messages, chatMessage{Role: "tool", ToolCallID: first.ToolCalls[0].ID, Content: toolOutput})
			second, secondFailed, secondExhausted := sendRequest(ctx, client, workload, config, messages, false, record, &sample)
			if secondFailed || secondExhausted {
				exhausted = secondExhausted
				setSampleFailure(&sample, ctx)
			} else if strings.TrimSpace(second.Content) == workload.Expected {
				sample.Correctness = CorrectnessPassed
			} else {
				sample.Correctness = CorrectnessFailed
				sample.ErrorClass = "unexpected_tool_output"
			}
		}
	} else if strings.TrimSpace(first.Content) == workload.Expected {
		sample.Correctness = CorrectnessPassed
	} else {
		sample.Correctness = CorrectnessFailed
	}
	sample.DurationMS = floatPointer(float64(time.Since(started)) / float64(time.Millisecond))
	for i := range sample.Requests {
		client.attachObservation(ctx, &sample.Requests[i], config.Backend)
	}
	sample.RequestCount = len(sample.Requests)
	return sample, exhausted
}

func sendRequest(ctx context.Context, client *APIClient, workload syntheticWorkload, config RequestedConfiguration, messages []chatMessage, allowTools bool, record *RunRecord, sample *SampleResult) (chatResult, bool, bool) {
	remaining := config.TokenBudget - record.OutputTokensReserved
	if remaining < 1 {
		sample.ErrorClass = "request_budget_exhausted"
		return chatResult{}, true, true
	}
	maxTokens := config.MaxOutput
	if maxTokens > remaining {
		maxTokens = remaining
	}
	record.OutputTokensReserved += maxTokens
	result, errorClass := client.Complete(ctx, config.Model, messages, workload, config, maxTokens, allowTools)
	sample.Requests = append(sample.Requests, result.Measure)
	sample.InputBytes += result.Measure.PromptBytes
	sample.OutputBytes += result.Measure.OutputBytes
	if errorClass != "" {
		sample.ErrorClass = errorClass
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

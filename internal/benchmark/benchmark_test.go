package benchmark

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/correlation"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modeldiagnostics"
)

func TestRunRequiresExplicitInterferenceOptIn(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		calls.Add(1)
	}))
	defer server.Close()
	client, err := NewAPIClient(server.URL, "test-key")
	if err != nil {
		t.Fatal(err)
	}
	_, err = Run(context.Background(), RunOptions{Client: client})
	if err == nil || !strings.Contains(err.Error(), "allow-active-inference-interference") {
		t.Fatalf("Run error = %v, want explicit opt-in error", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("made %d requests without opt-in", calls.Load())
	}
}

func TestNewAPIClientRejectsPublicOrigins(t *testing.T) {
	for _, endpoint := range []string{
		"https://example.com",
		"http://user:pass@192.168.1.5",
		"http://192.168.1.5/path",
	} {
		if _, err := NewAPIClient(endpoint, "test-key"); err == nil {
			t.Errorf("NewAPIClient(%q) succeeded; want rejection", endpoint)
		}
	}
}

func TestObserveReadyModelRejectsStaleLifecycleEvidence(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		snapshot := modeldiagnostics.ModelSnapshot{
			Identity: modeldiagnostics.ModelIdentity{
				PublicID: "test-model", BackendID: "default", LocalID: "test-model",
			},
			Lifecycle: &modeldiagnostics.ModelLifecycleSnapshot{
				CurrentState: "ready", Observed: true,
				ObservedAt: timePointer(time.Now().Add(-6 * time.Minute)),
			},
		}
		_ = json.NewEncoder(w).Encode(modelsResponse{Records: []*modeldiagnostics.ModelSnapshot{&snapshot}})
	}))
	defer server.Close()
	client, err := NewAPIClient(server.URL, "test-key")
	if err != nil {
		t.Fatal(err)
	}
	client.diagnosticInterval = 0
	if _, err := client.ObserveReadyModel(context.Background(), "default", "test-model"); err == nil ||
		!strings.Contains(err.Error(), "stale") {
		t.Fatalf("ObserveReadyModel error = %v, want stale-evidence rejection", err)
	}
}

func TestObserveReadyModelAcceptsBackendAliasResolution(t *testing.T) {
	const backend, alias, canonical = "bigboi", "bigboi/nemotron-bolt", "bigboi/Nemotron-3.5-Bolt-Turbo-Brilliance"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/diagnostics/v1/models" || r.URL.Query().Get("backend") != backend || r.URL.Query().Get("model") != alias {
			t.Errorf("unexpected alias preflight request: %s %s", r.Method, r.URL.String())
		}
		snapshot := modeldiagnostics.ModelSnapshot{
			Identity: modeldiagnostics.ModelIdentity{PublicID: canonical, BackendID: backend, LocalID: "Nemotron-3.5-Bolt-Turbo-Brilliance", Prefix: "bigboi/"},
			Lifecycle: &modeldiagnostics.ModelLifecycleSnapshot{
				CurrentState: "ready", Observed: true, ObservedAt: timePointer(time.Now()),
			},
		}
		_ = json.NewEncoder(w).Encode(modelsResponse{Records: []*modeldiagnostics.ModelSnapshot{&snapshot}, ResolvedModelID: canonical})
	}))
	defer server.Close()
	client, err := NewAPIClient(server.URL, "test-key")
	if err != nil {
		t.Fatal(err)
	}
	client.diagnosticInterval = 0
	observation, err := client.ObserveReadyModel(context.Background(), backend, alias)
	if err != nil {
		t.Fatalf("alias preflight failed: %v", err)
	}
	if observation.Identity.PublicID != canonical {
		t.Fatalf("resolved identity = %q", observation.Identity.PublicID)
	}
}

func TestRunUsesBoundedSyntheticRequestsAndPersistsNoResponseText(t *testing.T) {
	record := runBoundedSyntheticBenchmark(t)
	assertRecordStatusAndWorkloads(t, record)
	assertOutputTokensReserved(t, record)
	assertAllSamplesCorrectAndDiagnosticsAvailable(t, record)
	assertReportOmitsPrivateText(t, record)
	assertRunPersistence(t, record)
}

func runBoundedSyntheticBenchmark(t *testing.T) RunRecord {
	server := newFakeProxy(t, false)
	defer server.Close()
	client, err := NewAPIClient(server.URL, "test-key")
	if err != nil {
		t.Fatal(err)
	}
	client.diagnosticInterval = 0
	if _, err := client.ObserveReadyModel(context.Background(), "default", "test-model"); err != nil {
		t.Fatalf("model preflight failed: %v", err)
	}
	record, err := Run(context.Background(), testRunOptions(client))
	if err != nil {
		t.Fatal(err)
	}
	return record
}
func assertRecordStatusAndWorkloads(t *testing.T, record RunRecord) {
	if record.Status != RunCompleted || len(record.Workloads) != len(syntheticWorkloads) {
		t.Fatalf("status/workload count = %s/%d", record.Status, len(record.Workloads))
	}
}

func assertOutputTokensReserved(t *testing.T, record RunRecord) {
	if record.OutputTokensReserved != len(syntheticWorkloads)*64+64 {
		t.Fatalf("reserved output tokens = %d", record.OutputTokensReserved)
	}
}

func assertAllSamplesCorrectAndDiagnosticsAvailable(t *testing.T, record RunRecord) {
	for _, workload := range record.Workloads {
		for _, sample := range workload.Samples {
			if sample.Correctness != CorrectnessPassed {
				t.Errorf("workload %s correctness = %s (%s)", workload.Name, sample.Correctness, sample.ErrorClass)
			}
			for _, request := range sample.Requests {
				if request.DiagnosticState != "available" || request.ProxyObservation == nil {
					t.Errorf("request diagnostic state = %q", request.DiagnosticState)
				}
			}
		}
	}
}

func assertReportOmitsPrivateText(t *testing.T, record RunRecord) {
	report, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	for _, privateText := range []string{
		"TOOL_ROUND_TRIP_OK", "REASONING_CONTROL_OK", "SYSTEM_DEFAULTS_OK", "</thin>",
		"PRIVATE_RESPONSE_SENTINEL",
	} {
		if strings.Contains(string(report), privateText) {
			t.Fatalf("report persisted response or fixture text %q", privateText)
		}
	}
}

func assertRunPersistence(t *testing.T, record RunRecord) {
	directory := filepath.Join(t.TempDir(), "benchmark-runs")
	path, err := WriteRun(directory, record)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("report permissions = %o, want 600", info.Mode().Perm())
	}
	dirInfo, err := os.Stat(directory)
	if err != nil {
		t.Fatal(err)
	}
	if dirInfo.Mode().Perm() != 0700 {
		t.Fatalf("directory permissions = %o, want 700", dirInfo.Mode().Perm())
	}
	if _, err := WriteRun(directory, record); err == nil {
		t.Fatal("WriteRun overwrote an existing report")
	}
	loaded, err := ReadRun(directory, record.RunID)
	if err != nil || loaded.RunID != record.RunID {
		t.Fatalf("ReadRun = %q, %v", loaded.RunID, err)
	}
	runs, truncated, err := ListRuns(directory)
	if err != nil || truncated || len(runs) != 1 || runs[0].RunID != record.RunID {
		t.Fatalf("ListRuns = %+v, truncated %v, error %v", runs, truncated, err)
	}
	link := filepath.Join(directory, "bench-00000000000000000000000000000000.json")
	if err := os.Symlink(path, link); err == nil {
		if _, _, err := ListRuns(directory); err == nil {
			t.Fatal("listing accepted a symlink report")
		}
	}
}

// TestCompareSeparatesCorrectnessAndPerformanceAndMarksUnsupportedEvidence verifies that
// correctness gating is independent of performance metrics, that incomplete/truncated
// outcomes are counted per run, that declared cache-state divergence is flagged as a
// confounder, that metrics are deterministically ordered, that the latency comparison
// reflects the baseline/candidate direction, that context evidence retains unsupported
// state, and that the interpretation guards against overclaiming performance.
// Correctness, metrics, context, and interpretation assertions were extracted into named
// helpers to keep cognitive complexity below the S3776 threshold while preserving every
// assertion, comparison input, metric order, sample count, missing/unsupported state, and
// overclaim guard.
func TestCompareSeparatesCorrectnessAndPerformanceAndMarksUnsupportedEvidence(t *testing.T) {
	baseline := comparisonRun("bench-00000000000000000000000000000001", 100, CorrectnessPassed, 0.5, 8000)
	candidate := comparisonRun("bench-00000000000000000000000000000002", 80, CorrectnessFailed, 0.75, 9000)
	baseline.Requested.CacheState = CacheCold
	candidate.Requested.CacheState = CacheWarm
	baseline.Workloads[0].Samples[0].Requests[0].ProxyObservation.TransportOutcome = "timeout"
	baseline.Workloads[0].Samples[0].Requests[0].ProxyObservation.GenerationOutcome = "incomplete"
	candidate.Workloads[0].Samples[0].Requests[0].ProxyObservation.GenerationOutcome = "truncated"
	report, err := Compare(baseline, candidate)
	if err != nil {
		t.Fatal(err)
	}
	first := report.Workloads[0]
	assertCorrectnessSeparated(t, first)
	assertFailureOutcomesCounted(t, first)
	assertCacheConfounderPresent(t, report)
	assertMetricsDeterministicallyOrdered(t, first)
	assertLatencyComparison(t, first)
	assertContextEvidence(t, report)
	assertInterpretationGuardsOverclaim(t, report)
}

// assertCorrectnessSeparated verifies correctness is gated independently of performance:
// the baseline passes and the candidate fails.
func assertCorrectnessSeparated(t *testing.T, first WorkloadComparison) {
	if first.Correctness.Baseline.Passed != 1 || first.Correctness.Candidate.Failed != 1 {
		t.Fatalf("correctness was not separated: %#v", first.Correctness)
	}
}

// assertFailureOutcomesCounted verifies incomplete transport, incomplete generation, and
// truncated generation failures are counted on the correct side of the comparison.
func assertFailureOutcomesCounted(t *testing.T, first WorkloadComparison) {
	if first.Failures.Baseline.IncompleteTransport != 1 ||
		first.Failures.Baseline.IncompleteGeneration != 1 ||
		first.Failures.Candidate.TruncatedGeneration != 1 {
		t.Fatalf("incomplete/truncated outcomes were not counted: %#v", first.Failures)
	}
}

// assertCacheConfounderPresent verifies that declared cache-state divergence is reported
// as a confounder.
func assertCacheConfounderPresent(t *testing.T, report ComparisonReport) {
	if !containsString(report.Confounders, "declared cache states differ") {
		t.Fatalf("cache confounder missing: %#v", report.Confounders)
	}
}

// assertMetricsDeterministicallyOrdered verifies metrics are present and ordered with
// completion_tokens first.
func assertMetricsDeterministicallyOrdered(t *testing.T, first WorkloadComparison) {
	if len(first.Metrics) == 0 || first.Metrics[0].Name != "completion_tokens" {
		t.Fatalf("metrics were not deterministically ordered: %#v", first.Metrics)
	}
}

// assertLatencyComparison locates the upstream_header_ms metric and verifies the
// baseline-to-candidate direction (candidate 80ms vs baseline 100ms => -20%).
func assertLatencyComparison(t *testing.T, first WorkloadComparison) {
	var latency *MetricComparison
	for i := range first.Metrics {
		if first.Metrics[i].Name == "upstream_header_ms" {
			latency = &first.Metrics[i]
		}
	}
	if latency == nil || latency.ChangePercent == nil || math.Abs(*latency.ChangePercent+20) > 1e-9 {
		t.Fatalf("latency comparison = %#v, want -20%%", latency)
	}
}

// assertContextEvidence verifies context retains per-round acceptance state and
// unsupported aggregate/GPU state rather than fabricating per-run attribution.
func assertContextEvidence(t *testing.T, report ComparisonReport) {
	if report.Context.PerRoundAcceptanceState == "" ||
		report.Context.BaselineAfter.AggregateAcceptanceRate == nil ||
		report.Context.CandidateAfter.GPUMemoryMaxMB == nil {
		t.Fatalf("context evidence missing or unsupported state omitted: %#v", report.Context)
	}
}

// assertInterpretationGuardsOverclaim verifies the interpretation does not treat a
// higher acceptance percentage as evidence of faster performance.
func assertInterpretationGuardsOverclaim(t *testing.T, report ComparisonReport) {
	if !strings.Contains(report.Interpretation, "not evidence of faster performance") {
		t.Fatalf("interpretation does not guard against overclaiming: %q", report.Interpretation)
	}
}

func TestCompareIncompleteRunsReportsMissingWorkloadsAndObservations(t *testing.T) {
	baseline := comparisonRun("bench-00000000000000000000000000000001", 100, CorrectnessPassed, 0.5, 8000)
	candidate := comparisonRun("bench-00000000000000000000000000000002", 80, CorrectnessPassed, 0.5, 8000)
	baseline.Status = RunPartial
	baseline.Workloads = baseline.Workloads[:2]
	baseline.OutputTokensReserved = 2
	baseline.Workloads[0].Samples[0].Requests[0].ProxyObservation = nil
	baseline.Workloads[0].Samples[0].Requests[0].DiagnosticState = "observation_unavailable"
	candidate.Status = RunCanceled
	candidate.Workloads = nil
	candidate.OutputTokensReserved = 0
	for _, pair := range [][2]RunRecord{{baseline, candidate}, {candidate, baseline}} {
		assertIncompleteComparison(t, pair[0], pair[1])
	}
}

func assertIncompleteComparison(t *testing.T, baseline, candidate RunRecord) {
	report, err := Compare(baseline, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Workloads) != len(syntheticWorkloads) {
		t.Fatalf("comparison has %d workloads", len(report.Workloads))
	}
	if !containsString(report.Confounders, "baseline run is "+string(baseline.Status)) ||
		!containsString(report.Confounders, "candidate run is "+string(candidate.Status)) {
		t.Fatalf("missing incomplete run confounders: %v", report.Confounders)
	}
	first := report.Workloads[0]
	recorded := WorkloadCoverage{Recorded: true, SamplesRecorded: 1, SamplesExpected: 1}
	missing := WorkloadCoverage{SamplesExpected: 1}
	wantCoverage := PairedWorkloadCoverage{Baseline: recorded, Candidate: missing}
	if baseline.Status == RunCanceled {
		wantCoverage = PairedWorkloadCoverage{Baseline: missing, Candidate: recorded}
	}
	if first.Coverage != wantCoverage {
		t.Fatalf("incorrect comparison coverage: %+v", first.Coverage)
	}
	for _, metric := range first.Metrics {
		if metric.ChangePercent != nil {
			t.Fatalf("computed a change without samples on both sides: %+v", metric)
		}
	}
	failures := first.Failures.Baseline
	if baseline.Status == RunCanceled {
		failures = first.Failures.Candidate
	}
	if failures.UnknownTransport != 1 || failures.UnknownGeneration != 1 {
		t.Fatalf("missing observations were not reported as unknown: %+v", failures)
	}
	last := report.Workloads[len(report.Workloads)-1]
	if last.Coverage.Baseline.Recorded || last.Coverage.Candidate.Recorded || len(last.Metrics) != 0 {
		t.Fatalf("missing workload unexpectedly reported measurements: %+v", last)
	}
	if _, err := WriteComparison(t.TempDir(), report); err != nil {
		t.Fatalf("persist incomplete comparison: %v", err)
	}
}

func TestRunCancellationProducesValidPartialReport(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(fakeProxyHandler(t, true, started, release))
	defer server.Close()
	defer close(release)
	client, err := NewAPIClient(server.URL, "test-key")
	if err != nil {
		t.Fatal(err)
	}
	client.diagnosticInterval = 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type result struct {
		record RunRecord
		err    error
	}
	done := make(chan result, 1)
	go func() {
		record, err := Run(ctx, testRunOptions(client))
		done <- result{record: record, err: err}
	}()
	select {
	case <-started:
		cancel()
	case early := <-done:
		t.Fatalf("runner ended before inference request: %v", early.err)
	case <-time.After(5 * time.Second):
		t.Fatal("runner did not send the first inference request")
	}
	var got result
	select {
	case got = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runner did not stop after cancellation")
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.record.Status != RunCanceled {
		t.Fatalf("run status = %s, want canceled", got.record.Status)
	}
	if err := got.record.Validate(); err != nil {
		t.Fatalf("canceled report is invalid: %v", err)
	}
}

func TestJobManagerRequiresConsentAndSerializesCancellation(t *testing.T) {
	server, manager, directory, client, started, release := newConsentHarness(t)
	defer server.Close()
	defer close(release)
	defer manager.Stop()

	options := testRunOptions(client)
	options.AllowInterference = false
	assertStartRejectedWithoutConsent(t, manager, options, started)
	options.AllowInterference = true
	job := assertStartSucceedsWithConsent(t, manager, options, started)
	assertConcurrentStartRejected(t, manager, options)
	if err := manager.Cancel(job.ID); err != nil {
		t.Fatal(err)
	}
	status := waitForJobState(t, manager, job.ID, "canceled", time.Now().Add(5*time.Second))
	assertCanceledJob(t, status, directory)
	assertReCancelRejected(t, manager, job.ID)
	manager.Stop()
	assertStartRejectedAfterStop(t, manager, options)
}

// newConsentHarness builds the server, client, temp directory, and JobManager for the
// consent/cancellation test. The fake proxy blocks chat until the started channel is
// released, so the test can observe whether inference was launched.
func newConsentHarness(t *testing.T) (*httptest.Server, *JobManager, string, *APIClient, chan struct{}, chan struct{}) {
	started, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(fakeProxyHandler(t, true, started, release))
	client, err := NewAPIClient(server.URL, "test-key")
	if err != nil {
		t.Fatal(err)
	}
	client.diagnosticInterval = 0
	directory := t.TempDir()
	manager, err := NewJobManager(context.Background(), directory, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return server, manager, directory, client, started, release
}

// assertStartRejectedWithoutConsent verifies that Start is rejected when
// AllowInterference is false and that no inference request is issued to the proxy.
func assertStartRejectedWithoutConsent(t *testing.T, manager *JobManager, options RunOptions, started chan struct{}) {
	if _, err := manager.Start(options); err == nil {
		t.Fatal("started a job without consent")
	}
	select {
	case <-started:
		t.Fatal("inference started without consent")
	default:
	}
}

// assertStartSucceedsWithConsent verifies that Start succeeds when consent is given
// and that the fake proxy receives the inference request before returning the job.
func assertStartSucceedsWithConsent(t *testing.T, manager *JobManager, options RunOptions, started chan struct{}) JobStatus {
	job, err := manager.Start(options)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("fake inference did not start")
	}
	return job
}

// assertConcurrentStartRejected verifies that a second Start while a job is active
// is rejected with ErrJobActive.
func assertConcurrentStartRejected(t *testing.T, manager *JobManager, options RunOptions) {
	if _, err := manager.Start(options); !errors.Is(err, ErrJobActive) {
		t.Fatalf("concurrent job error = %v", err)
	}
}

// assertCanceledJob verifies the job transitions to canceled and the persisted
// report reflects cancellation.
func assertCanceledJob(t *testing.T, status JobStatus, directory string) {
	if status.State != "canceled" || status.RunID == "" || status.ErrorCode != "" {
		t.Fatalf("canceled job = %+v", status)
	}
	record, err := ReadRun(directory, status.RunID)
	if err != nil || record.Status != RunCanceled {
		t.Fatalf("canceled report = %s, %v", record.Status, err)
	}
}

// assertReCancelRejected verifies that canceling a finished job is rejected.
func assertReCancelRejected(t *testing.T, manager *JobManager, jobID string) {
	if err := manager.Cancel(jobID); err == nil {
		t.Fatal("finished job accepted cancellation")
	}
}

// assertStartRejectedAfterStop verifies that starting a stopped manager is rejected.
func assertStartRejectedAfterStop(t *testing.T, manager *JobManager, options RunOptions) {
	if _, err := manager.Start(options); !errors.Is(err, ErrJobStopped) {
		t.Fatalf("stopped job manager error = %v", err)
	}
}

func TestJobManagerReportsUncertainReportWrites(t *testing.T) {
	server := newFakeProxy(t, false)
	defer server.Close()
	client, err := NewAPIClient(server.URL, "test-key")
	if err != nil {
		t.Fatal(err)
	}
	client.diagnosticInterval = 0
	path := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(path, []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	manager, err := NewJobManager(context.Background(), path, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Stop()
	job, err := manager.Start(testRunOptions(client))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	status := waitForJobState(t, manager, job.ID, "failed", deadline)
	if status.State != "failed" || status.ErrorCode != "report_write_outcome_unknown" || status.RunID == "" {
		t.Fatalf("failed report write = %+v", status)
	}
}

func waitForJobState(t *testing.T, manager *JobManager, jobID string, wantState string, deadline time.Time) JobStatus {
	t.Helper()
	var status JobStatus
	var err error
	for time.Now().Before(deadline) {
		status, err = manager.Status(jobID)
		if err != nil {
			t.Fatal(err)
		}
		if status.State == wantState {
			return status
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for job %s to reach state %q", jobID, wantState)
	return JobStatus{}
}

func TestJobManagerPersistsCompletedRun(t *testing.T) {
	server := newFakeProxy(t, false)
	defer server.Close()
	client, err := NewAPIClient(server.URL, "test-key")
	if err != nil {
		t.Fatal(err)
	}
	client.diagnosticInterval = 0
	directory := t.TempDir()
	manager, err := NewJobManager(context.Background(), directory, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Stop()
	job, err := manager.Start(testRunOptions(client))
	if err != nil {
		t.Fatal(err)
	}
	var status JobStatus
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status, err = manager.Status(job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if status.State != "running" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if status.State != "completed" || status.RunID == "" || status.ErrorCode != "" {
		t.Fatalf("completed job = %+v", status)
	}
	record, err := ReadRun(directory, status.RunID)
	if err != nil || record.Status != RunCompleted {
		t.Fatalf("completed report = %s, %v", record.Status, err)
	}
	if _, err := manager.Status("job-missing"); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("unknown job error = %v", err)
	}
}

func TestSummarizeUsesSampleStandardDeviation(t *testing.T) {
	stats := summarize([]float64{2, 4})
	if stats.SampleCount != 2 || stats.Mean == nil || *stats.Mean != 3 ||
		stats.StdDev == nil || math.Abs(*stats.StdDev-math.Sqrt2) > 1e-9 {
		t.Fatalf("statistics = %#v", stats)
	}
}

func testRunOptions(client *APIClient) RunOptions {
	return RunOptions{
		Client:            client,
		Backend:           "default",
		Model:             "test-model",
		Temperature:       0,
		TopP:              1,
		MaxOutput:         64,
		Repetitions:       1,
		CacheState:        CacheUnknown,
		TimeBudget:        20 * time.Second,
		TokenBudget:       4096,
		AllowInterference: true,
	}
}

func newFakeProxy(t *testing.T, blockChat bool) *httptest.Server {
	t.Helper()
	handler := fakeProxyHandler(t, blockChat)
	return httptest.NewServer(handler)
}

func fakeProxyHandler(t *testing.T, blockChat bool, channels ...chan struct{}) http.Handler {
	t.Helper()
	return &fakeProxyState{
		t: t, blockChat: blockChat, channels: channels,
		observations: make(map[string]*correlation.ProxyObservation),
	}
}

type fakeProxyState struct {
	t            *testing.T
	blockChat    bool
	channels     []chan struct{}
	requestID    atomic.Uint64
	mu           sync.Mutex
	observations map[string]*correlation.ProxyObservation
	active       atomic.Int32
	maxActive    atomic.Int32
}

func (s *fakeProxyState) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeDiagnostics(w, r) {
		return
	}
	switch {
	case r.URL.Path == "/diagnostics/v1/models":
		s.serveModels(w)
	case r.URL.Path == "/v1/chat/completions":
		s.serveChat(w, r)
	case strings.HasPrefix(r.URL.Path, "/diagnostics/v1/requests/"):
		s.serveRequestObservation(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *fakeProxyState) authorizeDiagnostics(w http.ResponseWriter, r *http.Request) bool {
	if strings.HasPrefix(r.URL.Path, "/diagnostics/") && r.Header.Get("Authorization") != "Bearer test-key" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	return true
}

func (s *fakeProxyState) serveModels(w http.ResponseWriter) {
	snapshot := modeldiagnostics.ModelSnapshot{
		Identity: modeldiagnostics.ModelIdentity{
			PublicID: "test-model", BackendID: "default", LocalID: "test-model",
		},
		Lifecycle: &modeldiagnostics.ModelLifecycleSnapshot{
			CurrentState: "ready", Observed: true, StateAtActivity: "unknown",
			TransitionEvidence: "unsupported",
			ObservedAt:         timePointer(time.Now().UTC()),
		},
		Activity: &modeldiagnostics.ActivityMeasurement{
			DraftRate: &modeldiagnostics.DraftRateSummary{
				Rate: floatPointer(0.5), ProposedTokens: int64Pointer(10),
				AcceptedTokens: int64Pointer(5), State: modeldiagnostics.MeasurementSupported,
			},
		},
		GPUWindow: &modeldiagnostics.SharedGPUWindow{
			SampleCount: 2, MinMemUsedMB: intPointer(7000), MaxMemUsedMB: intPointer(8000),
			SharedDeviceOnly: true, ExecutionDeviceUnverified: true,
		},
	}
	_ = json.NewEncoder(w).Encode(modelsResponse{
		Records:     []*modeldiagnostics.ModelSnapshot{&snapshot},
		GeneratedAt: time.Now().UTC(),
		Coverage:    "complete",
	})
}

func (s *fakeProxyState) serveChat(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "" {
		s.t.Error("diagnostics key was sent to inference endpoint")
	}
	current := s.active.Add(1)
	defer s.active.Add(-1)
	s.recordMaxActive(current)
	if s.blockChat {
		s.waitForChatRelease(r)
		return
	}
	var body struct {
		Messages []chatMessage `json:"messages"`
		Tools    []any         `json:"tools"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		s.t.Error(err)
		http.Error(w, "invalid", http.StatusBadRequest)
		return
	}
	id := fmt.Sprintf("%032x", s.requestID.Add(1))
	w.Header().Set("X-Request-ID", id)
	s.storeObservation(id)
	s.writeChatResult(w, body)
}

func (s *fakeProxyState) recordMaxActive(current int32) {
	for current > s.maxActive.Load() && !s.maxActive.CompareAndSwap(s.maxActive.Load(), current) {
	}
}

func (s *fakeProxyState) waitForChatRelease(r *http.Request) {
	if len(s.channels) > 0 {
		select {
		case <-s.channels[0]:
		default:
			close(s.channels[0])
		}
	}
	if len(s.channels) > 1 {
		select {
		case <-r.Context().Done():
		case <-s.channels[1]:
		}
		return
	}
	<-r.Context().Done()
}

func (s *fakeProxyState) storeObservation(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.observations[id] = &correlation.ProxyObservation{
		UpstreamHeaderMs:  floatPointer(100),
		FirstByteMs:       floatPointer(120),
		FirstOutputMs:     floatPointer(130),
		FirstAnswerMs:     floatPointer(140),
		TotalDurationMs:   floatPointer(200),
		PromptTokens:      int64Pointer(12),
		CompletionTokens:  int64Pointer(3),
		PromptN:           int64Pointer(12),
		PromptMS:          floatPointer(20),
		PredictedN:        int64Pointer(3),
		PredictedMS:       floatPointer(30),
		TransportOutcome:  "returned",
		GenerationOutcome: "completed",
		InspectionState:   "complete",
	}
}

func (s *fakeProxyState) writeChatResult(w http.ResponseWriter, body struct {
	Messages []chatMessage `json:"messages"`
	Tools    []any         `json:"tools"`
}) {
	if len(body.Tools) > 0 {
		_, _ = fmt.Fprint(w, `{"choices":[{"message":{"content":"","tool_calls":[{"id":"call-1","type":"function","function":{"name":"benchmark_echo","arguments":"{\"message\":\"TOOL_ROUND_TRIP_OK\"}"}}]}}]}`)
		return
	}
	message := benchmarkFakeReply(body.Messages[len(body.Messages)-1])
	response, _ := json.Marshal(map[string]any{
		"choices": []any{map[string]any{"message": map[string]string{"content": message}}},
	})
	_, _ = w.Write(response)
}

func benchmarkFakeReply(last chatMessage) string {
	switch {
	case last.Role == "tool":
		return "TOOL_ROUND_TRIP_OK"
	case strings.Contains(last.Content, "REASONING_CONTROL_OK"):
		return "REASONING_CONTROL_OK"
	case strings.Contains(last.Content, "</think>"):
		return "</think>"
	case strings.Contains(last.Content, "SYSTEM_DEFAULTS_OK"):
		return "SYSTEM_DEFAULTS_OK"
	case strings.Contains(last.Content, "PRIVATE_RESPONSE_SENTINEL"):
		return "PRIVATE_RESPONSE_SENTINEL"
	default:
		return "2"
	}
}

func (s *fakeProxyState) serveRequestObservation(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/diagnostics/v1/requests/")
	s.mu.Lock()
	observation := s.observations[id]
	s.mu.Unlock()
	if observation == nil {
		http.NotFound(w, r)
		return
	}
	_ = json.NewEncoder(w).Encode(requestRecord{
		RequestID: id, BackendID: "default", ProxyObservation: observation,
	})
}

func comparisonRun(id string, latency float64, correctness CorrectnessStatus, acceptance float64, memory int) RunRecord {
	now := time.Now().UTC()
	record := RunRecord{
		SchemaVersion:   SchemaVersion,
		RunID:           id,
		WorkloadVersion: WorkloadVersion,
		StartedAt:       now,
		CompletedAt:     now.Add(time.Second),
		Status:          RunCompleted,
		Requested: RequestedConfiguration{
			Backend: "default", Model: "test-model", Temperature: 0, TopP: 1,
			MaxOutput: 64, Reasoning: "per_workload", CacheState: CacheUnknown,
			CacheEvidence: "declared_unverified", Repetitions: 1,
			Concurrency: 1, TimeBudget: "1m0s", TokenBudget: 4096,
		},
		Workloads:   make([]WorkloadResult, 0, len(syntheticWorkloads)),
		Limitations: []string{},
	}
	ready := &ModelObservation{
		State: "ready", Identity: modeldiagnostics.ModelIdentity{
			PublicID: "test-model", BackendID: "default", LocalID: "test-model",
		},
		Lifecycle: &modeldiagnostics.ModelLifecycleSnapshot{
			CurrentState: "ready", Observed: true, ObservedAt: timePointer(now),
		},
		ConfigState: "not_exposed_by_diagnostics_api",
		Activity: &modeldiagnostics.ActivityMeasurement{
			DraftRate: &modeldiagnostics.DraftRateSummary{Rate: floatPointer(acceptance)},
		},
		GPUWindow: &modeldiagnostics.SharedGPUWindow{
			SampleCount: 2, MaxMemUsedMB: intPointer(memory),
		},
	}
	record.ModelBefore = ready
	record.ModelAfter = ready
	for _, workload := range syntheticWorkloads {
		record.Workloads = append(record.Workloads, WorkloadResult{
			Name: workload.Name, Category: workload.Category, Version: workload.Version,
			ReasoningEffort: workload.ReasoningEffort,
			Samples: []SampleResult{{
				Iteration: 1, Status: SampleCompleted, Correctness: correctness,
				DurationMS: floatPointer(latency), RequestCount: 1, InputBytes: 10, OutputBytes: 3,
				Requests: []RequestMeasurement{{
					StartedAt: now, EndedAt: now.Add(time.Millisecond), HTTPStatus: 200,
					RequestBytes: 20, PromptBytes: 10, ResponseBytes: 30, OutputBytes: 3,
					MaxTokens: 1, DiagnosticState: "available",
					ProxyObservation: &correlation.ProxyObservation{
						UpstreamHeaderMs: floatPointer(latency),
						PromptTokens:     int64Pointer(10), CompletionTokens: int64Pointer(3),
						TransportOutcome: "returned", GenerationOutcome: "completed",
					},
				}},
			}},
		})
		record.OutputTokensReserved++
	}
	return record
}

func int64Pointer(value int64) *int64 {
	return &value
}

func intPointer(value int) *int {
	return &value
}

func timePointer(value time.Time) *time.Time {
	return &value
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

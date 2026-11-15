package correlation

import (
	"encoding/json"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/timing"
)

func ptrF64(v float64) *float64 { return &v }
func ptrI64(v int64) *int64     { return &v }

func TestProjectionCannotInventEvidenceWithoutInspection(t *testing.T) {
	for _, state := range []string{"uninspected", "SHORT_SECRET_STATE"} {
		s := validSummary(fixtureTime, 90*time.Millisecond)
		s.InspectionState = state
		p := projectSummary(s)
		if p == nil {
			t.Fatal("wall-only projection should remain valid")
		}
		if p.GenerationOutcome == "completed" || p.FirstOutputMs != nil || p.FirstAnswerMs != nil || p.CompletionTokens != nil || p.PredictedMS != nil || p.FinishReason != "" {
			t.Fatal("uninspected body supplied fabricated generation evidence")
		}
		if p.TotalDurationMs == nil || p.FirstByteMs == nil {
			t.Fatal("proxy wall evidence was unnecessarily lost")
		}
		b, _ := json.Marshal(p)
		if strings.Contains(string(b), "SHORT_SECRET_STATE") {
			t.Fatal("raw state retained")
		}
	}
}

// validSummary returns a finished, valid timing.Summary starting at start with
// the given total duration. Wall phases are ordered and within the total.
func validSummary(start time.Time, total time.Duration) timing.Summary {
	end := start.Add(total)
	ms := float64(total / time.Millisecond)
	return timing.Summary{
		StartTime:         start,
		EndTime:           end,
		TotalDurationMs:   ptrF64(ms),
		UpstreamHeaderMs:  ptrF64(10.0),
		FirstByteMs:       ptrF64(20.0),
		FirstOutputMs:     ptrF64(30.0),
		FirstAnswerMs:     ptrF64(50.0),
		TransportOutcome:  "returned",
		GenerationOutcome: "completed",
		InspectionState:   "full",
		TokenUsage: timing.TokenUsage{
			PromptTokens:     ptrI64(0),
			CompletionTokens: ptrI64(7),
			TotalTokens:      ptrI64(7),
			CachedTokens:     ptrI64(0),
			ReasoningTokens:  ptrI64(2),
			FinishReason:     "stop",
		},
		EngineTimings: timing.EngineTimings{
			PromptMS:    ptrF64(0.0),
			PredictedMS: ptrF64(10.0),
			PromptN:     ptrI64(0),
			PredictedN:  ptrI64(7),
		},
	}
}

func observation(t *testing.T, c *Correlator, requestID string) *ProxyObservation {
	t.Helper()
	r, ok := c.GetByRequestID(requestID)
	if !ok {
		t.Fatal("request not found")
	}
	return r.ProxyObservation
}

func TestCorrelatorAttachObservationHappyPath(t *testing.T) {
	c := newTestCorrelator(t)
	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)

	ok := c.AttachObservation("req-1", validSummary(fixtureTime, 90*time.Millisecond))
	if !ok {
		t.Fatal("expected attach to succeed")
	}

	obs := observation(t, c, "req-1")
	if obs.Source != "proxy" {
		t.Fatalf("source = %q, want proxy", obs.Source)
	}
	if obs.Scope != "proxy_request_only" {
		t.Fatalf("scope = %q, want proxy_request_only", obs.Scope)
	}
	if obs.UpstreamRequestMapping != "not_established" {
		t.Fatalf("mapping = %q, want not_established", obs.UpstreamRequestMapping)
	}
	if obs.UpstreamHeaderMs == nil || *obs.UpstreamHeaderMs != 10.0 {
		t.Fatalf("upstream_header_ms = %v, want 10", obs.UpstreamHeaderMs)
	}
	if obs.FirstByteMs == nil || *obs.FirstByteMs != 20.0 {
		t.Fatalf("first_byte_ms = %v, want 20", obs.FirstByteMs)
	}
	if obs.FirstOutputMs == nil || *obs.FirstOutputMs != 30.0 {
		t.Fatalf("first_output_ms = %v, want 30", obs.FirstOutputMs)
	}
	if obs.FirstAnswerMs == nil || *obs.FirstAnswerMs != 50.0 {
		t.Fatalf("first_answer_ms = %v, want 50", obs.FirstAnswerMs)
	}
	if obs.TotalDurationMs == nil || *obs.TotalDurationMs != 90.0 {
		t.Fatalf("total_duration_ms = %v, want 90", obs.TotalDurationMs)
	}
	if obs.PromptTokens == nil || *obs.PromptTokens != 0 {
		t.Fatalf("prompt_tokens = %v, want 0", obs.PromptTokens)
	}
	if obs.CompletionTokens == nil || *obs.CompletionTokens != 7 {
		t.Fatalf("completion_tokens = %v, want 7", obs.CompletionTokens)
	}
	if obs.PromptMS == nil || *obs.PromptMS != 0.0 {
		t.Fatalf("prompt_ms = %v, want 0", obs.PromptMS)
	}
	if obs.PredictedMS == nil || *obs.PredictedMS != 10.0 {
		t.Fatalf("predicted_ms = %v, want 10", obs.PredictedMS)
	}
	if obs.TransportOutcome != "returned" {
		t.Fatalf("transport_outcome = %q, want returned", obs.TransportOutcome)
	}
	if obs.GenerationOutcome != "completed" {
		t.Fatalf("generation_outcome = %q, want completed", obs.GenerationOutcome)
	}
	if obs.InspectionState != "full" {
		t.Fatalf("inspection_state = %q, want full", obs.InspectionState)
	}
	if obs.FinishReason != "stop" {
		t.Fatalf("finish_reason = %q, want stop", obs.FinishReason)
	}
	// No sourceModel/header/error/path/body fields are present by construction.
	assertSizeConsistent(t, c)
}

func TestCorrelatorAttachObservationUnknownID(t *testing.T) {
	c := newTestCorrelator(t)
	if c.AttachObservation("req-missing", validSummary(fixtureTime, 90*time.Millisecond)) {
		t.Fatal("expected attach to unknown ID to fail")
	}
	if c.AttachObservation("", validSummary(fixtureTime, 90*time.Millisecond)) {
		t.Fatal("expected attach to empty ID to fail")
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorAttachObservationExpiredID(t *testing.T) {
	c := newTestCorrelatorWithRetention(t, 0, time.Hour, 0)
	// Old request: expired by age retention.
	c.RegisterRequest("req-old", "backend-A", "model-1", fixtureTime.Add(-2*time.Hour))
	if c.AttachObservation("req-old", validSummary(fixtureTime, 90*time.Millisecond)) {
		t.Fatal("expected attach to expired ID to fail")
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorAttachObservationRejectsUnfinished(t *testing.T) {
	c := newTestCorrelator(t)
	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
	// Unfinished: zero EndTime.
	if c.AttachObservation("req-1", timing.Summary{StartTime: fixtureTime}) {
		t.Fatal("expected unfinished summary to be rejected")
	}
	// Invalid time horizon: end before start.
	bad := validSummary(fixtureTime, 90*time.Millisecond)
	bad.EndTime = fixtureTime.Add(-time.Minute)
	if c.AttachObservation("req-1", bad) {
		t.Fatal("expected invalid time horizon to be rejected")
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorAttachObservationRejectsInvalidPhases(t *testing.T) {
	c := newTestCorrelator(t)
	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)

	// Negative phase.
	bad := validSummary(fixtureTime, 90*time.Millisecond)
	bad.FirstByteMs = ptrF64(-1.0)
	if c.AttachObservation("req-1", bad) {
		t.Fatal("expected negative phase to be rejected")
	}

	// NaN phase.
	bad = validSummary(fixtureTime, 90*time.Millisecond)
	bad.FirstOutputMs = ptrF64(math.NaN())
	if c.AttachObservation("req-1", bad) {
		t.Fatal("expected NaN phase to be rejected")
	}

	// Inf phase.
	bad = validSummary(fixtureTime, 90*time.Millisecond)
	bad.TotalDurationMs = ptrF64(math.Inf(1))
	if c.AttachObservation("req-1", bad) {
		t.Fatal("expected Inf phase to be rejected")
	}

	// Phase exceeds total.
	bad = validSummary(fixtureTime, 90*time.Millisecond)
	bad.FirstByteMs = ptrF64(100.0)
	if c.AttachObservation("req-1", bad) {
		t.Fatal("expected phase > total to be rejected")
	}

	// Phase order violation: FirstByte after FirstOutput.
	bad = validSummary(fixtureTime, 90*time.Millisecond)
	bad.FirstByteMs = ptrF64(100.0)
	bad.FirstOutputMs = ptrF64(30.0)
	if c.AttachObservation("req-1", bad) {
		t.Fatal("expected phase order violation to be rejected")
	}

	assertSizeConsistent(t, c)
}

func TestCorrelatorAttachObservationRejectsInvalidCountersAndTimings(t *testing.T) {
	c := newTestCorrelator(t)
	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)

	// Negative counter.
	bad := validSummary(fixtureTime, 90*time.Millisecond)
	bad.TokenUsage.PromptTokens = ptrI64(-1)
	if c.AttachObservation("req-1", bad) {
		t.Fatal("expected negative counter to be rejected")
	}

	// NaN engine timing.
	bad = validSummary(fixtureTime, 90*time.Millisecond)
	bad.EngineTimings.PromptMS = ptrF64(math.NaN())
	if c.AttachObservation("req-1", bad) {
		t.Fatal("expected NaN engine timing to be rejected")
	}

	// Negative engine timing.
	bad = validSummary(fixtureTime, 90*time.Millisecond)
	bad.EngineTimings.PredictedMS = ptrF64(-5.0)
	if c.AttachObservation("req-1", bad) {
		t.Fatal("expected negative engine timing to be rejected")
	}

	assertSizeConsistent(t, c)
}

func TestCorrelatorAttachObservationPreservesZeroVsNil(t *testing.T) {
	c := newTestCorrelator(t)
	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)

	s := validSummary(fixtureTime, 90*time.Millisecond)
	// Zero value preserved as a pointer to zero.
	s.TokenUsage.PromptTokens = ptrI64(0)
	// Unknown remains nil (a nil phase, not the total).
	s.UpstreamHeaderMs = nil
	s.FirstOutputMs = nil

	if !c.AttachObservation("req-1", s) {
		t.Fatal("expected attach to succeed")
	}
	obs := observation(t, c, "req-1")
	if obs.PromptTokens == nil || *obs.PromptTokens != 0 {
		t.Fatalf("zero prompt_tokens should be preserved as &0, got %v", obs.PromptTokens)
	}
	if obs.UpstreamHeaderMs != nil {
		t.Fatal("unknown upstream_header_ms should remain nil")
	}
	if obs.FirstOutputMs != nil {
		t.Fatal("unknown first_output_ms should remain nil")
	}
	// Established total is retained.
	if obs.TotalDurationMs == nil || *obs.TotalDurationMs != 90.0 {
		t.Fatalf("total_duration_ms should be retained, got %v", obs.TotalDurationMs)
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorAttachObservationInputMutation(t *testing.T) {
	c := newTestCorrelator(t)
	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)

	s := validSummary(fixtureTime, 90*time.Millisecond)
	if !c.AttachObservation("req-1", s) {
		t.Fatal("expected attach to succeed")
	}
	// Mutate the input summary after attach; the retained observation must be
	// unaffected because projectSummary copies pointed-to values.
	*s.TotalDurationMs = 999.0
	*s.TokenUsage.CompletionTokens = 99
	s.TransportOutcome = "mutated"

	obs := observation(t, c, "req-1")
	if obs.TotalDurationMs == nil || *obs.TotalDurationMs != 90.0 {
		t.Fatalf("total_duration_ms was mutated by input, got %v", obs.TotalDurationMs)
	}
	if obs.CompletionTokens == nil || *obs.CompletionTokens != 7 {
		t.Fatalf("completion_tokens was mutated by input, got %v", obs.CompletionTokens)
	}
	if obs.TransportOutcome != "returned" {
		t.Fatalf("transport_outcome was mutated by input, got %q", obs.TransportOutcome)
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorAttachObservationQueryCopyMutation(t *testing.T) {
	c := newTestCorrelator(t)
	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
	if !c.AttachObservation("req-1", validSummary(fixtureTime, 90*time.Millisecond)) {
		t.Fatal("expected attach to succeed")
	}

	result := c.Query(QueryFilter{RequestID: "req-1"})
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	r := result[0]
	// Mutate the returned copy's observation pointers.
	if r.ProxyObservation.TotalDurationMs != nil {
		*r.ProxyObservation.TotalDurationMs = 999.0
	}
	if r.ProxyObservation.CompletionTokens != nil {
		*r.ProxyObservation.CompletionTokens = 99
	}
	r.ProxyObservation.TransportOutcome = "mutated"

	// Internal state must be unaffected.
	obs := observation(t, c, "req-1")
	if obs.TotalDurationMs == nil || *obs.TotalDurationMs != 90.0 {
		t.Fatalf("internal total_duration_ms was mutated, got %v", obs.TotalDurationMs)
	}
	if obs.CompletionTokens == nil || *obs.CompletionTokens != 7 {
		t.Fatalf("internal completion_tokens was mutated, got %v", obs.CompletionTokens)
	}
	if obs.TransportOutcome != "returned" {
		t.Fatalf("internal transport_outcome was mutated, got %q", obs.TransportOutcome)
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorAttachObservationHostileEnumNeverJSON(t *testing.T) {
	c := newTestCorrelator(t)
	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)

	s := validSummary(fixtureTime, 90*time.Millisecond)
	// Hostile / unknown enum values must be omitted, never copied.
	s.TransportOutcome = "hostile"
	s.GenerationOutcome = "X" // short sentinel
	s.InspectionState = "??"
	s.TokenUsage.FinishReason = "not_a_reason"

	if !c.AttachObservation("req-1", s) {
		t.Fatal("expected attach to succeed")
	}
	r, _ := c.GetByRequestID("req-1")
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	js := string(b)
	if strings.Contains(js, "hostile") || strings.Contains(js, "\"X\"") || strings.Contains(js, "??") || strings.Contains(js, "not_a_reason") {
		t.Fatalf("hostile enum/sentinel value retained in JSON: %s", js)
	}
	// Valid fixed labels and valid values must still be present.
	if !strings.Contains(js, "\"source\":\"proxy\"") || !strings.Contains(js, "\"scope\":\"proxy_request_only\"") {
		t.Fatalf("fixed labels missing in JSON: %s", js)
	}
	if !strings.Contains(js, "\"first_byte_ms\":20") {
		t.Fatalf("valid phase missing in JSON: %s", js)
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorAttachObservationNoActivityIDOrStatusChange(t *testing.T) {
	c := newTestCorrelator(t)
	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
	// Complete the request to set a status.
	c.CompleteRequest("req-1", time.Second, "completed")

	if !c.AttachObservation("req-1", validSummary(fixtureTime, 90*time.Millisecond)) {
		t.Fatal("expected attach to succeed")
	}
	r, _ := c.GetByRequestID("req-1")
	if r.ActivityID != "" {
		t.Fatalf("attach must not create an ActivityID, got %q", r.ActivityID)
	}
	if r.Status != "completed" {
		t.Fatalf("attach must not modify status, got %q", r.Status)
	}
	if r.RequestID != "req-1" || r.BackendID != "backend-A" || r.ModelID != "model-1" {
		t.Fatalf("attach must not modify request/backend/model IDs: %#v", r)
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorAttachObservationSizeConsistencyAndRetention(t *testing.T) {
	c := newTestCorrelator(t)
	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)

	// Minimal observation: only the total timestamp is established.
	minimal := timing.Summary{
		StartTime:        fixtureTime,
		EndTime:          fixtureTime.Add(90 * time.Millisecond),
		TotalDurationMs:  ptrF64(90.0),
		TransportOutcome: "returned",
	}
	if !c.AttachObservation("req-1", minimal) {
		t.Fatal("expected attach of minimal observation to succeed")
	}
	assertSizeConsistent(t, c)
	size1 := c.Stats()["total_size"].(int64)

	// Reattach the same observation: replace, no growth.
	if !c.AttachObservation("req-1", minimal) {
		t.Fatal("expected reattach to succeed")
	}
	assertSizeConsistent(t, c)
	size2 := c.Stats()["total_size"].(int64)
	if size2 != size1 {
		t.Fatalf("reattach should not grow size: %d -> %d", size1, size2)
	}

	// Reattach with a full observation: growth.
	full := validSummary(fixtureTime, 90*time.Millisecond)
	if !c.AttachObservation("req-1", full) {
		t.Fatal("expected reattach with full observation to succeed")
	}
	assertSizeConsistent(t, c)
	size3 := c.Stats()["total_size"].(int64)
	if size3 <= size2 {
		t.Fatalf("full observation should grow size: %d -> %d", size2, size3)
	}

	// Tighten count retention to evict.
	c.WithRetention(1, 0, 0)
	assertSizeConsistent(t, c)
	if c.Stats()["record_count"] != 1 {
		t.Fatalf("expected 1 record after count eviction, got %d", c.Stats()["record_count"])
	}
}

func TestCorrelatorAttachObservationFrozenPageIsolation(t *testing.T) {
	c := newTestCorrelator(t)
	for i := 0; i < 5; i++ {
		c.RegisterRequest("req-"+string(rune('1'+i)), "backend-A", "model-1", fixtureTime.Add(time.Duration(i)*time.Minute))
	}
	// Force a snapshot by paging a subset.
	page, err := c.QueryPage(QueryFilter{}, PageRequest{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Records) != 2 {
		t.Fatalf("expected 2 records on first page, got %d", len(page.Records))
	}

	// Attach an observation to a record after the snapshot was frozen.
	if !c.AttachObservation("req-1", validSummary(fixtureTime, 90*time.Millisecond)) {
		t.Fatal("expected attach to succeed")
	}

	// Resume the snapshot: records must be unchanged (defensive clones).
	resumed, err := c.QueryPage(QueryFilter{}, PageRequest{Cursor: page.Cursor})
	if err != nil {
		t.Fatal(err)
	}
	if len(resumed.Records) != 2 {
		t.Fatalf("expected 2 records on resumed page, got %d", len(resumed.Records))
	}
	for _, r := range resumed.Records {
		if r.ProxyObservation != nil {
			t.Fatalf("frozen snapshot record %q must not gain an observation", r.RequestID)
		}
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorAttachObservationConcurrency(t *testing.T) {
	c := newTestCorrelator(t)
	for i := 0; i < 10; i++ {
		c.RegisterRequest("req-"+string(rune('1'+i)), "backend-A", "model-1", fixtureTime.Add(time.Duration(i)*time.Minute))
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c.AttachObservation("req-"+string(rune('1'+i)), validSummary(fixtureTime, 90*time.Millisecond))
		}(i)
	}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Query(QueryFilter{})
		}()
	}
	wg.Wait()
	assertSizeConsistent(t, c)
}

func TestCorrelatorAttachObservationOutcomesDistinct(t *testing.T) {
	// Cross-outcome constraints apply: interrupted transport forces
	// completed/truncated generation to incomplete (unless uninspected);
	// partial inspection forces completed to unknown; unknown transport
	// forces completed to unknown; uninspected inspection forces generation
	// uninspected.
	cases := []struct {
		name           string
		transport      string
		generation     string
		inspection     string
		finishReason   string
		wantGeneration string
	}{
		{"finished", "returned", "completed", "full", "stop", "completed"},
		{"aborted", "aborted", "incomplete", "partial", "stop", "incomplete"},
		{"canceled", "canceled", "incomplete", "partial", "stop", "incomplete"},
		{"timeout", "timeout", "incomplete", "full", "stop", "incomplete"},
		{"transport_error", "transport_error", "unknown", "full", "stop", "unknown"},
		{"uninspected", "returned", "uninspected", "uninspected", "stop", "uninspected"},
		// Legit truncated reason length with returned full is accepted.
		{"truncated_length", "returned", "truncated", "full", "length", "truncated"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestCorrelator(t)
			c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
			s := validSummary(fixtureTime, 90*time.Millisecond)
			s.TransportOutcome = tc.transport
			s.GenerationOutcome = tc.generation
			s.InspectionState = tc.inspection
			s.TokenUsage.FinishReason = tc.finishReason
			if !c.AttachObservation("req-1", s) {
				t.Fatal("expected attach to succeed")
			}
			obs := observation(t, c, "req-1")
			if obs.TransportOutcome != tc.transport {
				t.Fatalf("transport_outcome = %q, want %q", obs.TransportOutcome, tc.transport)
			}
			if obs.GenerationOutcome != tc.wantGeneration {
				t.Fatalf("generation_outcome = %q, want %q", obs.GenerationOutcome, tc.wantGeneration)
			}
			if obs.InspectionState != tc.inspection {
				t.Fatalf("inspection_state = %q, want %q", obs.InspectionState, tc.inspection)
			}
			if tc.finishReason == "" || tc.inspection == "uninspected" {
				if obs.FinishReason != "" {
					t.Fatalf("finish_reason should be empty, got %q", obs.FinishReason)
				}
			} else {
				if obs.FinishReason != tc.finishReason {
					t.Fatalf("finish_reason = %q, want %q", obs.FinishReason, tc.finishReason)
				}
			}
			assertSizeConsistent(t, c)
		})
	}
}

func TestCorrelatorAttachObservationRejectsInvalidTime(t *testing.T) {
	c := newTestCorrelator(t)
	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)

	// Finished summary with nil total duration is rejected.
	nilTotal := validSummary(fixtureTime, 90*time.Millisecond)
	nilTotal.TotalDurationMs = nil
	if c.AttachObservation("req-1", nilTotal) {
		t.Fatal("expected nil total duration to be rejected")
	}

	// Zero StartTime is rejected.
	zeroStart := validSummary(fixtureTime, 90*time.Millisecond)
	zeroStart.StartTime = time.Time{}
	if c.AttachObservation("req-1", zeroStart) {
		t.Fatal("expected zero StartTime to be rejected")
	}

	// Invalid JSON year (>9999) is rejected.
	badYear := validSummary(fixtureTime, 90*time.Millisecond)
	badYear.StartTime = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
	if c.AttachObservation("req-1", badYear) {
		t.Fatal("expected year 10000 StartTime to be rejected")
	}

	// EndTime before StartTime (backwards) is rejected.
	backwards := validSummary(fixtureTime, 90*time.Millisecond)
	backwards.EndTime = fixtureTime.Add(-time.Minute)
	if c.AttachObservation("req-1", backwards) {
		t.Fatal("expected backwards time horizon to be rejected")
	}

	assertSizeConsistent(t, c)
}

func TestCorrelatorAttachObservationPairwiseOrderNilAnswer(t *testing.T) {
	c := newTestCorrelator(t)
	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)

	// Pairwise order holds when both known; missing answer is fine.
	s := validSummary(fixtureTime, 90*time.Millisecond)
	s.FirstAnswerMs = nil // answer missing
	if !c.AttachObservation("req-1", s) {
		t.Fatal("expected attach with nil answer to succeed")
	}
	obs := observation(t, c, "req-1")
	if obs.FirstAnswerMs != nil {
		t.Fatal("nil answer should remain nil")
	}
	assertSizeConsistent(t, c)

	// header > firstbyte is rejected even if answer is nil.
	bad := validSummary(fixtureTime, 90*time.Millisecond)
	bad.UpstreamHeaderMs = ptrF64(100.0)
	bad.FirstByteMs = ptrF64(20.0)
	bad.FirstAnswerMs = nil
	if c.AttachObservation("req-1", bad) {
		t.Fatal("expected header > firstbyte to be rejected")
	}

	// byte > output is rejected.
	bad = validSummary(fixtureTime, 90*time.Millisecond)
	bad.FirstByteMs = ptrF64(100.0)
	bad.FirstOutputMs = ptrF64(30.0)
	if c.AttachObservation("req-1", bad) {
		t.Fatal("expected byte > output to be rejected")
	}

	assertSizeConsistent(t, c)
}

func TestCorrelatorAttachObservationKnownFinishReasons(t *testing.T) {
	c := newTestCorrelator(t)
	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)

	// All producer finish reasons are retained.
	for _, reason := range []string{"stop", "length", "tool_calls", "function_call", "content_filter"} {
		s := validSummary(fixtureTime, 90*time.Millisecond)
		s.TokenUsage.FinishReason = reason
		if !c.AttachObservation("req-1", s) {
			t.Fatalf("expected finish reason %q to be accepted", reason)
		}
		obs := observation(t, c, "req-1")
		if obs.FinishReason != reason {
			t.Fatalf("finish_reason = %q, want %q", obs.FinishReason, reason)
		}
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorAttachObservationCrossOutcomeRegressions(t *testing.T) {
	cases := []struct {
		name           string
		transport      string
		generation     string
		inspection     string
		wantGeneration string
	}{
		// Interrupted transport cannot retain completed generation.
		{"forged completed+canceled", "canceled", "completed", "full", "incomplete"},
		// Partial inspection cannot be completed.
		{"completed+partial", "returned", "completed", "partial", "unknown"},
		// Unknown transport cannot confidently be completed.
		{"completed+unknown transport", "", "completed", "full", "unknown"},
		// Uninspected inspection forces generation uninspected.
		{"uninspected+completed", "returned", "completed", "uninspected", "uninspected"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestCorrelator(t)
			c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
			s := validSummary(fixtureTime, 90*time.Millisecond)
			s.TransportOutcome = tc.transport
			s.GenerationOutcome = tc.generation
			s.InspectionState = tc.inspection
			if !c.AttachObservation("req-1", s) {
				t.Fatal("expected attach to succeed")
			}
			obs := observation(t, c, "req-1")
			if obs.GenerationOutcome != tc.wantGeneration {
				t.Fatalf("generation_outcome = %q, want %q", obs.GenerationOutcome, tc.wantGeneration)
			}
			assertSizeConsistent(t, c)
		})
	}
}

func TestCorrelatorAttachObservationSelfEvictionReturnsFalse(t *testing.T) {
	// Tight size retention: the attached observation alone exceeds the cap,
	// so the record is evicted and AttachObservation returns false.
	c := newTestCorrelatorWithRetention(t, 0, 0, 200)
	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)
	if c.AttachObservation("req-1", validSummary(fixtureTime, 90*time.Millisecond)) {
		t.Fatal("expected attach to evict the record and return false")
	}
	if _, ok := c.records["req-1"]; ok {
		t.Fatal("expected record to be evicted")
	}
	assertSizeConsistent(t, c)
}

func TestCorrelatorAttachObservationFailedReplaceLeavesOldObservation(t *testing.T) {
	c := newTestCorrelator(t)
	c.RegisterRequest("req-1", "backend-A", "model-1", fixtureTime)

	// First attach succeeds.
	if !c.AttachObservation("req-1", validSummary(fixtureTime, 90*time.Millisecond)) {
		t.Fatal("expected first attach to succeed")
	}
	first := observation(t, c, "req-1")
	if first.TotalDurationMs == nil || *first.TotalDurationMs != 90.0 {
		t.Fatal("first observation total should be 90")
	}

	// Second attach with an unfinished summary is rejected; the old
	// observation must remain unchanged.
	unfinished := timing.Summary{StartTime: fixtureTime} // zero EndTime
	if c.AttachObservation("req-1", unfinished) {
		t.Fatal("expected unfinished summary to be rejected")
	}
	obs := observation(t, c, "req-1")
	if obs.TotalDurationMs == nil || *obs.TotalDurationMs != 90.0 {
		t.Fatal("old observation should be unchanged after failed replace")
	}
	assertSizeConsistent(t, c)
}

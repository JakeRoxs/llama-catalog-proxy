package timing

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func observe(t *testing.T, mode string) (*Observer, *httptest.ResponseRecorder) {
	t.Helper()
	w := httptest.NewRecorder()
	o := New(w)
	o.ObserveUpstream(200, http.Header{"Content-Type": {mode}})
	return o, w
}
func send(t *testing.T, o *Observer, s string) {
	t.Helper()
	if n, err := o.Write([]byte(s)); err != nil || n != len(s) {
		t.Fatalf("write=%d,%v", n, err)
	}
}
func ms(t *testing.T, p *float64, want float64) {
	t.Helper()
	if p == nil || *p != want {
		t.Fatalf("duration=%v, want %g", p, want)
	}
}

func TestPhasesAndLateTerminalUsage(t *testing.T) {
	now := time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)
	w := httptest.NewRecorder()
	o := New(w, WithClock(func() time.Time { return now }))
	if o.Summary().TotalDurationMs != nil {
		t.Fatal("unfinished summary has total duration")
	}
	now = now.Add(10 * time.Millisecond)
	o.ObserveUpstream(200, http.Header{"Content-Type": {"text/event-stream; charset=utf-8"}})
	o.WriteHeader(200)
	o.Flush()
	if o.Summary().FirstByteMs != nil {
		t.Fatal("headers/flush fabricated body bytes")
	}
	chunks := []string{
		"data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n",
		"data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"PRIVATE_REASONING\"}}]}\n\n",
		"data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"function\":{\"arguments\":\"PRIVATE_TOOL\"}}]}}]}\n\n",
		"data: {\"choices\":[{\"delta\":{\"content\":\"PRIVATE_ANSWER\"}}]}\n\n",
		"data: {\"choices\":[{\"finish_reason\":\"stop\"}]}\n\n",
		"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":0,\"completion_tokens\":7,\"total_tokens\":7,\"prompt_tokens_details\":{\"cached_tokens\":0},\"completion_tokens_details\":{\"reasoning_tokens\":2}},\"timings\":{\"prompt_ms\":0,\"predicted_ms\":10,\"prompt_n\":0,\"predicted_n\":7}}\n\n",
		"data: [DONE]\n\n",
	}
	for i, c := range chunks {
		now = now.Add(10 * time.Millisecond)
		send(t, o, c)
		if i == 0 && o.Summary().FirstOutputMs != nil {
			t.Fatal("role-only counted as output")
		}
	}
	now = now.Add(10 * time.Millisecond)
	_ = o.Finish(nil)
	s := o.Summary()
	ms(t, s.UpstreamHeaderMs, 10)
	ms(t, s.FirstByteMs, 20)
	ms(t, s.FirstOutputMs, 30)
	ms(t, s.FirstAnswerMs, 50)
	ms(t, s.TotalDurationMs, 90)
	if s.TokenUsage.PromptTokens == nil || *s.TokenUsage.PromptTokens != 0 || s.TokenUsage.CompletionTokens == nil || *s.TokenUsage.CompletionTokens != 7 || s.TokenUsage.CachedTokens == nil || *s.TokenUsage.CachedTokens != 0 || s.TokenUsage.ReasoningTokens == nil || *s.TokenUsage.ReasoningTokens != 2 {
		t.Fatalf("reported usage=%+v", s.TokenUsage)
	}
	if s.EngineTimings.PromptMS == nil || *s.EngineTimings.PromptMS != 0 || s.EngineTimings.PredictedMS == nil || *s.EngineTimings.PredictedMS != 10 {
		t.Fatal("engine timings absent")
	}
	if s.TransportOutcome != "returned" || s.GenerationOutcome != "completed" {
		t.Fatalf("outcomes=%s/%s", s.TransportOutcome, s.GenerationOutcome)
	}
	if w.Body.String() != strings.Join(chunks, "") {
		t.Fatal("forwarded bytes changed")
	}
	b, _ := json.Marshal(s)
	for _, secret := range []string{"PRIVATE_REASONING", "PRIVATE_TOOL", "PRIVATE_ANSWER"} {
		if bytes.Contains(b, []byte(secret)) {
			t.Fatal("text retained in summary")
		}
	}
	if o.parser.line != nil || o.parser.event != nil || o.parser.body != nil {
		t.Fatal("body buffers retained after Finish")
	}
	*s.TokenUsage.CompletionTokens = 999
	*s.FirstAnswerMs = 999
	if *o.Summary().TokenUsage.CompletionTokens != 7 || *o.Summary().FirstAnswerMs != 50 {
		t.Fatal("summary aliases observer")
	}
}

func TestFramingSplitCRLFMulitline(t *testing.T) {
	for _, sep := range []string{"\n", "\r\n", "\r"} {
		t.Run(strings.ReplaceAll(sep, "\r", "CR"), func(t *testing.T) {
			o, w := observe(t, "text/event-stream")
			body := ": heartbeat" + sep + sep + "data: {\"choices\":" + sep + "data: [{\"delta\":{\"content\":\"PRIVATE\"}}]}" + sep + sep + "data: [DONE]" + sep + sep
			for i := range body {
				send(t, o, body[i:i+1])
			}
			_ = o.Finish(nil)
			if s := o.Summary(); s.GenerationOutcome != "completed" || s.FirstOutputMs == nil || s.FirstAnswerMs == nil {
				t.Fatalf("framing outcome=%+v", s)
			}
			if w.Body.String() != body {
				t.Fatal("framing altered bytes")
			}
		})
	}
}

func TestPartialInspectionNeverRestoresCompletion(t *testing.T) {
	for _, bad := range []string{"data: BAD_SECRET\n\n", "data: " + strings.Repeat("x", maxEventBytes*3) + "\n\n"} {
		o, w := observe(t, "text/event-stream")
		body := bad + "data: {\"choices\":[{\"delta\":{\"content\":\"valid\"}}]}\n\ndata: [DONE]\n\n"
		send(t, o, body)
		if len(o.parser.line) > maxEventBytes || len(o.parser.event) > maxEventBytes {
			t.Fatal("unbounded framing buffer")
		}
		_ = o.Close()
		s := o.Summary()
		if s.InspectionState != "partial" || s.GenerationOutcome == "completed" || s.FirstAnswerMs == nil {
			t.Fatalf("resync=%+v", s)
		}
		if w.Body.String() != body {
			t.Fatal("fail-open bytes changed")
		}
	}
}

func TestOutcomesAndFailurePrecedence(t *testing.T) {
	cases := []struct {
		reason                string
		err                   error
		transport, generation string
	}{
		{"stop", nil, "returned", "completed"}, {"length", nil, "returned", "truncated"},
		{"stop", context.Canceled, "canceled", "incomplete"}, {"stop", context.DeadlineExceeded, "timeout", "incomplete"},
		{"stop", ErrAborted, "aborted", "incomplete"}, {"stop", http.ErrAbortHandler, "aborted", "incomplete"},
		{"stop", io.ErrUnexpectedEOF, "transport_error", "incomplete"},
	}
	for _, c := range cases {
		o, _ := observe(t, "text/event-stream")
		send(t, o, "data: {\"choices\":[{\"finish_reason\":\""+c.reason+"\"}]}\n\ndata: [DONE]\n\n")
		_ = o.Finish(c.err)
		_ = o.Close()
		s := o.Summary()
		if s.TransportOutcome != c.transport || s.GenerationOutcome != c.generation {
			t.Fatalf("%v: %+v", c.err, s)
		}
	}
	o, _ := observe(t, "text/event-stream")
	_ = o.Finish(errors.New("PRIVATE_TRANSPORT"))
	_ = o.Finish(context.Canceled)
	if o.Summary().TransportOutcome != "canceled" {
		t.Fatal("cancellation lost to earlier generic error")
	}
	o, _ = observe(t, "text/event-stream")
	send(t, o, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n")
	_ = o.Finish(nil)
	if o.Summary().GenerationOutcome != "incomplete" {
		t.Fatal("EOF inferred completion")
	}
}

func TestJSONUnknownCountersAndNoStreamingPhase(t *testing.T) {
	o, w := observe(t, "application/json")
	body := `{"choices":[{"message":{"content":"PRIVATE_JSON"},"finish_reason":"stop"}],"usage":{"prompt_tokens":0,"completion_tokens":1.5,"total_tokens":-1},"timings":{"prompt_ms":0,"predicted_ms":-2}}`
	send(t, o, body)
	_ = o.Finish(nil)
	s := o.Summary()
	if s.FirstOutputMs != nil || s.FirstAnswerMs != nil || s.FirstByteMs == nil {
		t.Fatal("JSON inferred streaming phases")
	}
	if s.TokenUsage.PromptTokens == nil || *s.TokenUsage.PromptTokens != 0 || s.TokenUsage.CompletionTokens != nil || s.TokenUsage.TotalTokens != nil || s.EngineTimings.PredictedMS != nil {
		t.Fatal("unknown/invalid counts invented")
	}
	if s.GenerationOutcome != "completed" || w.Body.String() != body {
		t.Fatal("JSON metadata/passthrough failed")
	}
	o, _ = observe(t, "text/event-stream")
	send(t, o, "data: {\"choices\":[{\"finish_reason\":\"PRIVATE_REASON\"}]}\n\n")
	_ = o.Finish(nil)
	b, _ := json.Marshal(o.Summary())
	if bytes.Contains(b, []byte("PRIVATE_REASON")) {
		t.Fatal("raw finish reason retained")
	}
}

func TestSSEChoiceFinishDoesNotCompleteWholeStream(t *testing.T) {
	o, _ := observe(t, "text/event-stream")
	send(t, o, "data: {\"choices\":[{\"index\":0,\"finish_reason\":\"stop\"}]}\n\n")
	send(t, o, "data: {\"choices\":[{\"index\":1,\"delta\":{\"content\":\"unfinished\"}}]}\n\n")
	_ = o.Finish(nil)
	if o.Summary().GenerationOutcome == "completed" {
		t.Fatal("choice-level finish was mistaken for stream completion")
	}
}

func TestSummaryCopyPreservesInFlightMetadata(t *testing.T) {
	o, _ := observe(t, "text/event-stream")
	send(t, o, "data: {\"usage\":{\"completion_tokens\":0},\"timings\":{\"predicted_ms\":2}}\n\n")
	s := o.Summary()
	if s.TokenUsage.CompletionTokens == nil || *s.TokenUsage.CompletionTokens != 0 || s.EngineTimings.PredictedMS == nil || *s.EngineTimings.PredictedMS != 2 {
		t.Fatal("in-flight reported metadata disappeared before Finish")
	}
	*s.TokenUsage.CompletionTokens = 999
	*s.EngineTimings.PredictedMS = 999
	if *o.Summary().TokenUsage.CompletionTokens != 0 || *o.Summary().EngineTimings.PredictedMS != 2 {
		t.Fatal("in-flight metadata was not defensively copied")
	}
	_ = o.Close()
}

func TestContentGateAndCloseOwnership(t *testing.T) {
	for _, c := range []struct {
		typ, enc string
		status   int
	}{{"application/json", "gzip", 200}, {"application/octet-stream", "", 200}, {"text/event-stream", "", 500}, {"text/event-stream", "", 101}} {
		w := &ownedWriter{ResponseRecorder: httptest.NewRecorder()}
		o := New(w)
		o.ObserveUpstream(c.status, http.Header{"Content-Type": {c.typ}, "Content-Encoding": {c.enc}})
		body := "data: [DONE]\n\n"
		send(t, o, body)
		_ = o.Close()
		if w.closed || w.Body.String() != body || o.Summary().InspectionState != "uninspected" || o.Summary().GenerationOutcome != "uninspected" {
			t.Fatal("content gate or writer ownership violated")
		}
	}
	w := &ownedWriter{ResponseRecorder: httptest.NewRecorder(), zero: true}
	o := New(w)
	o.ObserveUpstream(200, http.Header{"Content-Type": {"text/event-stream"}})
	_, _ = o.Write([]byte("x"))
	_ = o.WriteHeader
	_ = o.Close()
	if o.Summary().FirstByteMs != nil {
		t.Fatal("zero accepted bytes stamped")
	}
	w = &ownedWriter{ResponseRecorder: httptest.NewRecorder(), failure: true}
	o = New(w)
	_, _ = o.Write([]byte("x"))
	_ = o.Finish(nil)
	if o.Summary().TransportOutcome != "transport_error" {
		t.Fatal("write failure lost")
	}
	b, _ := json.Marshal(o.Summary())
	if bytes.Contains(b, []byte("PRIVATE_WRITE_ERROR")) {
		t.Fatal("raw write error retained")
	}
}

type ownedWriter struct {
	*httptest.ResponseRecorder
	closed, zero, failure bool
}

func (w *ownedWriter) Close() error { w.closed = true; return nil }
func (w *ownedWriter) Write(p []byte) (int, error) {
	if w.zero {
		return 0, nil
	}
	if w.failure {
		return 0, errors.New("PRIVATE_WRITE_ERROR")
	}
	return w.ResponseRecorder.Write(p)
}

func TestConcurrentSummaries(t *testing.T) {
	o, _ := observe(t, "text/event-stream")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			s := o.Summary()
			if s.FirstByteMs != nil {
				*s.FirstByteMs = 999
			}
		}
	}()
	for i := 0; i < 100; i++ {
		send(t, o, ": heartbeat\n\n")
	}
	_ = o.Close()
	wg.Wait()
}

type blockingResponse struct {
	discardResponse
	started, release chan struct{}
}

func (w *blockingResponse) Write(p []byte) (int, error) {
	close(w.started)
	<-w.release
	return len(p), nil
}

func TestObserverPreservesWriteBackpressure(t *testing.T) {
	w := &blockingResponse{discardResponse: discardResponse{h: make(http.Header)}, started: make(chan struct{}), release: make(chan struct{})}
	o := New(w)
	o.ObserveUpstream(200, http.Header{"Content-Type": {"text/event-stream"}})
	done := make(chan struct{})
	go func() { _, _ = o.Write([]byte("data: [DONE]\n\n")); close(done) }()
	<-w.started
	select {
	case <-done:
		t.Fatal("observer bypassed a blocked downstream writer")
	default:
	}
	if o.Summary().FirstByteMs != nil {
		t.Error("blocked write had not yet accepted any bytes")
	}
	close(w.release)
	<-done
	_ = o.Finish(nil)
	if o.Summary().FirstByteMs == nil {
		t.Fatal("successful write was not observed")
	}
}

type discardResponse struct{ h http.Header }

func (w *discardResponse) Header() http.Header         { return w.h }
func (w *discardResponse) Write(p []byte) (int, error) { return len(p), nil }
func (w *discardResponse) WriteHeader(int)             {}
func BenchmarkObservation(b *testing.B) {
	sse := []byte("data: {\"choices\":[{\"delta\":{\"content\":\"text\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	for _, mode := range []string{"baseline", "text/event-stream", "application/json"} {
		b.Run(mode, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				w := &discardResponse{h: make(http.Header)}
				if mode == "baseline" {
					_, _ = w.Write(sse)
					continue
				}
				o := New(w)
				o.ObserveUpstream(200, http.Header{"Content-Type": {mode}})
				data := sse
				if mode == "application/json" {
					data = []byte(`{"choices":[{"finish_reason":"stop"}],"usage":{"completion_tokens":1}}`)
				}
				_, _ = o.Write(data)
				_ = o.Finish(nil)
				_ = o.Summary()
			}
		})
	}
}

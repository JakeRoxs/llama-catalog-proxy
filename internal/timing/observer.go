// Package timing observes bounded response metadata without changing forwarding.
// Output times describe observed output events, not engine token timestamps.
package timing

import (
	"bufio"
	"context"
	"errors"
	"mime"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

var ErrAborted = errors.New("response aborted")

// Usage contains reported values only. Nil is unknown; a pointer to zero is zero.
type TokenUsage struct {
	PromptTokens     *int64 `json:"prompt_tokens,omitempty"`
	CompletionTokens *int64 `json:"completion_tokens,omitempty"`
	TotalTokens      *int64 `json:"total_tokens,omitempty"`
	CachedTokens     *int64 `json:"cached_tokens,omitempty"`
	ReasoningTokens  *int64 `json:"reasoning_tokens,omitempty"`
	FinishReason     string `json:"finish_reason,omitempty"`
}

type EngineTimings struct {
	PromptMS    *float64 `json:"prompt_ms,omitempty"`
	PredictedMS *float64 `json:"predicted_ms,omitempty"`
	PromptN     *int64   `json:"prompt_n,omitempty"`
	PredictedN  *int64   `json:"predicted_n,omitempty"`
}

type Summary struct {
	StartTime         time.Time     `json:"start_time"`
	EndTime           time.Time     `json:"end_time,omitzero"`
	TotalDurationMs   *float64      `json:"total_duration_ms,omitempty"`
	UpstreamHeaderMs  *float64      `json:"upstream_header_ms,omitempty"`
	FirstByteMs       *float64      `json:"first_byte_ms,omitempty"`
	FirstOutputMs     *float64      `json:"first_output_ms,omitempty"`
	FirstAnswerMs     *float64      `json:"first_answer_ms,omitempty"`
	TokenUsage        TokenUsage    `json:"usage"`
	EngineTimings     EngineTimings `json:"engine_timings"`
	TransportOutcome  string        `json:"transport_outcome"`
	GenerationOutcome string        `json:"generation_outcome"`
	InspectionState   string        `json:"inspection_state"`
}

type Option func(*Observer)

func WithClock(now func() time.Time) Option {
	return func(o *Observer) {
		if now != nil {
			o.now = now
		}
	}
}

// Observer owns its metadata, never the underlying writer. ResponseWriter writes
// remain caller-serialized; Summary is safe concurrently with observation.
type Observer struct {
	wrapped     http.ResponseWriter
	mu          sync.Mutex
	now         func() time.Time
	start       time.Time
	summary     Summary
	parser      metadataParser
	headersSeen bool
	finished    bool
	writeFailed bool
}

func New(w http.ResponseWriter, opts ...Option) *Observer {
	o := &Observer{wrapped: w, now: time.Now}
	for _, opt := range opts {
		opt(o)
	}
	o.start = o.now()
	o.summary = Summary{StartTime: o.start, TransportOutcome: "pending", GenerationOutcome: "unknown", InspectionState: "uninspected"}
	return o
}

func (o *Observer) elapsed(at time.Time) *float64 {
	v := float64(at.Sub(o.start)) / float64(time.Millisecond)
	if v < 0 {
		v = 0
	}
	return &v
}

// ObserveUpstream must be called at the reverse proxy's response-header hook.
// Downstream WriteHeader and Flush do not constitute upstream/body observations.
func (o *Observer) ObserveUpstream(status int, headers http.Header) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.finished || o.headersSeen || (status < 200 && status != 101) {
		return
	}
	o.headersSeen = true
	o.summary.UpstreamHeaderMs = o.elapsed(o.now())
	encoding := strings.TrimSpace(headers.Get("Content-Encoding"))
	if status < 200 || status >= 300 || (encoding != "" && !strings.EqualFold(encoding, "identity")) {
		return
	}
	media, _, err := mime.ParseMediaType(headers.Get("Content-Type"))
	if err != nil {
		return
	}
	switch strings.ToLower(media) {
	case "text/event-stream":
		o.parser.mode = "sse"
	case "application/json":
		o.parser.mode = "json"
	default:
		return
	}
	o.summary.InspectionState = "full"
}

func (o *Observer) Header() http.Header    { return o.wrapped.Header() }
func (o *Observer) WriteHeader(status int) { o.wrapped.WriteHeader(status) }
func (o *Observer) Write(p []byte) (int, error) {
	n, err := o.wrapped.Write(p)
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.finished {
		return n, err
	}
	if err != nil {
		o.writeFailed = true
	}
	if n > 0 && n <= len(p) {
		if o.summary.FirstByteMs == nil {
			o.summary.FirstByteMs = o.elapsed(o.now())
		}
		meaningful, answer := o.parser.feed(p[:n])
		if meaningful && o.summary.FirstOutputMs == nil {
			o.summary.FirstOutputMs = o.elapsed(o.now())
		}
		if answer && o.summary.FirstAnswerMs == nil {
			o.summary.FirstAnswerMs = o.elapsed(o.now())
		}
	}
	return n, err
}
func (o *Observer) Flush() { _ = http.NewResponseController(o.wrapped).Flush() }
func (o *Observer) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return http.NewResponseController(o.wrapped).Hijack()
}
func (o *Observer) Unwrap() http.ResponseWriter { return o.wrapped }

func transportOutcome(err error) string {
	switch {
	case err == nil:
		return "returned"
	case errors.Is(err, ErrAborted), errors.Is(err, http.ErrAbortHandler):
		return "aborted"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	default:
		return "transport_error"
	}
}

func outcomePriority(outcome string) int {
	switch outcome {
	case "aborted":
		return 4
	case "canceled":
		return 3
	case "timeout":
		return 2
	case "transport_error":
		return 1
	default:
		return 0
	}
}

// Finish releases all transient body buffers. A later error can downgrade a
// previously clean finish; Close(nil) can never erase interruption evidence.
func (o *Observer) Finish(err error) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	outcome := transportOutcome(err)
	if err == nil && o.writeFailed {
		outcome = "transport_error"
	}
	if o.finished {
		if outcomePriority(outcome) > outcomePriority(o.summary.TransportOutcome) {
			o.summary.TransportOutcome = outcome
			if o.summary.GenerationOutcome != "uninspected" {
				o.summary.GenerationOutcome = "incomplete"
			}
		}
		return nil
	}
	o.finished = true
	o.parser.finish()
	o.summary.EndTime = o.now()
	o.summary.TotalDurationMs = o.elapsed(o.summary.EndTime)
	o.summary.TransportOutcome = outcome
	o.summary.TokenUsage = o.parser.usage
	o.summary.EngineTimings = o.parser.timings
	switch {
	case o.parser.mode == "":
		o.summary.GenerationOutcome = "uninspected"
	case outcome != "returned":
		o.summary.GenerationOutcome = "incomplete"
	case o.parser.partial:
		o.summary.GenerationOutcome = "unknown"
	case o.parser.truncated:
		o.summary.GenerationOutcome = "truncated"
	case o.parser.terminal && o.parser.recognized:
		o.summary.GenerationOutcome = "completed"
	default:
		o.summary.GenerationOutcome = "incomplete"
	}
	if o.parser.partial {
		o.summary.InspectionState = "partial"
	}
	o.parser.release()
	return nil
}
func (o *Observer) Close() error { return o.Finish(nil) }

func copyInt(p *int64) *int64 {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}
func copyFloat(p *float64) *float64 {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}
func (o *Observer) Summary() Summary {
	o.mu.Lock()
	defer o.mu.Unlock()
	s := o.summary
	if !o.finished {
		s.TokenUsage = o.parser.usage
		s.EngineTimings = o.parser.timings
	}
	return s.Copy()
}

// Copy returns a defensive copy of the summary, including deep copies of all
// pointer fields.
func (s Summary) Copy() Summary {
	c := s
	c.TotalDurationMs = copyFloat(s.TotalDurationMs)
	c.UpstreamHeaderMs = copyFloat(s.UpstreamHeaderMs)
	c.FirstByteMs = copyFloat(s.FirstByteMs)
	c.FirstOutputMs = copyFloat(s.FirstOutputMs)
	c.FirstAnswerMs = copyFloat(s.FirstAnswerMs)
	c.TokenUsage = s.TokenUsage
	c.TokenUsage.PromptTokens = copyInt(s.TokenUsage.PromptTokens)
	c.TokenUsage.CompletionTokens = copyInt(s.TokenUsage.CompletionTokens)
	c.TokenUsage.TotalTokens = copyInt(s.TokenUsage.TotalTokens)
	c.TokenUsage.CachedTokens = copyInt(s.TokenUsage.CachedTokens)
	c.TokenUsage.ReasoningTokens = copyInt(s.TokenUsage.ReasoningTokens)
	c.EngineTimings = s.EngineTimings
	c.EngineTimings.PromptMS = copyFloat(s.EngineTimings.PromptMS)
	c.EngineTimings.PredictedMS = copyFloat(s.EngineTimings.PredictedMS)
	c.EngineTimings.PromptN = copyInt(s.EngineTimings.PromptN)
	c.EngineTimings.PredictedN = copyInt(s.EngineTimings.PredictedN)
	return c
}

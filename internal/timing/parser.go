package timing

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
)

const maxEventBytes = 64 * 1024

// Only transient framing buffers contain text. They never enter Summary.
type metadataParser struct {
	mode                                     string
	line, event, body                        []byte
	lineSize, eventSize                      int
	previousCR, dropping                     bool
	partial, terminal, truncated, recognized bool
	usage                                    TokenUsage
	timings                                  EngineTimings
}

func (p *metadataParser) feed(chunk []byte) (meaningful, answer bool) {
	if p.mode == "json" {
		return p.feedJSON(chunk)
	}
	if p.mode != "sse" {
		return
	}
	return p.feedSSE(chunk)
}

// feedJSON accumulates a JSON body chunk until the event is complete or the
// size cap is exceeded. Once partial, later chunks are ignored.
func (p *metadataParser) feedJSON(chunk []byte) (meaningful, answer bool) {
	if p.partial {
		return
	}
	if len(chunk) > maxEventBytes-len(p.body) {
		p.partial = true
		clear(p.body)
		p.body = nil
		return
	}
	p.body = append(p.body, chunk...)
	return
}

// feedSSE iterates SSE framing bytes, folding CRLF via the previousCR carry,
// applying line/event size caps and the dropping state, and aggregating the
// endLine outcome into meaningful/answer.
func (p *metadataParser) feedSSE(chunk []byte) (meaningful, answer bool) {
	for _, c := range chunk {
		lineEnd, skip := p.sseLineEnd(c)
		if skip {
			continue
		}
		if lineEnd {
			m, a := p.endLine()
			meaningful = meaningful || m
			answer = answer || a
			p.previousCR = c == '\r'
			continue
		}
		p.lineSize++
		p.eventSize++
		if p.lineSize > maxEventBytes || p.eventSize > maxEventBytes {
			p.partial = true
			p.dropping = true
			clear(p.line)
			p.line = nil
			clear(p.event)
			p.event = nil
		}
		if !p.dropping {
			p.line = append(p.line, c)
		}
	}
	return
}

// sseLineEnd resolves CR carry-over for one SSE framing byte. It reports
// whether c ends the current line (lineEnd) and whether c must be skipped
// because it is the '\n' of a pending CRLF pair (skip). A pending CR is
// cleared here; feedSSE re-arms it for a lone '\r' line end.
func (p *metadataParser) sseLineEnd(c byte) (lineEnd, skip bool) {
	if p.previousCR {
		p.previousCR = false
		if c == '\n' {
			return false, true
		}
	}
	return c == '\r' || c == '\n', false
}

// endEvent finalizes a completed SSE event. It detects the [DONE] terminal
// marker or parses the buffered event, then clears all event/line state and
// resets the dropping/eventSize flags. It preserves the (meaningful, answer)
// return order of the blank branch of endLine.
func (p *metadataParser) endEvent() (bool, bool) {
	var m, a bool
	if !p.dropping && len(p.event) > 0 {
		data := p.event[:len(p.event)-1] // remove the final SSE data newline
		if bytes.Equal(data, []byte("[DONE]")) {
			p.terminal = true
			p.recognized = true
		} else {
			m, a = p.parse(data, true)
		}
	}
	clear(p.event)
	p.event = nil
	clear(p.line)
	p.line = nil
	p.eventSize = 0
	p.dropping = false
	return m, a
}

func (p *metadataParser) endLine() (bool, bool) {
	blank := p.lineSize == 0
	p.lineSize = 0
	if blank {
		return p.endEvent()
	}
	if !p.dropping && bytes.HasPrefix(p.line, []byte("data:")) {
		data := p.line[5:]
		if len(data) > 0 && data[0] == ' ' {
			data = data[1:]
		}
		if len(data)+1 > maxEventBytes-len(p.event) {
			p.partial = true
			p.dropping = true
			clear(p.event)
			p.event = nil
		} else {
			p.event = append(p.event, data...)
			p.event = append(p.event, '\n')
		}
	}
	clear(p.line)
	p.line = nil
	return false, false
}

func obj(v any) map[string]any { m, _ := v.(map[string]any); return m }
func present(v any) bool       { s, ok := v.(string); return ok && s != "" }
func integer(v any) *int64 {
	n, ok := v.(json.Number)
	if !ok {
		return nil
	}
	i, err := n.Int64()
	if err != nil || i < 0 {
		return nil
	}
	return &i
}
func numeric(v any) *float64 {
	n, ok := v.(json.Number)
	if !ok {
		return nil
	}
	f, err := n.Float64()
	if err != nil || f < 0 || math.IsNaN(f) || math.IsInf(f, 0) {
		return nil
	}
	return &f
}
func mergeInt(dst **int64, v any) {
	if n := integer(v); n != nil {
		*dst = n
	}
}
func mergeFloat(dst **float64, v any) {
	if n := numeric(v); n != nil {
		*dst = n
	}
}

// decodeEventRoot decodes exactly one JSON object from data with UseNumber and
// verifies there is no trailing JSON. On any decode failure, a nil root, or
// trailing content the event is partial and a nil root is returned.
func decodeEventRoot(data []byte) (root map[string]any, partial bool) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if dec.Decode(&root) != nil || root == nil {
		return nil, true
	}
	var extra any
	if dec.Decode(&extra) != io.EOF {
		return nil, true
	}
	return root, false
}

// mergeUsage merges a decoded usage object into p.usage. It marks recognized
// and preserves any earlier valid value when a later field is invalid.
func (p *metadataParser) mergeUsage(u map[string]any) {
	p.recognized = true
	mergeInt(&p.usage.PromptTokens, u["prompt_tokens"])
	mergeInt(&p.usage.CompletionTokens, u["completion_tokens"])
	mergeInt(&p.usage.TotalTokens, u["total_tokens"])
	mergeInt(&p.usage.CachedTokens, obj(u["prompt_tokens_details"])["cached_tokens"])
	mergeInt(&p.usage.ReasoningTokens, obj(u["completion_tokens_details"])["reasoning_tokens"])
}

// mergeTimings merges a decoded timings object into p.timings. It merges
// numeric values but does not itself mark recognized.
func (p *metadataParser) mergeTimings(t map[string]any) {
	mergeFloat(&p.timings.PromptMS, t["prompt_ms"])
	mergeFloat(&p.timings.PredictedMS, t["predicted_ms"])
	mergeInt(&p.timings.PromptN, t["prompt_n"])
	mergeInt(&p.timings.PredictedN, t["predicted_n"])
}

// deltaContent reports whether a streamed choice delta carries answer-bearing
// content (content/text). It returns true only when meaningful AND answer.
func deltaContent(delta, choice map[string]any) bool {
	return present(delta["content"]) || present(choice["text"])
}

// deltaReasoning reports whether a streamed choice delta carries reasoning.
func deltaReasoning(delta map[string]any) bool {
	return present(delta["reasoning_content"]) || present(delta["reasoning"])
}

// deltaToolCalls reports whether a streamed choice delta carries tool calls.
func deltaToolCalls(delta map[string]any) bool {
	if tools, ok := delta["tool_calls"].([]any); ok {
		for _, raw := range tools {
			fn := obj(obj(raw)["function"])
			if present(fn["name"]) || present(fn["arguments"]) {
				return true
			}
		}
	}
	return false
}

// deltaFunctionCall reports whether a streamed choice delta carries a function call.
func deltaFunctionCall(delta map[string]any) bool {
	fn := obj(delta["function_call"])
	return present(fn["name"]) || present(fn["arguments"])
}

// parseChoices inspects the choices array of a decoded event. It marks
// recognized, applies terminal/truncated finish_reason handling, and, for
// streamed events, inspects content/reasoning/tool/function-call deltas to
// return (meaningful, answer). It preserves the return pair order.
func (p *metadataParser) parseChoices(choices []any, streamed bool) (meaningful, answer bool) {
	p.recognized = true
	for _, raw := range choices {
		choice := obj(raw)
		p.parseFinishReason(choice, streamed)
		deltaMeaningful, deltaAnswer := parseChoiceDelta(choice, streamed)
		meaningful = meaningful || deltaMeaningful
		answer = answer || deltaAnswer
	}
	return
}

func (p *metadataParser) parseFinishReason(choice map[string]any, streamed bool) {
	reason, ok := choice["finish_reason"].(string)
	if !ok || !recognizedFinishReason(reason) {
		return
	}
	// A choice may finish before other choices or a terminal usage event.
	// SSE completion requires its explicit [DONE] boundary.
	if !streamed {
		p.terminal = true
	}
	p.usage.FinishReason = reason
	p.truncated = p.truncated || reason == "length"
}

func recognizedFinishReason(reason string) bool {
	switch reason {
	case "stop", "length", "tool_calls", "function_call", "content_filter":
		return true
	default:
		return false
	}
}

func parseChoiceDelta(choice map[string]any, streamed bool) (meaningful, answer bool) {
	if !streamed {
		return false, false
	}
	delta := obj(choice["delta"])
	answer = deltaContent(delta, choice)
	return answer || deltaReasoning(delta) || deltaToolCalls(delta) || deltaFunctionCall(delta), answer
}

func (p *metadataParser) parse(data []byte, streamed bool) (meaningful, answer bool) {
	root, partial := decodeEventRoot(data)
	if partial {
		p.partial = true
		return
	}
	if root["error"] != nil {
		p.partial = true
	}
	if u := obj(root["usage"]); u != nil {
		p.mergeUsage(u)
	}
	if t := obj(root["timings"]); t != nil {
		p.mergeTimings(t)
	}
	if choices, ok := root["choices"].([]any); ok {
		meaningful, answer = p.parseChoices(choices, streamed)
	}
	return
}

func (p *metadataParser) finish() {
	if p.mode == "json" && !p.partial && len(p.body) > 0 {
		p.parse(p.body, false)
	}
	if p.mode == "sse" && (p.lineSize > 0 || len(p.event) > 0 || p.dropping) {
		p.partial = true
	}
}
func (p *metadataParser) release() {
	clear(p.line)
	clear(p.event)
	clear(p.body)
	p.line = nil
	p.event = nil
	p.body = nil
	p.lineSize = 0
	p.eventSize = 0
}

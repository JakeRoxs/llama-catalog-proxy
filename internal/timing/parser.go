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
	if p.mode != "sse" {
		return
	}
	for _, c := range chunk {
		if p.previousCR {
			p.previousCR = false
			if c == '\n' {
				continue
			}
		}
		if c == '\r' || c == '\n' {
			m, a := p.endLine()
			meaningful = meaningful || m
			answer = answer || a
			if c == '\r' {
				p.previousCR = true
			}
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

func (p *metadataParser) endLine() (bool, bool) {
	blank := p.lineSize == 0
	p.lineSize = 0
	if blank {
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

func (p *metadataParser) parse(data []byte, streamed bool) (meaningful, answer bool) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var root map[string]any
	if dec.Decode(&root) != nil || root == nil {
		p.partial = true
		return
	}
	var extra any
	if dec.Decode(&extra) != io.EOF {
		p.partial = true
		return
	}
	if root["error"] != nil {
		p.partial = true
	}
	if u := obj(root["usage"]); u != nil {
		p.recognized = true
		mergeInt(&p.usage.PromptTokens, u["prompt_tokens"])
		mergeInt(&p.usage.CompletionTokens, u["completion_tokens"])
		mergeInt(&p.usage.TotalTokens, u["total_tokens"])
		mergeInt(&p.usage.CachedTokens, obj(u["prompt_tokens_details"])["cached_tokens"])
		mergeInt(&p.usage.ReasoningTokens, obj(u["completion_tokens_details"])["reasoning_tokens"])
	}
	if t := obj(root["timings"]); t != nil {
		mergeFloat(&p.timings.PromptMS, t["prompt_ms"])
		mergeFloat(&p.timings.PredictedMS, t["predicted_ms"])
		mergeInt(&p.timings.PromptN, t["prompt_n"])
		mergeInt(&p.timings.PredictedN, t["predicted_n"])
	}
	if choices, ok := root["choices"].([]any); ok {
		p.recognized = true
		for _, raw := range choices {
			choice := obj(raw)
			if reason, ok := choice["finish_reason"].(string); ok {
				switch reason {
				case "stop", "length", "tool_calls", "function_call", "content_filter":
					// A choice may finish before other choices or a terminal usage
					// event. SSE completion requires its explicit [DONE] boundary.
					if !streamed {
						p.terminal = true
					}
					p.usage.FinishReason = reason
					if reason == "length" {
						p.truncated = true
					}
				}
			}
			if streamed {
				delta := obj(choice["delta"])
				if present(delta["content"]) || present(choice["text"]) {
					meaningful = true
					answer = true
				}
				if present(delta["reasoning_content"]) || present(delta["reasoning"]) {
					meaningful = true
				}
				if tools, ok := delta["tool_calls"].([]any); ok {
					for _, t := range tools {
						fn := obj(obj(t)["function"])
						if present(fn["name"]) || present(fn["arguments"]) {
							meaningful = true
						}
					}
				}
				fn := obj(delta["function_call"])
				if present(fn["name"]) || present(fn["arguments"]) {
					meaningful = true
				}
			}
		}
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

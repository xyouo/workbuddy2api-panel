package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// chatSSEBridge consumes the Chat Completions byte stream synchronously.  It
// deliberately has no goroutine or unbounded response buffer: a slow client
// applies backpressure to the upstream write, and request cancellation remains
// on the original request context.
type chatSSEBridge struct {
	dst       http.ResponseWriter
	header    http.Header
	status    int
	line      bytes.Buffer
	errorBody bytes.Buffer
	onData    func(string)
}

func (b *chatSSEBridge) Header() http.Header {
	if b.header == nil {
		b.header = make(http.Header)
	}
	return b.header
}
func (b *chatSSEBridge) WriteHeader(n int) {
	if b.status == 0 {
		b.status = n
	}
}
func (b *chatSSEBridge) Write(p []byte) (int, error) {
	if b.status == 0 {
		b.status = http.StatusOK
	}
	if b.status != http.StatusOK {
		_, _ = b.errorBody.Write(p)
		return len(p), nil
	}
	_, _ = b.line.Write(p)
	for {
		s := b.line.String()
		i := strings.IndexByte(s, '\n')
		if i < 0 {
			break
		}
		line := strings.TrimSuffix(s[:i], "\r")
		rest := append([]byte(nil), b.line.Bytes()[i+1:]...)
		b.line.Reset()
		_, _ = b.line.Write(rest)
		if strings.HasPrefix(line, "data:") {
			b.onData(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}
	return len(p), nil
}
func (b *chatSSEBridge) Flush() {
	if f, ok := b.dst.(http.Flusher); ok {
		f.Flush()
	}
}

func sseEmit(w http.ResponseWriter, typ string, value any) {
	b, _ := json.Marshal(value)
	fmt.Fprintf(w, "event: %s\ndata: %s\n\n", typ, b)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

type liveChat struct {
	text   strings.Builder
	finish string
	usage  any
	done   bool
	failed string
}

func (s *liveChat) consume(payload string, onText func(string)) {
	if payload == "[DONE]" {
		s.done = true
		return
	}
	var v map[string]any
	if err := json.Unmarshal([]byte(payload), &v); err != nil {
		s.failed = "invalid JSON in upstream event"
		return
	}
	if e := v["error"]; e != nil {
		s.failed = fmt.Sprint(e)
		return
	}
	if v["usage"] != nil {
		s.usage = v["usage"]
	}
	cs, _ := v["choices"].([]any)
	if len(cs) == 0 {
		return
	}
	c, _ := cs[0].(map[string]any)
	if c == nil {
		return
	}
	if f, ok := c["finish_reason"].(string); ok && f != "" {
		s.finish = f
	}
	d, _ := c["delta"].(map[string]any)
	if x, ok := d["content"].(string); ok && x != "" {
		s.text.WriteString(x)
		onText(x)
	}
}

func (h *Handler) streamResponses(w http.ResponseWriter, r *http.Request, chat, in map[string]any, custom map[string]bool) {
	w.Header().Set("Content-Type", "text/event-stream")
	id, itemID := protocolID("resp_"), protocolID("msg_")
	created := timeNowUnix()
	base := map[string]any{"id": id, "object": "response", "created_at": created, "status": "in_progress", "model": in["model"], "output": []any{}, "parallel_tool_calls": true, "error": nil, "incomplete_details": nil}
	sseEmit(w, "response.created", map[string]any{"type": "response.created", "response": base})
	state := &liveChat{}
	started := false
	bridge := &chatSSEBridge{dst: w}
	bridge.onData = func(payload string) {
		state.consume(payload, func(delta string) {
			if !started {
				started = true
				item := map[string]any{"id": itemID, "type": "message", "status": "in_progress", "role": "assistant", "content": []any{}}
				sseEmit(w, "response.output_item.added", map[string]any{"type": "response.output_item.added", "output_index": 0, "item": item})
				sseEmit(w, "response.content_part.added", map[string]any{"type": "response.content_part.added", "item_id": itemID, "output_index": 0, "content_index": 0, "part": map[string]any{"type": "output_text", "text": "", "annotations": []any{}}})
			}
			sseEmit(w, "response.output_text.delta", map[string]any{"type": "response.output_text.delta", "item_id": itemID, "output_index": 0, "content_index": 0, "delta": delta})
		})
	}
	h.runChat(r, chat, bridge)
	if bridge.status != http.StatusOK {
		sseEmit(w, "error", map[string]any{"type": "error", "error": map[string]any{"type": "upstream_error", "message": strings.TrimSpace(bridge.errorBody.String())}})
		return
	}
	if state.failed != "" || !state.done {
		if state.failed == "" {
			state.failed = "upstream stream ended before [DONE]"
		}
		sseEmit(w, "response.failed", map[string]any{"type": "response.failed", "response": map[string]any{"id": id, "status": "failed", "error": map[string]any{"code": "upstream_error", "message": state.failed}}})
		return
	}
	text := state.text.String()
	output := []any{}
	if started {
		item := map[string]any{"id": itemID, "type": "message", "status": "completed", "role": "assistant", "content": []any{map[string]any{"type": "output_text", "text": text, "annotations": []any{}}}}
		sseEmit(w, "response.output_text.done", map[string]any{"type": "response.output_text.done", "item_id": itemID, "output_index": 0, "content_index": 0, "text": text})
		sseEmit(w, "response.output_item.done", map[string]any{"type": "response.output_item.done", "output_index": 0, "item": item})
		output = append(output, item)
	}
	status := "completed"
	var incomplete any
	if state.finish == "length" {
		status = "incomplete"
		incomplete = map[string]any{"reason": "max_output_tokens"}
	}
	resp := map[string]any{"id": id, "object": "response", "created_at": created, "status": status, "model": in["model"], "output": output, "parallel_tool_calls": true, "error": nil, "incomplete_details": incomplete}
	if state.usage != nil {
		resp["usage"] = responsesUsage(state.usage)
	}
	event := "response.completed"
	if status == "incomplete" {
		event = "response.incomplete"
	}
	sseEmit(w, event, map[string]any{"type": event, "response": resp})
}

func (h *Handler) streamAnthropic(w http.ResponseWriter, r *http.Request, chat, in map[string]any) {
	w.Header().Set("Content-Type", "text/event-stream")
	id := protocolID("msg_")
	start := map[string]any{"id": id, "type": "message", "role": "assistant", "model": in["model"], "content": []any{}, "stop_reason": nil, "stop_sequence": nil, "usage": map[string]any{"input_tokens": 0, "output_tokens": 0}}
	sseEmit(w, "message_start", map[string]any{"type": "message_start", "message": start})
	state := &liveChat{}
	started := false
	bridge := &chatSSEBridge{dst: w}
	bridge.onData = func(payload string) {
		state.consume(payload, func(delta string) {
			if !started {
				started = true
				sseEmit(w, "content_block_start", map[string]any{"type": "content_block_start", "index": 0, "content_block": map[string]any{"type": "text", "text": ""}})
			}
			sseEmit(w, "content_block_delta", map[string]any{"type": "content_block_delta", "index": 0, "delta": map[string]any{"type": "text_delta", "text": delta}})
		})
	}
	h.runChat(r, chat, bridge)
	if bridge.status != http.StatusOK || state.failed != "" || !state.done {
		msg := state.failed
		if bridge.status != http.StatusOK {
			msg = strings.TrimSpace(bridge.errorBody.String())
		}
		if msg == "" {
			msg = "upstream stream ended before [DONE]"
		}
		sseEmit(w, "error", map[string]any{"type": "error", "error": map[string]any{"type": "api_error", "message": msg}})
		return
	}
	if started {
		sseEmit(w, "content_block_stop", map[string]any{"type": "content_block_stop", "index": 0})
	}
	reason := "end_turn"
	if state.finish == "length" {
		reason = "max_tokens"
	}
	usage := anthropicUsage(state.usage)
	sseEmit(w, "message_delta", map[string]any{"type": "message_delta", "delta": map[string]any{"stop_reason": reason, "stop_sequence": nil}, "usage": map[string]any{"output_tokens": usage["output_tokens"]}})
	sseEmit(w, "message_stop", map[string]any{"type": "message_stop"})
}

var timeNowUnix = func() int64 { return time.Now().Unix() }

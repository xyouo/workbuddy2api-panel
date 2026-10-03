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

type streamTool struct {
	index     int
	order     int
	id        string
	name      strings.Builder
	arguments strings.Builder
	started   bool
}

type chatStreamParser struct {
	text         strings.Builder
	finish       string
	usage        any
	done         bool
	failed       string
	tools        map[int]*streamTool
	order        []*streamTool
	nextFallback int
	onText       func(string)
	onTool       func(*streamTool, string)
}

func newChatStreamParser() *chatStreamParser { return &chatStreamParser{tools: map[int]*streamTool{}} }

func (s *chatStreamParser) fail(message string) {
	if s.failed == "" {
		s.failed = message
	}
}

func (s *chatStreamParser) toolIndex(raw map[string]any) int {
	if n, ok := raw["index"].(float64); ok && n >= 0 {
		return int(n)
	}
	if id, _ := raw["id"].(string); id != "" {
		for index, tool := range s.tools {
			if tool.id == id {
				return index
			}
		}
	}
	// A continuation without index/id is unambiguous only when one tool exists.
	if len(s.tools) == 1 {
		for index := range s.tools {
			return index
		}
	}
	index := s.nextFallback
	for s.tools[index] != nil {
		index++
	}
	s.nextFallback = index + 1
	return index
}

func (s *chatStreamParser) consume(payload string) {
	if payload == "[DONE]" {
		s.done = true
		return
	}
	var v map[string]any
	if err := json.Unmarshal([]byte(payload), &v); err != nil {
		s.fail("invalid JSON in upstream event")
		return
	}
	if e := v["error"]; e != nil {
		message := "upstream stream error"
		if em, ok := e.(map[string]any); ok {
			if m, ok := em["message"].(string); ok && m != "" {
				message = m
			}
		}
		s.fail(message)
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
		if s.onText != nil {
			s.onText(x)
		}
	}
	if fragments, ok := d["tool_calls"].([]any); ok {
		for _, fragment := range fragments {
			raw, ok := fragment.(map[string]any)
			if !ok {
				s.fail("invalid upstream tool delta")
				continue
			}
			index := s.toolIndex(raw)
			tool := s.tools[index]
			if tool == nil {
				tool = &streamTool{index: index, order: len(s.order)}
				s.tools[index] = tool
				s.order = append(s.order, tool)
			}
			if id, ok := raw["id"].(string); ok && id != "" {
				if tool.id != "" && tool.id != id {
					s.fail("conflicting tool call id")
				} else {
					tool.id = id
				}
			}
			fn, _ := raw["function"].(map[string]any)
			if name, ok := fn["name"].(string); ok {
				tool.name.WriteString(name)
			}
			argDelta, _ := fn["arguments"].(string)
			tool.arguments.WriteString(argDelta)
			if s.onTool != nil {
				s.onTool(tool, argDelta)
			}
		}
	}
}

func (s *chatStreamParser) validateTools(custom map[string]bool) error {
	for _, tool := range s.order {
		if tool.id == "" || tool.name.Len() == 0 {
			return fmt.Errorf("upstream tool call is missing id or name")
		}
		args := tool.arguments.String()
		if args == "" {
			return fmt.Errorf("tool %q has empty arguments", tool.name.String())
		}
		var value any
		if err := json.Unmarshal([]byte(args), &value); err != nil {
			return fmt.Errorf("tool %q has invalid arguments JSON: %w", tool.name.String(), err)
		}
		if custom[tool.name.String()] {
			obj, ok := value.(map[string]any)
			if !ok {
				return fmt.Errorf("custom tool %q arguments must be an object", tool.name.String())
			}
			if _, ok := obj["input"].(string); !ok {
				return fmt.Errorf("custom tool %q arguments require string input", tool.name.String())
			}
		}
	}
	return nil
}

func (h *Handler) streamResponses(w http.ResponseWriter, r *http.Request, chat, in map[string]any, custom map[string]bool) {
	w.Header().Set("Content-Type", "text/event-stream")
	id, itemID := protocolID("resp_"), protocolID("msg_")
	created := timeNowUnix()
	base := map[string]any{"id": id, "object": "response", "created_at": created, "status": "in_progress", "model": in["model"], "output": []any{}, "parallel_tool_calls": true, "error": nil, "incomplete_details": nil}
	sseEmit(w, "response.created", map[string]any{"type": "response.created", "response": base})
	state := newChatStreamParser()
	started := false
	outputOffset := 0
	state.onText = func(delta string) {
		if !started {
			started = true
			outputOffset = 1
			item := map[string]any{"id": itemID, "type": "message", "status": "in_progress", "role": "assistant", "content": []any{}}
			sseEmit(w, "response.output_item.added", map[string]any{"type": "response.output_item.added", "output_index": 0, "item": item})
			sseEmit(w, "response.content_part.added", map[string]any{"type": "response.content_part.added", "item_id": itemID, "output_index": 0, "content_index": 0, "part": map[string]any{"type": "output_text", "text": "", "annotations": []any{}}})
		}
		sseEmit(w, "response.output_text.delta", map[string]any{"type": "response.output_text.delta", "item_id": itemID, "output_index": 0, "content_index": 0, "delta": delta})
	}
	state.onTool = func(tool *streamTool, delta string) {
		name := tool.name.String()
		if name == "" || tool.id == "" {
			return
		}
		index := outputOffset + tool.order
		if !tool.started {
			tool.started = true
			typ := "function_call"
			item := map[string]any{"id": protocolID("fc_"), "type": typ, "status": "in_progress", "call_id": tool.id, "name": name, "arguments": ""}
			if custom[name] {
				typ = "custom_tool_call"
				item["type"] = typ
				delete(item, "arguments")
				item["input"] = ""
			}
			item["id"] = fmt.Sprintf("fc_%s", tool.id)
			sseEmit(w, "response.output_item.added", map[string]any{"type": "response.output_item.added", "output_index": index, "item": item})
		}
		if delta != "" && !custom[name] {
			sseEmit(w, "response.function_call_arguments.delta", map[string]any{"type": "response.function_call_arguments.delta", "item_id": fmt.Sprintf("fc_%s", tool.id), "output_index": index, "delta": delta})
		}
	}
	bridge := &chatSSEBridge{dst: w}
	bridge.onData = func(payload string) {
		state.consume(payload)
	}
	h.runChat(r, chat, bridge)
	if bridge.status != http.StatusOK {
		sseEmit(w, "error", map[string]any{"type": "error", "error": map[string]any{"type": "upstream_error", "message": strings.TrimSpace(bridge.errorBody.String())}})
		return
	}
	if state.failed == "" {
		if err := state.validateTools(custom); err != nil {
			state.failed = err.Error()
		}
	}
	if state.failed != "" || !state.done || state.finish == "" {
		if state.failed == "" {
			state.failed = "upstream stream ended without a finish reason"
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
	for _, tool := range state.order {
		name, args, index := tool.name.String(), tool.arguments.String(), outputOffset+tool.order
		item := map[string]any{"id": fmt.Sprintf("fc_%s", tool.id), "type": "function_call", "status": "completed", "call_id": tool.id, "name": name, "arguments": args}
		if custom[name] {
			var wrapper map[string]any
			_ = json.Unmarshal([]byte(args), &wrapper)
			input := wrapper["input"].(string)
			item["type"] = "custom_tool_call"
			item["input"] = input
			delete(item, "arguments")
			sseEmit(w, "response.custom_tool_call_input.delta", map[string]any{"type": "response.custom_tool_call_input.delta", "item_id": item["id"], "output_index": index, "delta": input})
			sseEmit(w, "response.custom_tool_call_input.done", map[string]any{"type": "response.custom_tool_call_input.done", "item_id": item["id"], "output_index": index, "input": input})
		} else {
			sseEmit(w, "response.function_call_arguments.done", map[string]any{"type": "response.function_call_arguments.done", "item_id": item["id"], "output_index": index, "arguments": args})
		}
		sseEmit(w, "response.output_item.done", map[string]any{"type": "response.output_item.done", "output_index": index, "item": item})
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
	// Chat SSE reports usage in its terminal chunk. Do not fabricate zero input
	// tokens in message_start; the observed values are emitted in message_delta.
	start := map[string]any{"id": id, "type": "message", "role": "assistant", "model": in["model"], "content": []any{}, "stop_reason": nil, "stop_sequence": nil, "usage": map[string]any{}}
	sseEmit(w, "message_start", map[string]any{"type": "message_start", "message": start})
	state := newChatStreamParser()
	started := false
	state.onText = func(delta string) {
		if !started {
			started = true
			sseEmit(w, "content_block_start", map[string]any{"type": "content_block_start", "index": 0, "content_block": map[string]any{"type": "text", "text": ""}})
		}
		sseEmit(w, "content_block_delta", map[string]any{"type": "content_block_delta", "index": 0, "delta": map[string]any{"type": "text_delta", "text": delta}})
	}
	state.onTool = func(tool *streamTool, delta string) {
		if tool.id == "" || tool.name.Len() == 0 {
			return
		}
		index := tool.order
		if started {
			index++
		}
		if !tool.started {
			tool.started = true
			sseEmit(w, "content_block_start", map[string]any{"type": "content_block_start", "index": index, "content_block": map[string]any{"type": "tool_use", "id": tool.id, "name": tool.name.String(), "input": map[string]any{}}})
		}
		if delta != "" {
			sseEmit(w, "content_block_delta", map[string]any{"type": "content_block_delta", "index": index, "delta": map[string]any{"type": "input_json_delta", "partial_json": delta}})
		}
	}
	bridge := &chatSSEBridge{dst: w}
	bridge.onData = state.consume
	h.runChat(r, chat, bridge)
	if state.failed == "" {
		if err := state.validateTools(map[string]bool{}); err != nil {
			state.failed = err.Error()
		}
	}
	if bridge.status != http.StatusOK || state.failed != "" || !state.done || state.finish == "" {
		msg := state.failed
		if bridge.status != http.StatusOK {
			msg = strings.TrimSpace(bridge.errorBody.String())
		}
		if msg == "" {
			msg = "upstream stream ended without a finish reason"
		}
		sseEmit(w, "error", map[string]any{"type": "error", "error": map[string]any{"type": "api_error", "message": msg}})
		return
	}
	if started {
		sseEmit(w, "content_block_stop", map[string]any{"type": "content_block_stop", "index": 0})
	}
	for _, tool := range state.order {
		index := tool.order
		if started {
			index++
		}
		sseEmit(w, "content_block_stop", map[string]any{"type": "content_block_stop", "index": index})
	}
	reason := "end_turn"
	if state.finish == "length" {
		reason = "max_tokens"
	}
	usage := anthropicUsage(state.usage)
	sseEmit(w, "message_delta", map[string]any{"type": "message_delta", "delta": map[string]any{"stop_reason": reason, "stop_sequence": nil}, "usage": map[string]any{"input_tokens": usage["input_tokens"], "output_tokens": usage["output_tokens"]}})
	sseEmit(w, "message_stop", map[string]any{"type": "message_stop"})
}

var timeNowUnix = func() int64 { return time.Now().Unix() }

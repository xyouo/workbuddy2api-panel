package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/linguo2625469/workbuddy2api-panel/internal/reqlog"
)

// chatSSEBridge consumes the Chat Completions byte stream synchronously.  It
// deliberately has no goroutine or unbounded response buffer: a slow client
// applies backpressure to the upstream write, and request cancellation remains
// on the original request context.
type chatSSEBridge struct {
	header    http.Header
	status    int
	line      bytes.Buffer
	errorBody bytes.Buffer
	onData    func(string) error
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
			if err := b.onData(strings.TrimSpace(strings.TrimPrefix(line, "data:"))); err != nil {
				return 0, err
			}
		}
	}
	return len(p), nil
}

// Each converted event flushes itself; flushing the bridge must not open an
// otherwise empty or failed HTTP response before its status is known.
func (b *chatSSEBridge) Flush() {}

type adapterEventWriter struct {
	w   http.ResponseWriter
	err error
}

func (e *adapterEventWriter) emit(typ string, value any) {
	if e.err != nil {
		return
	}
	b, err := json.Marshal(value)
	if err != nil {
		e.err = err
		return
	}
	_, e.err = fmt.Fprintf(e.w, "event: %s\ndata: %s\n\n", typ, b)
	if e.err != nil {
		return
	}
	if f, ok := e.w.(http.Flusher); ok {
		f.Flush()
	}
}

type streamTool struct {
	index      int
	id         string
	name       strings.Builder
	arguments  strings.Builder
	started    bool
	eventIndex int
}

type chatStreamParser struct {
	text   strings.Builder
	finish string
	usage  any
	done   bool
	failed string
	tools  map[int]*streamTool
	byID   map[string]*streamTool
	order  []*streamTool
	onText func(string)
	onTool func(*streamTool, string)
}

func newChatStreamParser() *chatStreamParser {
	return &chatStreamParser{tools: map[int]*streamTool{}, byID: map[string]*streamTool{}}
}

func (s *chatStreamParser) fail(message string) {
	if s.failed == "" {
		s.failed = message
	}
}

// Explicit indexes and call IDs are aliases, not separate tool records.
// Fallback-only tools never occupy an explicit index that may appear later.
func (s *chatStreamParser) toolForDelta(raw map[string]any) *streamTool {
	id, _ := raw["id"].(string)
	n, indexed := raw["index"].(float64)
	indexed = indexed && n >= 0 && n == float64(int(n))
	var tool *streamTool
	if indexed {
		tool = s.tools[int(n)]
	}
	if known := s.byID[id]; id != "" && known != nil {
		if tool != nil && tool != known {
			s.fail("conflicting tool call identity")
			return nil
		}
		tool = known
	}
	if !indexed && id == "" {
		if len(s.order) > 1 {
			s.fail("tool delta without index or id is ambiguous")
			return nil
		}
		if len(s.order) == 1 {
			tool = s.order[0]
		}
	}
	if tool == nil {
		tool = &streamTool{index: len(s.order), eventIndex: -1}
		s.order = append(s.order, tool)
	}
	if id != "" {
		if tool.id != "" && tool.id != id {
			s.fail("conflicting tool call id")
			return nil
		}
		tool.id = id
		s.byID[id] = tool
	}
	if indexed {
		s.tools[int(n)] = tool
		tool.index = int(n)
	}
	return tool
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
			tool := s.toolForDelta(raw)
			if tool == nil {
				continue
			}
			fn, _ := raw["function"].(map[string]any)
			if name, ok := fn["name"].(string); ok {
				if tool.started && name != "" {
					s.fail("tool name continued after its output item was published")
					continue
				}
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
		name := tool.name.String()
		if _, err := decodeToolArguments(tool.id, name, tool.arguments.String(), custom[name]); err != nil {
			return err
		}
	}
	return nil
}

func declaredToolNames(chat map[string]any) map[string]bool {
	result := map[string]bool{}
	tools, _ := chat["tools"].([]any)
	for _, raw := range tools {
		tool, _ := raw.(map[string]any)
		fn, _ := tool["function"].(map[string]any)
		if name, _ := fn["name"].(string); name != "" {
			result[name] = true
		}
	}
	return result
}

func (h *Handler) streamResponses(w http.ResponseWriter, r *http.Request, chat, in map[string]any, custom map[string]bool) {
	events := &adapterEventWriter{w: w}
	defer func() {
		if events.err != nil {
			setAdapterOutcome(r, http.StatusOK, reqlog.OutcomeInterrupted)
		}
	}()
	id, itemID := protocolID("resp_"), protocolID("msg_")
	created := time.Now().Unix()
	base := map[string]any{"id": id, "object": "response", "created_at": created, "status": "in_progress", "model": in["model"], "output": []any{}, "parallel_tool_calls": true, "error": nil, "incomplete_details": nil}
	state := newChatStreamParser()
	knownTools := declaredToolNames(chat)
	textStarted := false
	textIndex := -1
	nextIndex := 0
	state.onText = func(delta string) {
		if !textStarted {
			textStarted = true
			textIndex = nextIndex
			nextIndex++
			item := map[string]any{"id": itemID, "type": "message", "status": "in_progress", "role": "assistant", "content": []any{}}
			events.emit("response.output_item.added", map[string]any{"type": "response.output_item.added", "output_index": textIndex, "item": item})
			events.emit("response.content_part.added", map[string]any{"type": "response.content_part.added", "item_id": itemID, "output_index": textIndex, "content_index": 0, "part": map[string]any{"type": "output_text", "text": "", "annotations": []any{}}})
		}
		events.emit("response.output_text.delta", map[string]any{"type": "response.output_text.delta", "item_id": itemID, "output_index": textIndex, "content_index": 0, "delta": delta})
	}
	state.onTool = func(tool *streamTool, delta string) {
		name := tool.name.String()
		// Name-only fragments are buffered. The first argument fragment is the
		// earliest point at which Chat SSE has finished spelling the name.
		if name == "" || tool.id == "" || len(knownTools) > 0 && !knownTools[name] || delta == "" && tool.arguments.Len() == 0 {
			return
		}
		emitDelta := delta
		if !tool.started {
			tool.eventIndex = nextIndex
			nextIndex++
			tool.started = true
			emitDelta = tool.arguments.String()
			typ := "function_call"
			item := map[string]any{"id": fmt.Sprintf("fc_%s", tool.id), "type": typ, "status": "in_progress", "call_id": tool.id, "name": name, "arguments": ""}
			if custom[name] {
				typ = "custom_tool_call"
				item["type"] = typ
				delete(item, "arguments")
				item["input"] = ""
			}
			events.emit("response.output_item.added", map[string]any{"type": "response.output_item.added", "output_index": tool.eventIndex, "item": item})
		}
		if emitDelta != "" && !custom[name] {
			events.emit("response.function_call_arguments.delta", map[string]any{"type": "response.function_call_arguments.delta", "item_id": fmt.Sprintf("fc_%s", tool.id), "output_index": tool.eventIndex, "delta": emitDelta})
		}
	}
	bridge := &chatSSEBridge{}
	started := false
	bridge.onData = func(payload string) error {
		if !started {
			started = true
			copyAdapterHeaders(w, bridge.Header())
			w.Header().Set("Content-Type", "text/event-stream")
			events.emit("response.created", map[string]any{"type": "response.created", "response": base})
		}
		if events.err != nil {
			return events.err
		}
		state.consume(payload)
		return events.err
	}
	h.runChat(r, chat, bridge)
	if events.err != nil {
		return
	}
	if bridge.status != http.StatusOK {
		forwardAdapterError(w, bridge.status, bridge.Header(), bridge.errorBody.Bytes(), false)
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
		setAdapterOutcome(r, http.StatusBadGateway, reqlog.OutcomeStreamError)
		events.emit("response.failed", map[string]any{"type": "response.failed", "response": map[string]any{"id": id, "status": "failed", "error": map[string]any{"code": "upstream_error", "message": state.failed}}})
		return
	}
	text := state.text.String()
	outputByIndex := map[int]any{}
	if textStarted {
		item := map[string]any{"id": itemID, "type": "message", "status": "completed", "role": "assistant", "content": []any{map[string]any{"type": "output_text", "text": text, "annotations": []any{}}}}
		events.emit("response.output_text.done", map[string]any{"type": "response.output_text.done", "item_id": itemID, "output_index": textIndex, "content_index": 0, "text": text})
		events.emit("response.output_item.done", map[string]any{"type": "response.output_item.done", "output_index": textIndex, "item": item})
		outputByIndex[textIndex] = item
	}
	for _, tool := range state.order {
		name, args := tool.name.String(), tool.arguments.String()
		if tool.eventIndex < 0 {
			tool.eventIndex = nextIndex
			nextIndex++
		}
		index := tool.eventIndex
		item := map[string]any{"id": fmt.Sprintf("fc_%s", tool.id), "type": "function_call", "status": "completed", "call_id": tool.id, "name": name, "arguments": args}
		if custom[name] {
			var wrapper map[string]any
			_ = json.Unmarshal([]byte(args), &wrapper)
			input := wrapper["input"].(string)
			item["type"] = "custom_tool_call"
			item["input"] = input
			delete(item, "arguments")
			events.emit("response.custom_tool_call_input.delta", map[string]any{"type": "response.custom_tool_call_input.delta", "item_id": item["id"], "output_index": index, "delta": input})
			events.emit("response.custom_tool_call_input.done", map[string]any{"type": "response.custom_tool_call_input.done", "item_id": item["id"], "output_index": index, "input": input})
		} else {
			events.emit("response.function_call_arguments.done", map[string]any{"type": "response.function_call_arguments.done", "item_id": item["id"], "output_index": index, "arguments": args})
		}
		events.emit("response.output_item.done", map[string]any{"type": "response.output_item.done", "output_index": index, "item": item})
		outputByIndex[index] = item
	}
	output := make([]any, 0, len(outputByIndex))
	for i := 0; i < nextIndex; i++ {
		if item, ok := outputByIndex[i]; ok {
			output = append(output, item)
		}
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
	events.emit(event, map[string]any{"type": event, "response": resp})
}

func (h *Handler) streamAnthropic(w http.ResponseWriter, r *http.Request, chat, in map[string]any) {
	events := &adapterEventWriter{w: w}
	defer func() {
		if events.err != nil {
			setAdapterOutcome(r, http.StatusOK, reqlog.OutcomeInterrupted)
		}
	}()
	id := protocolID("msg_")
	// Chat SSE reports usage in its terminal chunk. Do not fabricate zero input
	// tokens in message_start; the observed values are emitted in message_delta.
	start := map[string]any{"id": id, "type": "message", "role": "assistant", "model": in["model"], "content": []any{}, "stop_reason": nil, "stop_sequence": nil, "usage": map[string]any{}}
	state := newChatStreamParser()
	knownTools := declaredToolNames(chat)
	textStarted := false
	textIndex := -1
	nextIndex := 0
	state.onText = func(delta string) {
		if !textStarted {
			textStarted = true
			textIndex = nextIndex
			nextIndex++
			events.emit("content_block_start", map[string]any{"type": "content_block_start", "index": textIndex, "content_block": map[string]any{"type": "text", "text": ""}})
		}
		events.emit("content_block_delta", map[string]any{"type": "content_block_delta", "index": textIndex, "delta": map[string]any{"type": "text_delta", "text": delta}})
	}
	state.onTool = func(tool *streamTool, delta string) {
		name := tool.name.String()
		if tool.id == "" || name == "" || len(knownTools) > 0 && !knownTools[name] || delta == "" && tool.arguments.Len() == 0 {
			return
		}
		emitDelta := delta
		if !tool.started {
			tool.eventIndex = nextIndex
			nextIndex++
			tool.started = true
			emitDelta = tool.arguments.String()
			events.emit("content_block_start", map[string]any{"type": "content_block_start", "index": tool.eventIndex, "content_block": map[string]any{"type": "tool_use", "id": tool.id, "name": tool.name.String(), "input": map[string]any{}}})
		}
		if emitDelta != "" {
			events.emit("content_block_delta", map[string]any{"type": "content_block_delta", "index": tool.eventIndex, "delta": map[string]any{"type": "input_json_delta", "partial_json": emitDelta}})
		}
	}
	bridge := &chatSSEBridge{}
	started := false
	bridge.onData = func(payload string) error {
		if !started {
			started = true
			copyAdapterHeaders(w, bridge.Header())
			w.Header().Set("Content-Type", "text/event-stream")
			events.emit("message_start", map[string]any{"type": "message_start", "message": start})
		}
		if events.err != nil {
			return events.err
		}
		state.consume(payload)
		return events.err
	}
	h.runChat(r, chat, bridge)
	if events.err != nil {
		return
	}
	if state.failed == "" {
		if err := state.validateTools(map[string]bool{}); err != nil {
			state.failed = err.Error()
		}
	}
	if bridge.status != http.StatusOK {
		forwardAdapterError(w, bridge.status, bridge.Header(), bridge.errorBody.Bytes(), true)
		return
	}
	if state.failed != "" || !state.done || state.finish == "" {
		msg := state.failed
		if msg == "" {
			msg = "upstream stream ended without a finish reason"
		}
		setAdapterOutcome(r, http.StatusBadGateway, reqlog.OutcomeStreamError)
		events.emit("error", map[string]any{"type": "error", "error": map[string]any{"type": "api_error", "message": msg}})
		return
	}
	if textStarted {
		events.emit("content_block_stop", map[string]any{"type": "content_block_stop", "index": textIndex})
	}
	for _, tool := range state.order {
		if tool.eventIndex < 0 {
			tool.eventIndex = nextIndex
			nextIndex++
		}
		events.emit("content_block_stop", map[string]any{"type": "content_block_stop", "index": tool.eventIndex})
	}
	reason := anthropicStopReason(state.finish, len(state.order) > 0)
	usage := anthropicUsage(state.usage)
	events.emit("message_delta", map[string]any{"type": "message_delta", "delta": map[string]any{"stop_reason": reason, "stop_sequence": nil}, "usage": map[string]any{"input_tokens": usage["input_tokens"], "output_tokens": usage["output_tokens"]}})
	events.emit("message_stop", map[string]any{"type": "message_stop"})
}

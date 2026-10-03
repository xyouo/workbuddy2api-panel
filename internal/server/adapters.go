package server

// This file is an intentionally small, stateless protocol boundary. WorkBuddy's
// upstream speaks Chat Completions; these handlers translate input and output
// without creating a second upstream request or a second usage-accounting path.

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

func protocolID(prefix string) string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return prefix + hex.EncodeToString(b)
}

type responseCapture struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (c *responseCapture) Header() http.Header {
	if c.header == nil {
		c.header = make(http.Header)
	}
	return c.header
}
func (c *responseCapture) WriteHeader(n int) {
	if c.status == 0 {
		c.status = n
	}
}
func (c *responseCapture) Write(p []byte) (int, error) {
	if c.status == 0 {
		c.status = http.StatusOK
	}
	return c.body.Write(p)
}

func (h *Handler) runChat(r *http.Request, body map[string]any, w http.ResponseWriter) {
	b, _ := json.Marshal(body)
	cr := r.Clone(r.Context())
	cr.URL.Path = "/v1/chat/completions"
	cr.Body = io.NopCloser(bytes.NewReader(b))
	h.chatCompletions(w, cr)
}

func adapterError(w http.ResponseWriter, status int, kind, msg string) {
	writeOpenAIError(w, status, kind, msg)
}
func unsupported(obj map[string]any, keys ...string) (string, bool) {
	for _, k := range keys {
		if v, ok := obj[k]; ok && v != nil && v != false {
			return k, true
		}
	}
	return "", false
}

func responseContent(v any, output bool) any {
	if s, ok := v.(string); ok {
		return s
	}
	a, ok := v.([]any)
	if !ok {
		return v
	}
	out := make([]any, 0, len(a))
	for _, raw := range a {
		p, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		t, _ := p["type"].(string)
		switch t {
		case "input_text", "output_text", "text":
			out = append(out, map[string]any{"type": "text", "text": p["text"]})
		case "input_image":
			if u, ok := p["image_url"].(string); ok {
				q := map[string]any{"type": "image_url", "image_url": map[string]any{"url": u}}
				if d := p["detail"]; d != nil {
					q["image_url"].(map[string]any)["detail"] = d
				}
				out = append(out, q)
			}
		default:
			if output {
				continue
			}
		}
	}
	return out
}

func responsesToChat(in map[string]any) (map[string]any, error) {
	if k, ok := unsupported(in, "previous_response_id", "background", "conversation", "prompt"); ok {
		return nil, fmt.Errorf("%s is not supported; send complete history on every request", k)
	}
	if inc, ok := in["include"].([]any); ok && len(inc) > 0 {
		return nil, fmt.Errorf("include (including encrypted reasoning) is not supported")
	}
	msgs := []any{}
	if ins, ok := in["instructions"].(string); ok && ins != "" {
		msgs = append(msgs, map[string]any{"role": "developer", "content": ins})
	}
	if arr, ok := in["instructions"].([]any); ok {
		for _, x := range arr {
			if m, ok := x.(map[string]any); ok {
				msgs = append(msgs, map[string]any{"role": m["role"], "content": responseContent(m["content"], false)})
			}
		}
	}
	switch input := in["input"].(type) {
	case string:
		msgs = append(msgs, map[string]any{"role": "user", "content": input})
	case []any:
		for _, x := range input {
			m, ok := x.(map[string]any)
			if !ok {
				continue
			}
			typ, _ := m["type"].(string)
			switch typ {
			case "message", "":
				msgs = append(msgs, map[string]any{"role": m["role"], "content": responseContent(m["content"], false)})
			case "function_call", "custom_tool_call":
				args := m["arguments"]
				if typ == "custom_tool_call" {
					wrapped, _ := json.Marshal(map[string]any{"input": m["input"]})
					args = string(wrapped)
				}
				msgs = append(msgs, map[string]any{"role": "assistant", "tool_calls": []any{map[string]any{"id": m["call_id"], "type": "function", "function": map[string]any{"name": m["name"], "arguments": args}}}})
			case "function_call_output", "custom_tool_call_output":
				msgs = append(msgs, map[string]any{"role": "tool", "tool_call_id": m["call_id"], "content": m["output"]})
			default:
				return nil, fmt.Errorf("input item type %q is not supported", typ)
			}
		}
	}
	out := map[string]any{"model": in["model"], "messages": msgs, "stream": in["stream"]}
	if v := in["max_output_tokens"]; v != nil {
		out["max_completion_tokens"] = v
	}
	if v := in["temperature"]; v != nil {
		out["temperature"] = v
	}
	if v := in["top_p"]; v != nil {
		out["top_p"] = v
	}
	if rs, ok := in["reasoning"].(map[string]any); ok {
		out["reasoning_effort"] = rs["effort"]
	}
	if ts, ok := in["tools"].([]any); ok {
		tools := []any{}
		for _, x := range ts {
			t, _ := x.(map[string]any)
			typ, _ := t["type"].(string)
			if typ != "function" && typ != "custom" {
				return nil, fmt.Errorf("hosted tool %q is not supported", typ)
			}
			name, _ := t["name"].(string)
			desc, _ := t["description"].(string)
			params := t["parameters"]
			if typ == "custom" {
				f, _ := t["format"].(map[string]any)
				if f != nil && f["type"] != "text" && f["type"] != "grammar" {
					return nil, fmt.Errorf("custom tool %q format %q is not supported", name, f["type"])
				}
				if f != nil && f["type"] == "grammar" {
					syntax, _ := f["syntax"].(string)
					definition, _ := f["definition"].(string)
					if syntax == "" || definition == "" {
						return nil, fmt.Errorf("custom tool %q grammar requires syntax and definition", name)
					}
					desc += "\nThe free-form input must satisfy this " + syntax + " grammar:\n" + definition
				}
				params = map[string]any{"type": "object", "properties": map[string]any{"input": map[string]any{"type": "string", "description": "Free-form input for the custom tool. " + desc}}, "required": []string{"input"}}
			}
			tools = append(tools, map[string]any{"type": "function", "function": map[string]any{"name": name, "description": desc, "parameters": params}})
		}
		out["tools"] = tools
	}
	if v := in["tool_choice"]; v != nil {
		switch x := v.(type) {
		case string:
			out["tool_choice"] = x
		case map[string]any:
			typ, _ := x["type"].(string)
			if typ != "function" && typ != "custom" {
				return nil, fmt.Errorf("tool_choice type %q is not supported", typ)
			}
			name, _ := x["name"].(string)
			if name == "" {
				return nil, fmt.Errorf("tool_choice name is required")
			}
			out["tool_choice"] = map[string]any{"type": "function", "function": map[string]any{"name": name}}
		default:
			return nil, fmt.Errorf("tool_choice has invalid type")
		}
	}
	return out, nil
}

func chatObject(rr *responseCapture) (map[string]any, error) {
	var v map[string]any
	if err := json.Unmarshal(rr.body.Bytes(), &v); err != nil {
		return nil, err
	}
	return v, nil
}
func chatOutput(chat map[string]any, custom map[string]bool) ([]any, string) {
	out := []any{}
	stop := "completed"
	choices, _ := chat["choices"].([]any)
	if len(choices) == 0 {
		return out, stop
	}
	c, _ := choices[0].(map[string]any)
	finish, _ := c["finish_reason"].(string)
	if finish == "length" {
		stop = "incomplete"
	}
	m, _ := c["message"].(map[string]any)
	if m == nil {
		m, _ = c["delta"].(map[string]any)
	}
	if s, _ := m["content"].(string); s != "" {
		out = append(out, map[string]any{"id": protocolID("msg_"), "type": "message", "status": "completed", "role": "assistant", "content": []any{map[string]any{"type": "output_text", "text": s, "annotations": []any{}}}})
	}
	if a, ok := m["tool_calls"].([]any); ok {
		for _, x := range a {
			t, _ := x.(map[string]any)
			f, _ := t["function"].(map[string]any)
			name, _ := f["name"].(string)
			args, _ := f["arguments"].(string)
			typ := "function_call"
			item := map[string]any{"id": protocolID("fc_"), "type": typ, "status": "completed", "call_id": t["id"], "name": name, "arguments": args}
			if custom[name] {
				item["type"] = "custom_tool_call"
				var p map[string]any
				if json.Unmarshal([]byte(args), &p) == nil {
					item["input"] = p["input"]
				}
				delete(item, "arguments")
			}
			out = append(out, item)
			if finish != "length" {
				stop = "completed"
			}
		}
	}
	return out, stop
}

func (h *Handler) responses(w http.ResponseWriter, r *http.Request) {
	var in map[string]any
	if json.NewDecoder(r.Body).Decode(&in) != nil {
		adapterError(w, 400, "invalid_request_error", "invalid JSON")
		return
	}
	chat, err := responsesToChat(in)
	if err != nil {
		adapterError(w, 400, "unsupported_parameter", err.Error())
		return
	}
	custom := map[string]bool{}
	if ts, ok := in["tools"].([]any); ok {
		for _, x := range ts {
			t, _ := x.(map[string]any)
			if t["type"] == "custom" {
				custom[fmt.Sprint(t["name"])] = true
			}
		}
	}
	stream, _ := in["stream"].(bool)
	if stream {
		h.streamResponses(w, r, chat, in, custom)
		return
	}
	rr := &responseCapture{}
	h.runChat(r, chat, rr)
	if rr.status != 200 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(rr.status)
		_, _ = w.Write(rr.body.Bytes())
		return
	}
	id := protocolID("resp_")
	created := time.Now().Unix()
	obj, err := chatObject(rr)
	if err != nil {
		adapterError(w, 502, "upstream_error", "invalid upstream response")
		return
	}
	output, _ := chatOutput(obj, custom)
	_, state := chatOutput(obj, custom)
	var incomplete any
	if state == "incomplete" {
		incomplete = map[string]any{"reason": "max_output_tokens"}
	}
	resp := map[string]any{"id": id, "object": "response", "created_at": created, "status": state, "model": in["model"], "output": output, "parallel_tool_calls": true, "error": nil, "incomplete_details": incomplete}
	if u := obj["usage"]; u != nil {
		resp["usage"] = responsesUsage(u)
	}
	writeJSON(w, 200, resp)
}

func cloneMap(m map[string]any) map[string]any {
	o := map[string]any{}
	for k, v := range m {
		o[k] = v
	}
	return o
}
func responsesUsage(v any) any {
	u, _ := v.(map[string]any)
	return map[string]any{"input_tokens": u["prompt_tokens"], "output_tokens": u["completion_tokens"], "total_tokens": u["total_tokens"]}
}
func aggregateChatSSE(s string) map[string]any {
	text := ""
	tools := map[int]map[string]any{}
	var usage any
	finish := ""
	done := false
	var streamErr any
	nextFallback := 0
	for _, line := range strings.Split(s, "\n") {
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data: "))
		if payload == "[DONE]" {
			done = true
			continue
		}
		var v map[string]any
		if json.Unmarshal([]byte(payload), &v) != nil {
			streamErr = map[string]any{"message": "invalid upstream SSE JSON"}
			continue
		}
		if v["error"] != nil {
			streamErr = v["error"]
			continue
		}
		if v["usage"] != nil {
			usage = v["usage"]
		}
		cs, _ := v["choices"].([]any)
		if len(cs) == 0 {
			continue
		}
		c, _ := cs[0].(map[string]any)
		if x, ok := c["finish_reason"].(string); ok && x != "" {
			finish = x
		}
		d, _ := c["delta"].(map[string]any)
		if x, ok := d["content"].(string); ok {
			text += x
		}
		if a, ok := d["tool_calls"].([]any); ok {
			for _, raw := range a {
				t, ok := raw.(map[string]any)
				if !ok {
					streamErr = map[string]any{"message": "invalid tool delta"}
					continue
				}
				idx := -1
				switch n := t["index"].(type) {
				case float64:
					idx = int(n)
				case json.Number:
					q, _ := n.Int64()
					idx = int(q)
				}
				if idx < 0 {
					if id, _ := t["id"].(string); id != "" {
						for k, z := range tools {
							if z["id"] == id {
								idx = k
								break
							}
						}
					}
					if idx < 0 {
						idx = nextFallback
						for tools[idx] != nil {
							idx++
						}
						nextFallback = idx + 1
					}
				}
				z := tools[idx]
				if z == nil {
					z = map[string]any{"function": map[string]any{}}
					tools[idx] = z
				}
				if x, ok := t["id"].(string); ok {
					z["id"] = x
				}
				f, _ := t["function"].(map[string]any)
				zf, _ := z["function"].(map[string]any)
				if zf == nil {
					zf = map[string]any{}
					z["function"] = zf
				}
				if x, ok := f["name"].(string); ok {
					old, _ := zf["name"].(string)
					zf["name"] = old + x
				}
				if x, ok := f["arguments"].(string); ok {
					old, _ := zf["arguments"].(string)
					zf["arguments"] = old + x
				}
			}
		}
	}
	a := []any{}
	keys := make([]int, 0, len(tools))
	for i := range tools {
		keys = append(keys, i)
	}
	sort.Ints(keys)
	for _, i := range keys {
		a = append(a, tools[i])
	}
	m := map[string]any{"content": text, "tool_calls": a}
	return map[string]any{"choices": []any{map[string]any{"message": m, "finish_reason": finish}}, "usage": usage, "stream_done": done, "stream_error": streamErr}
}

func anthropicToChat(in map[string]any) (map[string]any, error) {
	if k, ok := unsupported(in, "thinking", "context_management"); ok {
		return nil, fmt.Errorf("%s is not supported", k)
	}
	msgs := []any{}
	if s, ok := in["system"].(string); ok && s != "" {
		msgs = append(msgs, map[string]any{"role": "system", "content": s})
	}
	if a, ok := in["system"].([]any); ok {
		for _, x := range a {
			p, _ := x.(map[string]any)
			if p["cache_control"] != nil {
				return nil, fmt.Errorf("prompt caching is not supported")
			}
			msgs = append(msgs, map[string]any{"role": "system", "content": p["text"]})
		}
	}
	a, ok := in["messages"].([]any)
	if !ok {
		return nil, fmt.Errorf("messages must be an array")
	}
	for _, raw := range a {
		m, _ := raw.(map[string]any)
		role, _ := m["role"].(string)
		content := m["content"]
		if s, ok := content.(string); ok {
			msgs = append(msgs, map[string]any{"role": role, "content": s})
			continue
		}
		blocks, _ := content.([]any)
		parts := []any{}
		calls := []any{}
		for _, rb := range blocks {
			b, _ := rb.(map[string]any)
			if b["cache_control"] != nil {
				return nil, fmt.Errorf("prompt caching is not supported")
			}
			switch b["type"] {
			case "text":
				parts = append(parts, map[string]any{"type": "text", "text": b["text"]})
			case "image":
				src, _ := b["source"].(map[string]any)
				if src["type"] == "base64" {
					parts = append(parts, map[string]any{"type": "image_url", "image_url": map[string]any{"url": "data:" + fmt.Sprint(src["media_type"]) + ";base64," + fmt.Sprint(src["data"])}})
				} else if src["type"] == "url" {
					parts = append(parts, map[string]any{"type": "image_url", "image_url": map[string]any{"url": src["url"]}})
				} else {
					return nil, fmt.Errorf("image source is not supported")
				}
			case "tool_use":
				args, _ := json.Marshal(b["input"])
				calls = append(calls, map[string]any{"id": b["id"], "type": "function", "function": map[string]any{"name": b["name"], "arguments": string(args)}})
			case "tool_result":
				v := b["content"]
				if z, ok := v.([]any); ok {
					var sb strings.Builder
					for _, q := range z {
						p, _ := q.(map[string]any)
						if p["type"] != "text" {
							return nil, fmt.Errorf("non-text tool_result is not supported")
						}
						sb.WriteString(fmt.Sprint(p["text"]))
					}
					v = sb.String()
				}
				msgs = append(msgs, map[string]any{"role": "tool", "tool_call_id": b["tool_use_id"], "content": v})
			default:
				return nil, fmt.Errorf("content block %q is not supported", b["type"])
			}
		}
		if len(parts) > 0 || len(calls) > 0 {
			q := map[string]any{"role": role}
			if len(parts) > 0 {
				q["content"] = parts
			}
			if len(calls) > 0 {
				q["tool_calls"] = calls
			}
			msgs = append(msgs, q)
		}
	}
	out := map[string]any{"model": in["model"], "messages": msgs, "stream": in["stream"], "max_tokens": in["max_tokens"]}
	if ts, ok := in["tools"].([]any); ok {
		z := []any{}
		for _, raw := range ts {
			t, _ := raw.(map[string]any)
			if t["cache_control"] != nil {
				return nil, fmt.Errorf("tool caching is not supported")
			}
			z = append(z, map[string]any{"type": "function", "function": map[string]any{"name": t["name"], "description": t["description"], "parameters": t["input_schema"]}})
		}
		out["tools"] = z
	}
	if tc, ok := in["tool_choice"].(map[string]any); ok {
		switch tc["type"] {
		case "auto", "any", "none":
			choice := tc["type"]
			if choice == "any" {
				choice = "required"
			}
			out["tool_choice"] = choice
		case "tool":
			out["tool_choice"] = map[string]any{"type": "function", "function": map[string]any{"name": tc["name"]}}
		}
	}
	for _, k := range []string{"temperature", "top_p"} {
		if in[k] != nil {
			out[k] = in[k]
		}
	}
	if in["stop_sequences"] != nil {
		out["stop"] = in["stop_sequences"]
	}
	return out, nil
}

func anthropicBlocks(chat map[string]any) ([]any, string) {
	out, _ := chatOutput(chat, map[string]bool{})
	blocks := []any{}
	reason := "end_turn"
	if choices, _ := chat["choices"].([]any); len(choices) > 0 {
		if c, _ := choices[0].(map[string]any); c["finish_reason"] == "length" {
			reason = "max_tokens"
		}
	}
	for _, raw := range out {
		x := raw.(map[string]any)
		if x["type"] == "message" {
			c := x["content"].([]any)[0].(map[string]any)
			blocks = append(blocks, map[string]any{"type": "text", "text": c["text"]})
		} else {
			var input any = map[string]any{}
			_ = json.Unmarshal([]byte(fmt.Sprint(x["arguments"])), &input)
			blocks = append(blocks, map[string]any{"type": "tool_use", "id": x["call_id"], "name": x["name"], "input": input})
			reason = "tool_use"
		}
	}
	if len(blocks) == 0 {
		blocks = append(blocks, map[string]any{"type": "text", "text": ""})
	}
	return blocks, reason
}
func anthropicUsage(v any) map[string]any {
	u, _ := v.(map[string]any)
	return map[string]any{"input_tokens": numberOrZero(u["prompt_tokens"]), "output_tokens": numberOrZero(u["completion_tokens"])}
}
func numberOrZero(v any) any {
	if v == nil {
		return 0
	}
	return v
}
func writeAnthropicError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{"type": "error", "error": map[string]any{"type": "invalid_request_error", "message": msg}})
}
func (h *Handler) anthropicMessages(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("anthropic-version") == "" {
		writeAnthropicError(w, 400, "anthropic-version header is required")
		return
	}
	var in map[string]any
	if json.NewDecoder(r.Body).Decode(&in) != nil {
		writeAnthropicError(w, 400, "invalid JSON")
		return
	}
	chat, err := anthropicToChat(in)
	if err != nil {
		writeAnthropicError(w, 400, err.Error())
		return
	}
	stream, _ := in["stream"].(bool)
	if stream {
		h.streamAnthropic(w, r, chat, in)
		return
	}
	rr := &responseCapture{}
	h.runChat(r, chat, rr)
	if rr.status != 200 {
		writeAnthropicError(w, rr.status, strings.TrimSpace(rr.body.String()))
		return
	}
	obj, err := chatObject(rr)
	if err != nil {
		writeAnthropicError(w, 502, "invalid upstream response")
		return
	}
	blocks, reason := anthropicBlocks(obj)
	id := protocolID("msg_")
	usage := anthropicUsage(obj["usage"])
	message := map[string]any{"id": id, "type": "message", "role": "assistant", "model": in["model"], "content": blocks, "stop_reason": reason, "stop_sequence": nil, "usage": usage}
	writeJSON(w, 200, message)
}
func (h *Handler) anthropicCountTokens(w http.ResponseWriter, r *http.Request) {
	writeAnthropicError(w, http.StatusNotImplemented, "token counting is not implemented because the upstream does not expose a tokenizer; send the message request directly")
}

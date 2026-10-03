package server

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/linguo2625469/workbuddy2api-panel/internal/httpauth"
)

func TestResponsesCodexCustomToolRoundTrip(t *testing.T) {
	in := map[string]any{
		"model": "cn:test", "instructions": "be precise",
		"input": []any{
			map[string]any{"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": "edit it"}}},
			map[string]any{"type": "custom_tool_call", "call_id": "call_1", "name": "apply_patch", "input": "*** Begin Patch"},
			map[string]any{"type": "custom_tool_call_output", "call_id": "call_1", "output": "Done!"},
		},
		"tools": []any{map[string]any{"type": "custom", "name": "apply_patch", "description": "Apply a patch", "format": map[string]any{"type": "text"}}},
	}
	got, err := responsesToChat(in)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(got)
	for _, want := range []string{`"name":"apply_patch"`, `\"input\":\"*** Begin Patch\"`, `"tool_call_id":"call_1"`, `"content":"Done!"`} {
		if !strings.Contains(string(b), want) {
			t.Fatalf("missing %s in %s", want, b)
		}
	}
	out, _ := chatOutput(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"tool_calls": []any{map[string]any{"id": "call_2", "function": map[string]any{"name": "apply_patch", "arguments": `{"input":"patch text"}`}}}}}}}, map[string]bool{"apply_patch": true})
	item := out[0].(map[string]any)
	if item["type"] != "custom_tool_call" || item["input"] != "patch text" || item["call_id"] != "call_2" {
		t.Fatalf("bad custom result: %#v", item)
	}
}

func TestResponsesRejectsStateAndHostedTools(t *testing.T) {
	if _, err := responsesToChat(map[string]any{"previous_response_id": "resp_old"}); err == nil {
		t.Fatal("previous_response_id silently ignored")
	}
	if _, err := responsesToChat(map[string]any{"input": []any{}, "tools": []any{map[string]any{"type": "web_search"}}}); err == nil {
		t.Fatal("hosted tool silently ignored")
	}
}

func TestAnthropicClaudeCodeToolHistory(t *testing.T) {
	in := map[string]any{
		"model": "cn:test", "max_tokens": 100,
		"messages": []any{
			map[string]any{"role": "assistant", "content": []any{
				map[string]any{"type": "tool_use", "id": "toolu_1", "name": "Read", "input": map[string]any{"file_path": "README.md"}},
			}},
			map[string]any{"role": "user", "content": []any{
				map[string]any{"type": "tool_result", "tool_use_id": "toolu_1", "content": "contents"},
			}},
		},
		"tools": []any{map[string]any{"name": "Read", "input_schema": map[string]any{"type": "object"}}},
	}
	got, err := anthropicToChat(in)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(got)
	for _, want := range []string{`"id":"toolu_1"`, `"tool_call_id":"toolu_1"`, `"content":"contents"`, `"parameters":{"type":"object"}`} {
		if !strings.Contains(string(b), want) {
			t.Fatalf("missing %s in %s", want, b)
		}
	}
}

func TestAnthropicStreamEventOrder(t *testing.T) {
	chat := aggregateChatSSE("data: {\"choices\":[{\"delta\":{\"content\":\"hel\"}}]}\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"lo\"}}],\"usage\":{\"completion_tokens\":2}}\n\ndata: [DONE]\n\n")
	blocks, reason := anthropicBlocks(chat)
	if reason != "end_turn" || blocks[0].(map[string]any)["text"] != "hello" {
		t.Fatalf("bad aggregation: %#v %s", blocks, reason)
	}
}

func TestAnthropicAPIKeyAuth(t *testing.T) {
	r := httptest.NewRequest("POST", "/v1/messages?beta=true", nil)
	r.Header.Set("x-api-key", "synthetic-test-key")
	if !httpauth.VerifyBearer(r, "synthetic-test-key") {
		t.Fatal("x-api-key rejected")
	}
	r.Header.Set("x-api-key", "wrong")
	if httpauth.VerifyBearer(r, "synthetic-test-key") {
		t.Fatal("wrong key accepted")
	}
}

package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/linguo2625469/workbuddy2api-panel/internal/auth"
	"github.com/linguo2625469/workbuddy2api-panel/internal/reqlog"
	"github.com/linguo2625469/workbuddy2api-panel/internal/upstream"
)

func TestResponsesCodexCustomToolRoundTrip(t *testing.T) {
	in := map[string]any{
		"model": "cn:test", "instructions": "be precise",
		"input": []any{
			map[string]any{"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": "edit it"}}},
			map[string]any{"type": "custom_tool_call", "call_id": "call_1", "name": "apply_patch", "input": "*** Begin Patch"},
			map[string]any{"type": "custom_tool_call_output", "call_id": "call_1", "output": "Done!"},
		},
		"tools": []any{map[string]any{"type": "custom", "name": "apply_patch", "description": "Apply a patch", "format": map[string]any{"type": "grammar", "syntax": "lark", "definition": `start: "*** Begin Patch" /(.|\\n)*/ "*** End Patch"`}}},
	}
	got, err := responsesToChat(in)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(got)
	for _, want := range []string{`"name":"apply_patch"`, `\"input\":\"*** Begin Patch\"`, `"tool_call_id":"call_1"`, `"content":"Done!"`, `lark grammar`} {
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
	if !verifyAnthropicKey(r, "synthetic-test-key") {
		t.Fatal("x-api-key rejected")
	}
	r.Header.Set("x-api-key", "wrong")
	if verifyAnthropicKey(r, "synthetic-test-key") {
		t.Fatal("wrong key accepted")
	}
}

func TestAggregateChatSSEFragmentedInterleavedTools(t *testing.T) {
	s := strings.Join([]string{
		`data: {"choices":[{"delta":{"tool_calls":[{"index":3,"id":"call_b","function":{"name":"sec","arguments":"{\"b\":"}}]}}]}`,
		`data: {"choices":[{"delta":{"tool_calls":[{"index":1,"id":"call_a","function":{"name":"fir","arguments":"{\"a\":"}},{"index":3,"function":{"name":"ond","arguments":"2}"}}]}}]}`,
		`data: {"choices":[{"delta":{"tool_calls":[{"index":1,"function":{"name":"st","arguments":"1}"}}]},"finish_reason":"tool_calls"}]}`,
		`data: [DONE]`, "",
	}, "\n\n")
	got := aggregateChatSSE(s)
	if got["stream_done"] != true || got["stream_error"] != nil {
		t.Fatalf("bad terminal state: %#v", got)
	}
	choices := got["choices"].([]any)
	calls := choices[0].(map[string]any)["message"].(map[string]any)["tool_calls"].([]any)
	if len(calls) != 2 {
		t.Fatalf("calls=%#v", calls)
	}
	wants := []struct{ id, name, args string }{{"call_a", "first", `{"a":1}`}, {"call_b", "second", `{"b":2}`}}
	for i, w := range wants {
		c := calls[i].(map[string]any)
		f := c["function"].(map[string]any)
		if c["id"] != w.id || f["name"] != w.name || f["arguments"] != w.args {
			t.Fatalf("call %d = %#v", i, c)
		}
		var v map[string]any
		if json.Unmarshal([]byte(f["arguments"].(string)), &v) != nil {
			t.Fatalf("invalid arguments: %q", f["arguments"])
		}
		if strings.Contains(f["name"].(string)+f["arguments"].(string), "<nil>") {
			t.Fatal("nil prefix")
		}
	}
}

func TestAdapterMappings(t *testing.T) {
	r, err := responsesToChat(map[string]any{"input": "x", "tool_choice": map[string]any{"type": "function", "name": "read"}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(fmt.Sprint(r["tool_choice"]), "read") {
		t.Fatalf("tool choice: %#v", r)
	}
	a, err := anthropicToChat(map[string]any{"messages": []any{}, "tool_choice": map[string]any{"type": "any"}, "stop_sequences": []any{"STOP"}})
	if err != nil {
		t.Fatal(err)
	}
	if a["tool_choice"] != "required" || a["stop"] == nil || a["stop_sequences"] != nil {
		t.Fatalf("anthropic mapping: %#v", a)
	}
}

func TestAdapterHTTPStreamsBeforeUpstreamFinishes(t *testing.T) {
	tests := []struct {
		name, path, body, want string
		headers                map[string]string
		first                  string
	}{
		{"responses-text", "/v1/responses", `{"model":"glm-5.2","stream":true,"input":"hi"}`, `response.output_text.delta`, nil, `data: {"choices":[{"delta":{"content":"first"}}]}`},
		{"responses-tool", "/v1/responses", `{"model":"glm-5.2","stream":true,"input":"read","tools":[{"type":"function","name":"read","parameters":{"type":"object"}}]}`, `response.function_call_arguments.delta`, nil, `data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"read","arguments":"{\"path\":"}}]}}]}`},
		{"anthropic-text", "/v1/messages?beta=true", `{"model":"glm-5.2","stream":true,"max_tokens":10,"messages":[{"role":"user","content":"hi"}]}`, `text_delta`, map[string]string{"anthropic-version": "2023-06-01"}, `data: {"choices":[{"delta":{"content":"first"}}]}`},
		{"anthropic-tool", "/v1/messages", `{"model":"glm-5.2","stream":true,"max_tokens":10,"messages":[{"role":"user","content":"read"}],"tools":[{"name":"read","input_schema":{"type":"object"}}]}`, `input_json_delta`, map[string]string{"anthropic-version": "2023-06-01"}, `data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"read","arguments":"{\"path\":"}}]}}]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			release := make(chan struct{})
			finished := make(chan struct{})
			up := &upstream.Client{
				HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					pr, pw := io.Pipe()
					go func() {
						defer close(finished)
						_, _ = fmt.Fprint(pw, tt.first+"\n\n")
						<-release
						if strings.Contains(tt.name, "tool") {
							_, _ = fmt.Fprint(pw, `data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"x\"}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":7,"completion_tokens":2,"total_tokens":9}}`+"\n\n")
						} else {
							_, _ = fmt.Fprint(pw, `data: {"choices":[{"delta":{"content":" second"},"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":2,"total_tokens":9}}`+"\n\n")
						}
						_, _ = fmt.Fprint(pw, "data: [DONE]\n\n")
						_ = pw.Close()
					}()
					return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: pr}, nil
				})},
				ChatBaseCN: "https://fake.example", BillingBaseCN: "https://fake.example",
			}
			log := reqlog.New(reqlog.Config{})
			h := NewHandler(Config{Pool: testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}), Upstream: up, RequestLog: log})
			srv := httptest.NewServer(h)
			defer srv.Close()
			req, _ := http.NewRequest("POST", srv.URL+tt.path, bytes.NewBufferString(tt.body))
			req.Header.Set("Content-Type", "application/json")
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}
			resp, err := srv.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if !strings.HasPrefix(resp.Header.Get("X-Request-Id"), "req-") {
				t.Fatalf("request id=%q", resp.Header.Get("X-Request-Id"))
			}
			scanner := bufio.NewScanner(resp.Body)
			found := false
			for scanner.Scan() {
				if strings.Contains(scanner.Text(), tt.want) {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("did not receive %q before EOF", tt.want)
			}
			select {
			case <-finished:
				t.Fatal("upstream finished before downstream increment")
			default:
			}
			close(release)
			var tail strings.Builder
			for scanner.Scan() {
				tail.WriteString(scanner.Text())
			}
			if strings.Contains(tt.name, "anthropic") && !strings.Contains(tail.String(), `"input_tokens":7`) {
				t.Fatalf("final usage missing real input tokens: %s", tail.String())
			}
			select {
			case <-finished:
			case <-time.After(time.Second):
				t.Fatal("upstream did not finish")
			}
			snap := log.Snapshot()
			if snap.Completed != 1 || len(snap.Recent) != 1 || snap.Recent[0].Path != strings.Split(tt.path, "?")[0] {
				t.Fatalf("request log=%+v", snap)
			}
		})
	}
}

func TestStreamRejectsInvalidToolArguments(t *testing.T) {
	p := newChatStreamParser()
	p.consume(`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"read","arguments":"{"}}]}}]}`)
	p.consume("[DONE]")
	if err := p.validateTools(nil); err == nil || !strings.Contains(err.Error(), "invalid arguments JSON") {
		t.Fatalf("error=%v", err)
	}
}

func TestAdapterHTTPCancelPropagatesUpstream(t *testing.T) {
	cancelled := make(chan struct{})
	release := make(chan struct{})
	up := &upstream.Client{
		HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			pr, pw := io.Pipe()
			go func() {
				_, _ = fmt.Fprint(pw, `data: {"choices":[{"delta":{"content":"first"}}]}`+"\n\n")
				select {
				case <-r.Context().Done():
					close(cancelled)
					_ = pw.CloseWithError(r.Context().Err())
				case <-release:
					_ = pw.Close()
				}
			}()
			return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: pr}, nil
		})},
		ChatBaseCN: "https://fake.example", BillingBaseCN: "https://fake.example",
	}
	h := NewHandler(Config{Pool: testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}), Upstream: up})
	srv := httptest.NewServer(h)
	defer srv.Close()
	defer close(release)
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, "POST", srv.URL+"/v1/responses", strings.NewReader(`{"model":"glm-5.2","stream":true,"input":"hi"}`))
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), "output_text.delta") {
			break
		}
	}
	cancel()
	_ = resp.Body.Close()
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("downstream cancellation did not reach upstream request context")
	}
}

func TestAdapterHTTPReportsUpstreamErrorAndTruncation(t *testing.T) {
	for _, tc := range []struct{ name, body, want string }{{"error", "data: {\"error\":{\"message\":\"synthetic upstream failure\"}}\n\n", "synthetic upstream failure"}, {"truncated", "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n", "response.failed"}} {
		t.Run(tc.name, func(t *testing.T) {
			up := newFakeUpstream(t, func(string) (int, string, bool) { return 200, tc.body, true })
			h := NewHandler(Config{Pool: testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}), Upstream: up})
			srv := httptest.NewServer(h)
			defer srv.Close()
			resp, err := http.Post(srv.URL+"/v1/responses", "application/json", strings.NewReader(`{"model":"glm-5.2","stream":true,"input":"hi"}`))
			if err != nil {
				t.Fatal(err)
			}
			b, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if !strings.Contains(string(b), tc.want) {
				t.Fatalf("body=%s want=%s", b, tc.want)
			}
		})
	}
}

func TestAdapterHTTPMultiTurnToolRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name, path, first, second string
		headers                   map[string]string
	}{
		{"responses", "/v1/responses", `{"model":"glm-5.2","stream":true,"input":"read","tools":[{"type":"function","name":"read","parameters":{"type":"object"}}]}`, `{"model":"glm-5.2","stream":true,"input":[{"type":"function_call","call_id":"call_1","name":"read","arguments":"{\"path\":\"README.md\"}"},{"type":"function_call_output","call_id":"call_1","output":"contents"}]}`, nil},
		{"anthropic", "/v1/messages", `{"model":"glm-5.2","stream":true,"max_tokens":10,"messages":[{"role":"user","content":"read"}],"tools":[{"name":"read","input_schema":{"type":"object"}}]}`, `{"model":"glm-5.2","stream":true,"max_tokens":10,"messages":[{"role":"assistant","content":[{"type":"tool_use","id":"call_1","name":"read","input":{"path":"README.md"}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"call_1","content":"contents"}]}]}`, map[string]string{"anthropic-version": "2023-06-01"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var requests []string
			up := &upstream.Client{
				HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					b, _ := io.ReadAll(r.Body)
					mu.Lock()
					requests = append(requests, string(b))
					call := len(requests)
					mu.Unlock()
					body := sseOK
					if call == 1 {
						body = "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"read\",\"arguments\":\"{\\\"path\\\":\\\"README.md\\\"}\"}}]},\"finish_reason\":\"tool_calls\"}],\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":2,\"total_tokens\":6}}\n\ndata: [DONE]\n\n"
					}
					return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
				})},
				ChatBaseCN: "https://fake.example", BillingBaseCN: "https://fake.example",
			}
			h := NewHandler(Config{Pool: testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}), Upstream: up})
			srv := httptest.NewServer(h)
			defer srv.Close()
			for _, body := range []string{tc.first, tc.second} {
				req, _ := http.NewRequest("POST", srv.URL+tc.path, strings.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				for k, v := range tc.headers {
					req.Header.Set(k, v)
				}
				resp, err := srv.Client().Do(req)
				if err != nil {
					t.Fatal(err)
				}
				out, _ := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode != 200 {
					t.Fatalf("status=%d body=%s", resp.StatusCode, out)
				}
			}
			mu.Lock()
			defer mu.Unlock()
			if len(requests) != 2 || !strings.Contains(requests[1], `"role":"tool"`) || !strings.Contains(requests[1], `"tool_call_id":"call_1"`) || !strings.Contains(requests[1], `"content":"contents"`) {
				t.Fatalf("upstream requests=%q", requests)
			}
		})
	}
}

func runAdapterStream(t *testing.T, path, requestBody, upstreamBody string, headers map[string]string) string {
	t.Helper()
	up := newFakeUpstream(t, func(string) (int, string, bool) { return 200, upstreamBody, true })
	h := NewHandler(Config{Pool: testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}), Upstream: up})
	srv := httptest.NewServer(h)
	defer srv.Close()
	req, _ := http.NewRequest("POST", srv.URL+path, strings.NewReader(requestBody))
	req.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d body=%s", resp.StatusCode, body)
	}
	return string(body)
}

func streamDataObjects(t *testing.T, body string) []map[string]any {
	t.Helper()
	var result []map[string]any
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var value map[string]any
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &value); err != nil {
			t.Fatalf("bad downstream event %q: %v", line, err)
		}
		result = append(result, value)
	}
	return result
}

func TestAdapterHTTPStableIndexesFragmentedNamesAndMissingIndexes(t *testing.T) {
	upstreamBody := strings.Join([]string{
		`data: {"choices":[{"delta":{"tool_calls":[{"id":"call_a","function":{"name":"rea"}}]}}]}`,
		`data: {"choices":[{"delta":{"tool_calls":[{"id":"call_a","function":{"name":"d","arguments":"{\"path\":"}}]}}]}`,
		`data: {"choices":[{"delta":{"content":"between"}}]}`,
		`data: {"choices":[{"delta":{"tool_calls":[{"id":"call_b","function":{"name":"write","arguments":"{\"text\":"}},{"id":"call_a","function":{"arguments":"\"README.md\"}"}}]}}]}`,
		`data: {"choices":[{"delta":{"tool_calls":[{"id":"call_b","function":{"arguments":"\"ok\"}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":8,"completion_tokens":3,"total_tokens":11}}`,
		`data: [DONE]`, "",
	}, "\n\n")
	tests := []struct {
		name, path, request string
		headers             map[string]string
	}{
		{"responses", "/v1/responses", `{"model":"glm-5.2","stream":true,"input":"work","tools":[{"type":"function","name":"read","parameters":{"type":"object"}},{"type":"function","name":"write","parameters":{"type":"object"}}]}`, nil},
		{"anthropic", "/v1/messages", `{"model":"glm-5.2","stream":true,"max_tokens":20,"messages":[{"role":"user","content":"work"}],"tools":[{"name":"read","input_schema":{"type":"object"}},{"name":"write","input_schema":{"type":"object"}}]}`, map[string]string{"anthropic-version": "2023-06-01"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			body := runAdapterStream(t, tc.path, tc.request, upstreamBody, tc.headers)
			events := streamDataObjects(t, body)
			indexes := map[string]map[int]bool{"call_a": {}, "call_b": {}}
			textIndexes := map[int]bool{}
			for _, event := range events {
				id, _ := event["item_id"].(string)
				id = strings.TrimPrefix(id, "fc_")
				if item, _ := event["item"].(map[string]any); item != nil {
					if value, _ := item["call_id"].(string); value != "" {
						id = value
					}
				}
				if block, _ := event["content_block"].(map[string]any); block != nil {
					if value, _ := block["id"].(string); value != "" {
						id = value
					}
				}
				if _, ok := indexes[id]; ok {
					if n, ok := event["output_index"].(float64); ok {
						indexes[id][int(n)] = true
					}
					if n, ok := event["index"].(float64); ok {
						indexes[id][int(n)] = true
					}
				}
				if event["type"] == "response.output_text.delta" || event["type"] == "content_block_delta" && strings.Contains(fmt.Sprint(event["delta"]), "text_delta") {
					if n, ok := event["output_index"].(float64); ok {
						textIndexes[int(n)] = true
					}
					if n, ok := event["index"].(float64); ok {
						textIndexes[int(n)] = true
					}
				}
			}
			for id, seen := range indexes {
				if len(seen) != 1 {
					t.Fatalf("%s changed indexes: %v\n%s", id, seen, body)
				}
			}
			if len(textIndexes) != 1 {
				t.Fatalf("text changed indexes: %v", textIndexes)
			}
			var assigned []int
			for _, seen := range indexes {
				for index := range seen {
					assigned = append(assigned, index)
				}
			}
			for index := range textIndexes {
				assigned = append(assigned, index)
			}
			sort.Ints(assigned)
			if len(assigned) != 3 || assigned[0] == assigned[1] || assigned[1] == assigned[2] {
				t.Fatalf("item indexes are not unique: %v", assigned)
			}
			if tc.name == "anthropic" {
				blockIDs := map[int]string{}
				partial := map[int]string{}
				for _, event := range events {
					indexValue, hasIndex := event["index"].(float64)
					if !hasIndex {
						continue
					}
					index := int(indexValue)
					if block, _ := event["content_block"].(map[string]any); block != nil {
						if id, _ := block["id"].(string); id != "" {
							blockIDs[index] = id
						}
					}
					if delta, _ := event["delta"].(map[string]any); delta != nil {
						if fragment, _ := delta["partial_json"].(string); fragment != "" {
							partial[index] += fragment
						}
					}
				}
				wantInputs := map[string]string{"call_a": `{"path":"README.md"}`, "call_b": `{"text":"ok"}`}
				for index, id := range blockIDs {
					if partial[index] != wantInputs[id] {
						t.Fatalf("tool %s input fragments=%q want %q", id, partial[index], wantInputs[id])
					}
					var input map[string]any
					if err := json.Unmarshal([]byte(partial[index]), &input); err != nil {
						t.Fatalf("tool %s invalid input: %v", id, err)
					}
				}
			}
			if strings.Contains(body, `"name":"rea"`) {
				t.Fatalf("partial tool name was published: %s", body)
			}
			for _, want := range []string{`"name":"read"`, `"call_id":"call_a"`, `"name":"write"`, `"call_id":"call_b"`} {
				if tc.name == "anthropic" {
					want = strings.Replace(want, "call_id", "id", 1)
				}
				if !strings.Contains(body, want) {
					t.Fatalf("missing %s in %s", want, body)
				}
			}
			if tc.name == "anthropic" && !strings.Contains(body, `"stop_reason":"tool_use"`) {
				t.Fatalf("tool finish did not map to tool_use: %s", body)
			}
		})
	}
}

func TestResponsesHTTPFragmentedCustomToolName(t *testing.T) {
	upstreamBody := strings.Join([]string{
		`data: {"choices":[{"delta":{"tool_calls":[{"id":"call_patch","function":{"name":"apply_"}}]}}]}`,
		`data: {"choices":[{"delta":{"tool_calls":[{"id":"call_patch","function":{"name":"patch","arguments":"{\"input\":\"*** Begin "}}]}}]}`,
		`data: {"choices":[{"delta":{"tool_calls":[{"id":"call_patch","function":{"arguments":"Patch***\"}"}}]},"finish_reason":"tool_calls"}]}`,
		`data: [DONE]`, "",
	}, "\n\n")
	request := `{"model":"glm-5.2","stream":true,"input":"patch","tools":[{"type":"custom","name":"apply_patch","format":{"type":"grammar","syntax":"lark","definition":"start: /.+/"}}]}`
	body := runAdapterStream(t, "/v1/responses", request, upstreamBody, nil)
	if strings.Contains(body, `"name":"apply_"`) || strings.Contains(body, `"type":"function_call"`) {
		t.Fatalf("partial custom name was misclassified: %s", body)
	}
	for _, want := range []string{`"type":"custom_tool_call"`, `"name":"apply_patch"`, `"call_id":"call_patch"`, `"input":"*** Begin Patch***"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %s in %s", want, body)
		}
	}
}

func TestAdapterHTTPToolIdentitySwitchesBetweenIndexAndID(t *testing.T) {
	streams := []struct{ name, first, second string }{
		{"index-to-id", `{"index":0,"id":"call_a","type":"function","function":{"name":"read","arguments":"{"}}`, `{"id":"call_a","function":{"name":"read","arguments":"}"}}`},
		{"id-to-index", `{"id":"call_a","type":"function","function":{"name":"read","arguments":"{"}}`, `{"index":0,"id":"call_a","function":{"name":"read","arguments":"}"}}`},
	}
	protocols := []struct {
		name, path, request string
		headers             map[string]string
	}{
		{"responses", "/v1/responses", `{"model":"glm-5.2","stream":true,"input":"read","tools":[{"type":"function","name":"read","parameters":{"type":"object"}}]}`, nil},
		{"anthropic", "/v1/messages", `{"model":"glm-5.2","stream":true,"max_tokens":20,"messages":[{"role":"user","content":"read"}],"tools":[{"name":"read","input_schema":{"type":"object"}}]}`, map[string]string{"anthropic-version": "2023-06-01"}},
	}
	for _, stream := range streams {
		for _, protocol := range protocols {
			t.Run(protocol.name+"/"+stream.name, func(t *testing.T) {
				upstreamBody := strings.Join([]string{
					`data: {"choices":[{"delta":{"tool_calls":[` + stream.first + `]}}]}`,
					`data: {"choices":[{"delta":{"tool_calls":[` + stream.second + `]}}]}`,
					`data: {"choices":[{"delta":{},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":3,"completion_tokens":1,"total_tokens":4}}`,
					`data: [DONE]`, "",
				}, "\n\n")
				body := runAdapterStream(t, protocol.path, protocol.request, upstreamBody, protocol.headers)
				if strings.Contains(body, `"type":"error"`) || strings.Contains(body, `response.failed`) {
					t.Fatalf("identity switch failed stream: %s", body)
				}
				events := streamDataObjects(t, body)
				starts, indexes := 0, map[int]bool{}
				arguments := ""
				for _, event := range events {
					if protocol.name == "responses" {
						if event["type"] == "response.output_item.added" {
							if item, _ := event["item"].(map[string]any); item["call_id"] == "call_a" {
								starts++
								indexes[int(event["output_index"].(float64))] = true
							}
						}
						if strings.Contains(fmt.Sprint(event["item_id"]), "call_a") {
							if n, ok := event["output_index"].(float64); ok {
								indexes[int(n)] = true
							}
						}
						if event["type"] == "response.function_call_arguments.done" {
							arguments, _ = event["arguments"].(string)
						}
					} else {
						if event["type"] == "content_block_start" {
							block, _ := event["content_block"].(map[string]any)
							if block["id"] == "call_a" {
								starts++
								indexes[int(event["index"].(float64))] = true
								if block["name"] != "read" {
									t.Fatalf("name=%v", block["name"])
								}
							}
						}
						if event["type"] == "content_block_delta" {
							delta, _ := event["delta"].(map[string]any)
							if part, _ := delta["partial_json"].(string); part != "" {
								arguments += part
								indexes[int(event["index"].(float64))] = true
							}
						}
					}
				}
				if starts != 1 || len(indexes) != 1 || arguments != "{}" {
					t.Fatalf("starts=%d indexes=%v arguments=%q\n%s", starts, indexes, arguments, body)
				}
				if !strings.Contains(body, `"name":"read"`) || protocol.name == "responses" && !strings.Contains(body, `"type":"response.completed"`) || protocol.name == "anthropic" && !strings.Contains(body, `"stop_reason":"tool_use"`) {
					t.Fatalf("bad terminal tool event: %s", body)
				}
			})
		}
	}
}

func aggregateChatSSE(s string) map[string]any {
	parser := newChatStreamParser()
	for _, line := range strings.Split(s, "\n") {
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		parser.consume(strings.TrimSpace(strings.TrimPrefix(line, "data: ")))
	}
	a := []any{}
	tools := append([]*streamTool(nil), parser.order...)
	sort.SliceStable(tools, func(i, j int) bool { return tools[i].index < tools[j].index })
	for _, tool := range tools {
		a = append(a, map[string]any{"id": tool.id, "type": "function", "function": map[string]any{"name": tool.name.String(), "arguments": tool.arguments.String()}})
	}
	var streamErr any
	if parser.failed != "" {
		streamErr = map[string]any{"message": parser.failed}
	}
	m := map[string]any{"content": parser.text.String(), "tool_calls": a}
	return map[string]any{"choices": []any{map[string]any{"message": m, "finish_reason": parser.finish}}, "usage": parser.usage, "stream_done": parser.done, "stream_error": streamErr}
}

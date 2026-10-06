package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/linguo2625469/workbuddy2api-panel/internal/auth"
	"github.com/linguo2625469/workbuddy2api-panel/internal/reqlog"
)

func adapterTestRequest(path string, stream bool) *http.Request {
	var body map[string]any
	switch path {
	case "/v1/responses":
		body = map[string]any{"model": "glm-5.2", "input": "hi", "stream": stream}
	default:
		body = map[string]any{"model": "glm-5.2", "messages": []any{map[string]any{"role": "user", "content": "hi"}}, "stream": stream, "max_tokens": 20}
	}
	raw, _ := json.Marshal(body)
	r := httptest.NewRequest("POST", path, strings.NewReader(string(raw)))
	r.Header.Set("anthropic-version", "2023-06-01")
	return r
}

func TestAdapterPreservesPreStreamHTTPFailures(t *testing.T) {
	for _, failure := range []struct {
		name   string
		status int
		body   string
		empty  bool
	}{
		{name: "empty-pool", empty: true},
		{name: "invalid-params", status: 400, body: `{"code":11101,"msg":"synthetic bad parameters"}`},
		{name: "rate-limit", status: 429, body: `{"code":6004,"msg":"synthetic rate limit"}`},
	} {
		t.Run(failure.name, func(t *testing.T) {
			newHandler := func() *Handler {
				if failure.empty {
					return NewHandler(Config{Pool: testPoolWith()})
				}
				up := newFakeUpstream(t, func(string) (int, string, bool) { return failure.status, failure.body, false })
				return NewHandler(Config{Pool: testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}), Upstream: up})
			}
			baseline := httptest.NewRecorder()
			newHandler().ServeHTTP(baseline, adapterTestRequest("/v1/chat/completions", true))
			if baseline.Code < 400 {
				t.Fatalf("baseline was not an error: %d %s", baseline.Code, baseline.Body.String())
			}
			for _, path := range []string{"/v1/responses", "/v1/messages"} {
				w := httptest.NewRecorder()
				newHandler().ServeHTTP(w, adapterTestRequest(path, true))
				if w.Code != baseline.Code || !strings.Contains(w.Header().Get("Content-Type"), "application/json") || strings.Contains(w.Body.String(), "event:") {
					t.Fatalf("%s got %d %s; expected HTTP %d JSON", path, w.Code, w.Body.String(), baseline.Code)
				}
			}
		})
	}
}

func TestAdapterToolAliasesDoNotSplitOrCollide(t *testing.T) {
	upstreamBody := strings.Join([]string{
		`data: {"choices":[{"delta":{"tool_calls":[{"id":"call_a","function":{"name":"read","arguments":"{"}}]}}]}`,
		`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_b","function":{"name":"write","arguments":"{}"}}]}}]}`,
		`data: {"choices":[{"delta":{"tool_calls":[{"index":3,"id":"call_a","function":{"arguments":"}"}}]}}]}`,
		`data: {"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
		`data: [DONE]`, "",
	}, "\n\n")
	for _, path := range []string{"/v1/responses", "/v1/messages"} {
		up := newFakeUpstream(t, func(string) (int, string, bool) { return 200, upstreamBody, true })
		h := NewHandler(Config{Pool: testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}), Upstream: up})
		w := httptest.NewRecorder()
		h.ServeHTTP(w, adapterTestRequest(path, true))
		if strings.Contains(w.Body.String(), "response.failed") || strings.Contains(w.Body.String(), `"type":"error"`) {
			t.Fatal(w.Body.String())
		}
		starts := map[string]int{}
		for _, event := range streamDataObjects(t, w.Body.String()) {
			if event["type"] == "response.output_item.added" {
				item := event["item"].(map[string]any)
				starts[item["call_id"].(string)]++
			}
			if event["type"] == "content_block_start" {
				block := event["content_block"].(map[string]any)
				starts[block["id"].(string)]++
			}
		}
		if starts["call_a"] != 1 || starts["call_b"] != 1 || len(starts) != 2 {
			t.Fatalf("%s starts=%v\n%s", path, starts, w.Body.String())
		}
	}
}

func TestAdapterFailuresKeepUsageAndFinalLogs(t *testing.T) {
	upstreamBody := strings.Join([]string{
		`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"read","arguments":"{"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":7,"completion_tokens":2,"total_tokens":9}}`,
		`data: [DONE]`, "",
	}, "\n\n")
	for _, path := range []string{"/v1/responses", "/v1/messages"} {
		for _, stream := range []bool{false, true} {
			t.Run(path+map[bool]string{true: "/stream", false: "/sync"}[stream], func(t *testing.T) {
				withChatLog(t)
				up := newFakeUpstream(t, func(string) (int, string, bool) { return 200, upstreamBody, true })
				rec := reqlog.New(reqlog.Config{})
				h := NewHandler(Config{Pool: testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}), Upstream: up, RequestLog: rec})
				w := httptest.NewRecorder()
				logs := captureStdout(t, func() { h.ServeHTTP(w, adapterTestRequest(path, stream)) })
				snap := rec.Snapshot()
				wantOutcome := reqlog.OutcomeHTTPError
				if stream {
					wantOutcome = reqlog.OutcomeStreamError
				}
				if snap.Completed != 1 || snap.Succeeded != 0 || len(snap.Recent) != 1 {
					t.Fatalf("metrics=%+v", snap)
				}
				event := snap.Recent[0]
				if event.OK || event.Outcome != wantOutcome || event.TotalTokens != 9 {
					t.Fatalf("event=%+v", event)
				}
				if strings.Count(logs, "out="+wantOutcome) != 1 || strings.Contains(logs, "out=success") {
					t.Fatalf("logs=%s", logs)
				}
				if !stream && w.Code != 502 {
					t.Fatalf("HTTP=%d body=%s", w.Code, w.Body.String())
				}
			})
		}
	}
}

type adapterDisconnectWriter struct {
	header http.Header
	failOn string
	writes int
}

func (w *adapterDisconnectWriter) Header() http.Header { return w.header }
func (w *adapterDisconnectWriter) WriteHeader(int)     {}
func (w *adapterDisconnectWriter) Write(body []byte) (int, error) {
	w.writes++
	if w.failOn == "" || strings.Contains(string(body), w.failOn) {
		return 0, errors.New("synthetic client disconnect")
	}
	return len(body), nil
}

func TestAdapterWriteFailuresAreInterrupted(t *testing.T) {
	for _, path := range []string{"/v1/responses", "/v1/messages"} {
		terminal := map[string]string{"/v1/responses": "event: response.completed", "/v1/messages": "event: message_stop"}[path]
		for _, failOn := range []string{"", terminal} {
			up := newFakeUpstream(t, func(string) (int, string, bool) { return 200, sseOK, true })
			rec := reqlog.New(reqlog.Config{})
			h := NewHandler(Config{Pool: testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}), Upstream: up, RequestLog: rec})
			w := &adapterDisconnectWriter{header: make(http.Header), failOn: failOn}
			h.ServeHTTP(w, adapterTestRequest(path, true))
			event := rec.Snapshot().Recent[0]
			if event.OK || event.Outcome != reqlog.OutcomeInterrupted {
				t.Fatalf("%s failure=%q event=%+v", path, failOn, event)
			}
			if failOn == "" && w.writes != 1 {
				t.Fatalf("continued writes after disconnect: %d", w.writes)
			}
		}
	}
}

func TestAnthropicTruncationReasonMatchesAcrossModes(t *testing.T) {
	upstreamBody := strings.Join([]string{
		`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"read","arguments":"{}"}}]},"finish_reason":"length"}]}`,
		`data: [DONE]`, "",
	}, "\n\n")
	for _, stream := range []bool{false, true} {
		up := newFakeUpstream(t, func(string) (int, string, bool) { return 200, upstreamBody, true })
		h := NewHandler(Config{Pool: testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}), Upstream: up})
		w := httptest.NewRecorder()
		h.ServeHTTP(w, adapterTestRequest("/v1/messages", stream))
		if w.Code != 200 || !strings.Contains(w.Body.String(), `"stop_reason":"max_tokens"`) {
			t.Fatalf("stream=%v HTTP=%d body=%s", stream, w.Code, w.Body.String())
		}
	}
}

func TestAnthropicAPIKeyDoesNotChangeExistingRoutes(t *testing.T) {
	h := NewHandler(Config{Pool: testPoolWith(), APIKey: "synthetic-key"})
	for _, path := range []string{"/v1/chat/completions", "/v1/responses", "/v1/messages"} {
		r := adapterTestRequest(path, false)
		r.Header.Set("x-api-key", "synthetic-key")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		want := 401
		if path == "/v1/messages" {
			want = 503
		}
		if w.Code != want {
			t.Fatalf("%s got HTTP %d body=%s", path, w.Code, w.Body.String())
		}
	}
}

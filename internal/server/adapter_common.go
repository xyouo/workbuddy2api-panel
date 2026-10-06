package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

type adapterRequestKey struct{}
type adapterRequestState struct{ stat *chatStat }

// Adapter requests finish their single chat log after output validation. Usage
// and account handling still belong to the existing Chat Completions handler.
func adapterRequest(r *http.Request) (*http.Request, func()) {
	state := &adapterRequestState{}
	return r.WithContext(context.WithValue(r.Context(), adapterRequestKey{}, state)), func() {
		if state.stat != nil {
			if trace := requestTraceFrom(r); trace != nil {
				state.stat.requestID = trace.id
			}
			state.stat.done()
		}
	}
}

func finishChatStat(r *http.Request, stat *chatStat) {
	if state, _ := r.Context().Value(adapterRequestKey{}).(*adapterRequestState); state != nil {
		state.stat = stat
		return
	}
	stat.done()
}

func setAdapterOutcome(r *http.Request, status int, outcome string) {
	if state, _ := r.Context().Value(adapterRequestKey{}).(*adapterRequestState); state != nil && state.stat != nil {
		state.stat.status, state.stat.outcome = status, outcome
	}
}

func chatMessage(chat map[string]any) (map[string]any, string) {
	choices, _ := chat["choices"].([]any)
	if len(choices) == 0 {
		return nil, ""
	}
	choice, _ := choices[0].(map[string]any)
	message, _ := choice["message"].(map[string]any)
	if message == nil {
		message, _ = choice["delta"].(map[string]any)
	}
	finish, _ := choice["finish_reason"].(string)
	return message, finish
}

func decodeToolArguments(id, name, args string, custom bool) (any, error) {
	if id == "" || name == "" {
		return nil, fmt.Errorf("upstream tool call is missing id or name")
	}
	if args == "" {
		return nil, fmt.Errorf("tool %q has empty arguments", name)
	}
	var value any
	if err := json.Unmarshal([]byte(args), &value); err != nil {
		return nil, fmt.Errorf("tool %q has invalid arguments JSON: %w", name, err)
	}
	if custom {
		obj, ok := value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("custom tool %q arguments must be an object", name)
		}
		if _, ok := obj["input"].(string); !ok {
			return nil, fmt.Errorf("custom tool %q arguments require string input", name)
		}
	}
	return value, nil
}

func anthropicStopReason(finish string, hasTools bool) string {
	if finish == "length" {
		return "max_tokens"
	}
	if finish == "tool_calls" || hasTools {
		return "tool_use"
	}
	return "end_turn"
}

func anthropicErrorType(status int) string {
	switch {
	case status == http.StatusUnauthorized:
		return "authentication_error"
	case status == http.StatusForbidden:
		return "permission_error"
	case status == http.StatusTooManyRequests:
		return "rate_limit_error"
	case status >= 500:
		return "api_error"
	default:
		return "invalid_request_error"
	}
}

func copyAdapterHeaders(w http.ResponseWriter, headers http.Header) {
	for name, values := range headers {
		// These describe the inner Chat body, which the adapter replaces.
		if strings.EqualFold(name, "Content-Length") || strings.EqualFold(name, "Transfer-Encoding") {
			continue
		}
		w.Header()[name] = append([]string(nil), values...)
	}
}

// Before a stream starts, preserve the Chat handler's status and retry headers.
// Only the error envelope changes for Anthropic clients.
func forwardAdapterError(w http.ResponseWriter, status int, headers http.Header, body []byte, anthropic bool) {
	copyAdapterHeaders(w, headers)
	if anthropic {
		message := strings.TrimSpace(string(body))
		var obj struct {
			Error struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if json.Unmarshal(body, &obj) == nil && obj.Error.Message != "" {
			message = obj.Error.Message
		}
		writeAnthropicError(w, status, message)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

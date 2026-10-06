package server

import (
	"net/http"
	"strings"

	"github.com/linguo2625469/workbuddy2api-panel/internal/httpauth"
)

// Keep x-api-key compatibility local to Anthropic routes. Bearer verification
// remains the shared upstream primitive, including its constant-time comparison.
func verifyAnthropicKey(r *http.Request, key string) bool {
	if strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
		return httpauth.VerifyBearer(r, key)
	}
	authRequest := r.Clone(r.Context())
	authRequest.Header.Set("Authorization", "Bearer "+r.Header.Get("x-api-key"))
	return httpauth.VerifyBearer(authRequest, key)
}

func (h *Handler) withAnthropicAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !verifyAnthropicKey(r, h.loadLive().APIKey) {
			writeAnthropicError(w, http.StatusUnauthorized, "missing or invalid API key")
			return
		}
		next(w, r)
	}
}

package middleware

import (
	"context"
	"net/http"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/google/uuid"
)

// RequestIDKey is the context key for the request ID.
// Matches the key expected by handler/httperror/requestIDFromContext.
var RequestIDKey = domain.RequestIDKey

// RequestID middleware generates a UUID request ID for every request and
// writes it to the X-Request-ID response header. If the client sends an
// X-Request-ID header, that value is used as-is (distributed trace
// propagation). The ID is stored in context for downstream use.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := r.Header.Get("X-Request-ID")
		if reqID == "" {
			reqID = uuid.NewString()
		}
		ctx := context.WithValue(r.Context(), RequestIDKey, reqID)
		w.Header().Set("X-Request-ID", reqID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// GetRequestID extracts the request ID from context.
// Returns "" if not set.
func GetRequestID(ctx context.Context) string {
	if id, ok := ctx.Value(RequestIDKey).(string); ok {
		return id
	}
	return ""
}

package httperror

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
)

// ErrorCode is a machine-readable SCREAMING_SNAKE_CASE identifier.
type ErrorCode string

const (
	CodeBadRequest         ErrorCode = "BAD_REQUEST"
	CodeUnauthorized       ErrorCode = "UNAUTHORIZED"
	CodeForbidden          ErrorCode = "FORBIDDEN"
	CodeNotFound           ErrorCode = "NOT_FOUND"
	CodeConflict           ErrorCode = "CONFLICT"
	CodeQuotaExceeded      ErrorCode = "QUOTA_EXCEEDED"
	CodePaymentRequired    ErrorCode = "PAYMENT_REQUIRED"
	CodeInternalError      ErrorCode = "INTERNAL_ERROR"
	CodeBadGateway         ErrorCode = "BAD_GATEWAY"
	CodePayloadTooLarge    ErrorCode = "PAYLOAD_TOO_LARGE"
	CodeServiceUnavailable ErrorCode = "SERVICE_UNAVAILABLE"
)

// ErrorResponse is the canonical JSON error envelope.
// All 4xx responses include the real message. All 5xx responses
// return message "internal error" to avoid leaking internals.
// The request_id field enables correlation with server-side traces.
type ErrorResponse struct {
	Error ErrorDetail `json:"error"`
}

type ErrorDetail struct {
	Code      ErrorCode `json:"code"`
	Message   string    `json:"message"`
	RequestID string    `json:"request_id,omitempty"`
}

func write(w http.ResponseWriter, code ErrorCode, message string, httpStatus int, requestID string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	if err := json.NewEncoder(w).Encode(ErrorResponse{Error: ErrorDetail{Code: code, Message: message, RequestID: requestID}}); err != nil {
		log.Printf("httperror: failed to encode error response: %v", err)
	}
}

// requestIDKey is the context key for the request ID. It must match the
// key used by middleware/tracing.go. Using a plain string is sufficient
// since context keys are compared by value, not identity.
const requestIDKey = "request_id"

// requestIDFromContext extracts the request ID from context, or returns "".
func requestIDFromContext(ctx context.Context) string {
	if id, ok := ctx.Value(requestIDKey).(string); ok {
		return id
	}
	return ""
}

// Ctx variants — use when you have access to the request context.
// These populate the request_id field in the JSON envelope for client-side
// trace correlation.

// BadRequest reports a malformed request (HTTP 400).
func BadRequest(w http.ResponseWriter, message string) {
	write(w, CodeBadRequest, message, http.StatusBadRequest, "")
}

// BadRequestCtx reports a malformed request with trace context (HTTP 400).
func BadRequestCtx(w http.ResponseWriter, r *http.Request, message string) {
	write(w, CodeBadRequest, message, http.StatusBadRequest, requestIDFromContext(r.Context()))
}

// Unauthorized reports a missing or invalid credential (HTTP 401).
func Unauthorized(w http.ResponseWriter, message string) {
	write(w, CodeUnauthorized, message, http.StatusUnauthorized, "")
}

// UnauthorizedCtx reports a missing or invalid credential with trace context (HTTP 401).
func UnauthorizedCtx(w http.ResponseWriter, r *http.Request, message string) {
	write(w, CodeUnauthorized, message, http.StatusUnauthorized, requestIDFromContext(r.Context()))
}

// Forbidden reports insufficient permissions (HTTP 403).
func Forbidden(w http.ResponseWriter, message string) {
	write(w, CodeForbidden, message, http.StatusForbidden, "")
}

// ForbiddenCtx reports insufficient permissions with trace context (HTTP 403).
func ForbiddenCtx(w http.ResponseWriter, r *http.Request, message string) {
	write(w, CodeForbidden, message, http.StatusForbidden, requestIDFromContext(r.Context()))
}

// NotFound reports a missing resource (HTTP 404).
func NotFound(w http.ResponseWriter, message string) {
	write(w, CodeNotFound, message, http.StatusNotFound, "")
}

// NotFoundCtx reports a missing resource with trace context (HTTP 404).
func NotFoundCtx(w http.ResponseWriter, r *http.Request, message string) {
	write(w, CodeNotFound, message, http.StatusNotFound, requestIDFromContext(r.Context()))
}

// Conflict reports a state conflict such as duplicate creation (HTTP 409).
func Conflict(w http.ResponseWriter, message string) {
	write(w, CodeConflict, message, http.StatusConflict, "")
}

// ConflictCtx reports a state conflict with trace context (HTTP 409).
func ConflictCtx(w http.ResponseWriter, r *http.Request, message string) {
	write(w, CodeConflict, message, http.StatusConflict, requestIDFromContext(r.Context()))
}

// QuotaExceeded reports a quota limit hit (HTTP 429).
func QuotaExceeded(w http.ResponseWriter, message string) {
	write(w, CodeQuotaExceeded, message, http.StatusTooManyRequests, "")
}

// QuotaExceededCtx reports a quota limit hit with trace context (HTTP 429).
func QuotaExceededCtx(w http.ResponseWriter, r *http.Request, message string) {
	write(w, CodeQuotaExceeded, message, http.StatusTooManyRequests, requestIDFromContext(r.Context()))
}

// PaymentRequired reports a billing-boundary violation (HTTP 402). Used
// by the deploy-time enforcement path (issue #420) when a tenant is
// over their cap, has a past_due/canceled subscription, or is in
// free-tier lockdown. Distinct from QuotaExceeded (429): 402 says
// "this is a billing boundary, waiting won't fix it" while 429 says
// "you're rate-limited, back off and retry".
func PaymentRequired(w http.ResponseWriter, message string) {
	write(w, CodePaymentRequired, message, http.StatusPaymentRequired, "")
}

// PaymentRequiredCtx reports a billing-boundary violation with trace context (HTTP 402).
func PaymentRequiredCtx(w http.ResponseWriter, r *http.Request, message string) {
	write(w, CodePaymentRequired, message, http.StatusPaymentRequired, requestIDFromContext(r.Context()))
}

// InternalError reports an unspecified server fault (HTTP 500).
// Use this when logging has already captured the real error; the client
// always sees "internal error" regardless of what happened.
func InternalError(w http.ResponseWriter) {
	write(w, CodeInternalError, "internal error", http.StatusInternalServerError, "")
}

// InternalErrorCtx reports an unspecified server fault with trace context (HTTP 500).
func InternalErrorCtx(w http.ResponseWriter, r *http.Request) {
	write(w, CodeInternalError, "internal error", http.StatusInternalServerError, requestIDFromContext(r.Context()))
}

// PayloadTooLarge reports an oversize request body or response stream (HTTP 413).
func PayloadTooLarge(w http.ResponseWriter, message string) {
	write(w, CodePayloadTooLarge, message, http.StatusRequestEntityTooLarge, "")
}

// PayloadTooLargeCtx reports an oversize request body or response stream with trace context (HTTP 413).
func PayloadTooLargeCtx(w http.ResponseWriter, r *http.Request, message string) {
	write(w, CodePayloadTooLarge, message, http.StatusRequestEntityTooLarge, requestIDFromContext(r.Context()))
}

// writeWithDetails extends write() with arbitrary top-level fields
// merged into the envelope alongside the typed error block. Used for
// 502s (and any future error type) that carry extra structured detail
// the client should see.
//
// The "error" key is reserved — callers MUST NOT pass a detail key
// that would shadow it. Such keys are silently dropped so a caller
// bug never produces a malformed envelope.
func writeWithDetails(w http.ResponseWriter, code ErrorCode, message string, httpStatus int, requestID string, details map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	body := map[string]any{
		"error": ErrorDetail{Code: code, Message: message, RequestID: requestID},
	}
	for k, v := range details {
		if k == "error" {
			continue
		}
		body[k] = v
	}
	_ = json.NewEncoder(w).Encode(body)
}

// BadGateway reports an upstream-dependency failure (HTTP 502). Pass
// nil for details to emit the standard shape; otherwise detail keys
// are merged at the top level alongside the error block.
func BadGateway(w http.ResponseWriter, message string, details map[string]any) {
	writeWithDetails(w, CodeBadGateway, message, http.StatusBadGateway, "", details)
}

// BadGatewayCtx reports an upstream-dependency failure with trace context (HTTP 502).
func BadGatewayCtx(w http.ResponseWriter, r *http.Request, message string, details map[string]any) {
	writeWithDetails(w, CodeBadGateway, message, http.StatusBadGateway,
		requestIDFromContext(r.Context()), details)
}

// WriteCtx writes an error response with trace context using a custom
// HTTP status code. Used by middleware that needs a status not covered
// by the named helpers above (e.g. 503 Service Unavailable). The named
// helpers (BadRequest, Unauthorized, etc.) are preferred where they
// apply; this is the escape hatch.
func WriteCtx(w http.ResponseWriter, r *http.Request, httpStatus int, message string) {
	// Map common HTTP statuses to their canonical ErrorCode. Anything
	// outside this map gets a generic INTERNAL_ERROR code so the
	// response shape stays consistent.
	var code ErrorCode
	switch httpStatus {
	case http.StatusBadRequest:
		code = CodeBadRequest
	case http.StatusUnauthorized:
		code = CodeUnauthorized
	case http.StatusForbidden:
		code = CodeForbidden
	case http.StatusNotFound:
		code = CodeNotFound
	case http.StatusConflict:
		code = CodeConflict
	case http.StatusTooManyRequests:
		code = CodeQuotaExceeded
	case http.StatusPaymentRequired:
		code = CodePaymentRequired
	case http.StatusServiceUnavailable:
		code = CodeServiceUnavailable
	default:
		code = CodeInternalError
	}
	write(w, code, message, httpStatus, requestIDFromContext(r.Context()))
}

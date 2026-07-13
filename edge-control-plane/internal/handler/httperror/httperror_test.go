package httperror

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWrite_StatusCodeMapping(t *testing.T) {
	tests := []struct {
		name       string
		code       ErrorCode
		wantStatus int
	}{
		{"BadRequest", CodeBadRequest, http.StatusBadRequest},
		{"Unauthorized", CodeUnauthorized, http.StatusUnauthorized},
		{"Forbidden", CodeForbidden, http.StatusForbidden},
		{"NotFound", CodeNotFound, http.StatusNotFound},
		{"Conflict", CodeConflict, http.StatusConflict},
		{"QuotaExceeded", CodeQuotaExceeded, http.StatusTooManyRequests},
		{"InternalError", CodeInternalError, http.StatusInternalServerError},
		{"PayloadTooLarge", CodePayloadTooLarge, http.StatusRequestEntityTooLarge},
		{"BadGateway", CodeBadGateway, http.StatusBadGateway},
		{"PreflightDenied", CodePreflightDenied, http.StatusUnprocessableEntity},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			write(w, tt.code, "test message", tt.wantStatus, "")
			resp := w.Result()
			if resp.StatusCode != tt.wantStatus {
				t.Errorf("status = %d, want %d", resp.StatusCode, tt.wantStatus)
			}
		})
	}
}

func TestWrite_ResponseShape(t *testing.T) {
	w := httptest.NewRecorder()
	write(w, CodeBadRequest, "missing field", http.StatusBadRequest, "req-123")
	resp := w.Result()
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var body map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	errObj, ok := body["error"].(map[string]interface{})
	if !ok {
		t.Fatalf("missing 'error' key in body: %+v", body)
	}
	if errObj["code"] != string(CodeBadRequest) {
		t.Errorf("code = %v, want %v", errObj["code"], CodeBadRequest)
	}
	if errObj["message"] != "missing field" {
		t.Errorf("message = %v, want 'missing field'", errObj["message"])
	}
	if errObj["request_id"] != "req-123" {
		t.Errorf("request_id = %v, want 'req-123'", errObj["request_id"])
	}
}

func TestCtxVariants_PopulateRequestID(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/test", nil)
	ctx := context.WithValue(r.Context(), requestIDKey, "ctx-req-456") //nolint:staticcheck // production uses bare string key
	r = r.WithContext(ctx)

	tests := []struct {
		name string
		call func(w http.ResponseWriter)
	}{
		{
			"BadRequestCtx",
			func(w http.ResponseWriter) { BadRequestCtx(w, r, "bad input") },
		},
		{
			"UnauthorizedCtx",
			func(w http.ResponseWriter) { UnauthorizedCtx(w, r, "no access") },
		},
		{
			"ForbiddenCtx",
			func(w http.ResponseWriter) { ForbiddenCtx(w, r, "denied") },
		},
		{
			"NotFoundCtx",
			func(w http.ResponseWriter) { NotFoundCtx(w, r, "gone") },
		},
		{
			"ConflictCtx",
			func(w http.ResponseWriter) { ConflictCtx(w, r, "dup") },
		},
		{
			"QuotaExceededCtx",
			func(w http.ResponseWriter) { QuotaExceededCtx(w, r, "rate") },
		},
		{
			"InternalErrorCtx",
			func(w http.ResponseWriter) { InternalErrorCtx(w, r) },
		},
		{
			"PayloadTooLargeCtx",
			func(w http.ResponseWriter) { PayloadTooLargeCtx(w, r, "oversize") },
		},
		{
			"BadGatewayCtx",
			func(w http.ResponseWriter) { BadGatewayCtx(w, r, "upstream down", nil) },
		},
		{
			"PreflightDeniedCtx",
			func(w http.ResponseWriter) { PreflightDeniedCtx(w, r, "compile-time host-reach macro detected", nil) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			tt.call(w)
			var body map[string]interface{}
			if err := json.NewDecoder(w.Result().Body).Decode(&body); err != nil {
				t.Fatalf("decode: %v", err)
			}
			errObj := body["error"].(map[string]interface{})
			if errObj["request_id"] != "ctx-req-456" {
				t.Errorf("request_id = %v, want 'ctx-req-456'", errObj["request_id"])
			}
		})
	}
}

func TestNonCtxVariants_NoRequestID(t *testing.T) {
	w := httptest.NewRecorder()
	BadRequest(w, "plain")
	var body map[string]interface{}
	if err := json.NewDecoder(w.Result().Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	errObj := body["error"].(map[string]interface{})
	if rid, ok := errObj["request_id"].(string); ok && rid != "" {
		t.Errorf("request_id = %q, want empty/absent", rid)
	}
}

func TestInternalError_MessageIsSanitized(t *testing.T) {
	w := httptest.NewRecorder()
	InternalError(w)
	var body map[string]interface{}
	if err := json.NewDecoder(w.Result().Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	errObj := body["error"].(map[string]interface{})
	if errObj["message"] != "internal error" {
		t.Errorf("message = %q, want 'internal error'", errObj["message"])
	}
}

func TestBadGateway_MergesDetailKeys(t *testing.T) {
	w := httptest.NewRecorder()
	BadGateway(w, "upstream timeout", map[string]any{
		"upstream":   "https://backend",
		"timeout_ms": float64(5000),
	})
	var body map[string]interface{}
	if err := json.NewDecoder(w.Result().Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["upstream"] != "https://backend" {
		t.Errorf("upstream = %v, want 'https://backend'", body["upstream"])
	}
	if body["timeout_ms"] != float64(5000) {
		t.Errorf("timeout_ms = %v, want 5000", body["timeout_ms"])
	}
	// error key must still be present
	if _, ok := body["error"]; !ok {
		t.Fatal("missing 'error' key")
	}
}

func TestBadGateway_ErrorKeyShadowIsDropped(t *testing.T) {
	w := httptest.NewRecorder()
	BadGateway(w, "gateway timeout", map[string]any{
		"error": "user-supplied error override",
		"retry": true,
	})
	var body map[string]interface{}
	if err := json.NewDecoder(w.Result().Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	errObj, ok := body["error"].(map[string]interface{})
	if !ok {
		t.Fatal("missing 'error' key")
	}
	// The user-supplied "error" detail should NOT replace the real error block
	if errObj["code"] != string(CodeBadGateway) {
		t.Errorf("code = %v, want %v", errObj["code"], CodeBadGateway)
	}
	// retry should still be present
	if body["retry"] != true {
		t.Errorf("retry = %v, want true", body["retry"])
	}
}

func TestCtxVariant_NoRequestIDInContext_ReturnsEmpty(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/test", nil)
	// No request_id in context
	w := httptest.NewRecorder()
	BadRequestCtx(w, r, "test")
	var body map[string]interface{}
	if err := json.NewDecoder(w.Result().Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	errObj := body["error"].(map[string]interface{})
	// request_id should be absent or empty
	if rid, ok := errObj["request_id"].(string); ok && rid != "" {
		t.Errorf("request_id = %q, want empty/absent", rid)
	}
}

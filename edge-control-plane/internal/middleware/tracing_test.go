package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRequestID_GeneratesWhenMissing(t *testing.T) {
	var capturedID string
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedID = GetRequestID(r.Context())
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rr := httptest.NewRecorder()
	RequestID(next).ServeHTTP(rr, req)

	if capturedID == "" {
		t.Fatal("expected non-empty request ID when header is missing")
	}
	respID := rr.Header().Get("X-Request-ID")
	if respID != capturedID {
		t.Errorf("response X-Request-ID = %q, want %q (same as context value)", respID, capturedID)
	}
}

func TestRequestID_PassesThroughExisting(t *testing.T) {
	const existingID = "trace-abc-123"
	var capturedID string
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedID = GetRequestID(r.Context())
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("X-Request-ID", existingID)
	rr := httptest.NewRecorder()
	RequestID(next).ServeHTTP(rr, req)

	if capturedID != existingID {
		t.Errorf("captured ID = %q, want %q", capturedID, existingID)
	}
	respID := rr.Header().Get("X-Request-ID")
	if respID != existingID {
		t.Errorf("response X-Request-ID = %q, want %q", respID, existingID)
	}
}

func TestRequestID_StoredInContext(t *testing.T) {
	var capturedID string
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedID = GetRequestID(r.Context())
		w.WriteHeader(http.StatusNoContent)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rr := httptest.NewRecorder()
	RequestID(next).ServeHTTP(rr, req)

	if capturedID == "" {
		t.Fatal("GetRequestID returned empty string; expected a UUID")
	}
	if rr.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204", rr.Code)
	}
}

func TestGetRequestID_FromContext(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("X-Request-ID", "req-456")
	rr := httptest.NewRecorder()

	var capturedID string
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedID = GetRequestID(r.Context())
	})
	RequestID(next).ServeHTTP(rr, req)

	if capturedID != "req-456" {
		t.Errorf("GetRequestID = %q, want 'req-456'", capturedID)
	}
}

func TestGetRequestID_EmptyContext(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	// Without middleware, context has no request_id
	id := GetRequestID(req.Context())
	if id != "" {
		t.Errorf("GetRequestID = %q, want '' (empty context)", id)
	}
}

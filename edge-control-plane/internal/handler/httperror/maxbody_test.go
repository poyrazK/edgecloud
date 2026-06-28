package httperror

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMaxBodyBytes_Exceeded_WritesResponse(t *testing.T) {
	w := httptest.NewRecorder()
	exceeded := MaxBodyBytes(w, &http.MaxBytesError{Limit: 1024}, http.StatusRequestEntityTooLarge, "request body too large")
	if !exceeded {
		t.Fatal("expected true for MaxBytesError")
	}
	resp := w.Result()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusRequestEntityTooLarge)
	}
	body := w.Body.String()
	if body != `{"error":"request body too large"}`+"\n" {
		t.Errorf("body = %q", body)
	}
}

func TestMaxBodyBytes_DifferentStatusAndMessage(t *testing.T) {
	w := httptest.NewRecorder()
	exceeded := MaxBodyBytes(w, &http.MaxBytesError{Limit: 100}, http.StatusBadRequest, "batch too large")
	if !exceeded {
		t.Fatal("expected true for MaxBytesError")
	}
	resp := w.Result()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
	body := w.Body.String()
	if body != `{"error":"batch too large"}`+"\n" {
		t.Errorf("body = %q", body)
	}
}

func TestMaxBodyBytes_OtherError_ReturnsFalse(t *testing.T) {
	w := httptest.NewRecorder()
	exceeded := MaxBodyBytes(w, errors.New("some io error"), http.StatusBadRequest, "msg")
	if exceeded {
		t.Fatal("expected false for non-MaxBytesError")
	}
	resp := w.Result()
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200 (no write)", resp.StatusCode)
	}
}

func TestMaxBodyBytes_NilError_ReturnsFalse(t *testing.T) {
	w := httptest.NewRecorder()
	exceeded := MaxBodyBytes(w, nil, http.StatusBadRequest, "msg")
	if exceeded {
		t.Fatal("expected false for nil error")
	}
	resp := w.Result()
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200 (no write)", resp.StatusCode)
	}
}

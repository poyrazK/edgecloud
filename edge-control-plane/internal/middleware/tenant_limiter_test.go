package middleware

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestTenantCreationLimiter_AllowsUnderLimit(t *testing.T) {
	l := NewTenantCreationLimiter(3, time.Hour)
	defer l.Stop()

	for i := 0; i < 3; i++ {
		if !l.Allow("10.0.0.1") {
			t.Errorf("attempt %d: Allow returned false, want true", i+1)
		}
		l.Record("10.0.0.1")
	}
}

func TestTenantCreationLimiter_BlocksExcess(t *testing.T) {
	l := NewTenantCreationLimiter(2, time.Hour)
	defer l.Stop()

	for i := 0; i < 2; i++ {
		l.Allow("10.0.0.1")
		l.Record("10.0.0.1")
	}

	if l.Allow("10.0.0.1") {
		t.Error("Allow returned true after 2 creations, want false")
	}
}

func TestTenantCreationLimiter_DifferentIPsIndependent(t *testing.T) {
	l := NewTenantCreationLimiter(3, time.Hour)
	defer l.Stop()

	// IP A hits the limit
	for i := 0; i < 3; i++ {
		l.Allow("10.0.0.1")
		l.Record("10.0.0.1")
	}
	// IP B only uses 2 of 3
	for i := 0; i < 2; i++ {
		l.Allow("10.0.0.2")
		l.Record("10.0.0.2")
	}

	if l.Allow("10.0.0.1") {
		t.Error("10.0.0.1 should be blocked (3 creations)")
	}
	if !l.Allow("10.0.0.2") {
		t.Error("10.0.0.2 should still be allowed (only 2 creations)")
	}
}

func TestTenantCreationLimiter_ResetsAfterWindow(t *testing.T) {
	l := NewTenantCreationLimiter(1, 50*time.Millisecond)
	defer l.Stop()

	l.Allow("10.0.0.1")
	l.Record("10.0.0.1")

	if l.Allow("10.0.0.1") {
		t.Error("Allow should be false immediately after 1 creation")
	}

	time.Sleep(60 * time.Millisecond)

	if !l.Allow("10.0.0.1") {
		t.Error("Allow should be true after window expires")
	}
}

func TestTenantCreationLimiter_AllowWithoutRecord(t *testing.T) {
	l := NewTenantCreationLimiter(2, time.Hour)
	defer l.Stop()

	// Allow without Record should not count toward the limit.
	for i := 0; i < 5; i++ {
		if !l.Allow("10.0.0.1") {
			t.Fatalf("iteration %d: Allow returned false after only calling Allow (no Record)", i+1)
		}
	}
}

func TestTenantCreationLimiter_Middleware_Returns429(t *testing.T) {
	l := NewTenantCreationLimiter(1, time.Hour)
	defer l.Stop()

	// First request succeeds
	req := httptest.NewRequest("POST", "/api/v1/tenants", nil)
	req.RemoteAddr = "10.0.0.1:12345"
	rr := httptest.NewRecorder()
	l.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	})).ServeHTTP(rr, req)
	if rr.Code != http.StatusCreated {
		t.Errorf("first request: status %d, want 201", rr.Code)
	}
	l.Record("10.0.0.1")

	// Second request from same IP gets 429
	req2 := httptest.NewRequest("POST", "/api/v1/tenants", nil)
	req2.RemoteAddr = "10.0.0.1:12345"
	rr2 := httptest.NewRecorder()
	l.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	})).ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusTooManyRequests {
		t.Errorf("second request: status %d, want 429", rr2.Code)
	}
}

func TestTenantCreationLimiter_ConcurrentSafe(t *testing.T) {
	l := NewTenantCreationLimiter(1000, time.Hour)
	defer l.Stop()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if l.Allow("10.0.0.1") {
				l.Record("10.0.0.1")
			}
		}()
	}
	wg.Wait()

	// After 100 concurrent operations, we should have at most 1000 records.
	// The test must not panic or deadlock. Check that state is consistent.
	l.Allow("10.0.0.1") // still safe to call
}

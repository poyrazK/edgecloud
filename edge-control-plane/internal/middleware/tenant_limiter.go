package middleware

import (
	"net/http"
	"sync"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
)

// TenantCreationLimiter limits how many tenants a single client IP can
// create within a sliding time window. This prevents automated abuse of
// the self-signup endpoint without relying solely on the token-bucket
// rate limiter (which is per-second and trivially bypassed by spreading
// requests over time).
//
// A background goroutine evicts stale entries every 10 minutes.
type TenantCreationLimiter struct {
	mu      sync.Mutex
	entries map[string][]time.Time // IP → creation timestamps
	max     int                    // max creations per window
	window  time.Duration          // sliding window
	stopCh  chan struct{}
}

// NewTenantCreationLimiter creates a limiter allowing up to `max`
// creations per `window` for each client IP.
func NewTenantCreationLimiter(max int, window time.Duration) *TenantCreationLimiter {
	l := &TenantCreationLimiter{
		entries: make(map[string][]time.Time),
		max:     max,
		window:  window,
		stopCh:  make(chan struct{}),
	}
	go l.gcLoop()
	return l
}

// Allow reports whether the given IP is allowed to create a tenant.
func (l *TenantCreationLimiter) Allow(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	cutoff := time.Now().Add(-l.window)
	times := l.entries[ip]

	// Prune entries outside the window (lazy GC).
	var active []time.Time
	for _, t := range times {
		if t.After(cutoff) {
			active = append(active, t)
		}
	}

	if len(active) >= l.max {
		l.entries[ip] = active
		return false
	}

	// Don't record yet — Record() is called after a successful creation.
	// Store the pruned list so the next Allow sees accurate counts.
	l.entries[ip] = active
	return true
}

// Record records a successful tenant creation for the given IP.
// Must be called after Allow returned true AND the creation succeeded.
func (l *TenantCreationLimiter) Record(ip string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.entries[ip] = append(l.entries[ip], time.Now())
}

// Middleware returns an HTTP middleware that checks the creation limit
// before passing the request to the next handler. When the limit is
// exceeded, it responds with 429 Too Many Requests.
func (l *TenantCreationLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := ClientIP(r)
		if ip == "" {
			next.ServeHTTP(w, r)
			return
		}
		if !l.Allow(ip) {
			w.Header().Set("Retry-After", "60")
			httperror.QuotaExceededCtx(w, r, "too many tenants created from this IP; try again later")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// gcLoop prunes stale entries every 10 minutes.
func (l *TenantCreationLimiter) gcLoop() {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			l.GC()
		case <-l.stopCh:
			return
		}
	}
}

// GC removes entries older than the sliding window.
func (l *TenantCreationLimiter) GC() {
	l.mu.Lock()
	defer l.mu.Unlock()

	cutoff := time.Now().Add(-l.window)
	for ip, times := range l.entries {
		var active []time.Time
		for _, t := range times {
			if t.After(cutoff) {
				active = append(active, t)
			}
		}
		if len(active) == 0 {
			delete(l.entries, ip)
		} else {
			l.entries[ip] = active
		}
	}
}

// Stop shuts down the background GC goroutine.
func (l *TenantCreationLimiter) Stop() {
	select {
	case <-l.stopCh:
	default:
		close(l.stopCh)
	}
}

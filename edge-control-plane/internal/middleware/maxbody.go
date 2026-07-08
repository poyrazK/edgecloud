package middleware

import "net/http"

// MaxBodyBytes returns a middleware that wraps r.Body in
// http.MaxBytesReader with the given cap. Per-handler tighter caps
// compose — MaxBytesReader's wrapper chain means the smallest
// cap in the chain wins (so wrapping at the mux level here does
// NOT override a tighter per-handler wrap like Migrate's 50 MiB
// or IngestLogs's 1 MiB; both are enforced, the smaller returns
// *http.MaxBytesError first).
//
// Applied at the outermost mux so handlers that forgot their own
// cap (or don't read body at all) are still bounded — the
// unauthenticated POST /api/v1/tenants and POST /api/v1/keys
// endpoints otherwise let a multi-GiB body reach the handler
// before any limit applies. The default cap is
// service.MaxArtifactSize (100 MiB), which is large enough that
// the only consumer affected by the floor is the Deploy handler
// (whose own 100 MiB wrap composes to 100 MiB).
//
// A handler that reads r.Body with the default GET semantics gets
// a no-op (the body is empty); GET requests are unaffected.
func MaxBodyBytes(n int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r.Body = http.MaxBytesReader(w, r.Body, n)
			next.ServeHTTP(w, r)
		})
	}
}

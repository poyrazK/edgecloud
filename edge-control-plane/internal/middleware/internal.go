package middleware

import (
	"crypto/subtle"
	"net/http"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
)

// InternalAuth verifies a shared-secret header on requests from trusted
// service-to-service callers. Today this gates the traffic-split read
// endpoint that the edge-ingress polls to apply Caddy weights.
//
// The middleware is intentionally distinct from WorkerAuth (HMAC JWT) and
// AuthMiddleware (tenant API key). Workers don't fetch traffic splits and
// tenants don't either — this is purely the ingress's lane. Keeping it
// separate avoids minting a synthetic worker JWT for an ingress that's
// not a worker, and avoids exposing traffic splits to tenant tokens.
//
// `expectedToken` is the configured shared secret. A constant-time
// comparison guards against timing oracles; an empty `expectedToken`
// fail-closes (rejects every request) so a misconfigured control plane
// never accidentally exposes the protected endpoint.
//
// The header is `X-Internal-Token` rather than `Authorization: Bearer …`
// so operators reading access logs can immediately distinguish this
// service-to-service traffic from tenant or worker traffic without
// parsing the bearer scheme.
func InternalAuth(expectedToken string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if expectedToken == "" {
				httperror.UnauthorizedCtx(w, r, "internal auth not configured")
				return
			}
			got := r.Header.Get("X-Internal-Token")
			if got == "" {
				httperror.UnauthorizedCtx(w, r, "missing internal token")
				return
			}
			// Constant-time compare so an attacker can't probe the
			// server's response time to learn the leading bytes of the
			// expected secret.
			if subtle.ConstantTimeCompare([]byte(got), []byte(expectedToken)) != 1 {
				httperror.UnauthorizedCtx(w, r, "invalid internal token")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

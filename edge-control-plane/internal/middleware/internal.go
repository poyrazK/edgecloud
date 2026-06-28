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

// InternalOrWorkerAuth accepts either a valid worker JWT (Bearer in
// Authorization) OR an X-Internal-Token header matching `expectedToken`.
// It exists so the artifact download endpoint at
// `/api/internal/download/{deploymentID}` can serve BOTH:
//   - edge-workers, which present a 24h HMAC JWT (zero changes to the
//     worker — same flow as today), AND
//   - peer control planes, which present the operator-configured shared
//     secret via RemoteArtifactStore's pull-through cache.
//
// The two lanes are independent: a worker presenting only the JWT still
// gets through, a peer CP presenting only the token still gets through,
// and a request presenting both is accepted (worker first, token as
// fallback for requests without Authorization).
//
// Fail-closed: if `expectedToken` is empty, the token lane is disabled —
// only worker JWTs are accepted. This means a misconfigured operator
// (forgot to set EDGE_INTERNAL_TOKEN on the receiving CP) cannot
// accidentally widen access; the token lane silently refuses every
// request and the peer CP's pull-through just 404s on the missing
// artifact. Workers are unaffected.
//
// The handler-level tenant scoping (worker.JWT.TenantID must match the
// deployment's tenant_id) is NOT enforced here — it's the download
// handler's responsibility. This middleware only authenticates the
// caller; authorization lives one layer up where the deployment row
// can be looked up.
func InternalOrWorkerAuth(workerCfg WorkerJWTConfig, expectedToken string) func(http.Handler) http.Handler {
	workerGate := WorkerAuth(workerCfg)
	tokenGate := InternalAuth(expectedToken)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Lane 1: worker JWT. If the request has an Authorization
			// header, let WorkerAuth decide (it 401s on missing/invalid
			// token — we don't fall through to the token lane in that
			// case, since presenting an Authorization header is an
			// explicit "I am a worker" signal).
			if r.Header.Get("Authorization") != "" {
				workerGate(next).ServeHTTP(w, r)
				return
			}
			// Lane 2: shared-secret internal token. Delegated entirely
			// to InternalAuth, which already fail-closes on empty
			// expectedToken and does the constant-time compare.
			tokenGate(next).ServeHTTP(w, r)
		})
	}
}

package handler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"mime/multipart"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/provenance"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/storage"
)

// idempotencyKeyFormat (issue #52) is the byte-shape the
// Idempotency-Key header must match. [a-fA-F0-9-]{8,128} admits
// UUID v4 (8-4-4-4-12 = 36 chars with hyphens) while staying
// narrow enough to keep the lookup index selective — a 128-char
// upper bound gives callers room to use any future ID scheme
// (ULID, KSUID, etc.) without re-asking the server.
//
// Rejecting malformed keys with 400 is the right move: the
// value can't be reshaped into something useful, and a
// degenerate key in the replay cache would invite either an
// infinite cache or a hash-collision surface.
var idempotencyKeyFormat = regexp.MustCompile(`^[a-fA-F0-9-]{8,128}$`)

// DeploymentHandler handles deployment HTTP requests.
type DeploymentHandler struct {
	deploymentSvc *service.DeploymentService
	workerSvc     service.AppTargetLookup
	trafficSvc    *service.TrafficService
	rollbackSvc   deploymentRollbacker
	activateSvc   deploymentActivator
	promoteSvc    deploymentPromoter
	// artifactStore is used by the precompile step to read .wasm and write .cwasm.
	artifactStore storage.ArtifactStore
	// wasm2cwasmPath is the path to the wasm2cwasm binary for AOT pre-compilation.
	// Empty = skip pre-compilation.
	wasm2cwasmPath string
}

// deploymentRollbacker is the narrow contract the Rollback handler needs.
// Kept package-local so handler tests can implement it inline without
// having to mock the full DeploymentService surface.
type deploymentRollbacker interface {
	RollbackDeployment(ctx context.Context, tenantID, appName string) (string, error)
}

// deploymentActivator is the narrow contract the Activate handler needs.
// Mirrors deploymentRollbacker for the activate path.
type deploymentActivator interface {
	ActivateDeployment(ctx context.Context, tenantID, appName, deploymentID string) error
}

// deploymentPromoter is the narrow contract the Promote handler needs.
// PromoteDeployment activates a deployment under a different app name
// than it was originally deployed under (preview → production).
type deploymentPromoter interface {
	PromoteDeployment(ctx context.Context, tenantID, targetAppName, deploymentID string) error
}

func NewDeploymentHandler(deploymentSvc *service.DeploymentService, workerSvc service.AppTargetLookup, trafficSvc *service.TrafficService, artifactStore storage.ArtifactStore, wasm2cwasmPath string) *DeploymentHandler {
	return &DeploymentHandler{
		deploymentSvc:  deploymentSvc,
		workerSvc:      workerSvc,
		trafficSvc:     trafficSvc,
		rollbackSvc:    deploymentSvc,
		activateSvc:    deploymentSvc,
		promoteSvc:     deploymentSvc,
		artifactStore:  artifactStore,
		wasm2cwasmPath: wasm2cwasmPath,
	}
}

// deployResponse is the JSON shape returned by `POST /api/deploy/{appName}`.
// Typed (vs the prior anonymous `map[string]interface{}`) so the contract
// is explicit and the test asserts against a struct, not a string match.
type deployResponse struct {
	ID                  string   `json:"id"`
	Hash                string   `json:"hash"`
	URL                 string   `json:"url"`
	Regions             []string `json:"regions"`
	AutoRollbackEnabled bool     `json:"auto_rollback_enabled"`
	DesiredReplicas     int      `json:"desired_replicas"`
	// Preview metadata (issue #308). All three fields are
	// omitempty so non-preview deploys serialize identically to
	// pre-#308 responses. The CLI echoes them into
	// .edge/state.json so `edge status` can show the preview's
	// expiry. Empty strings / zero ints drop from the wire.
	PreviewID        string `json:"preview_id,omitempty"`
	PreviewPRNumber  int    `json:"preview_pr_number,omitempty"`
	PreviewExpiresAt string `json:"preview_expires_at,omitempty"` // RFC3339
}

// statusResponse is the JSON shape returned by
// `GET /api/v1/status/{deploymentID}`, `GET /api/v1/apps/{appName}/active`,
// and the items in `GET /api/v1/list/{appName}`. Typed (vs the prior
// anonymous `*domain.Deployment`) so the wire shape stays decoupled
// from the DB row — adding a column to `deployments` no longer leaks
// to clients, and the contract matches `openapi.yaml#Deployment`.
//
// The `URL` field is computed from `tenant_id` + `app_name` via
// `domain.IngressHost`, the same helper that powers `deployResponse.URL`,
// so every read endpoint surfaces the live ingress hostname.
type statusResponse struct {
	ID                  string          `json:"id"`
	TenantID            string          `json:"tenant_id"`
	AppName             string          `json:"app_name"`
	Status              string          `json:"status"`
	Hash                string          `json:"hash"`
	Signature           string          `json:"signature,omitempty"`
	SigningKeyID        string          `json:"signing_key_id,omitempty"`
	Regions             []string        `json:"regions"`
	AutoRollbackEnabled bool            `json:"auto_rollback_enabled"`
	DesiredReplicas     int             `json:"desired_replicas"`
	BuildAttestation    json.RawMessage `json:"build_attestation,omitempty"`
	PreviewID           string          `json:"preview_id,omitempty"`
	PreviewPRNumber     *int            `json:"preview_pr_number,omitempty"`
	PreviewExpiresAt    string          `json:"preview_expires_at,omitempty"` // RFC3339
	CreatedAt           string          `json:"created_at"`                   // RFC3339
	URL                 string          `json:"url"`
}

// deploymentListResponse is the envelope returned by
// `GET /api/v1/list/{appName}`. Mirrors the `DeploymentListResponse`
// schema in `openapi.yaml`.
type deploymentListResponse struct {
	Items  []statusResponse `json:"items"`
	Total  int              `json:"total"`
	Limit  int              `json:"limit"`
	Offset int              `json:"offset"`
}

// newStatusResponse maps a DB row to the typed wire DTO. The same
// mapper is used by `GetStatus`, `GetActive`, and `List` so the three
// endpoints can't drift in shape — any new field on the wire shows up
// in all three (or none), enforced by this single construction site.
//
// Precondition: `tenantID` MUST equal `d.TenantID`. All three call
// sites select the row with a WHERE clause on the auth-resolved
// tenant id, so this holds today. If a future caller passes a
// cross-tenant row (admin override), the body's `tenant_id` and the
// computed `url` will disagree — don't do that without adapting this
// mapper first.
func newStatusResponse(d *domain.Deployment, tenantID, appName string) statusResponse {
	r := statusResponse{
		ID:                  d.ID,
		TenantID:            d.TenantID,
		AppName:             d.AppName,
		Status:              d.Status,
		Hash:                d.Hash,
		Regions:             domain.StringArrayTo(d.Regions),
		AutoRollbackEnabled: d.AutoRollbackEnabled,
		DesiredReplicas:     d.DesiredReplicas,
		URL:                 "https://" + domain.IngressHost(tenantID, appName),
		CreatedAt:           d.CreatedAt.UTC().Format(time.RFC3339),
	}
	if d.Signature != "" {
		r.Signature = d.Signature
	}
	if d.SigningKeyID != "" {
		r.SigningKeyID = d.SigningKeyID
	}
	if len(d.BuildAttestation) > 0 {
		r.BuildAttestation = json.RawMessage(d.BuildAttestation)
	}
	if d.PreviewID != nil {
		r.PreviewID = *d.PreviewID
	}
	if d.PreviewPRNumber != nil {
		// Copy the int so the DTO doesn't share a pointer with the
		// DB row. The repo pool reuses domain.Deployment values; if
		// we aliased, a later mutation on the row would silently
		// change a response already on the wire. Cheaper than a
		// deep clone because the value is one word.
		n := *d.PreviewPRNumber
		r.PreviewPRNumber = &n
	}
	if d.PreviewExpiresAt != nil {
		r.PreviewExpiresAt = d.PreviewExpiresAt.UTC().Format(time.RFC3339)
	}
	return r
}

func (h *DeploymentHandler) Deploy(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")

	// Validate app name
	if !validateAppName(w, appName) {
		return
	}

	// Idempotency-Key (issue #52). Optional header that pins a
	// retry to the original deployment_id (200 + same body)
	// instead of minting a fresh row (201). Format is
	// [a-fA-F0-9-]{8,128} — narrow enough to keep the index
	// selective, wide enough to admit UUID v4 and any user
	// JID. An empty header means "no idempotency" and falls
	// through to fresh-deploy semantics, so a pre-#52 CLI on
	// a #52 server sees no behavior change.
	//
	// 400 (not 422) for a malformed key: the value can't be
	// reshaped into something useful; "go away and re-decide
	// whether you want idempotency" is the right message.
	idemKey := r.Header.Get("Idempotency-Key")
	if idemKey != "" && !idempotencyKeyFormat.MatchString(idemKey) {
		http.Error(w, `{"error":"invalid Idempotency-Key format (must match [a-fA-F0-9-]{8,128})"}`, http.StatusBadRequest)
		return
	}

	// Parse `?regions=us-east,eu-west`. Split on `,`, trim whitespace,
	// drop empties, dedupe (preserving first-seen order so the response
	// is stable). Invalid regions are caught at the service layer
	// (`IsValidRegion`) — we still return 400 from the handler with a
	// caller-friendly message because the service error string is
	// also surfaced via the internal log.
	regions, perr := parseRegions(r.URL.Query().Get("regions"))
	if perr != nil {
		http.Error(w, `{"error": "`+perr.Error()+`"}`, http.StatusBadRequest)
		return
	}

	// Parse `?auto-rollback=true|false`. Defaults to false. Uses
	// `strconv.ParseBool` so the user can pass any of the canonical
	// truthy strings ("1", "t", "true", "TRUE", …); a non-boolean
	// value returns 400 rather than being silently coerced to false
	// (silent coercion would let a typo like `?auto-rollback=ture`
	// disable a feature the tenant thought they had enabled).
	autoRollback, aperr := parseBoolQuery(r.URL.Query().Get("auto-rollback"), false)
	if aperr != nil {
		http.Error(w, `{"error": "`+aperr.Error()+`"}`, http.StatusBadRequest)
		return
	}

	// Parse `?replicas=N` (issue #316). Defaults to 0 (no threshold).
	// Must be a non-negative integer.
	desiredReplicas, rerr := parseIntQuery(r.URL.Query().Get("replicas"), 0)
	if rerr != nil {
		http.Error(w, `{"error": "`+rerr.Error()+`"}`, http.StatusBadRequest)
		return
	}

	// Parse preview metadata (issue #308). Three optional query
	// params:
	//   ?preview-id=<hex>           — 8..16 lowercase hex chars
	//   ?preview-pr-number=<int>    — >= 0
	//   ?preview-ttl=<duration>     — Go duration string (e.g. "24h")
	//
	// preview-id is the marker: setting it (with or without
	// preview-pr-number) makes the deploy a preview. Without
	// preview-id the request is a normal (non-preview) deploy
	// regardless of the other two params.
	//
	// preview-id + preview-pr-number are an OPTIONAL pair: a
	// laptop user running `edge deploy --preview` without a PR
	// context sets preview-id but not preview-pr-number.
	// Conversely, preview-pr-number without preview-id is
	// rejected — the pr number without a preview marker is a
	// client bug.
	//
	// preview-ttl defaults to PreviewDefaultTTL (7 days) when
	// preview-id is set. The handler resolves it to an absolute
	// time so the service layer doesn't need to import time math.
	previewOpts, perr := parsePreviewOpts(
		r.URL.Query().Get("preview-id"),
		r.URL.Query().Get("preview-pr-number"),
		r.URL.Query().Get("preview-ttl"),
	)
	if perr != nil {
		http.Error(w, `{"error": "`+perr.Error()+`"}`, http.StatusBadRequest)
		return
	}

	// PR2 — wire-format break: the deploy request is now
	// `multipart/form-data` with one required `file` part (the wasm
	// artifact bytes) and one optional `build_metadata` part (a JSON
	// object the CLI captured at build time). The raw-octet-stream
	// shape used by older CLIs is rejected with 415 — the CLI ships
	// alongside the server, so a wire break is acceptable per the
	// release notes for issue #307 PR2.
	//
	// The streaming multipart.Reader approach keeps the file
	// bytes flowing directly into the service's io.Reader, no
	// RAM buffering of the artifact. The optional `build_metadata`
	// part is parsed in-memory (a few KiB) so the service can
	// construct the SLSA L1 Statement envelope with toolchain
	// info — PR2.6 threads it through the service signature.
	mediaType, params, mterr := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if mterr != nil || !strings.HasPrefix(mediaType, "multipart/") {
		http.Error(w, `{"error":"deploy now requires multipart/form-data with a 'file' part (issue #307 PR2); upgrade the edge CLI"}`,
			http.StatusUnsupportedMediaType)
		return
	}
	boundary := params["boundary"]
	if boundary == "" {
		http.Error(w, `{"error":"missing multipart boundary"}`, http.StatusBadRequest)
		return
	}

	// Cap the request body at MaxArtifactSize. MaxBytesReader
	// surfaces *http.MaxBytesError mid-stream that we map to 413.
	r.Body = http.MaxBytesReader(w, r.Body, service.MaxArtifactSize)
	mr := multipart.NewReader(r.Body, boundary)

	filePart, buildMetadata, mperr := extractDeployParts(mr)
	if mperr != nil {
		// *http.MaxBytesError surfaces here when the multipart
		// body exceeds the cap. Map to 413 first; everything else
		// is 400 (malformed multipart).
		var maxErr *http.MaxBytesError
		if errors.As(mperr, &maxErr) {
			http.Error(w, `{"error":"artifact exceeds maximum size"}`, http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, `{"error":"`+mperr.Error()+`"}`, http.StatusBadRequest)
		return
	}
	defer func() { _ = filePart.Close() }()

	// Idempotency-Key artifact hash (issue #52). Read the
	// extracted filePart into a tee-buffer that mirrors
	// every byte to a SHA-256 hasher, then hand the service
	// an io.Reader over that buffer so its own streaming
	// SaveAndHash can re-read the bytes. The digest is the
	// "same key, different body" guard:
	//
	//   * Hashing the artifact (not the multipart envelope)
	//     keeps the contract stable across CLIs that vary
	//     the boundary string, swap build_metadata order,
	//     or stamp a fresh build time while rebuilding the
	//     same WASM. The unit of identity for a deploy is
	//     the artifact, not the wire envelope.
	//
	//   * Teeing into a buffer trades the streaming win
	//     for a bounded memory cost. MaxArtifactSize caps
	//     the body at 100 MiB; the buffer is bounded by
	//     the same MaxBytesReader cap on `r.Body`, so the
	//     worst case is one full-artifact buffer per
	//     in-flight deploy. Acceptable for the size class.
	var artifactBuf bytes.Buffer
	var artifactSHA [32]byte
	{
		hasher := sha256.New()
		mw := io.MultiWriter(&artifactBuf, hasher)
		written, copyErr := io.Copy(mw, filePart)
		if copyErr != nil {
			// Mid-stream read failure → 413 (the
			// MaxBytesReader cap surfaces here as
			// *http.MaxBytesError, but anything else is
			// also a bad-body 413 from the operator's POV).
			http.Error(w, `{"error":"artifact exceeds maximum size"}`, http.StatusRequestEntityTooLarge)
			return
		}
		if written == 0 {
			http.Error(w, `{"error":"deploy requires a non-empty file part"}`, http.StatusBadRequest)
			return
		}
		copy(artifactSHA[:], hasher.Sum(nil))
	}

	// Decode the optional `build_metadata` form field into a
	// CLISideMetadata struct. A missing or malformed value is
	// best-effort: the service builds an envelope with "unknown"
	// toolchain fields rather than refusing the deploy. We
	// intentionally swallow the error here — anything > 0 bytes
	// is a valid JSON object or an operator-typo; either way,
	// the deploy proceeds and the audit pipeline can flag the
	// missing provenance later.
	var cliMeta *provenance.CLISideMetadata
	if len(buildMetadata) > 0 {
		var parsed provenance.CLISideMetadata
		if jerr := json.Unmarshal(buildMetadata, &parsed); jerr == nil {
			cliMeta = &parsed
		}
	}

	// The service contract accepts an io.Reader over the
	// artifact bytes; the teeBuffer satisfies that. The
	// streaming win (no extra copy beyond what
	// extractDeployParts already does) is traded for the
	// replay-cache requirement to know the SHA-256 up front.
	deployment, fromCache, err := h.deploymentSvc.Deploy(r.Context(), tenantID, appName, &artifactBuf, regions, autoRollback, desiredReplicas, cliMeta, previewOpts, idemKey, artifactSHA)
	if err != nil {
		// *http.MaxBytesError surfaces from the service's streaming
		// reads when the body exceeds the cap (chunked uploads
		// without a Content-Length header). Map to 413.
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			http.Error(w, `{"error":"artifact exceeds maximum size"}`, http.StatusRequestEntityTooLarge)
			return
		}
		if errors.Is(err, service.ErrMaxDeploymentsQuotaExceeded) {
			httperror.QuotaExceededCtx(w, r, "max deployments quota exceeded")
			return
		}
		// Issue #420: deploy-time 402 PAYMENT_REQUIRED. The typed
		// PaymentRequiredError carries a stable reason code
		// (subscription_past_due, quota_will_be_exceeded, etc.) that
		// the client can route on; we surface it as the message so
		// the response shape stays aligned with the rest of the
		// httperror envelope (no extra top-level field).
		var prErr *service.PaymentRequiredError
		if errors.As(err, &prErr) {
			httperror.PaymentRequiredCtx(w, r, "deployment blocked: "+prErr.Reason)
			return
		}
		if errors.Is(err, service.ErrMaxAppsQuotaExceeded) {
			httperror.QuotaExceededCtx(w, r, "max apps quota exceeded")
			return
		}
		if errors.Is(err, service.ErrInvalidRegion) {
			http.Error(w, `{"error": "`+err.Error()+`"}`, http.StatusBadRequest)
			return
		}
		if errors.Is(err, service.ErrTooManyRegions) {
			http.Error(w, `{"error": "`+err.Error()+`"}`, http.StatusBadRequest)
			return
		}
		// ErrInvalidWasm is what the service returns when the
		// 4-byte magic check inside the tx callback fails. Map to
		// 400 (not 422 or 500) — the request body was syntactically
		// valid but the content wasn't a wasm module.
		if errors.Is(err, service.ErrInvalidWasm) {
			http.Error(w, `{"error": "invalid wasm artifact: missing magic bytes"}`, http.StatusBadRequest)
			return
		}
		// ErrIdempotencyKeyMismatch (issue #52) — the caller reused
		// a key against a request body whose artifact hash differs
		// from the one stored on the cached row. 422 (Unprocessable
		// Entity): the wire shape is well-formed, but a semantic
		// conflict between the key and the body means we can't
		// safely replay. The CLI should either pick a different
		// key or accept that the artifact changed.
		if errors.Is(err, service.ErrIdempotencyKeyMismatch) {
			http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusUnprocessableEntity)
			return
		}
		log.Printf("internal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	// 200 vs 201 selection (issue #52). A replay returns 200 OK
	// with the original deployment row; a fresh deploy returns
	// 201 Created. The response body is byte-equivalent in both
	// cases (same deployResponse shape) so a CLI that ignores
	// the status code still parses the row identically across
	// fresh / replay / idempotent retry.
	status := http.StatusCreated
	if fromCache {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	// Build the response with preview fields populated only when
	// the deploy was a preview (issue #308). The three omitempty
	// tags keep the non-preview wire shape byte-identical to the
	// pre-#308 response, so a CLI built before #308 still parses
	// the response cleanly.
	resp := deployResponse{
		ID:                  deployment.ID,
		Hash:                deployment.Hash,
		URL:                 "https://" + domain.IngressHost(tenantID, appName),
		Regions:             domain.StringArrayTo(deployment.Regions),
		AutoRollbackEnabled: deployment.AutoRollbackEnabled,
	}
	if deployment.PreviewID != nil {
		resp.PreviewID = *deployment.PreviewID
	}
	if deployment.PreviewPRNumber != nil {
		resp.PreviewPRNumber = *deployment.PreviewPRNumber
	}
	if deployment.PreviewExpiresAt != nil {
		resp.PreviewExpiresAt = deployment.PreviewExpiresAt.UTC().Format(time.RFC3339)
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("Deploy: failed to encode response: %v", err)
	}
	auditRecord(r, "deploy", "deployment", deployment.ID, "deployment "+deployment.ID+" created for app "+appName, "success")
}

// parseRegions turns the `?regions=` query value into a deduped slice.
// Returns an error for entries that don't match `[a-z0-9-]{1,64}` so
// the caller can return 400 with a precise message. Empty input or
// all-empty-after-trim returns a nil slice and no error — the service
// layer treats nil/empty as "use the control plane's default region".
func parseRegions(raw string) ([]string, error) {
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	seen := make(map[string]struct{}, len(parts))
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		t := strings.TrimSpace(p)
		if t == "" {
			continue
		}
		if _, dup := seen[t]; dup {
			continue
		}
		// Validate charset and length at the handler boundary so a
		// malformed value never reaches the service layer. Mirrors
		// service.IsValidRegion — the duplication is deliberate
		// (handler gives a clean 400 message; service is a second
		// line of defense).
		if !service.IsValidRegion(t) {
			return nil, fmt.Errorf("invalid region %q: must match [a-z0-9-]{1,64}", t)
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	if len(out) == 0 {
		// All entries were blank/dupes — equivalent to no regions.
		return nil, nil
	}
	// Enforce the per-deployment cap AFTER dedupe so duplicate values
	// don't count toward the limit. The service also enforces this as
	// defense-in-depth for non-HTTP callers.
	if len(out) > service.MaxRegionsPerDeployment {
		return nil, fmt.Errorf("too many regions: %d (max %d)", len(out), service.MaxRegionsPerDeployment)
	}
	return out, nil
}

// parseBoolQuery parses a query-string boolean with a default when the
// parameter is absent. Returns an error for unparseable values so the
// caller can return 400 — silently coercing to the default would let
// a typo disable a feature the tenant thought they had enabled
// (e.g. `?auto-rollback=ture` ≠ `?auto-rollback=true`).
func parseBoolQuery(raw string, defaultVal bool) (bool, error) {
	if raw == "" {
		return defaultVal, nil
	}
	return strconv.ParseBool(raw)
}

// parseIntQuery parses a query-string integer with a default when the
// parameter is absent. Returns an error for unparseable values so the
// caller can return 400. Negative values are rejected.
func parseIntQuery(raw string, defaultVal int) (int, error) {
	if raw == "" {
		return defaultVal, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid integer %q", raw)
	}
	if n < 0 {
		return 0, fmt.Errorf("negative value not allowed: %d", n)
	}
	return n, nil
}

// previewIDPattern constrains the preview-id query param to 8..16
// lowercase hex chars (issue #308). The min length defends against
// accidental 2-char abbreviations; the max length caps the
// runtime's `<EDGE_KV_STORE_PATH>/{tenant_id}/preview-{id}/` path
// length to a sensible value. Matches the constraint in
// service.PreviewOpts / mintPreviewID (12 hex chars from
// crypto/rand = 48 bits of entropy) so the server-minted default
// always passes this regex.
var previewIDPattern = regexp.MustCompile(`^[a-f0-9]{8,16}$`)

// parsePreviewOpts resolves the three preview query params into
// a service.PreviewOpts (issue #308). Returns nil when no preview
// marker is set — the caller passes nil to DeploymentService.Deploy
// to preserve the pre-#308 non-preview path.
//
// Validation rules:
//   - preview-id (when set) must match previewIDPattern.
//   - preview-pr-number (when set) must be >= 0; preview-pr-number
//     without preview-id is rejected (the pr number without a
//     preview marker is a client bug — silently dropping it would
//     lose data the caller thought they sent).
//   - preview-ttl (when set) must parse as a Go duration string.
//     preview-ttl without preview-id is rejected for the same
//     reason.
//   - When preview-id is set and preview-ttl is not, the TTL
//     defaults to service.PreviewDefaultTTL (7 days). The handler
//     resolves the duration to an absolute time before returning
//     so the service layer doesn't import time math.
//
// Errors are returned as a flat string suitable for inclusion
// in a 400 response body (matches the shape of parseRegions /
// parseIntQuery — caller does `{"error": "..."}`).
func parsePreviewOpts(idRaw, prRaw, ttlRaw string) (*service.PreviewOpts, error) {
	// Empty preview-id means "not a preview" — drop everything
	// else and return nil regardless of what the caller sent
	// for prRaw / ttlRaw. We do this even if the caller passed
	// the other two, because preview-pr-number / preview-ttl
	// without preview-id is a client bug and we want to surface
	// it as a clean 400 rather than silently dropping the call.
	if idRaw == "" {
		if prRaw != "" {
			return nil, fmt.Errorf("preview-pr-number set without preview-id; both must be present together")
		}
		if ttlRaw != "" {
			return nil, fmt.Errorf("preview-ttl set without preview-id; both must be present together")
		}
		return nil, nil
	}
	if !previewIDPattern.MatchString(idRaw) {
		return nil, fmt.Errorf("invalid preview-id %q: must match ^[a-f0-9]{8,16}$", idRaw)
	}
	opts := &service.PreviewOpts{PreviewID: idRaw}
	if prRaw != "" {
		n, err := strconv.Atoi(prRaw)
		if err != nil {
			return nil, fmt.Errorf("invalid preview-pr-number %q: %w", prRaw, err)
		}
		if n < 0 {
			return nil, fmt.Errorf("preview-pr-number must be >= 0, got %d", n)
		}
		opts.PreviewPRNumber = &n
	}
	if ttlRaw == "" {
		opts.ExpiresAt = time.Now().Add(service.PreviewDefaultTTL)
	} else {
		d, err := time.ParseDuration(ttlRaw)
		if err != nil {
			return nil, fmt.Errorf("invalid preview-ttl %q: must be a Go duration string (e.g. \"24h\", \"30m\")", ttlRaw)
		}
		if d <= 0 {
			return nil, fmt.Errorf("preview-ttl must be positive, got %s", d)
		}
		opts.ExpiresAt = time.Now().Add(d)
	}
	return opts, nil
}

func (h *DeploymentHandler) GetStatus(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	deploymentID := r.PathValue("deploymentID")

	deployment, err := h.deploymentSvc.GetDeployment(r.Context(), tenantID, deploymentID)
	if err != nil {
		httperror.InternalErrorCtx(w, r)
		return
	}
	if deployment == nil {
		httperror.NotFoundCtx(w, r, "deployment not found")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	resp := newStatusResponse(deployment, tenantID, deployment.AppName)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("GetStatus: failed to encode response: %v", err)
	}
}

func (h *DeploymentHandler) List(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")

	limit := 20
	offset := 0
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if parsed, err := strconv.Atoi(o); err == nil && parsed >= 0 {
			offset = parsed
		}
	}

	deployments, total, err := h.deploymentSvc.ListDeploymentsPaginatedWithTotal(r.Context(), tenantID, appName, limit, offset)
	if err != nil {
		log.Printf("internal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	items := make([]statusResponse, len(deployments))
	for i := range deployments {
		items[i] = newStatusResponse(&deployments[i], tenantID, appName)
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(deploymentListResponse{
		Items:  items,
		Total:  total,
		Limit:  limit,
		Offset: offset,
	}); err != nil {
		log.Printf("List deployments: failed to encode response: %v", err)
	}
}

// Activate handles POST /api/apps/{appName}/activate/{deploymentID}.
//
// Status codes:
//   - 200: activated; body is {"status": "activated"}.
//   - 409: tenant is disabled (issue #440 gate). The body is the
//     standard httperror envelope with `error.code = "CONFLICT"` and
//     `error.message = "tenant is disabled"`, identical to the same
//     409 already returned for `ErrNoLastGood` on the rollback
//     endpoint. CLI / operator tooling can branch on
//     `error.code = "CONFLICT"` plus a `error.message` starting with
//     "tenant is disabled" to distinguish the lockdown case from
//     any other 409 (e.g. no-last-good on rollback).
//   - 500: anything else (DB error, etc.).
//
// Note (issue #42): pre-#42, this handler could return 502 if the
// post-commit NATS publish failed. The publish is now durable: the
// outbox row is written in the same transaction as the
// active_deployments mutation, and the OutboxDrainer relays it after
// commit. A failed activate can only mean a DB error or a duplicate
// dedupe_key — both surface as 500.
//
// Note (issue #440): the ErrTenantDisabled → 409 mapping was added when
// the disable-vs-activate race gate landed. The handler previously
// surfaced any service error as a generic 500, which hid the
// billing/lockdown boundary from the CLI and from any operator tooling
// that wanted to differentiate "tenant is locked, don't retry" from
// "infrastructure broke, alert on-call". Note that this 409 is only
// reachable on the atomic path (weight == 100, the default): the
// canary branch (weight < 100 → trafficSvc.SetTraffic) is not gated
// by lockTenantForUpdate and so cannot return ErrTenantDisabled. If
// disable-vs-canary enforcement becomes a requirement, thread the
// gate through SetTraffic and add a sibling mapping here.
func (h *DeploymentHandler) Activate(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")
	deploymentID := r.PathValue("deploymentID")

	// Validate both path parameters. The deployment id flows into the
	// /registry/{tenant}/{app}/{deployment}.wasm path on the worker
	// (see Download handler) — a ".." or "/" in the id lets a caller
	// reference arbitrary files on the worker's filesystem. Reject
	// 400 here rather than 500 from the storage layer.
	if !validateAppName(w, appName) {
		return
	}
	if !validateDeploymentID(w, deploymentID) {
		return
	}

	weightStr := r.URL.Query().Get("weight")
	// Omitting ?weight entirely means atomic activation (weight=100) — the
	// legacy default. Parsing only overrides the value when the query
	// string is non-empty, so weight=100 also covers the `?weight=100`
	// explicit case (which is the same operation, not a canary).
	weight := 100
	if weightStr != "" {
		parsed, err := strconv.Atoi(weightStr)
		if err != nil || parsed < 0 || parsed > 100 {
			httperror.BadRequestCtx(w, r, "weight must be an integer between 0 and 100")
			return
		}
		weight = parsed
	}

	// weight == 100 (explicit or omitted): atomic activation. Goes through
	// deploymentSvc.ActivateDeployment so active_deployments is updated and
	// rollback / auto-rollback stability evaluation target the right row.
	// Treats ?weight=100 as identical to omitting ?weight= entirely (the
	// canary path is for partial weights only).
	if weight == 100 {
		if err := h.activateSvc.ActivateDeployment(r.Context(), tenantID, appName, deploymentID); err != nil {
			// Issue #440: surface the disable-vs-activate race gate as a
			// 409 Conflict so the CLI / operator tooling can distinguish
			// "tenant is locked, don't retry" from a generic infrastructure
			// 500. Anything else (db unreachable, lock timeout, …) stays
			// a 500 with the canonical "internal error" envelope.
			if errors.Is(err, service.ErrTenantDisabled) {
				httperror.ConflictCtx(w, r, "tenant is disabled")
				return
			}
			log.Printf("internal error: %v", err)
			httperror.InternalErrorCtx(w, r)
			return
		}

		// Fire-and-forget precompilation in the background so the
		// activation response is not blocked by compilation time.
		if h.wasm2cwasmPath != "" && h.artifactStore != nil {
			ctx := context.WithoutCancel(r.Context())
			go service.PrecompileCwasm(ctx, h.artifactStore, h.wasm2cwasmPath, tenantID, appName, deploymentID)
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]string{"status": "activated"}); err != nil {
			log.Printf("Activate: failed to encode response: %v", err)
		}
		auditRecord(r, "activate", "deployment", deploymentID, "deployment "+deploymentID+" activated for app "+appName, "success")
		return
	}

	// Partial weight: canary activation. Requires an existing active
	// deployment to act as the remainder — a canary staged against
	// nothing is the same as a plain activation, which is what
	// ActivateDeployment above already does. Reject with 400 rather than
	// silently producing a single-entry split whose sum != 100 (which
	// would 500 at ValidateSum).
	current, err := h.deploymentSvc.GetActiveDeployment(r.Context(), tenantID, appName)
	if err != nil {
		log.Printf("internal error: GetActiveDeployment: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}
	if current == nil {
		httperror.BadRequestCtx(w, r, "canary activation requires an existing active deployment; activate one first")
		return
	}
	if current.ID == deploymentID {
		httperror.BadRequestCtx(w, r, "deployment is already active; pick a different deployment for the canary")
		return
	}
	splits := []domain.TrafficSplitEntry{
		{DeploymentID: deploymentID, Weight: weight},
		{DeploymentID: current.ID, Weight: 100 - weight},
	}

	if err := h.trafficSvc.SetTraffic(r.Context(), tenantID, appName, splits); err != nil {
		log.Printf("internal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]string{"status": "traffic_set"}); err != nil {
		log.Printf("Canary activate: failed to encode response: %v", err)
	}
}

// Rollback handles POST /api/apps/{appName}/rollback. Swaps the active
// deployment back to the stored last_good_deployment_id and republishes a
// TaskMessage so workers reconcile.
//
// Status codes:
//   - 200: rolled back; body is {"deployment_id": "<new active id>"}.
//   - 404: no active deployment for this app (user never activated).
//   - 409: one of two conditions:
//     1. NoLastGood: app is active but has no last-good pointer
//     (only ever activated one deployment, so there is nothing to
//     roll back to). Body uses the older raw `http.Error` shape
//     `{"error": "no previous deployment to roll back to"}`.
//     2. Tenant is disabled (issue #440 gate). Body uses the canonical
//     httperror envelope with `error.code = "CONFLICT"` and
//     `error.message = "tenant is disabled"`.
//     Both cases share the same status code and the same
//     `error.code = "CONFLICT"` envelope; callers must inspect
//     `error.message` to disambiguate (or branch on the raw legacy
//     body in case 1 — envelope migration is a separate cleanup).
//   - 500: anything else (DB error, etc.).
//
// Note (issue #42): pre-#42, this handler could return 502 if the
// post-commit NATS publish failed. The publish is now durable (see
// Activate's note above); a failed rollback can only mean a DB error.
//
// Note (issue #440): ErrTenantDisabled → 409 mirrors the mapping added
// to Activate so callers can distinguish "tenant locked, don't retry"
// from infrastructure errors.
func (h *DeploymentHandler) Rollback(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")
	if !validateAppName(w, appName) {
		return
	}

	newID, err := h.rollbackSvc.RollbackDeployment(r.Context(), tenantID, appName)
	if err != nil {
		if errors.Is(err, service.ErrTenantDisabled) {
			httperror.ConflictCtx(w, r, "tenant is disabled")
			return
		}
		if errors.Is(err, service.ErrNoLastGood) {
			http.Error(w, `{"error": "no previous deployment to roll back to"}`, http.StatusConflict)
			return
		}
		if errors.Is(err, service.ErrNoActiveDeployment) {
			http.Error(w, `{"error": "no active deployment"}`, http.StatusNotFound)
			return
		}
		// Issue #440: tenant disabled mid-rollback. 409 matches
		// the state-conflict mapping above for ErrNoLastGood.
		if errors.Is(err, service.ErrTenantDisabled) {
			http.Error(w, `{"error": "tenant is disabled; re-enable via the admin endpoint and retry"}`, http.StatusConflict)
			return
		}
		log.Printf("internal error: %v", err)
		http.Error(w, `{"error": "rollback failed"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]string{"deployment_id": newID}); err != nil {
		log.Printf("Rollback: failed to encode response: %v", err)
	}
	auditRecord(r, "rollback", "deployment", newID, "app "+appName+" rolled back to deployment "+newID, "success")
}

func (h *DeploymentHandler) GetActive(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")

	deployment, err := h.deploymentSvc.GetActiveDeployment(r.Context(), tenantID, appName)
	if err != nil || deployment == nil {
		httperror.NotFoundCtx(w, r, "no active deployment")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	resp := newStatusResponse(deployment, tenantID, deployment.AppName)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("GetActive: failed to encode response: %v", err)
	}
}

// Promote handles POST /api/v1/apps/{appName}/promote/{deploymentID} —
// activates a deployment under a different app name than it was originally
// deployed under (preview → production workflow).
//
// Status codes:
//   - 200: promoted; body is {"status": "promoted"}.
//   - 404: deployment not found, or owned by a different tenant.
//   - 409: tenant is disabled (issue #440 gate; promotes flow through
//     the same lockTenantForUpdate helper as Activate).
//   - 500: anything else (DB error, etc.).
func (h *DeploymentHandler) Promote(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	targetAppName := r.PathValue("appName")
	deploymentID := r.PathValue("deploymentID")

	if !validateAppName(w, targetAppName) {
		return
	}
	if !validateDeploymentID(w, deploymentID) {
		return
	}

	if err := h.promoteSvc.PromoteDeployment(r.Context(), tenantID, targetAppName, deploymentID); err != nil {
		if errors.Is(err, service.ErrDeploymentNotFound) {
			httperror.NotFoundCtx(w, r, "deployment not found")
			return
		}
		// Issue #440: disable-vs-activate race gate. Promote delegates
		// to the same activateDeployment inner function as Activate, so
		// the gate fires identically and the handler maps to 409 here.
		if errors.Is(err, service.ErrTenantDisabled) {
			httperror.ConflictCtx(w, r, "tenant is disabled")
			return
		}
		log.Printf("internal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]string{"status": "promoted"}); err != nil {
		log.Printf("Promote: failed to encode response: %v", err)
	}
	auditRecord(r, "promote", "deployment", deploymentID, "deployment "+deploymentID+" promoted to app "+targetAppName, "success")
}

// validateAppName writes a 400 with {"error": "invalid app name"} and
// returns false if appName is empty or contains path-traversal
// characters. Callers should `return` immediately when this is false.
func validateAppName(w http.ResponseWriter, appName string) bool {
	if appName == "" || containsPathTraversal(appName) {
		http.Error(w, `{"error": "invalid app name"}`, http.StatusBadRequest)
		return false
	}
	return true
}

// validateDeploymentID writes a 400 with {"error": "invalid deployment
// id"} and returns false if deploymentID is empty or contains
// path-traversal characters. Callers should `return` when this is false.
// The deployment id flows into the /registry/{tenant}/{app}/{deployment}
// .wasm path on the worker (see Download handler) — a ".." or "/" in the
// id lets a caller reference arbitrary files on the worker's filesystem.
// Reject 400 here rather than 500 from the storage layer.
func validateDeploymentID(w http.ResponseWriter, deploymentID string) bool {
	if deploymentID == "" || containsPathTraversal(deploymentID) {
		http.Error(w, `{"error": "invalid deployment id"}`, http.StatusBadRequest)
		return false
	}
	return true
}

// containsPathTraversal blocks the *decoded* traversal shapes ("/", "\\",
// ".."). The caller is responsible for passing a value that has already
// been percent-decoded — e.g. http.Request.PathValue (used by AppIngress
// and Deploy), or an explicit url.PathUnescape for body fields. Encoding
// bypasses (e.g. %2F, %2E%2E) are intentionally not caught here because
// the input is already decoded by the time this helper sees it; the
// helper's job is to reject post-decode traversal, not to decode.
func containsPathTraversal(s string) bool {
	if s == "" {
		return true
	}
	return strings.ContainsAny(s, "/\\") || strings.Contains(s, "..")
}

// AppIngress handles GET /api/apps/{appName}/ingress — tenant-authenticated
// diagnostic that returns whether the calling tenant's app is currently
// routable on a worker (and on which addr/port). Used by the CLI's
// `edge status` to validate that a `live_url` is actually live. The
// tenant filter is applied at the SQL level so a tenant cannot learn
// about another tenant's apps.
func (h *DeploymentHandler) AppIngress(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")
	if !validateAppName(w, appName) {
		return
	}

	target, err := h.workerSvc.GetAppTarget(r.Context(), tenantID, appName)
	if err != nil {
		log.Printf("internal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}
	if target == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		if err := json.NewEncoder(w).Encode(map[string]interface{}{
			"ready":    false,
			"app_name": appName,
			"reason":   "no running app found for this tenant",
		}); err != nil {
			log.Printf("AppIngress ready false: failed to encode response: %v", err)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"ready":       true,
		"app_name":    target.AppName,
		"tenant_id":   target.TenantID,
		"worker_id":   target.WorkerID,
		"region":      target.Region,
		"worker_addr": target.WorkerAddr,
		"port":        target.Port,
	}); err != nil {
		log.Printf("AppIngress ready true: failed to encode response: %v", err)
	}
}

// extractDeployParts walks the multipart body looking for the required
// `file` part and the optional `build_metadata` part. Returns:
//   - the file `*multipart.Part` (already positioned at the first byte
//     of the wasm artifact), so the caller can stream it directly into
//     the service's io.Reader;
//   - the build_metadata bytes (possibly nil if the part was absent or
//     malformed — non-fatal; the service will use a default envelope);
//   - a non-nil error only when the request is structurally broken
//     (missing file part, malformed MIME headers, etc.). A missing
//     `build_metadata` is NOT an error.
//
// Parts other than `file` and `build_metadata` are silently drained
// and discarded, so the request body's bytes don't leak through to
// the file part stream.
//
// Stop scanning as soon as both the file part AND the optional
// build_metadata part have been collected. mime/multipart's NextPart
// consumes a part's body to find the next boundary, so iterating
// past the file part would drain the artifact bytes the caller
// wants to read. Concretely: a file-only multipart body hits
// NextPart three times — build_metadata absent, file collected,
// NextPart drains the file body to find the closing boundary, then
// returns io.EOF. The file body would be unreadable on return.
func extractDeployParts(mr *multipart.Reader) (*multipart.Part, []byte, error) {
	var (
		filePart       *multipart.Part
		buildMetaBytes []byte
	)
	for {
		p, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("read multipart: %w", err)
		}
		switch p.FormName() {
		case "file":
			if filePart != nil {
				_ = p.Close()
				return nil, nil, fmt.Errorf("multiple 'file' parts")
			}
			filePart = p
		case "build_metadata":
			if buildMetaBytes != nil {
				_ = p.Close()
				return nil, nil, fmt.Errorf("multiple 'build_metadata' parts")
			}
			// Cap the metadata payload at 64 KiB. A real entry is
			// ~300 bytes; 64 KiB is generous headroom for forward
			// compat without permitting a multi-MiB JSON DoS.
			const maxBuildMeta = 64 * 1024
			lr := io.LimitReader(p, maxBuildMeta+1)
			meta, readErr := io.ReadAll(lr)
			_ = p.Close()
			if readErr != nil {
				// Non-fatal: PR2 envelope builder drops unknown
				// metadata rather than rejecting the deploy.
				meta = nil
			}
			if int64(len(meta)) > maxBuildMeta {
				// Oversize metadata is non-fatal too — drop and
				// let the envelope be built with "unknown"
				// toolchain fields. Errs on the side of
				// availability per the deploy path's SLA.
				meta = nil
			}
			buildMetaBytes = meta
		default:
			// Silently drain and discard unknown parts so the
			// `file` part's bytes remain contiguous.
			_, _ = io.Copy(io.Discard, p)
			_ = p.Close()
		}
		// Stop once we have the file part. Continuing to scan
		// would drain the file part's body via NextPart (see
		// function comment); the caller is responsible for
		// closing filePart and reading it to EOF. The
		// build_metadata part must appear BEFORE the file part
		// in the multipart envelope — the CLI's PR2 envelope
		// builder writes fields in that order; if a future
		// caller reverses the order, this code returns the
		// file part first and never scans the metadata. The
		// handler treats missing metadata as "unknown" tooling,
		// so a reversed-order caller just loses the provenance
		// stamp, not the deploy.
		if filePart != nil {
			break
		}
	}
	if filePart == nil {
		return nil, nil, fmt.Errorf("missing 'file' part")
	}
	return filePart, buildMetaBytes, nil
}

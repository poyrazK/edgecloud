package storage

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/config"
)

// S3ArtifactStore persists WASM artifacts in an S3-compatible object
// store. Save is a PUT, Open is a GET, Delete is a DELETE against the
// AWS REST API with manual SigV4 signing.
//
// Why not aws-sdk-go-v2: pulling the SDK would add ~30 transitive deps
// (smithy, aws-config, aws-credentials, etc.) for what amounts to three
// HTTP calls. The S3 REST API is stable, SigV4 is a few lines of
// crypto/hmac, and the alternative forces every operator who runs the
// control plane behind a corporate proxy to vet a much larger module
// tree for CVEs. The cost is that we hand-roll the signing — the win
// is a small, auditable surface.
//
// Object keys are `<keyPrefix><tenantID>/<appName>/<deploymentID>.wasm`
// (keyPrefix is empty unless the operator sets it; used to namespace
// shared buckets across multiple edgeCloud deployments).
//
// Optional Endpoint + PathStyle cover minio/R2/LocalStack. Real AWS S3
// uses virtual-hosted addressing and HTTPS; minio uses path-style and
// often HTTP on localhost. Both are detected from config.
type S3ArtifactStore struct {
	bucket    string
	region    string
	endpoint  string
	pathStyle bool
	keyPrefix string

	// credentials. Empty accessKey/secretKey means use anonymous
	// (public-read buckets, minio without auth in dev).
	accessKey string
	secretKey string

	httpClient *http.Client
}

// osGetenv is a package-level seam for tests to inject env values
// without mutating process-wide state. Defaults to os.Getenv.
var osGetenv = os.Getenv

// NewS3ArtifactStore validates the required S3 config fields and
// constructs the store. Returns an error if S3Bucket or S3Region is
// empty — the operator's config is incomplete and we'd rather fail at
// startup than on the first deploy.
func NewS3ArtifactStore(ctx context.Context, cfg config.StorageConfig) (*S3ArtifactStore, error) {
	if cfg.S3Bucket == "" {
		return nil, fmt.Errorf("S3ArtifactStore: S3Bucket is required")
	}
	if cfg.S3Region == "" {
		return nil, fmt.Errorf("S3ArtifactStore: S3Region is required")
	}
	// Pick up credentials from the standard AWS env vars at construction
	// time so the rest of the type doesn't need to consult the
	// environment on every request. Operators who want IAM-role-based
	// auth can use minio/LocalStack with empty creds (and a public
	// bucket) until issue #127 follow-ups wire IRSA / instance profile.
	return &S3ArtifactStore{
		bucket:     cfg.S3Bucket,
		region:     cfg.S3Region,
		endpoint:   cfg.S3Endpoint,
		pathStyle:  cfg.S3PathStyle,
		keyPrefix:  cfg.S3KeyPrefix,
		accessKey:  osGetenv("AWS_ACCESS_KEY_ID"),
		secretKey:  osGetenv("AWS_SECRET_ACCESS_KEY"),
		httpClient: &http.Client{Timeout: 60 * time.Second},
	}, nil
}

// Save PUTs the artifact bytes to S3 with content-type application/wasm.
// The request body is streamed (no full buffering) so 100 MiB artifacts
// (the MaxArtifactSize cap in the migration handler) don't blow the heap.
//
// Returns an error on transport failure or non-2xx status.
func (s *S3ArtifactStore) Save(ctx context.Context, tenantID, appName, deploymentID string, r io.Reader) error {
	key, err := s.key(tenantID, appName, deploymentID)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, s.objectURL(key), r)
	if err != nil {
		return fmt.Errorf("S3ArtifactStore.Save: building request: %w", err)
	}
	req.Header.Set("Content-Type", "application/wasm")
	signRequest(req, s.accessKey, s.secretKey, s.region, nil, "UNSIGNED-PAYLOAD")
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("S3ArtifactStore.Save: PUT %s: %w", key, err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("S3.Save: failed to close response body: %v", err)
		}
	}()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		return fmt.Errorf("S3ArtifactStore.Save: draining body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("S3ArtifactStore.Save: PUT %s: status %d", key, resp.StatusCode)
	}
	return nil
}

// Open GETs the artifact bytes from S3. Returns os.ErrNotExist on a 404
// so the existing `httperror.NotFoundCtx` path in the worker download
// handler surfaces a clean 404 without having to special-case
// S3ArtifactStore.
//
// The returned ReadCloser wraps resp.Body with a MaxArtifactSize cap
// (see limitReadCloser in storage.go). This stops a hostile or
// misconfigured S3 response from OOM-ing the worker when streamed
// into io.Copy — once the cap is hit, Read returns ErrArtifactTooLarge
// and the handler maps it to HTTP 413. Callers MUST Close to release
// the underlying connection.
func (s *S3ArtifactStore) Open(ctx context.Context, tenantID, appName, deploymentID string) (io.ReadCloser, error) {
	key, err := s.key(tenantID, appName, deploymentID)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.objectURL(key), nil)
	if err != nil {
		return nil, fmt.Errorf("S3ArtifactStore.Open: building request: %w", err)
	}
	signRequest(req, s.accessKey, s.secretKey, s.region, nil, "UNSIGNED-PAYLOAD")
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("S3ArtifactStore.Open: GET %s: %w", key, err)
	}
	if resp.StatusCode == http.StatusNotFound {
		if closeErr := resp.Body.Close(); closeErr != nil {
			log.Printf("S3.Open: failed to close response body: %v", closeErr)
		}
		return nil, &fs.PathError{Op: "open", Path: key, Err: os.ErrNotExist}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if closeErr := resp.Body.Close(); closeErr != nil {
			log.Printf("S3.Open: failed to close response body: %v", closeErr)
		}
		return nil, fmt.Errorf("S3ArtifactStore.Open: GET %s: status %d", key, resp.StatusCode)
	}
	return newLimitReadCloser(resp.Body, MaxArtifactSize), nil
}

// SaveAndHash streams the artifact to S3 while computing its
// SHA-256 in the same io.MultiWriter pass. The streaming benefit
// (no intermediate buffer) is preserved: the read fills the
// hasher AND the request body concurrently. If Save fails the
// hash is meaningless — caller treats it as best-effort.
func (s *S3ArtifactStore) SaveAndHash(ctx context.Context, tenantID, appName, deploymentID string, r io.Reader) ([]byte, error) {
	key, err := s.key(tenantID, appName, deploymentID)
	if err != nil {
		return nil, err
	}
	hasher := sha256.New()
	body := io.TeeReader(r, hasher)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, s.objectURL(key), body)
	if err != nil {
		return nil, fmt.Errorf("S3ArtifactStore.SaveAndHash: building request: %w", err)
	}
	req.Header.Set("Content-Type", "application/wasm")
	signRequest(req, s.accessKey, s.secretKey, s.region, nil, "UNSIGNED-PAYLOAD")
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("S3ArtifactStore.SaveAndHash: PUT %s: %w", key, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("S3ArtifactStore.SaveAndHash: PUT %s: status %d", key, resp.StatusCode)
	}
	return hasher.Sum(nil), nil
}

// Delete removes the artifact bytes from S3. Idempotent: a 404 (key
// already gone) is treated as success so concurrent deletes don't
// surface spurious errors, matching the FSArtifactStore contract.
func (s *S3ArtifactStore) Delete(ctx context.Context, tenantID, appName, deploymentID string) error {
	key, err := s.key(tenantID, appName, deploymentID)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, s.objectURL(key), nil)
	if err != nil {
		return fmt.Errorf("S3ArtifactStore.Delete: building request: %w", err)
	}
	signRequest(req, s.accessKey, s.secretKey, s.region, nil, "UNSIGNED-PAYLOAD")
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("S3ArtifactStore.Delete: DELETE %s: %w", key, err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("S3.Delete: failed to close response body: %v", err)
		}
	}()
	if resp.StatusCode == http.StatusNotFound {
		return nil // idempotent
	}
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		return fmt.Errorf("S3ArtifactStore.Delete: draining body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("S3ArtifactStore.Delete: DELETE %s: status %d", key, resp.StatusCode)
	}
	return nil
}

// key constructs the S3 object key for a deployment artifact. Path
// components are validated the same way FSArtifactStore does — a
// malicious caller passing ".." or "/" gets a 400-equivalent error
// instead of escaping the tenant's directory.
func (s *S3ArtifactStore) key(tenantID, appName, deploymentID string) (string, error) {
	if err := validatePathComponent("tenantID", tenantID); err != nil {
		return "", fmt.Errorf("invalid artifact key: %w", err)
	}
	if err := validatePathComponent("appName", appName); err != nil {
		return "", fmt.Errorf("invalid artifact key: %w", err)
	}
	if err := validatePathComponent("deploymentID", deploymentID); err != nil {
		return "", fmt.Errorf("invalid artifact key: %w", err)
	}
	return s.keyPrefix + tenantID + "/" + appName + "/" + deploymentID + ".wasm", nil
}

// objectURL returns the URL to use for a given object key. For
// path-style addressing (minio) we hit
// `<endpoint>/<bucket>/<key>`; for virtual-hosted (AWS) we hit
// `<bucket>.<endpoint>/<key>`.
func (s *S3ArtifactStore) objectURL(key string) string {
	if s.endpoint != "" {
		base := s.endpoint
		if !strings.HasSuffix(base, "/") {
			base += "/"
		}
		if s.pathStyle {
			return base + s.bucket + "/" + key
		}
		// Virtual-hosted override (minio lets you do vhost with a custom endpoint too).
		if u, err := url.Parse(base); err == nil && u.Host != "" {
			u.Host = s.bucket + "." + u.Host
			u.Path = strings.TrimPrefix(u.Path, "/") + key
			return u.String()
		}
		return base + s.bucket + "/" + key
	}
	// Real AWS S3 — virtual-hosted, HTTPS.
	if s.pathStyle {
		return "https://s3." + s.region + ".amazonaws.com/" + s.bucket + "/" + key
	}
	return "https://" + s.bucket + ".s3." + s.region + ".amazonaws.com/" + key
}

// signRequest computes the AWS SigV4 signature for req. `body` is the
// SHA-256 of the request body, or nil for unsigned (most S3 operations
// can use the literal "UNSIGNED-PAYLOAD" string for streaming PUTs —
// saves us from buffering the artifact into memory just to hash it).
//
// SigV4 is documented in the AWS General Reference:
// https://docs.aws.amazon.com/general/latest/gr/sigv4_signing.html
// The flow is: canonical request → string to sign → signing key
// (derived by repeated HMAC) → signature → Authorization header.
//
// If accessKey and secretKey are both empty, the request is left
// unsigned. S3 will accept this for public buckets; minio without auth
// in dev also accepts it.
func signRequest(req *http.Request, accessKey, secretKey, region string, body []byte, payloadHash string) {
	t := time.Now().UTC()
	amzDate := t.Format("20060102T150405Z")
	dateStamp := t.Format("20060102")

	req.Header.Set("Host", req.URL.Host)
	req.Header.Set("X-Amz-Date", amzDate)
	if payloadHash == "" {
		if body == nil {
			payloadHash = emptyBodySHA256
		} else {
			h := sha256.Sum256(body)
			payloadHash = hex.EncodeToString(h[:])
		}
	}
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)

	if accessKey == "" && secretKey == "" {
		// Anonymous — leave Authorization unset.
		return
	}

	canonHeaders, signedHeaders := canonicalHeaders(req.Header)
	canonReq := strings.Join([]string{
		req.Method,
		req.URL.Path,
		canonicalQueryString(req.URL.Query()),
		canonHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")

	scope := dateStamp + "/" + region + "/s3/aws4_request"
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		sha256Hex([]byte(canonReq)),
	}, "\n")

	signingKey := deriveSigningKey(secretKey, dateStamp, region, "s3")
	signature := hmacSHA256Hex(signingKey, stringToSign)

	req.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential="+accessKey+"/"+scope+
			", SignedHeaders="+signedHeaders+
			", Signature="+signature)
}

// emptyBodySHA256 is the well-known SHA-256 of an empty string. Cached
// here so we don't recompute it on every unsigned GET / DELETE.
const emptyBodySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// canonicalHeaders builds the canonical header block for SigV4. Header
// names are lowercased, sorted, trimmed, and joined as `name:value\n`.
// SignedHeaders is the semicolon-separated list of header names that
// participated (so the verifier knows which to include in its own
// canonical block).
func canonicalHeaders(h http.Header) (string, string) {
	keys := make([]string, 0, len(h))
	for k := range h {
		lk := strings.ToLower(k)
		// Skip any header that's not relevant for SigV4 — Host and
		// X-Amz-* are kept; everything else (Authorization, User-Agent,
		// etc.) is set by the http client and ignored by S3.
		if lk == "host" || strings.HasPrefix(lk, "x-amz-") {
			keys = append(keys, lk)
		}
	}
	sort.Strings(keys)
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(k)
		b.WriteByte(':')
		// Header values can appear multiple times for some headers;
		// join with comma (per SigV4 spec).
		vals := h.Values(k)
		for j, v := range vals {
			if j > 0 {
				b.WriteByte(',')
			}
			b.WriteString(strings.TrimSpace(v))
		}
		b.WriteByte('\n')
	}
	return b.String(), strings.Join(keys, ";")
}

// canonicalQueryString returns the URL query parameters in the
// canonical form: keys sorted, values URL-escaped.
func canonicalQueryString(q url.Values) string {
	if len(q) == 0 {
		return ""
	}
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte('&')
		}
		b.WriteString(url.QueryEscape(k))
		b.WriteByte('=')
		b.WriteString(url.QueryEscape(q.Get(k)))
	}
	return b.String()
}

func deriveSigningKey(secret, date, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), date)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	kSigning := hmacSHA256(kService, "aws4_request")
	return kSigning
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

func hmacSHA256Hex(key []byte, data string) string {
	return hex.EncodeToString(hmacSHA256(key, data))
}

func sha256Hex(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

// Compile-time check that *S3ArtifactStore implements ArtifactStore.
// Catches signature drift at build time (e.g. a future FSArtifactStore
// method addition that the interface missed) without a separate test.
var _ ArtifactStore = (*S3ArtifactStore)(nil)

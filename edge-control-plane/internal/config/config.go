package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config holds all application configuration.
type Config struct {
	Database  DatabaseConfig  `yaml:"database"`
	NATS      NATSConfig      `yaml:"nats"`
	App       AppConfig       `yaml:"app"`
	Storage   StorageConfig   `yaml:"storage"`
	JWT       JWTConfig       `yaml:"jwt"`
	RateLimit RateLimitConfig `yaml:"rate_limit"`
	Migration MigrationConfig `yaml:"migration"`
	// Signing configures the Ed25519 artifact-signing key (issue #307).
	// The CP signs every new deployment's artifact at upload time;
	// workers verify the signature before instantiation. The private
	// key never leaves the CP. The public key is propagated to workers
	// out-of-band (today: EDGE_SIGNING_PUBKEY env var on each worker).
	Signing SigningConfig `yaml:"signing"`
	// Autoscale configures the cluster autoscaler (issue #85).
	// Disabled by default — operators flip `enabled: true` once the
	// fleet has multiple workers and the cloud-provider integration
	// (NoopCloudProvider today; Hetzner in a follow-up) is ready.
	Autoscale AutoscaleConfig `yaml:"autoscale"`
	// Region is this control plane's own region. Used as the default
	// `regions` list for deployments that don't explicitly opt into
	// multi-region — preserves today's "publish one TaskMessage to a
	// single subject" behavior. The wildcard NATS subject
	// `edgecloud.tasks.>` (configured in cmd/api/main.go) means the
	// literal default `"global"` works for any worker regardless of
	// its own region. See `service.ActivateDeployment` for the
	// fallback path. (Issue #82, v1.)
	Region string `yaml:"region"`
	// BootstrapSecret is a shared HMAC secret used by workers to
	// authenticate at bootstrap when WORKER_JWT_SECRET is not yet
	// provisioned. The handshake:
	//   1. Worker POSTs to /api/internal/bootstrap with a payload
	//      signed by this secret (HMAC-SHA256).
	//   2. CP returns a short-lived (5min) bootstrap JWT.
	//   3. Worker exchanges that JWT for the real JWT_SECRET at
	//      GET /api/internal/worker-secret.
	// Must be at least 32 bytes, like JWT_SECRET. Set via
	// BOOTSTRAP_SECRET env var or bootstrap.secret in config.
	BootstrapSecret string `yaml:"bootstrap_secret"`
	// InternalToken is a shared secret presented by trusted
	// service-to-service callers (today: the edge-ingress, which
	// fetches traffic splits to apply Caddy weights). When set, the
	// `internalAuth` middleware requires the
	// `X-Internal-Token: <value>` header on those endpoints. When
	// unset, the middleware fail-closes (rejects all requests) — the
	// ingress would then 401 and the canary split would not propagate
	// to Caddy. Operators must set EDGE_INTERNAL_TOKEN on both the
	// control plane and the ingress to the same value.
	InternalToken string `yaml:"internal_token"`
	// SecretsMasterKey is a hex-encoded 32-byte AES-256-GCM key used
	// to encrypt app env values at rest. Generate with:
	//   openssl rand -hex 32
	// When empty, env values are stored in plaintext (development mode).
	// In production, set EDGE_SECRETS_MASTER_KEY in the environment.
	//
	// DEPRECATED: use Secrets instead. When both are set, Load returns
	// an error. When SecretsMasterKey is set and Secrets is not, the
	// key is auto-assigned key ID "legacy".
	SecretsMasterKey string `yaml:"secrets_master_key"`
	// Secrets configures envelope encryption with key rotation support.
	// ActiveKeyID selects which key encrypts new values; all keys in the
	// map are used for decryption. When set, SecretsMasterKey must be empty.
	Secrets SecretsConfig `yaml:"secrets"`
}

type DatabaseConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Name     string `yaml:"name"`
	SSLMode  string `yaml:"sslmode"`
}

type NATSConfig struct {
	URL      string `yaml:"url"`
	Replicas int    `yaml:"replicas"`
}

type AppConfig struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
	Env  string `yaml:"env"`
}

type StorageConfig struct {
	// ArtifactPath is the filesystem root for FSArtifactStore and the
	// local cache directory for RemoteArtifactStore. Required when
	// ArtifactBackend is "" or "fs" (today's contract) or "remote".
	// Ignored when ArtifactBackend == "s3".
	ArtifactPath string `yaml:"artifact_path"`

	// ArtifactBackend selects the artifact store implementation.
	//   ""       → FSArtifactStore (backwards-compatible default)
	//   "fs"     → FSArtifactStore (explicit)
	//   "s3"     → S3ArtifactStore (PutObject/GetObject/DeleteObject)
	//   "remote" → RemoteArtifactStore (pull-through cache from a peer CP)
	//
	// Validated by validateStorageConfig; an unknown value fails startup
	// so a typo in config doesn't silently fall through to "fs".
	ArtifactBackend string `yaml:"artifact_backend"`

	// S3ArtifactStore fields. Ignored unless ArtifactBackend == "s3".
	S3Bucket    string `yaml:"s3_bucket"`
	S3Region    string `yaml:"s3_region"`
	S3Endpoint  string `yaml:"s3_endpoint"`   // optional, for minio/R2/LocalStack
	S3PathStyle bool   `yaml:"s3_path_style"` // true for minio; false for AWS
	S3KeyPrefix string `yaml:"s3_key_prefix"` // optional, e.g. "tenants/"

	// RemoteArtifactStore fields. Ignored unless ArtifactBackend == "remote".
	//
	// PeerControlPlaneInternalToken is the shared secret the local CP
	// presents to the peer via X-Internal-Token. Operators MUST set this
	// to the same value as EDGE_INTERNAL_TOKEN on the peer CP. Empty
	// value fails startup (fail-closed — never serve an unauthenticated
	// peer pull-through request).
	PeerControlPlaneURL           string `yaml:"peer_control_plane_url"`
	PeerControlPlaneInternalToken string `yaml:"peer_control_plane_internal_token"`

	// RegionArtifactCaches maps a region identifier (matching the
	// IsValidRegion pattern, `^[a-z0-9][a-z0-9-]{0,63}$`) to the base
	// URL of the per-region `edge-artifact-cache` binary serving that
	// region. Issue #332 (Layer 3: Push-to-Edge).
	//
	// On deployment activation, the CP PUTs the artifact bytes to each
	// region's cache BEFORE publishing the TaskMessage to NATS, so the
	// worker can fetch from a local cache (~1ms RTT) instead of from
	// the CP (~10ms+ RTT). Region entries with no value (or absent
	// from the map) skip the push step entirely; the worker continues
	// to pull from the CP's /api/internal/download/ endpoint as today.
	//
	// The URL must include scheme + host + (optional) port + (optional)
	// path prefix. Per-artifact path-component validation happens
	// inside the cache binary. The auth token is shared with the cache
	// via the `INTERNAL_TOKEN` env var on both sides; the CP presents
	// it as `X-Internal-Token` on every PUT.
	//
	// Empty map = no-op (zero behavioral change). Operators opt in by
	// adding a `region_artifact_caches:` block to config.yaml.
	RegionArtifactCaches map[string]string `yaml:"region_artifact_caches"`

	// ArtifactCacheInternalToken is the shared secret presented as the
	// `X-Internal-Token` header on every cache PUT. Operators MUST
	// set this to the same value as the cache binary's `INTERNAL_TOKEN`
	// env var. Empty value means "no cache push possible" — the
	// cache-push step is skipped entirely, which is the safe default
	// (no auth header sent, no region cache consulted). A regional
	// cache URL with an empty token is a startup-time validation
	// error (fail-closed).
	ArtifactCacheInternalToken string `yaml:"artifact_cache_internal_token"`
}

type JWTConfig struct {
	// Secret is a single JWT signing secret.
	// DEPRECATED: use Keys + ActiveKID instead.
	Secret string `yaml:"secret"`
	// ActiveKID selects which key in Keys is used for signing new tokens.
	// When only Secret is set, ActiveKID defaults to "default".
	ActiveKID string            `yaml:"active_kid"`
	Keys      map[string]string `yaml:"keys"`
	TTL       int               `yaml:"ttl_hours"`
	Issuer    string            `yaml:"issuer"`
}

// SecretsConfig configures envelope encryption with a keyring.
// ActiveKeyID selects which key encrypts new values; all keys in
// the map are available for decryption.
type SecretsConfig struct {
	ActiveKeyID string            `yaml:"active_key_id"`
	Keys        map[string]string `yaml:"keys"`
}

// RateLimitConfig controls per-tenant and per-IP rate limiting.
// A zero value selects the default (200 req/s tenant, 20 req/s IP).
// Set to negative values to disable a limiter entirely.
type RateLimitConfig struct {
	// TenantRate is the sustained requests-per-second per tenant.
	TenantRate int `yaml:"tenant_rate"`
	// TenantBurst is the maximum burst of requests per tenant.
	TenantBurst int `yaml:"tenant_burst"`
	// IPRate is the sustained requests-per-second per client IP.
	IPRate int `yaml:"ip_rate"`
	// IPBurst is the maximum burst of requests per client IP.
	IPBurst int `yaml:"ip_burst"`
}

// AutoscaleConfig configures the cluster autoscaler (issue #85).
// Disabled by default — operators flip enabled: true once the
// fleet has multiple workers and a cloud-provider is wired in.
type AutoscaleConfig struct {
	Enabled            bool   `yaml:"enabled"`
	MinWorkers         int    `yaml:"min_workers"`
	MaxWorkers         int    `yaml:"max_workers"`
	TargetHeadroomPct  int    `yaml:"target_headroom_pct"`
	ScaleUpCooldownS   int    `yaml:"scale_up_cooldown_s"`
	ScaleDownCooldownS int    `yaml:"scale_down_cooldown_s"`
	DecisionIntervalS  int    `yaml:"decision_interval_s"`
	ProviderKind       string `yaml:"provider_kind"`
}

// DSN returns the PostgreSQL connection string.
func (d *DatabaseConfig) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		d.Host, d.Port, d.User, d.Password, d.Name, d.SSLMode,
	)
}

// Load reads config from a YAML file, then overrides with environment variables.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	// Override with env vars if set
	if v := os.Getenv("DATABASE_HOST"); v != "" {
		cfg.Database.Host = v
	}
	if v := os.Getenv("DATABASE_PORT"); v != "" {
		port, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("DATABASE_PORT must be a valid integer: %w", err)
		}
		cfg.Database.Port = port
	}
	if v := os.Getenv("DATABASE_USER"); v != "" {
		cfg.Database.User = v
	}
	if v := os.Getenv("DATABASE_PASSWORD"); v != "" {
		cfg.Database.Password = v
	}
	if v := os.Getenv("DATABASE_NAME"); v != "" {
		cfg.Database.Name = v
	}
	if v := os.Getenv("DATABASE_SSLMODE"); v != "" {
		cfg.Database.SSLMode = v
	}
	if v := os.Getenv("NATS_URL"); v != "" {
		cfg.NATS.URL = v
	}
	if v := os.Getenv("APP_HOST"); v != "" {
		cfg.App.Host = v
	}
	if v := os.Getenv("APP_PORT"); v != "" {
		port, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("APP_PORT must be a valid integer: %w", err)
		}
		cfg.App.Port = port
	}
	if v := os.Getenv("APP_ENV"); v != "" {
		cfg.App.Env = v
	}
	if v := os.Getenv("STORAGE_ARTIFACT_PATH"); v != "" {
		cfg.Storage.ArtifactPath = v
	}
	if v := os.Getenv("STORAGE_ARTIFACT_BACKEND"); v != "" {
		cfg.Storage.ArtifactBackend = v
	}
	if v := os.Getenv("STORAGE_S3_BUCKET"); v != "" {
		cfg.Storage.S3Bucket = v
	}
	if v := os.Getenv("STORAGE_S3_REGION"); v != "" {
		cfg.Storage.S3Region = v
	}
	if v := os.Getenv("STORAGE_S3_ENDPOINT"); v != "" {
		cfg.Storage.S3Endpoint = v
	}
	if v := os.Getenv("STORAGE_S3_PATH_STYLE"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("STORAGE_S3_PATH_STYLE must be a valid boolean: %w", err)
		}
		cfg.Storage.S3PathStyle = b
	}
	if v := os.Getenv("STORAGE_S3_KEY_PREFIX"); v != "" {
		cfg.Storage.S3KeyPrefix = v
	}
	if v := os.Getenv("STORAGE_PEER_CONTROL_PLANE_URL"); v != "" {
		cfg.Storage.PeerControlPlaneURL = v
	}
	if v := os.Getenv("STORAGE_PEER_CONTROL_PLANE_INTERNAL_TOKEN"); v != "" {
		cfg.Storage.PeerControlPlaneInternalToken = v
	}
	if v := os.Getenv("JWT_SECRET"); v != "" {
		cfg.JWT.Secret = v
	}
	if v := os.Getenv("JWT_TTL_HOURS"); v != "" {
		ttl, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("JWT_TTL_HOURS must be a valid integer: %w", err)
		}
		cfg.JWT.TTL = ttl
	}
	if v := os.Getenv("JWT_ISSUER"); v != "" {
		cfg.JWT.Issuer = v
	}
	if v := os.Getenv("CONTROL_PLANE_REGION"); v != "" {
		cfg.Region = v
	}
	if v := os.Getenv("EDGE_INTERNAL_TOKEN"); v != "" {
		cfg.InternalToken = v
	}
	if v := os.Getenv("BOOTSTRAP_SECRET"); v != "" {
		cfg.BootstrapSecret = v
	}
	if v := os.Getenv("EDGE_SECRETS_MASTER_KEY"); v != "" {
		cfg.SecretsMasterKey = v
	}
	if v := os.Getenv("EDGE_SECRETS_ACTIVE_KEY_ID"); v != "" {
		cfg.Secrets.ActiveKeyID = v
	}
	// EDGE_SECRETS_KEY_<ID> env vars override entries in cfg.Secrets.Keys.
	for _, e := range os.Environ() {
		if before, after, ok := strings.Cut(e, "="); ok && strings.HasPrefix(before, "EDGE_SECRETS_KEY_") {
			keyID := before[len("EDGE_SECRETS_KEY_"):]
			if keyID != "" {
				if cfg.Secrets.Keys == nil {
					cfg.Secrets.Keys = make(map[string]string)
				}
				cfg.Secrets.Keys[keyID] = after
			}
		}
	}
	if v := os.Getenv("JWT_ACTIVE_KID"); v != "" {
		cfg.JWT.ActiveKID = v
	}
	// JWT_KEY_<KID> env vars override entries in cfg.JWT.Keys.
	for _, e := range os.Environ() {
		if before, after, ok := strings.Cut(e, "="); ok && strings.HasPrefix(before, "JWT_KEY_") {
			kid := before[len("JWT_KEY_"):]
			if kid != "" {
				if cfg.JWT.Keys == nil {
					cfg.JWT.Keys = make(map[string]string)
				}
				cfg.JWT.Keys[kid] = after
			}
		}
	}
	if v := os.Getenv("TASK_STREAM_REPLICAS"); v != "" {
		r, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("TASK_STREAM_REPLICAS must be a valid integer: %w", err)
		}
		cfg.NATS.Replicas = r
	}

	// Override rate-limit config with env vars
	if v := os.Getenv("RATE_LIMIT_TENANT_RATE"); v != "" {
		rate, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("RATE_LIMIT_TENANT_RATE must be a valid integer: %w", err)
		}
		cfg.RateLimit.TenantRate = rate
	}
	if v := os.Getenv("RATE_LIMIT_TENANT_BURST"); v != "" {
		burst, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("RATE_LIMIT_TENANT_BURST must be a valid integer: %w", err)
		}
		cfg.RateLimit.TenantBurst = burst
	}
	if v := os.Getenv("RATE_LIMIT_IP_RATE"); v != "" {
		rate, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("RATE_LIMIT_IP_RATE must be a valid integer: %w", err)
		}
		cfg.RateLimit.IPRate = rate
	}
	if v := os.Getenv("RATE_LIMIT_IP_BURST"); v != "" {
		burst, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("RATE_LIMIT_IP_BURST must be a valid integer: %w", err)
		}
		cfg.RateLimit.IPBurst = burst
	}

	// Override with autoscale config env vars (issue #85)
	if v := os.Getenv("AUTOSCALE_ENABLED"); v != "" {
		enabled, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("AUTOSCALE_ENABLED must be a valid boolean: %w", err)
		}
		cfg.Autoscale.Enabled = enabled
	}
	if v := os.Getenv("AUTOSCALE_MIN_WORKERS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("AUTOSCALE_MIN_WORKERS must be a valid integer: %w", err)
		}
		cfg.Autoscale.MinWorkers = n
	}
	if v := os.Getenv("AUTOSCALE_MAX_WORKERS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("AUTOSCALE_MAX_WORKERS must be a valid integer: %w", err)
		}
		cfg.Autoscale.MaxWorkers = n
	}
	if v := os.Getenv("AUTOSCALE_TARGET_HEADROOM_PCT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("AUTOSCALE_TARGET_HEADROOM_PCT must be a valid integer: %w", err)
		}
		cfg.Autoscale.TargetHeadroomPct = n
	}
	if v := os.Getenv("AUTOSCALE_SCALE_UP_COOLDOWN_S"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("AUTOSCALE_SCALE_UP_COOLDOWN_S must be a valid integer: %w", err)
		}
		cfg.Autoscale.ScaleUpCooldownS = n
	}
	if v := os.Getenv("AUTOSCALE_SCALE_DOWN_COOLDOWN_S"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("AUTOSCALE_SCALE_DOWN_COOLDOWN_S must be a valid integer: %w", err)
		}
		cfg.Autoscale.ScaleDownCooldownS = n
	}
	if v := os.Getenv("AUTOSCALE_DECISION_INTERVAL_S"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("AUTOSCALE_DECISION_INTERVAL_S must be a valid integer: %w", err)
		}
		cfg.Autoscale.DecisionIntervalS = n
	}
	if v := os.Getenv("AUTOSCALE_PROVIDER_KIND"); v != "" {
		cfg.Autoscale.ProviderKind = v
	}

	// Override with migration config env vars
	if v := os.Getenv("EDGE_MIGRATE_PATH"); v != "" {
		cfg.Migration.EdgeMigratePath = v
	}
	if v := os.Getenv("WASI_SDK_PATH"); v != "" {
		cfg.Migration.WasiSdkPath = v
	}
	if v := os.Getenv("RUSTC_PATH"); v != "" {
		cfg.Migration.RustcPath = v
	}
	if v := os.Getenv("EDGE_SIGNING_KEY_PATH"); v != "" {
		cfg.Signing.KeyPath = v
	}
	if v := os.Getenv("EDGE_SIGNING_KEY"); v != "" {
		cfg.Signing.Key = v
	}
	if v := os.Getenv("EDGE_SIGNING_KEY_ID"); v != "" {
		cfg.Signing.KeyID = v
	}
	// Multi-key keyring env vars (issue #307 follow-up PR1). When set,
	// take precedence over the legacy single-key vars above; the legacy
	// form remains accepted as a one-release deprecation fallback.
	if v := os.Getenv("EDGE_SIGNING_KEYRING_PATH"); v != "" {
		cfg.Signing.KeyringPath = v
	}
	if v := os.Getenv("EDGE_SIGNING_KEYRING"); v != "" {
		cfg.Signing.Keyring = v
	}

	// Defaults for JWT config
	if cfg.JWT.Issuer == "" {
		cfg.JWT.Issuer = "edgecloud"
	}
	if cfg.JWT.TTL == 0 {
		cfg.JWT.TTL = 24
	}

	// Default for the control plane's own region. `"global"` matches the
	// long-standing literal used at `service/deployment.go` call site
	// (PublishTaskUpdate("global", ...)) and works with the wildcard
	// NATS subject `edgecloud.tasks.>` so any worker in any region
	// receives the message. Operators who run region-specific control
	// planes (e.g. "us-east", "eu-west") can override via config or
	// `CONTROL_PLANE_REGION` env var. See issue #82.
	if cfg.Region == "" {
		cfg.Region = "global"
	}
	if cfg.NATS.Replicas <= 0 {
		cfg.NATS.Replicas = 3
	}

	// Defaults for rate-limit config. Zero means "use default";
	// negative means "disabled" (bypass middleware entirely).
	if cfg.RateLimit.TenantRate == 0 {
		cfg.RateLimit.TenantRate = 200
	}
	if cfg.RateLimit.TenantBurst == 0 {
		cfg.RateLimit.TenantBurst = 300
	}
	if cfg.RateLimit.IPRate == 0 {
		cfg.RateLimit.IPRate = 20
	}
	if cfg.RateLimit.IPBurst == 0 {
		cfg.RateLimit.IPBurst = 40
	}

	// Reject insecure JWT secrets. Operators frequently ship with the
	// default `change-me-in-production` placeholder and forget to override
	// it; failing startup is louder and safer than silently running with a
	// publicly-known secret. (Audit finding #2 — also referenced by tests.)
	if err := validateJWTSecret(cfg.JWT.Secret, cfg.JWT.ActiveKID, cfg.JWT.Keys); err != nil {
		return nil, err
	}

	// Validate bootstrap secret if configured. Same strength requirements
	// as JWT_SECRET — must be ≥32 bytes, not a known placeholder.
	// Optional: when empty, workers must use the direct JWT secret.
	if cfg.BootstrapSecret != "" {
		if err := validateBootstrapSecret(cfg.BootstrapSecret); err != nil {
			return nil, err
		}
	}

	// Validate secrets config: must not mix old and new formats.
	if err := validateSecretsConfig(cfg.SecretsMasterKey, cfg.Secrets); err != nil {
		return nil, err
	}

	// Validate the artifact-storage backend selection and its per-backend
	// required fields. Run after env overrides so a STORAGE_ARTIFACT_BACKEND
	// env var that names a backend whose required fields are missing from
	// the YAML still fails startup with a clear message.
	if err := validateStorageConfig(&cfg.Storage); err != nil {
		return nil, err
	}

	// Validate the signing key (issue #307). At least one of KeyPath /
	// Key must be set; a CP without a signing key cannot issue
	// signatures on new artifacts, and Deploy should fail rather than
	// silently produce unsigned rows.
	if err := validateSigningConfig(&cfg.Signing); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// validateSigningConfig enforces that an Ed25519 signing key is
// configured. The format / content of the key itself is validated
// at load time by `signing.LoadFromFile` / `signing.LoadFromEnv` —
// this validator only checks "is the operator pointing at SOMETHING".
// A missing KeyID is allowed (with a warning logged by the caller)
// so dev environments can boot without ceremony, but a missing key
// entirely is a hard failure.
func validateSigningConfig(s *SigningConfig) error {
	if s.KeyPath == "" && s.Key == "" && s.KeyringPath == "" && s.Keyring == "" {
		return fmt.Errorf("signing.key_path (EDGE_SIGNING_KEY_PATH), signing.key (EDGE_SIGNING_KEY), signing.keyring_path (EDGE_SIGNING_KEYRING_PATH), or signing.keyring (EDGE_SIGNING_KEYRING) is required — the CP must be configured with an Ed25519 signing key to issue deployment signatures (issue #307)")
	}
	return nil
}

// insecureJWTSecretValues is the set of well-known placeholder JWT secrets
// that must not be accepted in production. Operators must override these
// with a real secret via `JWT_SECRET` env var or `jwt.secret` config field.
//
// The set is small and curated — adding entries requires a code review so
// a typo doesn't accidentally invalidate a legitimate operator secret.
//
// An empty string is checked separately so operators get a clear "not set"
// message rather than the misleading "is a known placeholder".
var insecureJWTSecretValues = map[string]struct{}{
	"change-me-in-production": {},
	"changeme":                {},
	"secret":                  {},
	"default":                 {},
	"insecure":                {},
}

// validateJWTSecret enforces the JWT secret configuration.
// Supports two modes:
//  1. Legacy: single Secret field (must be ≥32 bytes, not a placeholder)
//  2. Keyring: ActiveKID + Keys map (ActiveKID must be in Keys, each key ≥32 bytes)
//
// When both are zero/unset, the function returns an error (no JWT auth configured).
func validateJWTSecret(secret string, activeKID string, keys map[string]string) error {
	// Keyring mode.
	if len(keys) > 0 {
		if activeKID == "" {
			return fmt.Errorf("jwt.active_kid is required when jwt.keys is set")
		}
		if _, ok := keys[activeKID]; !ok {
			return fmt.Errorf("jwt.active_kid %q not found in jwt.keys", activeKID)
		}
		for kid, val := range keys {
			if _, ok := insecureJWTSecretValues[val]; ok {
				return fmt.Errorf("jwt.keys[%q] %q is a known placeholder; use a unique value", kid, val)
			}
			if len(val) < 32 {
				return fmt.Errorf("jwt.keys[%q] must be at least 32 bytes (got %d)", kid, len(val))
			}
		}
		return nil
	}

	// Legacy mode: single secret. Must be set, ≥32 bytes, not a placeholder.
	if secret == "" {
		return fmt.Errorf("jwt.secret is not set; set JWT_SECRET or jwt.secret to a unique value")
	}
	if _, ok := insecureJWTSecretValues[secret]; ok {
		return fmt.Errorf("jwt.secret %q is a known placeholder; set JWT_SECRET or jwt.secret to a unique value", secret)
	}
	if len(secret) < 32 {
		return fmt.Errorf("jwt.secret must be at least 32 bytes (got %d)", len(secret))
	}
	return nil
}

// validateSecretsConfig enforces that the old and new secrets config
// formats are not mixed.
// validateBootstrapSecret enforces that the bootstrap secret, when set,
// meets the same strength requirements as JWT_SECRET.
func validateBootstrapSecret(secret string) error {
	if _, ok := insecureJWTSecretValues[secret]; ok {
		return fmt.Errorf("bootstrap.secret %q is a known placeholder; set BOOTSTRAP_SECRET to a unique value", secret)
	}
	if len(secret) < 32 {
		return fmt.Errorf("bootstrap.secret must be at least 32 bytes (got %d)", len(secret))
	}
	return nil
}

func validateSecretsConfig(masterKey string, secrets SecretsConfig) error {
	if masterKey != "" && secrets.ActiveKeyID != "" {
		return fmt.Errorf("cannot set both secrets_master_key and secrets.active_key_id; use secrets.keys exclusively")
	}
	if masterKey != "" && len(secrets.Keys) > 0 {
		return fmt.Errorf("cannot set both secrets_master_key and secrets.keys; use secrets.keys exclusively")
	}
	if secrets.ActiveKeyID != "" && len(secrets.Keys) == 0 {
		return fmt.Errorf("secrets.active_key_id is set but secrets.keys is empty")
	}
	if secrets.ActiveKeyID != "" {
		if _, ok := secrets.Keys[secrets.ActiveKeyID]; !ok {
			return fmt.Errorf("secrets.active_key_id %q not found in secrets.keys", secrets.ActiveKeyID)
		}
	}
	return nil
}

// validArtifactBackends is the closed set of values ArtifactBackend accepts.
// Empty ("") is handled separately — it defaults to "fs" so existing
// deployments without an explicit selection keep working. Adding a backend
// here also requires a constructor in internal/storage/factory.go (issue #127).
var validArtifactBackends = map[string]struct{}{
	"":       {},
	"fs":     {},
	"s3":     {},
	"remote": {},
}

// validateStorageConfig enforces the per-backend required fields for
// cfg.Storage. Run from Load() after env-var overrides so the values
// reflect the final configuration.
//
// The rules are deliberately minimal — backend selection + presence of
// the fields that backend's constructor requires. Anything more (e.g.
// validating that the S3 endpoint URL parses) belongs in the backend's
// constructor (NewS3ArtifactStore) so the same rules apply when the
// store is constructed outside of config.Load (e.g. tests).
//
// The fail-closed defaults below match the backend constructors:
//   - "fs"     → FSArtifactStore tolerates an empty ArtifactPath (the
//     constructor just sets basePath=""); in practice operators always
//     set it. Empty path is NOT a hard error so the migration test
//     fixtures (which pass an empty storage block) keep working.
//   - "s3"     → S3ArtifactStore requires S3Bucket + S3Region.
//   - "remote" → RemoteArtifactStore requires PeerControlPlaneURL +
//     PeerControlPlaneInternalToken + ArtifactPath. The token rule
//     mirrors the middleware's fail-closed behavior on empty token.
func validateStorageConfig(s *StorageConfig) error {
	backend := s.ArtifactBackend
	if backend == "" {
		backend = "fs"
	}
	if _, ok := validArtifactBackends[backend]; !ok {
		return fmt.Errorf(
			"storage.artifact_backend %q is not a recognized backend (want one of: fs, s3, remote)",
			s.ArtifactBackend,
		)
	}
	switch backend {
	case "s3":
		if s.S3Bucket == "" {
			return fmt.Errorf("storage.s3_bucket is required when artifact_backend is \"s3\"")
		}
		if s.S3Region == "" {
			return fmt.Errorf("storage.s3_region is required when artifact_backend is \"s3\"")
		}
	case "remote":
		if s.PeerControlPlaneURL == "" {
			return fmt.Errorf("storage.peer_control_plane_url is required when artifact_backend is \"remote\"")
		}
		if s.PeerControlPlaneInternalToken == "" {
			return fmt.Errorf("storage.peer_control_plane_internal_token is required when artifact_backend is \"remote\" (fail-closed)")
		}
		if s.ArtifactPath == "" {
			return fmt.Errorf("storage.artifact_path is required when artifact_backend is \"remote\" (local cache dir)")
		}
	}

	// RegionArtifactCaches (issue #332): every configured region must
	// have a parseable http(s) URL. An empty URL on a present key
	// (typo in config) is rejected at startup rather than silently
	// no-op'ing at activation. An empty `artifact_cache_internal_token`
	// alongside a non-empty region map is also rejected — a cache URL
	// without a token would send unauthenticated PUTs and the cache
	// would 401 every one.
	for region, rawURL := range s.RegionArtifactCaches {
		if !isValidRegionIdentifier(region) {
			return fmt.Errorf(
				"storage.region_artifact_caches region %q is not a valid region identifier (must match ^[a-z0-9][a-z0-9-]{0,63}$)",
				region,
			)
		}
		if rawURL == "" {
			return fmt.Errorf("storage.region_artifact_caches[%q] is empty; remove the key or set a non-empty URL", region)
		}
		u, err := url.Parse(rawURL)
		if err != nil {
			return fmt.Errorf("storage.region_artifact_caches[%q]: %w", region, err)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return fmt.Errorf("storage.region_artifact_caches[%q] must use http or https scheme (got %q)", region, u.Scheme)
		}
		if u.Host == "" {
			return fmt.Errorf("storage.region_artifact_caches[%q] has no host", region)
		}
	}
	if len(s.RegionArtifactCaches) > 0 && s.ArtifactCacheInternalToken == "" {
		return fmt.Errorf(
			"storage.artifact_cache_internal_token is required when region_artifact_caches is non-empty (fail-closed: never send unauthenticated cache PUTs)",
		)
	}

	return nil
}

// isValidRegionIdentifier mirrors the IsValidRegion check in the
// service package (service/deployment.go IsValidRegion), but lives in
// the config package so config validation doesn't have to import
// the service package (which would be a cycle). Both must stay in
// sync; if the service regex changes, update this one too.
//
// The match is intentionally identical: ^[a-z0-9][a-z0-9-]{0,63}$
// — see IsValidRegion doc comment.
func isValidRegionIdentifier(s string) bool {
	if len(s) < 1 || len(s) > 64 {
		return false
	}
	if s[0] < 'a' || s[0] > 'z' {
		if !(s[0] >= '0' && s[0] <= '9') {
			return false
		}
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-') {
			return false
		}
	}
	return true
}

// MigrationConfig holds paths to migration toolchain binaries.
type MigrationConfig struct {
	EdgeMigratePath string `yaml:"edge_migrate_path" env:"EDGE_MIGRATE_PATH" envDefault:"edge-migrate"`
	WasiSdkPath     string `yaml:"wasi_sdk_path"     env:"WASI_SDK_PATH"     envDefault:"/usr/local/wasi-sdk/bin"`
	RustcPath       string `yaml:"rustc_path"        env:"RUSTC_PATH"        envDefault:"rustc"`
	// Wasm2CwasmPath is the path to the wasm2cwasm binary used to
	// pre-compile .wasm artifacts to .cwasm during activation. When
	// empty, the pre-compilation step is skipped and workers JIT-compile
	// lazily on first load. Set via EDGE_WASM2CWASM_PATH env var.
	Wasm2CwasmPath string `yaml:"wasm2cwasm_path" env:"EDGE_WASM2CWASM_PATH"`
	// WasmToolsPath is the path to the wasm-tools binary used by the
	// rust migration path to wrap the cargo-produced core module into a
	// wasi:http@0.2.1 component (issue #415 — wasi:http version
	// mismatch). The default "wasm-tools" resolves via $PATH. Operators
	// must install wasm-tools on the CP host (cargo install
	// wasm-tools --locked). The migration returns 422 with a clear
	// error if this binary is missing.
	WasmToolsPath string `yaml:"wasm_tools_path" env:"EDGE_WASM_TOOLS_PATH" envDefault:"wasm-tools"`
	// CargoPath is the path to the cargo binary used by the rust
	// migration path to compile the user-submitted source against the
	// canonical WIT tree (issue #415 — bare rustc cannot resolve the
	// wit_bindgen::generate! proc-macro or its wasi:* dependency
	// surface). The default "cargo" resolves via $PATH. The migration
	// returns 422 with a clear error if this binary is missing or
	// the wasm32-unknown-unknown target is not installed.
	CargoPath string `yaml:"cargo_path" env:"EDGE_CARGO_PATH" envDefault:"cargo"`
}

// SigningConfig configures the Ed25519 signing key used to sign
// deployment artifacts (issue #307). The CP signs every new
// deployment's artifact at upload time; workers verify the
// signature before instantiation. The private key never leaves the
// CP. The public key is propagated to workers out-of-band (today:
// the EDGE_SIGNING_KEYRING env var on each worker). At least one of
// KeyPath / Key / KeyringPath / Keyring must be set or the CP fails
// to start.
type SigningConfig struct {
	// KeyPath is the path to a file containing a single Ed25519
	// private key. Legacy single-key form; superseded by
	// KeyringPath for rotation-aware deployments. Two formats are
	// accepted (selected by file size): 32 raw bytes (seed form,
	// expanded via ed25519.NewKeyFromSeed) or 64 raw bytes (the
	// full private key per RFC 8032 §5.1.2). Hex variants (64 or
	// 128 hex chars) of either are also accepted.
	KeyPath string `yaml:"key_path"`
	// Key is the inline single Ed25519 private key, used when
	// KeyPath is unset (typical in container deployments where the
	// key is injected via a sealed secret). Legacy single-key form.
	Key string `yaml:"key"`
	// KeyID is a logical identifier (operator-chosen, e.g. "k1")
	// stamped onto each `deployments` row at sign time. For the
	// single-key legacy form this is implicit ("default"). For the
	// keyring form it picks which key in the keyring is the
	// active signing key.
	KeyID string `yaml:"key_id"`
	// KeyringPath is the path to a multi-key keyring file
	// (issue #307 follow-up PR1). File format: one
	// `<kid> = <32-byte seed hex>` per line; see
	// `signing/keyring.go` for the full grammar. Takes precedence
	// over KeyPath / Key when set. Loaded via
	// `signing.LoadKeyringFromFile`.
	KeyringPath string `yaml:"keyring_path"`
	// Keyring is the inline multi-key keyring payload, used when
	// KeyringPath is unset. Same format as the file.
	Keyring string `yaml:"keyring"`
}

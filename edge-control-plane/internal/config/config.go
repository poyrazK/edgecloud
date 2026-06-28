package config

import (
	"fmt"
	"os"
	"strconv"

	"gopkg.in/yaml.v3"
)

// Config holds all application configuration.
type Config struct {
	Database  DatabaseConfig  `yaml:"database"`
	NATS      NATSConfig      `yaml:"nats"`
	App       AppConfig       `yaml:"app"`
	Storage   StorageConfig   `yaml:"storage"`
	JWT       JWTConfig       `yaml:"jwt"`
	Migration MigrationConfig `yaml:"migration"`
	// Region is this control plane's own region. Used as the default
	// `regions` list for deployments that don't explicitly opt into
	// multi-region — preserves today's "publish one TaskMessage to a
	// single subject" behavior. The wildcard NATS subject
	// `edgecloud.tasks.>` (configured in cmd/api/main.go) means the
	// literal default `"global"` works for any worker regardless of
	// its own region. See `service.ActivateDeployment` for the
	// fallback path. (Issue #82, v1.)
	Region string `yaml:"region"`
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
	URL string `yaml:"url"`
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
}

type JWTConfig struct {
	Secret string `yaml:"secret"`
	TTL    int    `yaml:"ttl_hours"`
	Issuer string `yaml:"issuer"`
	// BootstrapPSK is the pre-shared key workers use to enroll via
	// `POST /api/internal/auth/token` (Phase 4). Must match
	// `WORKER_BOOTSTRAP_PSK` on the worker side.
	//
	// Empty disables the bootstrap endpoint entirely (the route
	// returns 503). Operators wanting strict enrollment-only auth
	// can run with `BootstrapPSK` set and `Secret` set to a strong
	// random value used only for token verification — the two
	// paths are independent at the server level.
	//
	// Validated like Secret: non-empty + ≥32 bytes when set. We
	// skip the "known placeholder" check (the one on Secret)
	// because operators typically source PSKs from a secrets
	// manager rather than a config file — a placeholder check
	// would generate false positives on short randomly-generated
	// keys that happen to collide with a known-bad value.
	BootstrapPSK string `yaml:"bootstrap_psk"`
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
	if v := os.Getenv("BOOTSTRAP_PSK"); v != "" {
		cfg.JWT.BootstrapPSK = v
	}
	if v := os.Getenv("CONTROL_PLANE_REGION"); v != "" {
		cfg.Region = v
	}
	if v := os.Getenv("EDGE_INTERNAL_TOKEN"); v != "" {
		cfg.InternalToken = v
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

	// Reject insecure JWT secrets. Operators frequently ship with the
	// default `change-me-in-production` placeholder and forget to override
	// it; failing startup is louder and safer than silently running with a
	// publicly-known secret. (Audit finding #2 — also referenced by tests.)
	if err := validateJWTSecret(cfg.JWT.Secret); err != nil {
		return nil, err
	}

	// Validate the artifact-storage backend selection and its per-backend
	// required fields. Run after env overrides so a STORAGE_ARTIFACT_BACKEND
	// env var that names a backend whose required fields are missing from
	// the YAML still fails startup with a clear message.
	if err := validateStorageConfig(&cfg.Storage); err != nil {
		return nil, err
	}

	// Bootstrap PSK is optional; when set, it must be ≥32 bytes (same
	// floor as the JWT secret — short keys make HMAC-SHA256 brute-force
	// feasible). Empty PSK leaves the bootstrap endpoint disabled; the
	// route still exists but returns 503 from the middleware, so
	// operators can roll out the worker side before the server side
	// without a config mismatch producing confusing 401s.
	if err := validateBootstrapPSK(cfg.JWT.BootstrapPSK); err != nil {
		return nil, err
	}

	return &cfg, nil
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

// validateJWTSecret enforces three rules in priority order:
//  1. secret must be set (non-empty),
//  2. secret must not match a known placeholder,
//  3. secret must be at least 32 bytes long.
//
// Empty and placeholder checks are separate because the operator action
// they imply is different (set the var vs. choose a unique value).
func validateJWTSecret(s string) error {
	if s == "" {
		return fmt.Errorf("jwt.secret is not set; set JWT_SECRET or jwt.secret to a unique value")
	}
	if _, ok := insecureJWTSecretValues[s]; ok {
		return fmt.Errorf("jwt.secret %q is a known placeholder; set JWT_SECRET or jwt.secret to a unique value", s)
	}
	if len(s) < 32 {
		return fmt.Errorf("jwt.secret must be at least 32 bytes (got %d)", len(s))
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
	return nil
}

// validateBootstrapPSK enforces two rules (in priority order):
//  1. when set, the PSK must be at least 32 bytes,
//  2. (no placeholder check — see JWTConfig.BootstrapPSK doc for the
//     rationale).
//
// Empty is allowed: the bootstrap endpoint is opt-in. An empty PSK
// causes the route to return 503 instead of 401 so the failure mode
// is distinguishable from a wrong-PSK submission.
func validateBootstrapPSK(s string) error {
	if s == "" {
		return nil
	}
	if len(s) < 32 {
		return fmt.Errorf("jwt.bootstrap_psk must be at least 32 bytes when set (got %d)", len(s))
	}
	return nil
}

// MigrationConfig holds paths to migration toolchain binaries.
type MigrationConfig struct {
	EdgeMigratePath string `yaml:"edge_migrate_path" env:"EDGE_MIGRATE_PATH" envDefault:"edge-migrate"`
	WasiSdkPath     string `yaml:"wasi_sdk_path"     env:"WASI_SDK_PATH"     envDefault:"/usr/local/wasi-sdk/bin"`
	// RustcPath is the absolute path to a rustc binary capable of
	// targeting wasm32-wasip2 (i.e. `rustup target add wasm32-wasip2`
	// has been run on the host). Used by the migration service when
	// language == "rust" to compile the transformed source into a
	// wasm component. Falls back to "rustc" (PATH lookup) if unset.
	RustcPath string `yaml:"rustc_path" env:"RUSTC_PATH" envDefault:"rustc"`
}

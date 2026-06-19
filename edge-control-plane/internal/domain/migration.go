package domain

// MigrationStatus represents the overall outcome of a migration.
type MigrationStatus string

const (
	MigrationStatusSuccess = MigrationStatus("success") // all patterns auto-transformed
	MigrationStatusPartial = MigrationStatus("partial") // some require manual review
	MigrationStatusFailed  = MigrationStatus("failed")  // untransformable patterns detected
)

// MigrateEnvelopeVersion is the wire-format version of the
// `TransformOutput` envelope emitted by `edge-migrate --format json`.
// Must match edge-migrate-lib's `TRANSFORM_OUTPUT_VERSION`. Bumped when
// the envelope's JSON shape changes in a way that requires consumer
// coordination (renamed keys, changed semantics). Additive changes
// (new optional fields) do NOT require a bump — consumers should
// ignore unknown fields.
const MigrateEnvelopeVersion uint32 = 1

// MigrationReport is the JSON response returned by POST /api/migrate.
type MigrationReport struct {
	Status               MigrationStatus `json:"status"`
	WasmStored           bool            `json:"wasm_stored"`
	DeploymentID         *string         `json:"deployment_id,omitempty"`
	AppName              string          `json:"app_name"`
	PatternsDetected     []PatternInfo   `json:"patterns_detected"`
	PatternsTransformed  []PatternInfo   `json:"patterns_transformed"`
	PatternsManualReview []PatternInfo   `json:"patterns_manual_review"`
	Errors               []ErrorInfo     `json:"errors"`
}

// Transformability classifies how transformable a POSIX pattern is.
// Wire form is kebab-case to match edge-migrate-lib's `Transformability`
// serde rename. The named type catches typos at compile time (e.g.,
// `"auto-transformable "` with a stray trailing space) and lets code
// compare against typed constants instead of stringly-typed literals.
// The JSON wire format is unchanged from a plain string — Go marshals
// named string types identically to `string`.
type Transformability string

const (
	TransformabilityAutoTransformable Transformability = "auto-transformable"
	TransformabilityBestEffort        Transformability = "best-effort"
	TransformabilityNotTransformable  Transformability = "not-transformable"
)

// PatternInfo describes a single POSIX pattern detected in the source.
type PatternInfo struct {
	Line             int              `json:"line"`
	Pattern          string           `json:"pattern"`
	Snippet          string           `json:"snippet"`
	WasiEquivalent   string           `json:"wasi_equivalent"`
	Transformability Transformability `json:"transformability"`
}

// ErrorInfo describes an error encountered during migration.
type ErrorInfo struct {
	Line    int    `json:"line"`
	Message string `json:"message"`
}

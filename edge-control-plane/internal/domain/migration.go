package domain

// MigrationStatus represents the overall outcome of a migration.
type MigrationStatus string

const (
	MigrationStatusSuccess = MigrationStatus("success") // all patterns auto-transformed
	MigrationStatusPartial = MigrationStatus("partial") // some require manual review
	// MigrationStatusFailed is returned when migration could not produce a
	// runnable artifact. Two cases both surface as Failed:
	//   1. The analyzer classified every detected pattern as
	//      NotTransformable (Rust side: `Failed` here, never `Success`).
	//   2. The toolchain (clang / rustc) refused to compile the
	//      transformed source. Other artifact-validation failures
	//      produced by code paths below — the resulting wasm
	//      exceeding MaxArtifactSize, or failing the 4-byte
	//      wasm magic-number check — also surface as Failed, since
	//      no runnable artifact was produced.
	// Tenants should read the Errors[] array for the cause.
	MigrationStatusFailed  = MigrationStatus("failed")
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
	Status               MigrationStatus   `json:"status"`
	WasmStored           bool              `json:"wasm_stored"`
	DeploymentID         *string           `json:"deployment_id,omitempty"`
	AppName              string            `json:"app_name"`
	PatternsDetected     []PatternInfo     `json:"patterns_detected"`
	PatternsTransformed  []PatternInfo     `json:"patterns_transformed"`
	PatternsManualReview []PatternInfo     `json:"patterns_manual_review"`
	Errors               []ErrorInfo       `json:"errors"`
	Preprocessor         *PreprocessorInfo `json:"preprocessor,omitempty"`
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

// PreprocessorInfo is a mirror of edge-migrate-lib's PreprocessorInfo
// (see edge-migrate/edge-migrate-lib/src/preprocessor.rs). Populated
// by the per-file `--analyze --json` subprocess in MigrateTree.
type PreprocessorInfo struct {
	ClangVersion   *string `json:"clang_version,omitempty"`
	FilesProcessed int     `json:"files_processed"`
	MacrosExpanded int     `json:"macros_expanded"`
}

// FileEntry is a single source file carried in the /api/migrate-tree
// multipart form (variant A: per-file `file` parts). The client-side
// `path` is forward-slash-relative to the tree root. `Source` is
// populated by the handler before the service is called.
type FileEntry struct {
	Path   string `json:"path"`
	Source string `json:"source"`
}

// FileReport is the per-file result embedded in TreeMigrationReport.
// Mirrors the Rust FileReport type (see
// edge-migrate/edge-migrate-lib/src/report.rs). JSON tags must match
// the Rust serialization exactly.
type FileReport struct {
	Path             string            `json:"path"`
	Status           MigrationStatus   `json:"status"`
	PatternsDetected []PatternInfo     `json:"patterns_detected"`
	Transformations  []PatternInfo     `json:"transformations"`
	ManualReview     []PatternInfo     `json:"manual_review"`
	Errors           []ErrorInfo       `json:"errors"`
	Preprocessor     *PreprocessorInfo `json:"preprocessor,omitempty"`
}

// TreeMigrationReport is the JSON response returned by
// POST /api/migrate-tree. Aggregates per-file FileReports with
// tree-level metadata.
type TreeMigrationReport struct {
	Status            MigrationStatus `json:"status"`
	WasmStored        bool            `json:"wasm_stored"`
	DeploymentID      *string         `json:"deployment_id,omitempty"`
	AppName           string          `json:"app_name"`
	Files             []FileReport    `json:"files"`
	Errors            []ErrorInfo     `json:"errors"`
	FilesTotal        int             `json:"files_total"`
	FilesTransformed  int             `json:"files_transformed"`
	FilesManualReview int             `json:"files_manual_review"`
}

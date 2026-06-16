package domain

// MigrationStatus represents the overall outcome of a migration.
type MigrationStatus string

const (
	MigrationStatusSuccess = "success" // all patterns auto-transformed
	MigrationStatusPartial = "partial" // some require manual review
	MigrationStatusFailed  = "failed"  // untransformable patterns detected
)

// MigrationReport is the JSON response returned by POST /api/migrate.
// It is serialized directly to the edge-migrate CLI client.
type MigrationReport struct {
	Status               MigrationStatus `json:"status"`
	WasmStored           bool           `json:"wasm_stored"`
	DeploymentID         *string       `json:"deployment_id,omitempty"`
	AppName              string        `json:"app_name"`
	PatternsDetected     []PatternInfo `json:"patterns_detected"`
	PatternsTransformed  []PatternInfo `json:"patterns_transformed"`
	PatternsManualReview []PatternInfo `json:"patterns_manual_review"`
	Errors               []ErrorInfo   `json:"errors"`
}

// PatternInfo describes a single POSIX pattern detected in the source.
type PatternInfo struct {
	Line             int    `json:"line"`
	Pattern          string `json:"pattern"`
	Snippet          string `json:"snippet"`
	WasiEquivalent   string `json:"wasi_equivalent"`
	Transformability string `json:"transformability"`
}

// ErrorInfo describes an error encountered during migration.
type ErrorInfo struct {
	Line    int    `json:"line"`
	Message string `json:"message"`
}

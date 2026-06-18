package domain

import (
	"encoding/json"
	"testing"
)

func TestFileReport_JSONRoundTrip(t *testing.T) {
	// Confirm JSON tags are byte-for-byte identical to the Rust
	// FileReport (edge-migrate/edge-migrate-lib/src/report.rs). The
	// Go service uses json.Unmarshal to read the per-file report
	// emitted by the `edge-migrate --analyze --json` subprocess, so
	// any drift breaks the wire format silently.
	report := FileReport{
		Path:             "src/main.c",
		Status:           MigrationStatusPartial,
		PatternsDetected: []PatternInfo{{Line: 1, Pattern: "SocketTcp", Snippet: "socket(...)", WasiEquivalent: "create-tcp-socket", Transformability: "AutoTransformable"}},
		Transformations:  []PatternInfo{{Line: 1, Pattern: "SocketTcp", Snippet: "socket(...)", WasiEquivalent: "create-tcp-socket", Transformability: "AutoTransformable"}},
		ManualReview:     []PatternInfo{{Line: 7, Pattern: "Poll", Snippet: "poll(...)", WasiEquivalent: "no WASI equivalent", Transformability: "NotTransformable"}},
		Errors:           []ErrorInfo{},
		Preprocessor: &PreprocessorInfo{
			ClangVersion:   nil,
			FilesProcessed: 1,
			MacrosExpanded: 0,
		},
	}
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// Spot-check required field names are present (Rust serde names).
	required := []string{
		`"path":"src/main.c"`,
		`"status":"partial"`,
		`"patterns_detected"`,
		`"transformations"`,
		`"manual_review"`,
		`"preprocessor"`,
		`"files_processed":1`,
	}
	for _, want := range required {
		if !contains(data, want) {
			t.Errorf("expected %q in JSON: %s", want, string(data))
		}
	}
	// Round-trip: marshal + unmarshal should preserve values.
	var back FileReport
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.Path != report.Path {
		t.Errorf("path mismatch: %q vs %q", back.Path, report.Path)
	}
	if back.Status != report.Status {
		t.Errorf("status mismatch: %q vs %q", back.Status, report.Status)
	}
	if len(back.ManualReview) != 1 {
		t.Errorf("expected 1 manual_review, got %d", len(back.ManualReview))
	}
	if back.Preprocessor == nil || back.Preprocessor.FilesProcessed != 1 {
		t.Errorf("preprocessor info not preserved: %+v", back.Preprocessor)
	}
}

func TestTreeMigrationReport_OptionalFieldsOmitWhenEmpty(t *testing.T) {
	// deployment_id and preprocessor must be omitted from JSON when nil.
	r := TreeMigrationReport{
		Status:            MigrationStatusSuccess,
		WasmStored:        false,
		DeploymentID:      nil,
		AppName:           "hello",
		Files:             []FileReport{},
		Errors:            []ErrorInfo{},
		FilesTotal:        0,
		FilesTransformed:  0,
		FilesManualReview: 0,
	}
	data, _ := json.Marshal(r)
	if contains(data, "deployment_id") {
		t.Errorf("deployment_id should be omitted when nil, got: %s", string(data))
	}
	// Required fields must be present.
	for _, want := range []string{`"status"`, `"app_name"`, `"files"`, `"files_total"`} {
		if !contains(data, want) {
			t.Errorf("missing %q: %s", want, string(data))
		}
	}
}

func contains(haystack []byte, needle string) bool {
	return len(haystack) >= len(needle) && indexOf(haystack, needle) >= 0
}

func indexOf(haystack []byte, needle string) int {
	if len(needle) == 0 {
		return 0
	}
outer:
	for i := 0; i+len(needle) <= len(haystack); i++ {
		for j := 0; j < len(needle); j++ {
			if haystack[i+j] != needle[j] {
				continue outer
			}
		}
		return i
	}
	return -1
}
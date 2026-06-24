//! Integration test for `edge-migrate --transform --format json`.
//!
//! Pins the wire shape of the `TransformOutput` envelope that the Go
//! control plane consumes. Runs the actual compiled binary as a child
//! process (matching the pattern in `cli_transform_test.rs`).
//!
//! The assertions deliberately use `serde_json::Value` rather than
//! deserializing into `TransformOutput` directly — that way the test
//! pins the WIRE shape (the actual JSON keys and value forms the Go
//! server will see), not the Rust struct layout. If we rename `wasi_c`
//! to `wasiC` in Rust, this test will fail before the Go side silently
//! breaks.

use std::process::Command;

#[test]
fn test_transform_json_outputs_envelope() {
    let test_file = concat!(env!("CARGO_MANIFEST_DIR"), "/../testdata/http_client.c");

    let output = Command::new(env!("CARGO_BIN_EXE_edge-migrate"))
        .args(["--transform", test_file, "--format", "json"])
        .output()
        .expect("failed to execute edge-migrate --transform --format json");

    assert!(
        output.status.success(),
        "edge-migrate --transform --format json exited non-zero: stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout must be utf-8");

    // Parse as generic JSON Value so this test stays decoupled from the
    // exact internal struct layout.
    let v: serde_json::Value = serde_json::from_str(&stdout).expect("output must be valid JSON");

    // Version field pins the wire-format contract; bumping
    // TRANSFORM_OUTPUT_VERSION is a breaking change for the Go server.
    assert_eq!(
        v["version"]
            .as_u64()
            .expect("version must be a non-negative integer"),
        1,
        "envelope version must be 1 (matches domain.MigrateEnvelopeVersion on Go side)"
    );

    let report = &v["report"];
    // #128: http_client.c contains an `accept(fd, NULL, NULL)` call.
    // The MVP fix downgrades accept from BestEffort to
    // NotTransformable so it lands in manual_review. The overall
    // status is therefore `partial` (not `success`) — the honest
    // answer is "we transformed what we could; accept needs manual
    // attention". `partial` is already a documented `MigrationStatus`
    // variant; the wire envelope shape is unchanged.
    assert_eq!(
        report["status"]
            .as_str()
            .expect("report.status is a string"),
        "partial",
        "http_client.c has accept() which is not transformable in MVP, expect status=partial"
    );

    let detected = report["patterns_detected"]
        .as_array()
        .expect("patterns_detected must be an array");
    // http_client.c has socket + bind + listen + accept — expect at least 4.
    assert!(
        detected.len() >= 4,
        "expected at least 4 detected patterns (socket/bind/listen/accept), got: {}",
        detected.len()
    );

    // Every detected pattern must have a kebab-case transformability and a
    // non-zero line number. A future regression that drops the serde
    // rename (back to Debug CamelCase) would fail here, not silently
    // produce empty API responses.
    let valid = ["auto-transformable", "best-effort", "not-transformable"];
    for p in detected {
        let t = p["transformability"]
            .as_str()
            .expect("transformability must be a string");
        assert!(
            valid.contains(&t),
            "transformability must be one of {valid:?}, got: {t}"
        );
        assert!(
            p["line"].as_u64().expect("line is a non-negative integer") > 0,
            "line must be > 0 (was 0 in the old string-matching bug #3)"
        );
    }

    // wasi_c is the raw transformed source — must contain the WASI header
    // so the Go server has something to feed to clang.
    let wasi_c = v["wasi_c"].as_str().expect("wasi_c must be a string");
    assert!(
        wasi_c.contains("#include <wasi/sockets.h>"),
        "wasi_c must contain the WASI sockets header, got first 80 chars: {:?}",
        wasi_c.chars().take(80).collect::<String>()
    );
}
